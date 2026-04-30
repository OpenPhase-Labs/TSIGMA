"""
Directory watch ingestion method.

Watches local directories for CSV/dat files dropped by external
processes (manual uploads, third-party tools, controller download
utilities).  Processes files on arrival and persists decoded events
through the ingestion target (controller_event_log for signals,
roadside_event for sensors).

Layer-2 server config (paths to watch, glob patterns, default decoder)
comes from process env vars via ``ListenerService``.  Per-device routing
(decoder override, signal_id override, processed/error subdirs, recursive
flag) comes from each device's ``metadata.collection`` JSONB and is
passed in via the ``devices`` argument from the orchestrator.

A device's ``signal_id`` is the orchestrator-supplied ``device_id``
unless overridden in its JSONB.  When no per-device config exists for
a watch path, files in that path resolve their device via filename
convention: everything before the first underscore is treated as the
device_id (``gdot-0142_20260415_events.dat`` → ``gdot-0142``).

Uses the watchdog library for filesystem monitoring.  Files that arrive
while TSIGMA is stopped are picked up on startup via an initial
directory scan.

This is an EventDrivenIngestionMethod — the ListenerService manages
start/stop lifecycle.
"""

import asyncio
import fnmatch
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from pydantic import BaseModel, Field
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from ..decoders.base import DecoderRegistry
from ..registry import EventDrivenIngestionMethod, IngestionMethodRegistry
from ..targets import ControllerTarget, IngestionTarget

logger = logging.getLogger(__name__)

_EXTENSION_DECODER_MAP = {
    ".dat": "asc3",
    ".csv": "csv",
}

_WRITE_SETTLE_SECONDS = 0.5
_RETRY_DELAY_SECONDS = 1.0


class DirectoryWatchServerConfig(BaseModel):
    """Layer-2 server config for the directory watcher."""

    paths: list[str]
    patterns: list[str] = Field(
        default_factory=lambda: ["*.dat", "*.csv", "*.DAT", "*.CSV"],
    )
    decoder: str = "auto"
    move_after_processing: bool = True
    processed_subdir: str = "processed"
    error_subdir: str = "errors"
    recursive: bool = False


class _FileEventHandler(FileSystemEventHandler):
    """Bridges synchronous watchdog callbacks to async file processing."""

    def __init__(
        self,
        watch_root: str,
        cfg: DirectoryWatchServerConfig,
        loop: asyncio.AbstractEventLoop,
        method: "DirectoryWatchMethod",
    ) -> None:
        super().__init__()
        self._watch_root = watch_root
        self._cfg = cfg
        self._loop = loop
        self._method = method

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._schedule(event.src_path)

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._schedule(event.dest_path)

    def _schedule(self, file_path: str) -> None:
        filename = os.path.basename(file_path)
        if not self._matches_patterns(filename):
            return
        asyncio.run_coroutine_threadsafe(
            self._method._process_file(file_path, self._watch_root),
            self._loop,
        )

    def _matches_patterns(self, filename: str) -> bool:
        return any(
            fnmatch.fnmatch(filename, p) for p in self._cfg.patterns
        )


@IngestionMethodRegistry.register("directory_watch")
class DirectoryWatchMethod(EventDrivenIngestionMethod):
    """Watches one or more directories and processes matching files."""

    name = "directory_watch"

    def __init__(self) -> None:
        self._observers: list[Observer] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._cfg: Optional[DirectoryWatchServerConfig] = None
        self._target: IngestionTarget = ControllerTarget()
        self._session_factory = None
        # device_id -> per-device collection.* dict (decoder override etc.)
        self._device_overrides: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _build_server_config(raw: dict[str, Any]) -> DirectoryWatchServerConfig:
        paths = raw.get("paths") or []
        if not paths:
            raise ValueError(
                "directory_watch config requires at least one path "
                "(set TSIGMA_DIRECTORY_WATCH_PATHS)",
            )
        # Resolve all paths to absolute up front to prevent traversal.
        resolved = [str(Path(p).resolve()) for p in paths]
        return DirectoryWatchServerConfig(
            paths=resolved,
            patterns=raw.get("patterns") or [
                "*.dat", "*.csv", "*.DAT", "*.CSV",
            ],
            decoder=raw.get("decoder") or "auto",
            move_after_processing=raw.get("move_after_processing", True),
            processed_subdir=raw.get("processed_subdir", "processed"),
            error_subdir=raw.get("error_subdir", "errors"),
            recursive=bool(raw.get("recursive", False)),
        )

    @staticmethod
    def _build_device_overrides(
        devices: Iterable[tuple[str, dict[str, Any]]],
    ) -> dict[str, dict[str, Any]]:
        return {device_id: dict(config) for device_id, config in devices}

    async def start(
        self,
        config: dict[str, Any],
        session_factory,
        *,
        target: Any = None,
        devices: Any = None,
    ) -> None:
        self._cfg = self._build_server_config(config)
        self._session_factory = session_factory
        self._target = target if target is not None else ControllerTarget()
        self._device_overrides = self._build_device_overrides(devices or [])
        self._loop = asyncio.get_running_loop()

        for raw_path in self._cfg.paths:
            watch_dir = Path(raw_path)
            if not watch_dir.is_dir():
                logger.error(
                    "Directory watch path does not exist: %s — skipping",
                    raw_path,
                )
                continue

            handler = _FileEventHandler(
                str(watch_dir), self._cfg, self._loop, self,
            )
            observer = Observer()
            observer.schedule(
                handler, str(watch_dir), recursive=self._cfg.recursive,
            )
            observer.start()
            self._observers.append(observer)

            logger.info(
                "Started directory watcher (%s) on %s "
                "(recursive=%s, patterns=%s)",
                self._target.device_type,
                raw_path, self._cfg.recursive, self._cfg.patterns,
            )

            await self._startup_scan(str(watch_dir))

    async def stop(self) -> None:
        for observer in self._observers:
            try:
                observer.stop()
                observer.join(timeout=5.0)
            except Exception:
                logger.exception("Error stopping directory watcher")
        self._observers.clear()
        if self._cfg is not None:
            logger.info("Stopped directory watcher")

    async def health_check(self) -> bool:
        if not self._observers:
            return False
        return all(o.is_alive() for o in self._observers)

    async def _startup_scan(self, watch_dir: str) -> None:
        """Pick up files that arrived while TSIGMA was stopped."""
        wd = Path(watch_dir)
        existing: list[Path] = []
        for pattern in self._cfg.patterns:
            if self._cfg.recursive:
                existing.extend(wd.rglob(pattern))
            else:
                existing.extend(wd.glob(pattern))

        seen: set[str] = set()
        unique: list[Path] = []
        for fp in existing:
            r = str(fp.resolve())
            if r not in seen:
                seen.add(r)
                unique.append(fp)
        unique.sort(key=lambda p: p.stat().st_mtime)

        if unique:
            logger.info(
                "Startup scan found %d existing files in %s",
                len(unique), watch_dir,
            )
            for fp in unique:
                await self._process_file(str(fp), watch_dir)

    async def _process_file(self, file_path: str, watch_dir: str) -> None:
        """Decode events from one file and persist via the target."""
        # Wait for the file write to settle.
        await asyncio.sleep(_WRITE_SETTLE_SECONDS)

        fp = Path(file_path)
        filename = fp.name

        if not fp.exists():
            logger.debug("File no longer exists, skipping: %s", filename)
            return

        device_id = self._resolve_device_id(filename)
        if device_id is None:
            logger.warning(
                "Cannot determine %s device_id for file %s, skipping",
                self._target.device_type, filename,
            )
            self._move_to_error(fp, watch_dir)
            return

        data = await self._read_file_with_retry(fp)
        if data is None:
            logger.error("Unable to read file %s (file locked)", filename)
            self._move_to_error(fp, watch_dir)
            return

        try:
            decoder = self._resolve_decoder(filename, device_id)
            events = decoder.decode_bytes(data)
        except Exception:
            logger.exception(
                "Failed to decode file %s for %s %s",
                filename, self._target.device_type, device_id,
            )
            self._move_to_error(fp, watch_dir)
            return

        try:
            await self._target.persist(
                events, device_id, self._session_factory,
            )
        except Exception:
            logger.exception(
                "Failed to persist events from %s for %s %s",
                filename, self._target.device_type, device_id,
            )
            self._move_to_error(fp, watch_dir)
            return

        logger.info(
            "Processed %s: %d events for %s %s",
            filename, len(events),
            self._target.device_type, device_id,
        )

        if self._cfg.move_after_processing:
            self._move_to_processed(fp, watch_dir)

    def _resolve_device_id(self, filename: str) -> Optional[str]:
        """Filename convention: everything before the first underscore.

        e.g. ``gdot-0142_20260415_events.dat`` → ``gdot-0142``.  If the
        resolved id is registered in ``devices``, we use it; otherwise
        we still return it (the caller will route to the target's
        upsert which will reject unknown ids if applicable).
        """
        stem = Path(filename).stem
        if "_" in stem:
            return stem.split("_", 1)[0]
        return None

    def _resolve_decoder(self, filename: str, device_id: str):
        """Per-device decoder override > server default > extension lookup."""
        per_device = self._device_overrides.get(device_id, {})
        explicit = per_device.get("decoder") or (
            self._cfg.decoder if self._cfg.decoder != "auto" else None
        )
        if explicit:
            cls = DecoderRegistry.get(explicit)
            return cls()

        ext = Path(filename).suffix.lower()
        if ext in _EXTENSION_DECODER_MAP:
            cls = DecoderRegistry.get(_EXTENSION_DECODER_MAP[ext])
            return cls()

        candidates = DecoderRegistry.get_for_extension(ext)
        if not candidates:
            raise ValueError(f"No decoder found for extension '{ext}'")
        return candidates[0]()

    @staticmethod
    async def _read_file_with_retry(file_path: Path) -> Optional[bytes]:
        for attempt in range(2):
            try:
                return file_path.read_bytes()
            except OSError:
                if attempt == 0:
                    logger.debug(
                        "File locked, retrying in %.1fs: %s",
                        _RETRY_DELAY_SECONDS, file_path.name,
                    )
                    await asyncio.sleep(_RETRY_DELAY_SECONDS)
        return None

    def _move_to_processed(self, file_path: Path, watch_dir: str) -> None:
        self._move(file_path, watch_dir, self._cfg.processed_subdir, "processed")

    def _move_to_error(self, file_path: Path, watch_dir: str) -> None:
        self._move(file_path, watch_dir, self._cfg.error_subdir, "errors")

    @staticmethod
    def _move(
        file_path: Path, watch_dir: str, subdir: str, label: str,
    ) -> None:
        dest_dir = Path(watch_dir) / subdir
        dest_dir.mkdir(parents=True, exist_ok=True)
        timestamp_prefix = datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S_",
        )
        dest_path = dest_dir / f"{timestamp_prefix}{file_path.name}"
        try:
            shutil.move(str(file_path), str(dest_path))
        except Exception:
            logger.exception(
                "Failed to move %s to %s", file_path.name, label,
            )
