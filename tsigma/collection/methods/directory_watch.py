"""
Directory watch ingestion method.

Watches local directories for CSV/dat files dropped by external
processes (e.g., manual uploads, third-party tools, controller
download utilities). Processes files on arrival and persists
decoded events to the database.

Uses the watchdog library for filesystem monitoring. Files that
arrive while TSIGMA is stopped are picked up on startup via an
initial directory scan.

This is an EventDrivenIngestionMethod — the CollectorService
manages start/stop lifecycle. File events trigger processing
asynchronously via the running event loop.
"""

import asyncio
import fnmatch
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from ..decoders.base import DecoderRegistry
from ..registry import (
    EventDrivenIngestionMethod,
    IngestionMethodRegistry,
)
from ..sdk import persist_events

logger = logging.getLogger(__name__)

_EXTENSION_DECODER_MAP = {
    ".dat": "asc3",
    ".csv": "csv",
}

_WRITE_SETTLE_SECONDS = 0.5
_RETRY_DELAY_SECONDS = 1.0


class DirectoryWatchConfig(BaseModel):
    """
    Configuration for the directory watch ingestion method.

    Args:
        watch_dir: Directory path to watch (required).
        file_patterns: Glob patterns for files to process.
        decoder: Explicit decoder name, or None to infer from extension.
        signal_id: Explicit signal ID, or None to infer from filename.
        move_after_processing: Move processed files to a subdirectory.
        processed_subdir: Subdirectory name for successfully processed files.
        error_subdir: Subdirectory name for files that failed processing.
        recursive: Watch subdirectories.
    """

    watch_dir: str
    file_patterns: list[str] = Field(
        default_factory=lambda: ["*.dat", "*.csv", "*.DAT", "*.CSV"]
    )
    decoder: Optional[str] = None
    signal_id: Optional[str] = None
    move_after_processing: bool = True
    processed_subdir: str = "processed"
    error_subdir: str = "errors"
    recursive: bool = False


class _FileEventHandler(FileSystemEventHandler):
    """
    Watchdog event handler that bridges filesystem events to async processing.

    Receives CREATE and MOVED_TO events from the watchdog observer thread
    and schedules async file processing on the main event loop.
    """

    def __init__(
        self,
        config: DirectoryWatchConfig,
        session_factory,
        loop: asyncio.AbstractEventLoop,
        method: "DirectoryWatchMethod",
    ) -> None:
        super().__init__()
        self._config = config
        self._session_factory = session_factory
        self._loop = loop
        self._method = method

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle file creation events."""
        if event.is_directory:
            return
        self._schedule_processing(event.src_path)

    def on_moved(self, event: FileSystemEvent) -> None:
        """Handle file move/rename events (MOVED_TO)."""
        if event.is_directory:
            return
        self._schedule_processing(event.dest_path)

    def _schedule_processing(self, file_path: str) -> None:
        """
        Schedule async file processing on the event loop.

        Args:
            file_path: Absolute path to the new/moved file.
        """
        filename = os.path.basename(file_path)
        if not self._matches_patterns(filename):
            return

        asyncio.run_coroutine_threadsafe(
            self._method._process_file(
                file_path, self._config, self._session_factory
            ),
            self._loop,
        )

    def _matches_patterns(self, filename: str) -> bool:
        """
        Check if filename matches any of the configured patterns.

        Args:
            filename: Name of the file (without directory).

        Returns:
            True if the file matches at least one pattern.
        """
        for pattern in self._config.file_patterns:
            if fnmatch.fnmatch(filename, pattern):
                return True
        return False


@IngestionMethodRegistry.register("directory_watch")
class DirectoryWatchMethod(EventDrivenIngestionMethod):
    """
    Directory watch ingestion method.

    An event-driven plugin: the CollectorService calls start() once
    at startup and stop() at shutdown. Filesystem events trigger
    asynchronous file processing via the watchdog library.

    On start, performs an initial scan of the watched directory
    to pick up files that arrived while TSIGMA was stopped.
    """

    name = "directory_watch"

    def __init__(self) -> None:
        self._observer: Optional[Observer] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @staticmethod
    def _build_config(raw: dict[str, Any]) -> DirectoryWatchConfig:
        """
        Build DirectoryWatchConfig from a raw config dict.

        Args:
            raw: Config dict from the collection configuration.

        Returns:
            DirectoryWatchConfig instance.

        Raises:
            ValueError: If watch_dir is not provided.
        """
        watch_dir = raw.get("watch_dir")
        if not watch_dir:
            raise ValueError("directory_watch config requires 'watch_dir'")

        # Resolve watch_dir to an absolute path to prevent traversal
        resolved_watch = Path(watch_dir).resolve()

        processed_subdir = raw.get("processed_subdir", "processed")
        error_subdir = raw.get("error_subdir", "errors")

        # Validate subdirs don't escape the watch directory
        for label, subdir in [
            ("processed_subdir", processed_subdir),
            ("error_subdir", error_subdir),
        ]:
            sub_resolved = (resolved_watch / subdir).resolve()
            if not sub_resolved.is_relative_to(resolved_watch):
                raise ValueError(
                    f"{label} {subdir!r} escapes watch_dir"
                )

        return DirectoryWatchConfig(
            watch_dir=str(resolved_watch),
            file_patterns=raw.get(
                "file_patterns", ["*.dat", "*.csv", "*.DAT", "*.CSV"]
            ),
            decoder=raw.get("decoder"),
            signal_id=raw.get("signal_id"),
            move_after_processing=raw.get("move_after_processing", True),
            processed_subdir=processed_subdir,
            error_subdir=error_subdir,
            recursive=raw.get("recursive", False),
        )

    async def start(self, config: dict[str, Any], session_factory) -> None:
        """
        Start watching for filesystem events.

        Creates the watchdog Observer, schedules the event handler,
        starts the observer thread, and performs an initial scan of
        the directory for any existing files.

        Args:
            config: Watcher config (directory path, patterns, etc.).
            session_factory: Async session factory for DB writes.
        """
        watch_config = self._build_config(config)
        watch_dir = Path(watch_config.watch_dir)

        if not watch_dir.is_dir():
            raise FileNotFoundError(
                f"Watch directory does not exist: {watch_config.watch_dir}"
            )

        self._loop = asyncio.get_running_loop()

        handler = _FileEventHandler(
            watch_config, session_factory, self._loop, self
        )

        self._observer = Observer()
        self._observer.schedule(
            handler, str(watch_dir), recursive=watch_config.recursive
        )
        self._observer.start()

        logger.info(
            "Started directory watcher on %s (recursive=%s, patterns=%s)",
            watch_config.watch_dir,
            watch_config.recursive,
            watch_config.file_patterns,
        )

        # Startup scan — process files that arrived while TSIGMA was stopped
        await self._startup_scan(watch_config, session_factory)

    async def stop(self) -> None:
        """Stop watching and release resources."""
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5.0)
            self._observer = None
            logger.info("Stopped directory watcher")

    async def health_check(self) -> bool:
        """
        Check if the directory watcher is healthy.

        Returns:
            True if the observer thread is alive, False otherwise.
        """
        if self._observer is None:
            return False
        return self._observer.is_alive()

    async def _startup_scan(
        self, config: DirectoryWatchConfig, session_factory
    ) -> None:
        """
        Scan the watched directory for existing files on startup.

        Processes any files matching the configured patterns that
        are already present in the directory. This ensures files
        that arrived while TSIGMA was stopped are not missed.

        Args:
            config: Directory watch configuration.
            session_factory: Async session factory for DB writes.
        """
        watch_dir = Path(config.watch_dir)
        existing_files: list[Path] = []

        for pattern in config.file_patterns:
            if config.recursive:
                existing_files.extend(watch_dir.rglob(pattern))
            else:
                existing_files.extend(watch_dir.glob(pattern))

        # Deduplicate (patterns may overlap) and sort by modification time
        seen: set[str] = set()
        unique_files: list[Path] = []
        for fp in existing_files:
            resolved = str(fp.resolve())
            if resolved not in seen:
                seen.add(resolved)
                unique_files.append(fp)
        unique_files.sort(key=lambda p: p.stat().st_mtime)

        if unique_files:
            logger.info(
                "Startup scan found %d existing files in %s",
                len(unique_files),
                config.watch_dir,
            )
            for fp in unique_files:
                await self._process_file(str(fp), config, session_factory)

    async def _process_file(
        self,
        file_path: str,
        config: DirectoryWatchConfig,
        session_factory,
    ) -> None:
        """
        Process a single file: decode events and persist to DB.

        Waits briefly for the file write to complete, reads the
        file, resolves the decoder, decodes events, persists them,
        and moves the file to the processed or error subdirectory.

        Args:
            file_path: Absolute path to the file to process.
            config: Directory watch configuration.
            session_factory: Async session factory for DB writes.
        """
        # Wait for the file write to settle
        await asyncio.sleep(_WRITE_SETTLE_SECONDS)

        file_path_obj = Path(file_path)
        filename = file_path_obj.name

        if not file_path_obj.exists():
            logger.debug("File no longer exists, skipping: %s", filename)
            return

        # Resolve signal_id
        signal_id = self._resolve_signal_id(filename, config)
        if signal_id is None:
            logger.warning(
                "Cannot determine signal_id for file %s, skipping", filename
            )
            self._move_to_error(file_path_obj, config)
            return

        # Read file contents with retry for locked files
        data = await self._read_file_with_retry(file_path_obj)
        if data is None:
            logger.error("Unable to read file %s (file locked)", filename)
            self._move_to_error(file_path_obj, config)
            return

        # Decode
        try:
            decoder = self._resolve_decoder(filename, config)
            events = decoder.decode_bytes(data)
        except Exception:
            logger.exception(
                "Failed to decode file %s for signal %s", filename, signal_id
            )
            self._move_to_error(file_path_obj, config)
            return

        # Persist
        try:
            await persist_events(events, signal_id, session_factory)
        except Exception:
            logger.exception(
                "Failed to persist events from %s for signal %s",
                filename,
                signal_id,
            )
            self._move_to_error(file_path_obj, config)
            return

        logger.info(
            "Processed %s: %d events for signal %s",
            filename,
            len(events),
            signal_id,
        )

        # Move to processed subdirectory
        if config.move_after_processing:
            self._move_to_processed(file_path_obj, config)

    @staticmethod
    def _resolve_signal_id(
        filename: str, config: DirectoryWatchConfig
    ) -> Optional[str]:
        """
        Determine the signal_id for a file.

        If signal_id is set in config, use it. Otherwise, extract
        from filename: everything before the first underscore.

        Args:
            filename: Name of the file (without directory).
            config: Directory watch configuration.

        Returns:
            Signal ID string, or None if it cannot be determined.
        """
        if config.signal_id:
            return config.signal_id

        # Convention: filename starts with signal_id before first underscore
        # e.g., "gdot-0142_20240115_events.dat" -> "gdot-0142"
        stem = Path(filename).stem
        if "_" in stem:
            return stem.split("_", 1)[0]

        return None

    @staticmethod
    def _resolve_decoder(filename: str, config: DirectoryWatchConfig):
        """
        Get a decoder instance for the given filename.

        Uses the explicit decoder from config if set. Otherwise,
        infers from the file extension using the extension-to-decoder
        map, falling back to DecoderRegistry extension lookup.

        Args:
            filename: Name of the file to decode.
            config: Directory watch configuration.

        Returns:
            Decoder instance.

        Raises:
            ValueError: If no decoder can be found.
        """
        if config.decoder:
            cls = DecoderRegistry.get(config.decoder)
            return cls()

        ext = Path(filename).suffix.lower()

        # Check explicit extension map first
        if ext in _EXTENSION_DECODER_MAP:
            cls = DecoderRegistry.get(_EXTENSION_DECODER_MAP[ext])
            return cls()

        # Fall back to registry extension lookup
        candidates = DecoderRegistry.get_for_extension(ext)
        if not candidates:
            raise ValueError(f"No decoder found for extension '{ext}'")
        return candidates[0]()

    @staticmethod
    async def _read_file_with_retry(file_path: Path) -> Optional[bytes]:
        """
        Read file contents, retrying once if the file is locked.

        Args:
            file_path: Path to the file.

        Returns:
            File contents as bytes, or None if the file is inaccessible.
        """
        for attempt in range(2):
            try:
                return file_path.read_bytes()
            except OSError:
                if attempt == 0:
                    logger.debug(
                        "File locked, retrying in %.1fs: %s",
                        _RETRY_DELAY_SECONDS,
                        file_path.name,
                    )
                    await asyncio.sleep(_RETRY_DELAY_SECONDS)
        return None

    @staticmethod
    def _move_to_processed(
        file_path: Path, config: DirectoryWatchConfig
    ) -> None:
        """
        Move a successfully processed file to the processed subdirectory.

        Adds a UTC timestamp prefix to the filename to avoid collisions.

        Args:
            file_path: Path to the processed file.
            config: Directory watch configuration.
        """
        dest_dir = Path(config.watch_dir) / config.processed_subdir
        dest_dir.mkdir(parents=True, exist_ok=True)

        timestamp_prefix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_")
        dest_path = dest_dir / f"{timestamp_prefix}{file_path.name}"

        try:
            shutil.move(str(file_path), str(dest_path))
        except Exception:
            logger.exception("Failed to move %s to processed", file_path.name)

    @staticmethod
    def _move_to_error(
        file_path: Path, config: DirectoryWatchConfig
    ) -> None:
        """
        Move a failed file to the error subdirectory.

        Adds a UTC timestamp prefix to the filename to avoid collisions.

        Args:
            file_path: Path to the failed file.
            config: Directory watch configuration.
        """
        dest_dir = Path(config.watch_dir) / config.error_subdir
        dest_dir.mkdir(parents=True, exist_ok=True)

        timestamp_prefix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_")
        dest_path = dest_dir / f"{timestamp_prefix}{file_path.name}"

        try:
            shutil.move(str(file_path), str(dest_path))
        except Exception:
            logger.exception("Failed to move %s to errors", file_path.name)
