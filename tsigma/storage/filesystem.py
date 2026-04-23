"""Filesystem storage backend."""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

from .base import StorageBackend, StoredFile

logger = logging.getLogger(__name__)


class FilesystemBackend(StorageBackend):
    """Storage backend using the local filesystem."""

    def __init__(self, base_path: str) -> None:
        self._base = Path(base_path)
        logger.info("Filesystem storage initialised at %s", self._base)

    def _resolve(self, key: str) -> Path:
        """Resolve a storage key to an absolute filesystem path.

        Raises:
            ValueError: If the key would escape the base directory.
        """
        resolved = (self._base / key).resolve()
        if not resolved.is_relative_to(self._base.resolve()):
            raise ValueError(
                f"Path traversal denied: {key!r} escapes storage root"
            )
        return resolved

    async def put(
        self, key: str, data: bytes, metadata: dict[str, str] | None = None
    ) -> StoredFile:
        """Store data at the given key."""
        path = self._resolve(key)

        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

        await asyncio.to_thread(_write)

        stat = await asyncio.to_thread(path.stat)
        stored = StoredFile(
            key=key,
            size=len(data),
            last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            metadata=metadata or {},
        )
        logger.debug("Stored %s (%d bytes)", key, len(data))
        return stored

    async def get(self, key: str) -> bytes:
        """Retrieve data by key. Raises FileNotFoundError if not found."""
        path = self._resolve(key)
        try:
            data: bytes = await asyncio.to_thread(path.read_bytes)
        except FileNotFoundError:
            raise FileNotFoundError(f"Storage key not found: {key}")
        return data

    async def delete(self, key: str) -> None:
        """Delete data by key. No-op if key doesn't exist."""
        path = self._resolve(key)

        def _unlink() -> None:
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        await asyncio.to_thread(_unlink)
        logger.debug("Deleted %s", key)

    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        path = self._resolve(key)
        return await asyncio.to_thread(path.exists)

    async def list_files(self, prefix: str) -> AsyncIterator[StoredFile]:
        """List all files under a prefix."""
        base = self._resolve(prefix)

        def _collect() -> list[Path]:
            if not base.exists():
                return []
            if base.is_file():
                return [base]
            return [p for p in base.rglob("*") if p.is_file()]

        paths = await asyncio.to_thread(_collect)

        for path in paths:
            stat = await asyncio.to_thread(path.stat)
            yield StoredFile(
                key=str(path.relative_to(self._base)),
                size=stat.st_size,
                last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            )

    async def get_url(self, key: str, expires_in: int = 3600) -> str:
        """Return a file:// URL for the stored file."""
        path = self._resolve(key)
        if not await asyncio.to_thread(path.exists):
            raise FileNotFoundError(f"Storage key not found: {key}")
        return path.resolve().as_uri()
