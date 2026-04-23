"""Storage backend abstract base class."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncIterator


@dataclass
class StoredFile:
    """Metadata for a stored file."""

    key: str
    size: int
    last_modified: datetime
    metadata: dict[str, str] = field(default_factory=dict)
    content_type: str = "application/octet-stream"


class StorageBackend(ABC):
    """Abstract base class for all storage backends."""

    @abstractmethod
    async def put(
        self, key: str, data: bytes, metadata: dict[str, str] | None = None
    ) -> StoredFile:
        """Store data at the given key."""
        ...

    @abstractmethod
    async def get(self, key: str) -> bytes:
        """Retrieve data by key. Raises FileNotFoundError if not found."""
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete data by key. No-op if key doesn't exist."""
        ...

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        ...

    @abstractmethod
    async def list_files(self, prefix: str) -> AsyncIterator[StoredFile]:
        """List all files under a prefix."""
        ...

    @abstractmethod
    async def get_url(self, key: str, expires_in: int = 3600) -> str:
        """Get a URL for the file.

        For filesystem, returns file:// path.
        For S3, returns a presigned URL.
        """
        ...
