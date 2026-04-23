"""Storage backend factory."""

import logging

from tsigma.config import settings

from .base import StorageBackend
from .filesystem import FilesystemBackend

logger = logging.getLogger(__name__)


def get_storage_backend() -> StorageBackend:
    """Create a storage backend based on configuration.

    Returns a FilesystemBackend by default, or an S3Backend
    when TSIGMA_STORAGE_BACKEND is set to "s3".
    """
    backend_type = settings.storage_backend.lower()

    if backend_type == "s3":
        from .s3 import S3Backend

        logger.info("Creating S3 storage backend (bucket=%s)", settings.storage_s3_bucket)
        return S3Backend(
            bucket=settings.storage_s3_bucket,
            region=settings.storage_s3_region,
            endpoint_url=settings.storage_s3_endpoint or None,
            access_key=settings.storage_s3_access_key or None,
            secret_key=settings.storage_s3_secret_key or None,
        )

    if backend_type != "filesystem":
        logger.warning(
            "Unknown storage backend '%s', falling back to filesystem",
            backend_type,
        )

    logger.info("Creating filesystem storage backend (path=%s)", settings.storage_path)
    return FilesystemBackend(base_path=settings.storage_path)
