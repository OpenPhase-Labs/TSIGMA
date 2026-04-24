"""S3-compatible storage backend."""

import logging
from datetime import datetime, timezone
from typing import AsyncIterator

from .base import StorageBackend, StoredFile

logger = logging.getLogger(__name__)


class S3Backend(StorageBackend):
    """Storage backend using S3-compatible object storage (AWS S3, MinIO, etc.)."""

    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ) -> None:
        try:
            # Import-as-presence-check for an optional dependency. The binding
            # is intentionally unused — we only care that the import succeeds.
            import aiobotocore  # noqa: F401  # presence check; removing this import would defeat the runtime guard
        except ImportError:
            raise ImportError(
                "The 's3' storage backend requires aiobotocore. "
                "Install it with: pip install aiobotocore"
            )

        self._bucket = bucket
        self._region = region
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._session = None
        self._client = None
        logger.info(
            "S3 storage initialised for bucket=%s region=%s endpoint=%s",
            bucket,
            region,
            endpoint_url or "(default)",
        )

    async def _get_client(self):
        """Get or create the S3 client (lazy initialisation)."""
        if self._client is None:
            from aiobotocore.session import AioSession

            self._session = AioSession()
            kwargs = {
                "region_name": self._region,
            }
            if self._endpoint_url:
                kwargs["endpoint_url"] = self._endpoint_url
            if self._access_key and self._secret_key:
                kwargs["aws_access_key_id"] = self._access_key
                kwargs["aws_secret_access_key"] = self._secret_key

            ctx = self._session.create_client("s3", **kwargs)
            self._client = await ctx.__aenter__()
            self._client_ctx = ctx
        return self._client

    async def put(
        self, key: str, data: bytes, metadata: dict[str, str] | None = None
    ) -> StoredFile:
        """Store data at the given key."""
        client = await self._get_client()
        put_kwargs: dict = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": data,
        }
        if metadata:
            put_kwargs["Metadata"] = metadata

        await client.put_object(**put_kwargs)

        stored = StoredFile(
            key=key,
            size=len(data),
            last_modified=datetime.now(timezone.utc),
            metadata=metadata or {},
        )
        logger.debug("Stored s3://%s/%s (%d bytes)", self._bucket, key, len(data))
        return stored

    async def get(self, key: str) -> bytes:
        """Retrieve data by key. Raises FileNotFoundError if not found."""
        client = await self._get_client()
        try:
            response = await client.get_object(Bucket=self._bucket, Key=key)
        except client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Storage key not found: {key}")
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(f"Storage key not found: {key}")
            raise

        async with response["Body"] as stream:
            data = await stream.read()
        return data

    async def delete(self, key: str) -> None:
        """Delete data by key. No-op if key doesn't exist."""
        client = await self._get_client()
        await client.delete_object(Bucket=self._bucket, Key=key)
        logger.debug("Deleted s3://%s/%s", self._bucket, key)

    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        client = await self._get_client()
        try:
            await client.head_object(Bucket=self._bucket, Key=key)
            return True
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise

    async def list_files(self, prefix: str) -> AsyncIterator[StoredFile]:
        """List all files under a prefix."""
        client = await self._get_client()
        paginator = client.get_paginator("list_objects_v2")

        async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield StoredFile(
                    key=obj["Key"],
                    size=obj["Size"],
                    last_modified=obj["LastModified"],
                )

    async def get_url(self, key: str, expires_in: int = 3600) -> str:
        """Generate a presigned URL for the stored file."""
        client = await self._get_client()
        url = await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=expires_in,
        )
        return url

    async def close(self) -> None:
        """Close the S3 client connection."""
        if self._client is not None:
            await self._client_ctx.__aexit__(None, None, None)
            self._client = None
            self._session = None


def _is_not_found(exc: Exception) -> bool:
    """Check if an exception represents a 404 / not-found response."""
    try:
        from botocore.exceptions import ClientError

        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            return code in ("404", "NoSuchKey")
    except ImportError:
        pass
    return getattr(exc, "response", {}).get("Error", {}).get("Code", "") in (
        "404",
        "NoSuchKey",
    )
