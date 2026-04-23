# File Storage Backend

## Overview

TSIGMA includes a pluggable file storage subsystem for cold-tier exports, backups, raw device file archives, and any other blob-oriented I/O. Two backends ship out of the box:

- **Filesystem** -- stores files under a local directory (default).
- **S3** -- stores files in any S3-compatible object store (AWS S3, MinIO, etc.).

The active backend is selected at startup via the `TSIGMA_STORAGE_BACKEND` environment variable and accessed through a single factory function.

## Architecture

```
tsigma/storage/
    __init__.py          Public API: StorageBackend, StoredFile, get_storage_backend
    base.py              ABC + StoredFile dataclass
    factory.py           get_storage_backend() factory
    filesystem.py        FilesystemBackend
    s3.py                S3Backend (requires aiobotocore)
```

### StoredFile

A `@dataclass` returned by `put()` and `list_files()`:

| Field            | Type               | Default                      |
|------------------|--------------------|------------------------------|
| `key`            | `str`              | --                           |
| `size`           | `int`              | --                           |
| `last_modified`  | `datetime`         | --                           |
| `metadata`       | `dict[str, str]`   | `{}`                         |
| `content_type`   | `str`              | `"application/octet-stream"` |

### StorageBackend ABC

All backends implement six async methods:

| Method       | Signature                                                        | Notes                                                                 |
|--------------|------------------------------------------------------------------|-----------------------------------------------------------------------|
| `put`        | `(key, data, metadata=None) -> StoredFile`                       | Stores bytes at `key`. Optional metadata dict.                        |
| `get`        | `(key) -> bytes`                                                 | Returns raw bytes. Raises `FileNotFoundError` if missing.             |
| `delete`     | `(key) -> None`                                                  | Deletes the key. No-op if it does not exist.                          |
| `exists`     | `(key) -> bool`                                                  | Returns `True` when the key exists.                                   |
| `list_files` | `(prefix) -> AsyncIterator[StoredFile]`                          | Yields `StoredFile` for every file under the prefix.                  |
| `get_url`    | `(key, expires_in=3600) -> str`                                  | Filesystem: `file://` URI. S3: presigned URL (default 1-hour expiry). |

### StorageFactory

`get_storage_backend()` reads `settings.storage_backend`, instantiates the matching backend, and returns it. If the value is unrecognized it logs a warning and falls back to `FilesystemBackend`.

```python
from tsigma.storage import get_storage_backend

storage = get_storage_backend()
info = await storage.put("exports/2025-04-10.csv", csv_bytes)
```

The S3 backend import is deferred (inside the `if` branch) so that `aiobotocore` is only required when S3 is actually selected.

## Filesystem Backend

`FilesystemBackend` stores blobs as plain files under a configurable root directory.

**Constructor:** `FilesystemBackend(base_path: str)`

Key behaviors:

- **Path traversal protection** -- `_resolve()` calls `Path.resolve()` and checks `is_relative_to()` against the base path. Any key that would escape the root raises `ValueError`.
- **Directory auto-creation** -- `put()` creates parent directories as needed (`mkdir(parents=True, exist_ok=True)`).
- **Blocking I/O offloaded** -- all filesystem calls run through `asyncio.to_thread()` so the event loop is never blocked.
- **`list_files(prefix)`** -- uses `rglob("*")` under the resolved prefix path. If the prefix points to a single file, yields only that file.
- **`get_url(key)`** -- returns the `file://` URI via `Path.as_uri()`. Raises `FileNotFoundError` if the file does not exist.

### Configuration

| Environment Variable     | Default                    | Description              |
|--------------------------|----------------------------|--------------------------|
| `TSIGMA_STORAGE_BACKEND` | `filesystem`               | Must be `"filesystem"`.  |
| `TSIGMA_STORAGE_PATH`    | `/var/lib/tsigma/storage`  | Root directory for files. |

## S3 Backend

`S3Backend` stores blobs in an S3-compatible bucket using `aiobotocore` for async access.

**Constructor:**

```python
S3Backend(
    bucket: str,
    region: str = "us-east-1",
    endpoint_url: str | None = None,   # custom endpoint for MinIO, etc.
    access_key: str | None = None,
    secret_key: str | None = None,
)
```

Key behaviors:

- **Lazy client** -- the `aiobotocore` session and S3 client are created on the first operation (`_get_client()`), not at construction time.
- **Credentials** -- if `access_key` and `secret_key` are both provided they are passed directly. Otherwise `aiobotocore` falls back to its default credential chain (env vars, instance profile, etc.).
- **Custom endpoint** -- set `endpoint_url` for MinIO or other S3-compatible services.
- **`list_files(prefix)`** -- uses the `list_objects_v2` paginator so it handles buckets with arbitrarily many keys.
- **`get_url(key, expires_in)`** -- generates a presigned GET URL. Default expiry is 3600 seconds (1 hour).
- **`close()`** -- S3Backend exposes an explicit `close()` coroutine to shut down the client context. Call it during application shutdown.
- **Error handling** -- `get()` and `exists()` catch `NoSuchKey` and `ClientError` with code `404` via the helper `_is_not_found()` and translate them to `FileNotFoundError` or `False`.
- **Dependency** -- requires `aiobotocore`. The import is guarded; if missing, `__init__` raises `ImportError` with install instructions.

### Configuration

| Environment Variable            | Default        | Description                                             |
|---------------------------------|----------------|---------------------------------------------------------|
| `TSIGMA_STORAGE_BACKEND`        | --             | Must be `"s3"`.                                         |
| `TSIGMA_STORAGE_S3_BUCKET`      | `""`           | Bucket name (required).                                 |
| `TSIGMA_STORAGE_S3_REGION`      | `us-east-1`    | AWS region.                                             |
| `TSIGMA_STORAGE_S3_ENDPOINT`    | `""`           | Custom endpoint URL (e.g. `http://minio:9000`). Empty = AWS default. |
| `TSIGMA_STORAGE_S3_ACCESS_KEY`  | `""`           | AWS access key. Empty = use default credential chain.   |
| `TSIGMA_STORAGE_S3_SECRET_KEY`  | `""`           | AWS secret key. Empty = use default credential chain.   |

## Configuration Reference

All storage settings live in `tsigma.config.Settings` with the `TSIGMA_` env-var prefix (case-insensitive).

| Setting                    | Env Var                         | Type   | Default                    |
|----------------------------|---------------------------------|--------|----------------------------|
| `storage_backend`          | `TSIGMA_STORAGE_BACKEND`        | `str`  | `"filesystem"`             |
| `storage_path`             | `TSIGMA_STORAGE_PATH`           | `str`  | `"/var/lib/tsigma/storage"`|
| `storage_s3_bucket`        | `TSIGMA_STORAGE_S3_BUCKET`      | `str`  | `""`                       |
| `storage_s3_region`        | `TSIGMA_STORAGE_S3_REGION`      | `str`  | `"us-east-1"`              |
| `storage_s3_endpoint`      | `TSIGMA_STORAGE_S3_ENDPOINT`    | `str`  | `""`                       |
| `storage_s3_access_key`    | `TSIGMA_STORAGE_S3_ACCESS_KEY`  | `str`  | `""`                       |
| `storage_s3_secret_key`    | `TSIGMA_STORAGE_S3_SECRET_KEY`  | `str`  | `""`                       |

Related cold-tier settings (not part of the storage backend, but relevant to data lifecycle):

| Setting                | Env Var                          | Type   | Default                     |
|------------------------|----------------------------------|--------|-----------------------------|
| `storage_cold_enabled` | `TSIGMA_STORAGE_COLD_ENABLED`    | `bool` | `False`                     |
| `storage_cold_after`   | `TSIGMA_STORAGE_COLD_AFTER`      | `str`  | `"6 months"`                |
| `storage_cold_path`    | `TSIGMA_STORAGE_COLD_PATH`       | `str`  | `"/var/lib/tsigma/cold"`    |

## Adding a New Storage Backend

1. **Create the module** -- add `tsigma/storage/yourbackend.py`.

2. **Subclass `StorageBackend`** -- implement all six abstract methods (`put`, `get`, `delete`, `exists`, `list_files`, `get_url`). All methods are async.

   ```python
   from tsigma.storage.base import StorageBackend, StoredFile

   class YourBackend(StorageBackend):
       def __init__(self, ...):
           ...

       async def put(self, key, data, metadata=None) -> StoredFile:
           ...

       async def get(self, key) -> bytes:
           ...

       async def delete(self, key) -> None:
           ...

       async def exists(self, key) -> bool:
           ...

       async def list_files(self, prefix):
           ...

       async def get_url(self, key, expires_in=3600) -> str:
           ...
   ```

3. **Add configuration** -- add any required settings to `tsigma.config.Settings` following the `storage_yourbackend_*` naming convention so they map to `TSIGMA_STORAGE_YOURBACKEND_*` env vars.

4. **Register in the factory** -- edit `tsigma/storage/factory.py` and add a branch for your backend name. Use a deferred import to keep the dependency optional:

   ```python
   if backend_type == "yourbackend":
       from .yourbackend import YourBackend
       return YourBackend(
           setting_a=settings.storage_yourbackend_setting_a,
           ...
       )
   ```

5. **Export (optional)** -- if callers should be able to import the class directly, add it to `tsigma/storage/__init__.py`'s `__all__`.

6. **Error semantics** -- follow the conventions established by the existing backends:
   - `get()` raises `FileNotFoundError` when the key does not exist.
   - `delete()` is a no-op when the key does not exist.
   - `exists()` returns `bool`, never raises for missing keys.
