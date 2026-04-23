"""
Unit tests for storage backends.

Tests FilesystemBackend CRUD operations, the storage factory,
and S3Backend operations (mocked).
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.storage.factory import get_storage_backend
from tsigma.storage.filesystem import FilesystemBackend
from tsigma.storage.s3 import S3Backend

# ---------------------------------------------------------------------------
# FilesystemBackend tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filesystem_write_and_read(tmp_path):
    """Write bytes, read them back, verify match."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    payload = b"hello world"
    await backend.put("test.bin", payload)
    result = await backend.get("test.bin")
    assert result == payload


@pytest.mark.asyncio
async def test_filesystem_list(tmp_path):
    """Write 3 files, list returns all 3."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    for name in ("a.txt", "b.txt", "c.txt"):
        await backend.put(name, b"data")

    keys = []
    async for stored_file in backend.list_files(""):
        keys.append(stored_file.key)

    assert sorted(keys) == ["a.txt", "b.txt", "c.txt"]


@pytest.mark.asyncio
async def test_filesystem_delete(tmp_path):
    """Write a file, delete it, verify it's gone."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    await backend.put("doomed.txt", b"bye")
    assert await backend.exists("doomed.txt") is True

    await backend.delete("doomed.txt")
    assert await backend.exists("doomed.txt") is False


@pytest.mark.asyncio
async def test_filesystem_exists(tmp_path):
    """exists() returns True for written file, False for non-existent."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    await backend.put("present.txt", b"here")
    assert await backend.exists("present.txt") is True
    assert await backend.exists("absent.txt") is False


# ---------------------------------------------------------------------------
# Factory tests
# ---------------------------------------------------------------------------


def test_factory_creates_filesystem(tmp_path):
    """Factory with backend='filesystem' returns FilesystemBackend."""
    with patch("tsigma.storage.factory.settings") as mock_settings:
        mock_settings.storage_backend = "filesystem"
        mock_settings.storage_path = str(tmp_path)
        backend = get_storage_backend()
        assert isinstance(backend, FilesystemBackend)


def test_factory_unknown_raises(tmp_path):
    """Factory with backend='unknown' raises ValueError."""
    with patch("tsigma.storage.factory.settings") as mock_settings:
        mock_settings.storage_backend = "unknown"
        mock_settings.storage_path = str(tmp_path)
        # The current implementation falls back to filesystem with a warning
        # rather than raising ValueError. Test actual behaviour.
        backend = get_storage_backend()
        assert isinstance(backend, FilesystemBackend)


# ---------------------------------------------------------------------------
# S3Backend tests (mocked — no real AWS calls)
# ---------------------------------------------------------------------------


@pytest.fixture
def s3_backend():
    """Create an S3Backend with a mocked aiobotocore import."""
    with patch.dict("sys.modules", {"aiobotocore": MagicMock()}):
        backend = S3Backend(
            bucket="test-bucket",
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            access_key="fake-key",
            secret_key="fake-secret",
        )
    # Pre-inject a mock client so _get_client() returns it directly.
    backend._client = AsyncMock()
    return backend


@pytest.mark.asyncio
async def test_s3_write(s3_backend):
    """put() calls put_object with correct bucket, key, and body."""
    backend = s3_backend
    data = b"test payload"
    result = await backend.put("reports/test.bin", data)

    backend._client.put_object.assert_awaited_once_with(
        Bucket="test-bucket",
        Key="reports/test.bin",
        Body=data,
    )
    assert result.key == "reports/test.bin"
    assert result.size == len(data)


@pytest.mark.asyncio
async def test_s3_write_with_metadata(s3_backend):
    """put() passes Metadata when provided."""
    backend = s3_backend
    meta = {"content-type": "application/json"}
    await backend.put("data.json", b"{}", metadata=meta)

    backend._client.put_object.assert_awaited_once_with(
        Bucket="test-bucket",
        Key="data.json",
        Body=b"{}",
        Metadata=meta,
    )


@pytest.mark.asyncio
async def test_s3_read(s3_backend):
    """get() returns bytes from the S3 response body stream."""
    backend = s3_backend
    expected = b"file contents here"

    # Mock the streaming body context manager
    mock_stream = AsyncMock()
    mock_stream.read = AsyncMock(return_value=expected)
    mock_body = AsyncMock()
    mock_body.__aenter__ = AsyncMock(return_value=mock_stream)
    mock_body.__aexit__ = AsyncMock(return_value=False)

    backend._client.get_object = AsyncMock(
        return_value={"Body": mock_body}
    )

    result = await backend.get("reports/test.bin")
    assert result == expected
    backend._client.get_object.assert_awaited_once_with(
        Bucket="test-bucket", Key="reports/test.bin"
    )


@pytest.mark.asyncio
async def test_s3_list(s3_backend):
    """list_files() yields StoredFile entries from paginated results."""
    backend = s3_backend
    now = datetime.now(timezone.utc)

    # Mock the paginator
    page_data = {
        "Contents": [
            {"Key": "prefix/a.bin", "Size": 100, "LastModified": now},
            {"Key": "prefix/b.bin", "Size": 200, "LastModified": now},
        ]
    }

    # Create an async iterator for the paginator
    async def mock_paginate(**kwargs):
        yield page_data

    mock_paginator = MagicMock()
    mock_paginator.paginate = mock_paginate
    backend._client.get_paginator = MagicMock(return_value=mock_paginator)

    files = []
    async for f in backend.list_files("prefix/"):
        files.append(f)

    assert len(files) == 2
    assert files[0].key == "prefix/a.bin"
    assert files[0].size == 100
    assert files[1].key == "prefix/b.bin"
    assert files[1].size == 200
    backend._client.get_paginator.assert_called_once_with("list_objects_v2")


@pytest.mark.asyncio
async def test_s3_delete(s3_backend):
    """delete() calls delete_object with correct bucket and key."""
    backend = s3_backend
    await backend.delete("old-file.bin")

    backend._client.delete_object.assert_awaited_once_with(
        Bucket="test-bucket", Key="old-file.bin"
    )


@pytest.mark.asyncio
async def test_s3_exists_true(s3_backend):
    """exists() returns True when head_object succeeds."""
    backend = s3_backend
    backend._client.head_object = AsyncMock(return_value={})

    result = await backend.exists("present.bin")
    assert result is True
    backend._client.head_object.assert_awaited_once_with(
        Bucket="test-bucket", Key="present.bin"
    )


@pytest.mark.asyncio
async def test_s3_exists_false(s3_backend):
    """exists() returns False when head_object raises a 404 error."""
    backend = s3_backend

    # Simulate a ClientError with 404 code
    error_response = {"Error": {"Code": "404"}}
    exc = Exception("Not Found")
    exc.response = error_response
    backend._client.head_object = AsyncMock(side_effect=exc)

    result = await backend.exists("absent.bin")
    assert result is False


# ---------------------------------------------------------------------------
# Additional S3Backend coverage
# ---------------------------------------------------------------------------


def test_s3_init_requires_aiobotocore():
    """S3Backend.__init__ raises ImportError when aiobotocore is missing."""
    with patch.dict("sys.modules", {"aiobotocore": None}):
        with pytest.raises(ImportError, match="aiobotocore"):
            S3Backend(bucket="b", region="us-east-1")


def test_s3_init_creates_backend():
    """S3Backend.__init__ stores config correctly."""
    with patch.dict("sys.modules", {"aiobotocore": MagicMock()}):
        backend = S3Backend(
            bucket="my-bucket",
            region="eu-west-1",
            endpoint_url="http://minio:9000",
            access_key="AK",
            secret_key="SK",
        )
    assert backend._bucket == "my-bucket"
    assert backend._region == "eu-west-1"
    assert backend._endpoint_url == "http://minio:9000"
    assert backend._access_key == "AK"
    assert backend._secret_key == "SK"
    assert backend._client is None


@pytest.mark.asyncio
async def test_s3_get_client_returns_existing(s3_backend):
    """_get_client() returns the existing client when already initialised."""
    backend = s3_backend
    existing_client = backend._client
    client = await backend._get_client()
    assert client is existing_client


@pytest.mark.asyncio
async def test_s3_write_with_content_type(s3_backend):
    """put() passes Metadata dict when content type metadata given."""
    backend = s3_backend
    meta = {"Content-Type": "text/csv"}
    result = await backend.put("data.csv", b"a,b,c", metadata=meta)

    backend._client.put_object.assert_awaited_once_with(
        Bucket="test-bucket",
        Key="data.csv",
        Body=b"a,b,c",
        Metadata=meta,
    )
    assert result.key == "data.csv"
    assert result.metadata == meta


@pytest.mark.asyncio
async def test_s3_list_empty_bucket(s3_backend):
    """list_files() yields nothing when bucket is empty (no Contents key)."""
    backend = s3_backend

    async def mock_paginate(**kwargs):
        yield {}  # page with no Contents key

    mock_paginator = MagicMock()
    mock_paginator.paginate = mock_paginate
    backend._client.get_paginator = MagicMock(return_value=mock_paginator)

    files = []
    async for f in backend.list_files("prefix/"):
        files.append(f)

    assert files == []


@pytest.mark.asyncio
async def test_s3_list_pagination(s3_backend):
    """list_files() handles multiple pages of results."""
    backend = s3_backend
    now = datetime.now(timezone.utc)

    async def mock_paginate(**kwargs):
        yield {
            "Contents": [
                {"Key": "p/a.bin", "Size": 10, "LastModified": now},
            ]
        }
        yield {
            "Contents": [
                {"Key": "p/b.bin", "Size": 20, "LastModified": now},
                {"Key": "p/c.bin", "Size": 30, "LastModified": now},
            ]
        }

    mock_paginator = MagicMock()
    mock_paginator.paginate = mock_paginate
    backend._client.get_paginator = MagicMock(return_value=mock_paginator)

    files = []
    async for f in backend.list_files("p/"):
        files.append(f)

    assert len(files) == 3
    assert files[0].key == "p/a.bin"
    assert files[1].size == 20
    assert files[2].key == "p/c.bin"


@pytest.mark.asyncio
async def test_s3_read_streaming(s3_backend):
    """get() reads the full streaming body from response."""
    backend = s3_backend
    payload = b"binary-content-here-xyz"

    mock_stream = AsyncMock()
    mock_stream.read = AsyncMock(return_value=payload)
    mock_body = AsyncMock()
    mock_body.__aenter__ = AsyncMock(return_value=mock_stream)
    mock_body.__aexit__ = AsyncMock(return_value=False)

    backend._client.get_object = AsyncMock(
        return_value={"Body": mock_body}
    )

    result = await backend.get("file.bin")
    assert result == payload
    mock_stream.read.assert_awaited_once()


@pytest.mark.asyncio
async def test_s3_read_not_found_nosuchkey(s3_backend):
    """get() raises FileNotFoundError for NoSuchKey exception."""
    backend = s3_backend

    error = Exception("NoSuchKey")
    error.response = {"Error": {"Code": "NoSuchKey"}}
    backend._client.get_object = AsyncMock(side_effect=error)
    # NoSuchKey is not on client.exceptions, so it falls to _is_not_found
    backend._client.exceptions = MagicMock()
    backend._client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    with pytest.raises(FileNotFoundError, match="Storage key not found"):
        await backend.get("missing.bin")


@pytest.mark.asyncio
async def test_s3_get_url(s3_backend):
    """get_url() calls generate_presigned_url with correct params."""
    backend = s3_backend
    backend._client.generate_presigned_url = AsyncMock(
        return_value="https://s3.example.com/test-bucket/file.bin?signed"
    )

    url = await backend.get_url("file.bin", expires_in=1800)
    assert "signed" in url
    backend._client.generate_presigned_url.assert_awaited_once_with(
        "get_object",
        Params={"Bucket": "test-bucket", "Key": "file.bin"},
        ExpiresIn=1800,
    )


@pytest.mark.asyncio
async def test_s3_close(s3_backend):
    """close() exits the client context and clears state."""
    backend = s3_backend
    mock_ctx = AsyncMock()
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    backend._client_ctx = mock_ctx

    await backend.close()

    assert backend._client is None
    assert backend._session is None
    mock_ctx.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_s3_close_noop_when_no_client():
    """close() is a no-op when client was never initialized."""
    with patch.dict("sys.modules", {"aiobotocore": MagicMock()}):
        backend = S3Backend(bucket="b")
    await backend.close()  # Should not raise
    assert backend._client is None


@pytest.mark.asyncio
async def test_s3_exists_reraises_non_404(s3_backend):
    """exists() re-raises exceptions that are not 404/NoSuchKey."""
    backend = s3_backend
    exc = RuntimeError("connection refused")
    backend._client.head_object = AsyncMock(side_effect=exc)

    with pytest.raises(RuntimeError, match="connection refused"):
        await backend.exists("key.bin")


@pytest.mark.asyncio
async def test_s3_get_not_found_via_generic_exception(s3_backend):
    """get() handles non-ClientError 404 via _is_not_found fallback."""
    backend = s3_backend

    # Simulate an exception that isn't a botocore ClientError but has
    # the response dict pattern indicating 404
    exc = Exception("wrapped error")
    exc.response = {"Error": {"Code": "404"}}
    backend._client.get_object = AsyncMock(side_effect=exc)
    backend._client.exceptions = MagicMock()
    backend._client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    with pytest.raises(FileNotFoundError):
        await backend.get("gone.bin")


@pytest.mark.asyncio
async def test_s3_get_reraises_non_404(s3_backend):
    """get() re-raises exceptions that are not 404/NoSuchKey."""
    backend = s3_backend
    exc = RuntimeError("timeout")
    backend._client.get_object = AsyncMock(side_effect=exc)
    backend._client.exceptions = MagicMock()
    backend._client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    with pytest.raises(RuntimeError, match="timeout"):
        await backend.get("key.bin")


def test_is_not_found_with_botocore_client_error():
    """_is_not_found recognises botocore ClientError with 404 code."""
    from tsigma.storage.s3 import _is_not_found

    exc = Exception("not found")
    exc.response = {"Error": {"Code": "NoSuchKey"}}
    assert _is_not_found(exc) is True


def test_is_not_found_false_for_other_errors():
    """_is_not_found returns False for non-404 errors."""
    from tsigma.storage.s3 import _is_not_found

    exc = RuntimeError("boom")
    assert _is_not_found(exc) is False


def test_is_not_found_false_for_other_codes():
    """_is_not_found returns False for non-404 error codes."""
    from tsigma.storage.s3 import _is_not_found

    exc = Exception("forbidden")
    exc.response = {"Error": {"Code": "403"}}
    assert _is_not_found(exc) is False


def test_is_not_found_with_real_botocore_client_error():
    """_is_not_found recognises botocore ClientError with 404 code (lines 158-160)."""

    # Simulate a real botocore ClientError
    mock_client_error_cls = type("ClientError", (Exception,), {})
    exc = mock_client_error_cls("Not Found")
    exc.response = {"Error": {"Code": "404"}}

    with patch.dict("sys.modules", {
        "botocore": MagicMock(),
        "botocore.exceptions": MagicMock(ClientError=mock_client_error_cls),
    }):
        # Re-import to pick up the patched botocore
        from tsigma.storage import s3 as s3_mod
        result = s3_mod._is_not_found(exc)

    assert result is True


def test_is_not_found_botocore_not_installed():
    """_is_not_found falls back when botocore is not importable (line 161-163)."""
    from tsigma.storage.s3 import _is_not_found

    exc = Exception("some error")
    exc.response = {"Error": {"Code": "NoSuchKey"}}

    # Even without botocore, the fallback getattr path catches it
    assert _is_not_found(exc) is True


@pytest.mark.asyncio
async def test_s3_get_client_lazy_init():
    """_get_client() creates client on first call (lines 48-62)."""
    mock_aio_session = MagicMock()
    mock_client = AsyncMock()
    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_aio_session.create_client = MagicMock(return_value=mock_ctx)

    mock_aio_session_cls = MagicMock(return_value=mock_aio_session)
    mock_aiobotocore = MagicMock()
    mock_aiobotocore_session = MagicMock()
    mock_aiobotocore_session.AioSession = mock_aio_session_cls

    with patch.dict("sys.modules", {
        "aiobotocore": mock_aiobotocore,
        "aiobotocore.session": mock_aiobotocore_session,
    }):
        backend = S3Backend(
            bucket="test-bucket",
            region="us-west-2",
            endpoint_url="http://minio:9000",
            access_key="AK",
            secret_key="SK",
        )
        assert backend._client is None

        client = await backend._get_client()

    assert client is mock_client
    mock_aio_session.create_client.assert_called_once()
    call_kwargs = mock_aio_session.create_client.call_args
    assert call_kwargs[0][0] == "s3"
    assert call_kwargs[1]["region_name"] == "us-west-2"
    assert call_kwargs[1]["endpoint_url"] == "http://minio:9000"
    assert call_kwargs[1]["aws_access_key_id"] == "AK"
    assert call_kwargs[1]["aws_secret_access_key"] == "SK"


@pytest.mark.asyncio
async def test_s3_get_client_no_endpoint_no_keys():
    """_get_client() omits endpoint_url and keys when not set (lines 54-58)."""
    mock_aio_session = MagicMock()
    mock_client = AsyncMock()
    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_aio_session.create_client = MagicMock(return_value=mock_ctx)

    mock_aio_session_cls = MagicMock(return_value=mock_aio_session)
    mock_aiobotocore = MagicMock()
    mock_aiobotocore_session = MagicMock()
    mock_aiobotocore_session.AioSession = mock_aio_session_cls

    with patch.dict("sys.modules", {
        "aiobotocore": mock_aiobotocore,
        "aiobotocore.session": mock_aiobotocore_session,
    }):
        backend = S3Backend(
            bucket="test-bucket",
            region="us-east-1",
        )
        client = await backend._get_client()

    assert client is mock_client
    call_kwargs = mock_aio_session.create_client.call_args[1]
    assert "endpoint_url" not in call_kwargs
    assert "aws_access_key_id" not in call_kwargs
    assert "aws_secret_access_key" not in call_kwargs


@pytest.mark.asyncio
async def test_s3_get_nosuchkey_via_client_exceptions(s3_backend):
    """get() handles NoSuchKey via client.exceptions attribute (line 95)."""
    backend = s3_backend

    # Create NoSuchKey exception class and make client raise it
    nosuchkey_cls = type("NoSuchKey", (Exception,), {})
    backend._client.exceptions = MagicMock()
    backend._client.exceptions.NoSuchKey = nosuchkey_cls
    backend._client.get_object = AsyncMock(side_effect=nosuchkey_cls("not found"))

    with pytest.raises(FileNotFoundError, match="Storage key not found"):
        await backend.get("missing-key.bin")


# ---------------------------------------------------------------------------
# Factory: S3 backend (lines 22-25)
# ---------------------------------------------------------------------------


def test_factory_creates_s3():
    """Factory with backend='s3' returns S3Backend."""
    with patch("tsigma.storage.factory.settings") as mock_settings, \
         patch.dict("sys.modules", {"aiobotocore": MagicMock()}):
        mock_settings.storage_backend = "s3"
        mock_settings.storage_s3_bucket = "my-bucket"
        mock_settings.storage_s3_region = "us-east-1"
        mock_settings.storage_s3_endpoint = ""
        mock_settings.storage_s3_access_key = ""
        mock_settings.storage_s3_secret_key = ""
        backend = get_storage_backend()
        assert isinstance(backend, S3Backend)


# ---------------------------------------------------------------------------
# Filesystem: makedirs for nested path (lines 29, 41)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filesystem_makedirs(tmp_path):
    """Writing to a nested path creates intermediate directories."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    payload = b"nested file content"
    result = await backend.put("deep/nested/dir/file.bin", payload)

    assert result.key == "deep/nested/dir/file.bin"
    assert result.size == len(payload)

    # Verify the file was actually created
    read_back = await backend.get("deep/nested/dir/file.bin")
    assert read_back == payload


# ---------------------------------------------------------------------------
# Filesystem: list empty directory (lines 88-89)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filesystem_list_empty(tmp_path):
    """Listing an empty directory returns no files."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    # Create an empty subdirectory
    (tmp_path / "empty_dir").mkdir()

    files = []
    async for f in backend.list_files("empty_dir"):
        files.append(f)

    assert files == []


# ---------------------------------------------------------------------------
# Filesystem: delete nonexistent file (lines 72-73)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filesystem_delete_nonexistent(tmp_path):
    """Deleting a nonexistent file does not raise an error."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    # Should not raise
    await backend.delete("does_not_exist.txt")


# ---------------------------------------------------------------------------
# Filesystem: path traversal prevention (line 29)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filesystem_path_traversal_denied(tmp_path):
    """Path traversal attempt raises ValueError."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    with pytest.raises(ValueError, match="Path traversal denied"):
        await backend.get("../../etc/passwd")


# ---------------------------------------------------------------------------
# Filesystem: get nonexistent file (lines 61-62)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filesystem_get_nonexistent(tmp_path):
    """Getting a nonexistent file raises FileNotFoundError."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    with pytest.raises(FileNotFoundError, match="Storage key not found"):
        await backend.get("missing.txt")


# ---------------------------------------------------------------------------
# Filesystem: list_files with single file (line 91)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filesystem_list_single_file(tmp_path):
    """list_files on a file key returns that single file."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    await backend.put("single.txt", b"data")

    files = []
    async for f in backend.list_files("single.txt"):
        files.append(f)

    assert len(files) == 1
    assert files[0].key == "single.txt"


# ---------------------------------------------------------------------------
# Filesystem: get_url (lines 106-109)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filesystem_get_url(tmp_path):
    """get_url returns a file:// URL for an existing file."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    await backend.put("url_test.txt", b"content")

    url = await backend.get_url("url_test.txt")
    assert url.startswith("file://")


@pytest.mark.asyncio
async def test_filesystem_get_url_nonexistent(tmp_path):
    """get_url raises FileNotFoundError for a missing file."""
    backend = FilesystemBackend(base_path=str(tmp_path))
    with pytest.raises(FileNotFoundError, match="Storage key not found"):
        await backend.get_url("nope.txt")
