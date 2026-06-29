"""
Unit tests for recursive-listing config wiring in the FTP pull method.

Phase B: the per-signal recursion options (recursive / max_depth /
follow_symlinks) live on FTPPullConfig, flow through _build_config from the
signal_metadata collection dict, and are passed through to client.list_dir by
the passive and rotate poll callers. The recursive list_dir(...) primitive
itself already exists (Phase A); these tests only exercise the config + caller
passthrough.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests._helpers import make_mock_session
from tsigma.collection.methods.ftp_pull import (
    FTPPullConfig,
    FTPPullMethod,
)

# ---------------------------------------------------------------------------
# Helpers (mirror tests/unit/test_ftp_pull.py)
# ---------------------------------------------------------------------------


def _make_config_dict(**overrides) -> dict:
    """Create a raw collection config dict with sensible defaults."""
    defaults = {
        "host": "192.168.1.100",
        "protocol": "ftp",
        "username": "anonymous",
        "password": "",
        "remote_dir": "/",
        "file_extensions": [".dat", ".csv", ".log"],
    }
    defaults.update(overrides)
    return defaults


def _mock_client(files=None):
    """Create a mock _FileTransferClient."""
    client = AsyncMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.list_dir = AsyncMock(return_value=files or [])
    client.download = AsyncMock(return_value=b"")
    return client


def _mock_session_factory():
    """Create a mock async session factory with proper checkpoint support."""
    mock_session = make_mock_session()
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    mock_session.execute = AsyncMock(return_value=result_mock)
    mock_session.flush = AsyncMock()
    mock_session_ctx = MagicMock()
    mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock(return_value=mock_session_ctx)
    return factory, mock_session


def _make_mock_target() -> MagicMock:
    """Build a mock ``IngestionTarget`` with async SDK methods."""
    target = MagicMock()
    target.device_type = "controller"
    target.load_checkpoint = AsyncMock(return_value=None)
    target.save_checkpoint = AsyncMock()
    target.record_error = AsyncMock()
    target.persist = AsyncMock()
    target.persist_with_drift_check = AsyncMock()
    target.resolve_decoder = MagicMock()
    return target


# ---------------------------------------------------------------------------
# FTPPullConfig — recursion fields
# ---------------------------------------------------------------------------


class TestRecursiveConfigFields:
    """Tests for the recursion fields on FTPPullConfig."""

    def test_recursive_defaults_to_false(self):
        """A default-constructed config does not recurse."""
        config = FTPPullConfig(host="192.168.1.100", signal_id="SIG-001")
        assert config.recursive is False

    def test_max_depth_defaults_to_five(self):
        """The default recursion depth ceiling is 5."""
        config = FTPPullConfig(host="192.168.1.100", signal_id="SIG-001")
        assert config.max_depth == 5

    def test_follow_symlinks_defaults_to_false(self):
        """Symlinked directories are not followed by default."""
        config = FTPPullConfig(host="192.168.1.100", signal_id="SIG-001")
        assert config.follow_symlinks is False

    def test_recursion_fields_settable(self):
        """The recursion fields accept non-default values."""
        config = FTPPullConfig(
            host="192.168.1.100",
            signal_id="SIG-001",
            recursive=True,
            max_depth=3,
            follow_symlinks=True,
        )
        assert config.recursive is True
        assert config.max_depth == 3
        assert config.follow_symlinks is True


# ---------------------------------------------------------------------------
# _build_config — recursion mapping
# ---------------------------------------------------------------------------


class TestBuildConfigRecursion:
    """Tests for recursion options flowing through _build_config()."""

    def test_recursion_fields_from_dict(self):
        """recursive / max_depth / follow_symlinks flow from the raw dict."""
        raw = {
            "host": "ftp.agency.gov",
            "recursive": True,
            "max_depth": 7,
            "follow_symlinks": True,
        }
        config = FTPPullMethod._build_config("SIG-042", raw)
        assert config.recursive is True
        assert config.max_depth == 7
        assert config.follow_symlinks is True

    def test_recursion_defaults_when_absent(self):
        """Missing recursion keys fall back to the config defaults."""
        raw = {"host": "192.168.1.1"}
        config = FTPPullMethod._build_config("SIG-001", raw)
        assert config.recursive is False
        assert config.max_depth == 5
        assert config.follow_symlinks is False


# ---------------------------------------------------------------------------
# Caller passthrough — poll callers forward recursion options to list_dir
# ---------------------------------------------------------------------------


class TestListDirPassthrough:
    """Tests that poll callers pass recursion options through to list_dir."""

    @pytest.mark.asyncio
    async def test_passive_poll_passes_recursion_kwargs(self):
        """_poll_passive forwards recursive/max_depth/follow_symlinks."""
        method = FTPPullMethod()
        config = _make_config_dict(
            mode="passive",
            recursive=True,
            max_depth=3,
            follow_symlinks=True,
        )
        client = _mock_client(files=[])
        factory, _ = _mock_session_factory()
        target = _make_mock_target()
        with patch.object(method, "_create_client", return_value=client):
            await method.poll_once("SIG-001", config, factory, target=target)
        client.list_dir.assert_awaited_with(
            "/", recursive=True, max_depth=3, follow_symlinks=True,
        )

    @pytest.mark.asyncio
    async def test_rotate_poll_passes_recursion_kwargs(self):
        """_poll_rotate forwards recursive/max_depth/follow_symlinks."""
        method = FTPPullMethod()
        config = _make_config_dict(
            mode="rotate",
            recursive=True,
            max_depth=3,
            follow_symlinks=True,
        )
        client = _mock_client(files=[])
        factory, _ = _mock_session_factory()
        target = _make_mock_target()
        with patch.object(method, "_create_client", return_value=client):
            await method.poll_once("SIG-001", config, factory, target=target)
        client.list_dir.assert_awaited_with(
            "/", recursive=True, max_depth=3, follow_symlinks=True,
        )
