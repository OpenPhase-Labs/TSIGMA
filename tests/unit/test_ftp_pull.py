"""
Unit tests for FTP/FTPS/SFTP pull ingestion method plugin.

Tests configuration, protocol adapters, poll cycle logic,
and config construction from signal_metadata JSONB dicts.
"""

from datetime import datetime, timezone
from pathlib import PurePosixPath
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.collection.methods.ftp_pull import (
    FTPProtocol,
    FTPPullConfig,
    FTPPullMethod,
    RemoteFile,
    _compute_files_hash,
)
from tsigma.collection.registry import ExecutionMode, IngestionMethodRegistry

# Module path prefix for patching SDK imports used in ftp_pull.py
_MOD = "tsigma.collection.methods.ftp_pull"


class TestFTPProtocol:
    """Tests for FTPProtocol enum."""

    def test_ftp_value(self):
        """Test FTP protocol string value."""
        assert FTPProtocol.FTP == "ftp"

    def test_ftps_value(self):
        """Test FTPS protocol string value."""
        assert FTPProtocol.FTPS == "ftps"

    def test_sftp_value(self):
        """Test SFTP protocol string value."""
        assert FTPProtocol.SFTP == "sftp"

    def test_from_string(self):
        """Test enum creation from string."""
        assert FTPProtocol("ftp") is FTPProtocol.FTP
        assert FTPProtocol("ftps") is FTPProtocol.FTPS
        assert FTPProtocol("sftp") is FTPProtocol.SFTP


class TestFTPPullConfig:
    """Tests for FTPPullConfig dataclass."""

    def test_required_fields_only(self):
        """Test config with just host and signal_id."""
        config = FTPPullConfig(host="192.168.1.100", signal_id="SIG-001")
        assert config.host == "192.168.1.100"
        assert config.signal_id == "SIG-001"
        assert config.protocol == FTPProtocol.FTP
        assert config.username == "anonymous"
        assert config.password == ""
        assert config.remote_dir == "/"
        assert config.decoder is None
        assert config.ssh_key_path is None
        assert config.passive_mode is True

    def test_all_fields(self):
        """Test config with all fields populated."""
        config = FTPPullConfig(
            host="ftp.agency.gov",
            signal_id="SIG-042",
            protocol=FTPProtocol.SFTP,
            port=2222,
            username="collector",
            password="s3cret",
            remote_dir="/data/events",
            file_extensions=[".dat", ".bin"],
            decoder="asc3",
            ssh_key_path="/home/user/.ssh/id_rsa",
            passive_mode=False,
        )
        assert config.host == "ftp.agency.gov"
        assert config.protocol == FTPProtocol.SFTP
        assert config.port == 2222
        assert config.decoder == "asc3"
        assert config.ssh_key_path == "/home/user/.ssh/id_rsa"

    def test_default_port_ftp(self):
        """Test default port for FTP is 21."""
        config = FTPPullConfig(host="h", signal_id="s", protocol=FTPProtocol.FTP)
        assert config.default_port == 21

    def test_default_port_ftps(self):
        """Test default port for FTPS is 990."""
        config = FTPPullConfig(host="h", signal_id="s", protocol=FTPProtocol.FTPS)
        assert config.default_port == 990

    def test_default_port_sftp(self):
        """Test default port for SFTP is 22."""
        config = FTPPullConfig(host="h", signal_id="s", protocol=FTPProtocol.SFTP)
        assert config.default_port == 22

    def test_explicit_port_overrides_default(self):
        """Test explicit port takes precedence over protocol default."""
        config = FTPPullConfig(host="h", signal_id="s", port=2121)
        assert config.effective_port == 2121

    def test_effective_port_uses_default_when_none(self):
        """Test effective_port falls back to default_port when port is None."""
        config = FTPPullConfig(host="h", signal_id="s")
        assert config.effective_port == 21

    def test_default_file_extensions(self):
        """Test default file extensions include common formats."""
        config = FTPPullConfig(host="h", signal_id="s")
        assert ".dat" in config.file_extensions
        assert ".csv" in config.file_extensions
        assert ".log" in config.file_extensions


class TestRemoteFile:
    """Tests for RemoteFile dataclass."""

    def test_create_with_all_fields(self):
        """Test RemoteFile creation with all fields."""
        now = datetime.now(timezone.utc)
        rf = RemoteFile(name="events.dat", size=1024, mtime=now)
        assert rf.name == "events.dat"
        assert rf.size == 1024
        assert rf.mtime == now

    def test_create_with_none_mtime(self):
        """Test RemoteFile with unknown modification time."""
        rf = RemoteFile(name="data.csv", size=512, mtime=None)
        assert rf.mtime is None


# ---------------------------------------------------------------------------
# Helpers
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


def _mock_decoder(events=None):
    """Create a mock decoder instance."""
    decoder = MagicMock()
    decoder.decode_bytes.return_value = events or []
    return decoder


def _mock_session_factory():
    """Create a mock async session factory with proper checkpoint support."""
    mock_session = AsyncMock()
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    mock_session.execute = AsyncMock(return_value=result_mock)
    mock_session.add = MagicMock()
    mock_session.add_all = MagicMock()
    mock_session.expunge = MagicMock()
    mock_session.flush = AsyncMock()
    mock_session_ctx = MagicMock()
    mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock(return_value=mock_session_ctx)
    return factory, mock_session


# ---------------------------------------------------------------------------
# FTPPullMethod — registration and construction
# ---------------------------------------------------------------------------


class TestFTPPullMethodRegistration:
    """Tests for FTPPullMethod plugin registration."""

    def test_registered_in_registry(self):
        """Test FTPPullMethod is registered as 'ftp_pull'."""
        assert "ftp_pull" in IngestionMethodRegistry.list_available()
        cls = IngestionMethodRegistry.get("ftp_pull")
        assert cls is FTPPullMethod

    def test_execution_mode_is_polling(self):
        """Test FTPPullMethod declares polling execution mode."""
        assert FTPPullMethod.execution_mode == ExecutionMode.POLLING

    def test_constructor_no_args(self):
        """Test constructor takes no arguments."""
        method = FTPPullMethod()
        assert isinstance(method, FTPPullMethod)


class TestBuildConfig:
    """Tests for FTPPullMethod._build_config()."""

    def test_full_config(self):
        """Test building FTPPullConfig from a complete dict."""
        raw = {
            "host": "ftp.agency.gov",
            "protocol": "ftps",
            "port": 990,
            "username": "collector",
            "password": "s3cret",
            "remote_dir": "/data/events",
            "decoder": "asc3",
            "file_extensions": [".dat", ".datz"],
            "ssh_key_path": "/keys/id_rsa",
            "passive_mode": False,
        }
        config = FTPPullMethod._build_config("SIG-042", raw)
        assert config.host == "ftp.agency.gov"
        assert config.signal_id == "SIG-042"
        assert config.protocol == FTPProtocol.FTPS
        assert config.port == 990
        assert config.username == "collector"
        assert config.decoder == "asc3"
        assert config.passive_mode is False

    def test_defaults_for_missing_fields(self):
        """Test missing optional fields get defaults."""
        raw = {"host": "192.168.1.1"}
        config = FTPPullMethod._build_config("SIG-001", raw)
        assert config.protocol == FTPProtocol.FTP
        assert config.username == "anonymous"
        assert config.password == ""
        assert config.remote_dir == "/"
        assert config.decoder is None
        assert config.passive_mode is True

    def test_host_from_dict(self):
        """Test host is extracted from config dict."""
        raw = {"host": "10.0.0.50"}
        config = FTPPullMethod._build_config("SIG-001", raw)
        assert config.host == "10.0.0.50"


# ---------------------------------------------------------------------------
# FTPPullMethod — health check
# ---------------------------------------------------------------------------


class TestFTPPullHealthCheck:
    """Tests for health_check()."""

    @pytest.mark.asyncio
    async def test_health_check_returns_true(self):
        """Test health_check returns True (polling methods always healthy)."""
        method = FTPPullMethod()
        result = await method.health_check()
        assert result is True


# ---------------------------------------------------------------------------
# FTPPullMethod — poll_once
# ---------------------------------------------------------------------------


class TestPollOnce:
    """Tests for poll_once() poll cycle logic."""

    @pytest.mark.asyncio
    async def test_lists_remote_files(self):
        """Test poll_once lists files from remote directory."""
        method = FTPPullMethod()
        config = _make_config_dict()
        client = _mock_client()
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None):
            await method.poll_once("SIG-001", config, factory)
        client.list_dir.assert_awaited_once_with("/")

    @pytest.mark.asyncio
    async def test_filters_by_extension(self):
        """Test poll_once only downloads files with matching extensions."""
        method = FTPPullMethod()
        config = _make_config_dict(file_extensions=[".dat"])
        files = [
            RemoteFile(name="events.dat", size=100, mtime=None),
            RemoteFile(name="readme.txt", size=50, mtime=None),
            RemoteFile(name="data.dat", size=200, mtime=None),
        ]
        client = _mock_client(files=files)
        decoder = _mock_decoder()
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock):
            await method.poll_once("SIG-001", config, factory)
        assert client.download.await_count == 2

    @pytest.mark.asyncio
    async def test_skips_unchanged_directory(self):
        """Test poll_once skips when directory hash is unchanged from checkpoint."""
        method = FTPPullMethod()
        config = _make_config_dict()
        from tsigma.models.checkpoint import PollingCheckpoint

        files = [RemoteFile(name="events.dat", size=100, mtime=None)]
        known_hash = _compute_files_hash(["events.dat"])

        checkpoint = MagicMock(spec=PollingCheckpoint)
        checkpoint.files_hash = known_hash
        checkpoint.last_file_mtime = None

        client = _mock_client(files=files)
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock,
                   return_value=checkpoint):
            await method.poll_once("SIG-001", config, factory)
        client.download.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_downloads_and_decodes(self):
        """Test poll_once downloads files and passes bytes to decoder."""
        method = FTPPullMethod()
        config = _make_config_dict()
        files = [RemoteFile(name="events.dat", size=100, mtime=None)]
        client = _mock_client(files=files)
        client.download.return_value = b"\x00\x01\x02"
        decoder = _mock_decoder()
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock):
            await method.poll_once("SIG-001", config, factory)
        decoder.decode_bytes.assert_called_once_with(b"\x00\x01\x02")

    @pytest.mark.asyncio
    async def test_persists_decoded_events(self):
        """Test poll_once persists decoded events to database."""
        method = FTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()
        now = datetime.now(timezone.utc)
        events = [
            DecodedEvent(timestamp=now, event_code=1, event_param=2),
            DecodedEvent(timestamp=now, event_code=3, event_param=4),
        ]
        files = [RemoteFile(name="events.dat", size=100, mtime=None)]
        client = _mock_client(files=files)
        decoder = _mock_decoder(events=events)
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock) as mock_persist:
            await method.poll_once("SIG-001", config, factory)

        mock_persist.assert_awaited_once()
        call_args = mock_persist.call_args
        persisted_events = call_args[0][0]
        persisted_signal = call_args[0][1]
        assert len(persisted_events) == 2
        assert persisted_signal == "SIG-001"
        assert persisted_events[0].event_code == 1
        assert persisted_events[1].event_code == 3

    @pytest.mark.asyncio
    async def test_saves_checkpoint_after_ingest(self):
        """Test poll_once saves checkpoint after successful file ingest."""
        method = FTPPullMethod()
        config = _make_config_dict()
        files = [RemoteFile(name="events.dat", size=100, mtime=None)]
        client = _mock_client(files=files)
        decoder = _mock_decoder()
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            await method.poll_once("SIG-001", config, factory)
        mock_save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handles_download_error(self):
        """Test poll_once continues when download fails for one file."""
        method = FTPPullMethod()
        config = _make_config_dict()
        files = [
            RemoteFile(name="bad.dat", size=100, mtime=None),
            RemoteFile(name="good.csv", size=200, mtime=None),
        ]
        client = _mock_client(files=files)
        client.download.side_effect = [OSError("download failed"), b"\x00"]
        decoder = _mock_decoder()
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            await method.poll_once("SIG-001", config, factory)
        # One file failed, one succeeded — checkpoint should still be saved
        mock_save.assert_awaited_once()
        assert mock_save.call_args[1]["new_files"] == 1

    @pytest.mark.asyncio
    async def test_handles_decode_error(self):
        """Test poll_once continues when decoder fails for one file."""
        method = FTPPullMethod()
        config = _make_config_dict()
        files = [RemoteFile(name="corrupt.dat", size=100, mtime=None)]
        client = _mock_client(files=files)
        client.download.return_value = b"\xff"
        decoder = _mock_decoder()
        decoder.decode_bytes.side_effect = ValueError("corrupt data")
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            await method.poll_once("SIG-001", config, factory)
        # Decode failed — no files ingested, checkpoint not saved
        mock_save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_file_list(self):
        """Test poll_once handles empty directory gracefully."""
        method = FTPPullMethod()
        config = _make_config_dict()
        client = _mock_client(files=[])
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None):
            await method.poll_once("SIG-001", config, factory)
        client.download.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_explicit_decoder(self):
        """Test poll_once uses explicitly configured decoder when set."""
        method = FTPPullMethod()
        config = _make_config_dict(decoder="asc3")
        decoder = _mock_decoder()
        files = [RemoteFile(name="events.dat", size=100, mtime=None)]
        client = _mock_client(files=files)
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder) as mock_resolve, \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock):
            await method.poll_once("SIG-001", config, factory)
        mock_resolve.assert_called_once_with(
            "events.dat", explicit_decoder="asc3",
        )

    @pytest.mark.asyncio
    async def test_auto_detect_decoder(self):
        """Test poll_once auto-detects decoder by file extension."""
        method = FTPPullMethod()
        config = _make_config_dict()  # no explicit decoder
        decoder = _mock_decoder()
        files = [RemoteFile(name="events.dat", size=100, mtime=None)]
        client = _mock_client(files=files)
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder) as mock_resolve, \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock):
            await method.poll_once("SIG-001", config, factory)
        mock_resolve.assert_called_once_with(
            "events.dat", explicit_decoder=None,
        )

    @pytest.mark.asyncio
    async def test_no_decoder_found_raises(self):
        """Test poll_once logs error when no decoder matches (per-file catch)."""
        method = FTPPullMethod()
        config = _make_config_dict()
        files = [RemoteFile(name="events.xyz", size=100, mtime=None)]
        client = _mock_client(files=files)
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.resolve_decoder_by_extension",
                   side_effect=ValueError("No decoder found for extension '.xyz'")), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            # _download_and_ingest catches per-file exceptions, so poll_once
            # does not raise — it just skips the file and saves no checkpoint
            await method.poll_once("SIG-001", config, factory)
        mock_save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connection_error_aborts_poll(self):
        """Test poll_once aborts gracefully on connection error."""
        method = FTPPullMethod()
        config = _make_config_dict()
        client = _mock_client()
        client.connect.side_effect = ConnectionError("refused")
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.record_error",
                   new_callable=AsyncMock) as mock_error:
            await method.poll_once("SIG-001", config, factory)
        client.list_dir.assert_not_awaited()
        mock_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_poll_once_dispatches_rotate_mode(self):
        """Test poll_once dispatches to _poll_rotate when mode is rotate."""
        method = FTPPullMethod()
        config = _make_config_dict(mode="rotate")
        client = _mock_client()
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch.object(method, "_poll_rotate",
                          new_callable=AsyncMock) as mock_rotate:
            await method.poll_once("SIG-001", config, factory)
        mock_rotate.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_poll_once_exception_records_error(self):
        """Test poll_once records error when poll cycle raises."""
        method = FTPPullMethod()
        config = _make_config_dict()
        client = _mock_client()
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock,
                   side_effect=RuntimeError("db down")), \
             patch(f"{_MOD}.record_error",
                   new_callable=AsyncMock) as mock_error:
            await method.poll_once("SIG-001", config, factory)
        mock_error.assert_awaited_once()
        client.disconnect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_poll_once_disconnects_on_success(self):
        """Test poll_once always disconnects the client after poll."""
        method = FTPPullMethod()
        config = _make_config_dict()
        client = _mock_client(files=[])
        factory, _ = _mock_session_factory()
        with patch.object(method, "_create_client", return_value=client), \
             patch(f"{_MOD}.load_checkpoint",
                   new_callable=AsyncMock, return_value=None):
            await method.poll_once("SIG-001", config, factory)
        client.disconnect.assert_awaited_once()


# ---------------------------------------------------------------------------
# _AioFTPClient tests
# ---------------------------------------------------------------------------


class TestAioFTPClient:
    """Tests for _AioFTPClient protocol adapter."""

    def _make_ftp_config(self, protocol=FTPProtocol.FTP, **kw):
        return FTPPullConfig(host="192.168.1.1", signal_id="SIG-001",
                             protocol=protocol, **kw)

    @pytest.mark.asyncio
    async def test_init_stores_config(self):
        """Test constructor stores config and initialises client to None."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config()
        client = _AioFTPClient(config)
        assert client._config is config
        assert client._client is None

    @pytest.mark.asyncio
    async def test_connect_ftp_plain_warns(self):
        """Test plain FTP connect logs a TLS warning."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config(protocol=FTPProtocol.FTP)
        client = _AioFTPClient(config)

        mock_ctx_mgr = MagicMock()
        mock_ctx_mgr.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_ctx_mgr.__aexit__ = AsyncMock(return_value=False)

        mock_aioftp = MagicMock()
        mock_aioftp.Client.context.return_value = mock_ctx_mgr

        with patch.dict("sys.modules", {"aioftp": mock_aioftp}), \
             patch(f"{_MOD}.logger") as mock_logger:
            await client.connect()
        mock_logger.warning.assert_called_once()
        assert "cleartext" in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_connect_ftps_uses_ssl(self):
        """Test FTPS connect creates SSL context and passes ssl kwarg."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config(protocol=FTPProtocol.FTPS)
        client = _AioFTPClient(config)

        mock_ctx_mgr = MagicMock()
        mock_ctx_mgr.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_ctx_mgr.__aexit__ = AsyncMock(return_value=False)

        mock_aioftp = MagicMock()
        mock_aioftp.Client.context.return_value = mock_ctx_mgr

        mock_ssl = MagicMock()
        mock_ssl_ctx = MagicMock()
        mock_ssl.create_default_context.return_value = mock_ssl_ctx

        with patch.dict("sys.modules", {"aioftp": mock_aioftp, "ssl": mock_ssl}):
            await client.connect()
        call_kwargs = mock_aioftp.Client.context.call_args
        assert call_kwargs[1].get("ssl") is mock_ssl_ctx or \
               (len(call_kwargs[0]) > 4 or "ssl" in call_kwargs[1])

    @pytest.mark.asyncio
    async def test_list_dir_filters_files(self):
        """Test list_dir returns only file-type entries."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config()
        client = _AioFTPClient(config)

        # Simulate async iteration over list results
        entries = [
            (PurePosixPath("/data/event.dat"), {"type": "file", "size": "100"}),
            (PurePosixPath("/data/subdir"), {"type": "dir"}),
            (PurePosixPath("/data/log.csv"), {"type": "file", "size": "50"}),
        ]

        async def mock_list(_path):
            for entry in entries:
                yield entry

        client._ctx = MagicMock()
        client._ctx.list = mock_list

        result = await client.list_dir("/data")
        assert len(result) == 2
        assert result[0].name == "event.dat"
        assert result[0].size == 100
        assert result[1].name == "log.csv"
        assert result[1].size == 50
        assert result[0].mtime is None

    @pytest.mark.asyncio
    async def test_download_reads_stream(self):
        """Test download assembles bytes from stream blocks."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config()
        client = _AioFTPClient(config)

        blocks = [b"chunk1", b"chunk2", b"chunk3"]

        async def mock_iter():
            for b in blocks:
                yield b

        mock_stream = MagicMock()
        mock_stream.iter_by_block = mock_iter
        mock_stream.finish = AsyncMock()

        client._ctx = MagicMock()
        client._ctx.download_stream = AsyncMock(return_value=mock_stream)

        data = await client.download("/data/event.dat")
        assert data == b"chunk1chunk2chunk3"
        mock_stream.finish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rename_delegates_to_ctx(self):
        """Test rename calls ctx.rename with correct args."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config()
        client = _AioFTPClient(config)
        client._ctx = MagicMock()
        client._ctx.rename = AsyncMock()

        await client.rename("/src", "/dst")
        client._ctx.rename.assert_awaited_once_with("/src", "/dst")

    @pytest.mark.asyncio
    async def test_delete_delegates_to_ctx(self):
        """Test delete calls ctx.remove with correct path."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config()
        client = _AioFTPClient(config)
        client._ctx = MagicMock()
        client._ctx.remove = AsyncMock()

        await client.delete("/data/event.dat")
        client._ctx.remove.assert_awaited_once_with("/data/event.dat")

    @pytest.mark.asyncio
    async def test_disconnect_calls_aexit(self):
        """Test disconnect calls __aexit__ and sets client to None."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config()
        client = _AioFTPClient(config)
        mock_ctx_mgr = MagicMock()
        mock_ctx_mgr.__aexit__ = AsyncMock(return_value=False)
        client._client = mock_ctx_mgr

        await client.disconnect()
        mock_ctx_mgr.__aexit__.assert_awaited_once_with(None, None, None)
        assert client._client is None

    @pytest.mark.asyncio
    async def test_disconnect_noop_when_no_client(self):
        """Test disconnect does nothing when _client is None."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        config = self._make_ftp_config()
        client = _AioFTPClient(config)
        assert client._client is None
        await client.disconnect()  # Should not raise


# ---------------------------------------------------------------------------
# _AsyncSSHClient tests
# ---------------------------------------------------------------------------


class TestAsyncSSHClient:
    """Tests for _AsyncSSHClient protocol adapter."""

    def _make_sftp_config(self, **kw):
        return FTPPullConfig(host="192.168.1.1", signal_id="SIG-001",
                             protocol=FTPProtocol.SFTP, **kw)

    @pytest.mark.asyncio
    async def test_init_stores_config(self):
        """Test constructor stores config and initialises conn/sftp to None."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config()
        client = _AsyncSSHClient(config)
        assert client._config is config
        assert client._conn is None
        assert client._sftp is None

    @pytest.mark.asyncio
    async def test_connect_with_password(self):
        """Test SFTP connect uses password when no ssh_key_path."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config(password="s3cret")
        client = _AsyncSSHClient(config)

        mock_sftp = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.start_sftp_client = AsyncMock(return_value=mock_sftp)

        mock_asyncssh = MagicMock()
        mock_asyncssh.connect = AsyncMock(return_value=mock_conn)

        with patch.dict("sys.modules", {"asyncssh": mock_asyncssh}):
            await client.connect()
        call_kwargs = mock_asyncssh.connect.call_args[1]
        assert call_kwargs["password"] == "s3cret"
        assert "client_keys" not in call_kwargs
        assert client._sftp is mock_sftp

    @pytest.mark.asyncio
    async def test_connect_with_ssh_key(self):
        """Test SFTP connect uses client_keys when ssh_key_path is set."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config(ssh_key_path="/keys/id_rsa")
        client = _AsyncSSHClient(config)

        mock_sftp = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.start_sftp_client = AsyncMock(return_value=mock_sftp)

        mock_asyncssh = MagicMock()
        mock_asyncssh.connect = AsyncMock(return_value=mock_conn)

        with patch.dict("sys.modules", {"asyncssh": mock_asyncssh}):
            await client.connect()
        call_kwargs = mock_asyncssh.connect.call_args[1]
        assert call_kwargs["client_keys"] == ["/keys/id_rsa"]
        assert "password" not in call_kwargs

    @pytest.mark.asyncio
    async def test_connect_warns_no_known_hosts(self):
        """Test SFTP connect warns when known_hosts_path is None."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config(known_hosts_path=None)
        client = _AsyncSSHClient(config)

        mock_sftp = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.start_sftp_client = AsyncMock(return_value=mock_sftp)

        mock_asyncssh = MagicMock()
        mock_asyncssh.connect = AsyncMock(return_value=mock_conn)

        with patch.dict("sys.modules", {"asyncssh": mock_asyncssh}), \
             patch(f"{_MOD}.logger") as mock_logger:
            await client.connect()
        mock_logger.warning.assert_called_once()
        assert "host key" in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_list_dir_parses_entries(self):
        """Test list_dir parses SFTP directory entries correctly."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config()
        client = _AsyncSSHClient(config)

        # Regular file with mtime
        entry1 = MagicMock()
        entry1.filename = "event.dat"
        entry1.attrs.type = 1  # regular file
        entry1.attrs.size = 1024
        entry1.attrs.mtime = 1712600000  # a UTC timestamp

        # Directory (should be skipped)
        entry2 = MagicMock()
        entry2.filename = "subdir"
        entry2.attrs.type = 2  # directory

        # File with no mtime
        entry3 = MagicMock()
        entry3.filename = "log.csv"
        entry3.attrs.type = 1
        entry3.attrs.size = 0
        entry3.attrs.mtime = None

        mock_sftp = AsyncMock()
        mock_sftp.readdir = AsyncMock(return_value=[entry1, entry2, entry3])
        client._sftp = mock_sftp

        result = await client.list_dir("/data")
        assert len(result) == 2
        assert result[0].name == "event.dat"
        assert result[0].size == 1024
        assert result[0].mtime is not None
        assert result[1].name == "log.csv"
        assert result[1].size == 0
        assert result[1].mtime is None

    @pytest.mark.asyncio
    async def test_download_reads_file(self):
        """Test download opens and reads remote file."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config()
        client = _AsyncSSHClient(config)

        mock_file = AsyncMock()
        mock_file.read = AsyncMock(return_value=b"file-data")
        mock_file.__aenter__ = AsyncMock(return_value=mock_file)
        mock_file.__aexit__ = AsyncMock(return_value=False)

        mock_sftp = AsyncMock()
        mock_sftp.open = MagicMock(return_value=mock_file)
        client._sftp = mock_sftp

        data = await client.download("/data/event.dat")
        assert data == b"file-data"
        mock_sftp.open.assert_called_once_with("/data/event.dat", "rb")

    @pytest.mark.asyncio
    async def test_rename_delegates_to_sftp(self):
        """Test rename calls sftp.rename with correct args."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config()
        client = _AsyncSSHClient(config)
        client._sftp = AsyncMock()
        client._sftp.rename = AsyncMock()

        await client.rename("/src", "/dst")
        client._sftp.rename.assert_awaited_once_with("/src", "/dst")

    @pytest.mark.asyncio
    async def test_delete_delegates_to_sftp(self):
        """Test delete calls sftp.remove with correct path."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config()
        client = _AsyncSSHClient(config)
        client._sftp = AsyncMock()
        client._sftp.remove = AsyncMock()

        await client.delete("/data/event.dat")
        client._sftp.remove.assert_awaited_once_with("/data/event.dat")

    @pytest.mark.asyncio
    async def test_disconnect_closes_conn(self):
        """Test disconnect closes connection and clears state."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config()
        client = _AsyncSSHClient(config)
        mock_conn = MagicMock()
        client._conn = mock_conn
        client._sftp = MagicMock()

        await client.disconnect()
        mock_conn.close.assert_called_once()
        assert client._conn is None
        assert client._sftp is None

    @pytest.mark.asyncio
    async def test_disconnect_noop_when_no_conn(self):
        """Test disconnect does nothing when _conn is None."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient
        config = self._make_sftp_config()
        client = _AsyncSSHClient(config)
        assert client._conn is None
        await client.disconnect()  # Should not raise


# ---------------------------------------------------------------------------
# _create_client factory function
# ---------------------------------------------------------------------------


class TestCreateClient:
    """Tests for _create_client factory function."""

    def test_sftp_returns_asyncssh_client(self):
        """Test SFTP protocol returns _AsyncSSHClient."""
        from tsigma.collection.methods.ftp_pull import _AsyncSSHClient, _create_client
        config = FTPPullConfig(host="h", signal_id="s", protocol=FTPProtocol.SFTP)
        client = _create_client(config)
        assert isinstance(client, _AsyncSSHClient)

    def test_ftp_returns_aioftp_client(self):
        """Test FTP protocol returns _AioFTPClient."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient, _create_client
        config = FTPPullConfig(host="h", signal_id="s", protocol=FTPProtocol.FTP)
        client = _create_client(config)
        assert isinstance(client, _AioFTPClient)

    def test_ftps_returns_aioftp_client(self):
        """Test FTPS protocol returns _AioFTPClient."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient, _create_client
        config = FTPPullConfig(host="h", signal_id="s", protocol=FTPProtocol.FTPS)
        client = _create_client(config)
        assert isinstance(client, _AioFTPClient)

    def test_method_create_client_delegates(self):
        """Test FTPPullMethod._create_client delegates to module function."""
        from tsigma.collection.methods.ftp_pull import _AioFTPClient
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s", protocol=FTPProtocol.FTP)
        client = method._create_client(config)
        assert isinstance(client, _AioFTPClient)


# ---------------------------------------------------------------------------
# Rotate mode helpers
# ---------------------------------------------------------------------------


class TestRotateHelpers:
    """Tests for rotate mode helper methods."""

    def test_is_tsigma_renamed_true(self):
        """Test _is_tsigma_renamed detects TSIGMA tag."""
        assert FTPPullMethod._is_tsigma_renamed(
            "event1.dat.tsigma.20260408T150000"
        ) is True

    def test_is_tsigma_renamed_false(self):
        """Test _is_tsigma_renamed returns False for normal files."""
        assert FTPPullMethod._is_tsigma_renamed("event1.dat") is False

    def test_original_name_from_renamed(self):
        """Test extracting original name from TSIGMA-renamed file."""
        assert FTPPullMethod._original_name_from_renamed(
            "event1.dat.tsigma.20260408T150000"
        ) == "event1.dat"

    def test_original_name_from_not_renamed(self):
        """Test extracting name from file without TSIGMA tag returns as-is."""
        assert FTPPullMethod._original_name_from_renamed(
            "event1.dat"
        ) == "event1.dat"

    def test_resolve_rotate_targets_specific_file_exists(self):
        """Test rotate targets with explicit filename that exists."""
        from tsigma.collection.methods.ftp_pull import FTPMode
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="h", signal_id="s", mode=FTPMode.ROTATE,
            rotate_filename="ATSPM.dat",
        )
        files = [
            RemoteFile(name="ATSPM.dat", size=100, mtime=None),
            RemoteFile(name="other.dat", size=50, mtime=None),
        ]
        targets = method._resolve_rotate_targets(config, files)
        assert targets == ["ATSPM.dat"]

    def test_resolve_rotate_targets_specific_file_missing(self):
        """Test rotate targets with explicit filename not in listing."""
        from tsigma.collection.methods.ftp_pull import FTPMode
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="h", signal_id="s", mode=FTPMode.ROTATE,
            rotate_filename="ATSPM.dat",
        )
        files = [RemoteFile(name="other.dat", size=50, mtime=None)]
        targets = method._resolve_rotate_targets(config, files)
        assert targets == []

    def test_resolve_rotate_targets_by_extension(self):
        """Test rotate targets filters by extension excluding renamed."""
        from tsigma.collection.methods.ftp_pull import FTPMode
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="h", signal_id="s", mode=FTPMode.ROTATE,
            file_extensions=[".dat"],
        )
        files = [
            RemoteFile(name="event1.dat", size=100, mtime=None),
            RemoteFile(name="event2.dat", size=200, mtime=None),
            RemoteFile(name="event1.dat.tsigma.20260408T150000", size=100, mtime=None),
            RemoteFile(name="readme.txt", size=10, mtime=None),
        ]
        targets = method._resolve_rotate_targets(config, files)
        assert targets == ["event1.dat", "event2.dat"]


# ---------------------------------------------------------------------------
# Rotate mode poll cycle
# ---------------------------------------------------------------------------


class TestPollRotate:
    """Tests for _poll_rotate rotate-mode poll cycle."""

    @pytest.mark.asyncio
    async def test_rotate_ingests_leftovers_first(self):
        """Test rotate mode ingests leftover renamed files from crashed cycles."""
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="h", signal_id="s",
            mode=FTPPullMethod._build_config("s", {"mode": "rotate"}).mode,
            file_extensions=[".dat"],
        )

        leftover = RemoteFile(
            name="event1.dat.tsigma.20260408T150000", size=100, mtime=None,
        )
        # First list_dir returns leftover, second returns no targets
        client = _mock_client()
        client.list_dir = AsyncMock(
            side_effect=[[leftover], []]
        )

        factory, _ = _mock_session_factory()

        with patch.object(method, "_ingest_and_delete",
                          new_callable=AsyncMock) as mock_iad:
            await method._poll_rotate(client, config, "SIG-001", factory)
        # Should have ingested the leftover
        mock_iad.assert_awaited_once()
        assert mock_iad.call_args[0][1] == leftover.name

    @pytest.mark.asyncio
    async def test_rotate_renames_downloads_deletes(self):
        """Test full rotate cycle: rename, SNMP cycle, download, delete."""
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="192.168.1.1", signal_id="SIG-001",
            mode=FTPPullMethod._build_config("s", {"mode": "rotate"}).mode,
            file_extensions=[".dat"],
        )

        active_file = RemoteFile(name="event1.dat", size=100, mtime=None)
        # First list_dir: no leftovers; second list_dir: one active file
        client = _mock_client()
        client.list_dir = AsyncMock(side_effect=[[], [active_file]])
        client.rename = AsyncMock()

        factory, _ = _mock_session_factory()

        with patch.object(method, "_snmp_stop_logging",
                          new_callable=AsyncMock) as mock_stop, \
             patch.object(method, "_snmp_start_logging",
                          new_callable=AsyncMock) as mock_start, \
             patch.object(method, "_ingest_and_delete",
                          new_callable=AsyncMock) as mock_iad:
            await method._poll_rotate(client, config, "SIG-001", factory)

        # Rename was called
        client.rename.assert_awaited_once()
        # SNMP cycle happened
        mock_stop.assert_awaited_once()
        mock_start.assert_awaited_once()
        # Download+delete of renamed file
        mock_iad.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rotate_no_targets_returns_early(self):
        """Test rotate mode returns early when no files match."""
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="h", signal_id="s",
            mode=FTPPullMethod._build_config("s", {"mode": "rotate"}).mode,
            file_extensions=[".dat"],
        )
        # No leftovers, no active files
        client = _mock_client()
        client.list_dir = AsyncMock(side_effect=[[], []])

        factory, _ = _mock_session_factory()

        with patch.object(method, "_snmp_stop_logging",
                          new_callable=AsyncMock) as mock_stop:
            await method._poll_rotate(client, config, "SIG-001", factory)
        mock_stop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rotate_rename_failure_skips_file(self):
        """Test rotate mode continues when rename fails for one file."""
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="h", signal_id="s",
            mode=FTPPullMethod._build_config("s", {"mode": "rotate"}).mode,
            file_extensions=[".dat"],
        )
        files = [
            RemoteFile(name="a.dat", size=100, mtime=None),
            RemoteFile(name="b.dat", size=200, mtime=None),
        ]
        client = _mock_client()
        client.list_dir = AsyncMock(side_effect=[[], files])
        # First rename fails, second succeeds
        client.rename = AsyncMock(
            side_effect=[OSError("permission denied"), None]
        )

        factory, _ = _mock_session_factory()

        with patch.object(method, "_snmp_stop_logging",
                          new_callable=AsyncMock), \
             patch.object(method, "_snmp_start_logging",
                          new_callable=AsyncMock), \
             patch.object(method, "_ingest_and_delete",
                          new_callable=AsyncMock) as mock_iad:
            await method._poll_rotate(client, config, "SIG-001", factory)
        # Only one file was renamed successfully, so only one ingest
        assert mock_iad.await_count == 1

    @pytest.mark.asyncio
    async def test_rotate_all_renames_fail_returns_early(self):
        """Test rotate mode returns early when all renames fail."""
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="h", signal_id="s",
            mode=FTPPullMethod._build_config("s", {"mode": "rotate"}).mode,
            file_extensions=[".dat"],
        )
        files = [RemoteFile(name="a.dat", size=100, mtime=None)]
        client = _mock_client()
        client.list_dir = AsyncMock(side_effect=[[], files])
        client.rename = AsyncMock(side_effect=OSError("denied"))

        factory, _ = _mock_session_factory()

        with patch.object(method, "_snmp_stop_logging",
                          new_callable=AsyncMock) as mock_stop:
            await method._poll_rotate(client, config, "SIG-001", factory)
        # No renames succeeded, so SNMP never called
        mock_stop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rotate_snmp_failure_continues(self):
        """Test rotate mode continues ingest even when SNMP cycle fails."""
        method = FTPPullMethod()
        config = FTPPullConfig(
            host="h", signal_id="s",
            mode=FTPPullMethod._build_config("s", {"mode": "rotate"}).mode,
            file_extensions=[".dat"],
        )
        files = [RemoteFile(name="a.dat", size=100, mtime=None)]
        client = _mock_client()
        client.list_dir = AsyncMock(side_effect=[[], files])
        client.rename = AsyncMock()

        factory, _ = _mock_session_factory()

        with patch.object(method, "_snmp_stop_logging",
                          new_callable=AsyncMock,
                          side_effect=RuntimeError("SNMP timeout")), \
             patch.object(method, "_ingest_and_delete",
                          new_callable=AsyncMock) as mock_iad:
            await method._poll_rotate(client, config, "SIG-001", factory)
        # Ingest still happens despite SNMP failure
        mock_iad.assert_awaited_once()


# ---------------------------------------------------------------------------
# _ingest_and_delete
# ---------------------------------------------------------------------------


class TestIngestAndDelete:
    """Tests for _ingest_and_delete helper."""

    @pytest.mark.asyncio
    async def test_full_success(self):
        """Test ingest_and_delete: download, decode, persist, delete, checkpoint."""
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s", remote_dir="/data")
        client = _mock_client()
        client.download = AsyncMock(return_value=b"\x00\x01")
        client.delete = AsyncMock()

        now = datetime.now(timezone.utc)
        events = [
            DecodedEvent(timestamp=now, event_code=1, event_param=0),
        ]
        decoder = _mock_decoder(events=events)
        factory, _ = _mock_session_factory()

        with patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            await method._ingest_and_delete(
                client, "event.dat.tsigma.20260408T150000",
                config, "SIG-001", factory,
            )
        client.download.assert_awaited_once()
        client.delete.assert_awaited_once()
        mock_save.assert_awaited_once()
        # original name should be extracted for decoder lookup
        assert mock_save.call_args[1]["last_filename"] == "event.dat"

    @pytest.mark.asyncio
    async def test_download_failure_skips_rest(self):
        """Test ingest_and_delete returns on download failure."""
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s", remote_dir="/data")
        client = _mock_client()
        client.download = AsyncMock(side_effect=OSError("timeout"))

        factory, _ = _mock_session_factory()

        with patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            await method._ingest_and_delete(
                client, "event.dat", config, "SIG-001", factory,
            )
        client.delete.assert_not_awaited()
        mock_save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_decode_failure_skips_persist_and_delete(self):
        """Test ingest_and_delete returns on decode failure."""
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s", remote_dir="/data")
        client = _mock_client()
        client.download = AsyncMock(return_value=b"\xff")

        factory, _ = _mock_session_factory()

        with patch(f"{_MOD}.resolve_decoder_by_extension",
                   side_effect=ValueError("bad")), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            await method._ingest_and_delete(
                client, "event.dat", config, "SIG-001", factory,
            )
        client.delete.assert_not_awaited()
        mock_save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_persist_failure_skips_delete(self):
        """Test ingest_and_delete returns on persist failure (file not deleted)."""
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s", remote_dir="/data")
        client = _mock_client()
        client.download = AsyncMock(return_value=b"\x00")

        decoder = _mock_decoder(events=[
            DecodedEvent(timestamp=datetime.now(timezone.utc),
                         event_code=1, event_param=0),
        ])
        factory, _ = _mock_session_factory()

        with patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock,
                   side_effect=RuntimeError("db error")), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            await method._ingest_and_delete(
                client, "event.dat", config, "SIG-001", factory,
            )
        client.delete.assert_not_awaited()
        mock_save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_failure_still_checkpoints(self):
        """Test ingest_and_delete saves checkpoint even when delete fails."""
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s", remote_dir="/data")
        client = _mock_client()
        client.download = AsyncMock(return_value=b"\x00")
        client.delete = AsyncMock(side_effect=OSError("permission denied"))

        decoder = _mock_decoder(events=[
            DecodedEvent(timestamp=datetime.now(timezone.utc),
                         event_code=1, event_param=0),
        ])
        factory, _ = _mock_session_factory()

        with patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch.object(method, "_save_checkpoint",
                          new_callable=AsyncMock) as mock_save:
            await method._ingest_and_delete(
                client, "event.dat", config, "SIG-001", factory,
            )
        # Checkpoint saved despite delete failure (idempotent upsert will dedup)
        mock_save.assert_awaited_once()


# ---------------------------------------------------------------------------
# SNMP helpers
# ---------------------------------------------------------------------------


class TestSNMPHelpers:
    """Tests for SNMP helper methods using pysnmp."""

    @pytest.mark.asyncio
    async def test_snmp_set_v1(self):
        """Test _snmp_set sends SNMP v1 SET with CommunityData."""
        config = FTPPullConfig(
            host="192.168.1.1", signal_id="s",
            snmp_version="v1",
            snmp_community="private", snmp_port=161,
        )

        with patch(f"{_MOD}.set_cmd", new_callable=AsyncMock) as mock_set_cmd, \
             patch(f"{_MOD}.UdpTransportTarget") as mock_target_cls, \
             patch(f"{_MOD}._get_snmp_engine", return_value=MagicMock()):
            mock_target_cls.create = AsyncMock(return_value=MagicMock())
            mock_set_cmd.return_value = (None, None, None, [])

            await FTPPullMethod._snmp_set("192.168.1.1", config, 0)

        mock_set_cmd.assert_awaited_once()
        args = mock_set_cmd.call_args
        auth_data = args[0][1]
        assert auth_data.message_processing_model == 0

    @pytest.mark.asyncio
    async def test_snmp_set_v2c(self):
        """Test _snmp_set sends SNMP v2c SET with CommunityData mpModel=1."""
        config = FTPPullConfig(
            host="192.168.1.1", signal_id="s",
            snmp_version="v2c",
            snmp_community="public", snmp_port=161,
        )

        with patch(f"{_MOD}.set_cmd", new_callable=AsyncMock) as mock_set_cmd, \
             patch(f"{_MOD}.UdpTransportTarget") as mock_target_cls, \
             patch(f"{_MOD}._get_snmp_engine", return_value=MagicMock()):
            mock_target_cls.create = AsyncMock(return_value=MagicMock())
            mock_set_cmd.return_value = (None, None, None, [])

            await FTPPullMethod._snmp_set("192.168.1.1", config, 1)

        args = mock_set_cmd.call_args
        auth_data = args[0][1]
        assert auth_data.message_processing_model == 1

    @pytest.mark.asyncio
    async def test_snmp_set_v3_authpriv(self):
        """Test _snmp_set sends SNMP v3 SET with UsmUserData."""
        config = FTPPullConfig(
            host="192.168.1.1", signal_id="s",
            snmp_version="v3",
            snmp_username="admin",
            snmp_security_level="authPriv",
            snmp_auth_protocol="SHA256",
            snmp_auth_passphrase="authpass",
            snmp_priv_protocol="AES128",
            snmp_priv_passphrase="privpass",
        )

        with patch(f"{_MOD}.set_cmd", new_callable=AsyncMock) as mock_set_cmd, \
             patch(f"{_MOD}.UdpTransportTarget") as mock_target_cls, \
             patch(f"{_MOD}._get_snmp_engine", return_value=MagicMock()), \
             patch(f"{_MOD}._build_usm_user_data") as mock_usm:
            mock_target_cls.create = AsyncMock(return_value=MagicMock())
            mock_set_cmd.return_value = (None, None, None, [])
            mock_usm.return_value = MagicMock(userName="admin")

            await FTPPullMethod._snmp_set("192.168.1.1", config, 0)

        mock_usm.assert_called_once_with(config)
        mock_set_cmd.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_snmp_set_v3_noauthnopriv(self):
        """Test _snmp_set sends SNMP v3 SET with noAuthNoPriv."""
        config = FTPPullConfig(
            host="192.168.1.1", signal_id="s",
            snmp_version="v3",
            snmp_username="readonly",
            snmp_security_level="noAuthNoPriv",
        )

        with patch(f"{_MOD}.set_cmd", new_callable=AsyncMock) as mock_set_cmd, \
             patch(f"{_MOD}.UdpTransportTarget") as mock_target_cls, \
             patch(f"{_MOD}._get_snmp_engine", return_value=MagicMock()), \
             patch(f"{_MOD}._build_usm_user_data") as mock_usm:
            mock_target_cls.create = AsyncMock(return_value=MagicMock())
            mock_set_cmd.return_value = (None, None, None, [])
            mock_usm.return_value = MagicMock(userName="readonly")

            await FTPPullMethod._snmp_set("192.168.1.1", config, 0)

        mock_usm.assert_called_once_with(config)

    @pytest.mark.asyncio
    async def test_snmp_set_error_indication_raises(self):
        """Test _snmp_set raises RuntimeError on SNMP error indication."""
        config = FTPPullConfig(
            host="192.168.1.1", signal_id="s",
            snmp_version="v1", snmp_community="public",
        )

        with patch(f"{_MOD}.set_cmd", new_callable=AsyncMock) as mock_set_cmd, \
             patch(f"{_MOD}.UdpTransportTarget") as mock_target_cls, \
             patch(f"{_MOD}._get_snmp_engine", return_value=MagicMock()):
            mock_target_cls.create = AsyncMock(return_value=MagicMock())
            mock_set_cmd.return_value = ("requestTimedOut", None, None, [])

            with pytest.raises(RuntimeError, match="SNMP SET failed"):
                await FTPPullMethod._snmp_set("192.168.1.1", config, 0)

    @pytest.mark.asyncio
    async def test_snmp_set_error_status_raises(self):
        """Test _snmp_set raises RuntimeError on SNMP error status."""
        config = FTPPullConfig(
            host="192.168.1.1", signal_id="s",
            snmp_version="v1", snmp_community="public",
        )

        mock_error_status = MagicMock()
        mock_error_status.prettyPrint.return_value = "noAccess"
        mock_error_status.__bool__ = lambda self: True
        mock_var_bind = (MagicMock(), MagicMock())

        with patch(f"{_MOD}.set_cmd", new_callable=AsyncMock) as mock_set_cmd, \
             patch(f"{_MOD}.UdpTransportTarget") as mock_target_cls, \
             patch(f"{_MOD}._get_snmp_engine", return_value=MagicMock()):
            mock_target_cls.create = AsyncMock(return_value=MagicMock())
            mock_set_cmd.return_value = (None, mock_error_status, 1, [mock_var_bind])

            with pytest.raises(RuntimeError, match="SNMP SET error"):
                await FTPPullMethod._snmp_set("192.168.1.1", config, 0)

    @pytest.mark.asyncio
    async def test_snmp_stop_logging(self):
        """Test _snmp_stop_logging calls _snmp_set with OFF value."""
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s")
        with patch.object(method, "_snmp_set",
                          new_callable=AsyncMock) as mock_set:
            await method._snmp_stop_logging(config)
        mock_set.assert_awaited_once_with(config.host, config, 0)

    @pytest.mark.asyncio
    async def test_snmp_start_logging(self):
        """Test _snmp_start_logging calls _snmp_set with ON value."""
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s")
        with patch.object(method, "_snmp_set",
                          new_callable=AsyncMock) as mock_set:
            await method._snmp_start_logging(config)
        mock_set.assert_awaited_once_with(config.host, config, 1)


# ---------------------------------------------------------------------------
# _save_checkpoint
# ---------------------------------------------------------------------------


class TestSaveCheckpoint:
    """Tests for _save_checkpoint method."""

    @pytest.mark.asyncio
    async def test_creates_new_checkpoint(self):
        """Test _save_checkpoint creates a new checkpoint row when none exists."""
        method = FTPPullMethod()
        factory, mock_session = _mock_session_factory()

        # The real PollingCheckpoint ORM model has default=0 that only fires
        # on flush, not in-memory. Pre-set the defaults that _save_checkpoint
        # expects to += against by intercepting session.add.
        original_add = mock_session.add

        def patched_add(obj):
            # Set numeric defaults that the ORM would provide on flush
            if hasattr(obj, "events_ingested") and obj.events_ingested is None:
                obj.events_ingested = 0
            if hasattr(obj, "files_ingested") and obj.files_ingested is None:
                obj.files_ingested = 0
            if hasattr(obj, "consecutive_silent_cycles") and obj.consecutive_silent_cycles is None:
                obj.consecutive_silent_cycles = 0
            if hasattr(obj, "consecutive_errors") and obj.consecutive_errors is None:
                obj.consecutive_errors = 0
            return original_add(obj)

        mock_session.add = patched_add

        await method._save_checkpoint(
            "SIG-001", factory,
            last_filename="event.dat",
            files_hash="abc123",
            new_events=10,
            new_files=1,
        )
        mock_session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_updates_existing_checkpoint(self):
        """Test _save_checkpoint updates an existing checkpoint row."""
        method = FTPPullMethod()
        factory, mock_session = _mock_session_factory()

        existing = MagicMock()
        existing.events_ingested = 50
        existing.files_ingested = 5
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = existing
        mock_session.execute = AsyncMock(return_value=result_mock)

        await method._save_checkpoint(
            "SIG-001", factory,
            last_filename="event2.dat",
            new_events=10,
            new_files=1,
        )
        # Should NOT add a new row
        mock_session.add.assert_not_called()
        # Should update fields on existing
        assert existing.last_filename == "event2.dat"
        assert existing.consecutive_errors == 0
        assert existing.last_error is None
        mock_session.flush.assert_awaited_once()


# ---------------------------------------------------------------------------
# _filter_new_files (file hash/checkpoint comparison logic)
# ---------------------------------------------------------------------------


class TestFilterNewFiles:
    """Tests for _filter_new_files checkpoint comparison logic."""

    def _make_method(self):
        return FTPPullMethod()

    def test_first_poll_downloads_everything(self):
        """Test first poll (no checkpoint) downloads all matching files."""
        method = self._make_method()
        files = [
            RemoteFile(name="a.dat", size=100, mtime=None),
            RemoteFile(name="b.dat", size=200, mtime=None),
        ]
        result = method._filter_new_files(files, None, "hash1")
        assert len(result) == 2

    def test_unchanged_hash_downloads_nothing(self):
        """Test unchanged directory hash means no new files."""
        method = self._make_method()
        checkpoint = MagicMock()
        checkpoint.files_hash = "hash1"
        checkpoint.last_file_mtime = None
        files = [RemoteFile(name="a.dat", size=100, mtime=None)]
        result = method._filter_new_files(files, checkpoint, "hash1")
        assert len(result) == 0

    def test_changed_hash_no_mtime_downloads_all(self):
        """Test changed hash without mtime checkpoint downloads everything."""
        method = self._make_method()
        checkpoint = MagicMock()
        checkpoint.files_hash = "hash1"
        checkpoint.last_file_mtime = None
        files = [
            RemoteFile(name="a.dat", size=100, mtime=None),
            RemoteFile(name="b.dat", size=200, mtime=None),
        ]
        result = method._filter_new_files(files, checkpoint, "hash2")
        assert len(result) == 2

    def test_changed_hash_with_mtime_filters_newer(self):
        """Test changed hash with mtime narrows to newer files."""
        method = self._make_method()
        old_time = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        new_time = datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc)
        checkpoint = MagicMock()
        checkpoint.files_hash = "hash1"
        checkpoint.last_file_mtime = old_time
        files = [
            RemoteFile(name="old.dat", size=100, mtime=old_time),
            RemoteFile(name="new.dat", size=200, mtime=new_time),
        ]
        result = method._filter_new_files(files, checkpoint, "hash2")
        assert len(result) == 1
        assert result[0].name == "new.dat"

    def test_changed_hash_mtime_fallback_when_no_newer(self):
        """Test mtime filter falls back to all files when none are newer."""
        method = self._make_method()
        old_time = datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc)
        checkpoint = MagicMock()
        checkpoint.files_hash = "hash1"
        checkpoint.last_file_mtime = old_time
        files = [
            RemoteFile(name="a.dat", size=100, mtime=None),
            RemoteFile(name="b.dat", size=200, mtime=None),
        ]
        result = method._filter_new_files(files, checkpoint, "hash2")
        # Falls back to all files since mtime comparison yields nothing
        assert len(result) == 2

    def test_results_sorted_by_mtime(self):
        """Test output is sorted by mtime ascending."""
        method = self._make_method()
        t1 = datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        files = [
            RemoteFile(name="later.dat", size=100, mtime=t1),
            RemoteFile(name="earlier.dat", size=200, mtime=t2),
        ]
        result = method._filter_new_files(files, None, "hash1")
        assert result[0].name == "earlier.dat"
        assert result[1].name == "later.dat"

    def test_checkpoint_no_hash_downloads_everything(self):
        """Test checkpoint with no files_hash downloads everything."""
        method = self._make_method()
        checkpoint = MagicMock()
        checkpoint.files_hash = None
        files = [RemoteFile(name="a.dat", size=100, mtime=None)]
        result = method._filter_new_files(files, checkpoint, "hash1")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _download_and_ingest mtime tracking
# ---------------------------------------------------------------------------


class TestDownloadAndIngest:
    """Tests for _download_and_ingest mtime tracking."""

    @pytest.mark.asyncio
    async def test_tracks_newest_mtime(self):
        """Test _download_and_ingest tracks the newest file mtime."""
        method = FTPPullMethod()
        config = FTPPullConfig(host="h", signal_id="s", remote_dir="/data")
        t1 = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc)
        files = [
            RemoteFile(name="a.dat", size=100, mtime=t1),
            RemoteFile(name="b.dat", size=200, mtime=t2),
        ]
        client = _mock_client()
        client.download = AsyncMock(return_value=b"\x00")
        decoder = _mock_decoder(events=[
            DecodedEvent(timestamp=datetime.now(timezone.utc),
                         event_code=1, event_param=0),
        ])
        factory, _ = _mock_session_factory()

        with patch(f"{_MOD}.resolve_decoder_by_extension",
                   return_value=decoder), \
             patch(f"{_MOD}.persist_events_with_drift_check",
                   new_callable=AsyncMock):
            _total_events, total_files, newest_name, newest_mtime = \
                await method._download_and_ingest(
                    client, files, config, "SIG-001", factory, None,
                )
        assert total_files == 2
        assert newest_mtime == t2
        assert newest_name == "b.dat"


# ---------------------------------------------------------------------------
# Build config with rotate/SNMP fields
# ---------------------------------------------------------------------------


class TestBuildConfigRotate:
    """Tests for _build_config with rotate mode fields."""

    def test_rotate_mode_fields(self):
        """Test _build_config parses rotate-mode SNMP fields."""
        raw = {
            "host": "192.168.1.1",
            "mode": "rotate",
            "snmp_community": "private",
            "snmp_port": 162,
            "logging_oid": "1.2.3.4",
            "rotate_filename": "ATSPM.dat",
        }
        config = FTPPullMethod._build_config("SIG-001", raw)
        from tsigma.collection.methods.ftp_pull import FTPMode
        assert config.mode == FTPMode.ROTATE
        assert config.snmp_community == "private"
        assert config.snmp_port == 162
        assert config.logging_oid == "1.2.3.4"
        assert config.rotate_filename == "ATSPM.dat"

    def test_rotate_mode_defaults(self):
        """Test _build_config uses defaults for missing rotate fields."""
        raw = {"host": "192.168.1.1"}
        config = FTPPullMethod._build_config("SIG-001", raw)
        from tsigma.collection.methods.ftp_pull import _ASC3_LOGGING_OID, FTPMode
        assert config.mode == FTPMode.PASSIVE
        assert config.snmp_community == "public"
        assert config.snmp_port == 161
        assert config.logging_oid == _ASC3_LOGGING_OID
        assert config.rotate_filename is None


class TestBuildConfigSNMPv3:
    """Tests for _build_config with SNMPv3 fields."""

    def test_snmpv3_fields_parsed(self):
        """Test _build_config parses SNMPv3 fields from JSONB."""
        raw = {
            "host": "192.168.1.1",
            "mode": "rotate",
            "snmp_version": "v3",
            "snmp_username": "admin",
            "snmp_security_level": "authPriv",
            "snmp_auth_protocol": "SHA256",
            "snmp_auth_passphrase": "authpass123",
            "snmp_priv_protocol": "AES128",
            "snmp_priv_passphrase": "privpass123",
        }
        config = FTPPullMethod._build_config("SIG-001", raw)
        assert config.snmp_version == "v3"
        assert config.snmp_username == "admin"
        assert config.snmp_security_level == "authPriv"
        assert config.snmp_auth_protocol == "SHA256"
        assert config.snmp_auth_passphrase == "authpass123"
        assert config.snmp_priv_protocol == "AES128"
        assert config.snmp_priv_passphrase == "privpass123"

    def test_snmpv3_defaults_to_v1(self):
        """Test _build_config defaults snmp_version to v1."""
        raw = {"host": "192.168.1.1"}
        config = FTPPullMethod._build_config("SIG-001", raw)
        assert config.snmp_version == "v1"
        assert config.snmp_username == ""
        assert config.snmp_security_level == "authPriv"
        assert config.snmp_auth_protocol == "SHA"
        assert config.snmp_auth_passphrase == ""
        assert config.snmp_priv_protocol == "AES128"
        assert config.snmp_priv_passphrase == ""

    def test_v2c_version(self):
        """Test _build_config accepts v2c."""
        raw = {
            "host": "192.168.1.1",
            "snmp_version": "v2c",
            "snmp_community": "private",
        }
        config = FTPPullMethod._build_config("SIG-001", raw)
        assert config.snmp_version == "v2c"
        assert config.snmp_community == "private"
