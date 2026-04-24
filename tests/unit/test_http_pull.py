"""
Unit tests for HTTP pull ingestion method plugin.

Tests configuration, URL building, poll cycle logic,
and config construction from signal_metadata JSONB dicts.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.collection.methods.http_pull import (
    HTTPPullConfig,
    HTTPPullMethod,
)
from tsigma.collection.registry import ExecutionMode, IngestionMethodRegistry


class TestHTTPPullConfig:
    """Tests for HTTPPullConfig dataclass."""

    def test_required_fields_only(self):
        """Test config with just host and signal_id."""
        config = HTTPPullConfig(host="192.168.1.100", signal_id="SIG-001")
        assert config.host == "192.168.1.100"
        assert config.signal_id == "SIG-001"
        assert config.port == 80
        assert config.use_tls is False
        assert config.path == "/v1/asclog/xml/full"
        assert config.timeout_seconds == 30
        assert config.decoder is None

    def test_all_fields(self):
        """Test config with all fields populated."""
        config = HTTPPullConfig(
            host="10.0.0.50",
            signal_id="SIG-042",
            port=8443,
            use_tls=True,
            path="/api/events",
            timeout_seconds=10,
            decoder="maxtime",
        )
        assert config.host == "10.0.0.50"
        assert config.port == 8443
        assert config.use_tls is True
        assert config.path == "/api/events"
        assert config.timeout_seconds == 10
        assert config.decoder == "maxtime"

    def test_default_port(self):
        """Test default port is 80."""
        config = HTTPPullConfig(host="h", signal_id="s")
        assert config.port == 80

    def test_default_path(self):
        """Test default path is MaxTime endpoint."""
        config = HTTPPullConfig(host="h", signal_id="s")
        assert config.path == "/v1/asclog/xml/full"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config_dict(**overrides) -> dict:
    """Create a raw collection config dict with sensible defaults."""
    defaults = {
        "host": "192.168.1.100",
        "port": 80,
        "path": "/v1/asclog/xml/full",
        "timeout_seconds": 30,
    }
    defaults.update(overrides)
    return defaults


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


def _mock_aiohttp_response(status=200, body=b"<EventResponses/>"):
    """Create a mock aiohttp response."""
    response = AsyncMock()
    response.status = status
    response.read = AsyncMock(return_value=body)
    return response


def _mock_aiohttp_session(response=None):
    """Create a mock aiohttp.ClientSession context manager."""
    if response is None:
        response = _mock_aiohttp_response()

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=response),
        __aexit__=AsyncMock(return_value=False),
    ))

    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    return mock_ctx, mock_session


# ---------------------------------------------------------------------------
# HTTPPullMethod — registration and construction
# ---------------------------------------------------------------------------


class TestHTTPPullMethodRegistration:
    """Tests for HTTPPullMethod plugin registration."""

    def test_registered_in_registry(self):
        """Test HTTPPullMethod is registered as 'http_pull'."""
        assert "http_pull" in IngestionMethodRegistry.list_available()
        cls = IngestionMethodRegistry.get("http_pull")
        assert cls is HTTPPullMethod

    def test_execution_mode_is_polling(self):
        """Test HTTPPullMethod declares polling execution mode."""
        assert HTTPPullMethod.execution_mode == ExecutionMode.POLLING

    def test_constructor_no_args(self):
        """Test constructor takes no arguments."""
        method = HTTPPullMethod()
        assert isinstance(method, HTTPPullMethod)


class TestBuildConfig:
    """Tests for HTTPPullMethod._build_config()."""

    def test_full_config(self):
        """Test building HTTPPullConfig from a complete dict."""
        raw = {
            "host": "10.0.0.50",
            "port": 8443,
            "use_tls": True,
            "path": "/api/events",
            "timeout_seconds": 10,
            "decoder": "maxtime",
        }
        config = HTTPPullMethod._build_config("SIG-042", raw)
        assert config.host == "10.0.0.50"
        assert config.signal_id == "SIG-042"
        assert config.port == 8443
        assert config.use_tls is True
        assert config.path == "/api/events"
        assert config.timeout_seconds == 10
        assert config.decoder == "maxtime"

    def test_defaults_for_missing_fields(self):
        """Test missing optional fields get defaults."""
        raw = {"host": "192.168.1.1"}
        config = HTTPPullMethod._build_config("SIG-001", raw)
        assert config.port == 80
        assert config.use_tls is False
        assert config.path == "/v1/asclog/xml/full"
        assert config.timeout_seconds == 30
        assert config.decoder is None

    def test_host_from_dict(self):
        """Test host is extracted from config dict."""
        raw = {"host": "10.0.0.50"}
        config = HTTPPullMethod._build_config("SIG-001", raw)
        assert config.host == "10.0.0.50"


# ---------------------------------------------------------------------------
# HTTPPullMethod — URL building
# ---------------------------------------------------------------------------


class TestBuildUrl:
    """Tests for HTTPPullMethod._build_url()."""

    def test_url_without_since(self):
        """Test URL without since parameter."""
        method = HTTPPullMethod()
        config = HTTPPullConfig(host="10.0.0.1", signal_id="SIG-001")
        url = method._build_url(config, since=None)
        assert url == "http://10.0.0.1:80/v1/asclog/xml/full"

    def test_url_with_since(self):
        """Test URL with since parameter."""
        method = HTTPPullMethod()
        config = HTTPPullConfig(host="10.0.0.1", signal_id="SIG-001")
        since = datetime(2024, 3, 15, 14, 30, 45, 500000, tzinfo=timezone.utc)
        url = method._build_url(config, since=since)
        assert "?since=" in url
        assert "03-15-2024" in url
        assert "14:30:45" in url

    def test_url_with_tls(self):
        """Test URL uses https scheme when TLS enabled."""
        method = HTTPPullMethod()
        config = HTTPPullConfig(
            host="10.0.0.1", signal_id="SIG-001", use_tls=True
        )
        url = method._build_url(config, since=None)
        assert url.startswith("https://")

    def test_url_with_custom_port(self):
        """Test URL includes custom port."""
        method = HTTPPullMethod()
        config = HTTPPullConfig(
            host="10.0.0.1", signal_id="SIG-001", port=8443
        )
        url = method._build_url(config, since=None)
        assert ":8443" in url

    def test_url_with_custom_path(self):
        """Test URL includes custom path."""
        method = HTTPPullMethod()
        config = HTTPPullConfig(
            host="10.0.0.1", signal_id="SIG-001", path="/api/v2/events"
        )
        url = method._build_url(config, since=None)
        assert url.endswith("/api/v2/events")


# ---------------------------------------------------------------------------
# HTTPPullMethod — poll_once
# ---------------------------------------------------------------------------


class TestPollOnce:
    """Tests for poll_once() poll cycle logic."""

    @pytest.mark.asyncio
    async def test_makes_http_get_request(self):
        """Test poll_once makes HTTP GET to correct URL."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        mock_ctx, mock_http_session = _mock_aiohttp_session()
        decoder = MagicMock()
        decoder.decode_bytes.return_value = []

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=decoder), \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock):
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        mock_http_session.get.assert_called_once()
        call_url = mock_http_session.get.call_args[0][0]
        assert "192.168.1.100" in call_url
        assert "/v1/asclog/xml/full" in call_url

    @pytest.mark.asyncio
    async def test_passes_response_to_decoder(self):
        """Test poll_once passes response bytes to decoder."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        xml_body = (
            b'<EventResponses><EventResponse>'
            b'<Event TimeStamp="2024-03-15 14:30:45"'
            b' EventTypeID="1" Parameter="2"/>'
            b'</EventResponse></EventResponses>'
        )
        response = _mock_aiohttp_response(body=xml_body)
        mock_ctx, _ = _mock_aiohttp_session(response)

        decoder = MagicMock()
        decoder.decode_bytes.return_value = []

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=decoder), \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock):
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        decoder.decode_bytes.assert_called_once_with(xml_body)

    @pytest.mark.asyncio
    async def test_persists_decoded_events(self):
        """Test poll_once persists decoded events to database."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        now = datetime.now(timezone.utc)
        events = [
            DecodedEvent(timestamp=now, event_code=1, event_param=2),
            DecodedEvent(timestamp=now, event_code=82, event_param=5),
        ]

        response = _mock_aiohttp_response(body=b"<xml/>")
        mock_ctx, _ = _mock_aiohttp_session(response)

        decoder = MagicMock()
        decoder.decode_bytes.return_value = events

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=decoder), \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.save_checkpoint",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock) as mock_persist:
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        mock_persist.assert_awaited_once_with(events, "SIG-001", factory)

    @pytest.mark.asyncio
    async def test_saves_checkpoint_on_success(self):
        """Test poll_once saves checkpoint after ingesting events."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        now = datetime(2024, 3, 15, 14, 30, 45, tzinfo=timezone.utc)
        events = [DecodedEvent(timestamp=now, event_code=1, event_param=2)]

        response = _mock_aiohttp_response(body=b"<xml/>")
        mock_ctx, _ = _mock_aiohttp_session(response)

        decoder = MagicMock()
        decoder.decode_bytes.return_value = events

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=decoder), \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.save_checkpoint",
                   new_callable=AsyncMock) as mock_save, \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock):
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        mock_save.assert_awaited_once()
        assert mock_save.call_args[1]["last_event_timestamp"] == now
        assert mock_save.call_args[1]["events_ingested"] == 1

    @pytest.mark.asyncio
    async def test_uses_since_param_on_subsequent_poll(self):
        """Test second poll includes ?since= from checkpoint timestamp."""
        method = HTTPPullMethod()
        last_time = datetime(2024, 3, 15, 14, 0, 0, tzinfo=timezone.utc)

        checkpoint = MagicMock()
        checkpoint.last_event_timestamp = last_time

        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        response = _mock_aiohttp_response(body=b"<xml/>")
        mock_ctx, mock_http_session = _mock_aiohttp_session(response)

        decoder = MagicMock()
        decoder.decode_bytes.return_value = []

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=decoder), \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock,
                   return_value=checkpoint), \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock):
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        call_url = mock_http_session.get.call_args[0][0]
        assert "?since=" in call_url

    @pytest.mark.asyncio
    async def test_no_since_on_first_poll(self):
        """Test first poll has no ?since= parameter."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        response = _mock_aiohttp_response(body=b"<xml/>")
        mock_ctx, mock_http_session = _mock_aiohttp_session(response)

        decoder = MagicMock()
        decoder.decode_bytes.return_value = []

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=decoder), \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock):
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        call_url = mock_http_session.get.call_args[0][0]
        assert "?since=" not in call_url

    @pytest.mark.asyncio
    async def test_connection_error_records_error(self):
        """Test failed connection records error via checkpoint."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(side_effect=ConnectionError("refused"))
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.record_error",
                   new_callable=AsyncMock) as mock_error:
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        mock_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_response_no_events(self):
        """Test empty XML response produces no events."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        response = _mock_aiohttp_response(body=b"<EventResponses/>")
        mock_ctx, _ = _mock_aiohttp_session(response)

        decoder = MagicMock()
        decoder.decode_bytes.return_value = []

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=decoder), \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.methods.http_pull.save_checkpoint",
                   new_callable=AsyncMock) as mock_save:
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        # No events — checkpoint should not be saved
        mock_save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_http_error_status_records_error(self):
        """Test non-200 response records error and does not persist events."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        response = _mock_aiohttp_response(status=500, body=b"Internal Server Error")
        mock_ctx, _ = _mock_aiohttp_session(response)

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.record_error",
                   new_callable=AsyncMock) as mock_error, \
             patch("tsigma.collection.methods.http_pull.save_checkpoint",
                   new_callable=AsyncMock) as mock_save:
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        mock_error.assert_awaited_once()
        mock_save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_explicit_decoder(self):
        """Test poll_once uses explicitly configured decoder name."""
        method = HTTPPullMethod()
        config = _make_config_dict(decoder="custom_xml")
        factory, _ = _mock_session_factory()

        mock_decoder_inst = MagicMock()
        mock_decoder_inst.decode_bytes.return_value = []

        response = _mock_aiohttp_response(body=b"<xml/>")
        mock_ctx, _ = _mock_aiohttp_session(response)

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=mock_decoder_inst) as mock_resolve, \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock):
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        mock_resolve.assert_called_once_with("custom_xml")

    @pytest.mark.asyncio
    async def test_default_decoder_is_maxtime(self):
        """Test no explicit decoder defaults to 'maxtime'."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        mock_decoder_inst = MagicMock()
        mock_decoder_inst.decode_bytes.return_value = []

        response = _mock_aiohttp_response(body=b"<xml/>")
        mock_ctx, _ = _mock_aiohttp_session(response)

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=mock_decoder_inst) as mock_resolve, \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock):
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        mock_resolve.assert_called_once_with("maxtime")


# ---------------------------------------------------------------------------
# HTTPPullMethod — health check
# ---------------------------------------------------------------------------


class TestSaveCheckpoint:
    """Tests for save_checkpoint SDK function (used by poll_once)."""

    @pytest.mark.asyncio
    async def test_save_checkpoint_creates_new(self):
        """No existing checkpoint: creates a new PollingCheckpoint."""
        from tsigma.collection.sdk import save_checkpoint as sdk_save_checkpoint

        factory, mock_session = _mock_session_factory()

        last_event = datetime(2024, 6, 1, 11, 59, 0, tzinfo=timezone.utc)

        with patch("tsigma.collection.sdk.settings") as mock_settings:
            mock_settings.checkpoint_future_tolerance_seconds = 300
            await sdk_save_checkpoint(
                "http_pull",
                "controller",
                "SIG-NEW",
                factory,
                last_event_timestamp=last_event,
                events_ingested=5,
            )

        # session.add should have been called with new checkpoint
        mock_session.add.assert_called_once()
        added_obj = mock_session.add.call_args[0][0]
        assert added_obj.device_type == "controller"
        assert added_obj.device_id == "SIG-NEW"
        assert added_obj.method == "http_pull"
        mock_session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_save_checkpoint_updates_existing(self):
        """Existing checkpoint: updates timestamp and counters."""
        from tsigma.collection.sdk import save_checkpoint as sdk_save_checkpoint
        from tsigma.models.checkpoint import PollingCheckpoint

        existing_cp = MagicMock(spec=PollingCheckpoint)
        existing_cp.signal_id = "SIG-EXIST"
        existing_cp.method = "http_pull"
        existing_cp.events_ingested = 100
        existing_cp.consecutive_errors = 2
        existing_cp.consecutive_silent_cycles = 1

        mock_session = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = existing_cp
        mock_session.execute = AsyncMock(return_value=result_mock)
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        factory = MagicMock(return_value=mock_ctx)

        last_event = datetime(2024, 6, 1, 11, 59, 0, tzinfo=timezone.utc)

        with patch("tsigma.collection.sdk.settings") as mock_settings:
            mock_settings.checkpoint_future_tolerance_seconds = 300
            await sdk_save_checkpoint(
                "http_pull",
                "controller",
                "SIG-EXIST",
                factory,
                last_event_timestamp=last_event,
                events_ingested=10,
            )

        # Should NOT call add (existing checkpoint)
        mock_session.add.assert_not_called()
        # Counters should be updated
        assert existing_cp.last_event_timestamp == last_event
        assert existing_cp.events_ingested == 110
        assert existing_cp.consecutive_errors == 0
        assert existing_cp.last_error is None
        assert existing_cp.consecutive_silent_cycles == 0

    @pytest.mark.asyncio
    async def test_save_checkpoint_caps_future(self):
        """Future-dated event is capped at server_time + tolerance."""
        from datetime import timedelta

        from tsigma.collection.sdk import save_checkpoint as sdk_save_checkpoint
        from tsigma.models.checkpoint import PollingCheckpoint

        existing_cp = MagicMock(spec=PollingCheckpoint)
        existing_cp.signal_id = "SIG-FUTURE"
        existing_cp.method = "http_pull"
        existing_cp.events_ingested = 50
        existing_cp.consecutive_errors = 0
        existing_cp.consecutive_silent_cycles = 0

        mock_session = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = existing_cp
        mock_session.execute = AsyncMock(return_value=result_mock)
        mock_session.flush = AsyncMock()

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        factory = MagicMock(return_value=mock_ctx)

        # Event timestamp 2 hours in the future
        future_event = datetime.now(timezone.utc) + timedelta(hours=2)

        with patch("tsigma.collection.sdk.settings") as mock_settings:
            mock_settings.checkpoint_future_tolerance_seconds = 300
            await sdk_save_checkpoint(
                "http_pull",
                "controller",
                "SIG-FUTURE",
                factory,
                last_event_timestamp=future_event,
                events_ingested=1,
            )

        # Checkpoint should be capped — not the future event timestamp
        saved_ts = existing_cp.last_event_timestamp
        assert saved_ts < future_event


class TestPollOnceDecodeError:
    """Tests for decode error path in poll_once."""

    @pytest.mark.asyncio
    async def test_poll_once_decode_error(self):
        """Decoder raises, error recorded via record_error."""
        method = HTTPPullMethod()
        config = _make_config_dict()
        factory, _ = _mock_session_factory()

        response = _mock_aiohttp_response(body=b"<garbage/>")
        mock_ctx, _ = _mock_aiohttp_session(response)

        mock_decoder = MagicMock()
        mock_decoder.decode_bytes.side_effect = ValueError("bad XML")

        with patch("tsigma.collection.methods.http_pull.aiohttp") as mock_aiohttp, \
             patch("tsigma.collection.methods.http_pull.resolve_decoder_by_name",
                   return_value=mock_decoder), \
             patch("tsigma.collection.methods.http_pull.load_checkpoint",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.methods.http_pull.record_error",
                   new_callable=AsyncMock) as mock_error, \
             patch("tsigma.collection.methods.http_pull.persist_events_with_drift_check",
                   new_callable=AsyncMock) as mock_persist:
            mock_aiohttp.ClientSession.return_value = mock_ctx
            mock_aiohttp.ClientTimeout = MagicMock()
            await method.poll_once("SIG-001", config, factory)

        mock_error.assert_awaited_once()
        mock_persist.assert_not_awaited()


class TestHTTPPullHealthCheck:
    """Tests for health_check()."""

    @pytest.mark.asyncio
    async def test_health_check_returns_true(self):
        """Test health_check returns True (polling methods always healthy)."""
        method = HTTPPullMethod()
        result = await method.health_check()
        assert result is True
