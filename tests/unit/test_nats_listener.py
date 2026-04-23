"""
Unit tests for NATS listener ingestion method plugin.

Tests registration, health checks, subscription lifecycle,
message decoding, error handling, and teardown.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.methods.nats_listener import (
    NATSListenerConfig,
    NATSListenerMethod,
)
from tsigma.collection.registry import IngestionMethodRegistry


class TestNATSListenerRegistration:
    """Tests for registry integration."""

    def test_registered(self):
        """IngestionMethodRegistry.get('nats_listener') returns NATSListenerMethod."""
        cls = IngestionMethodRegistry.get("nats_listener")
        assert cls is NATSListenerMethod


class TestNATSListenerHealthCheck:
    """Tests for health_check behaviour."""

    @pytest.mark.asyncio
    async def test_health_check_no_clients(self):
        """health_check returns False when no clients are connected."""
        method = NATSListenerMethod()
        assert await method.health_check() is False


class TestNATSListenerStart:
    """Tests for start() method."""

    @pytest.mark.asyncio
    async def test_start_no_signals(self, caplog):
        """start with empty signals list logs warning, no subscriptions created."""
        method = NATSListenerMethod()
        session_factory = AsyncMock()

        await method.start({"signals": []}, session_factory)

        assert not method._subscriptions
        assert "no signal subscriptions" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_start_skips_empty_subject(self, caplog):
        """Signal config with empty subject is skipped."""
        method = NATSListenerMethod()
        session_factory = AsyncMock()

        config = {
            "signals": [
                {"signal_id": "SIG-001", "url": "nats://localhost:4222", "subject": ""},
            ]
        }

        with patch(
            "tsigma.collection.methods.nats_listener.nats"
        ) as mock_nats_mod:
            await method.start(config, session_factory)

        assert "SIG-001" not in method._subscriptions
        assert "no subject" in caplog.text.lower()
        mock_nats_mod.connect.assert_not_called()


class TestNATSListenerHandleMessage:
    """Tests for _handle_message."""

    @pytest.mark.asyncio
    async def test_handle_message_decodes_and_persists(self):
        """Decoder is called, events are persisted via SDK."""
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()

        config = NATSListenerConfig(
            signal_id="SIG-100",
            url="nats://localhost:4222",
            subject="signals.100.events",
            decoder="asc3",
        )

        fake_events = [MagicMock(), MagicMock()]
        mock_decoder = MagicMock()
        mock_decoder.decode_bytes.return_value = fake_events

        mock_msg = MagicMock()
        mock_msg.data = b"\x01\x02\x03"

        with patch(
            "tsigma.collection.methods.nats_listener.resolve_decoder_by_name",
            return_value=mock_decoder,
        ) as mock_resolve, patch(
            "tsigma.collection.methods.nats_listener.persist_events_with_drift_check",
            new_callable=AsyncMock,
        ) as mock_persist:
            await method._handle_message("SIG-100", config, mock_msg)

        mock_resolve.assert_called_once_with("asc3")
        mock_decoder.decode_bytes.assert_called_once_with(b"\x01\x02\x03")
        mock_persist.assert_awaited_once_with(
            fake_events, "SIG-100", method._session_factory
        )

    @pytest.mark.asyncio
    async def test_handle_message_decode_error(self, caplog):
        """Decoder exception is caught and logged without crashing."""
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()

        config = NATSListenerConfig(
            signal_id="SIG-200",
            url="nats://localhost:4222",
            subject="signals.200.events",
            decoder="bad_decoder",
        )

        mock_msg = MagicMock()
        mock_msg.data = b"\xff\xff"

        with patch(
            "tsigma.collection.methods.nats_listener.resolve_decoder_by_name",
            side_effect=RuntimeError("decode exploded"),
        ), patch(
            "tsigma.collection.methods.nats_listener.persist_events_with_drift_check",
            new_callable=AsyncMock,
        ) as mock_persist:
            await method._handle_message("SIG-200", config, mock_msg)

        mock_persist.assert_not_called()
        assert "failed to decode" in caplog.text.lower()


class TestNATSListenerSubscribe:
    """Tests for _subscribe connection and subscription paths."""

    @pytest.mark.asyncio
    async def test_subscribe_connects_and_subscribes(self):
        """Mock nats.connect, verify subscription created."""
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()

        mock_client = AsyncMock()
        mock_sub = AsyncMock()
        mock_client.subscribe = AsyncMock(return_value=mock_sub)

        config = NATSListenerConfig(
            signal_id="SIG-SUB",
            url="nats://localhost:4222",
            subject="signals.sub.events",
        )

        with patch(
            "tsigma.collection.methods.nats_listener.nats"
        ) as mock_nats_mod:
            mock_nats_mod.connect = AsyncMock(return_value=mock_client)
            await method._subscribe(config)

        mock_nats_mod.connect.assert_awaited_once_with(
            servers=["nats://localhost:4222"]
        )
        mock_client.subscribe.assert_awaited_once()
        assert method._clients["SIG-SUB"] is mock_client
        assert method._subscriptions["SIG-SUB"] is mock_sub

    @pytest.mark.asyncio
    async def test_subscribe_with_queue_group(self):
        """Queue parameter passed to subscribe."""
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()

        mock_client = AsyncMock()
        mock_sub = AsyncMock()
        mock_client.subscribe = AsyncMock(return_value=mock_sub)

        config = NATSListenerConfig(
            signal_id="SIG-QG",
            url="nats://localhost:4222",
            subject="signals.qg.events",
            queue_group="workers",
        )

        with patch(
            "tsigma.collection.methods.nats_listener.nats"
        ) as mock_nats_mod:
            mock_nats_mod.connect = AsyncMock(return_value=mock_client)
            await method._subscribe(config)

        # Should have called subscribe with queue kwarg
        call_kwargs = mock_client.subscribe.call_args
        assert call_kwargs.kwargs.get("queue") == "workers" or call_kwargs[1].get("queue") == "workers"

    @pytest.mark.asyncio
    async def test_subscribe_connection_error(self, caplog):
        """nats.connect raises, logged not crashed."""
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()

        config = NATSListenerConfig(
            signal_id="SIG-ERR",
            url="nats://badhost:4222",
            subject="signals.err.events",
        )

        with patch(
            "tsigma.collection.methods.nats_listener.nats"
        ) as mock_nats_mod:
            mock_nats_mod.connect = AsyncMock(
                side_effect=ConnectionRefusedError("refused")
            )
            # Should not raise
            await method._subscribe(config)

        assert "SIG-ERR" not in method._subscriptions
        assert "failed to subscribe" in caplog.text.lower()


class TestNATSListenerStop:
    """Tests for stop() teardown."""

    @pytest.mark.asyncio
    async def test_stop_clears_state(self):
        """After stop(), _clients, _subscriptions, and _configs are all empty."""
        method = NATSListenerMethod()

        # Populate internal state with mocks
        mock_client = AsyncMock()
        mock_sub = AsyncMock()

        method._clients["SIG-001"] = mock_client
        method._subscriptions["SIG-001"] = mock_sub
        method._configs["SIG-001"] = NATSListenerConfig(
            signal_id="SIG-001",
            url="nats://localhost:4222",
            subject="signals.1.events",
        )

        await method.stop()

        assert method._clients == {}
        assert method._subscriptions == {}
        assert method._configs == {}

        mock_sub.unsubscribe.assert_awaited_once()
        mock_client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_handles_unsubscribe_error(self, caplog):
        """Unsubscribe exception is caught — close still called."""
        method = NATSListenerMethod()

        mock_client = AsyncMock()
        mock_sub = AsyncMock()
        mock_sub.unsubscribe = AsyncMock(side_effect=RuntimeError("unsub boom"))

        method._clients["SIG-001"] = mock_client
        method._subscriptions["SIG-001"] = mock_sub
        method._configs["SIG-001"] = NATSListenerConfig(
            signal_id="SIG-001",
            url="nats://localhost:4222",
            subject="signals.1.events",
        )

        await method.stop()

        # Unsubscribe raised but close was still called
        mock_client.close.assert_awaited_once()
        assert method._clients == {}
        assert method._subscriptions == {}
        assert "error unsubscribing" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_stop_handles_close_error(self, caplog):
        """client.close exception is caught — state still cleared."""
        method = NATSListenerMethod()

        mock_client = AsyncMock()
        mock_client.close = AsyncMock(side_effect=RuntimeError("close boom"))
        mock_sub = AsyncMock()

        method._clients["SIG-001"] = mock_client
        method._subscriptions["SIG-001"] = mock_sub
        method._configs["SIG-001"] = NATSListenerConfig(
            signal_id="SIG-001",
            url="nats://localhost:4222",
            subject="signals.1.events",
        )

        await method.stop()

        mock_sub.unsubscribe.assert_awaited_once()
        assert method._clients == {}
        assert method._subscriptions == {}
        assert "error closing" in caplog.text.lower()


class TestNATSListenerStartAuth:
    """Tests for start() with token and credentials_file."""

    @pytest.mark.asyncio
    async def test_start_with_token(self):
        """Config with token passes it to nats.connect."""
        method = NATSListenerMethod()
        session_factory = AsyncMock()

        mock_client = AsyncMock()
        mock_sub = AsyncMock()
        mock_client.subscribe = AsyncMock(return_value=mock_sub)

        config = {
            "signals": [
                {
                    "signal_id": "SIG-TOKEN",
                    "url": "nats://localhost:4222",
                    "subject": "signals.token.events",
                    "token": "my-secret-token",
                }
            ]
        }

        with patch(
            "tsigma.collection.methods.nats_listener.nats"
        ) as mock_nats_mod:
            mock_nats_mod.connect = AsyncMock(return_value=mock_client)
            await method.start(config, session_factory)

        mock_nats_mod.connect.assert_awaited_once_with(
            servers=["nats://localhost:4222"],
            token="my-secret-token",
        )

    @pytest.mark.asyncio
    async def test_start_with_credentials_file(self):
        """Config with credentials_file passes it to nats.connect."""
        method = NATSListenerMethod()
        session_factory = AsyncMock()

        mock_client = AsyncMock()
        mock_sub = AsyncMock()
        mock_client.subscribe = AsyncMock(return_value=mock_sub)

        config = {
            "signals": [
                {
                    "signal_id": "SIG-CREDS",
                    "url": "nats://localhost:4222",
                    "subject": "signals.creds.events",
                    "credentials_file": "/path/to/creds.nk",
                }
            ]
        }

        with patch(
            "tsigma.collection.methods.nats_listener.nats"
        ) as mock_nats_mod:
            mock_nats_mod.connect = AsyncMock(return_value=mock_client)
            await method.start(config, session_factory)

        mock_nats_mod.connect.assert_awaited_once_with(
            servers=["nats://localhost:4222"],
            user_credentials="/path/to/creds.nk",
        )
