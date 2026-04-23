"""
Unit tests for MQTT listener ingestion method plugin.

Tests registration, lifecycle (start/stop), health checks,
and message handling with mocked MQTT and decoder dependencies.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.methods.mqtt_listener import (
    MQTTListenerConfig,
    MQTTListenerMethod,
)
from tsigma.collection.registry import IngestionMethodRegistry


class TestMQTTListenerRegistration:
    """Tests for registry integration."""

    def test_registered(self):
        """IngestionMethodRegistry.get('mqtt_listener') returns MQTTListenerMethod."""
        assert IngestionMethodRegistry.get("mqtt_listener") is MQTTListenerMethod


class TestMQTTListenerHealthCheck:
    """Tests for health_check behaviour."""

    @pytest.mark.asyncio
    async def test_health_check_no_tasks(self):
        """health_check returns False when no tasks are running."""
        method = MQTTListenerMethod()
        assert await method.health_check() is False


class TestMQTTListenerStart:
    """Tests for start() behaviour."""

    @pytest.mark.asyncio
    async def test_start_no_signals(self):
        """start with empty signals list logs warning, no tasks created."""
        method = MQTTListenerMethod()
        session_factory = AsyncMock()

        await method.start({"signals": []}, session_factory)

        assert len(method._tasks) == 0
        assert len(method._configs) == 0

    @pytest.mark.asyncio
    async def test_start_skips_empty_topic(self):
        """Signal config with empty topic is skipped."""
        method = MQTTListenerMethod()
        session_factory = AsyncMock()

        config = {
            "signals": [
                {
                    "signal_id": "SIG-001",
                    "broker": "mqtt://localhost:1883",
                    "topic": "",
                    "decoder": "asc3",
                }
            ]
        }

        await method.start(config, session_factory)

        assert "SIG-001" not in method._tasks
        assert "SIG-001" not in method._configs


class TestMQTTListenerHandleMessage:
    """Tests for _handle_message behaviour."""

    @pytest.mark.asyncio
    @patch(
        "tsigma.collection.methods.mqtt_listener.persist_events_with_drift_check",
        new_callable=AsyncMock,
    )
    @patch("tsigma.collection.methods.mqtt_listener.resolve_decoder_by_name")
    async def test_handle_message_decodes_and_persists(
        self, mock_resolve_decoder, mock_persist
    ):
        """Decoder.decode_bytes is called and events are persisted."""
        fake_events = [MagicMock(), MagicMock()]
        mock_decoder = MagicMock()
        mock_decoder.decode_bytes.return_value = fake_events
        mock_resolve_decoder.return_value = mock_decoder

        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()

        cfg = MQTTListenerConfig(
            signal_id="SIG-100",
            broker="mqtt://localhost:1883",
            topic="signals/100/events",
            decoder="asc3",
        )

        payload = b"\x01\x02\x03"
        await method._handle_message("SIG-100", cfg, payload)

        mock_resolve_decoder.assert_called_once_with("asc3")
        mock_decoder.decode_bytes.assert_called_once_with(payload)
        mock_persist.assert_awaited_once_with(
            fake_events, "SIG-100", method._session_factory
        )

    @pytest.mark.asyncio
    @patch(
        "tsigma.collection.methods.mqtt_listener.persist_events_with_drift_check",
        new_callable=AsyncMock,
    )
    @patch("tsigma.collection.methods.mqtt_listener.resolve_decoder_by_name")
    async def test_handle_message_decode_error(
        self, mock_resolve_decoder, mock_persist
    ):
        """Decoder exception is caught — no crash, no persist call."""
        mock_resolve_decoder.side_effect = RuntimeError("bad decoder")

        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()

        cfg = MQTTListenerConfig(
            signal_id="SIG-200",
            broker="mqtt://localhost:1883",
            topic="signals/200/events",
            decoder="broken",
        )

        # Should not raise
        await method._handle_message("SIG-200", cfg, b"\xff")

        mock_persist.assert_not_awaited()


class TestMQTTSubscriberLoop:
    """Tests for _subscriber_loop connection paths."""

    @pytest.mark.asyncio
    async def test_subscriber_loop_connects(self):
        """Mock aiomqtt.Client context manager, verify subscribe called."""

        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()

        config = MQTTListenerConfig(
            signal_id="SIG-MQTT-CONN",
            broker="mqtt://localhost:1883",
            topic="signals/conn/events",
            decoder="auto",
            qos=1,
        )

        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock()

        # Simulate: yield one message then stop
        mock_message = MagicMock()
        mock_message.payload = b"\x01\x02"

        async def _fake_messages():
            yield mock_message
            # After first message, set stop to break the loop
            method._stop_event.set()

        mock_client.messages = _fake_messages()

        mock_client_cls = MagicMock()
        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client_ctx

        with patch(
            "tsigma.collection.methods.mqtt_listener.aiomqtt"
        ) as mock_aiomqtt:
            mock_aiomqtt.Client = mock_client_cls
            mock_aiomqtt.MqttError = Exception
            mock_aiomqtt.TLSParameters = MagicMock()

            with patch.object(
                method, "_handle_message", new_callable=AsyncMock
            ):
                await method._subscriber_loop(config)

        mock_client.subscribe.assert_awaited_once_with(
            "signals/conn/events", qos=1
        )

    @pytest.mark.asyncio
    async def test_subscriber_loop_reconnects_on_mqtt_error(self):
        """MqttError triggers reconnect attempt."""

        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()

        config = MQTTListenerConfig(
            signal_id="SIG-MQTT-RECONN",
            broker="mqtt://localhost:1883",
            topic="signals/reconn/events",
        )

        call_count = 0

        class FakeMqttError(Exception):
            pass

        async def _fake_aenter(_self_ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise FakeMqttError("connection lost")
            # Second call: set stop event so loop exits
            method._stop_event.set()
            raise FakeMqttError("stop now")

        mock_client_cls = MagicMock()
        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = _fake_aenter
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client_ctx

        with patch(
            "tsigma.collection.methods.mqtt_listener.aiomqtt"
        ) as mock_aiomqtt:
            mock_aiomqtt.Client = mock_client_cls
            mock_aiomqtt.MqttError = FakeMqttError
            mock_aiomqtt.TLSParameters = MagicMock()

            with patch("asyncio.sleep", new_callable=AsyncMock):
                await method._subscriber_loop(config)

        # Should have tried to connect at least twice (reconnect)
        assert call_count >= 2


class TestMQTTListenerStop:
    """Tests for stop() behaviour."""

    @pytest.mark.asyncio
    async def test_stop_clears_state(self):
        """After stop(), _tasks and _configs are empty."""
        import asyncio

        method = MQTTListenerMethod()

        # Simulate some state
        method._configs["SIG-001"] = MQTTListenerConfig(
            signal_id="SIG-001",
            broker="mqtt://localhost:1883",
            topic="signals/1/events",
        )

        # Create a real asyncio task that we can cancel
        async def _noop():
            await asyncio.sleep(3600)

        task = asyncio.create_task(_noop())
        method._tasks["SIG-001"] = task

        await method.stop()

        assert len(method._tasks) == 0
        assert len(method._configs) == 0


class TestMQTTSubscriberLoopGenericError:
    """Tests for _subscriber_loop handling of unexpected exceptions."""

    @pytest.mark.asyncio
    async def test_subscriber_loop_handles_unexpected_error(self, caplog):
        """Non-MqttError exception logs and retries before stop."""

        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()

        config = MQTTListenerConfig(
            signal_id="SIG-MQTT-GENERIC",
            broker="mqtt://localhost:1883",
            topic="signals/generic/events",
        )

        call_count = 0

        async def _fake_aenter(_self_ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("unexpected boom")
            # Second call: set stop event so loop exits
            method._stop_event.set()
            raise RuntimeError("stop now")

        mock_client_cls = MagicMock()
        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = _fake_aenter
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client_ctx

        class FakeMqttError(Exception):
            pass

        with patch(
            "tsigma.collection.methods.mqtt_listener.aiomqtt"
        ) as mock_aiomqtt:
            mock_aiomqtt.Client = mock_client_cls
            mock_aiomqtt.MqttError = FakeMqttError
            mock_aiomqtt.TLSParameters = MagicMock()

            with patch("asyncio.sleep", new_callable=AsyncMock):
                await method._subscriber_loop(config)

        assert call_count >= 2
        assert "unexpected error" in caplog.text.lower()


class TestMQTTHealthCheckDoneTasks:
    """Tests for health_check when all tasks are done."""

    @pytest.mark.asyncio
    async def test_health_check_with_done_tasks(self):
        """health_check returns False when all tasks are done."""
        method = MQTTListenerMethod()

        done_task = MagicMock()
        done_task.done.return_value = True
        method._tasks["SIG-DONE"] = done_task

        assert await method.health_check() is False
