"""
Unit tests for NATS listener ingestion method plugin.

Covers the new contract: Layer-2 server config (URL, credentials, TLS,
instance) sourced from process env via ``ListenerService``; per-device
subscriptions (subject, decoder, queue_group) come from the orchestrator
``devices`` argument; events persisted through the ``IngestionTarget``;
single shared NATS connection per listener container.
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.methods.nats_listener import (
    NATSListenerMethod,
    NATSServerConfig,
    NATSSubscription,
)
from tsigma.collection.registry import IngestionMethodRegistry
from tsigma.collection.targets import ControllerTarget


def _server_config(**overrides):
    base = {
        "url": "nats://localhost:4222",
        "credentials_file": None,
        "tls": False,
        "max_reconnects": -1,
        "instance": "default",
    }
    base.update(overrides)
    return base


class TestNATSListenerRegistration:
    def test_registered(self):
        assert IngestionMethodRegistry.get("nats_listener") is NATSListenerMethod


class TestNATSListenerHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_no_client(self):
        method = NATSListenerMethod()
        assert await method.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_when_client_open(self):
        method = NATSListenerMethod()
        mock_client = MagicMock()
        mock_client.is_closed = False
        method._client = mock_client
        assert await method.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_when_client_closed(self):
        method = NATSListenerMethod()
        mock_client = MagicMock()
        mock_client.is_closed = True
        method._client = mock_client
        assert await method.health_check() is False


class TestNATSBuildServerConfig:
    def test_defaults(self):
        cfg = NATSListenerMethod._build_server_config({"url": "nats://h:4222"})
        assert cfg.url == "nats://h:4222"
        assert cfg.credentials_file is None
        assert cfg.tls is False
        assert cfg.max_reconnects == -1
        assert cfg.instance == "default"

    def test_overrides(self):
        cfg = NATSListenerMethod._build_server_config({
            "url": "tls://h:4222",
            "credentials_file": "/run/secrets/nats.creds",
            "tls": True,
            "max_reconnects": 10,
            "instance": "cloud",
        })
        assert cfg.credentials_file == "/run/secrets/nats.creds"
        assert cfg.tls is True
        assert cfg.max_reconnects == 10
        assert cfg.instance == "cloud"


class TestNATSBuildSubscriptions:
    def test_skips_devices_without_subject(self):
        subs = NATSListenerMethod._build_subscriptions([
            ("SIG-OK", {"subject": "signals.ok.events"}),
            ("SIG-NO-SUBJ", {"decoder": "openphase"}),
            ("SIG-EMPTY", {"subject": ""}),
        ])
        assert list(subs.keys()) == ["SIG-OK"]

    def test_queue_group_passthrough(self):
        subs = NATSListenerMethod._build_subscriptions([
            ("SIG-A", {"subject": "a", "queue_group": "workers"}),
            ("SIG-B", {"subject": "b"}),
        ])
        assert subs["SIG-A"].queue_group == "workers"
        assert subs["SIG-B"].queue_group is None


class TestNATSHandleMessage:
    @pytest.mark.asyncio
    async def test_decodes_and_persists_via_target(self):
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()
        method._target.persist_with_drift_check = AsyncMock()

        sub = NATSSubscription(
            device_id="SIG-100",
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
        ) as mock_resolve:
            await method._handle_message(sub, mock_msg)

        mock_resolve.assert_called_once_with("asc3")
        mock_decoder.decode_bytes.assert_called_once_with(b"\x01\x02\x03")
        method._target.persist_with_drift_check.assert_awaited_once()
        assert (
            method._target.persist_with_drift_check.call_args[0][1]
            == "SIG-100"
        )

    @pytest.mark.asyncio
    async def test_decode_error_is_swallowed(self, caplog):
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()
        method._target.persist_with_drift_check = AsyncMock()

        sub = NATSSubscription(
            device_id="SIG-200",
            subject="signals.200.events",
            decoder="bad",
        )

        mock_msg = MagicMock()
        mock_msg.data = b"\xff\xff"

        with (
            caplog.at_level(
                logging.ERROR,
                logger="tsigma.collection.methods.nats_listener",
            ),
            patch(
                "tsigma.collection.methods.nats_listener.resolve_decoder_by_name",
                side_effect=RuntimeError("decode exploded"),
            ),
        ):
            await method._handle_message(sub, mock_msg)

        method._target.persist_with_drift_check.assert_not_called()
        assert "failed to decode" in caplog.text.lower()


class TestNATSSubscribe:
    @pytest.mark.asyncio
    async def test_subscribe_no_queue(self):
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()

        mock_client = AsyncMock()
        mock_sub = MagicMock()
        mock_client.subscribe = AsyncMock(return_value=mock_sub)
        method._client = mock_client

        sub_cfg = NATSSubscription(
            device_id="SIG-SUB", subject="signals.sub.events",
        )
        await method._subscribe("SIG-SUB", sub_cfg)

        mock_client.subscribe.assert_awaited_once()
        call_kwargs = mock_client.subscribe.call_args.kwargs
        assert "queue" not in call_kwargs
        assert method._subscriptions["SIG-SUB"] is mock_sub

    @pytest.mark.asyncio
    async def test_subscribe_with_queue_group(self):
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()

        mock_client = AsyncMock()
        mock_sub = MagicMock()
        mock_client.subscribe = AsyncMock(return_value=mock_sub)
        method._client = mock_client

        sub_cfg = NATSSubscription(
            device_id="SIG-QG", subject="signals.qg",
            queue_group="workers",
        )
        await method._subscribe("SIG-QG", sub_cfg)

        call_kwargs = mock_client.subscribe.call_args.kwargs
        assert call_kwargs.get("queue") == "workers"

    @pytest.mark.asyncio
    async def test_subscribe_error_is_logged(self, caplog):
        method = NATSListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()

        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock(
            side_effect=RuntimeError("subscription rejected"),
        )
        method._client = mock_client

        sub_cfg = NATSSubscription(
            device_id="SIG-ERR", subject="signals.err",
        )
        with caplog.at_level(
            logging.ERROR,
            logger="tsigma.collection.methods.nats_listener",
        ):
            await method._subscribe("SIG-ERR", sub_cfg)

        assert "SIG-ERR" not in method._subscriptions
        assert "failed to subscribe" in caplog.text.lower()


class TestNATSStartStop:
    @pytest.mark.asyncio
    async def test_start_no_url_refuses(self, caplog):
        method = NATSListenerMethod()
        with caplog.at_level(
            logging.ERROR,
            logger="tsigma.collection.methods.nats_listener",
        ):
            await method.start(
                {"url": ""},
                AsyncMock(),
                target=ControllerTarget(),
                devices=[("SIG-1", {"subject": "x"})],
            )
        assert method._client is None
        assert "missing url" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_start_no_devices_warns_and_skips_connect(self, caplog):
        method = NATSListenerMethod()
        with (
            caplog.at_level(
                logging.WARNING,
                logger="tsigma.collection.methods.nats_listener",
            ),
            patch(
                "tsigma.collection.methods.nats_listener.nats"
            ) as mock_nats,
        ):
            await method.start(
                _server_config(),
                AsyncMock(),
                target=ControllerTarget(),
                devices=[],
            )
        assert method._client is None
        assert "no matching" in caplog.text.lower()
        mock_nats.connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_connects_once_and_subscribes_per_device(self):
        method = NATSListenerMethod()
        mock_client = AsyncMock()
        mock_sub_a = MagicMock()
        mock_sub_b = MagicMock()
        mock_client.subscribe = AsyncMock(
            side_effect=[mock_sub_a, mock_sub_b],
        )

        with patch(
            "tsigma.collection.methods.nats_listener.nats"
        ) as mock_nats:
            mock_nats.connect = AsyncMock(return_value=mock_client)
            await method.start(
                _server_config(),
                AsyncMock(),
                target=ControllerTarget(),
                devices=[
                    ("SIG-A", {"subject": "signals.a"}),
                    ("SIG-B", {"subject": "signals.b"}),
                ],
            )

        # Single shared connection
        mock_nats.connect.assert_awaited_once()
        # One subscribe per device
        assert mock_client.subscribe.await_count == 2
        assert method._subscriptions["SIG-A"] is mock_sub_a
        assert method._subscriptions["SIG-B"] is mock_sub_b

    @pytest.mark.asyncio
    async def test_start_passes_credentials_file(self):
        method = NATSListenerMethod()
        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock(return_value=MagicMock())

        with patch(
            "tsigma.collection.methods.nats_listener.nats"
        ) as mock_nats:
            mock_nats.connect = AsyncMock(return_value=mock_client)
            await method.start(
                _server_config(credentials_file="/run/secrets/nats.creds"),
                AsyncMock(),
                target=ControllerTarget(),
                devices=[("SIG-X", {"subject": "signals.x"})],
            )

        kwargs = mock_nats.connect.call_args.kwargs
        assert kwargs["user_credentials"] == "/run/secrets/nats.creds"
        assert kwargs["servers"] == ["nats://localhost:4222"]

    @pytest.mark.asyncio
    async def test_stop_clears_state(self):
        method = NATSListenerMethod()
        method._server_config = NATSServerConfig(**_server_config())

        mock_client = AsyncMock()
        mock_sub = AsyncMock()

        method._client = mock_client
        method._subscriptions["SIG-001"] = mock_sub
        method._configs["SIG-001"] = NATSSubscription(
            device_id="SIG-001", subject="signals.1.events",
        )

        await method.stop()

        assert method._client is None
        assert not method._subscriptions
        assert not method._configs
        mock_sub.unsubscribe.assert_awaited_once()
        mock_client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_handles_unsubscribe_error(self, caplog):
        method = NATSListenerMethod()

        mock_client = AsyncMock()
        mock_sub = AsyncMock()
        mock_sub.unsubscribe = AsyncMock(
            side_effect=RuntimeError("unsub boom"),
        )

        method._client = mock_client
        method._subscriptions["SIG-001"] = mock_sub

        with caplog.at_level(
            logging.ERROR,
            logger="tsigma.collection.methods.nats_listener",
        ):
            await method.stop()

        mock_client.close.assert_awaited_once()
        assert method._client is None
        assert "error unsubscribing" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_stop_handles_close_error(self, caplog):
        method = NATSListenerMethod()

        mock_client = AsyncMock()
        mock_client.close = AsyncMock(side_effect=RuntimeError("close boom"))
        mock_sub = AsyncMock()

        method._client = mock_client
        method._subscriptions["SIG-001"] = mock_sub

        with caplog.at_level(
            logging.ERROR,
            logger="tsigma.collection.methods.nats_listener",
        ):
            await method.stop()

        mock_sub.unsubscribe.assert_awaited_once()
        assert method._client is None
        assert "error closing" in caplog.text.lower()
