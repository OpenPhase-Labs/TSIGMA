"""
Unit tests for gRPC server ingestion method plugin.

Covers the new contract: Layer-2 server config in ``config`` dict,
device validation against the registered set passed via ``devices``,
event persistence through the ``IngestionTarget``.
"""

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# Make the generated proto modules importable for the tests.
_PROTO_DIR = str(
    Path(__file__).resolve().parent.parent.parent
    / "tsigma/collection/decoders/proto"
)
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from openphase.v1 import common_pb2, ihr_events_pb2  # noqa: E402

from tsigma.collection.decoders.base import DecodedEvent  # noqa: E402
from tsigma.collection.methods.grpc_server import (  # noqa: E402
    GRPCServerConfig,
    GRPCServerMethod,
    _IngestionServicer,
)
from tsigma.collection.registry import IngestionMethodRegistry  # noqa: E402
from tsigma.collection.targets import (  # noqa: E402
    ControllerTarget,
    RoadsideTarget,
)


def _make_servicer(
    decoder=None,
    target=None,
    registered=None,
    session_factory=None,
):
    """Build an _IngestionServicer with sensible defaults for tests."""
    return _IngestionServicer(
        decoder=decoder if decoder is not None else MagicMock(),
        session_factory=(
            session_factory if session_factory is not None else AsyncMock()
        ),
        target=target if target is not None else ControllerTarget(),
        registered_device_ids=(
            registered if registered is not None else {"INT-001"}
        ),
    )


class TestGRPCServerRegistration:
    def test_registered(self):
        assert IngestionMethodRegistry.get("grpc_server") is GRPCServerMethod


class TestGRPCServerHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_when_not_started(self):
        method = GRPCServerMethod()
        assert await method.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_when_server_set(self):
        method = GRPCServerMethod()
        method._server = MagicMock()
        assert await method.health_check() is True


class TestGRPCServerConfig:
    def test_defaults(self):
        cfg = GRPCServerConfig()
        assert cfg.port == 50051
        assert cfg.bind_address == "0.0.0.0"
        assert cfg.decoder == "openphase"
        assert cfg.tls_cert_file is None
        assert cfg.tls_key_file is None
        assert cfg.max_message_size == 4 * 1024 * 1024

    def test_overrides(self):
        cfg = GRPCServerConfig(
            port=12345, decoder="auto", max_message_size=10_000,
        )
        assert cfg.port == 12345
        assert cfg.decoder == "auto"
        assert cfg.max_message_size == 10_000


class TestIngestionServicerPublishBatch:
    @pytest.mark.asyncio
    async def test_publish_batch_persists_via_target(self):
        decoder = MagicMock()
        decoder.decode_bytes.return_value = [
            DecodedEvent(timestamp=None, event_code=82, event_param=5),
        ]
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock()
        servicer = _make_servicer(
            decoder=decoder, target=target, registered={"INT-001"},
        )

        batch = common_pb2.CompactEventBatch(
            intersection_id="INT-001",
            base_timestamp_ns=1_700_000_000_000_000_000,
        )
        evt = batch.events.add()
        evt.offset_ms = 0
        evt.code = ihr_events_pb2.EVENT_DETECTOR_ON
        evt.param = 5

        ack = await servicer.PublishBatch(batch, MagicMock())
        assert ack.events_accepted == 1
        assert ack.error == ""
        target.persist_with_drift_check.assert_awaited_once()
        assert target.persist_with_drift_check.call_args[0][1] == "INT-001"

    @pytest.mark.asyncio
    async def test_publish_batch_rejects_unregistered_device(self):
        decoder = MagicMock()
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock()
        servicer = _make_servicer(
            decoder=decoder, target=target, registered={"INT-001"},
        )

        batch = common_pb2.CompactEventBatch(intersection_id="UNKNOWN-002")
        ack = await servicer.PublishBatch(batch, MagicMock())

        assert ack.events_accepted == 0
        assert "unregistered" in ack.error
        target.persist_with_drift_check.assert_not_called()
        decoder.decode_bytes.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_batch_decode_failure_returns_error(self):
        decoder = MagicMock()
        decoder.decode_bytes.side_effect = ValueError("bad bytes")
        servicer = _make_servicer(decoder=decoder, registered={"INT-002"})

        batch = common_pb2.CompactEventBatch(intersection_id="INT-002")
        ack = await servicer.PublishBatch(batch, MagicMock())

        assert ack.events_accepted == 0
        assert "bad bytes" in ack.error

    @pytest.mark.asyncio
    async def test_publish_batch_persist_failure_returns_error(self):
        decoder = MagicMock()
        decoder.decode_bytes.return_value = [
            DecodedEvent(timestamp=None, event_code=82, event_param=5),
        ]
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock(
            side_effect=RuntimeError("db down"),
        )
        servicer = _make_servicer(
            decoder=decoder, target=target, registered={"INT-003"},
        )

        batch = common_pb2.CompactEventBatch(intersection_id="INT-003")
        ack = await servicer.PublishBatch(batch, MagicMock())

        assert ack.events_accepted == 0
        assert "db down" in ack.error


class TestIngestionServicerPublishUpdate:
    @pytest.mark.asyncio
    async def test_publish_update_persists_via_target(self):
        decoder = MagicMock()
        decoder.decode_bytes.return_value = [
            DecodedEvent(timestamp=None, event_code=1, event_param=2),
        ]
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock()
        servicer = _make_servicer(
            decoder=decoder, target=target, registered={"INT-042"},
        )

        atspm = ihr_events_pb2.AtspmEvent(
            code=ihr_events_pb2.EVENT_PHASE_BEGIN_GREEN, param=2,
        )
        update = common_pb2.IntersectionUpdate(
            intersection_id="INT-042", event=atspm,
        )

        ack = await servicer.PublishUpdate(update, MagicMock())
        assert ack.events_accepted == 1
        assert ack.error == ""
        target.persist_with_drift_check.assert_awaited_once()
        assert target.persist_with_drift_check.call_args[0][1] == "INT-042"

    @pytest.mark.asyncio
    async def test_publish_update_rejects_unregistered(self):
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock()
        servicer = _make_servicer(target=target, registered={"INT-042"})

        update = common_pb2.IntersectionUpdate(intersection_id="UNKNOWN")
        ack = await servicer.PublishUpdate(update, MagicMock())

        assert ack.events_accepted == 0
        assert "unregistered" in ack.error
        target.persist_with_drift_check.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_update_no_events_returns_zero(self):
        decoder = MagicMock()
        decoder.decode_bytes.return_value = []
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock()
        servicer = _make_servicer(
            decoder=decoder, target=target, registered={"INT-empty"},
        )

        update = common_pb2.IntersectionUpdate(intersection_id="INT-empty")
        ack = await servicer.PublishUpdate(update, MagicMock())

        assert ack.events_accepted == 0
        target.persist_with_drift_check.assert_not_called()


class TestIngestionServicerStreamBatches:
    @pytest.mark.asyncio
    async def test_stream_batches_sums_accepted_events(self):
        decoder = MagicMock()
        decoder.decode_bytes.side_effect = [
            [DecodedEvent(timestamp=None, event_code=82, event_param=1),
             DecodedEvent(timestamp=None, event_code=82, event_param=2)],
            [DecodedEvent(timestamp=None, event_code=82, event_param=3),
             DecodedEvent(timestamp=None, event_code=82, event_param=4),
             DecodedEvent(timestamp=None, event_code=82, event_param=5)],
        ]
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock()
        servicer = _make_servicer(
            decoder=decoder, target=target, registered={"INT-A", "INT-B"},
        )

        b1 = common_pb2.CompactEventBatch(intersection_id="INT-A")
        b2 = common_pb2.CompactEventBatch(intersection_id="INT-B")

        async def request_iter():
            yield b1
            yield b2

        ack = await servicer.StreamBatches(request_iter(), MagicMock())

        assert ack.events_accepted == 5
        assert ack.error == ""
        assert target.persist_with_drift_check.await_count == 2

    @pytest.mark.asyncio
    async def test_stream_batches_continues_after_per_batch_failure(self):
        decoder = MagicMock()
        decoder.decode_bytes.side_effect = [
            [DecodedEvent(timestamp=None, event_code=82, event_param=1)],
            ValueError("bad batch"),
            [DecodedEvent(timestamp=None, event_code=82, event_param=2)],
        ]
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock()
        servicer = _make_servicer(
            decoder=decoder, target=target,
            registered={"INT-A", "INT-B", "INT-C"},
        )

        b1 = common_pb2.CompactEventBatch(intersection_id="INT-A")
        b2 = common_pb2.CompactEventBatch(intersection_id="INT-B")
        b3 = common_pb2.CompactEventBatch(intersection_id="INT-C")

        async def request_iter():
            yield b1
            yield b2
            yield b3

        ack = await servicer.StreamBatches(request_iter(), MagicMock())
        assert ack.events_accepted == 2

    @pytest.mark.asyncio
    async def test_stream_batches_skips_unregistered(self):
        decoder = MagicMock()
        decoder.decode_bytes.return_value = [
            DecodedEvent(timestamp=None, event_code=82, event_param=1),
        ]
        target = ControllerTarget()
        target.persist_with_drift_check = AsyncMock()
        servicer = _make_servicer(
            decoder=decoder, target=target, registered={"INT-A"},
        )

        b1 = common_pb2.CompactEventBatch(intersection_id="INT-A")
        b2 = common_pb2.CompactEventBatch(intersection_id="INT-UNKNOWN")

        async def request_iter():
            yield b1
            yield b2

        ack = await servicer.StreamBatches(request_iter(), MagicMock())
        # Unregistered batch is skipped; only b1 contributes events.
        assert ack.events_accepted == 1


class TestGRPCServerLifecycle:
    @pytest.mark.asyncio
    async def test_stop_when_never_started(self):
        method = GRPCServerMethod()
        await method.stop()
        assert method._server is None

    @pytest.mark.asyncio
    async def test_start_insecure_then_stop(self):
        """Start a real server on an ephemeral port with one device, then stop."""
        method = GRPCServerMethod()
        await method.start(
            {"port": 0},
            AsyncMock(),
            target=ControllerTarget(),
            devices=[("INT-001", {})],
        )
        try:
            assert method._server is not None
            assert await method.health_check() is True
        finally:
            await method.stop()
        assert method._server is None
        assert await method.health_check() is False

    @pytest.mark.asyncio
    async def test_start_with_roadside_target(self):
        """Roadside target plumbs through to the servicer."""
        method = GRPCServerMethod()
        target = RoadsideTarget()
        await method.start(
            {"port": 0},
            AsyncMock(),
            target=target,
            devices=[("SENSOR-A", {})],
        )
        try:
            assert method._server is not None
        finally:
            await method.stop()

    @pytest.mark.asyncio
    async def test_start_with_no_devices_warns(self, caplog):
        """Starting with empty devices list logs a warning but boots."""
        import logging
        method = GRPCServerMethod()
        with caplog.at_level(
            logging.WARNING, logger="tsigma.collection.methods.grpc_server",
        ):
            await method.start(
                {"port": 0},
                AsyncMock(),
                target=ControllerTarget(),
                devices=[],
            )
        try:
            assert method._server is not None
            assert "no registered" in caplog.text.lower()
        finally:
            await method.stop()
