"""
Unit tests for gRPC server ingestion method plugin.

Tests registration, health checks, server lifecycle, RPC handling
(PublishUpdate / PublishBatch / StreamBatches), and TLS path.
"""

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

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


class TestGRPCServerRegistration:
    """Tests for registry integration."""

    def test_registered(self):
        cls = IngestionMethodRegistry.get("grpc_server")
        assert cls is GRPCServerMethod


class TestGRPCServerHealthCheck:
    """Tests for health_check behavior."""

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
    """Tests for config validation defaults."""

    def test_defaults(self):
        cfg = GRPCServerConfig()
        assert cfg.port == 50051
        assert cfg.bind_address == "0.0.0.0"
        assert cfg.decoder == "openphase"
        assert cfg.tls_cert_file is None
        assert cfg.tls_key_file is None
        assert cfg.max_message_size_bytes == 4 * 1024 * 1024

    def test_overrides(self):
        cfg = GRPCServerConfig(port=12345, decoder="auto", max_message_size_bytes=10_000)
        assert cfg.port == 12345
        assert cfg.decoder == "auto"
        assert cfg.max_message_size_bytes == 10_000


class TestIngestionServicerPublishBatch:
    """Tests for PublishBatch RPC handler."""

    @pytest.mark.asyncio
    async def test_publish_batch_persists_events(self):
        decoder = MagicMock()
        decoder.decode_bytes.return_value = [
            DecodedEvent(timestamp=None, event_code=82, event_param=5),
        ]
        session_factory = AsyncMock()
        servicer = _IngestionServicer(decoder, session_factory)

        batch = common_pb2.CompactEventBatch(
            intersection_id="INT-001",
            base_timestamp_ns=1_700_000_000_000_000_000,
        )
        evt = batch.events.add()
        evt.offset_ms = 0
        evt.code = ihr_events_pb2.EVENT_DETECTOR_ON
        evt.param = 5

        with patch(
            "tsigma.collection.methods.grpc_server.persist_events_with_drift_check",
            new_callable=AsyncMock,
        ) as persist:
            ack = await servicer.PublishBatch(batch, MagicMock())

        assert ack.events_accepted == 1
        assert ack.error == ""
        persist.assert_called_once()
        # signal_id is intersection_id from the batch
        assert persist.call_args[0][1] == "INT-001"

    @pytest.mark.asyncio
    async def test_publish_batch_decode_failure_returns_error(self):
        decoder = MagicMock()
        decoder.decode_bytes.side_effect = ValueError("bad bytes")
        session_factory = AsyncMock()
        servicer = _IngestionServicer(decoder, session_factory)

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
        session_factory = AsyncMock()
        servicer = _IngestionServicer(decoder, session_factory)

        batch = common_pb2.CompactEventBatch(intersection_id="INT-003")

        with patch(
            "tsigma.collection.methods.grpc_server.persist_events_with_drift_check",
            new_callable=AsyncMock,
            side_effect=RuntimeError("db down"),
        ):
            ack = await servicer.PublishBatch(batch, MagicMock())

        assert ack.events_accepted == 0
        assert "db down" in ack.error


class TestIngestionServicerPublishUpdate:
    """Tests for PublishUpdate RPC handler."""

    @pytest.mark.asyncio
    async def test_publish_update_persists_events(self):
        decoder = MagicMock()
        decoder.decode_bytes.return_value = [
            DecodedEvent(timestamp=None, event_code=1, event_param=2),
        ]
        session_factory = AsyncMock()
        servicer = _IngestionServicer(decoder, session_factory)

        atspm = ihr_events_pb2.AtspmEvent(
            code=ihr_events_pb2.EVENT_PHASE_BEGIN_GREEN, param=2
        )
        update = common_pb2.IntersectionUpdate(intersection_id="INT-042", event=atspm)

        with patch(
            "tsigma.collection.methods.grpc_server.persist_events_with_drift_check",
            new_callable=AsyncMock,
        ) as persist:
            ack = await servicer.PublishUpdate(update, MagicMock())

        assert ack.events_accepted == 1
        assert ack.error == ""
        persist.assert_called_once()
        assert persist.call_args[0][1] == "INT-042"

    @pytest.mark.asyncio
    async def test_publish_update_no_events_returns_zero(self):
        decoder = MagicMock()
        decoder.decode_bytes.return_value = []
        session_factory = AsyncMock()
        servicer = _IngestionServicer(decoder, session_factory)

        update = common_pb2.IntersectionUpdate(intersection_id="INT-empty")

        with patch(
            "tsigma.collection.methods.grpc_server.persist_events_with_drift_check",
            new_callable=AsyncMock,
        ) as persist:
            ack = await servicer.PublishUpdate(update, MagicMock())

        assert ack.events_accepted == 0
        persist.assert_not_called()


class TestIngestionServicerStreamBatches:
    """Tests for StreamBatches client-streaming RPC."""

    @pytest.mark.asyncio
    async def test_stream_batches_sums_accepted_events(self):
        decoder = MagicMock()
        # Two batches in the stream, decoder returns 2 events for the first,
        # 3 for the second.
        decoder.decode_bytes.side_effect = [
            [DecodedEvent(timestamp=None, event_code=82, event_param=1),
             DecodedEvent(timestamp=None, event_code=82, event_param=2)],
            [DecodedEvent(timestamp=None, event_code=82, event_param=3),
             DecodedEvent(timestamp=None, event_code=82, event_param=4),
             DecodedEvent(timestamp=None, event_code=82, event_param=5)],
        ]
        session_factory = AsyncMock()
        servicer = _IngestionServicer(decoder, session_factory)

        b1 = common_pb2.CompactEventBatch(intersection_id="INT-A")
        b2 = common_pb2.CompactEventBatch(intersection_id="INT-B")

        async def request_iter():
            yield b1
            yield b2

        with patch(
            "tsigma.collection.methods.grpc_server.persist_events_with_drift_check",
            new_callable=AsyncMock,
        ) as persist:
            ack = await servicer.StreamBatches(request_iter(), MagicMock())

        assert ack.events_accepted == 5
        assert ack.error == ""
        assert persist.await_count == 2

    @pytest.mark.asyncio
    async def test_stream_batches_continues_after_per_batch_failure(self):
        decoder = MagicMock()
        # First batch decodes fine, second blows up; third decodes fine.
        decoder.decode_bytes.side_effect = [
            [DecodedEvent(timestamp=None, event_code=82, event_param=1)],
            ValueError("bad batch"),
            [DecodedEvent(timestamp=None, event_code=82, event_param=2)],
        ]
        session_factory = AsyncMock()
        servicer = _IngestionServicer(decoder, session_factory)

        b1 = common_pb2.CompactEventBatch(intersection_id="INT-A")
        b2 = common_pb2.CompactEventBatch(intersection_id="INT-B")
        b3 = common_pb2.CompactEventBatch(intersection_id="INT-C")

        async def request_iter():
            yield b1
            yield b2
            yield b3

        with patch(
            "tsigma.collection.methods.grpc_server.persist_events_with_drift_check",
            new_callable=AsyncMock,
        ):
            ack = await servicer.StreamBatches(request_iter(), MagicMock())

        # First and third batches succeed; total = 2 accepted.
        assert ack.events_accepted == 2


class TestGRPCServerLifecycle:
    """Tests for start() and stop()."""

    @pytest.mark.asyncio
    async def test_stop_when_never_started(self):
        method = GRPCServerMethod()
        # Should not raise.
        await method.stop()
        assert method._server is None

    @pytest.mark.asyncio
    async def test_start_insecure_then_stop(self):
        """Start an actual gRPC server on an ephemeral port, then stop it."""
        method = GRPCServerMethod()
        session_factory = AsyncMock()

        await method.start({"port": 0}, session_factory)
        try:
            assert method._server is not None
            assert await method.health_check() is True
        finally:
            await method.stop()

        assert method._server is None
        assert await method.health_check() is False
