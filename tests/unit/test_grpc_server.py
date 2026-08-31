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


from tsigma.collection.methods.grpc_server import (  # noqa: E402
    GRPCServerMethod,
    _IngestionServicer,
)
from tsigma.collection.registry import IngestionMethodRegistry  # noqa: E402
from tsigma.collection.targets import (  # noqa: E402
    ControllerTarget,
    RoadsideTarget,
)


def _make_servicer(
    decoder_name="openphase",
    target=None,
    registered=None,
    session_factory=None,
):
    """Build an _IngestionServicer with sensible defaults for tests."""
    return _IngestionServicer(
        decoder_name=decoder_name,
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
