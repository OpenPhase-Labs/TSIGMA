"""Phase 3 gate: the connection seam and ADR-0019's three process models.

Every mode is exercised against a real plugin subprocess where it has one, so a
mode that only works on paper fails here rather than in deployment.
"""

import sys
from pathlib import Path

import pytest

from tsigma.plugins.connection import (
    DiscoveredConnection,
    LaunchedConnection,
    PluginConnection,
    ProcessModel,
    ScheduledConnection,
)
from tsigma.plugins.protocol import HandshakeConfig, PluginProcess

FAKE_PLUGIN = str(Path(__file__).parent / "fake_plugin.py")


def _command(*args: str) -> list[str]:
    return [sys.executable, FAKE_PLUGIN, *args]


class TestSeamConformance:
    """Whatever the mode, callers above the seam see one interface."""

    @pytest.mark.parametrize(
        "conn",
        [
            LaunchedConnection("a", ["x"]),
            DiscoveredConnection("b", HandshakeConfig(1, 1, "tcp", "127.0.0.1:9", "grpc")),
            ScheduledConnection("c", ["x"]),
        ],
        ids=["launched", "discovered", "scheduled"],
    )
    def test_satisfies_the_protocol(self, conn):
        assert isinstance(conn, PluginConnection)

    def test_process_models_are_distinct(self):
        models = {
            LaunchedConnection("a", ["x"]).process_model,
            DiscoveredConnection("b", HandshakeConfig(1, 1, "tcp", "h:1", "grpc")).process_model,
            ScheduledConnection("c", ["x"]).process_model,
        }
        assert models == {ProcessModel.CHILD, ProcessModel.EXTERNAL, ProcessModel.CRON}

    def test_only_the_host_managed_modes_own_lifecycle(self):
        # The distinction supervision depends on: restart-on-crash is mode 1/3 only.
        assert LaunchedConnection("a", ["x"]).host_owns_lifecycle is True
        assert ScheduledConnection("c", ["x"]).host_owns_lifecycle is True
        hs = HandshakeConfig(1, 1, "tcp", "h:1", "grpc")
        assert DiscoveredConnection("b", hs).host_owns_lifecycle is False


class TestLaunchedConnection:
    @pytest.mark.asyncio
    async def test_full_lifecycle(self):
        conn = LaunchedConnection("fake", _command())
        handshake = await conn.connect()
        try:
            assert handshake.network == "tcp"
            assert conn.channel is not None
            assert await conn.is_healthy() is True
        finally:
            await conn.shutdown()
        assert conn.channel is None


class TestDiscoveredConnection:
    """Mode 2: the plugin is already running; the host only dials it."""

    @pytest.mark.asyncio
    async def test_connects_to_an_already_running_plugin(self):
        # Stand the plugin up as if an orchestrator had, then discover it.
        running = PluginProcess("external", _command())
        advertised = await running.launch()
        try:
            conn = DiscoveredConnection("external", advertised)
            handshake = await conn.connect()
            assert handshake == advertised
            assert await conn.is_healthy() is True

            # Host shutdown must NOT stop an externally-orchestrated plugin.
            await conn.shutdown()
            assert conn.channel is None
            assert running.process.returncode is None
            assert await running.is_healthy() is True
        finally:
            await running.shutdown()

    @pytest.mark.asyncio
    async def test_unreachable_plugin_is_unhealthy_not_an_error(self):
        conn = DiscoveredConnection(
            "gone", HandshakeConfig(1, 1, "tcp", "127.0.0.1:1", "grpc")
        )
        await conn.connect()
        try:
            assert await conn.is_healthy(timeout=1.0) is False
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_health_is_false_before_connect(self):
        hs = HandshakeConfig(1, 1, "tcp", "127.0.0.1:1", "grpc")
        assert await DiscoveredConnection("x", hs).is_healthy() is False


class TestScheduledConnection:
    """Mode 3: a run is started, does its work, and exits."""

    @pytest.mark.asyncio
    async def test_run_then_exit(self):
        conn = ScheduledConnection("cron", _command())
        assert conn.running is False
        assert await conn.is_healthy() is False  # idle, not unhealthy

        await conn.connect()
        assert conn.running is True
        assert await conn.is_healthy() is True

        await conn.shutdown()
        assert conn.running is False
        assert conn.channel is None

    @pytest.mark.asyncio
    async def test_successive_runs_are_independent(self):
        conn = ScheduledConnection("cron", _command())
        await conn.connect()
        first = conn.handshake.address
        await conn.shutdown()

        await conn.connect()
        second = conn.handshake.address
        await conn.shutdown()

        # A fresh process each run - the port must not be carried over.
        assert first != second
