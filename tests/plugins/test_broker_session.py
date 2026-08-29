"""Phase 4 gate: host-served broker callbacks and per-invocation sessions.

The dial-back tests run a real plugin subprocess that opens a SECOND connection
back to a host-served service - the consume-side of PROTOCOL.md section 4. The
session tests pin the property that makes it safe: every broker invocation gets
its own session, so overlapping callbacks never share a transaction.
"""

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from grpc_health.v1 import health, health_pb2_grpc

from tsigma.plugins.broker import (
    FIRST_BROKER_SERVICE_ID,
    BrokerServer,
    BrokerServicer,
    scoped_session_for_plugin,
)
from tsigma.plugins.protocol import PluginProcess

FAKE_PLUGIN = str(Path(__file__).parent / "fake_plugin.py")


def _command(*args: str) -> list[str]:
    return [sys.executable, FAKE_PLUGIN, *args]


def _serve_health(server):
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)


@pytest_asyncio.fixture
async def broker():
    b = BrokerServer()
    yield b
    await b.stop(grace=None)


class TestBrokerServer:
    @pytest.mark.asyncio
    async def test_serves_a_registered_callback(self, broker):
        broker.add_service("health", _serve_health)
        address = await broker.start()
        assert address.startswith("127.0.0.1:")
        assert broker.address == address

    def test_service_ids_start_above_go_plugin_reserved_range(self):
        b = BrokerServer()
        assert b.add_service("a", _serve_health) == FIRST_BROKER_SERVICE_ID
        assert b.add_service("b", _serve_health) == FIRST_BROKER_SERVICE_ID + 1

    def test_address_before_start_is_an_error(self):
        with pytest.raises(RuntimeError, match="not started"):
            BrokerServer().address

    @pytest.mark.asyncio
    async def test_services_cannot_be_added_after_start(self, broker):
        await broker.start()
        with pytest.raises(RuntimeError, match="before start"):
            broker.add_service("late", _serve_health)

    @pytest.mark.asyncio
    async def test_conn_info_describes_the_dialable_address(self, broker):
        broker.add_service("health", _serve_health)
        await broker.start()
        info = broker.conn_info("health")
        assert info.service_id == broker.service_id("health")
        assert info.network == "tcp"
        assert info.address == broker.address


class TestConsumeSide:
    """The plugin dials BACK to the host - the whole point of the broker."""

    @pytest.mark.asyncio
    async def test_plugin_reaches_a_host_served_callback(self, broker):
        broker.add_service("health", _serve_health)
        address = await broker.start()

        plugin = PluginProcess("dialer", _command("--dial-back", address))
        await plugin.launch()
        try:
            # The plugin reports SERVING only if its callback to us succeeded.
            assert await plugin.is_healthy() is True
        finally:
            await plugin.shutdown()

    @pytest.mark.asyncio
    async def test_plugin_reports_unhealthy_when_the_callback_is_unreachable(self):
        plugin = PluginProcess("dialer", _command("--dial-back", "127.0.0.1:1"))
        await plugin.launch()
        try:
            assert await plugin.is_healthy() is False
        finally:
            await plugin.shutdown()


class TestBrokerServicer:
    @pytest.mark.asyncio
    async def test_knock_is_acked_with_the_address(self, broker):
        broker.add_service("health", _serve_health)
        await broker.start()
        servicer = BrokerServicer(broker)

        from tsigma.plugins.protocol import grpc_broker_pb2

        async def knocks():
            yield grpc_broker_pb2.ConnInfo(service_id=broker.service_id("health"))

        replies = [r async for r in servicer.StartStream(knocks(), MagicMock())]
        assert len(replies) == 1
        assert replies[0].knock.ack is True
        assert replies[0].address == broker.address

    @pytest.mark.asyncio
    async def test_unknown_service_id_is_refused_not_crashed(self, broker):
        broker.add_service("health", _serve_health)
        await broker.start()
        servicer = BrokerServicer(broker)

        from tsigma.plugins.protocol import grpc_broker_pb2

        async def knocks():
            yield grpc_broker_pb2.ConnInfo(service_id=9999)

        replies = [r async for r in servicer.StartStream(knocks(), MagicMock())]
        assert replies[0].knock.ack is False
        assert "unknown broker service id" in replies[0].knock.error


class TestScopedSession:
    """Each broker invocation gets its own session, scoped to the caller."""

    @staticmethod
    def _factory():
        """A session factory whose sessions are distinguishable."""
        made = []

        def factory():
            session = AsyncMock()
            session.__aenter__.return_value = session
            session.__aexit__.return_value = False
            made.append(session)
            return session

        return factory, made

    @pytest.mark.asyncio
    async def test_sets_the_calling_user_for_audit_attribution(self):
        factory, made = self._factory()
        facade = MagicMock()
        facade.dialect.set_app_user_sql.return_value = "SET LOCAL app.current_user = :username"
        with patch("tsigma.database.db.get_db_facade", return_value=facade):
            async with scoped_session_for_plugin(factory, "alice") as session:
                assert session is made[0]
        args = made[0].execute.call_args
        assert args[0][1] == {"username": "alice"}

    @pytest.mark.asyncio
    async def test_anonymous_call_sets_no_user(self):
        factory, made = self._factory()
        async with scoped_session_for_plugin(factory, None):
            pass
        made[0].execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_each_invocation_gets_a_distinct_session(self):
        """Overlapping callbacks must never alias one transaction."""
        factory, made = self._factory()
        async with scoped_session_for_plugin(factory, None) as first:
            async with scoped_session_for_plugin(factory, None) as second:
                assert first is not second
        assert len(made) == 2

    @pytest.mark.asyncio
    async def test_commits_on_success(self):
        factory, made = self._factory()
        async with scoped_session_for_plugin(factory, None):
            pass
        made[0].commit.assert_awaited_once()
        made[0].rollback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rolls_back_and_propagates_on_error(self):
        factory, made = self._factory()
        with pytest.raises(ValueError):
            async with scoped_session_for_plugin(factory, None):
                raise ValueError("plugin callback blew up")
        made[0].rollback.assert_awaited_once()
        made[0].commit.assert_not_awaited()
