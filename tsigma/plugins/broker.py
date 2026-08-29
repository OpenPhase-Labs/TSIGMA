"""Broker consume-side - host-served callbacks a plugin dials back.

PROTOCOL.md section 4: the single host<->plugin connection carries a `GRPCBroker`
that brokers additional connections in both directions. Implement-side is the host
calling the plugin; consume-side is the plugin dialling BACK to host-served
services over the same connection.

The plugin never receives database credentials or schema. It asks a host broker
service for what it needs and the host answers from its own session, under the
request's tenant context - that host-mediated data plane, not a sandbox, is what
contains an untrusted plugin.
"""

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from grpc import aio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .protocol import grpc_broker_pb2, grpc_broker_pb2_grpc

logger = logging.getLogger(__name__)

# Broker stream service ids are allocated by the host; go-plugin reserves low
# numbers for its own use, so host-served callbacks start above them.
FIRST_BROKER_SERVICE_ID = 100


@asynccontextmanager
async def scoped_session_for_plugin(
    session_factory: Callable[[], AsyncSession],
    username: str | None,
) -> AsyncIterator[AsyncSession]:
    """A FRESH session per broker invocation, scoped to the calling user.

    Deliberately not the originating request's session object: broker callbacks
    from one plugin can overlap, and sharing a session would let concurrent
    callbacks alias one transaction. Each gets its own, with app.current_user set
    transaction-locally so audit triggers attribute correctly.
    """
    from ..database.db import get_db_facade

    async with session_factory() as session:
        if username:
            sql = get_db_facade().dialect.set_app_user_sql()
            await session.execute(text(sql), {"username": username})
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


class BrokerServer:
    """Host-side broker: serves callback services and advertises them to plugins.

    The host stands up a gRPC server carrying the consume-side services, then
    hands the plugin its address over the GRPCBroker stream. The plugin dials
    that address; it never learns anything about the database behind it.
    """

    def __init__(self, host: str = "127.0.0.1"):
        self.host = host
        self._server: aio.Server | None = None
        self._port: int | None = None
        self._registrars: list[Callable[[aio.Server], None]] = []
        self._next_service_id = FIRST_BROKER_SERVICE_ID
        self._service_ids: dict[str, int] = {}

    def add_service(self, name: str, registrar: Callable[[aio.Server], None]) -> int:
        """Register a host-served callback service. Returns its broker service id."""
        if self._server is not None:
            raise RuntimeError("add_service must be called before start()")
        self._registrars.append(registrar)
        service_id = self._next_service_id
        self._next_service_id += 1
        self._service_ids[name] = service_id
        return service_id

    def service_id(self, name: str) -> int:
        return self._service_ids[name]

    @property
    def address(self) -> str:
        if self._port is None:
            raise RuntimeError("broker not started")
        return f"{self.host}:{self._port}"

    async def start(self) -> str:
        """Bind and serve the registered callback services."""
        self._server = aio.server()
        for registrar in self._registrars:
            registrar(self._server)
        self._port = self._server.add_insecure_port(f"{self.host}:0")
        await self._server.start()
        logger.debug("broker serving %d service(s) on %s", len(self._registrars), self.address)
        return self.address

    def conn_info(self, name: str) -> grpc_broker_pb2.ConnInfo:
        """The ConnInfo a plugin needs to dial one host-served service."""
        return grpc_broker_pb2.ConnInfo(
            service_id=self.service_id(name),
            network="tcp",
            address=self.address,
        )

    async def stop(self, grace: float | None = None) -> None:
        if self._server is not None:
            await self._server.stop(grace)
            self._server = None
            self._port = None


class BrokerServicer(grpc_broker_pb2_grpc.GRPCBrokerServicer):
    """Serves GRPCBroker.StartStream, handing out host callback addresses.

    go-plugin's broker is a bidi stream of ConnInfo. A plugin knocks with a
    service id; the host answers with the address to dial and acks the knock.
    """

    def __init__(self, broker: BrokerServer):
        self._broker = broker
        self._by_id = {v: k for k, v in broker._service_ids.items()}

    async def StartStream(self, request_iterator, context):
        async for conn_info in request_iterator:
            name = self._by_id.get(conn_info.service_id)
            if name is None:
                yield grpc_broker_pb2.ConnInfo(
                    service_id=conn_info.service_id,
                    knock=grpc_broker_pb2.ConnInfo.Knock(
                        knock=True,
                        ack=False,
                        error=f"unknown broker service id {conn_info.service_id}",
                    ),
                )
                continue
            answer = self._broker.conn_info(name)
            answer.knock.knock = True
            answer.knock.ack = True
            yield answer
