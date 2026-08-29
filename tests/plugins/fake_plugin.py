"""A minimal conforming TSIGMA plugin, used as a real subprocess in tests.

Serves grpc.health.v1 and GRPCController, prints one handshake line, and exits
on Shutdown. Behaviour is switchable by argv so tests can drive the failure paths:

    fake_plugin.py                 conforming plugin
    fake_plugin.py --bad-line      prints a malformed handshake
    fake_plugin.py --no-line       serves but never prints a handshake
    fake_plugin.py --unhealthy     handshakes, reports NOT_SERVING
    fake_plugin.py --dial-back ADDR dials ADDR (a host broker address) and reports
                                   SERVING only if the callback succeeded
"""

import asyncio
import os
import sys

from grpc import aio
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_health.v1.health import aio as health_aio

from tsigma.plugins import constants
from tsigma.plugins.protocol import (
    HandshakeConfig,
    format_handshake_line,
    grpc_controller_pb2,
    grpc_controller_pb2_grpc,
)


class Controller(grpc_controller_pb2_grpc.GRPCControllerServicer):
    def __init__(self, stop: asyncio.Event):
        self._stop = stop

    async def Shutdown(self, request, context):
        self._stop.set()
        return grpc_controller_pb2.Empty()


async def main() -> int:
    # Refuse to run as a normal program (PROTOCOL.md section 1).
    if os.environ.get(constants.MAGIC_COOKIE_KEY) != constants.MAGIC_COOKIE_VALUE:
        print("missing or wrong magic cookie", file=sys.stderr)
        return 1

    args = set(sys.argv[1:])
    stop = asyncio.Event()
    server = aio.server()

    servicer = health_aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    grpc_controller_pb2_grpc.add_GRPCControllerServicer_to_server(Controller(stop), server)

    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    status = (
        health_pb2.HealthCheckResponse.NOT_SERVING
        if "--unhealthy" in args
        else health_pb2.HealthCheckResponse.SERVING
    )

    # Consume-side proof: dial BACK to a host-served broker service over a second
    # connection. Reporting SERVING is how the host observes that it worked.
    if "--dial-back" in sys.argv:
        target = sys.argv[sys.argv.index("--dial-back") + 1]
        try:
            async with aio.insecure_channel(target) as back:
                stub = health_pb2_grpc.HealthStub(back)
                reply = await stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=5)
            ok = reply.status == health_pb2.HealthCheckResponse.SERVING
        except Exception as exc:
            print(f"dial-back failed: {exc}", file=sys.stderr)
            ok = False
        status = (
            health_pb2.HealthCheckResponse.SERVING
            if ok
            else health_pb2.HealthCheckResponse.NOT_SERVING
        )
    await servicer.set("", status)

    if "--bad-line" in args:
        print("this is not a handshake", flush=True)
    elif "--no-line" not in args:
        line = format_handshake_line(
            HandshakeConfig(
                core_version=constants.CORE_PROTOCOL_VERSION,
                app_version=constants.PLUGIN_PROTOCOL_VERSION,
                network="tcp",
                address=f"127.0.0.1:{port}",
                protocol="grpc",
            )
        )
        print(line, flush=True)

    await stop.wait()
    await server.stop(grace=None)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
