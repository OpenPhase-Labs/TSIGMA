"""A minimal conforming report plugin, run as a real subprocess in tests.

Serves tsigma.report.v1.Report alongside health and controller, so the whole
vertical slice - host calls Generate, plugin streams a ViewModel plus Arrow
batches, host reassembles a DataFrame - runs over a real connection.

    fake_report_plugin.py                conforming; 3 rows in 2 batches
    fake_report_plugin.py --empty        ViewModel(empty=True), no batches
    fake_report_plugin.py --status 422   sets ViewModel.preferred_http_status
    fake_report_plugin.py --no-viewmodel streams a batch with no ViewModel first
    fake_report_plugin.py --two-viewmodels streams two ViewModels
"""

import asyncio
import os
import sys

import pandas as pd
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
from tsigma.plugins.remote_report import dataframe_to_arrow_batch
from tsigma.report.v1 import report_pb2, report_pb2_grpc

COLUMNS = ["phase", "delay_seconds"]


class Controller(grpc_controller_pb2_grpc.GRPCControllerServicer):
    def __init__(self, stop: asyncio.Event):
        self._stop = stop

    async def Shutdown(self, request, context):
        self._stop.set()
        return grpc_controller_pb2.Empty()


class FakeReport(report_pb2_grpc.ReportServicer):
    def __init__(self, args: set[str], status: int):
        self._args = args
        self._status = status

    async def Describe(self, request, context):
        return report_pb2.DescribeResponse(
            name="fake-delay",
            description="A fake report used to prove the remote slice",
            category=report_pb2.STANDARD,
            estimated_time=report_pb2.FAST,
            supports_export=True,
            export_formats=["csv", "json"],
        )

    async def Generate(self, request, context):
        if "--no-viewmodel" in self._args:
            yield report_pb2.GenerateResult(
                rows_arrow_ipc=dataframe_to_arrow_batch(pd.DataFrame({c: [] for c in COLUMNS}))
            )
            return

        empty = "--empty" in self._args
        yield report_pb2.GenerateResult(
            view_model=report_pb2.ViewModel(
                columns=COLUMNS, empty=empty, preferred_http_status=self._status
            )
        )
        if "--two-viewmodels" in self._args:
            yield report_pb2.GenerateResult(
                view_model=report_pb2.ViewModel(columns=COLUMNS, empty=empty)
            )
            return
        if empty:
            return

        # Two batches, to prove reassembly rather than a single-shot payload.
        yield report_pb2.GenerateResult(
            rows_arrow_ipc=dataframe_to_arrow_batch(
                pd.DataFrame({"phase": [2, 4], "delay_seconds": [12.5, 7.25]})
            )
        )
        yield report_pb2.GenerateResult(
            rows_arrow_ipc=dataframe_to_arrow_batch(
                pd.DataFrame({"phase": [6], "delay_seconds": [3.0]})
            )
        )


async def main() -> int:
    if os.environ.get(constants.MAGIC_COOKIE_KEY) != constants.MAGIC_COOKIE_VALUE:
        print("missing or wrong magic cookie", file=sys.stderr)
        return 1

    args = set(sys.argv[1:])
    status = 0
    if "--status" in sys.argv:
        status = int(sys.argv[sys.argv.index("--status") + 1])

    stop = asyncio.Event()
    server = aio.server()
    servicer = health_aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    grpc_controller_pb2_grpc.add_GRPCControllerServicer_to_server(Controller(stop), server)
    report_pb2_grpc.add_ReportServicer_to_server(FakeReport(args, status), server)

    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    await servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    print(
        format_handshake_line(
            HandshakeConfig(
                constants.CORE_PROTOCOL_VERSION,
                constants.PLUGIN_PROTOCOL_VERSION,
                "tcp",
                f"127.0.0.1:{port}",
                "grpc",
            )
        ),
        flush=True,
    )
    await stop.wait()
    await server.stop(grace=None)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
