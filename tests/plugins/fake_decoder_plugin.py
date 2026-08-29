"""A minimal conforming decoder plugin, run as a real subprocess in tests.

    fake_decoder_plugin.py                 3 events in 2 batches, SUCCESS
    fake_decoder_plugin.py --partial       2 events then PARTIAL, 1 segment dropped
    fake_decoder_plugin.py --failure       no events, FAILURE with an error
    fake_decoder_plugin.py --no-status     batches then closes without a status
    fake_decoder_plugin.py --two-status    two terminal statuses
    fake_decoder_plugin.py --batch-after   a batch AFTER the terminal status
    fake_decoder_plugin.py --with-metadata emits a FileMetadata first
    fake_decoder_plugin.py --cannot        CanDecode returns false
    fake_decoder_plugin.py --naive-local   declares TIME_SEMANTICS_NAIVE_LOCAL
"""

import asyncio
import os
import sys
from datetime import datetime

from grpc import aio
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_health.v1.health import aio as health_aio

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.decoder.v1 import decoder_pb2, decoder_pb2_grpc
from tsigma.plugins import constants
from tsigma.plugins.protocol import (
    HandshakeConfig,
    format_handshake_line,
    grpc_controller_pb2,
    grpc_controller_pb2_grpc,
)
from tsigma.plugins.remote_decoder import events_to_arrow_batch

BASE = datetime(2026, 8, 1, 12, 0, 0)


class Controller(grpc_controller_pb2_grpc.GRPCControllerServicer):
    def __init__(self, stop):
        self._stop = stop

    async def Shutdown(self, request, context):
        self._stop.set()
        return grpc_controller_pb2.Empty()


class FakeDecoder(decoder_pb2_grpc.DecoderServicer):
    def __init__(self, args: set[str]):
        self._args = args
        self.head_seen: bytes = b""
        self.bytes_received = 0

    async def Describe(self, request, context):
        naive = "--naive-local" in self._args
        return decoder_pb2.DescribeResponse(
            name="fake-asc3",
            extensions=[".dat"],
            description="A fake decoder used to prove the remote slice",
            output_kind=decoder_pb2.EVENTS,
            time_semantics=(
                decoder_pb2.TIME_SEMANTICS_NAIVE_LOCAL
                if naive
                else decoder_pb2.TIME_SEMANTICS_UTC
            ),
        )

    async def CanDecode(self, request, context):
        self.head_seen = request.head
        return decoder_pb2.CanDecodeResponse(can_decode="--cannot" not in self._args)

    async def Decode(self, request_iterator, context):
        async for chunk in request_iterator:
            self.bytes_received += len(chunk.chunk)

        if "--with-metadata" in self._args:
            meta = decoder_pb2.FileMetadata(device_ip="10.0.0.1", log_version="3.2")
            meta.log_begin.FromDatetime(BASE)
            yield decoder_pb2.DecodeResult(metadata=meta)

        if "--failure" in self._args:
            yield decoder_pb2.DecodeResult(
                status=decoder_pb2.DecodeStatus(
                    outcome=decoder_pb2.DECODE_OUTCOME_FAILURE,
                    events_emitted=0,
                    error="unreadable header",
                )
            )
            return

        first = [
            DecodedEvent(timestamp=BASE, event_code=1, event_param=2),
            DecodedEvent(timestamp=BASE.replace(second=30), event_code=8, event_param=2),
        ]
        yield decoder_pb2.DecodeResult(events_arrow_ipc=events_to_arrow_batch(first))

        if "--partial" in self._args:
            yield decoder_pb2.DecodeResult(
                status=decoder_pb2.DecodeStatus(
                    outcome=decoder_pb2.DECODE_OUTCOME_PARTIAL,
                    events_emitted=2,
                    error="truncated at byte 8192",
                    segments_dropped=1,
                )
            )
            return

        second = [DecodedEvent(timestamp=BASE.replace(minute=1), event_code=82, event_param=5)]
        yield decoder_pb2.DecodeResult(events_arrow_ipc=events_to_arrow_batch(second))

        if "--no-status" in self._args:
            return

        status = decoder_pb2.DecodeResult(
            status=decoder_pb2.DecodeStatus(
                outcome=decoder_pb2.DECODE_OUTCOME_SUCCESS, events_emitted=3
            )
        )
        yield status
        if "--two-status" in self._args:
            yield status
        if "--batch-after" in self._args:
            yield decoder_pb2.DecodeResult(events_arrow_ipc=events_to_arrow_batch(second))


async def main() -> int:
    if os.environ.get(constants.MAGIC_COOKIE_KEY) != constants.MAGIC_COOKIE_VALUE:
        print("missing or wrong magic cookie", file=sys.stderr)
        return 1

    args = set(sys.argv[1:])
    stop = asyncio.Event()
    server = aio.server()
    servicer = health_aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    grpc_controller_pb2_grpc.add_GRPCControllerServicer_to_server(Controller(stop), server)
    decoder_pb2_grpc.add_DecoderServicer_to_server(FakeDecoder(args), server)

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
