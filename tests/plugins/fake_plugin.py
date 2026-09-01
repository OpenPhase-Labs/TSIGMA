"""A fake TSIGMA plugin subprocess, driven entirely from argv.

Spawned by tests/plugins/test_protocol_handshake.py to exercise the host side of
PROTOCOL.md sections 1-3 against a real process, real pipes, and a real gRPC
health server. Every misbehaviour the host must survive is a flag here:

  --emit-line TEXT     print TEXT as the handshake line (conforming or not)
  --emit-raw-bytes N   print N bytes with no newline at all, before anything else
  --bulk-bytes N       after the handshake, write N bytes to stdout AND stderr
  --serve-port N       serve grpc.health.v1 on 127.0.0.1:N (0 = never serve)
  --controller MODE    'graceful' also serves GRPCController.Shutdown

The magic cookie is checked first: a process started without it exits at once,
as the contract requires, so every successful launch in the suite also proves
the host set it.

Not a test module: pytest does not collect ``fake_plugin.py``.
"""

import argparse
import os
import sys
import threading
import time
from concurrent import futures
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Bulk output is newline-terminated in small chunks so a host that drains by
# line and a host that drains by block are both able to keep up. The point of
# the test is the volume, not a pathological single line.
BULK_CHUNK_WIDTH = 64

EXIT_NO_COOKIE = 3


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fake TSIGMA plugin for host tests.")
    parser.add_argument("--pidfile", required=True)
    parser.add_argument("--cookie-key", required=True)
    parser.add_argument("--cookie-value", required=True)
    parser.add_argument("--emit-line", default=None)
    parser.add_argument("--emit-raw-bytes", type=int, default=0)
    parser.add_argument("--bulk-bytes", type=int, default=0)
    parser.add_argument("--serve-port", type=int, default=0)
    parser.add_argument("--controller", choices=("none", "graceful"), default="none")
    parser.add_argument("--exit-after-handshake", action="store_true")
    return parser.parse_args(argv)


def _write_bulk(stream, total: int) -> None:
    chunk = "b" * (BULK_CHUNK_WIDTH - 1) + "\n"
    written = 0
    while written < total:
        stream.write(chunk)
        written += len(chunk)
    stream.flush()


def _add_controller(server) -> None:
    from tsigma.plugins.protocol import grpc_controller_pb2, grpc_controller_pb2_grpc

    class _Controller(grpc_controller_pb2_grpc.GRPCControllerServicer):
        def Shutdown(self, request, context):
            # Answer first, then leave: the host must see a clean RPC and a
            # process that goes away on its own, with no kill needed.
            threading.Timer(0.2, lambda: os._exit(0)).start()
            return grpc_controller_pb2.Empty()

    grpc_controller_pb2_grpc.add_GRPCControllerServicer_to_server(_Controller(), server)


def _serve(port: int, controller: str) -> None:
    import grpc
    from grpc_health.v1 import health, health_pb2, health_pb2_grpc

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    if controller == "graceful":
        _add_controller(server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    server.wait_for_termination()


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    Path(args.pidfile).write_text(str(os.getpid()), encoding="utf-8")

    if os.environ.get(args.cookie_key) != args.cookie_value:
        sys.stderr.write("magic cookie missing or wrong; this is a plugin, not a program\n")
        sys.stderr.flush()
        return EXIT_NO_COOKIE

    if args.emit_raw_bytes:
        sys.stdout.write("x" * args.emit_raw_bytes)
        sys.stdout.flush()
    if args.emit_line is not None:
        sys.stdout.write(args.emit_line + "\n")
        sys.stdout.flush()
    if args.bulk_bytes:
        _write_bulk(sys.stdout, args.bulk_bytes)
        _write_bulk(sys.stderr, args.bulk_bytes)

    if args.serve_port:
        _serve(args.serve_port, args.controller)
        return 0

    if args.exit_after_handshake:
        # A cron run that did its work and ended. The host still opened a channel
        # to it during launch, so this is the only shape that exposes what a
        # finished scheduled run leaves behind.
        return 0

    # Nothing else to do, but stay alive: a host that fails the handshake has to
    # kill this process, and a leftover here is the orphan the test looks for.
    while True:
        time.sleep(0.5)


if __name__ == "__main__":
    sys.exit(main())
