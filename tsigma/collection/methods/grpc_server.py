"""
gRPC server ingestion method.

Hosts a long-lived gRPC `IngestionService` (defined in OpenPhase
`ingestion.proto`) that accepts pushed OpenPhase telemetry from any
number of devices. Unlike NATS/MQTT (which require per-signal
topic/subject subscriptions), gRPC is a single endpoint accepting
many clients — `signal_id` is extracted from the message content
(`IntersectionUpdate.intersection_id` / `CompactEventBatch.intersection_id`),
not from per-signal listener config.

Listener config:

    {
        "method": "grpc_server",
        "port": 50051,
        "bind_address": "0.0.0.0",
        "decoder": "openphase",            # optional, default "openphase"
        "tls_cert_file": null,             # optional PEM cert path
        "tls_key_file": null,              # optional PEM key path
        "max_message_size_bytes": 4194304  # optional, default 4 MB
    }

This is a ListenerIngestionMethod — the CollectorService manages
start/stop lifecycle.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Optional

import grpc
from grpc import aio
from pydantic import BaseModel

from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
from ..sdk import persist_events_with_drift_check, resolve_decoder_by_name

logger = logging.getLogger(__name__)

# Add proto output directory to sys.path so generated modules can
# resolve their cross-imports (ingestion_pb2 imports common_pb2, etc.).
_PROTO_DIR = str(Path(__file__).parent.parent / "decoders" / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from openphase.v1 import ingestion_pb2, ingestion_pb2_grpc  # noqa: E402

_DEFAULT_DECODER = "openphase"
_DEFAULT_PORT = 50051
_DEFAULT_BIND = "0.0.0.0"
_DEFAULT_MAX_MSG_BYTES = 4 * 1024 * 1024  # 4 MB


class GRPCServerConfig(BaseModel):
    """Configuration for the gRPC ingestion server."""

    port: int = _DEFAULT_PORT
    bind_address: str = _DEFAULT_BIND
    decoder: Optional[str] = _DEFAULT_DECODER
    tls_cert_file: Optional[str] = None
    tls_key_file: Optional[str] = None
    max_message_size_bytes: int = _DEFAULT_MAX_MSG_BYTES


class _IngestionServicer(ingestion_pb2_grpc.IngestionServiceServicer):
    """
    Implements the OpenPhase `IngestionService` gRPC contract.

    Each RPC re-serializes the parsed proto message back to bytes and
    hands them to the existing decoder, so the gRPC path uses the same
    decoder code as NATS/MQTT/HTTP/file ingestion. The serialize/parse
    round-trip is negligible compared to DB write cost.
    """

    def __init__(self, decoder, session_factory) -> None:
        self._decoder = decoder
        self._session_factory = session_factory

    async def PublishUpdate(self, request, context):
        """Single multi-payload IntersectionUpdate."""
        signal_id = request.intersection_id
        try:
            data = request.SerializeToString()
            events = self._decoder.decode_bytes(data)
        except Exception as exc:
            logger.exception(
                "gRPC PublishUpdate decode failed for signal %s", signal_id
            )
            return ingestion_pb2.PublishAck(events_accepted=0, error=str(exc))

        accepted = 0
        if events:
            try:
                await persist_events_with_drift_check(
                    events, signal_id, self._session_factory
                )
                accepted = len(events)
                logger.debug(
                    "gRPC: %d events from PublishUpdate signal=%s",
                    accepted,
                    signal_id,
                )
            except Exception as exc:
                logger.exception(
                    "gRPC PublishUpdate persist failed for signal %s", signal_id
                )
                return ingestion_pb2.PublishAck(
                    events_accepted=0, error=f"persist: {exc}"
                )

        return ingestion_pb2.PublishAck(events_accepted=accepted)

    async def PublishBatch(self, request, context):
        """Single CompactEventBatch."""
        signal_id = request.intersection_id
        try:
            data = request.SerializeToString()
            events = self._decoder.decode_bytes(data)
        except Exception as exc:
            logger.exception(
                "gRPC PublishBatch decode failed for signal %s", signal_id
            )
            return ingestion_pb2.PublishAck(events_accepted=0, error=str(exc))

        accepted = 0
        if events:
            try:
                await persist_events_with_drift_check(
                    events, signal_id, self._session_factory
                )
                accepted = len(events)
                logger.debug(
                    "gRPC: %d events from PublishBatch signal=%s",
                    accepted,
                    signal_id,
                )
            except Exception as exc:
                logger.exception(
                    "gRPC PublishBatch persist failed for signal %s", signal_id
                )
                return ingestion_pb2.PublishAck(
                    events_accepted=0, error=f"persist: {exc}"
                )

        return ingestion_pb2.PublishAck(events_accepted=accepted)

    async def StreamBatches(self, request_iterator, context):
        """Long-lived client-streaming push of CompactEventBatch messages."""
        total_accepted = 0
        try:
            async for batch in request_iterator:
                signal_id = batch.intersection_id
                try:
                    data = batch.SerializeToString()
                    events = self._decoder.decode_bytes(data)
                except Exception:
                    logger.exception(
                        "gRPC StreamBatches decode failed for signal %s",
                        signal_id,
                    )
                    continue

                if events:
                    try:
                        await persist_events_with_drift_check(
                            events, signal_id, self._session_factory
                        )
                        total_accepted += len(events)
                    except Exception:
                        logger.exception(
                            "gRPC StreamBatches persist failed for signal %s",
                            signal_id,
                        )
                        # Continue accepting subsequent batches even if one fails.
        except Exception as exc:
            logger.exception("gRPC StreamBatches stream error")
            return ingestion_pb2.PublishAck(
                events_accepted=total_accepted, error=str(exc)
            )

        logger.debug("gRPC: stream closed, %d events total", total_accepted)
        return ingestion_pb2.PublishAck(events_accepted=total_accepted)


@IngestionMethodRegistry.register("grpc_server")
class GRPCServerMethod(ListenerIngestionMethod):
    """
    gRPC server ingestion method.

    Single bound port. Devices connect and push `IntersectionUpdate`,
    `CompactEventBatch`, or stream `CompactEventBatch` over the
    OpenPhase `IngestionService`. The same `OpenPhaseDecoder` that
    handles NATS/MQTT/HTTP/file payloads handles gRPC payloads — the
    decoder is transport-agnostic.
    """

    name = "grpc_server"

    def __init__(self) -> None:
        self._server: Optional[aio.Server] = None
        self._config: Optional[GRPCServerConfig] = None

    async def health_check(self) -> bool:
        """Return True when the gRPC server is bound and serving."""
        return self._server is not None

    async def start(self, config: dict[str, Any], session_factory) -> None:
        """
        Start the gRPC server.

        Args:
            config: Listener config (port, bind_address, decoder, optional TLS).
            session_factory: Async session factory for DB writes.
        """
        cfg = GRPCServerConfig(**(config or {}))
        self._config = cfg

        decoder = resolve_decoder_by_name(cfg.decoder or _DEFAULT_DECODER)
        servicer = _IngestionServicer(decoder, session_factory)

        options = [
            ("grpc.max_receive_message_length", cfg.max_message_size_bytes),
            ("grpc.max_send_message_length", cfg.max_message_size_bytes),
        ]

        self._server = aio.server(options=options)
        ingestion_pb2_grpc.add_IngestionServiceServicer_to_server(
            servicer, self._server
        )

        listen_addr = f"{cfg.bind_address}:{cfg.port}"
        if cfg.tls_cert_file and cfg.tls_key_file:
            with open(cfg.tls_cert_file, "rb") as f:
                cert = f.read()
            with open(cfg.tls_key_file, "rb") as f:
                key = f.read()
            credentials = grpc.ssl_server_credentials([(key, cert)])
            self._server.add_secure_port(listen_addr, credentials)
            logger.info("gRPC ingestion server listening on %s (TLS)", listen_addr)
        else:
            self._server.add_insecure_port(listen_addr)
            logger.info("gRPC ingestion server listening on %s (insecure)", listen_addr)

        await self._server.start()

    async def stop(self) -> None:
        """Stop the gRPC server, allowing in-flight RPCs up to 5 s to finish."""
        if self._server is not None:
            await self._server.stop(grace=5)
            self._server = None
            logger.info("gRPC ingestion server stopped")
        self._config = None
