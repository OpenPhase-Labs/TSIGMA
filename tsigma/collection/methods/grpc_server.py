"""
gRPC server ingestion method.

Hosts a long-lived gRPC ``IngestionService`` (defined in OpenPhase
``ingestion.proto``) that accepts pushed OpenPhase telemetry from any
number of devices. Unlike NATS/MQTT (which require per-device
topic/subject subscriptions), gRPC is a single endpoint accepting many
clients — ``device_id`` is extracted from the message content
(``IntersectionUpdate.intersection_id`` /
``CompactEventBatch.intersection_id``), not from per-device listener
config.

Inbound ``intersection_id`` values are validated against the registered
device set passed in via ``devices`` from the orchestrator.  Messages
referencing an unregistered device are dropped with a warning — the
operator must add the device before its telemetry is accepted.

This is a ListenerIngestionMethod — the ListenerService manages
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
from ..sdk import resolve_decoder_by_name
from ..targets import ControllerTarget, IngestionTarget

logger = logging.getLogger(__name__)

# Add proto output directory to sys.path so generated modules can
# resolve their cross-imports (ingestion_pb2 imports common_pb2, etc.).
_PROTO_DIR = str(Path(__file__).parent.parent / "decoders" / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from openphase.v1 import (  # noqa: E402  # generated proto modules require sys.path.insert above
    ingestion_pb2,
    ingestion_pb2_grpc,
)

_DEFAULT_DECODER = "openphase"
_DEFAULT_PORT = 50051
_DEFAULT_BIND = "0.0.0.0"
_DEFAULT_MAX_MSG_BYTES = 4 * 1024 * 1024  # 4 MB


class GRPCServerConfig(BaseModel):
    """Layer-2 server config for the gRPC listener."""

    port: int = _DEFAULT_PORT
    bind_address: str = _DEFAULT_BIND
    decoder: Optional[str] = _DEFAULT_DECODER
    tls_cert_file: Optional[str] = None
    tls_key_file: Optional[str] = None
    max_message_size: int = _DEFAULT_MAX_MSG_BYTES


class _IngestionServicer(ingestion_pb2_grpc.IngestionServiceServicer):
    """
    Implements the OpenPhase ``IngestionService`` gRPC contract.

    Each RPC re-serializes the parsed proto message back to bytes and
    hands them to the existing decoder, so the gRPC path uses the same
    decoder code as NATS/MQTT/HTTP/file ingestion.

    ``intersection_id`` on every inbound message is validated against
    the registered device set; messages from unregistered devices are
    rejected with an error in ``PublishAck``.
    """

    def __init__(
        self,
        decoder,
        session_factory,
        target: IngestionTarget,
        registered_device_ids: set[str],
    ) -> None:
        self._decoder = decoder
        self._session_factory = session_factory
        self._target = target
        self._registered = registered_device_ids

    def _validate_device(self, device_id: str) -> Optional[str]:
        """Return None if registered, error string otherwise."""
        if not self._registered:
            return (
                "no devices registered for grpc_server — operator must "
                "configure at least one device with method=grpc_server"
            )
        if device_id not in self._registered:
            return f"unregistered device_id {device_id!r}"
        return None

    async def PublishUpdate(self, request, context):
        device_id = request.intersection_id
        rejection = self._validate_device(device_id)
        if rejection:
            logger.warning("gRPC PublishUpdate rejected: %s", rejection)
            return ingestion_pb2.PublishAck(events_accepted=0, error=rejection)

        try:
            data = request.SerializeToString()
            events = self._decoder.decode_bytes(data)
        except Exception as exc:
            logger.exception(
                "gRPC PublishUpdate decode failed for %s %s",
                self._target.device_type, device_id,
            )
            return ingestion_pb2.PublishAck(events_accepted=0, error=str(exc))

        accepted = 0
        if events:
            try:
                await self._target.persist_with_drift_check(
                    events, device_id, self._session_factory,
                    source_label=self._target.device_type,
                )
                accepted = len(events)
            except Exception as exc:
                logger.exception(
                    "gRPC PublishUpdate persist failed for %s %s",
                    self._target.device_type, device_id,
                )
                return ingestion_pb2.PublishAck(
                    events_accepted=0, error=f"persist: {exc}",
                )

        return ingestion_pb2.PublishAck(events_accepted=accepted)

    async def PublishBatch(self, request, context):
        device_id = request.intersection_id
        rejection = self._validate_device(device_id)
        if rejection:
            logger.warning("gRPC PublishBatch rejected: %s", rejection)
            return ingestion_pb2.PublishAck(events_accepted=0, error=rejection)

        try:
            data = request.SerializeToString()
            events = self._decoder.decode_bytes(data)
        except Exception as exc:
            logger.exception(
                "gRPC PublishBatch decode failed for %s %s",
                self._target.device_type, device_id,
            )
            return ingestion_pb2.PublishAck(events_accepted=0, error=str(exc))

        accepted = 0
        if events:
            try:
                await self._target.persist_with_drift_check(
                    events, device_id, self._session_factory,
                    source_label=self._target.device_type,
                )
                accepted = len(events)
            except Exception as exc:
                logger.exception(
                    "gRPC PublishBatch persist failed for %s %s",
                    self._target.device_type, device_id,
                )
                return ingestion_pb2.PublishAck(
                    events_accepted=0, error=f"persist: {exc}",
                )

        return ingestion_pb2.PublishAck(events_accepted=accepted)

    async def StreamBatches(self, request_iterator, context):
        total_accepted = 0
        try:
            async for batch in request_iterator:
                device_id = batch.intersection_id
                rejection = self._validate_device(device_id)
                if rejection:
                    logger.warning(
                        "gRPC StreamBatches batch rejected: %s", rejection,
                    )
                    continue

                try:
                    data = batch.SerializeToString()
                    events = self._decoder.decode_bytes(data)
                except Exception:
                    logger.exception(
                        "gRPC StreamBatches decode failed for %s %s",
                        self._target.device_type, device_id,
                    )
                    continue

                if events:
                    try:
                        await self._target.persist_with_drift_check(
                            events, device_id, self._session_factory,
                            source_label=self._target.device_type,
                        )
                        total_accepted += len(events)
                    except Exception:
                        logger.exception(
                            "gRPC StreamBatches persist failed for %s %s",
                            self._target.device_type, device_id,
                        )
                        # Continue accepting subsequent batches.
        except Exception as exc:
            logger.exception("gRPC StreamBatches stream error")
            return ingestion_pb2.PublishAck(
                events_accepted=total_accepted, error=str(exc),
            )

        return ingestion_pb2.PublishAck(events_accepted=total_accepted)


@IngestionMethodRegistry.register("grpc_server")
class GRPCServerMethod(ListenerIngestionMethod):
    """Single-port gRPC server accepting OpenPhase IngestionService RPCs."""

    name = "grpc_server"

    def __init__(self) -> None:
        self._server: Optional[aio.Server] = None
        self._config: Optional[GRPCServerConfig] = None

    async def health_check(self) -> bool:
        return self._server is not None

    async def start(
        self,
        config: dict[str, Any],
        session_factory,
        *,
        target: Any = None,
        devices: Any = None,
    ) -> None:
        cfg = GRPCServerConfig(**(config or {}))
        self._config = cfg
        target = target if target is not None else ControllerTarget()

        # Pre-compute the registered device id set for inbound validation.
        registered: set[str] = {
            device_id for device_id, _ in (devices or [])
        }
        if not registered:
            logger.warning(
                "gRPC server has no registered %s devices — every inbound "
                "RPC will be rejected. Configure devices with "
                "method=grpc_server before starting.",
                target.device_type,
            )

        decoder = resolve_decoder_by_name(cfg.decoder or _DEFAULT_DECODER)
        servicer = _IngestionServicer(
            decoder, session_factory, target, registered,
        )

        options = [
            ("grpc.max_receive_message_length", cfg.max_message_size),
            ("grpc.max_send_message_length", cfg.max_message_size),
        ]

        self._server = aio.server(options=options)
        ingestion_pb2_grpc.add_IngestionServiceServicer_to_server(
            servicer, self._server,
        )

        listen_addr = f"{cfg.bind_address}:{cfg.port}"
        if cfg.tls_cert_file and cfg.tls_key_file:
            with open(cfg.tls_cert_file, "rb") as f:
                cert = f.read()
            with open(cfg.tls_key_file, "rb") as f:
                key = f.read()
            credentials = grpc.ssl_server_credentials([(key, cert)])
            self._server.add_secure_port(listen_addr, credentials)
            logger.info(
                "gRPC ingestion server (%s) listening on %s (TLS) — %d device(s)",
                target.device_type, listen_addr, len(registered),
            )
        else:
            self._server.add_insecure_port(listen_addr)
            logger.info(
                "gRPC ingestion server (%s) listening on %s (insecure) — %d device(s)",
                target.device_type, listen_addr, len(registered),
            )

        await self._server.start()

    async def stop(self) -> None:
        if self._server is not None:
            await self._server.stop(grace=5)
            self._server = None
            logger.info("gRPC ingestion server stopped")
        self._config = None
