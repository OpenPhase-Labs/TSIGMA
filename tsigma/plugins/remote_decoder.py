"""RemoteDecoder - a decoder plugin that runs out of process.

`Decode` is bidi-streaming: the host client-streams bounded byte chunks, the
plugin server-streams Arrow record batches and closes with exactly one terminal
`DecodeStatus`. `CanDecode` sends a bounded head only, never the whole file.

Decoders DO NOT convert time. The contract fixes this:
`TIME_SEMANTICS_NAIVE_LOCAL = 2; // controller-local instant; host converts at
ingest`. A decoder declares its semantics at `Describe` and emits instants
unchanged; the host converts, being the only party holding the jurisdiction zone
and the checkpoint state needed to resolve a DST fold across a batch boundary.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import pyarrow as pa

from ..collection.decoders.base import BaseDecoder, DecodedEvent, DecodeResult, FileMetadata
from .connection import PluginConnection

logger = logging.getLogger(__name__)

# Bytes of the file head sent to CanDecode. Matches the in-process convention
# ("typically first 1KB for magic byte check", BaseDecoder.can_decode).
CAN_DECODE_HEAD_BYTES = 4096

# Chunk size for the client-streamed payload. Bounded so a large file never
# approaches the gRPC message cap.
CHUNK_BYTES = 256 * 1024


class RemoteDecodeError(RuntimeError):
    """The decode stream was malformed, or the plugin reported FAILURE."""


class DecodeOutcome:
    """Mirrors the contract's DecodeOutcome enum without importing it eagerly."""

    UNSPECIFIED = 0
    SUCCESS = 1
    PARTIAL = 2
    FAILURE = 3

    NAMES = {0: "UNSPECIFIED", 1: "SUCCESS", 2: "PARTIAL", 3: "FAILURE"}


@dataclass
class RemoteDecodeResult:
    """A decode envelope plus the terminal status the plugin reported."""

    result: DecodeResult
    outcome: int
    events_emitted: int
    error: str
    segments_dropped: int

    @property
    def succeeded(self) -> bool:
        return self.outcome == DecodeOutcome.SUCCESS

    @property
    def partial(self) -> bool:
        return self.outcome == DecodeOutcome.PARTIAL

    @property
    def failed(self) -> bool:
        return self.outcome in (DecodeOutcome.FAILURE, DecodeOutcome.UNSPECIFIED)


def chunk_bytes(data: bytes, size: int = CHUNK_BYTES):
    """Split a payload into bounded, in-order transport chunks."""
    for start in range(0, len(data), size):
        yield data[start : start + size]


def resolve_naive_local(instants: list[datetime], zone: str) -> list[datetime]:
    """Convert controller-local instants to UTC, resolving the DST fall-back fold.

    Hi-res logs are sequential, so a local timestamp moving BACKWARDS marks the
    fall-back crossing; everything after it resolves as ``fold=1``. The caller
    passes the whole ordered run, so a crossing on a batch boundary is still seen
    - which is why this is host-side and not in the decoder.

    Spring-forward gap times are NOT coerced here; they are left for the
    validation layer to flag (ADR-0046, flag-never-block).
    """
    tz = ZoneInfo(zone)
    out: list[datetime] = []
    fold = 0
    previous: datetime | None = None
    for instant in instants:
        naive = instant.replace(tzinfo=None)
        if previous is not None and naive < previous:
            fold = 1
        previous = naive
        out.append(naive.replace(tzinfo=tz, fold=fold).astimezone(ZoneInfo("UTC")))
    return out


def arrow_batch_to_events(blob: bytes) -> list[DecodedEvent]:
    """One Arrow batch -> DecodedEvent list, in wire order."""
    with pa.ipc.open_stream(pa.BufferReader(blob)) as reader:
        table = reader.read_all()
    rows = table.to_pylist()
    return [
        DecodedEvent(
            timestamp=row["timestamp"],
            event_code=row["event_code"],
            event_param=row["event_param"],
        )
        for row in rows
    ]


def events_to_arrow_batch(events: list[DecodedEvent]) -> bytes:
    """DecodedEvent list -> one Arrow IPC batch. Used by decoder plugins."""
    table = pa.table(
        {
            "timestamp": pa.array([e.timestamp for e in events], type=pa.timestamp("us")),
            "event_code": pa.array([e.event_code for e in events], type=pa.int32()),
            "event_param": pa.array([e.event_param for e in events], type=pa.int32()),
        }
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def file_metadata_from_proto(message) -> FileMetadata:
    """Contract FileMetadata -> the in-process dataclass. Unset fields stay None."""

    def opt(field: str):
        value = getattr(message, field)
        return value or None

    def ts(field: str):
        return getattr(message, field).ToDatetime() if message.HasField(field) else None

    return FileMetadata(
        device_ip=opt("device_ip"),
        device_mac=opt("device_mac"),
        log_version=opt("log_version"),
        source_filename=opt("source_filename"),
        phases_in_use=list(message.phases_in_use) or None,
        log_begin=ts("log_begin"),
        header_anchor=ts("header_anchor"),
        header_anchor_secondary=ts("header_anchor_secondary"),
        raw=dict(message.raw) or None,
    )


class RemoteDecoder(BaseDecoder):
    """A `BaseDecoder` whose work happens in a plugin process.

    `decode_bytes` / `can_decode` are the in-process synchronous interface and
    are NOT implemented here - a remote decode is inherently async. Callers use
    `decode_remote` / `can_decode_remote`; the host-side ingest path awaits them.
    """

    def __init__(self, connection: PluginConnection, name: str, extensions: list[str],
                 description: str, time_semantics: int, output_kind: int):
        self.connection = connection
        self.name = name
        self.extensions = extensions
        self.description = description
        self.time_semantics = time_semantics
        self.output_kind = output_kind

    # ------------------------------------------------------------------ stub
    def _stub(self):
        if self.connection.channel is None:
            raise RemoteDecodeError(f"{self.name}: plugin is not connected")
        from tsigma.decoder.v1 import decoder_pb2_grpc

        return decoder_pb2_grpc.DecoderStub(self.connection.channel)

    # --------------------------------------------------------------- decode
    async def decode_remote(self, data: bytes) -> RemoteDecodeResult:
        """Stream `data` to the plugin and reassemble its decoded events."""
        from tsigma.decoder.v1 import decoder_pb2

        async def requests():
            for chunk in chunk_bytes(data):
                yield decoder_pb2.DecodeChunk(chunk=chunk)

        events: list[DecodedEvent] = []
        metadata: FileMetadata | None = None
        status = None

        async for item in self._stub().Decode(requests()):
            kind = item.WhichOneof("payload")
            if kind == "metadata":
                metadata = file_metadata_from_proto(item.metadata)
            elif kind == "events_arrow_ipc":
                if status is not None:
                    raise RemoteDecodeError(f"{self.name}: batch after the terminal status")
                events.extend(arrow_batch_to_events(item.events_arrow_ipc))
            elif kind == "status":
                if status is not None:
                    raise RemoteDecodeError(f"{self.name}: more than one terminal status")
                status = item.status

        if status is None:
            # A stream that ends without a status is FAILURE, never success -
            # a crashed decoder cannot send one (contract decoder.proto).
            raise RemoteDecodeError(
                f"{self.name}: decode stream ended with no terminal status"
            )

        return RemoteDecodeResult(
            result=DecodeResult(events=events, metadata=metadata),
            outcome=status.outcome,
            events_emitted=status.events_emitted,
            error=status.error,
            segments_dropped=status.segments_dropped,
        )

    async def can_decode_remote(self, data: bytes) -> bool:
        """Content-sniff a bounded head; never sends the whole payload."""
        from tsigma.decoder.v1 import decoder_pb2

        response = await self._stub().CanDecode(
            decoder_pb2.CanDecodeRequest(head=data[:CAN_DECODE_HEAD_BYTES])
        )
        return response.can_decode

    # ------------------------------------------- in-process interface (unused)
    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        raise NotImplementedError(f"{self.name} is a remote decoder; use decode_remote()")

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        raise NotImplementedError("remote decoder; use can_decode_remote()")
