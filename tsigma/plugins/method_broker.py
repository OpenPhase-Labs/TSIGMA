"""Host-served method broker services - the consume-side of the method contract.

A method plugin is transport-only: it fetches bytes and hands them back. It never
decodes, validates, persists, or decides whether its own checkpoint advances.
These services are how it reaches the host for the state it legitimately needs.

`EventSink.DecodeAndPersist` is the wire form of `ingest_raw` (ADR-0034: the host
owns decode -> validate -> persist). `CheckpointService` wraps the checkpoint SDK
so a plugin can read its cursor and report an error without touching the database.
"""

import logging
from datetime import datetime, timezone

from ..collection import sdk
from ..collection.ingest import IngestOutcome, ingest_raw

logger = logging.getLogger(__name__)

# Contract DeviceType -> the string the checkpoint SDK stores.
_DEVICE_TYPE = {0: "controller", 1: "controller", 2: "sensor"}

# Host maps its own outcome onto the contract's IngestOutcome enum.
_OUTCOME_TO_PROTO = {
    IngestOutcome.SUCCESS: 1,
    IngestOutcome.PARTIAL: 2,
    IngestOutcome.FAILURE: 3,
}

ERROR_MSG_MAX = 1000


def device_type_of(value: int) -> str:
    """Contract DeviceType -> checkpoint device_type string (default controller)."""
    return _DEVICE_TYPE.get(value, "controller")


def stated_device_type_of(header) -> str | None:
    """`DecodeAndPersistHeader.device_type` as a device_type string, or None.

    UNSPECIFIED is absence, not an assertion. A proto enum defaults to 0, so a
    plugin that sets nothing reads back as 0, and answering "controller" there
    states on its behalf something it never said. The spine then routes a sensor
    batch to the controller table, fails the element-type check, and persists
    nothing - losing rows on a path that worked by inference before anyone
    stated anything. Returning None leaves the spine free to infer.
    """
    value = getattr(header, "device_type", 0)
    if not value:
        return None
    return _DEVICE_TYPE.get(value)


def poll_reference_of(header) -> datetime | None:
    """`DecodeAndPersistHeader.last_successful_poll` as tz-aware UTC, or None.

    `ToDatetime()` with no argument returns a NAIVE datetime, and
    `is_backward_poisoned` compares the reference against tz-aware event
    times: a naive one raises and silently disables the check.
    """
    has_field = getattr(header, "HasField", None)
    if has_field is not None and not has_field("last_successful_poll"):
        return None
    value = getattr(header, "last_successful_poll", None)
    if value is None or isinstance(value, datetime):
        return value
    return value.ToDatetime(tzinfo=timezone.utc)


class CheckpointService:
    """Mirrors the checkpoint half of tsigma/collection/sdk.

    The plugin reads and reports; the host decides. `SaveCheckpoint` is reachable
    only for a cycle the host already ruled advanceable - the advancement policy
    is not delegated (see tsigma/collection/advancement.py).
    """

    def __init__(self, session_factory):
        self._session_factory = session_factory

    async def load_checkpoint(self, request):
        return await sdk.load_checkpoint(
            request.method_name,
            device_type_of(request.device_type),
            request.device_id,
            self._session_factory,
        )

    async def save_checkpoint(self, request) -> None:
        """Forward only the fields the request actually set.

        Presence means "set this column"; absence means "leave unchanged", so an
        unset field must not be forwarded as None - that would clear a column the
        plugin never mentioned.
        """
        kwargs: dict = {}
        if request.HasField("last_event_timestamp"):
            kwargs["last_event_timestamp"] = request.last_event_timestamp.ToDatetime()
        if request.HasField("last_file_mtime"):
            kwargs["last_file_mtime"] = request.last_file_mtime.ToDatetime()
        if request.HasField("last_filename"):
            kwargs["last_filename"] = request.last_filename
        if request.HasField("files_hash"):
            kwargs["files_hash"] = request.files_hash
        # Additive counters: 0 is a valid no-op, so they need no presence flag.
        for field in ("events_ingested", "duplicates_absorbed", "files_ingested"):
            value = getattr(request, field)
            if value:
                kwargs[field] = value

        await sdk.save_checkpoint(
            request.method_name,
            device_type_of(request.device_type),
            request.device_id,
            self._session_factory,
            **kwargs,
        )

    async def record_error(self, request) -> None:
        await sdk.record_error(
            request.method_name,
            device_type_of(request.device_type),
            request.device_id,
            self._session_factory,
            request.error_msg[:ERROR_MSG_MAX],
        )


class EventSinkService:
    """`DecodeAndPersist`: raw bytes in, an ingest outcome out.

    Client-streaming on the wire - a header frame then bounded chunks - so a large
    fetched file never approaches the gRPC cap. The plugin supplies bytes and a
    decoder name; everything after that is the host's spine.
    """

    def __init__(self, session_factory):
        self._session_factory = session_factory

    async def decode_and_persist(self, header, raw: bytes):
        """Run one payload through `ingest_raw` and return the wire result.

        Every field the header carries reaches the spine: an out-of-process
        method gets the same integrity path as an in-process one, so its
        review row names the source file and its poison check runs.
        """
        result = await ingest_raw(
            raw,
            device_id=header.device_id,
            session_factory=self._session_factory,
            decoder_name=header.decoder_name or None,
            filename=header.filename or None,
            source_label=header.source_label or "signal",
            device_type=stated_device_type_of(header),
            last_successful_poll=poll_reference_of(header),
        )
        return to_persist_response(result)


def to_persist_response(result):
    """IngestResult -> contract PersistResponse.

    `duplicates_absorbed` is deliberately not on the wire: the contract has the
    host compute it as attempted-minus-inserted, so an untrusted method cannot
    assert a duplicate count.
    """
    from tsigma.method.v1 import method_pb2

    response = method_pb2.PersistResponse(
        events_inserted=result.events_inserted,
        outcome=_OUTCOME_TO_PROTO.get(result.outcome, 3),
    )
    if result.max_event_time is not None:
        response.max_event_time.FromDatetime(result.max_event_time)
    return response


async def collect_chunks(request_iterator):
    """Reassemble a DecodeAndPersist stream into (header, payload).

    The first frame MUST be the header and every later frame a chunk; a stream
    that opens with a chunk is malformed and is refused rather than guessed at.
    """
    header = None
    parts: list[bytes] = []
    async for frame in request_iterator:
        kind = frame.WhichOneof("frame")
        if kind == "header":
            if header is not None:
                raise ValueError("DecodeAndPersist: more than one header frame")
            header = frame.header
        elif kind == "chunk":
            if header is None:
                raise ValueError("DecodeAndPersist: chunk before the header frame")
            parts.append(frame.chunk)
    if header is None:
        raise ValueError("DecodeAndPersist: stream ended with no header frame")
    return header, b"".join(parts)
