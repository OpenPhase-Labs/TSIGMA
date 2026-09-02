"""Tests for the host-owned ingest spine (`tsigma.collection.ingest`).

Covers R7a: the three-state outcome is consulted before any shortcut, no
exception escapes `ingest_raw`, local-to-UTC normalization is driven by the
timestamp and resolves a DST fall-back per occurrence, a spring-forward gap is
ingested and flagged, a signal running on the deployment default zone reaches
the review queue, a persist failure writes a review row, and every review row
names the source file rather than the decoder.

Doubles are constrained to the interfaces the real objects expose (the
in-process decoder subclasses `BaseDecoder`; the remote decoder returns a real
`RemoteDecodeResult`), so a state the production path cannot reach cannot be
asserted about here either.

Zone facts these tests rest on, for America/New_York in 2025:

* spring forward 2025-03-09, local 02:00 -> 03:00, so 02:30 never happens
* fall back    2025-11-02, local 02:00 -> 01:00, so 01:30 happens twice:
  first as EDT (-04:00, 05:30Z), then as EST (-05:00, 06:30Z)
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from tsigma.plugins.remote_decoder import DecodeOutcome, RemoteDecodeResult, arrow_batch_to_events

from tests._helpers import make_mock_session_factory
from tsigma.collection.decoders.base import BaseDecoder, DecodedEvent, DecodeResult, DecoderRegistry
from tsigma.collection.ingest import (
    DECODE_REVIEW_REASON,
    SPRING_FORWARD_REASON,
    STAGE_DECODE,
    STAGE_NORMALIZE,
    STAGE_PERSIST,
    STAGE_TIMEZONE,
    UNRESOLVED_TIMEZONE_REASON,
    IngestOutcome,
    ingest_raw,
    normalize_event_times,
)
from tsigma.collection.sdk import ZONE_FROM_DEFAULT, ZONE_FROM_SIGNAL
from tsigma.models.ingest_review import IngestReview

EASTERN = "America/New_York"
UTC = timezone.utc

INGEST = "tsigma.collection.ingest"


# ---------------------------------------------------------------------------
# Doubles - constrained to the real interfaces
# ---------------------------------------------------------------------------


class StubDecoder(BaseDecoder):
    """In-process decoder double: a real BaseDecoder, so `decode` is the real seam.

    It has no way to declare PARTIAL, exactly like every in-process decoder -
    which is why the outcome tests drive the remote path instead.
    """

    name = "stub"
    extensions = [".stub"]
    description = "test double"

    def __init__(self, events=None, *, metadata=None, raises=None):
        self._events = list(events or [])
        self._metadata = metadata
        self._raises = raises

    def decode_bytes(self, data: bytes):
        if self._raises is not None:
            raise self._raises
        return list(self._events)

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        return True

    def decode(self, data: bytes) -> DecodeResult:
        return DecodeResult(events=self.decode_bytes(data), metadata=self._metadata)


class StubRemoteDecoder:
    """Remote decoder double: `decode_remote` returns a real RemoteDecodeResult."""

    def __init__(self, *, result=None, outcome=DecodeOutcome.SUCCESS, error="",
                 segments_dropped=0, raises=None):
        self._result = result
        self._outcome = outcome
        self._error = error
        self._segments_dropped = segments_dropped
        self._raises = raises

    async def decode_remote(self, raw: bytes) -> RemoteDecodeResult:
        if self._raises is not None:
            raise self._raises
        return RemoteDecodeResult(
            result=self._result,
            outcome=self._outcome,
            events_emitted=len(self._result.events) if self._result else 0,
            error=self._error,
            segments_dropped=self._segments_dropped,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _event(ts: datetime, code: int = 1, param: int = 0) -> DecodedEvent:
    return DecodedEvent(timestamp=ts, event_code=code, event_param=param)


def _local(year, month, day, hour, minute) -> datetime:
    """A naive controller-local timestamp."""
    return datetime(year, month, day, hour, minute)


def _utc(year, month, day, hour, minute) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def _reviews(session, reason: str | None = None) -> list[IngestReview]:
    """The IngestReview rows added through this session, optionally by reason."""
    rows = [
        call.args[0] for call in session.add.call_args_list
        if call.args and isinstance(call.args[0], IngestReview)
    ]
    return [row for row in rows if reason is None or row.reason == reason]


def _allow_dedupe(session) -> None:
    """Make the open-row dedupe query report 'no existing row'."""
    result = MagicMock()
    result.first.return_value = None
    session.execute.return_value = result


def _zone_is(zone: str, origin: str = ZONE_FROM_SIGNAL):
    """Patch the timezone resolver to answer with a zone and its origin."""
    return patch(
        f"{INGEST}.resolve_source_timezone_with_origin",
        new=AsyncMock(return_value=(zone, origin)),
    )


def _remote(decoder: StubRemoteDecoder):
    """Route the decoder name to a plugin connection carrying `decoder`."""
    connection = MagicMock()
    connection.decoder = decoder
    return (
        patch.object(DecoderRegistry, "is_remote", return_value=True),
        patch.object(DecoderRegistry, "get_connection", return_value=connection),
    )


# ---------------------------------------------------------------------------
# normalize_event_times - timestamp-driven conversion and per-occurrence fold
# ---------------------------------------------------------------------------


def test_naive_is_converted_and_aware_is_passed_through():
    """Naive is controller-local and converted; aware is absolute and untouched."""
    naive = _event(_local(2025, 6, 1, 12, 0))
    aware = _event(_utc(2025, 6, 1, 12, 0))

    normalize_event_times([naive, aware], EASTERN)

    # 12:00 EDT is 16:00Z; the aware event names an instant already.
    assert naive.timestamp == _utc(2025, 6, 1, 16, 0)
    assert aware.timestamp == _utc(2025, 6, 1, 12, 0)


def test_out_of_order_pair_does_not_shift_a_later_ambiguous_time():
    """One backward step outside the fall-back hour must not re-read what follows.

    Under the latching flag this replaces, the 00:35 step set fold=1 for the
    rest of the run and moved the 01:30 event an hour later.
    """
    events = [
        _event(_local(2025, 11, 2, 0, 30)),
        _event(_local(2025, 11, 2, 0, 40)),
        _event(_local(2025, 11, 2, 0, 35)),   # jitter, outside the ambiguous hour
        _event(_local(2025, 11, 2, 1, 30)),   # ambiguous: first occurrence
    ]

    report = normalize_event_times(events, EASTERN)

    assert events[3].timestamp == _utc(2025, 11, 2, 5, 30)   # EDT, not EST
    assert report.ambiguous_times == [_local(2025, 11, 2, 1, 30)]
    assert report.gap_times == []


def test_backward_step_inside_the_fall_back_hour_takes_the_second_occurrence():
    """A backward step BETWEEN two ambiguous times is the fall-back crossing."""
    events = [
        _event(_local(2025, 11, 2, 1, 50)),   # first occurrence, EDT
        _event(_local(2025, 11, 2, 1, 10)),   # clock stepped back: second, EST
        _event(_local(2025, 11, 2, 1, 20)),   # still the second occurrence
    ]

    normalize_event_times(events, EASTERN)

    assert events[0].timestamp == _utc(2025, 11, 2, 5, 50)
    assert events[1].timestamp == _utc(2025, 11, 2, 6, 10)
    assert events[2].timestamp == _utc(2025, 11, 2, 6, 20)


def test_spring_forward_gap_times_are_reported_and_ingested():
    """A time the wall clock never read is kept, mapped onto the valid side."""
    gap = _event(_local(2025, 3, 9, 2, 30))

    report = normalize_event_times([gap], EASTERN)

    assert report.gap_times == [_local(2025, 3, 9, 2, 30)]
    # Read with the pre-transition offset (EST, -05:00) - 03:30 EDT.
    assert gap.timestamp == _utc(2025, 3, 9, 7, 30)
    assert gap.timestamp.astimezone(ZoneInfo(EASTERN)).hour == 3


# ---------------------------------------------------------------------------
# The decode outcome is consulted before any shortcut
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_decode_with_no_rows_does_not_report_success():
    """A PARTIAL decode never reports SUCCESS, whatever the row count."""
    session_factory, session = make_mock_session_factory()
    is_remote, get_connection = _remote(
        StubRemoteDecoder(
            result=DecodeResult(events=[], metadata=None),
            outcome=DecodeOutcome.PARTIAL,
            error="2 segments unreadable",
            segments_dropped=2,
        )
    )

    with is_remote, get_connection:
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="vendor-plugin", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.PARTIAL
    assert result.events_inserted == 0
    # No last-good point, so the advancement policy holds the checkpoint.
    assert result.max_event_time is None

    row = _reviews(session, DECODE_REVIEW_REASON)[0]
    assert row.severity == "warning"
    assert row.detail["segments_dropped"] == 2
    assert row.status == "open"


@pytest.mark.asyncio
async def test_failure_decode_carrying_rows_still_persists_them():
    """Once rows reach the host it does not discard them (ADR-0034 as amended).

    The plugin's declared FAILURE is recorded and flagged; it is not an
    instruction to destroy data already in hand.
    """
    events = [_event(_utc(2025, 6, 1, 12, 0)), _event(_utc(2025, 6, 1, 12, 1))]
    session_factory, session = make_mock_session_factory()
    is_remote, get_connection = _remote(
        StubRemoteDecoder(
            result=DecodeResult(events=events, metadata=None),
            outcome=DecodeOutcome.FAILURE,
            error="stream closed early",
        )
    )

    with is_remote, get_connection, \
            patch(f"{INGEST}.persist_events_with_drift_check", new=AsyncMock(return_value=2)):
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="vendor-plugin", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.FAILURE
    assert result.events_inserted == 2
    assert result.events_decoded == 2
    assert result.failed_stage == STAGE_DECODE

    row = _reviews(session, DECODE_REVIEW_REASON)[0]
    assert row.severity == "error"
    assert row.detail["error"] == "stream closed early"


# ---------------------------------------------------------------------------
# No exception escapes ingest_raw; the failing stage is identified
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transport_error_from_a_plugin_becomes_a_decode_outcome():
    """A dropped gRPC connection is an outcome, not an exception in the poll loop."""
    session_factory, session = make_mock_session_factory()
    is_remote, get_connection = _remote(
        StubRemoteDecoder(raises=ConnectionError("channel closed by peer"))
    )

    with is_remote, get_connection:
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="vendor-plugin", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.FAILURE
    assert result.failed_stage == STAGE_DECODE
    assert "channel closed by peer" in result.error
    assert _reviews(session, DECODE_REVIEW_REASON)


@pytest.mark.asyncio
async def test_malformed_arrow_batch_becomes_a_decode_outcome():
    """A plugin's unreadable Arrow payload fails through the real Arrow reader."""
    session_factory, session = make_mock_session_factory()

    class ArrowBreakingDecoder(StubRemoteDecoder):
        async def decode_remote(self, raw: bytes) -> RemoteDecodeResult:
            arrow_batch_to_events(b"this is not an arrow ipc stream")
            raise AssertionError("unreachable: the batch above is malformed")

    is_remote, get_connection = _remote(ArrowBreakingDecoder())

    with is_remote, get_connection:
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="vendor-plugin", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.FAILURE
    assert result.failed_stage == STAGE_DECODE
    assert _reviews(session, DECODE_REVIEW_REASON)


def test_the_malformed_arrow_fixture_really_raises_in_pyarrow():
    """Guards the test above: the payload must break the real reader, not a mock."""
    with pytest.raises(pa.ArrowInvalid):
        arrow_batch_to_events(b"this is not an arrow ipc stream")


@pytest.mark.asyncio
async def test_malformed_timezone_identifier_becomes_a_normalize_outcome():
    """Signal.source_timezone is unvalidated free text; a bad zone names its stage."""
    events = [_event(_local(2025, 6, 1, 12, 0))]
    session_factory, session = make_mock_session_factory()

    with patch(f"{INGEST}.resolve_decoder_by_name", return_value=StubDecoder(events)), \
            patch.object(DecoderRegistry, "is_remote", return_value=False), \
            _zone_is("Mars/Olympus_Mons"):
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="stub", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.FAILURE
    assert result.failed_stage == STAGE_NORMALIZE
    assert "Mars/Olympus_Mons" in result.error

    row = _reviews(session, f"{STAGE_NORMALIZE}_failure")[0]
    assert row.severity == "error"
    assert row.source_filename == "poll.dat"


@pytest.mark.asyncio
async def test_database_failure_at_the_timezone_stage_becomes_a_timezone_outcome():
    """A dead database during the zone lookup is an outcome, not an exception."""
    session_factory = MagicMock()
    session_factory.return_value.__aenter__ = AsyncMock(
        side_effect=OSError("connection refused")
    )
    session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
    events = [_event(_local(2025, 6, 1, 12, 0))]

    with patch(f"{INGEST}.resolve_decoder_by_name", return_value=StubDecoder(events)), \
            patch.object(DecoderRegistry, "is_remote", return_value=False):
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="stub", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.FAILURE
    assert result.failed_stage == STAGE_TIMEZONE
    assert "connection refused" in result.error


# ---------------------------------------------------------------------------
# Findings reach the review queue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spring_forward_gap_is_ingested_and_flagged():
    """Flag, never block: the gap event lands AND raises a review row."""
    gap = _event(_local(2025, 3, 9, 2, 30))
    session_factory, session = make_mock_session_factory()
    _allow_dedupe(session)

    with patch(f"{INGEST}.resolve_decoder_by_name", return_value=StubDecoder([gap])), \
            patch.object(DecoderRegistry, "is_remote", return_value=False), \
            _zone_is(EASTERN), \
            patch(f"{INGEST}.persist_events_with_drift_check", new=AsyncMock(return_value=1)):
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="stub", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.SUCCESS
    assert result.events_inserted == 1
    assert gap.timestamp == _utc(2025, 3, 9, 7, 30)

    row = _reviews(session, SPRING_FORWARD_REASON)[0]
    assert row.severity == "warning"
    assert row.detail["zone"] == EASTERN
    assert row.detail["gap_event_count"] == 1
    assert row.source_filename == "poll.dat"


@pytest.mark.asyncio
async def test_deployment_default_zone_reaches_the_review_queue():
    """The seeded default must not silently label controller-local time as UTC."""
    events = [_event(_local(2025, 6, 1, 12, 0))]
    session_factory, session = make_mock_session_factory()
    _allow_dedupe(session)

    with patch(f"{INGEST}.resolve_decoder_by_name", return_value=StubDecoder(events)), \
            patch.object(DecoderRegistry, "is_remote", return_value=False), \
            _zone_is("UTC", ZONE_FROM_DEFAULT), \
            patch(f"{INGEST}.persist_events_with_drift_check", new=AsyncMock(return_value=1)):
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="stub", filename="poll.dat",
        )

    # Flagged, not withheld: the events are still ingested.
    assert result.outcome is IngestOutcome.SUCCESS
    assert result.events_inserted == 1

    row = _reviews(session, UNRESOLVED_TIMEZONE_REASON)[0]
    assert row.severity == "warning"
    assert row.detail["assumed_zone"] == "UTC"
    assert row.detail["zone_origin"] == ZONE_FROM_DEFAULT


@pytest.mark.asyncio
async def test_a_zone_set_on_the_signal_raises_no_timezone_finding():
    """A signal with its own zone is configured, not a finding."""
    events = [_event(_local(2025, 6, 1, 12, 0))]
    session_factory, session = make_mock_session_factory()
    _allow_dedupe(session)

    with patch(f"{INGEST}.resolve_decoder_by_name", return_value=StubDecoder(events)), \
            patch.object(DecoderRegistry, "is_remote", return_value=False), \
            _zone_is(EASTERN, ZONE_FROM_SIGNAL), \
            patch(f"{INGEST}.persist_events_with_drift_check", new=AsyncMock(return_value=1)):
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="stub", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.SUCCESS
    assert events[0].timestamp == _utc(2025, 6, 1, 16, 0)
    assert _reviews(session, UNRESOLVED_TIMEZONE_REASON) == []


@pytest.mark.asyncio
async def test_persist_failure_writes_a_review_row():
    """A persist failure reaches the worklist, as a decode failure does."""
    events = [_event(_utc(2025, 6, 1, 12, 0))]
    session_factory, session = make_mock_session_factory()

    with patch(f"{INGEST}.resolve_decoder_by_name", return_value=StubDecoder(events)), \
            patch.object(DecoderRegistry, "is_remote", return_value=False), \
            patch(f"{INGEST}.persist_events_with_drift_check",
                  new=AsyncMock(side_effect=RuntimeError("deadlock detected"))):
        result = await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="stub", filename="poll.dat",
        )

    assert result.outcome is IngestOutcome.FAILURE
    assert result.failed_stage == STAGE_PERSIST

    row = _reviews(session, f"{STAGE_PERSIST}_failure")[0]
    assert row.severity == "error"
    assert row.detail["stage"] == STAGE_PERSIST
    assert "deadlock detected" in row.detail["error"]
    assert row.status == "open"


@pytest.mark.asyncio
async def test_review_row_names_the_file_not_the_decoder():
    """`source_filename` identifies the source. A decoder name is not a source."""
    events = [_event(_utc(2025, 6, 1, 12, 0))]
    session_factory, session = make_mock_session_factory()

    with patch(f"{INGEST}.resolve_decoder_by_name", return_value=StubDecoder(events)), \
            patch.object(DecoderRegistry, "is_remote", return_value=False), \
            patch(f"{INGEST}.persist_events_with_drift_check",
                  new=AsyncMock(side_effect=RuntimeError("deadlock detected"))):
        await ingest_raw(
            b"raw", device_id="SIG_001", session_factory=session_factory,
            decoder_name="maxtime", filename="ATSPM_1001_20250601.dat",
        )

    rows = _reviews(session)
    assert rows, "the persist failure must have written a review row"
    for row in rows:
        assert row.source_filename == "ATSPM_1001_20250601.dat"
        assert row.source_filename != "maxtime"
