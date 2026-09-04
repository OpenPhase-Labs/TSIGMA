"""The wire has to preserve which clock a decoder's timestamps are on.

TYPES.md pins `event_time` as a bare `timestamp[us]` and carries the anchoring
in the `tsigma.time_semantics` schema-metadata key. That indirection is not
ceremony: a tz-typed column would hand back every naive controller-local instant
as UTC, and `ingest_raw` dispatches on naive-vs-aware, so the whole batch would
be reinterpreted as absolute time and shifted by the local offset.
"""

from datetime import datetime, timezone

import pyarrow as pa
import pytest

import tsigma.plugins  # noqa: F401  # puts the generated stubs on sys.path
from tsigma.plugins.remote_decoder import (
    TIME_SEMANTICS_KEY,
    TIME_SEMANTICS_NAIVE_LOCAL,
    TIME_SEMANTICS_UTC,
    MissingTimeSemantics,
    arrow_batch_to_events,
    events_to_arrow_batch,
)

from tsigma.collection.decoders.base import DecodedEvent

UTC_INSTANT = datetime(2026, 1, 15, 17, 30, tzinfo=timezone.utc)
LOCAL_INSTANT = datetime(2026, 1, 15, 17, 30)


def _event(ts):
    return DecodedEvent(timestamp=ts, event_code=1, event_param=0)


class TestTheRoundTripIsLossless:
    def test_a_utc_instant_comes_back_aware_and_equal(self):
        blob = events_to_arrow_batch([_event(UTC_INSTANT)],
                                     time_semantics=TIME_SEMANTICS_UTC)
        back = arrow_batch_to_events(blob)[0].timestamp
        assert back == UTC_INSTANT
        assert back.tzinfo is not None, (
            "a naive return would be read as controller-local and shifted"
        )

    def test_a_local_instant_comes_back_naive_and_equal(self):
        blob = events_to_arrow_batch([_event(LOCAL_INSTANT)],
                                     time_semantics=TIME_SEMANTICS_NAIVE_LOCAL)
        back = arrow_batch_to_events(blob)[0].timestamp
        assert back == LOCAL_INSTANT
        assert back.tzinfo is None, (
            "an aware return would label controller-local time as absolute UTC"
        )

    def test_the_two_anchorings_are_distinguishable_on_the_wire(self):
        # Same wall-clock fields, different meaning. If the wire collapsed them
        # the host could not tell 17:30 UTC from 17:30 local.
        utc = arrow_batch_to_events(
            events_to_arrow_batch([_event(UTC_INSTANT)],
                                  time_semantics=TIME_SEMANTICS_UTC))[0].timestamp
        local = arrow_batch_to_events(
            events_to_arrow_batch([_event(LOCAL_INSTANT)],
                                  time_semantics=TIME_SEMANTICS_NAIVE_LOCAL))[0].timestamp
        assert (utc.tzinfo is None) != (local.tzinfo is None)


class TestTheCanonicalSchema:
    def test_the_column_is_named_and_typed_as_the_contract_pins_it(self):
        blob = events_to_arrow_batch([_event(UTC_INSTANT)])
        with pa.ipc.open_stream(pa.BufferReader(blob)) as reader:
            schema = reader.read_all().schema
        assert schema.field("event_time").type == pa.timestamp("us"), (
            "a tz-typed column makes naive_local unrepresentable"
        )
        assert {"event_time", "event_code", "event_param"} <= set(schema.names)

    def test_every_batch_carries_its_anchoring(self):
        blob = events_to_arrow_batch([_event(LOCAL_INSTANT)],
                                     time_semantics=TIME_SEMANTICS_NAIVE_LOCAL)
        with pa.ipc.open_stream(pa.BufferReader(blob)) as reader:
            meta = reader.read_all().schema.metadata
        assert meta[TIME_SEMANTICS_KEY] == TIME_SEMANTICS_NAIVE_LOCAL


class TestAnUndeclaredBatchIsRefused:
    """TYPES.md: a missing or unspecified key is a contract violation the host
    flags - it never silently assumes UTC."""

    @staticmethod
    def _batch_without_metadata():
        table = pa.table({
            "event_time": pa.array([LOCAL_INSTANT], type=pa.timestamp("us")),
            "event_code": pa.array([1], type=pa.int32()),
            "event_param": pa.array([0], type=pa.int32()),
        })
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes()

    def test_a_batch_with_no_key_raises_rather_than_assuming_utc(self):
        with pytest.raises(MissingTimeSemantics):
            arrow_batch_to_events(self._batch_without_metadata())

    def test_an_unrecognised_value_raises(self):
        blob = events_to_arrow_batch([_event(UTC_INSTANT)],
                                     time_semantics=b"sidereal")
        with pytest.raises(MissingTimeSemantics):
            arrow_batch_to_events(blob)


class TestDescribeAndTheWireMustAgree:
    """`Describe` declares the anchoring once; every batch repeats it. If they
    disagree the decoder contradicts itself, and believing either one silently
    moves every timestamp by a whole offset."""

    def test_a_contradiction_is_refused(self):
        blob = events_to_arrow_batch([_event(LOCAL_INSTANT)],
                                     time_semantics=TIME_SEMANTICS_NAIVE_LOCAL)
        with pytest.raises(MissingTimeSemantics, match="declared utc"):
            arrow_batch_to_events(blob, declared=TIME_SEMANTICS_UTC)

    def test_agreement_passes(self):
        blob = events_to_arrow_batch([_event(LOCAL_INSTANT)],
                                     time_semantics=TIME_SEMANTICS_NAIVE_LOCAL)
        assert len(arrow_batch_to_events(blob,
                                         declared=TIME_SEMANTICS_NAIVE_LOCAL)) == 1
