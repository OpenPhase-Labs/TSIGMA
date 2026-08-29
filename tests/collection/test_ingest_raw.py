"""P7: the host-owned ingest orchestrator.

Covers the timezone Phase B absorbed into P7 — decoders emit naive for
controller-local sources, and conversion happens HERE, once, for every method.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from tsigma.collection.decoders.base import DecodedEvent, DecodeResult
from tsigma.collection.ingest import (
    IngestOutcome,
    IngestResult,
    has_naive_timestamps,
    ingest_raw,
    normalize_event_times,
)

NY = "America/New_York"


def _ev(ts, code=1, param=0):
    return DecodedEvent(timestamp=ts, event_code=code, event_param=param)


def _factory():
    def factory():
        s = AsyncMock()
        s.__aenter__.return_value = s
        s.__aexit__.return_value = False
        return s
    return factory


class TestNaiveDetection:
    def test_detects_naive(self):
        assert has_naive_timestamps([_ev(datetime(2026, 8, 1, 12, 0))]) is True

    def test_aware_is_not_naive(self):
        assert has_naive_timestamps([_ev(datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc))]) is False

    def test_mixed_counts_as_naive(self):
        events = [
            _ev(datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)),
            _ev(datetime(2026, 8, 1, 12, 1)),
        ]
        assert has_naive_timestamps(events) is True


class TestNormalization:
    def test_converts_local_to_utc(self):
        events = [_ev(datetime(2026, 8, 1, 12, 0))]
        normalize_event_times(events, NY)
        assert events[0].timestamp == datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)

    def test_aware_events_pass_through_untouched(self):
        original = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
        events = [_ev(original)]
        normalize_event_times(events, NY)
        assert events[0].timestamp == original

    def test_one_decoder_may_emit_both_kinds(self):
        """maxtime emits local from XML and epoch-UTC from binary."""
        aware = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
        events = [_ev(datetime(2026, 8, 1, 12, 0)), _ev(aware)]
        normalize_event_times(events, NY)
        assert events[0].timestamp == datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)
        assert events[1].timestamp == aware

    def test_fall_back_fold_on_a_backwards_step(self):
        events = [
            _ev(datetime(2026, 11, 1, 1, 30)),
            _ev(datetime(2026, 11, 1, 1, 45)),
            _ev(datetime(2026, 11, 1, 1, 30)),   # clock went back
            _ev(datetime(2026, 11, 1, 1, 45)),
        ]
        normalize_event_times(events, NY)
        assert [e.timestamp.hour for e in events] == [5, 5, 6, 6]

    def test_spring_forward_gap_is_ingested_not_withheld(self):
        events = [_ev(datetime(2026, 3, 8, 2, 30))]   # nonexistent local time
        normalize_event_times(events, NY)
        assert events[0].timestamp.tzinfo is not None


class TestIngestRaw:
    @pytest.mark.asyncio
    async def test_unknown_decoder_is_failure_not_an_exception(self):
        out = await ingest_raw(b"x", device_id="S", decoder_name="nope",
                               session_factory=_factory())
        assert out.outcome is IngestOutcome.FAILURE
        assert out.advanced is False
        assert "nope" in out.error

    @pytest.mark.asyncio
    async def test_decoder_exception_is_contained(self):
        boom = SimpleNamespace(decode=lambda raw: (_ for _ in ()).throw(ValueError("bad file")))
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=boom):
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        assert out.outcome is IngestOutcome.FAILURE
        assert "bad file" in out.error

    @pytest.mark.asyncio
    async def test_empty_decode_is_success_with_zero(self):
        empty = SimpleNamespace(decode=lambda raw: DecodeResult(events=[]))
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=empty):
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        assert out.outcome is IngestOutcome.SUCCESS
        assert out.events_inserted == 0

    @pytest.mark.asyncio
    async def test_aware_events_need_no_timezone_lookup(self):
        aware = SimpleNamespace(
            decode=lambda raw: DecodeResult(
                events=[_ev(datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc))]
            )
        )
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=aware), \
             patch("tsigma.collection.ingest.resolve_source_timezone",
                   new_callable=AsyncMock) as tz, \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1):
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        tz.assert_not_awaited()
        assert out.outcome is IngestOutcome.SUCCESS

    @pytest.mark.asyncio
    async def test_local_events_are_converted_before_persist(self):
        local = SimpleNamespace(
            decode=lambda raw: DecodeResult(events=[_ev(datetime(2026, 8, 1, 12, 0))])
        )
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=local), \
             patch("tsigma.collection.ingest.resolve_source_timezone",
                   new_callable=AsyncMock, return_value=NY), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1) as persist:
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        persisted = persist.call_args[0][0]
        assert persisted[0].timestamp == datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)
        assert out.max_event_time == datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_unresolvable_timezone_still_ingests_and_warns(self):
        """Never withhold (ADR-0034); spec sec.3 rules out an unresolvable-zone quarantine."""
        local = SimpleNamespace(
            decode=lambda raw: DecodeResult(events=[_ev(datetime(2026, 8, 1, 12, 0))])
        )
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=local), \
             patch("tsigma.collection.ingest.resolve_source_timezone",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1) as persist, \
             patch("tsigma.collection.ingest.logger") as log:
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        assert out.outcome is IngestOutcome.SUCCESS
        assert out.advanced is True
        persist.assert_awaited_once()
        log.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_last_resort_zone_is_utc(self):
        local = SimpleNamespace(
            decode=lambda raw: DecodeResult(events=[_ev(datetime(2026, 8, 1, 12, 0))])
        )
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=local), \
             patch("tsigma.collection.ingest.resolve_source_timezone",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1) as persist:
            await ingest_raw(b"x", device_id="S", decoder_name="d",
                             session_factory=_factory())
        # 12:00 naive interpreted as UTC stays 12:00Z - no silent shift.
        assert persist.call_args[0][0][0].timestamp == datetime(
            2026, 8, 1, 12, 0, tzinfo=timezone.utc
        )

    @pytest.mark.asyncio
    async def test_returns_the_insert_count_and_high_water_mark(self):
        events = [
            _ev(datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)),
            _ev(datetime(2026, 8, 1, 12, 5, tzinfo=timezone.utc)),
        ]
        dec = SimpleNamespace(decode=lambda raw: DecodeResult(events=events))
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=dec), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=2):
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        assert out.events_inserted == 2
        assert out.max_event_time == datetime(2026, 8, 1, 12, 5, tzinfo=timezone.utc)
        assert out.advanced is True


class TestIngestResult:
    def test_failure_does_not_advance(self):
        assert IngestResult(IngestOutcome.FAILURE, 0).advanced is False

    def test_partial_advances_to_the_high_water_mark(self):
        r = IngestResult(IngestOutcome.PARTIAL, 5, datetime(2026, 8, 1, tzinfo=timezone.utc))
        assert r.advanced is True
