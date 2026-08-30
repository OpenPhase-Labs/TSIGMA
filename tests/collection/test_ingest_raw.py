"""P7: the host-owned ingest orchestrator.

Covers the timezone Phase B absorbed into P7 — decoders emit naive for
controller-local sources, and conversion happens HERE, once, for every method.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.decoders.base import DecodedEvent, DecodeResult
from tsigma.collection.ingest import (
    UNRESOLVED_TIMEZONE_REASON,
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
             patch("tsigma.collection.ingest._flag_unresolved_timezone",
                   new_callable=AsyncMock), \
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
             patch("tsigma.collection.ingest._flag_unresolved_timezone",
                   new_callable=AsyncMock), \
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


class TestUnresolvedTimezoneFlag:
    """ADR-0034: ingest + FLAG + needs-review. Logging alone is not flagging."""

    @staticmethod
    def _local_decoder():
        return SimpleNamespace(
            decode=lambda raw: DecodeResult(events=[_ev(datetime(2026, 8, 1, 12, 0))])
        )

    @staticmethod
    def _session_capture(existing=None):
        """Session factory whose added rows are inspectable.

        `execute` is async and its RESULT is sync - a MagicMock, not an
        AsyncMock, so `.first()` returns a value rather than an unawaited
        coroutine.
        """
        added = []
        session = AsyncMock()
        session.__aenter__.return_value = session
        session.__aexit__.return_value = False
        session.add = MagicMock(side_effect=lambda row: added.append(row))
        result = MagicMock()
        result.first = MagicMock(return_value=existing)
        session.execute = AsyncMock(return_value=result)
        return (lambda: session), added, session

    @pytest.mark.asyncio
    async def test_queues_a_review_when_no_zone_resolves(self):
        factory, added, session = self._session_capture()
        with patch("tsigma.collection.ingest.resolve_decoder_by_name",
                   return_value=self._local_decoder()), \
             patch("tsigma.collection.ingest.resolve_source_timezone",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.ingest.validate_and_record_provenance",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1):
            out = await ingest_raw(b"x", device_id="SIG-1", decoder_name="d",
                                   session_factory=factory)
        assert out.outcome is IngestOutcome.SUCCESS      # still ingested
        review = next(r for r in added if getattr(r, "reason", None))
        assert review.reason == UNRESOLVED_TIMEZONE_REASON
        assert review.signal_id == "SIG-1"
        assert review.status == "open"
        assert review.detail["assumed_zone"] == "UTC"
        session.commit.assert_awaited()

    @pytest.mark.asyncio
    async def test_does_not_duplicate_an_open_review(self):
        """A deployment-wide misconfiguration recurs every poll; one row is enough."""
        factory, added, _ = self._session_capture(existing=("some-review-id",))
        with patch("tsigma.collection.ingest.resolve_decoder_by_name",
                   return_value=self._local_decoder()), \
             patch("tsigma.collection.ingest.resolve_source_timezone",
                   new_callable=AsyncMock, return_value=None), \
             patch("tsigma.collection.ingest.validate_and_record_provenance",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1):
            await ingest_raw(b"x", device_id="SIG-1", decoder_name="d",
                             session_factory=factory)
        assert added == []

    @pytest.mark.asyncio
    async def test_flagging_swallows_db_failures(self):
        """never-lose-data outranks flagging: the flag must not raise."""
        from tsigma.collection.ingest import _flag_unresolved_timezone

        broken = AsyncMock()
        broken.__aenter__.side_effect = RuntimeError("db down")
        # Must return normally, not raise - ingest continues either way.
        await _flag_unresolved_timezone(lambda: broken, "SIG-1")


class TestPersistFailure:
    @pytest.mark.asyncio
    async def test_persist_failure_is_an_outcome_not_an_exception(self):
        """An escaping exception would leave a watched file unquarantined."""
        dec = SimpleNamespace(
            decode=lambda raw: DecodeResult(
                events=[_ev(datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc))]
            )
        )
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=dec), \
             patch("tsigma.collection.ingest.validate_and_record_provenance",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, side_effect=RuntimeError("db down")):
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        assert out.outcome is IngestOutcome.FAILURE
        assert out.failed_stage == "persist"
        assert "db down" in out.error
        assert out.advanced is False

    @pytest.mark.asyncio
    async def test_decode_failure_is_staged_as_decode(self):
        boom = SimpleNamespace(decode=lambda raw: (_ for _ in ()).throw(ValueError("bad")))
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=boom):
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        assert out.failed_stage == "decode"

    @pytest.mark.asyncio
    async def test_success_has_no_failed_stage(self):
        dec = SimpleNamespace(decode=lambda raw: DecodeResult(events=[]))
        with patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=dec):
            out = await ingest_raw(b"x", device_id="S", decoder_name="d",
                                   session_factory=_factory())
        assert out.failed_stage == ""


class TestRemoteDecoderPath:
    """P6b/P6c: a gRPC decoder plugin, and its 3-state terminal status.

    Dispatch is per NAME (ADR-0018 coexistence): a name registered over gRPC
    resolves to a plugin, everything else to the in-process registry.
    """

    @staticmethod
    def _remote(outcome, events=(), error="", dropped=0):
        """A stand-in RemoteDecoder returning a given terminal status."""
        from tsigma.collection.decoders.base import DecodeResult as DR
        from tsigma.plugins.remote_decoder import RemoteDecodeResult

        out = RemoteDecodeResult(
            result=DR(events=list(events)),
            outcome=outcome,
            events_emitted=len(events),
            error=error,
            segments_dropped=dropped,
        )
        return SimpleNamespace(
            name="vendor-decoder",
            decode_remote=AsyncMock(return_value=out),
        )

    @staticmethod
    def _register(decoder):
        from tsigma.collection.decoders.base import DecoderRegistry

        DecoderRegistry.register_grpc(
            "vendor-decoder", SimpleNamespace(decoder=decoder)
        )
        return DecoderRegistry

    @pytest.fixture(autouse=True)
    def _clean(self):
        from tsigma.collection.decoders.base import DecoderRegistry

        yield
        DecoderRegistry._grpc_plugins.clear()

    @pytest.mark.asyncio
    async def test_a_grpc_registered_name_dispatches_to_the_plugin(self):
        now = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
        decoder = self._remote(1, [_ev(now)])          # DECODE_OUTCOME_SUCCESS
        self._register(decoder)
        with patch("tsigma.collection.ingest.validate_and_record_provenance",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1):
            out = await ingest_raw(b"x", device_id="S", decoder_name="vendor-decoder",
                                   session_factory=_factory())
        decoder.decode_remote.assert_awaited_once_with(b"x")
        assert out.outcome is IngestOutcome.SUCCESS

    @pytest.mark.asyncio
    async def test_partial_persists_the_decodable_rows_and_flags(self):
        now = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
        decoder = self._remote(2, [_ev(now)], error="truncated", dropped=3)
        self._register(decoder)
        with patch("tsigma.collection.ingest.validate_and_record_provenance",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1) as persist, \
             patch("tsigma.collection.ingest._flag_decode_outcome",
                   new_callable=AsyncMock) as flag:
            out = await ingest_raw(b"x", device_id="S", decoder_name="vendor-decoder",
                                   session_factory=_factory())
        assert out.outcome is IngestOutcome.PARTIAL
        assert out.events_inserted == 1        # what survived went in
        assert out.advanced is True            # PARTIAL advances to the high-water mark
        persist.assert_awaited_once()
        flag.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failure_persists_nothing_and_flags(self):
        decoder = self._remote(3, [], error="unreadable header")
        self._register(decoder)
        with patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock) as persist, \
             patch("tsigma.collection.ingest._flag_decode_outcome",
                   new_callable=AsyncMock) as flag:
            out = await ingest_raw(b"x", device_id="S", decoder_name="vendor-decoder",
                                   session_factory=_factory())
        assert out.outcome is IngestOutcome.FAILURE
        assert out.advanced is False
        assert "unreadable header" in out.error
        persist.assert_not_awaited()
        flag.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unspecified_outcome_is_treated_as_failure(self):
        """A decoder that reports nothing has not reported success."""
        decoder = self._remote(0, [])
        self._register(decoder)
        with patch("tsigma.collection.ingest._flag_decode_outcome",
                   new_callable=AsyncMock):
            out = await ingest_raw(b"x", device_id="S", decoder_name="vendor-decoder",
                                   session_factory=_factory())
        assert out.outcome is IngestOutcome.FAILURE

    @pytest.mark.asyncio
    async def test_remote_naive_timestamps_are_converted_host_side(self):
        """P6b: the same naive->UTC rule applies whatever decoded the bytes."""
        decoder = self._remote(1, [_ev(datetime(2026, 8, 1, 12, 0))])   # naive
        self._register(decoder)
        with patch("tsigma.collection.ingest.resolve_source_timezone",
                   new_callable=AsyncMock, return_value=NY), \
             patch("tsigma.collection.ingest.validate_and_record_provenance",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.persist_events_with_drift_check",
                   new_callable=AsyncMock, return_value=1) as persist:
            await ingest_raw(b"x", device_id="S", decoder_name="vendor-decoder",
                             session_factory=_factory())
        persisted = persist.call_args[0][0]
        assert persisted[0].timestamp == datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_an_unregistered_name_still_uses_the_in_process_registry(self):
        """Regression guard: coexistence, not replacement."""
        dec = SimpleNamespace(decode=lambda raw: DecodeResult(events=[]))
        with patch("tsigma.collection.ingest.resolve_decoder_by_name",
                   return_value=dec) as resolve:
            out = await ingest_raw(b"x", device_id="S", decoder_name="asc3",
                                   session_factory=_factory())
        resolve.assert_called_once_with("asc3")
        assert out.outcome is IngestOutcome.SUCCESS
