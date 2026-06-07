"""Unit tests for the pure temporal-integrity (clock-offset) check.

Covers ``compute_clock_offset`` (signed newest-event vs server-now offset)
and ``check_temporal_integrity`` (tolerance-gated WARNING finding) plus the
shared ``IntegrityFinding`` dataclass. These are pure, synchronous, no-DB
functions living beside ``is_backward_poisoned`` in
``tsigma.collection.sdk``.
"""

from datetime import datetime, timedelta, timezone

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.collection.sdk import (
    IntegrityFinding,
    check_temporal_integrity,
    compute_clock_offset,
)
from tsigma.notifications.registry import WARNING
from tsigma.notifications.suppression import CHECK_TEMPORAL_INTEGRITY

_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _evt(ts: datetime) -> DecodedEvent:
    return DecodedEvent(timestamp=ts, event_code=1, event_param=0)


class TestComputeClockOffset:
    def test_empty_events_returns_none(self):
        assert compute_clock_offset([], _NOW) is None

    def test_future_newest_is_positive_offset(self):
        # newest event 100s AFTER server_now -> positive offset
        events = [_evt(_NOW + timedelta(seconds=100))]
        assert compute_clock_offset(events, _NOW) == timedelta(seconds=100)

    def test_past_newest_is_negative_offset(self):
        # newest event 100s BEFORE server_now -> negative offset
        events = [_evt(_NOW - timedelta(seconds=100))]
        assert compute_clock_offset(events, _NOW) == timedelta(seconds=-100)

    def test_uses_max_timestamp(self):
        # multiple events -> offset computed from the newest (max) timestamp
        events = [
            _evt(_NOW - timedelta(hours=2)),
            _evt(_NOW + timedelta(seconds=100)),
            _evt(_NOW - timedelta(minutes=5)),
        ]
        assert compute_clock_offset(events, _NOW) == timedelta(seconds=100)


class TestCheckTemporalIntegrity:
    def test_no_events_returns_none(self):
        assert check_temporal_integrity([], _NOW, 300) is None

    def test_within_tolerance_returns_none(self):
        # offset 100s, tolerance 300 -> within -> None
        events = [_evt(_NOW + timedelta(seconds=100))]
        assert check_temporal_integrity(events, _NOW, 300) is None

    def test_boundary_equal_tolerance_returns_none(self):
        # offset exactly 300s, tolerance 300 -> <= is allowed -> None
        events = [_evt(_NOW + timedelta(seconds=300))]
        assert check_temporal_integrity(events, _NOW, 300) is None

    def test_far_future_returns_finding(self):
        # offset +600s, tolerance 300 -> finding; signed offset positive
        events = [_evt(_NOW + timedelta(seconds=600))]
        finding = check_temporal_integrity(events, _NOW, 300)
        assert finding is not None
        assert isinstance(finding, IntegrityFinding)
        assert finding.check_name == CHECK_TEMPORAL_INTEGRITY
        assert finding.severity == WARNING
        assert finding.detail["offset_seconds"] == 600.0
        assert finding.detail["suggested_correction_seconds"] == -600.0
        assert finding.detail["events"] == 1

    def test_far_past_returns_finding(self):
        # offset -600s, tolerance 300 -> finding; signed offset negative
        events = [_evt(_NOW - timedelta(seconds=600))]
        finding = check_temporal_integrity(events, _NOW, 300)
        assert finding is not None
        assert isinstance(finding, IntegrityFinding)
        assert finding.check_name == CHECK_TEMPORAL_INTEGRITY
        assert finding.severity == WARNING
        assert finding.detail["offset_seconds"] == -600.0
        assert finding.detail["suggested_correction_seconds"] == 600.0
        assert finding.detail["events"] == 1

    def test_finding_event_count_reflects_batch(self):
        # offset comes from the newest (max) timestamp; events count is the batch size
        events = [
            _evt(_NOW - timedelta(hours=1)),
            _evt(_NOW + timedelta(seconds=600)),
        ]
        finding = check_temporal_integrity(events, _NOW, 300)
        assert finding is not None
        assert finding.detail["offset_seconds"] == 600.0
        assert finding.detail["suggested_correction_seconds"] == -600.0
        assert finding.detail["events"] == 2
