"""Unit tests for the backward-poison polling-window guard predicate."""

from datetime import datetime, timedelta, timezone

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.collection.sdk import is_backward_poisoned

_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _ev(ts: datetime) -> DecodedEvent:
    return DecodedEvent(timestamp=ts, event_code=1, event_param=0)


class TestBackwardPoisonGuard:
    def test_newest_below_last_polled_is_poisoned(self):
        # whole batch (incl. its newest event) predates the last poll -> slow/reset clock
        events = [_ev(_NOW - timedelta(hours=2)), _ev(_NOW - timedelta(hours=1))]
        assert is_backward_poisoned(events, _NOW) is True

    def test_single_old_event_is_poisoned(self):
        # one stale event, newest == that event, below the watermark -> poison
        assert is_backward_poisoned([_ev(_NOW - timedelta(hours=1))], _NOW) is True

    def test_current_newest_not_poisoned(self):
        # old events but a current newest event = normal overlap, not poison
        last_polled = _NOW - timedelta(minutes=5)
        events = [_ev(_NOW - timedelta(hours=2)), _ev(_NOW)]
        assert is_backward_poisoned(events, last_polled) is False

    def test_newest_equal_to_last_polled_not_poisoned(self):
        # boundary: newest == last_polled is acceptable (not below)
        events = [_ev(_NOW)]
        assert is_backward_poisoned(events, _NOW) is False

    def test_first_poll_no_checkpoint_skips(self):
        # no reference time yet -> cannot judge -> not poisoned
        events = [_ev(_NOW - timedelta(days=10))]
        assert is_backward_poisoned(events, None) is False

    def test_empty_events_not_poisoned(self):
        assert is_backward_poisoned([], _NOW) is False
