"""P8: the checkpoint-advancement policy.

The rule that decides whether a device's cursor moves. Getting it wrong in the
permissive direction loses data silently - the checkpoint skips past records
that were never read and re-polling can no longer recover them.
"""

from datetime import datetime, timezone

import pytest

from tsigma.collection.advancement import (
    DEFAULT_ALERT_AFTER_FAILURES,
    Advancement,
    decide_advancement,
)
from tsigma.collection.ingest import IngestOutcome, IngestResult

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def _result(outcome, inserted=0, mark=None, error="", decoded=0):
    return IngestResult(outcome, inserted, mark, events_decoded=decoded, error=error)


class TestSuccess:
    def test_advances_to_the_newest_event(self):
        d = decide_advancement(_result(IngestOutcome.SUCCESS, 5, NOW, decoded=5))
        assert d.action is Advancement.ADVANCE
        assert d.last_event_timestamp == NOW
        assert d.advances is True
        assert d.alert is False

    def test_an_empty_but_successful_poll_still_advances(self):
        """Nothing new is not a failure; the cursor moves on time, not on rows."""
        d = decide_advancement(_result(IngestOutcome.SUCCESS, 0, None))
        assert d.action is Advancement.ADVANCE
        assert d.advances is True


class TestPartial:
    def test_advances_only_to_the_last_good_event(self):
        """The tail that could not be read must be re-attempted next cycle."""
        d = decide_advancement(
            _result(IngestOutcome.PARTIAL, 3, NOW, error="truncated at 8192", decoded=9)
        )
        assert d.action is Advancement.ADVANCE_TO_LAST_GOOD
        assert d.last_event_timestamp == NOW
        assert d.advances is True
        assert "truncated" in d.error

    def test_partial_with_no_readable_rows_holds(self):
        """No last-good point exists, so advancing would skip the whole payload."""
        d = decide_advancement(_result(IngestOutcome.PARTIAL, 0, None))
        assert d.action is Advancement.HOLD
        assert d.advances is False


class TestFailure:
    def test_never_advances(self):
        d = decide_advancement(_result(IngestOutcome.FAILURE, 0, None, error="bad header"))
        assert d.action is Advancement.HOLD
        assert d.advances is False
        assert d.error == "bad header"

    def test_carries_a_default_reason_when_none_given(self):
        assert decide_advancement(_result(IngestOutcome.FAILURE)).error == "ingest failed"

    def test_a_failure_never_advances_even_with_a_high_water_mark(self):
        """A stale mark from an earlier attempt must not license advancing."""
        d = decide_advancement(_result(IngestOutcome.FAILURE, 0, NOW, error="db down"))
        assert d.advances is False


class TestAlerting:
    def test_first_failure_does_not_alert(self):
        assert decide_advancement(_result(IngestOutcome.FAILURE)).alert is False

    def test_alerts_on_the_run_that_reaches_the_threshold(self):
        """consecutive_errors is the count BEFORE this ingest, so it fires on time."""
        d = decide_advancement(
            _result(IngestOutcome.FAILURE),
            consecutive_errors=DEFAULT_ALERT_AFTER_FAILURES - 1,
        )
        assert d.alert is True

    def test_keeps_alerting_while_it_stays_broken(self):
        d = decide_advancement(_result(IngestOutcome.FAILURE), consecutive_errors=20)
        assert d.alert is True

    def test_threshold_is_configurable(self):
        d = decide_advancement(_result(IngestOutcome.FAILURE), alert_after=1)
        assert d.alert is True

    def test_success_after_failures_never_alerts(self):
        d = decide_advancement(
            _result(IngestOutcome.SUCCESS, 1, NOW), consecutive_errors=99
        )
        assert d.alert is False
        assert d.action is Advancement.ADVANCE

    def test_partial_with_a_mark_does_not_alert(self):
        """Partial progress is progress, not a failure run."""
        d = decide_advancement(
            _result(IngestOutcome.PARTIAL, 1, NOW), consecutive_errors=99
        )
        assert d.alert is False


class TestPolicyShape:
    @pytest.mark.parametrize(
        "outcome,expected",
        [
            (IngestOutcome.SUCCESS, True),
            (IngestOutcome.PARTIAL, False),   # no mark -> hold
            (IngestOutcome.FAILURE, False),
        ],
    )
    def test_advances_property_matches_the_action(self, outcome, expected):
        assert decide_advancement(_result(outcome)).advances is expected
