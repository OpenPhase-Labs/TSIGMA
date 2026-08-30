"""Checkpoint-advancement policy.

Turns an ingest outcome into a decision about the checkpoint. Host-owned and
kept separate from `ingest_raw` so the rule is testable on its own and so an
untrusted method plugin never decides whether its own cursor advances.

The rule (plan P8):

* SUCCESS            -> advance
* event-time PARTIAL -> advance to last-good (`max_event_time`); the tail is
                        re-attempted next cycle
* file-identity PARTIAL and every FAILURE
                     -> do NOT advance, record the error, and bubble to an alert
                        once failures repeat

Never advancing past unread data is what keeps re-polling able to recover it;
advancing on a failure is how data is silently lost.
"""

import enum
import logging
from dataclasses import dataclass
from datetime import datetime

from .ingest import IngestOutcome, IngestResult

logger = logging.getLogger(__name__)

# Consecutive failures before a device's trouble is escalated beyond the log.
DEFAULT_ALERT_AFTER_FAILURES = 3


class Advancement(str, enum.Enum):
    """What the caller should do with the checkpoint."""

    ADVANCE = "advance"                  # move the cursor to the newest ingested event
    ADVANCE_TO_LAST_GOOD = "last_good"   # partial: move only as far as we actually read
    HOLD = "hold"                        # do not move; record the error


@dataclass
class AdvancementDecision:
    """The policy's verdict for one ingest."""

    action: Advancement
    last_event_timestamp: datetime | None = None
    error: str = ""
    alert: bool = False

    @property
    def advances(self) -> bool:
        return self.action is not Advancement.HOLD


def decide_advancement(
    result: IngestResult,
    *,
    consecutive_errors: int = 0,
    alert_after: int = DEFAULT_ALERT_AFTER_FAILURES,
) -> AdvancementDecision:
    """Decide the checkpoint action for one ingest result.

    `consecutive_errors` is the count BEFORE this ingest, so the alert fires on
    the run that reaches the threshold rather than one cycle late.
    """
    if result.outcome is IngestOutcome.SUCCESS:
        return AdvancementDecision(
            Advancement.ADVANCE, last_event_timestamp=result.max_event_time,
        )

    if result.outcome is IngestOutcome.PARTIAL:
        if result.max_event_time is None:
            # Nothing readable landed, so there is no last-good point to move to.
            # Treated as a hold: advancing here would skip the whole payload.
            return _hold(result, consecutive_errors, alert_after)
        return AdvancementDecision(
            Advancement.ADVANCE_TO_LAST_GOOD,
            last_event_timestamp=result.max_event_time,
            error=result.error,
        )

    return _hold(result, consecutive_errors, alert_after)


def _hold(result: IngestResult, consecutive_errors: int, alert_after: int
          ) -> AdvancementDecision:
    failures = consecutive_errors + 1
    return AdvancementDecision(
        Advancement.HOLD,
        error=result.error or "ingest failed",
        alert=failures >= alert_after,
    )
