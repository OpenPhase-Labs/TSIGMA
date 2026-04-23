"""Shared helpers for analytics endpoints."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from ....models import ControllerEventLog

logger = logging.getLogger(__name__)

CEL = ControllerEventLog


def _default_start() -> datetime:
    """Default start time: 24 hours ago."""
    return datetime.now(timezone.utc) - timedelta(hours=24)


def _default_end() -> datetime:
    """Default end time: now."""
    return datetime.now(timezone.utc)


@dataclass
class CycleStats:
    """Computed cycle length statistics from a list of event times."""

    cycles: list[float]
    avg_cycle: float
    deviations: list[float]

    @property
    def count(self) -> int:
        return len(self.cycles)


def _compute_cycle_stats(times: list[datetime]) -> CycleStats:
    """
    Compute cycle lengths and deviations from a sorted list of event times.

    Args:
        times: Sorted list of event timestamps (must have >= 2 entries).

    Returns:
        CycleStats with cycle lengths, average, and absolute deviations.
    """
    cycles = [
        (times[i + 1] - times[i]).total_seconds()
        for i in range(len(times) - 1)
    ]
    avg_cycle = sum(cycles) / len(cycles)
    deviations = [abs(c - avg_cycle) for c in cycles]
    return CycleStats(cycles=cycles, avg_cycle=avg_cycle, deviations=deviations)
