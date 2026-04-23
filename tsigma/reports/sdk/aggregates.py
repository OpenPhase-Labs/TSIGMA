"""
Shared aggregate / statistical helpers for report plugins.

Eliminates repeated safe-average, safe-min/max, percentage, and
percentile calculations across report modules.
"""

from __future__ import annotations


def safe_avg(values: list[float], decimals: int = 2) -> float:
    """Return rounded average or 0.0 if *values* is empty."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), decimals)


def safe_min(values: list[float], decimals: int = 1) -> float:
    """Return rounded minimum or 0.0 if *values* is empty."""
    if not values:
        return 0.0
    return round(min(values), decimals)


def safe_max(values: list[float], decimals: int = 1) -> float:
    """Return rounded maximum or 0.0 if *values* is empty."""
    if not values:
        return 0.0
    return round(max(values), decimals)


def pct(count: int | float, total: int | float, decimals: int = 1) -> float:
    """Return percentage or 0.0 if *total* is zero."""
    if not total:
        return 0.0
    return round(count / total * 100, decimals)


def percentile_from_sorted(values: list[float], p: float) -> float:
    """
    Return the *p*-th percentile from an already-sorted list.

    Uses nearest-rank method. *p* should be in [0, 100].
    Returns 0.0 if *values* is empty.
    """
    if not values:
        return 0.0
    count = len(values)
    idx = int(count * p / 100)
    return values[min(idx, count - 1)]
