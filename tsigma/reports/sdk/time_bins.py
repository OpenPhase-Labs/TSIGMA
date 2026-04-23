"""
Time parsing and time-bin helpers used by every report plugin.
"""

from datetime import datetime
from typing import Any


def parse_time(value: Any) -> datetime:
    """
    Coerce a report parameter to a datetime.

    Accepts either a `datetime` or an ISO-8601 string.  This exists so
    every report stops repeating the same isinstance check inline.
    """
    return datetime.fromisoformat(value) if isinstance(value, str) else value


def bin_timestamp(dt: datetime, bin_size_minutes: int) -> str:
    """
    Truncate `dt` to the start of its `bin_size_minutes` bin and
    return the result as an ISO-8601 string.
    """
    minute = (dt.minute // bin_size_minutes) * bin_size_minutes
    return dt.replace(minute=minute, second=0, microsecond=0).isoformat()


def bin_index(moment: datetime, start: datetime, bin_minutes: int) -> int:
    """Integer bin index of `moment` within a window starting at `start`."""
    return int((moment - start).total_seconds() // (bin_minutes * 60))


def total_bins(start: datetime, end: datetime, bin_minutes: int) -> int:
    """
    Number of bins of size `bin_minutes` needed to cover [start, end].

    Ceil division — a partial bin at the end still counts.
    """
    span_seconds = int((end - start).total_seconds())
    bin_seconds = bin_minutes * 60
    if bin_seconds <= 0:
        return 1
    return max(1, -(-span_seconds // bin_seconds))
