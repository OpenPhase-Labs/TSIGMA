"""
Decoder plugin SDK.

Shared helpers for TSIGMA decoder plugins.  Decoders are plugins, and
plugins should be able to stand on a well-defined toolbox instead of
copy-pasting column name sets, date format lists, delimiter detection,
and timestamp parsing into every decoder file.

``BaseDecoder`` and ``DecoderRegistry`` intentionally live in
``tsigma.collection.decoders.base`` — they define the *contract* between
core and plugins.  This package provides the *toolbox* plugins use to
implement that contract.
"""

from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Column name sets — canonical names used across delimited event log formats.
# Decoders compare header columns against these to auto-detect layout.
# ---------------------------------------------------------------------------

TIMESTAMP_NAMES: set[str] = {
    "timestamp", "time", "datetime", "date_time", "event_time",
}

EVENT_CODE_NAMES: set[str] = {
    "event_code", "code", "eventcode", "ec", "event_id",
}

EVENT_PARAM_NAMES: set[str] = {
    "event_param", "param", "eventparam", "ep", "parameter",
}

# XML attribute variants (MaxTime uses slightly different casing)
TIMESTAMP_ATTRS: set[str] = {
    "timestamp", "ts", "time", "datetime", "event_time",
}

EVENT_CODE_ATTRS: set[str] = {
    "event_code", "ec", "code", "eventcode",
}

EVENT_PARAM_ATTRS: set[str] = {
    "event_param", "ep", "param", "eventparam", "parameter",
}

# ---------------------------------------------------------------------------
# Date / time format lists — tried in order by parse_timestamp().
# ---------------------------------------------------------------------------

DATE_FORMATS: list[str] = [
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f",
]

DATE_ONLY_FORMATS: list[str] = [
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%d/%m/%Y",
]

TIME_ONLY_FORMATS: list[str] = [
    "%H:%M:%S",
    "%H:%M:%S.%f",
]

# ---------------------------------------------------------------------------
# Delimiter detection
# ---------------------------------------------------------------------------

DELIMITERS: list[str] = [",", "\t", ";", "|"]


def detect_delimiter(line: str, *, fallback: str = ",") -> str:
    """Detect the delimiter used in a delimited text line.

    Counts occurrences of each candidate delimiter and returns the most
    frequent one.  Falls back to ``fallback`` when no delimiters are found.

    Args:
        line: A header or data line from a delimited file.
        fallback: Delimiter to return when none are detected.

    Returns:
        Detected delimiter character.
    """
    counts = {d: line.count(d) for d in DELIMITERS}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else fallback


# ---------------------------------------------------------------------------
# Column index lookup
# ---------------------------------------------------------------------------


def find_column_index(
    headers: list[str], names: set[str],
) -> Optional[int]:
    """Find the index of a column by matching against known names.

    Args:
        headers: List of header column names (should be lowercased).
        names: Set of recognized column names to match against.

    Returns:
        Column index, or ``None`` if no match is found.
    """
    for i, h in enumerate(headers):
        if h in names:
            return i
    return None


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------


def parse_timestamp(
    value: str,
    *,
    fmt: Optional[str] = None,
    formats: Optional[list[str]] = None,
    file_date: Optional[datetime] = None,
) -> datetime:
    """Parse a timestamp string, trying multiple formats.

    Supports three modes:

    1. **Explicit format** (``fmt`` given): tries only that format.
    2. **Format list** (``formats`` given): tries each in order.
    3. **Default**: tries ``DATE_FORMATS``, then ``TIME_ONLY_FORMATS``
       combined with ``file_date`` (if provided).

    All returned datetimes are UTC.

    Args:
        value: Timestamp string to parse.
        fmt: Explicit strptime format.  Overrides ``formats``.
        formats: List of strptime formats to try in order.
        file_date: Base date for time-only strings (e.g. from a
            Siemens SEPAC "Date:" header line).

    Returns:
        Parsed ``datetime`` in UTC.

    Raises:
        ValueError: If no format matches.
    """
    value = value.strip()

    # Build the list of formats to try
    if fmt:
        try_formats = [fmt]
    elif formats:
        try_formats = formats
    else:
        try_formats = DATE_FORMATS

    # Try full datetime formats
    for f in try_formats:
        try:
            dt = datetime.strptime(value, f)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    # Try time-only formats combined with file_date
    if file_date:
        for f in TIME_ONLY_FORMATS:
            try:
                t = datetime.strptime(value, f)
                return file_date.replace(
                    hour=t.hour,
                    minute=t.minute,
                    second=t.second,
                    microsecond=t.microsecond,
                )
            except ValueError:
                continue

    raise ValueError(f"Cannot parse timestamp: {value!r}")


__all__ = [
    # column names
    "TIMESTAMP_NAMES",
    "EVENT_CODE_NAMES",
    "EVENT_PARAM_NAMES",
    "TIMESTAMP_ATTRS",
    "EVENT_CODE_ATTRS",
    "EVENT_PARAM_ATTRS",
    # date formats
    "DATE_FORMATS",
    "DATE_ONLY_FORMATS",
    "TIME_ONLY_FORMATS",
    # delimiters
    "DELIMITERS",
    "detect_delimiter",
    # column lookup
    "find_column_index",
    # timestamp parsing
    "parse_timestamp",
]
