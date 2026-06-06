"""
D4 controller event-log decoder (Fourth Dimension Traffic "D4" CSV format).

D4 controllers (Fourth Dimension Traffic) emit positional CSV event logs
(no recognizable header names) in two layouts, discriminated by field count:

    4 fields  : event_code, event_param, timestamp, msg_idx
    5+ fields : event_code, event_name, event_param, timestamp, msg_idx
                (the "streamed" variant)

Only (timestamp, event_code, event_param) are kept; event_name and
msg_idx are discarded. Rows are parsed with the stdlib ``csv`` module
(RFC 4180), matching the Kimley-Horn reference's CsvHelper parser, so a
quoted event_name may contain commas and escaped quotes. Files may be
gzip-compressed; the decoder decompresses transparently.
Format/parse only -- transport (FTP/SFTP pull, the download/delete
lifecycle, date-window filtering) is handled by TSIGMA's ingestion layer
and is intentionally NOT reimplemented here.
"""

import csv
import gzip
import io
from typing import ClassVar, Optional

from .base import BaseDecoder, DecodedEvent, DecoderRegistry
from .sdk import (
    DATE_FORMATS,
    EVENT_CODE_NAMES,
    TIMESTAMP_NAMES,
    find_column_index,
    parse_timestamp,
)

# KH parity: the Kimley-Horn D4 reference parses timestamps with .NET
# CurrentCulture, which accepts 12-hour AM/PM, no-seconds, date-only, and
# 2-digit-year forms in addition to the SDK defaults. Extend the default list
# so D4 does not silently drop rows whose timestamps use those styles.
D4_DATE_FORMATS: list[str] = [
    *DATE_FORMATS,
    # 4-digit-year forms (AM/PM, no-seconds, date-only)
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M",
    "%m/%d/%Y",
    "%Y-%m-%d",
    # 2-digit-year forms (strptime %y maps 00-68 -> 2000-2068)
    "%m/%d/%y %I:%M:%S %p",
    "%m/%d/%y %I:%M %p",
    "%m/%d/%y %H:%M:%S",
    "%m/%d/%y %H:%M",
    "%m/%d/%y",
]


def _maybe_gunzip(data: bytes) -> bytes:
    """Decompress gzip data (magic 1f 8b); return as-is otherwise."""
    if len(data) >= 2 and data[:2] == b"\x1f\x8b":
        return gzip.decompress(data)
    return data


def _row_to_event(row: list[str]) -> Optional[DecodedEvent]:
    """Convert one parsed CSV row into a DecodedEvent, or None if it is
    not a valid data row (header, blank, or malformed -- skipped).

    Two layouts are supported, discriminated by field count:
        4 fields  : event_code, event_param, timestamp, msg_idx
        5+ fields : event_code, event_name, event_param, timestamp, msg_idx
                    (the "streamed" variant; event_name is discarded)
    """
    fields = [f.strip() for f in row]
    # Drop trailing empty fields (e.g. a trailing comma) so a 4-column row is
    # not misclassified as the 5-column streamed layout. CsvHelper tolerates a
    # trailing column via index mapping; match that.
    while fields and fields[-1] == "":
        fields.pop()
    if len(fields) >= 5:
        ec_s, ep_s, ts_s = fields[0], fields[2], fields[3]
    elif len(fields) == 4:
        ec_s, ep_s, ts_s = fields[0], fields[1], fields[2]
    else:
        return None
    try:
        return DecodedEvent(
            timestamp=parse_timestamp(ts_s, formats=D4_DATE_FORMATS),
            event_code=int(ec_s),
            event_param=int(ep_s),
        )
    except (ValueError, IndexError):
        return None


@DecoderRegistry.register
class D4Decoder(BaseDecoder):
    """Decoder for Fourth Dimension Traffic D4 positional-CSV event logs."""

    name: ClassVar[str] = "d4"
    extensions: ClassVar[list[str]] = [".d4", ".csv", ".gz"]
    description: ClassVar[str] = "Fourth Dimension Traffic D4 controller CSV event log"

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        if not data:
            return False
        try:
            text = _maybe_gunzip(data).decode("utf-8")
            rows = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
            if not rows:
                return False
            # Defer name-tagged CSVs to the generic 'csv' decoder.
            headers = [h.strip().lower() for h in rows[0]]
            if (
                find_column_index(headers, TIMESTAMP_NAMES) is not None
                and find_column_index(headers, EVENT_CODE_NAMES) is not None
            ):
                return False
            # Positively identify D4 by at least one valid positional row.
            return any(_row_to_event(r) is not None for r in rows)
        except Exception:
            return False

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        if not data:
            return []
        text = _maybe_gunzip(data).decode("utf-8")
        events: list[DecodedEvent] = []
        for row in csv.reader(io.StringIO(text)):
            event = _row_to_event(row)
            if event is not None:
                events.append(event)
        return events
