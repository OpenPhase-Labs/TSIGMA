"""
Siemens SEPAC event log decoder.

Parses text-based event logs from Siemens SEPAC controllers.
Expects a header section with "SEPAC" or "Siemens" marker,
a "Date:" line for base date, and delimited data rows.
"""

import re
from datetime import datetime, timezone
from typing import Optional

from .base import BaseDecoder, DecodedEvent, DecoderRegistry
from .sdk import (
    DATE_ONLY_FORMATS,
    EVENT_CODE_NAMES,
    EVENT_PARAM_NAMES,
    TIMESTAMP_NAMES,
    detect_delimiter,
    find_column_index,
    parse_timestamp,
)

_MARKER_RE = re.compile(r"(?i)\b(sepac|siemens)\b")
_DATE_LINE_RE = re.compile(r"(?i)^date:\s*(.+)$")


def _extract_file_date(lines: list[str]) -> Optional[datetime]:
    """Extract the date from a 'Date: ...' header line.

    Args:
        lines: All lines of the file.

    Returns:
        Date as a datetime (time=midnight), or None.
    """
    for line in lines:
        m = _DATE_LINE_RE.match(line.strip())
        if m:
            date_str = m.group(1).strip()
            for fmt in DATE_ONLY_FORMATS:
                try:
                    return datetime.strptime(date_str, fmt).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    continue
    return None


def _find_data_start(lines: list[str]) -> int:
    """Find the index of the header row (row with column names).

    Scans for a line containing recognized column names.

    Args:
        lines: All lines of the file.

    Returns:
        Index of the header row, or len(lines) if not found.
    """
    for i, line in enumerate(lines):
        stripped = line.strip().lower()
        if not stripped:
            continue
        # Try each delimiter to split columns
        for d in ["\t", ",", ";"]:
            cols = {c.strip() for c in stripped.split(d)}
            if cols & TIMESTAMP_NAMES and cols & EVENT_CODE_NAMES:
                return i
    return len(lines)


@DecoderRegistry.register
class SiemensDecoder(BaseDecoder):
    """Decoder for Siemens SEPAC event logs."""

    name = "siemens"
    extensions = [".log", ".txt", ".csv", ".sepac"]
    description = "Siemens SEPAC event log"

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Check if data contains SEPAC or Siemens markers.

        Args:
            data: Raw file bytes.

        Returns:
            True if data is a Siemens SEPAC log.
        """
        if not data:
            return False
        try:
            text = data.decode("utf-8")
        except ValueError:
            return False
        return bool(_MARKER_RE.search(text[:1024]))

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Decode Siemens SEPAC data into events.

        Args:
            data: Raw file bytes.

        Returns:
            List of decoded events.
        """
        text = data.decode("utf-8")
        lines = text.splitlines()

        file_date = _extract_file_date(lines)
        header_idx = _find_data_start(lines)

        if header_idx >= len(lines):
            return []

        header_line = lines[header_idx]
        delimiter = detect_delimiter(header_line, fallback="\t")
        headers = [h.strip().lower() for h in header_line.split(delimiter)]

        ts_col = find_column_index(headers, TIMESTAMP_NAMES)
        ec_col = find_column_index(headers, EVENT_CODE_NAMES)
        ep_col = find_column_index(headers, EVENT_PARAM_NAMES)

        events = []
        for line in lines[header_idx + 1:]:
            line = line.strip()
            if not line:
                continue
            fields = line.split(delimiter)
            try:
                ts = parse_timestamp(fields[ts_col], file_date=file_date)
                event_code = int(fields[ec_col])
                event_param = int(fields[ep_col]) if ep_col is not None else 0
                events.append(
                    DecodedEvent(
                        timestamp=ts,
                        event_code=event_code,
                        event_param=event_param,
                    )
                )
            except (ValueError, IndexError):
                continue

        return events
