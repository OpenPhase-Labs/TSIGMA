"""
Generic CSV/TSV event log decoder.

Parses text-based delimited event log files with configurable
column mapping and delimiter auto-detection. Supports comma,
tab, semicolon, and pipe delimiters.
"""

from dataclasses import dataclass
from typing import Optional

from .base import BaseDecoder, DecodedEvent, DecoderRegistry
from .sdk import (
    EVENT_CODE_NAMES,
    EVENT_PARAM_NAMES,
    TIMESTAMP_NAMES,
    detect_delimiter,
    find_column_index,
    parse_timestamp,
)


@dataclass
class CSVConfig:
    """Configuration for CSV decoder column mapping.

    Args:
        delimiter: Field delimiter. None = auto-detect.
        timestamp_col: Column index for timestamp. None = auto-detect by name.
        event_code_col: Column index for event code. None = auto-detect by name.
        event_param_col: Column index for event param. None = auto-detect by name.
        date_format: Timestamp format string. None = try multiple formats.
        skip_rows: Number of non-data rows to skip before the header.
    """

    delimiter: Optional[str] = None
    timestamp_col: Optional[int] = None
    event_code_col: Optional[int] = None
    event_param_col: Optional[int] = None
    date_format: Optional[str] = None
    skip_rows: int = 0


def _has_recognized_columns(text: str) -> bool:
    """Check if the first line contains recognized column names.

    Args:
        text: Decoded text content.

    Returns:
        True if timestamp and event code columns are found.
    """
    first_line = text.split("\n", 1)[0].strip()
    if not first_line:
        return False
    delimiter = detect_delimiter(first_line)
    headers = [h.strip().lower() for h in first_line.split(delimiter)]
    ts_col = find_column_index(headers, TIMESTAMP_NAMES)
    ec_col = find_column_index(headers, EVENT_CODE_NAMES)
    return ts_col is not None and ec_col is not None


@DecoderRegistry.register
class CSVDecoder(BaseDecoder):
    """Decoder for generic CSV/TSV event log files."""

    name = "csv"
    extensions = [".csv", ".txt", ".tsv"]
    description = "Generic CSV/TSV event log"

    def __init__(self, config: Optional[CSVConfig] = None):
        self._config = config or CSVConfig()

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Check if data looks like a CSV with recognized event columns.

        Args:
            data: Raw file bytes.

        Returns:
            True if data is decodable text with recognized columns.
        """
        if not data:
            return False
        try:
            text = data.decode("utf-8")
        except ValueError:
            return False
        return _has_recognized_columns(text)

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Decode CSV data into events.

        Args:
            data: Raw file bytes.

        Returns:
            List of decoded events.
        """
        if not data:
            return []

        text = data.decode("utf-8")
        lines = text.splitlines()

        # Skip metadata rows
        lines = lines[self._config.skip_rows:]
        if not lines:
            return []

        # Detect delimiter
        delimiter = self._config.delimiter or detect_delimiter(lines[0])

        # Parse header
        headers = [h.strip().lower() for h in lines[0].split(delimiter)]

        # Resolve column indices
        ts_col = self._config.timestamp_col
        ec_col = self._config.event_code_col
        ep_col = self._config.event_param_col

        if ts_col is None:
            ts_col = find_column_index(headers, TIMESTAMP_NAMES)
        if ec_col is None:
            ec_col = find_column_index(headers, EVENT_CODE_NAMES)
        if ep_col is None:
            ep_col = find_column_index(headers, EVENT_PARAM_NAMES)

        events = []
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            fields = line.split(delimiter)
            try:
                ts = parse_timestamp(
                    fields[ts_col], fmt=self._config.date_format
                )
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
