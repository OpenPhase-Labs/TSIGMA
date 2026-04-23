"""
ASC/3 (Econolite) Hi-Resolution event log decoder.

Parses the binary format used by Econolite ASC/3 controllers:
- 20-byte ASCII date header ("MM/DD/YYYY HH:MM:SS ")
- 7 newline-terminated header lines
- 4-byte records: [event_code(1), event_param(1), time_offset(2 big-endian)]

Time offset is in tenths of seconds from the header timestamp.
Supports zlib and gzip compressed files (.datz).
"""

import gzip
import re
import struct
import zlib
from datetime import datetime, timedelta, timezone

from .base import BaseDecoder, DecodedEvent, DecoderRegistry

_HEADER_DATE_RE = re.compile(rb"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} ")
_HEADER_DATE_FMT = "%m/%d/%Y %H:%M:%S"
_HEADER_DATE_LEN = 20
_HEADER_LINE_COUNT = 7
_RECORD_SIZE = 4


def _decompress(data: bytes) -> bytes:
    """Auto-detect and decompress zlib or gzip data.

    Args:
        data: Raw bytes, possibly compressed.

    Returns:
        Decompressed bytes, or original if not compressed.
    """
    if len(data) >= 2:
        if data[:2] == b"\x1f\x8b":
            return gzip.decompress(data)
        if data[0] == 0x78 and data[1] in (0x01, 0x5E, 0x9C, 0xDA):
            return zlib.decompress(data)
    return data


def _parse_header_timestamp(data: bytes) -> datetime:
    """Extract the base timestamp from the first 20 bytes.

    Args:
        data: Decompressed ASC/3 data.

    Returns:
        Base datetime in UTC.
    """
    raw = data[:_HEADER_DATE_LEN].decode("ascii").rstrip()
    return datetime.strptime(raw, _HEADER_DATE_FMT).replace(tzinfo=timezone.utc)


def _skip_header_lines(data: bytes) -> int:
    """Find the byte offset where records begin (after 7 header lines).

    Args:
        data: Decompressed ASC/3 data.

    Returns:
        Byte offset of the first record.
    """
    offset = _HEADER_DATE_LEN
    for _ in range(_HEADER_LINE_COUNT):
        nl = data.index(b"\n", offset)
        offset = nl + 1
    return offset


@DecoderRegistry.register
class ASC3Decoder(BaseDecoder):
    """Decoder for Econolite ASC/3 Hi-Resolution event logs."""

    name = "asc3"
    extensions = [".dat", ".datz"]
    description = "Econolite ASC/3 Hi-Resolution event log"

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Check if data is a valid ASC/3 event log.

        Args:
            data: Raw file bytes.

        Returns:
            True if data matches ASC/3 format.
        """
        if not data:
            return False
        try:
            decompressed = _decompress(data)
        except Exception:
            return False
        return bool(_HEADER_DATE_RE.match(decompressed))

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Decode ASC/3 binary data into events.

        Args:
            data: Raw file bytes (possibly compressed).

        Returns:
            List of decoded events.
        """
        data = _decompress(data)
        base_ts = _parse_header_timestamp(data)
        offset = _skip_header_lines(data)

        events = []
        while offset + _RECORD_SIZE <= len(data):
            event_code, event_param, time_offset = struct.unpack(
                ">BBH", data[offset : offset + _RECORD_SIZE]
            )
            ts = base_ts + timedelta(seconds=time_offset / 10.0)
            events.append(
                DecodedEvent(
                    timestamp=ts,
                    event_code=event_code,
                    event_param=event_param,
                )
            )
            offset += _RECORD_SIZE

        return events
