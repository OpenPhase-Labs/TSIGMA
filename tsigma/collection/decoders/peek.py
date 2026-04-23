"""
Peek/McCain event log decoder.

Parses binary event logs from Peek and McCain controllers.
Supports multiple header formats auto-detected by magic bytes:

- PEEK: 16-byte header, millisecond timestamp offsets
- MCCN: 32-byte header, microsecond timestamp offsets
- ATC\\0: 16-byte header, tenth-second timestamp offsets

Records are 8 bytes each:
[timestamp_offset(4 LE uint32), event_code(1), event_param(1), reserved(2)]
"""

import struct
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

from .base import BaseDecoder, DecodedEvent, DecoderRegistry

_RECORD_SIZE = 8

_MAGIC_PEEK = b"PEEK"
_MAGIC_MCCN = b"MCCN"
_MAGIC_ATC = b"ATC\x00"


class _HeaderInfo(NamedTuple):
    """Parsed header metadata."""

    header_size: int
    base_timestamp: datetime
    divisor: float  # offset units per second


def _parse_header(data: bytes) -> _HeaderInfo:
    """Detect header format and extract metadata.

    Args:
        data: Raw binary data.

    Returns:
        HeaderInfo with size, base timestamp, and time divisor.

    Raises:
        ValueError: If header format is not recognized.
    """
    if len(data) < 8:
        raise ValueError("Data too short for header")

    magic = data[:4]
    base_epoch = struct.unpack("<I", data[4:8])[0]
    base_ts = datetime.fromtimestamp(base_epoch, tz=timezone.utc)

    if magic == _MAGIC_PEEK:
        return _HeaderInfo(header_size=16, base_timestamp=base_ts, divisor=1000.0)
    if magic == _MAGIC_MCCN:
        return _HeaderInfo(header_size=32, base_timestamp=base_ts, divisor=1_000_000.0)
    if magic == _MAGIC_ATC:
        return _HeaderInfo(header_size=16, base_timestamp=base_ts, divisor=10.0)

    raise ValueError(f"Unknown magic bytes: {magic!r}")


@DecoderRegistry.register
class PeekDecoder(BaseDecoder):
    """Decoder for Peek/McCain binary event logs."""

    name = "peek"
    extensions = [".bin", ".dat", ".atc", ".log"]
    description = "Peek/McCain binary event log"

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Check if data has a recognized Peek/McCain magic header.

        Args:
            data: Raw file bytes.

        Returns:
            True if data starts with PEEK, MCCN, or ATC magic.
        """
        if len(data) < 4:
            return False
        magic = data[:4]
        return magic in (_MAGIC_PEEK, _MAGIC_MCCN, _MAGIC_ATC)

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Decode Peek/McCain binary data into events.

        Args:
            data: Raw file bytes.

        Returns:
            List of decoded events.
        """
        header = _parse_header(data)
        offset = header.header_size

        events = []
        while offset + _RECORD_SIZE <= len(data):
            time_offset, event_code, event_param, _ = struct.unpack(
                "<IBBH", data[offset : offset + _RECORD_SIZE]
            )
            ts = header.base_timestamp + timedelta(
                seconds=time_offset / header.divisor
            )
            events.append(
                DecodedEvent(
                    timestamp=ts,
                    event_code=event_code,
                    event_param=event_param,
                )
            )
            offset += _RECORD_SIZE

        return events
