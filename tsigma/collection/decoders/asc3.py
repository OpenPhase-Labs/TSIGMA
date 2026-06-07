"""
ASC/3 (Econolite) Hi-Resolution event log decoder.

Parses the text-header + binary-record format Econolite ASC/3 controllers
emit:

- Header: 7 comma-delimited text lines, each prefixed with a timestamp.
  Line 1's leading field (up to the first comma) is the anchor datetime, in
  ``M-D-YYYY HH:MM:SS.s`` (hyphen, non-padded, fractional) or the slash
  equivalent.
- Records (line 8+): 4-byte ``>BBH`` (event_code, event_param, time_offset);
  time_offset is tenths of a second from the anchor.

``decode_bytes`` returns the bare event list (record core). ``decode``
returns a ``DecodeResult`` envelope that also captures the header as
``FileMetadata`` (device IP/MAC, version, phases, both anchors).

Supports zlib and gzip compression. The anchor parse is lenient on format
but fails closed on an unparseable anchor (never fabricates a base time).
"""

import gzip
import logging
import re
import struct
import zlib
from datetime import datetime, timedelta, timezone

from .base import BaseDecoder, DecodedEvent, DecodeResult, DecoderRegistry, FileMetadata

logger = logging.getLogger(__name__)

# Accepted anchor datetime formats (line-1 field up to the first comma).
_HEADER_DATE_FORMATS = (
    "%m-%d-%Y %H:%M:%S.%f",
    "%m-%d-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S",
)
# can_decode gate: a leading non-padded M[-/]D[-/]YYYY HH:MM:SS date.
_HEADER_DATE_RE = re.compile(rb"^\d{1,2}[-/]\d{1,2}[-/]\d{4} \d{1,2}:\d{2}:\d{2}")
_HEADER_LINE_COUNT = 7
_RECORD_SIZE = 4


class ASC3DecodeError(ValueError):
    """Raised when an ASC/3 file's header anchor cannot be parsed."""


def _decompress(data: bytes) -> bytes:
    """Auto-detect and decompress zlib or gzip data.

    Args:
        data: Raw bytes, possibly compressed.

    Returns:
        Decompressed bytes, or the original if not compressed.
    """
    if len(data) >= 2:
        if data[:2] == b"\x1f\x8b":
            return gzip.decompress(data)
        if data[0] == 0x78 and data[1] in (0x01, 0x5E, 0x9C, 0xDA):
            return zlib.decompress(data)
    return data


def _try_parse_anchor(field: str) -> datetime | None:
    """Parse an anchor datetime string, or return None if no format matches."""
    field = field.strip()
    for fmt in _HEADER_DATE_FORMATS:
        try:
            return datetime.strptime(field, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_header_anchor(data: bytes) -> datetime:
    """Parse the line-1 anchor datetime (the field up to the first comma).

    Lenient on format (hyphen/slash, non-padded, optional fractional seconds);
    strict on validity — raises ``ASC3DecodeError`` if no format matches.
    Never fabricates a base (no None / now / epoch default).

    Args:
        data: Decompressed ASC/3 data.

    Returns:
        Anchor datetime in UTC.
    """
    first_line = data.split(b"\n", 1)[0]
    field = first_line.split(b",", 1)[0].decode("ascii", "replace").strip()
    ts = _try_parse_anchor(field)
    if ts is None:
        raise ASC3DecodeError(f"Unparseable ASC/3 header anchor: {field!r}")
    return ts


def _skip_header_lines(data: bytes) -> int:
    """Find the byte offset where binary records begin (after 7 header lines).

    Args:
        data: Decompressed ASC/3 data.

    Returns:
        Byte offset of the first binary record.
    """
    offset = 0
    for _ in range(_HEADER_LINE_COUNT):
        nl = data.index(b"\n", offset)
        offset = nl + 1
    return offset


def _parse_header_metadata(decompressed: bytes) -> FileMetadata:
    """Parse the 7-line comma-delimited header into FileMetadata.

    Best-effort provenance capture: unparseable/missing fields stay None.
    The fail-closed anchor gate lives in ``_parse_header_anchor``, not here.

    Args:
        decompressed: Decompressed ASC/3 data.

    Returns:
        Populated FileMetadata (fields default to None when absent).
    """
    header_lines: list[str] = []
    offset = 0
    try:
        for _ in range(_HEADER_LINE_COUNT):
            nl = decompressed.index(b"\n", offset)
            header_lines.append(decompressed[offset:nl].decode("ascii", "replace"))
            offset = nl + 1
    except ValueError:
        pass

    meta = FileMetadata()
    raw: dict[str, str] = {}

    if header_lines:
        meta.header_anchor = _try_parse_anchor(header_lines[0].split(",", 1)[0])

    for line in header_lines:
        fields = [f.strip() for f in line.split(",")]
        if len(fields) < 2:
            continue
        label = fields[1]
        rest = fields[2:]
        if label == "Version #:" and rest:
            meta.log_version = rest[0]
            raw["version"] = rest[0]
        elif label == "IP Address:" and rest:
            meta.device_ip = rest[0]
            raw["ip_address"] = rest[0]
        elif label == "MAC Address:" and rest:
            meta.device_mac = rest[0]
            raw["mac_address"] = rest[0]
        elif label == "Intersection #:" and rest:
            raw["intersection"] = rest[0]
        elif label == "Controller Data Log Beginning:" and rest:
            secondary = " ".join(rest)
            meta.header_anchor_secondary = _try_parse_anchor(secondary)
            meta.log_begin = meta.header_anchor_secondary
            raw["log_begin"] = secondary
        elif label == "Phases in use:" and rest:
            phases: list[int] = []
            for token in rest:
                try:
                    phases.append(int(token))
                except ValueError:
                    continue
            meta.phases_in_use = phases or None
            raw["phases_in_use"] = ",".join(rest)
        elif ":" not in label and label:
            meta.source_filename = label
            raw["source_filename"] = label

    meta.raw = raw or None
    return meta


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
            True if data matches the ASC/3 header shape.
        """
        if not data:
            return False
        try:
            decompressed = _decompress(data)
        except Exception:
            return False
        return bool(_HEADER_DATE_RE.match(decompressed))

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Decode ASC/3 data into events.

        Args:
            data: Raw file bytes (possibly compressed).

        Returns:
            List of decoded events.

        Raises:
            ASC3DecodeError: If the header anchor is unparseable.
        """
        data = _decompress(data)
        base_ts = _parse_header_anchor(data)
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

    def decode(self, data: bytes) -> DecodeResult:
        """Decode into an envelope: events plus header ``FileMetadata``.

        Logs a warning when the line-1 and line-6 header anchors disagree
        (exact-match cross-check); does not fail on disagreement (flag, not
        reject — quarantine routing lands in a later increment).

        Args:
            data: Raw file bytes (possibly compressed).

        Returns:
            DecodeResult with events and populated FileMetadata.
        """
        decompressed = _decompress(data)
        events = self.decode_bytes(data)
        metadata = _parse_header_metadata(decompressed)
        if (
            metadata.header_anchor is not None
            and metadata.header_anchor_secondary is not None
            and metadata.header_anchor != metadata.header_anchor_secondary
        ):
            logger.warning(
                "ASC/3 header anchor mismatch: line1=%s line6=%s (file flagged)",
                metadata.header_anchor.isoformat(),
                metadata.header_anchor_secondary.isoformat(),
            )
        return DecodeResult(events=events, metadata=metadata)
