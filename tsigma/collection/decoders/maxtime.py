"""
MaxTime (Trafficware/MaxView/Synchro) event log decoder.

Supports two formats:
- XML: Root elements MaxTimeEvents, TrafficwareEvents, MaxViewLog, etc.
  Event elements with auto-detected attribute names.
- Binary MXTM: 4-byte magic, 20-byte header, 8-byte records with
  millisecond timestamp offsets.
"""

import re
import struct
from datetime import datetime, timedelta, timezone
from typing import Optional

import defusedxml.ElementTree as ET

from .base import BaseDecoder, DecodedEvent, DecoderRegistry
from .sdk import (
    EVENT_CODE_ATTRS,
    EVENT_PARAM_ATTRS,
    TIMESTAMP_ATTRS,
    parse_timestamp,
)

_MAGIC_MXTM = b"MXTM"
_MXTM_HEADER_SIZE = 20
_MXTM_RECORD_SIZE = 8

_XML_MARKERS_RE = re.compile(
    r"(?i)(MaxTime|Trafficware|MaxView|Synchro)", re.IGNORECASE
)


def _is_xml(data: bytes) -> bool:
    """Check if data looks like XML."""
    stripped = data.lstrip()
    return stripped[:1] == b"<"


def _has_xml_markers(data: bytes) -> bool:
    """Check if XML contains MaxTime/Trafficware/MaxView markers."""
    try:
        text = data.decode("utf-8")
    except ValueError:
        return False
    return bool(_XML_MARKERS_RE.search(text[:2048]))


def _detect_attr(element: ET.Element, names: set[str]) -> Optional[str]:
    """Find which attribute name from a set exists on an element."""
    for attr in element.attrib:
        if attr.lower() in names:
            return attr
    return None


def _decode_xml(data: bytes) -> list[DecodedEvent]:
    """Decode XML MaxTime event data.

    Args:
        data: UTF-8 XML bytes.

    Returns:
        List of decoded events.
    """
    root = ET.fromstring(data)

    # Find all Event elements at any depth
    event_elements = root.findall(".//Event")
    if not event_elements:
        return []

    # Detect attribute names from first element
    first = event_elements[0]
    ts_attr = _detect_attr(first, TIMESTAMP_ATTRS)
    ec_attr = _detect_attr(first, EVENT_CODE_ATTRS)
    ep_attr = _detect_attr(first, EVENT_PARAM_ATTRS)

    events = []
    for elem in event_elements:
        try:
            ts = parse_timestamp(elem.attrib[ts_attr])
            event_code = int(elem.attrib[ec_attr])
            event_param = int(elem.attrib[ep_attr]) if ep_attr else 0
            events.append(
                DecodedEvent(
                    timestamp=ts,
                    event_code=event_code,
                    event_param=event_param,
                )
            )
        except (KeyError, ValueError):
            continue

    return events


def _decode_mxtm(data: bytes) -> list[DecodedEvent]:
    """Decode binary MXTM event data.

    Header: MXTM(4) + base_epoch_le_u32(4) + reserved(12) = 20 bytes
    Records: timestamp_offset_le_u32(4) + event_code(1) + event_param(1) + reserved(2)

    Timestamp offsets are in milliseconds.

    Args:
        data: Raw binary data.

    Returns:
        List of decoded events.
    """
    base_epoch = struct.unpack("<I", data[4:8])[0]
    base_ts = datetime.fromtimestamp(base_epoch, tz=timezone.utc)

    offset = _MXTM_HEADER_SIZE
    events = []
    while offset + _MXTM_RECORD_SIZE <= len(data):
        time_offset, event_code, event_param, _ = struct.unpack(
            "<IBBH", data[offset : offset + _MXTM_RECORD_SIZE]
        )
        ts = base_ts + timedelta(milliseconds=time_offset)
        events.append(
            DecodedEvent(
                timestamp=ts,
                event_code=event_code,
                event_param=event_param,
            )
        )
        offset += _MXTM_RECORD_SIZE

    return events


@DecoderRegistry.register
class MaxTimeDecoder(BaseDecoder):
    """Decoder for MaxTime/Trafficware/MaxView event logs."""

    name = "maxtime"
    extensions = [".xml", ".maxtime", ".mtl", ".bin", ".synchro"]
    description = "MaxTime/Trafficware/MaxView event log"

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Check if data is a MaxTime event log.

        Args:
            data: Raw file bytes.

        Returns:
            True if data is XML with MaxTime markers or MXTM binary.
        """
        if not data:
            return False
        if data[:4] == _MAGIC_MXTM:
            return True
        if _is_xml(data) and _has_xml_markers(data):
            return True
        return False

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Decode MaxTime data into events.

        Args:
            data: Raw file bytes.

        Returns:
            List of decoded events.
        """
        if data[:4] == _MAGIC_MXTM:
            return _decode_mxtm(data)
        return _decode_xml(data)
