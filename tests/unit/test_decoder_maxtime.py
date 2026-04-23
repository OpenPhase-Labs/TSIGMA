"""
Unit tests for MaxTime (Trafficware/MaxView) event log decoder.

Tests XML parsing, binary MXTM format, attribute name detection,
and various XML root element variants.
"""

import struct
from datetime import datetime, timezone

from tsigma.collection.decoders.base import DecoderRegistry
from tsigma.collection.decoders.maxtime import MaxTimeDecoder


def _build_xml(events=None, root_tag="MaxTimeEvents", attr_style="standard"):
    """Build an XML event log for testing.

    Args:
        events: List of (timestamp_str, event_code, event_param) tuples.
        root_tag: XML root element name.
        attr_style: "standard" uses timestamp/event_code/event_param,
                    "short" uses ts/ec/ep.
    """
    lines = ['<?xml version="1.0"?>', f"<{root_tag}>"]
    for ts, ec, ep in (events or []):
        if attr_style == "standard":
            lines.append(
                f'  <Event timestamp="{ts}" event_code="{ec}" event_param="{ep}"/>'
            )
        elif attr_style == "short":
            lines.append(f'  <Event ts="{ts}" ec="{ec}" ep="{ep}"/>')
    lines.append(f"</{root_tag}>")
    return "\n".join(lines).encode("utf-8")


def _build_mxtm(base_epoch, records=None):
    """Build binary MXTM data for testing.

    Header: MXTM(4) + base_epoch_le_u32(4) + reserved(12) = 20 bytes
    Records: timestamp_offset_le_u32(4) + event_code(1) + event_param(1) + reserved(2) = 8 bytes
    """
    header = b"MXTM" + struct.pack("<I", base_epoch) + b"\x00" * 12
    body = b""
    for time_offset, ec, ep in (records or []):
        body += struct.pack("<IBBH", time_offset, ec, ep, 0)
    return header + body


_BASE_EPOCH = int(datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc).timestamp())


class TestMaxTimeRegistration:
    """Tests for MaxTimeDecoder plugin registration."""

    def test_registered_in_registry(self):
        """Test MaxTimeDecoder is registered as 'maxtime'."""
        assert "maxtime" in DecoderRegistry.list_all()
        cls = DecoderRegistry.get("maxtime")
        assert cls is MaxTimeDecoder

    def test_extensions(self):
        """Test supported file extensions."""
        assert ".xml" in MaxTimeDecoder.extensions
        assert ".bin" in MaxTimeDecoder.extensions

    def test_has_description(self):
        """Test decoder has a description."""
        assert MaxTimeDecoder.description


class TestMaxTimeCanDecode:
    """Tests for MaxTimeDecoder.can_decode()."""

    def test_maxtime_xml(self):
        """Test can_decode returns True for MaxTimeEvents XML."""
        data = _build_xml(root_tag="MaxTimeEvents")
        assert MaxTimeDecoder.can_decode(data) is True

    def test_trafficware_xml(self):
        """Test can_decode returns True for Trafficware XML."""
        data = _build_xml(root_tag="TrafficwareEvents")
        assert MaxTimeDecoder.can_decode(data) is True

    def test_maxview_xml(self):
        """Test can_decode returns True for MaxView XML."""
        data = _build_xml(root_tag="MaxViewLog")
        assert MaxTimeDecoder.can_decode(data) is True

    def test_mxtm_binary(self):
        """Test can_decode returns True for MXTM binary magic."""
        data = _build_mxtm(_BASE_EPOCH)
        assert MaxTimeDecoder.can_decode(data) is True

    def test_random_data(self):
        """Test can_decode returns False for random data."""
        assert MaxTimeDecoder.can_decode(b"\x00\x01\x02\x03\x04") is False

    def test_empty_data(self):
        """Test can_decode returns False for empty data."""
        assert MaxTimeDecoder.can_decode(b"") is False

    def test_unrelated_xml(self):
        """Test can_decode returns False for unrelated XML."""
        data = b'<?xml version="1.0"?><Configuration><Setting name="x"/></Configuration>'
        assert MaxTimeDecoder.can_decode(data) is False


class TestMaxTimeDecodeBytes:
    """Tests for MaxTimeDecoder.decode_bytes()."""

    def test_xml_standard_attributes(self):
        """Test XML with standard attribute names."""
        data = _build_xml(
            events=[
                ("2024-01-15 08:00:00", 1, 2),
                ("2024-01-15 08:00:01", 3, 4),
            ],
            attr_style="standard",
        )
        decoder = MaxTimeDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 2
        assert events[0].event_code == 1
        assert events[0].event_param == 2
        assert events[1].event_code == 3

    def test_xml_short_attributes(self):
        """Test XML with short attribute names (ts, ec, ep)."""
        data = _build_xml(
            events=[("2024-01-15 08:00:00", 82, 5)],
            attr_style="short",
        )
        decoder = MaxTimeDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 82
        assert events[0].event_param == 5

    def test_xml_empty_events(self):
        """Test XML with no Event elements."""
        data = _build_xml(events=[])
        decoder = MaxTimeDecoder()
        events = decoder.decode_bytes(data)
        assert events == []

    def test_xml_trafficware_variant(self):
        """Test Trafficware XML variant."""
        data = _build_xml(
            root_tag="TrafficwareEvents",
            events=[("01/15/2024 08:00:00", 1, 2)],
        )
        decoder = MaxTimeDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_xml_maxview_variant(self):
        """Test MaxView XML variant."""
        data = _build_xml(
            root_tag="MaxViewLog",
            events=[("2024-01-15T08:00:00", 8, 2)],
        )
        decoder = MaxTimeDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 8

    def test_mxtm_binary_single_record(self):
        """Test binary MXTM format with single record."""
        data = _build_mxtm(_BASE_EPOCH, records=[(1500, 1, 2)])
        decoder = MaxTimeDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2

    def test_mxtm_binary_multiple_records(self):
        """Test binary MXTM format with multiple records."""
        data = _build_mxtm(
            _BASE_EPOCH,
            records=[(1000, 1, 2), (2000, 3, 4), (3000, 5, 6)],
        )
        decoder = MaxTimeDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 3
        assert events[0].event_code == 1
        assert events[1].event_code == 3
        assert events[2].event_code == 5

    def test_mxtm_binary_timestamp(self):
        """Test MXTM binary timestamp offset (ms resolution)."""
        data = _build_mxtm(_BASE_EPOCH, records=[(2500, 1, 1)])
        decoder = MaxTimeDecoder()
        events = decoder.decode_bytes(data)
        expected = datetime(2024, 1, 15, 8, 0, 2, 500000, tzinfo=timezone.utc)
        assert events[0].timestamp == expected
