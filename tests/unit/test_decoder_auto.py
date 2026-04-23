"""
Unit tests for auto-detect decoder wrapper.

Tests that the auto decoder probes registered decoders in priority
order and routes data to the correct decoder.
"""

import struct
from datetime import datetime, timezone

import pytest

from tsigma.collection.decoders.auto import AutoDecoder
from tsigma.collection.decoders.base import DecoderRegistry


def _build_asc3(records=None):
    """Build minimal ASC/3 binary data."""
    header = b"01/15/2024 08:00:00 "
    lines = b"".join(f"Header line {i}\n".encode() for i in range(1, 8))
    body = header + lines
    for ec, ep, offset in (records or []):
        body += struct.pack(">BBH", ec, ep, offset)
    return body


def _build_peek(records=None):
    """Build minimal PEEK binary data."""
    epoch = int(datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc).timestamp())
    header = b"PEEK" + struct.pack("<I", epoch) + b"\x00" * 8
    body = b""
    for offset, ec, ep in (records or []):
        body += struct.pack("<IBBH", offset, ec, ep, 0)
    return header + body


def _build_maxtime_xml(events=None):
    """Build minimal MaxTimeEvents XML."""
    lines = ['<?xml version="1.0"?>', "<MaxTimeEvents>"]
    for ts, ec, ep in (events or []):
        lines.append(f'  <Event timestamp="{ts}" event_code="{ec}" event_param="{ep}"/>')
    lines.append("</MaxTimeEvents>")
    return "\n".join(lines).encode("utf-8")


def _build_sepac(records=None):
    """Build minimal SEPAC text log."""
    lines = [
        "SEPAC Event Log",
        "Controller: Test-001",
        "Date: 01/15/2024",
        "",
        "time\tevent_code\tevent_param",
    ]
    for t, ec, ep in (records or []):
        lines.append(f"{t}\t{ec}\t{ep}")
    return "\n".join(lines).encode("utf-8")


def _build_csv(records=None):
    """Build minimal CSV data."""
    lines = ["timestamp,event_code,event_param"]
    for ts, ec, ep in (records or []):
        lines.append(f"{ts},{ec},{ep}")
    return "\n".join(lines).encode("utf-8")


class TestAutoRegistration:
    """Tests for AutoDecoder plugin registration."""

    def test_registered_in_registry(self):
        """Test AutoDecoder is registered as 'auto'."""
        assert "auto" in DecoderRegistry.list_all()
        cls = DecoderRegistry.get("auto")
        assert cls is AutoDecoder

    def test_has_description(self):
        """Test decoder has a description."""
        assert AutoDecoder.description

    def test_can_decode_always_true(self):
        """Test can_decode always returns True."""
        assert AutoDecoder.can_decode(b"anything") is True
        assert AutoDecoder.can_decode(b"\x00\x01") is True


class TestAutoDecodeRouting:
    """Tests for AutoDecoder routing to correct decoder."""

    def test_routes_asc3(self):
        """Test ASC/3 data is routed to ASC3Decoder."""
        data = _build_asc3(records=[(1, 2, 10)])
        decoder = AutoDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_routes_peek(self):
        """Test PEEK data is routed to PeekDecoder."""
        data = _build_peek(records=[(1000, 82, 5)])
        decoder = AutoDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 82

    def test_routes_maxtime_xml(self):
        """Test MaxTime XML is routed to MaxTimeDecoder."""
        data = _build_maxtime_xml(
            events=[("2024-01-15 08:00:00", 1, 2)]
        )
        decoder = AutoDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_routes_sepac(self):
        """Test SEPAC text is routed to SiemensDecoder."""
        data = _build_sepac(records=[("08:00:00", 1, 2)])
        decoder = AutoDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_routes_csv(self):
        """Test generic CSV falls back to CSVDecoder."""
        data = _build_csv(records=[("2024-01-15 08:00:00", 1, 2)])
        decoder = AutoDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_raises_for_undecodable(self):
        """Test ValueError raised for data no decoder can handle."""
        decoder = AutoDecoder()
        with pytest.raises(ValueError, match="No decoder"):
            decoder.decode_bytes(b"\x00\x01\x02\x03\x04\x05\x06\x07")

    def test_auto_decode_unknown_format(self):
        """Test auto decoder raises ValueError when no decoder recognizes data."""
        from unittest.mock import patch

        decoder = AutoDecoder()

        # Mock DecoderRegistry.get to always raise ValueError (unregistered)
        with patch(
            "tsigma.collection.decoders.auto.DecoderRegistry"
        ) as mock_reg:
            mock_reg.get.side_effect = ValueError("not found")
            with pytest.raises(ValueError, match="No decoder found"):
                decoder.decode_bytes(b"\xde\xad\xbe\xef")
