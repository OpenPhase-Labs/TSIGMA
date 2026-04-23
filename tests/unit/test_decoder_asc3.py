"""
Unit tests for ASC/3 (Econolite) Hi-Resolution event log decoder.

Tests binary parsing, header extraction, time offset calculation,
and compression handling (zlib, gzip).
"""

import gzip
import struct
import zlib
from datetime import datetime, timezone

from tsigma.collection.decoders.asc3 import ASC3Decoder
from tsigma.collection.decoders.base import DecoderRegistry


def _build_asc3(timestamp_str="01/15/2024 08:00:00 ", records=None):
    """Build raw ASC/3 binary data for testing."""
    header = timestamp_str.encode("ascii")
    # 7 header lines
    lines = b"".join(f"Header line {i}\n".encode() for i in range(1, 8))
    body = header + lines
    for event_code, event_param, time_offset in (records or []):
        body += struct.pack(">BBH", event_code, event_param, time_offset)
    return body


class TestASC3Registration:
    """Tests for ASC3Decoder plugin registration."""

    def test_registered_in_registry(self):
        """Test ASC3Decoder is registered as 'asc3'."""
        assert "asc3" in DecoderRegistry.list_all()
        cls = DecoderRegistry.get("asc3")
        assert cls is ASC3Decoder

    def test_extensions(self):
        """Test supported file extensions."""
        assert ".dat" in ASC3Decoder.extensions
        assert ".datz" in ASC3Decoder.extensions

    def test_has_description(self):
        """Test decoder has a description."""
        assert ASC3Decoder.description


class TestASC3CanDecode:
    """Tests for ASC3Decoder.can_decode()."""

    def test_valid_header(self):
        """Test can_decode returns True for valid ASC/3 header."""
        data = _build_asc3()
        assert ASC3Decoder.can_decode(data) is True

    def test_random_bytes(self):
        """Test can_decode returns False for random binary data."""
        assert ASC3Decoder.can_decode(b"\x00\x01\x02\x03\x04") is False

    def test_zlib_compressed(self):
        """Test can_decode returns True for zlib-compressed ASC/3."""
        raw = _build_asc3()
        compressed = zlib.compress(raw)
        assert ASC3Decoder.can_decode(compressed) is True

    def test_gzip_compressed(self):
        """Test can_decode returns True for gzip-compressed ASC/3."""
        raw = _build_asc3()
        compressed = gzip.compress(raw)
        assert ASC3Decoder.can_decode(compressed) is True

    def test_empty_data(self):
        """Test can_decode returns False for empty data."""
        assert ASC3Decoder.can_decode(b"") is False


class TestASC3DecodeBytes:
    """Tests for ASC3Decoder.decode_bytes()."""

    def test_parses_header_timestamp(self):
        """Test header timestamp is parsed correctly."""
        data = _build_asc3(
            timestamp_str="03/01/2026 14:30:00 ",
            records=[(1, 2, 0)],
        )
        decoder = ASC3Decoder()
        events = decoder.decode_bytes(data)
        assert events[0].timestamp == datetime(2026, 3, 1, 14, 30, 0, tzinfo=timezone.utc)

    def test_single_record(self):
        """Test decoding a single event record."""
        data = _build_asc3(records=[(1, 2, 10)])
        decoder = ASC3Decoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2

    def test_time_offset_tenths(self):
        """Test time offset is applied in tenths of seconds."""
        # 153 tenths = 15.3 seconds
        data = _build_asc3(
            timestamp_str="01/15/2024 08:00:00 ",
            records=[(82, 5, 153)],
        )
        decoder = ASC3Decoder()
        events = decoder.decode_bytes(data)
        expected = datetime(2024, 1, 15, 8, 0, 15, 300000, tzinfo=timezone.utc)
        assert events[0].timestamp == expected

    def test_multiple_records(self):
        """Test decoding multiple event records."""
        data = _build_asc3(records=[
            (1, 2, 10),
            (82, 5, 153),
            (8, 2, 450),
            (81, 5, 158),
        ])
        decoder = ASC3Decoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 4
        assert events[0].event_code == 1
        assert events[1].event_code == 82
        assert events[2].event_code == 8
        assert events[3].event_code == 81

    def test_empty_records(self):
        """Test header-only file returns empty list."""
        data = _build_asc3(records=[])
        decoder = ASC3Decoder()
        events = decoder.decode_bytes(data)
        assert events == []

    def test_zlib_compressed(self):
        """Test decoding zlib-compressed data."""
        raw = _build_asc3(records=[(1, 2, 10), (3, 4, 20)])
        compressed = zlib.compress(raw)
        decoder = ASC3Decoder()
        events = decoder.decode_bytes(compressed)
        assert len(events) == 2
        assert events[0].event_code == 1

    def test_gzip_compressed(self):
        """Test decoding gzip-compressed data."""
        raw = _build_asc3(records=[(5, 6, 100)])
        compressed = gzip.compress(raw)
        decoder = ASC3Decoder()
        events = decoder.decode_bytes(compressed)
        assert len(events) == 1
        assert events[0].event_code == 5

    def test_high_time_offset(self):
        """Test time offset near max (65535 tenths = ~109 minutes)."""
        data = _build_asc3(
            timestamp_str="01/15/2024 08:00:00 ",
            records=[(1, 1, 36000)],  # 3600.0 seconds = 1 hour
        )
        decoder = ASC3Decoder()
        events = decoder.decode_bytes(data)
        expected = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
        assert events[0].timestamp == expected
