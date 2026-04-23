"""
Unit tests for Peek/McCain event log decoder.

Tests multiple header formats (PEEK, MCCN, ATC, headerless),
binary record parsing, timestamp resolution per format,
and base timestamp extraction.
"""

import struct
from datetime import datetime, timezone

from tsigma.collection.decoders.base import DecoderRegistry
from tsigma.collection.decoders.peek import PeekDecoder


def _build_peek_header(base_epoch, record_data=b""):
    """Build PEEK-format binary data (16-byte header, ms resolution).

    Header: PEEK(4) + base_epoch_le_u32(4) + reserved(8)
    Records: timestamp_offset_le_u32(4) + event_code(1) + event_param(1) + reserved(2)
    """
    header = b"PEEK" + struct.pack("<I", base_epoch) + b"\x00" * 8
    return header + record_data


def _build_mccn_header(base_epoch, record_data=b""):
    """Build MCCN-format binary data (32-byte header, us resolution).

    Header: MCCN(4) + base_epoch_le_u32(4) + reserved(24)
    """
    header = b"MCCN" + struct.pack("<I", base_epoch) + b"\x00" * 24
    return header + record_data


def _build_atc_header(base_epoch, record_data=b""):
    """Build ATC-format binary data (16-byte header, tenths resolution).

    Header: ATC\\0(4) + base_epoch_le_u32(4) + reserved(8)
    """
    header = b"ATC\x00" + struct.pack("<I", base_epoch) + b"\x00" * 8
    return header + record_data


def _make_record(time_offset, event_code, event_param):
    """Build a single 8-byte record."""
    return struct.pack("<IBBH", time_offset, event_code, event_param, 0)


# Base epoch for 2024-01-15 08:00:00 UTC
_BASE_EPOCH = int(datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc).timestamp())


class TestPeekRegistration:
    """Tests for PeekDecoder plugin registration."""

    def test_registered_in_registry(self):
        """Test PeekDecoder is registered as 'peek'."""
        assert "peek" in DecoderRegistry.list_all()
        cls = DecoderRegistry.get("peek")
        assert cls is PeekDecoder

    def test_extensions(self):
        """Test supported file extensions."""
        assert ".bin" in PeekDecoder.extensions
        assert ".dat" in PeekDecoder.extensions
        assert ".log" in PeekDecoder.extensions

    def test_has_description(self):
        """Test decoder has a description."""
        assert PeekDecoder.description


class TestPeekCanDecode:
    """Tests for PeekDecoder.can_decode()."""

    def test_peek_magic(self):
        """Test can_decode returns True for PEEK magic bytes."""
        data = _build_peek_header(_BASE_EPOCH)
        assert PeekDecoder.can_decode(data) is True

    def test_mccn_magic(self):
        """Test can_decode returns True for MCCN magic bytes."""
        data = _build_mccn_header(_BASE_EPOCH)
        assert PeekDecoder.can_decode(data) is True

    def test_atc_magic(self):
        """Test can_decode returns True for ATC magic bytes."""
        data = _build_atc_header(_BASE_EPOCH)
        assert PeekDecoder.can_decode(data) is True

    def test_random_bytes(self):
        """Test can_decode returns False for random bytes."""
        assert PeekDecoder.can_decode(b"\x00\x01\x02\x03\x04") is False

    def test_empty_data(self):
        """Test can_decode returns False for empty data."""
        assert PeekDecoder.can_decode(b"") is False


class TestPeekDecodeBytes:
    """Tests for PeekDecoder.decode_bytes()."""

    def test_peek_header_single_record(self):
        """Test PEEK format: single record with ms resolution."""
        record = _make_record(1500, 1, 2)  # 1500 ms = 1.5 seconds
        data = _build_peek_header(_BASE_EPOCH, record)
        decoder = PeekDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2
        expected = datetime(2024, 1, 15, 8, 0, 1, 500000, tzinfo=timezone.utc)
        assert events[0].timestamp == expected

    def test_mccn_header_microsecond_resolution(self):
        """Test MCCN format: microsecond timestamp resolution."""
        record = _make_record(2500000, 82, 5)  # 2,500,000 us = 2.5 seconds
        data = _build_mccn_header(_BASE_EPOCH, record)
        decoder = PeekDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 82
        expected = datetime(2024, 1, 15, 8, 0, 2, 500000, tzinfo=timezone.utc)
        assert events[0].timestamp == expected

    def test_atc_header_tenths_resolution(self):
        """Test ATC format: tenths-of-second resolution."""
        record = _make_record(153, 8, 2)  # 153 tenths = 15.3 seconds
        data = _build_atc_header(_BASE_EPOCH, record)
        decoder = PeekDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 8
        expected = datetime(2024, 1, 15, 8, 0, 15, 300000, tzinfo=timezone.utc)
        assert events[0].timestamp == expected

    def test_multiple_records(self):
        """Test decoding multiple records."""
        records = (
            _make_record(1000, 1, 2)
            + _make_record(2000, 3, 4)
            + _make_record(3000, 5, 6)
        )
        data = _build_peek_header(_BASE_EPOCH, records)
        decoder = PeekDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 3
        assert events[0].event_code == 1
        assert events[1].event_code == 3
        assert events[2].event_code == 5

    def test_empty_records_after_header(self):
        """Test header with no records returns empty list."""
        data = _build_peek_header(_BASE_EPOCH)
        decoder = PeekDecoder()
        events = decoder.decode_bytes(data)
        assert events == []

    def test_base_timestamp_from_header(self):
        """Test base timestamp is extracted from header."""
        # Use a different base time
        epoch_mar = int(
            datetime(2026, 3, 1, 14, 30, 0, tzinfo=timezone.utc).timestamp()
        )
        record = _make_record(0, 1, 1)  # 0 offset
        data = _build_peek_header(epoch_mar, record)
        decoder = PeekDecoder()
        events = decoder.decode_bytes(data)
        assert events[0].timestamp == datetime(
            2026, 3, 1, 14, 30, 0, tzinfo=timezone.utc
        )
