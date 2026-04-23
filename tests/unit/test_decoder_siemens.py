"""
Unit tests for Siemens SEPAC event log decoder.

Tests SEPAC header detection, tab/comma/semicolon delimited parsing,
header line skipping, and date extraction.
"""

from datetime import datetime, timezone

from tsigma.collection.decoders.base import DecoderRegistry
from tsigma.collection.decoders.siemens import SiemensDecoder


def _build_sepac(
    marker="SEPAC",
    date_line="Date: 01/15/2024",
    delimiter="\t",
    records=None,
):
    """Build a SEPAC-formatted text file for testing."""
    lines = [
        f"{marker} Event Log",
        "Controller: Test-001",
        f"{date_line}",
        "Firmware: v4.2.1",
        "",
        f"time{delimiter}event_code{delimiter}event_param",
    ]
    for time_str, ec, ep in (records or []):
        lines.append(f"{time_str}{delimiter}{ec}{delimiter}{ep}")
    return "\n".join(lines).encode("utf-8")


class TestSiemensRegistration:
    """Tests for SiemensDecoder plugin registration."""

    def test_registered_in_registry(self):
        """Test SiemensDecoder is registered as 'siemens'."""
        assert "siemens" in DecoderRegistry.list_all()
        cls = DecoderRegistry.get("siemens")
        assert cls is SiemensDecoder

    def test_extensions(self):
        """Test supported file extensions."""
        assert ".log" in SiemensDecoder.extensions
        assert ".txt" in SiemensDecoder.extensions
        assert ".sepac" in SiemensDecoder.extensions

    def test_has_description(self):
        """Test decoder has a description."""
        assert SiemensDecoder.description


class TestSiemensCanDecode:
    """Tests for SiemensDecoder.can_decode()."""

    def test_sepac_marker(self):
        """Test can_decode returns True for SEPAC marker."""
        data = _build_sepac(marker="SEPAC")
        assert SiemensDecoder.can_decode(data) is True

    def test_siemens_marker(self):
        """Test can_decode returns True for Siemens marker."""
        data = _build_sepac(marker="Siemens")
        assert SiemensDecoder.can_decode(data) is True

    def test_random_binary(self):
        """Test can_decode returns False for binary data."""
        assert SiemensDecoder.can_decode(b"\x00\x01\x02\x03\x04") is False

    def test_empty_data(self):
        """Test can_decode returns False for empty data."""
        assert SiemensDecoder.can_decode(b"") is False

    def test_generic_csv_no_marker(self):
        """Test can_decode returns False for CSV without SEPAC/Siemens."""
        data = b"timestamp,event_code,event_param\n2024-01-15 08:00:00,1,2\n"
        assert SiemensDecoder.can_decode(data) is False


class TestSiemensDecodeBytes:
    """Tests for SiemensDecoder.decode_bytes()."""

    def test_tab_delimited(self):
        """Test decoding tab-delimited SEPAC log."""
        data = _build_sepac(
            delimiter="\t",
            records=[("08:00:00", 1, 2), ("08:00:01", 3, 4)],
        )
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 2
        assert events[0].event_code == 1
        assert events[1].event_code == 3

    def test_comma_delimited(self):
        """Test decoding comma-delimited SEPAC variant."""
        data = _build_sepac(
            delimiter=",",
            records=[("08:00:00", 1, 2)],
        )
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_semicolon_delimited(self):
        """Test decoding semicolon-delimited SEPAC variant."""
        data = _build_sepac(
            delimiter=";",
            records=[("08:00:00", 1, 2)],
        )
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1

    def test_skips_header_lines(self):
        """Test that non-data header lines are skipped."""
        data = _build_sepac(
            records=[("08:00:00", 1, 2)],
        )
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        # Should only get data records, not header lines
        assert len(events) == 1

    def test_time_only_uses_file_date(self):
        """Test time-only timestamps use date from header."""
        data = _build_sepac(
            date_line="Date: 03/01/2026",
            records=[("14:30:00", 82, 5)],
        )
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert events[0].timestamp == datetime(
            2026, 3, 1, 14, 30, 0, tzinfo=timezone.utc
        )

    def test_full_datetime_in_records(self):
        """Test records with full datetime strings."""
        lines = [
            "SEPAC Event Log",
            "Controller: Test-001",
            "Date: 01/15/2024",
            "",
            "timestamp\tevent_code\tevent_param",
            "01/15/2024 08:00:00\t1\t2",
        ]
        data = "\n".join(lines).encode("utf-8")
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp.hour == 8

    def test_empty_log(self):
        """Test header-only SEPAC file returns empty list."""
        data = _build_sepac(records=[])
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert events == []

    def test_alternate_date_format(self):
        """Test alternate date format in header."""
        data = _build_sepac(
            date_line="Date: 2024-01-15",
            records=[("08:00:00", 1, 2)],
        )
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp.year == 2024

    def test_can_decode_non_utf8_returns_false(self):
        """Non-UTF-8 data returns False from can_decode (ValueError path)."""
        # Build bytes that are invalid UTF-8
        data = b"\xc3\x28\xff\xfe"
        assert SiemensDecoder.can_decode(data) is False

    def test_no_header_row_returns_empty(self):
        """File with marker but no recognizable column header returns empty."""
        lines = [
            "SEPAC Event Log",
            "Controller: Test-001",
            "Date: 01/15/2024",
            "",
            "just some random text without column names",
            "08:00:00\t1\t2",
        ]
        data = "\n".join(lines).encode("utf-8")
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert events == []

    def test_bad_data_row_skipped(self):
        """Row with non-integer event code is skipped (ValueError/IndexError)."""
        lines = [
            "SEPAC Event Log",
            "Controller: Test-001",
            "Date: 01/15/2024",
            "",
            "time\tevent_code\tevent_param",
            "08:00:00\tNOT_A_NUMBER\t2",
            "08:01:00\t1\t3",
        ]
        data = "\n".join(lines).encode("utf-8")
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        # Only the second data row should parse
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_missing_event_param_column(self):
        """File with no event_param column uses default 0."""
        lines = [
            "SEPAC Event Log",
            "Controller: Test-001",
            "Date: 01/15/2024",
            "",
            "time\tevent_code",
            "08:00:00\t1",
        ]
        data = "\n".join(lines).encode("utf-8")
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_param == 0

    def test_no_date_line_uses_full_timestamp(self):
        """File without Date: line uses full datetime in records."""
        lines = [
            "SEPAC Event Log",
            "Controller: Test-001",
            "",
            "timestamp\tevent_code\tevent_param",
            "2024-01-15 08:00:00\t1\t2",
        ]
        data = "\n".join(lines).encode("utf-8")
        decoder = SiemensDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp.year == 2024
