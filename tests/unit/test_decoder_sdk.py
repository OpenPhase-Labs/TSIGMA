"""
Tests for tsigma.collection.decoders.sdk helpers.

Covers parse_timestamp edge cases (explicit fmt, explicit formats list,
time-only with file_date), detect_delimiter, and find_column_index.
"""

from datetime import datetime, timezone

import pytest

from tsigma.collection.decoders.sdk import (
    parse_timestamp,
)

# ---------------------------------------------------------------------------
# parse_timestamp — explicit fmt (line 160)
# ---------------------------------------------------------------------------


class TestParseTimestampExplicitFmt:
    """Tests for parse_timestamp with explicit fmt parameter."""

    def test_explicit_fmt_parses_correctly(self):
        """Explicit fmt uses only that format (line 160)."""
        result = parse_timestamp(
            "01-06-2025 14:30:00",
            fmt="%d-%m-%Y %H:%M:%S",
        )
        assert result == datetime(2025, 6, 1, 14, 30, 0, tzinfo=timezone.utc)

    def test_explicit_fmt_wrong_format_raises(self):
        """Explicit fmt that doesn't match raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            parse_timestamp("2025-06-01", fmt="%m/%d/%Y %H:%M:%S")


# ---------------------------------------------------------------------------
# parse_timestamp — explicit formats list (line 162)
# ---------------------------------------------------------------------------


class TestParseTimestampExplicitFormats:
    """Tests for parse_timestamp with explicit formats list."""

    def test_formats_list_tries_each(self):
        """Formats list tries each format in order (line 162)."""
        result = parse_timestamp(
            "06/01/2025 14:30:00",
            formats=["%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"],
        )
        assert result == datetime(2025, 6, 1, 14, 30, 0, tzinfo=timezone.utc)

    def test_formats_list_no_match_raises(self):
        """Formats list with no match raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            parse_timestamp("not-a-date", formats=["%Y-%m-%d"])


# ---------------------------------------------------------------------------
# parse_timestamp — time-only with file_date (lines 185-188)
# ---------------------------------------------------------------------------


class TestParseTimestampTimeOnly:
    """Tests for parse_timestamp with time-only strings and file_date."""

    def test_time_only_with_file_date(self):
        """Time-only string combined with file_date (lines 185-188)."""
        file_date = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = parse_timestamp("14:30:00", file_date=file_date)
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 0
        assert result.year == 2025
        assert result.month == 6
        assert result.day == 1

    def test_time_only_with_microseconds(self):
        """Time-only with microseconds and file_date."""
        file_date = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = parse_timestamp("14:30:00.123456", file_date=file_date)
        assert result.hour == 14
        assert result.microsecond == 123456

    def test_time_only_without_file_date_raises(self):
        """Time-only string without file_date raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            parse_timestamp("14:30:00")

    def test_time_only_no_match_raises(self):
        """Invalid time-only string with file_date raises ValueError."""
        file_date = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            parse_timestamp("not-a-time", file_date=file_date)


# ---------------------------------------------------------------------------
# parse_timestamp — whitespace stripping
# ---------------------------------------------------------------------------


class TestParseTimestampStripping:
    """Test that parse_timestamp strips whitespace."""

    def test_strips_leading_trailing_whitespace(self):
        """Whitespace around value is stripped."""
        result = parse_timestamp("  2025-06-01 14:30:00  ")
        assert result == datetime(2025, 6, 1, 14, 30, 0, tzinfo=timezone.utc)
