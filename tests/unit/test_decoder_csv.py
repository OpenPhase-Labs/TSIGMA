"""
Unit tests for generic CSV event log decoder.

Tests delimiter auto-detection, column mapping, date parsing,
and various CSV format variations.
"""


from tsigma.collection.decoders.base import DecoderRegistry
from tsigma.collection.decoders.csv_decoder import CSVConfig, CSVDecoder


class TestCSVRegistration:
    """Tests for CSVDecoder plugin registration."""

    def test_registered_in_registry(self):
        """Test CSVDecoder is registered as 'csv'."""
        assert "csv" in DecoderRegistry.list_all()
        cls = DecoderRegistry.get("csv")
        assert cls is CSVDecoder

    def test_extensions(self):
        """Test supported file extensions."""
        assert ".csv" in CSVDecoder.extensions
        assert ".txt" in CSVDecoder.extensions
        assert ".tsv" in CSVDecoder.extensions

    def test_has_description(self):
        """Test decoder has a description."""
        assert CSVDecoder.description


class TestCSVCanDecode:
    """Tests for CSVDecoder.can_decode()."""

    def test_valid_csv_with_header(self):
        """Test can_decode returns True for CSV with recognized columns."""
        data = b"timestamp,event_code,event_param\n2024-01-15 08:00:00,1,2\n"
        assert CSVDecoder.can_decode(data) is True

    def test_alternate_column_names(self):
        """Test can_decode recognizes alternate column names."""
        data = b"time,code,param\n2024-01-15 08:00:00,1,2\n"
        assert CSVDecoder.can_decode(data) is True

    def test_random_binary(self):
        """Test can_decode returns False for binary data."""
        assert CSVDecoder.can_decode(b"\x00\x01\x02\x03\x04") is False

    def test_empty_data(self):
        """Test can_decode returns False for empty data."""
        assert CSVDecoder.can_decode(b"") is False

    def test_no_recognized_columns(self):
        """Test can_decode returns False when no columns match."""
        data = b"foo,bar,baz\n1,2,3\n"
        assert CSVDecoder.can_decode(data) is False


class TestCSVDecodeBytes:
    """Tests for CSVDecoder.decode_bytes()."""

    def test_comma_delimited(self):
        """Test decoding comma-delimited CSV."""
        data = (
            b"timestamp,event_code,event_param\n"
            b"2024-01-15 08:00:00,1,2\n"
            b"2024-01-15 08:00:01,3,4\n"
        )
        decoder = CSVDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 2
        assert events[0].event_code == 1
        assert events[0].event_param == 2
        assert events[1].event_code == 3

    def test_tab_delimited(self):
        """Test decoding tab-delimited TSV."""
        data = (
            b"timestamp\tevent_code\tevent_param\n"
            b"2024-01-15 08:00:00\t1\t2\n"
        )
        decoder = CSVDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_semicolon_delimited(self):
        """Test decoding semicolon-delimited CSV."""
        data = (
            b"timestamp;event_code;event_param\n"
            b"2024-01-15 08:00:00;1;2\n"
        )
        decoder = CSVDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_alternate_column_names(self):
        """Test auto-detecting alternate column names."""
        data = (
            b"time,code,param\n"
            b"2024-01-15 08:00:00,1,2\n"
        )
        decoder = CSVDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2

    def test_datetime_column_name(self):
        """Test 'datetime' as timestamp column name."""
        data = (
            b"datetime,event_code,event_param\n"
            b"01/15/2024 08:00:00,1,2\n"
        )
        decoder = CSVDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp.year == 2024

    def test_multiple_date_formats(self):
        """Test parsing multiple date formats."""
        for ts_str in [
            b"2024-01-15 08:00:00",
            b"01/15/2024 08:00:00",
            b"2024-01-15T08:00:00",
        ]:
            data = b"timestamp,event_code,event_param\n" + ts_str + b",1,2\n"
            decoder = CSVDecoder()
            events = decoder.decode_bytes(data)
            assert len(events) == 1
            assert events[0].timestamp.month == 1

    def test_empty_file(self):
        """Test empty file returns empty list."""
        decoder = CSVDecoder()
        events = decoder.decode_bytes(b"")
        assert events == []

    def test_header_only(self):
        """Test header-only file returns empty list."""
        data = b"timestamp,event_code,event_param\n"
        decoder = CSVDecoder()
        events = decoder.decode_bytes(data)
        assert events == []

    def test_skips_blank_lines(self):
        """Test blank lines are skipped."""
        data = (
            b"timestamp,event_code,event_param\n"
            b"\n"
            b"2024-01-15 08:00:00,1,2\n"
            b"\n"
            b"2024-01-15 08:00:01,3,4\n"
        )
        decoder = CSVDecoder()
        events = decoder.decode_bytes(data)
        assert len(events) == 2

    def test_custom_config_delimiter(self):
        """Test CSVConfig with custom delimiter."""
        data = (
            b"timestamp|event_code|event_param\n"
            b"2024-01-15 08:00:00|1|2\n"
        )
        config = CSVConfig(delimiter="|")
        decoder = CSVDecoder(config=config)
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_custom_config_column_indices(self):
        """Test CSVConfig with explicit column indices."""
        data = (
            b"id,ts,ec,ep,extra\n"
            b"100,2024-01-15 08:00:00,1,2,x\n"
        )
        config = CSVConfig(
            timestamp_col=1,
            event_code_col=2,
            event_param_col=3,
        )
        decoder = CSVDecoder(config=config)
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2

    def test_custom_config_skip_rows(self):
        """Test CSVConfig with rows to skip."""
        data = (
            b"Some metadata line\n"
            b"Another metadata line\n"
            b"timestamp,event_code,event_param\n"
            b"2024-01-15 08:00:00,1,2\n"
        )
        config = CSVConfig(skip_rows=2)
        decoder = CSVDecoder(config=config)
        events = decoder.decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
