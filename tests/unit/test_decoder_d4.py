"""Unit tests for the D4 (Fourth Dimension Traffic) controller event-log decoder."""

import gzip
from datetime import datetime, timezone

import pytest

from tsigma.collection.decoders.auto import AutoDecoder
from tsigma.collection.decoders.base import DecoderRegistry
from tsigma.collection.decoders.csv_decoder import CSVDecoder
from tsigma.collection.decoders.d4 import D4Decoder


class TestD4Registration:
    def test_registered_in_registry(self):
        assert "d4" in DecoderRegistry.list_all()
        assert DecoderRegistry.get("d4") is D4Decoder

    def test_extensions(self):
        assert ".d4" in D4Decoder.extensions

    def test_has_description(self):
        assert D4Decoder.description

    def test_claims_csv_and_gz_extensions(self):
        # KH globs *.csv?/*.gz for D4 files, so d4 claims those extensions too.
        assert ".d4" in D4Decoder.extensions
        assert ".csv" in D4Decoder.extensions
        assert ".gz" in D4Decoder.extensions
        assert D4Decoder in DecoderRegistry.get_for_extension(".gz")
        assert D4Decoder in DecoderRegistry.get_for_extension(".csv")


class TestD4DecodeFourColumn:
    def test_basic_four_column(self):
        # event_code, event_param, timestamp, msg_idx
        data = (
            b"1,2,2024-01-15 08:00:00,1001\n"
            b"82,5,2024-01-15 08:00:15,1002\n"
        )
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 2
        assert events[0].event_code == 1
        assert events[0].event_param == 2
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
        assert events[1].event_code == 82
        assert events[1].event_param == 5
        assert events[1].timestamp == datetime(2024, 1, 15, 8, 0, 15, tzinfo=timezone.utc)

    def test_empty_data(self):
        assert D4Decoder().decode_bytes(b"") == []


class TestD4Robustness:
    def test_skips_header_row(self):
        data = (
            b"EventCode,Param,DateTime,MsgIdx\n"
            b"1,2,2024-01-15 08:00:00,1001\n"
        )
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1

    def test_skips_blank_lines(self):
        data = (
            b"1,2,2024-01-15 08:00:00,1001\n"
            b"\n"
            b"3,4,2024-01-15 08:00:01,1002\n"
            b"\n"
        )
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 2
        assert [e.event_code for e in events] == [1, 3]

    def test_skips_malformed_lines(self):
        data = (
            b"1,2,2024-01-15 08:00:00,1001\n"
            b"garbage,not,a,row\n"
            b"3,4,2024-01-15 08:00:01,1002\n"
        )
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 2
        assert [e.event_code for e in events] == [1, 3]

    def test_skips_row_with_bad_timestamp(self):
        # 4 columns with valid ints but an unparseable timestamp:
        # must be skipped (timestamp-parse branch), not crash.
        data = (
            b"1,2,not-a-timestamp,1001\n"
            b"3,4,2024-01-15 08:00:01,1002\n"
        )
        events = D4Decoder().decode_bytes(data)
        assert [e.event_code for e in events] == [3]

    def test_too_few_columns_skipped(self):
        data = b"1,2,2024-01-15 08:00:00\n"  # only 3 fields
        assert D4Decoder().decode_bytes(data) == []


class TestD4DecodeStreamed:
    def test_five_column_streamed(self):
        # event_code, event_name, event_param, timestamp, msg_idx
        data = (
            b"1,PhaseGreen,2,2024-01-15 08:00:00,1001\n"
            b"82,DetectorOn,5,2024-01-15 08:00:15,1002\n"
        )
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 2
        assert events[0].event_code == 1
        assert events[0].event_param == 2  # from index 2, not the name at index 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
        assert events[1].event_code == 82
        assert events[1].event_param == 5

    def test_streamed_param_from_index_2_not_1(self):
        # index 1 is NUMERIC here (99): proves event_param is read from
        # index 2 (==2), not index 1 (==99).
        data = b"1,99,2,2024-01-15 08:00:00,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2

    def test_streamed_event_name_discarded(self):
        data = b"7,SomeLabel,3,2024-01-15 09:30:00,42\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 7
        assert events[0].event_param == 3
        assert events[0].timestamp == datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)


class TestD4Gzip:
    def test_decodes_gzip_four_column(self):
        raw = (
            b"1,2,2024-01-15 08:00:00,1001\n"
            b"3,4,2024-01-15 08:00:01,1002\n"
        )
        events = D4Decoder().decode_bytes(gzip.compress(raw))
        assert len(events) == 2
        assert events[0].event_code == 1
        assert events[1].event_code == 3

    def test_decodes_gzip_streamed(self):
        raw = b"7,SomeLabel,3,2024-01-15 09:30:00,42\n"
        events = D4Decoder().decode_bytes(gzip.compress(raw))
        assert len(events) == 1
        assert events[0].event_code == 7
        assert events[0].event_param == 3

    def test_corrupt_gzip_propagates(self):
        # gzip magic (1f 8b) but invalid body: the decoder must NOT silently
        # swallow it -- it raises, and callers quarantine the file.
        corrupt = b"\x1f\x8bnot-a-real-gzip-body"
        with pytest.raises(Exception):
            D4Decoder().decode_bytes(corrupt)


class TestD4CanDecode:
    def test_positional_four_column(self):
        assert D4Decoder.can_decode(b"1,2,2024-01-15 08:00:00,1001\n") is True

    def test_positional_streamed(self):
        data = b"1,PhaseGreen,2,2024-01-15 08:00:00,1001\n"
        assert D4Decoder.can_decode(data) is True

    def test_gzip(self):
        raw = b"1,2,2024-01-15 08:00:00,1001\n"
        assert D4Decoder.can_decode(gzip.compress(raw)) is True

    def test_defers_named_csv(self):
        # name-tagged CSV belongs to the generic 'csv' decoder, not d4 -- and
        # csv must actually claim it (guards against one-sided NAME-set drift).
        data = b"timestamp,event_code,event_param\n2024-01-15 08:00:00,1,2\n"
        assert D4Decoder.can_decode(data) is False
        assert CSVDecoder.can_decode(data) is True

    def test_corrupt_gzip_rejected_not_raised(self):
        # gzip magic but invalid body: can_decode returns False, never raises.
        assert D4Decoder.can_decode(b"\x1f\x8bnot-a-real-gzip-body") is False

    def test_binary_rejected(self):
        assert D4Decoder.can_decode(b"\x00\x01\x02\x03\x04") is False

    def test_empty_rejected(self):
        assert D4Decoder.can_decode(b"") is False


class TestD4AutoDetect:
    def test_auto_routes_positional_to_d4(self):
        # Headerless positional CSV is claimed by no other decoder; auto must
        # route it to d4 and decode it.
        data = (
            b"1,2,2024-01-15 08:00:00,1001\n"
            b"82,5,2024-01-15 08:00:15,1002\n"
        )
        events = AutoDecoder().decode_bytes(data)
        assert len(events) == 2
        assert events[0].event_code == 1
        assert events[1].event_code == 82

    def test_auto_routes_gzip_d4(self):
        raw = b"7,SomeLabel,3,2024-01-15 09:30:00,42\n"
        events = AutoDecoder().decode_bytes(gzip.compress(raw))
        assert len(events) == 1
        assert events[0].event_code == 7
        assert events[0].event_param == 3

    def test_auto_still_routes_named_csv_to_csv(self):
        # Regression: name-tagged CSV must NOT be hijacked by d4.
        data = b"timestamp,event_code,event_param\n2024-01-15 08:00:00,1,2\n"
        events = AutoDecoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2


class TestD4TimestampFormats:
    """KH parity: .NET CurrentCulture accepts these timestamp styles, so the
    D4 decoder must too (otherwise these rows are silently dropped)."""

    def test_twelve_hour_am(self):
        data = b"1,2,01/15/2024 8:00:00 AM,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_twelve_hour_pm(self):
        data = b"1,2,01/15/2024 1:30:00 PM,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 13, 30, 0, tzinfo=timezone.utc)

    def test_no_seconds_24h(self):
        data = b"1,2,01/15/2024 08:00,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_date_only(self):
        data = b"1,2,01/15/2024,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

    def test_iso_no_seconds(self):
        data = b"1,2,2024-01-15 08:00,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_full_datetime_not_truncated_by_date_only(self):
        # Ordering safety: a full slash datetime must keep its time and NOT be
        # truncated to midnight by the date-only format.
        data = b"1,2,01/15/2024 08:00:00,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_can_decode_date_only_file(self):
        # KH parity: date-only positional rows are valid D4, so can_decode claims them.
        assert D4Decoder.can_decode(b"1,2,01/15/2024,1001\n") is True


class TestD4TwoDigitYear:
    """KH parity: .NET accepts 2-digit years; '24' resolves to 2024."""

    def test_two_digit_year_am_pm(self):
        data = b"1,2,01/15/24 8:00:00 AM,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_two_digit_year_24h(self):
        data = b"1,2,01/15/24 08:00:00,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_two_digit_year_date_only(self):
        data = b"1,2,01/15/24,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

    def test_four_digit_year_not_mangled_by_two_digit_format(self):
        # Regression: a 4-digit-year datetime must NOT be mis-parsed (e.g. to
        # year 0024) by any 2-digit-year format.
        data = b"1,2,01/15/2024 08:00:00,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_two_digit_year_ampm_no_seconds(self):
        data = b"1,2,01/15/24 8:00 AM,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_two_digit_year_24h_no_seconds(self):
        data = b"1,2,01/15/24 08:00,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_four_digit_date_only_year_correct(self):
        # Cross-match guard: a 4-digit date-only string must yield 2024, NOT be
        # captured/mangled by the 2-digit %m/%d/%y format.
        data = b"1,2,01/15/2024,1001\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].timestamp == datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

    def test_can_decode_two_digit_year_file(self):
        # KH parity: a 2-digit-year positional row is valid D4, so can_decode claims it.
        assert D4Decoder.can_decode(b"1,2,01/15/24,1001\n") is True


class TestD4QuotedFields:
    """CsvHelper parity: RFC-4180 quoted fields (commas/escaped quotes inside
    a quoted EventName) must parse as a single field, not be split."""

    def test_streamed_quoted_event_name_with_comma(self):
        # 5-column streamed; EventName is quoted and contains a comma.
        # It must remain ONE field so param=index2 and timestamp=index3.
        data = b'1,"Coord, Pattern 3",2,2024-01-15 08:00:00,1001\n'
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_streamed_quoted_event_name_escaped_quote(self):
        # Escaped quote ("") inside a quoted EventName must not break parsing.
        data = b'7,"Phase ""A"" change",3,2024-01-15 09:30:00,42\n'
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 7
        assert events[0].event_param == 3
        assert events[0].timestamp == datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)

    def test_can_decode_quoted_streamed(self):
        data = b'1,"Coord, Pattern 3",2,2024-01-15 08:00:00,1001\n'
        assert D4Decoder.can_decode(data) is True


class TestD4CsvRobustness:
    def test_trailing_comma_four_column(self):
        # A 4-column row with a trailing comma (empty 5th field) must still be
        # read as 4-column, not misclassified as the 5-column streamed layout
        # and dropped. (CsvHelper tolerates the trailing column via index map.)
        data = b"1,2,2024-01-15 08:00:00,1001,\n"
        events = D4Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_can_decode_oversized_field_does_not_raise(self):
        # csv.reader raises _csv.Error on a field over the size limit; can_decode
        # is a detector probed across decoders and must return False, never raise.
        assert D4Decoder.can_decode(b"a" * 200000) is False
