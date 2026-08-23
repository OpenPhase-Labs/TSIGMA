"""
Unit tests for the ASC/3 (Econolite) Hi-Resolution event log decoder.

Real Econolite samples in tests/fixtures/asc3/ are the source of truth.
Synthetic data uses the real comma-delimited header format; one synthetic
case retains the legacy slash date format.
"""

import gzip
import struct
import zlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tsigma.collection.decoders.asc3 import ASC3DecodeError, ASC3Decoder
from tsigma.collection.decoders.base import DecodeResult, DecoderRegistry, FileMetadata
from tsigma.collection.decoders.csv_decoder import CSVDecoder

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "asc3"
_REAL_DATZ = _FIXTURES / "1210_ECON_10.204.7.239_2021_08_09_1841.datZ"
_REAL_DAT = _FIXTURES / "4895_ECON_10.210.8.179_2024_02_21_1115.dat"


def _build_asc3(anchor="01/15/2024 08:00:00", records=None, line6_anchor=None):
    """Build raw ASC/3 data in the real comma-delimited header format.

    Seven comma-delimited header lines (each prefixed with the anchor
    datetime), then binary >BBH records. `anchor` is the line-1 leading
    datetime field (parsed up to the first comma); `line6_anchor` overrides
    the line-6 "Controller Data Log Beginning" secondary anchor (defaults
    to `anchor`).
    """
    l6 = anchor if line6_anchor is None else line6_anchor
    lines = [
        f"{anchor},Version #:,3",
        f"{anchor},ECON_synthetic.dat",
        f"{anchor},Intersection #:,1.2.3.4",
        f"{anchor},IP Address:,1.2.3.4",
        f"{anchor},MAC Address:,00:04:81:00:00:00",
        f"{anchor},Controller Data Log Beginning:,{l6}",
        f"{anchor},Phases in use:,2,4,6,8",
    ]
    body = ("\n".join(lines) + "\n").encode("ascii")
    for event_code, event_param, time_offset in (records or []):
        body += struct.pack(">BBH", event_code, event_param, time_offset)
    return body


class TestASC3Registration:
    def test_registered_in_registry(self):
        assert "asc3" in DecoderRegistry.list_all()
        assert DecoderRegistry.get("asc3") is ASC3Decoder

    def test_extensions(self):
        assert ".dat" in ASC3Decoder.extensions
        assert ".datz" in ASC3Decoder.extensions

    def test_has_description(self):
        assert ASC3Decoder.description


class TestASC3CanDecode:
    def test_real_datz(self):
        assert ASC3Decoder.can_decode(_REAL_DATZ.read_bytes()) is True

    def test_real_dat(self):
        assert ASC3Decoder.can_decode(_REAL_DAT.read_bytes()) is True

    def test_synthetic_comma_header(self):
        assert ASC3Decoder.can_decode(_build_asc3()) is True

    def test_zlib_compressed(self):
        assert ASC3Decoder.can_decode(zlib.compress(_build_asc3())) is True

    def test_gzip_compressed(self):
        assert ASC3Decoder.can_decode(gzip.compress(_build_asc3())) is True

    def test_random_bytes(self):
        assert ASC3Decoder.can_decode(b"\x00\x01\x02\x03\x04") is False

    def test_empty_data(self):
        assert ASC3Decoder.can_decode(b"") is False


class TestASC3RealFixtures:
    def test_datz_decodes_275_events(self):
        events = ASC3Decoder().decode_bytes(_REAL_DATZ.read_bytes())
        assert len(events) == 275
        assert events[0].event_code == 7
        assert events[0].event_param == 2
        assert events[0].timestamp == datetime(2021, 8, 9, 18, 42, 6, 700000, tzinfo=timezone.utc)
        assert events[-1].event_code == 43
        assert events[-1].event_param == 6
        assert events[-1].timestamp == datetime(2021, 8, 9, 18, 44, 57, 100000, tzinfo=timezone.utc)

    def test_dat_decodes_1914_events(self):
        events = ASC3Decoder().decode_bytes(_REAL_DAT.read_bytes())
        assert len(events) == 1914
        assert events[0].event_code == 81
        assert events[0].event_param == 29
        assert events[0].timestamp == datetime(2024, 2, 21, 11, 15, 1, tzinfo=timezone.utc)
        assert events[-1].event_code == 43
        assert events[-1].event_param == 3
        assert events[-1].timestamp == datetime(2024, 2, 21, 11, 28, 41, 600000, tzinfo=timezone.utc)


class TestASC3PoisonAware:
    def test_unparseable_anchor_raises(self):
        # line-1 leading field is not a parseable datetime in any accepted
        # format -> fail closed (raise), never fabricate a base.
        data = _build_asc3(anchor="not-a-date")
        with pytest.raises(ASC3DecodeError):
            ASC3Decoder().decode_bytes(data)


class TestASC3DecodeBytes:
    def test_parses_slash_legacy_anchor(self):
        # retained legacy slash date format
        data = _build_asc3(anchor="03/01/2026 14:30:00", records=[(1, 2, 0)])
        events = ASC3Decoder().decode_bytes(data)
        assert events[0].timestamp == datetime(2026, 3, 1, 14, 30, 0, tzinfo=timezone.utc)

    def test_parses_hyphen_fractional_anchor(self):
        # real Econolite format: hyphen, non-padded, fractional seconds
        data = _build_asc3(anchor="8-9-2021 18:41:58.4", records=[(1, 2, 0)])
        events = ASC3Decoder().decode_bytes(data)
        assert events[0].timestamp == datetime(2021, 8, 9, 18, 41, 58, 400000, tzinfo=timezone.utc)

    def test_single_record(self):
        data = _build_asc3(records=[(1, 2, 10)])
        events = ASC3Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 1
        assert events[0].event_param == 2

    def test_time_offset_tenths(self):
        data = _build_asc3(anchor="01/15/2024 08:00:00", records=[(82, 5, 153)])
        events = ASC3Decoder().decode_bytes(data)
        assert events[0].timestamp == datetime(2024, 1, 15, 8, 0, 15, 300000, tzinfo=timezone.utc)

    def test_multiple_records(self):
        data = _build_asc3(records=[(1, 2, 10), (82, 5, 153), (8, 2, 450), (81, 5, 158)])
        events = ASC3Decoder().decode_bytes(data)
        assert [e.event_code for e in events] == [1, 82, 8, 81]

    def test_empty_records(self):
        assert ASC3Decoder().decode_bytes(_build_asc3(records=[])) == []

    def test_zlib_compressed(self):
        data = zlib.compress(_build_asc3(records=[(1, 2, 10), (3, 4, 20)]))
        events = ASC3Decoder().decode_bytes(data)
        assert len(events) == 2
        assert events[0].event_code == 1

    def test_gzip_compressed(self):
        data = gzip.compress(_build_asc3(records=[(5, 6, 100)]))
        events = ASC3Decoder().decode_bytes(data)
        assert len(events) == 1
        assert events[0].event_code == 5

    def test_high_time_offset(self):
        data = _build_asc3(anchor="01/15/2024 08:00:00", records=[(1, 1, 36000)])
        events = ASC3Decoder().decode_bytes(data)
        assert events[0].timestamp == datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)


class TestASC3Envelope:
    def test_decode_returns_result_with_metadata(self):
        result = ASC3Decoder().decode(_REAL_DATZ.read_bytes())
        assert isinstance(result, DecodeResult)
        assert len(result.events) == 275
        assert result.events[0].event_code == 7
        assert result.events[-1].event_code == 43
        m = result.metadata
        assert isinstance(m, FileMetadata)
        assert m.device_ip == "10.204.7.239"
        assert m.device_mac == "00:04:81:02:15:0d"
        assert m.log_version == "3"
        assert m.source_filename == "ECON_10.204.7.239_2021_08_09_1841.dat"
        assert m.phases_in_use == [2, 4, 5, 6, 8]
        assert m.header_anchor == datetime(2021, 8, 9, 18, 41, 58, 400000, tzinfo=timezone.utc)
        assert m.header_anchor_secondary == datetime(2021, 8, 9, 18, 41, 58, 400000, tzinfo=timezone.utc)
        assert m.log_begin == datetime(2021, 8, 9, 18, 41, 58, 400000, tzinfo=timezone.utc)

    def test_decode_events_match_decode_bytes(self):
        data = _REAL_DATZ.read_bytes()
        assert ASC3Decoder().decode(data).events == ASC3Decoder().decode_bytes(data)

    def test_consistent_anchors_match(self):
        # real file: line-1 and line-6 anchors are equal -> not a mismatch
        m = ASC3Decoder().decode(_REAL_DATZ.read_bytes()).metadata
        assert m.header_anchor == m.header_anchor_secondary

    def test_line1_line6_mismatch_surfaced(self):
        # line-6 anchor differs from line-1: both surfaced in metadata and
        # not equal; decode does NOT raise (flag, not fail).
        data = _build_asc3(
            anchor="01/15/2024 08:00:00",
            line6_anchor="01/15/2024 09:00:00",
            records=[(1, 2, 0)],
        )
        m = ASC3Decoder().decode(data).metadata
        assert m.header_anchor == datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
        assert m.header_anchor_secondary == datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
        assert m.header_anchor != m.header_anchor_secondary


class TestDecodeEnvelopeDefault:
    def test_metadata_less_decoder_returns_none_metadata(self):
        # BaseDecoder.decode() default wraps decode_bytes with metadata=None.
        data = b"timestamp,event_code,event_param\n2024-01-15 08:00:00,1,2\n"
        result = CSVDecoder().decode(data)
        assert isinstance(result, DecodeResult)
        assert result.metadata is None
        assert result.events == CSVDecoder().decode_bytes(data)
        assert len(result.events) == 1
