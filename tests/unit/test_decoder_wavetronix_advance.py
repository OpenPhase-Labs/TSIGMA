"""
Unit tests for the Wavetronics SmartSensor Advance decoder.

Covers the five validation gates from Packet.cs (ported):

    1. Wrong packet length -> []
    2. Missing / wrong closing flags -> []
    3. mph/kph don't satisfy kph = round(mph * 1.609344) -> []
    4. Sensor tag contains non-alphanumeric characters -> []
    5. All gates pass -> single SensorDetection with populated mph/kph
"""

import struct

from tsigma.collection.decoders import DecoderRegistry, SensorDetection
from tsigma.collection.decoders.wavetronix_advance import WavetronixAdvanceDecoder
from tsigma.models.roadside_event import ROADSIDE_EVENT_TYPE_SPEED

_MI_TO_KM = 1.609344
_CLOSING_FLAGS = b"\x7e\x0d\x0d"
_PACKET_FORMAT = ">8xBB6s3s"


def _packet(
    mph: int,
    kph: int | None = None,
    tag: bytes = b"SIG001",
    closing: bytes = _CLOSING_FLAGS,
) -> bytes:
    """Build a 19-byte packet with optional overrides for each field."""
    if kph is None:
        kph = round(mph * _MI_TO_KM)
    return struct.pack(_PACKET_FORMAT, mph, kph, tag, closing)


class TestWavetronixAdvanceDecoder:
    def test_registered_by_name(self):
        cls = DecoderRegistry.get("wavetronix_advance")
        assert cls is WavetronixAdvanceDecoder

    def test_decodes_valid_packet(self):
        decoder = WavetronixAdvanceDecoder()
        detections = decoder.decode_bytes(_packet(mph=35))
        assert len(detections) == 1
        det = detections[0]
        assert isinstance(det, SensorDetection)
        assert det.event_type == ROADSIDE_EVENT_TYPE_SPEED
        assert det.mph == 35
        assert det.kph == 56  # round(35 * 1.609344) = 56
        assert det.vendor_tag == "SIG001"
        assert det.timestamp is not None

    def test_zero_mph_is_valid(self):
        """Round(0 * anything) = 0 — a valid boundary case, stopped vehicle."""
        decoder = WavetronixAdvanceDecoder()
        detections = decoder.decode_bytes(_packet(mph=0, kph=0))
        assert len(detections) == 1
        assert detections[0].mph == 0
        assert detections[0].kph == 0

    def test_short_packet_dropped(self):
        decoder = WavetronixAdvanceDecoder()
        assert decoder.decode_bytes(_packet(mph=35)[:10]) == []

    def test_long_packet_dropped(self):
        decoder = WavetronixAdvanceDecoder()
        assert decoder.decode_bytes(_packet(mph=35) + b"\x00") == []

    def test_empty_packet_dropped(self):
        decoder = WavetronixAdvanceDecoder()
        assert decoder.decode_bytes(b"") == []

    def test_wrong_closing_flags_dropped(self):
        decoder = WavetronixAdvanceDecoder()
        bad = _packet(mph=35, closing=b"\x00\x00\x00")
        assert decoder.decode_bytes(bad) == []

    def test_mph_kph_mismatch_dropped(self):
        """KPH must equal round(MPH * 1.609344); packets violating drop."""
        decoder = WavetronixAdvanceDecoder()
        bad = _packet(mph=35, kph=99)
        assert decoder.decode_bytes(bad) == []

    def test_non_alphanumeric_tag_dropped(self):
        decoder = WavetronixAdvanceDecoder()
        # Hyphen in tag — alphanumeric filter rejects it.
        bad = _packet(mph=35, tag=b"SIG-01")
        assert decoder.decode_bytes(bad) == []

    def test_non_ascii_tag_dropped(self):
        """Non-ASCII bytes in tag -> parse fails, no exception surfaces."""
        decoder = WavetronixAdvanceDecoder()
        bad = _packet(mph=35, tag=b"\xff\xff\xff\xff\xff\xff")
        assert decoder.decode_bytes(bad) == []

    def test_can_decode_rejects_wrong_shape(self):
        assert WavetronixAdvanceDecoder.can_decode(b"") is False
        assert WavetronixAdvanceDecoder.can_decode(b"x" * 18) is False
        assert WavetronixAdvanceDecoder.can_decode(b"x" * 20) is False

    def test_can_decode_rejects_wrong_closing(self):
        bad = _packet(mph=35, closing=b"\x00\x00\x00")
        assert WavetronixAdvanceDecoder.can_decode(bad) is False

    def test_can_decode_accepts_valid_shape(self):
        assert WavetronixAdvanceDecoder.can_decode(_packet(mph=35)) is True

    def test_representative_speeds_pass_kph_check(self):
        """Every 5-mph increment from 5 to 80 should round-trip cleanly."""
        decoder = WavetronixAdvanceDecoder()
        for mph in range(5, 81, 5):
            detections = decoder.decode_bytes(_packet(mph=mph))
            assert len(detections) == 1, f"mph={mph} failed"
            assert detections[0].mph == mph
