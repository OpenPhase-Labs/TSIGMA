"""
Unit tests for OpenPhase protobuf decoder.

Tests CompactEventBatch, IntersectionUpdate, and IntersectionUpdateBatch
decoding, and registry integration.

EventType enum values are Indiana Hi-Res event codes (Purdue/INDOT 2012),
so AtspmEvent.code and event_code are the same number.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

# Add proto directory so generated pb2 modules can resolve cross-imports.
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parent.parent.parent
        / "tsigma/collection/decoders/proto"
    ),
)

from google.protobuf.timestamp_pb2 import Timestamp  # noqa: E402
from openphase.v1 import common_pb2, ihr_events_pb2  # noqa: E402

from tsigma.collection.decoders.base import DecoderRegistry  # noqa: E402
from tsigma.collection.decoders.openphase import OpenPhaseDecoder  # noqa: E402


class TestRegistration:
    """OpenPhaseDecoder is discoverable via the DecoderRegistry."""

    def test_registered(self):
        cls = DecoderRegistry.get("openphase")
        assert cls is OpenPhaseDecoder


class TestCanDecode:
    """can_decode correctly accepts valid messages and rejects garbage."""

    def test_can_decode_compact_batch(self):
        """can_decode returns True for valid CompactEventBatch bytes."""
        batch = common_pb2.CompactEventBatch(
            intersection_id="INT-001",
            base_timestamp_ns=1_700_000_000_000_000_000,
        )
        evt = batch.events.add()
        evt.offset_ms = 0
        evt.code = ihr_events_pb2.EVENT_PHASE_BEGIN_GREEN
        evt.param = 1

        assert OpenPhaseDecoder.can_decode(batch.SerializeToString()) is True

    def test_can_decode_rejects_garbage(self):
        assert OpenPhaseDecoder.can_decode(b"\xde\xad\xbe\xef" * 10) is False


class TestCompactBatch:
    """Decode CompactEventBatch with delta-encoded timestamps."""

    def test_decode_compact_batch(self):
        base_ns = 1_700_000_000_000_000_000  # 2023-11-14 22:13:20 UTC

        batch = common_pb2.CompactEventBatch(
            intersection_id="INT-001",
            base_timestamp_ns=base_ns,
        )

        # Event 1: Phase Begin Green at offset 0 ms (IHR code 1)
        e1 = batch.events.add()
        e1.offset_ms = 0
        e1.code = ihr_events_pb2.EVENT_PHASE_BEGIN_GREEN
        e1.param = 2

        # Event 2: Detector On at offset 150 ms (IHR code 82)
        e2 = batch.events.add()
        e2.offset_ms = 150
        e2.code = ihr_events_pb2.EVENT_DETECTOR_ON
        e2.param = 5

        # Event 3: Phase Begin Yellow Clearance at offset 3000 ms (IHR code 8)
        e3 = batch.events.add()
        e3.offset_ms = 3000
        e3.code = ihr_events_pb2.EVENT_PHASE_BEGIN_YELLOW_CLEARANCE
        e3.param = 2

        decoder = OpenPhaseDecoder()
        events = decoder.decode_bytes(batch.SerializeToString())

        assert len(events) == 3

        # EventType integer == Indiana Hi-Res event code
        assert events[0].event_code == 1   # Phase Begin Green
        assert events[1].event_code == 82  # Detector On
        assert events[2].event_code == 8   # Phase Begin Yellow Clearance

        assert events[0].event_param == 2
        assert events[1].event_param == 5
        assert events[2].event_param == 2

        base_dt = datetime.fromtimestamp(
            base_ns / 1_000_000_000, tz=timezone.utc
        )
        from datetime import timedelta

        assert events[0].timestamp == base_dt
        assert events[1].timestamp == base_dt + timedelta(milliseconds=150)
        assert events[2].timestamp == base_dt + timedelta(milliseconds=3000)


class TestIntersectionUpdate:
    """Decode single IntersectionUpdate with AtspmEvent payload."""

    def _make_update(self, ts_seconds, event_code, event_param):
        """Build an IntersectionUpdate with an AtspmEvent."""
        ts = Timestamp(seconds=ts_seconds, nanos=0)

        atspm = ihr_events_pb2.AtspmEvent(
            code=event_code,
            param=event_param,
        )

        update = common_pb2.IntersectionUpdate(
            intersection_id="INT-042",
            ts=ts,
            event=atspm,
        )
        return update

    def test_decode_single_intersection_update(self):
        update = self._make_update(
            ts_seconds=1_700_000_000,
            event_code=ihr_events_pb2.EVENT_PHASE_BEGIN_GREEN,
            event_param=3,
        )

        decoder = OpenPhaseDecoder()
        events = decoder.decode_bytes(update.SerializeToString())

        assert len(events) == 1
        assert events[0].event_code == 1   # Phase Begin Green (IHR 1)
        assert events[0].event_param == 3
        assert events[0].timestamp == datetime(
            2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc
        )

    def test_decode_intersection_update_batch(self):
        u1 = self._make_update(
            ts_seconds=1_700_000_000,
            event_code=ihr_events_pb2.EVENT_DETECTOR_ON,
            event_param=5,
        )
        u2 = self._make_update(
            ts_seconds=1_700_000_001,
            event_code=ihr_events_pb2.EVENT_DETECTOR_OFF,
            event_param=5,
        )

        batch = common_pb2.IntersectionUpdateBatch(
            intersection_id="INT-042",
        )
        batch.updates.append(u1)
        batch.updates.append(u2)

        decoder = OpenPhaseDecoder()
        events = decoder.decode_bytes(batch.SerializeToString())

        assert len(events) == 2
        assert events[0].event_code == 82  # Detector On (IHR 82)
        assert events[1].event_code == 81  # Detector Off (IHR 81)
        assert events[0].event_param == 5
        assert events[1].event_param == 5

    def test_non_event_payload_skipped(self):
        """IntersectionUpdate with non-event payload returns None/empty."""
        from openphase.v1 import health_pb2

        ts = Timestamp(seconds=1_700_000_000, nanos=0)
        health = health_pb2.DeviceHealth()

        update = common_pb2.IntersectionUpdate(
            intersection_id="INT-042",
            ts=ts,
            health=health,
        )

        decoder = OpenPhaseDecoder()
        import pytest

        with pytest.raises(ValueError, match="does not match"):
            decoder.decode_bytes(update.SerializeToString())


class TestErrorHandling:
    """Invalid/empty inputs raise ValueError."""

    def test_empty_batch_raises(self):
        import pytest

        with pytest.raises(ValueError, match="does not match"):
            decoder = OpenPhaseDecoder()
            decoder.decode_bytes(b"\x00\x01\x02\x03")
