"""
End-to-end integration test for the Wavetronix speed ingestion path.

Exercises the full Python pipeline against a real PostgreSQL database:

    19-byte synthetic packet
        -> WavetronixAdvanceDecoder.decode_bytes
        -> SensorDetection list
        -> _upsert_events (type-dispatches to sensor path)
        -> vendor_tag -> (sensor_id UUID, signal_id) lookup via
           roadside_sensor_lane JOIN roadside_sensor
        -> INSERT INTO roadside_event
        -> SELECT confirms the row

Real-wire validation against Wavetronix hardware is not possible from
this test suite (no hardware).  This test closes the loop on
everything TSIGMA owns — decoder, persister, mapping lookup, table
schema — leaving only the "does real Wavetronix emit exactly this
byte shape?" question for the field validation.

Single-dialect PostgreSQL: ``roadside_event`` uses PG-specific
constructs (``pg_insert().on_conflict_do_nothing``, ``JSONB``,
TimescaleDB hypertable) so the test is scoped to the ``pg_engine``
fixture, not the parametrised ``dialect_engine``.
"""

from __future__ import annotations

import struct
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from tsigma.collection.decoders.wavetronix_advance import WavetronixAdvanceDecoder
from tsigma.collection.sdk import _upsert_events
from tsigma.models.approach import Approach
from tsigma.models.reference import (
    DirectionType,
    RoadsideSensorModel,
    RoadsideSensorVendor,
)
from tsigma.models.roadside_event import ROADSIDE_EVENT_TYPE_SPEED, RoadsideEvent
from tsigma.models.roadside_sensor import RoadsideSensor, RoadsideSensorLane
from tsigma.models.signal import Signal

pytestmark = pytest.mark.integration

_MI_TO_KM = 1.609344
_CLOSING_FLAGS = b"\x7e\x0d\x0d"
_VENDOR_TAG = "SIG001"


def _build_packet(mph: int, tag: bytes = _VENDOR_TAG.encode("ascii")) -> bytes:
    """Build a 19-byte Wavetronix Advance push packet."""
    kph = round(mph * _MI_TO_KM)
    return struct.pack(">8xBB6s3s", mph, kph, tag, _CLOSING_FLAGS)


@pytest_asyncio.fixture
async def seeded_pg(pg_engine: AsyncEngine):
    """Seed the minimum FK chain the Wavetronix path needs.

    Returns ``(engine, session_factory, sensor_id, approach_id)`` so
    tests can assert against the same identities they seeded.
    """
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    sensor_id = uuid4()
    approach_id = uuid4()
    vendor_id = uuid4()
    model_id = uuid4()

    async with session_factory() as session:
        # direction_type seed — 1 = NB per the model docstring.
        await session.execute(
            insert(DirectionType).values(
                direction_type_id=1, abbreviation="NB", description="Northbound",
            ),
        )

        # Roadside sensor vendor + model (reference tables; unseeded by default).
        await session.execute(
            insert(RoadsideSensorVendor).values(
                vendor_id=vendor_id, name="Wavetronix",
            ),
        )
        await session.execute(
            insert(RoadsideSensorModel).values(
                model_id=model_id,
                vendor_id=vendor_id,
                name="SmartSensor Advance",
                sensor_type="RADAR",
                default_protocol="TCP",
            ),
        )

        # Signal.
        await session.execute(
            insert(Signal).values(
                signal_id="SIG-TEST-001",
                primary_street="Peachtree Street",
                enabled=True,
            ),
        )

        # Approach.
        await session.execute(
            insert(Approach).values(
                approach_id=approach_id,
                signal_id="SIG-TEST-001",
                direction_type_id=1,
                description="Northbound approach",
                mph=35,
            ),
        )

        # Roadside sensor.
        await session.execute(
            insert(RoadsideSensor).values(
                sensor_id=sensor_id,
                model_id=model_id,
                signal_id="SIG-TEST-001",
                device_name="NB Advance #1",
                emits_speed=True,
            ),
        )

        # Sensor lane — vendor_lane_id MUST match the Wavetronix tag.
        await session.execute(
            insert(RoadsideSensorLane).values(
                sensor_id=sensor_id,
                approach_id=approach_id,
                lane_number=1,
                vendor_lane_id=_VENDOR_TAG,
            ),
        )

        await session.commit()

    return pg_engine, session_factory, sensor_id, approach_id


class TestWavetronixEndToEnd:
    @pytest.mark.asyncio
    async def test_synthetic_packet_lands_in_roadside_event(self, seeded_pg):
        """Full pipeline: packet -> decoder -> persister -> real row."""
        _engine, session_factory, sensor_id, _approach_id = seeded_pg

        decoder = WavetronixAdvanceDecoder()
        packet = _build_packet(mph=42)
        detections = decoder.decode_bytes(packet)

        assert len(detections) == 1, "decoder should emit one detection"

        await _upsert_events(
            detections,
            "ignored-for-sensor-path",
            session_factory,
        )

        # Assert the row is there, with the right columns.
        async with session_factory() as session:
            rows = (await session.execute(
                select(RoadsideEvent).where(
                    RoadsideEvent.sensor_id == sensor_id,
                ),
            )).scalars().all()

        assert len(rows) == 1, f"expected 1 roadside_event row, got {len(rows)}"
        row = rows[0]
        assert row.signal_id == "SIG-TEST-001"
        assert row.sensor_id == sensor_id
        assert row.event_type == ROADSIDE_EVENT_TYPE_SPEED
        assert row.mph == 42
        assert row.kph == round(42 * _MI_TO_KM)

    @pytest.mark.asyncio
    async def test_unregistered_vendor_tag_drops_event(self, seeded_pg):
        """Packet tag with no matching roadside_sensor_lane row -> no INSERT."""
        _engine, session_factory, _sensor_id, _approach_id = seeded_pg

        decoder = WavetronixAdvanceDecoder()
        unknown = _build_packet(mph=42, tag=b"NOMATC")
        detections = decoder.decode_bytes(unknown)
        assert len(detections) == 1

        await _upsert_events(
            detections, "ignored", session_factory,
        )

        async with session_factory() as session:
            count = (await session.execute(
                select(RoadsideEvent),
            )).scalars().all()
        assert len(count) == 0, "unresolved vendor_tag should insert nothing"

    @pytest.mark.asyncio
    async def test_re_ingest_is_idempotent(self, seeded_pg):
        """Same packet ingested twice -> one row (ON CONFLICT DO NOTHING)."""
        _engine, session_factory, sensor_id, _approach_id = seeded_pg

        decoder = WavetronixAdvanceDecoder()
        packet = _build_packet(mph=30)
        detections = decoder.decode_bytes(packet)

        await _upsert_events(detections, "ignored", session_factory)
        # Same detections dataclass -> same timestamp -> PK collision -> skipped.
        await _upsert_events(detections, "ignored", session_factory)

        async with session_factory() as session:
            rows = (await session.execute(
                select(RoadsideEvent).where(
                    RoadsideEvent.sensor_id == sensor_id,
                ),
            )).scalars().all()

        assert len(rows) == 1, "re-ingest must not duplicate on PK"
