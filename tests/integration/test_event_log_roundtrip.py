"""Insert/read round-trips for the event-stream tables.

Covers the happy path for ``controller_event_log`` and ``roadside_event``
across every supported dialect, plus a PostgreSQL-only check that the
``INSERT ... ON CONFLICT DO NOTHING`` path wired into
``tsigma.collection.sdk._upsert_events`` is genuinely idempotent.

The idempotency check is scoped to PostgreSQL because the helper uses
``sqlalchemy.dialects.postgresql.insert`` — other dialects have their
own MERGE-equivalent paths that are exercised by
``tsigma.database.db.DatabaseFacade`` tests and are not in scope here.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.collection.sdk import _upsert_events
from tsigma.models.event import ControllerEventLog
from tsigma.models.roadside_event import (
    ROADSIDE_EVENT_TYPE_CLASSIFICATION,
    ROADSIDE_EVENT_TYPE_OCCUPANCY,
    ROADSIDE_EVENT_TYPE_QUEUE,
    ROADSIDE_EVENT_TYPE_SPEED,
    RoadsideEvent,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_controller_event_log_insert_and_read(
    dialect_session: AsyncSession,
) -> None:
    """Three ORM inserts round-trip back through a ``SELECT``."""
    signal_id = "SIG-ROUNDTRIP"
    base = datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        ControllerEventLog(
            signal_id=signal_id,
            event_time=base + timedelta(seconds=i),
            event_code=1,
            event_param=i,
        )
        for i in range(3)
    ]
    dialect_session.add_all(rows)
    await dialect_session.flush()

    result = await dialect_session.execute(
        select(ControllerEventLog).where(
            ControllerEventLog.signal_id == signal_id,
        )
    )
    fetched = list(result.scalars())

    assert len(fetched) == 3
    fetched.sort(key=lambda r: r.event_param)
    for i, row in enumerate(fetched):
        assert row.event_code == 1
        assert row.event_param == i
        assert row.event_time == base + timedelta(seconds=i)


@pytest.mark.asyncio
async def test_roadside_event_insert_and_read(
    dialect_session: AsyncSession,
) -> None:
    """One row per event_type, each with only its active columns set.

    Per the model docstring, the active columns per event_type are:

        SPEED          -> mph, kph, length_feet, lane_number, direction_id
        CLASSIFICATION -> vehicle_class, length_feet, lane_number, direction_id
        QUEUE          -> queue_length_feet, lane_number
        OCCUPANCY      -> occupancy_pct, lane_number
    """
    signal_id = "SIG-ROADSIDE"
    sensor_id = uuid4()
    base = datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc)

    speed = RoadsideEvent(
        signal_id=signal_id,
        sensor_id=sensor_id,
        event_time=base,
        event_type=ROADSIDE_EVENT_TYPE_SPEED,
        mph=55, kph=88, length_feet=18, lane_number=2, direction_id=1,
    )
    classification = RoadsideEvent(
        signal_id=signal_id,
        sensor_id=sensor_id,
        event_time=base + timedelta(seconds=1),
        event_type=ROADSIDE_EVENT_TYPE_CLASSIFICATION,
        vehicle_class=3, length_feet=40, lane_number=2, direction_id=1,
    )
    queue = RoadsideEvent(
        signal_id=signal_id,
        sensor_id=sensor_id,
        event_time=base + timedelta(seconds=2),
        event_type=ROADSIDE_EVENT_TYPE_QUEUE,
        queue_length_feet=125.5, lane_number=1,
    )
    occupancy = RoadsideEvent(
        signal_id=signal_id,
        sensor_id=sensor_id,
        event_time=base + timedelta(seconds=3),
        event_type=ROADSIDE_EVENT_TYPE_OCCUPANCY,
        occupancy_pct=42.00, lane_number=1,
    )
    dialect_session.add_all([speed, classification, queue, occupancy])
    await dialect_session.flush()

    result = await dialect_session.execute(
        select(RoadsideEvent).where(
            RoadsideEvent.signal_id == signal_id,
        )
    )
    rows = {r.event_type: r for r in result.scalars()}

    assert set(rows) == {
        ROADSIDE_EVENT_TYPE_SPEED,
        ROADSIDE_EVENT_TYPE_CLASSIFICATION,
        ROADSIDE_EVENT_TYPE_QUEUE,
        ROADSIDE_EVENT_TYPE_OCCUPANCY,
    }

    s = rows[ROADSIDE_EVENT_TYPE_SPEED]
    assert (s.mph, s.kph, s.length_feet, s.lane_number, s.direction_id) == (
        55, 88, 18, 2, 1,
    )
    assert s.vehicle_class is None
    assert s.occupancy_pct is None
    assert s.queue_length_feet is None

    c = rows[ROADSIDE_EVENT_TYPE_CLASSIFICATION]
    assert (c.vehicle_class, c.length_feet, c.lane_number, c.direction_id) == (
        3, 40, 2, 1,
    )
    assert c.mph is None
    assert c.kph is None
    assert c.occupancy_pct is None
    assert c.queue_length_feet is None

    q = rows[ROADSIDE_EVENT_TYPE_QUEUE]
    assert float(q.queue_length_feet) == 125.5
    assert q.lane_number == 1
    assert q.mph is None
    assert q.vehicle_class is None
    assert q.occupancy_pct is None

    o = rows[ROADSIDE_EVENT_TYPE_OCCUPANCY]
    assert float(o.occupancy_pct) == 42.00
    assert o.lane_number == 1
    assert o.mph is None
    assert o.vehicle_class is None
    assert o.queue_length_feet is None


@pytest.mark.asyncio
async def test_controller_event_log_idempotent_reinsert_postgresql(
    pg_engine,
) -> None:
    """Re-inserting the same events via ``_upsert_events`` is a no-op.

    PostgreSQL-only because the helper uses
    ``sqlalchemy.dialects.postgresql.insert().on_conflict_do_nothing()``.
    """
    signal_id = "SIG-IDEMPOTENT"
    base = datetime(2026, 4, 24, 13, 0, 0, tzinfo=timezone.utc)
    events = [
        DecodedEvent(
            timestamp=base + timedelta(seconds=i),
            event_code=7,
            event_param=i,
        )
        for i in range(3)
    ]

    session_factory = async_sessionmaker(
        bind=pg_engine, class_=AsyncSession, expire_on_commit=False,
    )

    await _upsert_events(events, signal_id, session_factory)
    await _upsert_events(events, signal_id, session_factory)

    async with session_factory() as session:
        result = await session.execute(
            select(ControllerEventLog).where(
                ControllerEventLog.signal_id == signal_id,
            )
        )
        fetched = list(result.scalars())

    assert len(fetched) == 3, (
        f"expected 3 rows after idempotent re-insert, got {len(fetched)}"
    )
