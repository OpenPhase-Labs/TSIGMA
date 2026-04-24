"""
Roadside sensor event stream.

Per-vehicle / per-lane detection records emitted by radar, LiDAR, and
video sensors deployed at the roadway edge.  Parallel to
``ControllerEventLog`` — that table holds Indiana Hi-Res records from
the controller; this table holds records from sensors that never pass
through the controller at all.

A single table with an ``event_type`` discriminator covers speed,
classification, queue-length, and occupancy events.  Vendor-specific
extra fields land in ``vendor_metadata`` (JSONB).  The active columns
per event type are:

    SPEED            -> mph, kph, length_feet, lane_number, direction_id
    CLASSIFICATION   -> vehicle_class, length_feet, lane_number, direction_id
    QUEUE            -> queue_length_feet, lane_number
    OCCUPANCY        -> occupancy_pct, lane_number

Partitioning matches ``controller_event_log``: TimescaleDB hypertable
on PostgreSQL, dialect-native partitioning on MS-SQL / Oracle / MySQL.
See the alembic initial-schema migration for the dialect-specific
CREATE TABLE.  The ORM model below reflects the PostgreSQL shape;
runtime reads go through ``db_facade`` so dialect differences are
hidden.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import Index, Integer, Numeric, SmallInteger
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema

# ---------------------------------------------------------------------------
# event_type discriminator values.  Kept as integer constants rather than a
# reference table so the hot path (ingestion + aggregation) never joins.
# ---------------------------------------------------------------------------
ROADSIDE_EVENT_TYPE_SPEED = 1
ROADSIDE_EVENT_TYPE_CLASSIFICATION = 2
ROADSIDE_EVENT_TYPE_QUEUE = 3
ROADSIDE_EVENT_TYPE_OCCUPANCY = 4


class RoadsideEvent(Base):
    """
    Per-detection event from a roadside sensor.

    PK is ``(signal_id, sensor_id, event_time, event_type)`` — sensors
    can emit multiple event types (speed + occupancy) at the same
    instant so all four columns participate.
    """

    __tablename__ = "roadside_event"

    signal_id: Mapped[str] = mapped_column(primary_key=True)
    sensor_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True,
    )
    event_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    event_type: Mapped[int] = mapped_column(SmallInteger, primary_key=True)

    # Type-specific columns — populated only for the relevant event_type.
    mph: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    kph: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    length_feet: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    vehicle_class: Mapped[Optional[int]] = mapped_column(
        SmallInteger, nullable=True,
    )
    lane_number: Mapped[Optional[int]] = mapped_column(
        SmallInteger, nullable=True,
    )
    direction_id: Mapped[Optional[int]] = mapped_column(
        SmallInteger, nullable=True,
    )
    occupancy_pct: Mapped[Optional[float]] = mapped_column(
        Numeric(5, 2), nullable=True,
    )
    queue_length_feet: Mapped[Optional[float]] = mapped_column(
        Numeric(8, 2), nullable=True,
    )

    # Vendor-specific long tail (confidence scores, track IDs, raw payload
    # fragments, etc.).  Use sparingly — any field reused across > 1 vendor
    # deserves promotion to a first-class column.
    vendor_metadata: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        Index(
            "idx_re_sensor_time", "sensor_id", "event_time",
            postgresql_ops={"event_time": "DESC"},
        ),
        Index(
            "idx_re_signal_time", "signal_id", "event_time",
            postgresql_ops={"event_time": "DESC"},
        ),
        Index(
            "idx_re_event_type_time", "event_type", "event_time",
            postgresql_ops={"event_time": "DESC"},
        ),
        {"schema": tsigma_schema("events")},
    )
