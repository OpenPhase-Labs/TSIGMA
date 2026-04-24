"""
Roadside sensor models — radar / LiDAR / video devices that live at the
roadway edge and feed TSIGMA independently of the traffic controller.

Cabinet-connected detection (inductive loops, video-over-SDLC, etc.)
lives in ``tsigma.models.detector``.  Roadside sensors are a parallel
ingestion source: Wavetronix SmartSensor radar, Iteris Vantage video,
Houston Radar, FLIR TrafiCam, Quanergy / Ouster / Velodyne LiDAR, and
similar.  They publish per-vehicle speed / classification / queue-length
records over vendor protocols (TCP, HTTP, serial, FTP dumps, etc.) that
never pass through the controller.

See ``tsigma.models.event.RoadsideEvent`` for the event stream these
devices produce.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, tsigma_schema


class RoadsideSensor(Base, TimestampMixin):
    """
    Configuration for a single radar / LiDAR / video sensor.

    Associates a physical device (identified by vendor model + serial)
    with a TSIGMA signal and records how TSIGMA reaches it on the
    network.  Credentials are encrypted at rest via the project's
    ``SENSITIVE_FIELDS`` mechanism; see ``tsigma.config`` for keys.

    Per-lane / per-zone mapping lives in ``RoadsideSensorLane``.
    """

    __tablename__ = "roadside_sensor"

    sensor_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    # Vendor / model lookup — see tsigma.models.reference.RoadsideSensorModel.
    model_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("roadside_sensor_model.model_id"),
        nullable=False,
    )
    # Each sensor is associated with exactly one signal (ATSPM 5x convention).
    # Corridor-scale sensors that serve multiple signals would require a
    # separate join table and are deferred until an actual use case.
    signal_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("signal.signal_id"),
        nullable=False,
    )

    device_name: Mapped[str] = mapped_column(Text, nullable=False)
    serial_number: Mapped[Optional[str]] = mapped_column(Text)
    firmware_version: Mapped[Optional[str]] = mapped_column(Text)
    install_date: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
    )

    # Location + mounting
    latitude: Mapped[Optional[float]] = mapped_column(Numeric)
    longitude: Mapped[Optional[float]] = mapped_column(Numeric)
    mounting_height_feet: Mapped[Optional[float]] = mapped_column(Numeric)
    azimuth_degrees: Mapped[Optional[float]] = mapped_column(Numeric)

    # Network
    ip_address: Mapped[Optional[str]] = mapped_column(INET)
    port: Mapped[Optional[int]] = mapped_column(Integer)
    protocol: Mapped[Optional[str]] = mapped_column(
        Text,
    )  # overrides roadside_sensor_model.default_protocol when set
    username: Mapped[Optional[str]] = mapped_column(Text)
    password: Mapped[Optional[str]] = mapped_column(
        Text,
    )  # encrypted at rest via SENSITIVE_FIELDS

    # Capabilities — what event types this sensor emits
    emits_speed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false",
    )
    emits_classification: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false",
    )
    emits_queue_length: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false",
    )
    emits_occupancy: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false",
    )

    # Operational
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="true",
    )
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
    )

    # Vendor-specific config (detection zones, sensitivity, filters, etc.)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSONB)

    __table_args__ = (
        Index("idx_roadside_sensor_signal", "signal_id"),
        Index("idx_roadside_sensor_model", "model_id"),
        Index("idx_roadside_sensor_active", "is_active"),
        {"schema": tsigma_schema("config")},
    )


class RoadsideSensorLane(Base, TimestampMixin):
    """
    Lane-level mapping for a roadside sensor.

    A single sensor typically covers multiple lanes / zones and emits
    separate detection records per zone.  This table maps the sensor's
    own vendor lane identifier (``vendor_lane_id``) to a TSIGMA approach
    + lane number so downstream aggregation can resolve a vendor zone
    to a signal approach.
    """

    __tablename__ = "roadside_sensor_lane"

    zone_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    sensor_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("roadside_sensor.sensor_id", ondelete="CASCADE"),
        nullable=False,
    )
    approach_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("approach.approach_id"),
        nullable=False,
    )
    lane_number: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    vendor_lane_id: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        Index("idx_roadside_sensor_lane_sensor", "sensor_id"),
        Index("idx_roadside_sensor_lane_approach", "approach_id"),
        {"schema": tsigma_schema("config")},
    )
