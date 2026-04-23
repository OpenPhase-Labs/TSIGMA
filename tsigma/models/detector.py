"""
Detector model.

Represents individual vehicle/pedestrian detectors on approaches.
"""

from typing import Optional
from uuid import UUID

from sqlalchemy import ForeignKey, Index, Integer, SmallInteger, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, tsigma_schema


class Detector(Base, TimestampMixin):
    """
    Vehicle or pedestrian detector configuration.

    Represents a single detector (loop, video, radar, etc.) on an approach.
    Includes configuration for detection zones, speed filtering, and timing adjustments.
    """

    __tablename__ = "detector"

    detector_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    approach_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("approach.approach_id"),
        nullable=False,
    )
    detector_channel: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    distance_from_stop_bar: Mapped[Optional[int]] = mapped_column(Integer)
    min_speed_filter: Mapped[Optional[int]] = mapped_column(SmallInteger)
    decision_point: Mapped[Optional[int]] = mapped_column(Integer)
    movement_delay: Mapped[Optional[int]] = mapped_column(SmallInteger)
    lane_number: Mapped[Optional[int]] = mapped_column(SmallInteger)
    lane_type_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("lane_type.lane_type_id"),
    )
    movement_type_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("movement_type.movement_type_id"),
    )
    detection_hardware_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("detection_hardware.detection_hardware_id"),
    )
    lat_lon_distance: Mapped[Optional[int]] = mapped_column(Integer)

    __table_args__ = (
        Index("idx_detector_approach", "approach_id"),
        {"schema": tsigma_schema("config")},
    )
