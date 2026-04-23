"""
Approach model.

Represents a directional approach to an intersection (e.g., Northbound approach).
"""

from typing import Optional
from uuid import UUID

from sqlalchemy import Boolean, ForeignKey, Index, SmallInteger, Text, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, tsigma_schema


class Approach(Base, TimestampMixin):
    """
    Directional approach to an intersection.

    Defines the configuration for one approach (e.g., Northbound) including
    protected/permissive phases, speed limit, and pedestrian configuration.
    """

    __tablename__ = "approach"

    approach_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    signal_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("signal.signal_id"),
        nullable=False,
    )
    direction_type_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("direction_type.direction_type_id"),
        nullable=False,
    )
    description: Mapped[Optional[str]] = mapped_column(Text)
    mph: Mapped[Optional[int]] = mapped_column(SmallInteger)
    protected_phase_number: Mapped[Optional[int]] = mapped_column(SmallInteger)
    is_protected_phase_overlap: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default="false",
    )
    permissive_phase_number: Mapped[Optional[int]] = mapped_column(SmallInteger)
    is_permissive_phase_overlap: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default="false",
    )
    ped_phase_number: Mapped[Optional[int]] = mapped_column(SmallInteger)

    __table_args__ = (
        Index("idx_approach_signal", "signal_id"),
        {"schema": tsigma_schema("config")},
    )
