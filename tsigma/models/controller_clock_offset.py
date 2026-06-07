"""
Raw per-ingest controller clock-offset records (ASC/3 integrity program, Inc-4).

A ``ControllerClockOffset`` row captures the clock-offset observed for a
single ingest of a controller's event stream, for trending over time. The
signed ``offset_seconds`` is computed as the newest event timestamp minus the
server receive time, so a positive value means the controller clock is ahead
of the server and a negative value means it is behind. The measurement is
controller-wide (per signal, not per detector or phase) and carries its own
``observed_at`` timestamp (no TimestampMixin).
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import Float, Index, Integer, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, schema_fk, tsigma_schema


class ControllerClockOffset(Base):
    """
    A single raw controller clock-offset measurement for trending.

    The signed ``offset_seconds`` is the newest event timestamp minus the
    server receive time for one ingest of the controller's event stream, so a
    positive value means the controller clock leads the server. The
    measurement is controller-wide (keyed by ``signal_id``); ``event_count``
    records how many events backed the measurement, and ``observed_at`` marks
    when the offset was computed.
    """

    __tablename__ = "controller_clock_offset"

    offset_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    signal_id: Mapped[str] = mapped_column(
        Text,
        schema_fk("config", "signal.signal_id"),
        nullable=False,
    )
    offset_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    event_count: Mapped[Optional[int]] = mapped_column(Integer)
    observed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_clock_offset_signal_observed", "signal_id", "observed_at"),
        {"schema": tsigma_schema("config")},
    )
