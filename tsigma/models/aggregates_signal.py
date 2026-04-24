"""
Signal-scoped aggregate tables for TSIGMA analytics.

Companion module to ``tsigma.models.aggregates`` / ``aggregates_phase``.
Holds aggregates whose PK is not phase-scoped:

  - ``SignalEventCount15Min``: per-signal overall event volume.
  - ``Preemption15Min``: per-signal, per-preempt-channel counts and delay.

Same population rules: TimescaleDB continuous aggregates for PostgreSQL,
APScheduler jobs for the other dialects (see
``tsigma.scheduler.jobs.aggregate_signal``).
"""

from datetime import datetime

from sqlalchemy import Float, Index, Integer, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class Preemption15Min(Base):
    """
    15-minute preemption activity per signal/preempt-channel.

    Source events (Indiana Hi-Res):
      - 102 = Preempt Call Input On  (request)
      - 105 = Preempt Entry Started  (service)

    ``mean_delay_seconds`` is an approximation computed per bin (mean time
    between the earliest 102 and the earliest 105 within the bin for a
    given preempt channel).  Exact pair-matching across bin boundaries is
    delegated to downstream report plugins.
    """

    __tablename__ = "preemption_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    preempt_channel: Mapped[int] = mapped_column(Integer, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    service_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mean_delay_seconds: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0,
    )

    __table_args__ = (
        Index("idx_pe15_signal_bin", "signal_id", "bin_start"),
        Index("idx_pe15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class SignalEventCount15Min(Base):
    """
    15-minute total event count per signal.

    Coarse heartbeat metric — the number of rows written to
    ``controller_event_log`` per signal per 15-minute bucket.  Feeds
    signal-health dashboards and silent-signal detection.
    """

    __tablename__ = "signal_event_count_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_sec15_signal_bin", "signal_id", "bin_start"),
        Index("idx_sec15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )
