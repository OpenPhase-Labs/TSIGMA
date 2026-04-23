"""
Pre-computed aggregate tables for TSIGMA analytics.

These tables store time-bucketed metrics derived from raw ControllerEventLog
data.  They are populated by one of two mechanisms depending on the database:

  - **PostgreSQL + TimescaleDB**: Continuous aggregates (automatic, incremental)
  - **All other databases**: APScheduler jobs that delete-and-reinsert a
    sliding window every N minutes (see scheduler/jobs/aggregate.py)

The API layer reads from these tables identically regardless of how they
are populated.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Float, Index, Integer, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class DetectorVolumeHourly(Base):
    """
    Hourly detector actuation counts per signal/channel.

    Populated from ControllerEventLog event codes 81 (OFF) and 82 (ON).
    """

    __tablename__ = "detector_volume_hourly"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    detector_channel: Mapped[int] = mapped_column(Integer, primary_key=True)
    hour_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    volume: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    activations: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_dvh_signal_hour", "signal_id", "hour_start"),
        Index("idx_dvh_hour", "hour_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class DetectorOccupancyHourly(Base):
    """
    Hourly detector occupancy percentage per signal/channel.

    Occupancy = (total ON-to-OFF duration) / (3600 seconds) * 100.
    """

    __tablename__ = "detector_occupancy_hourly"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    detector_channel: Mapped[int] = mapped_column(Integer, primary_key=True)
    hour_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    occupancy_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_on_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    activation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_doh_signal_hour", "signal_id", "hour_start"),
        Index("idx_doh_hour", "hour_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class SplitFailureHourly(Base):
    """
    Hourly split failure counts per signal/phase.

    A split failure occurs when detector occupancy remains high (>79%)
    at both green onset and red onset within the same cycle.
    """

    __tablename__ = "split_failure_hourly"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    hour_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    total_cycles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_cycles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_rate_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    __table_args__ = (
        Index("idx_sfh_signal_hour", "signal_id", "hour_start"),
        Index("idx_sfh_hour", "hour_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class ApproachDelay15Min(Base):
    """
    15-minute approach delay metrics per signal/phase.

    Delay = time from detector activation to phase green start.
    """

    __tablename__ = "approach_delay_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    avg_delay_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    max_delay_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_arrivals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_ad15_signal_bin", "signal_id", "bin_start"),
        Index("idx_ad15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class ArrivalOnRedHourly(Base):
    """
    Hourly arrivals-on-red percentage per signal/phase.

    Arrival on red = detector activation during red phase interval.
    Complement of arrivals-on-green.
    """

    __tablename__ = "arrival_on_red_hourly"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    hour_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    total_arrivals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    arrivals_on_red: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    arrivals_on_green: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    red_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    green_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    __table_args__ = (
        Index("idx_aor_signal_hour", "signal_id", "hour_start"),
        Index("idx_aor_hour", "hour_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class CoordinationQualityHourly(Base):
    """
    Hourly coordination quality metrics per signal.

    Measures cycle length consistency on the coordinated phase (phase 2).
    """

    __tablename__ = "coordination_quality_hourly"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    hour_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    total_cycles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cycles_within_tolerance: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quality_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_cycle_length_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_offset_error_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    __table_args__ = (
        Index("idx_cqh_signal_hour", "signal_id", "hour_start"),
        Index("idx_cqh_hour", "hour_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class PhaseTerminationHourly(Base):
    """
    Hourly phase termination breakdown per signal/phase.

    Counts of gap-out, max-out, and force-off termination events.
    """

    __tablename__ = "phase_termination_hourly"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    hour_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    total_cycles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    gap_outs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_outs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    force_offs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_pth_signal_hour", "signal_id", "hour_start"),
        Index("idx_pth_hour", "hour_start"),
        {"schema": tsigma_schema("aggregation")},
    )


# ---------------------------------------------------------------------------
# PCD Continuous Aggregate Views (TimescaleDB only)
#
# These models map to TimescaleDB continuous aggregate views, not regular
# tables. They are created via SQL in the initial migration when TimescaleDB
# is available. For non-TimescaleDB databases, these views don't exist and
# the PCD report falls back to querying raw events.
# ---------------------------------------------------------------------------


class CycleBoundary(Base):
    """
    Per-cycle phase timing boundaries.

    One row per signal/phase/cycle. Records green start, yellow start,
    red start, cycle end, and cycle duration. Feeds split monitor,
    phase termination analysis, and cycle length consistency checks.

    TimescaleDB continuous aggregate on controller_event_log.
    Non-TimescaleDB: populated by scheduler job.
    """

    __tablename__ = "cycle_boundary"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    green_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    yellow_start: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    red_start: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    cycle_end: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    green_duration_seconds: Mapped[Optional[float]] = mapped_column(Float)
    yellow_duration_seconds: Mapped[Optional[float]] = mapped_column(Float)
    red_duration_seconds: Mapped[Optional[float]] = mapped_column(Float)
    cycle_duration_seconds: Mapped[Optional[float]] = mapped_column(Float)
    termination_type: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        Index("idx_cb_signal_phase_green", "signal_id", "phase", "green_start",
              postgresql_ops={"green_start": "DESC"}),
        Index("idx_cb_green_start", "green_start",
              postgresql_ops={"green_start": "DESC"}),
        {"schema": tsigma_schema("aggregation")},
    )


class CycleDetectorArrival(Base):
    """
    Per-detector-activation within a cycle.

    One row per detector activation during a phase cycle. Records the
    arrival time, which phase state the detector fired during (green,
    yellow, red), and the time-in-cycle offset. This is what the PCD
    plot renders — each dot is one row.

    TimescaleDB continuous aggregate on controller_event_log.
    Non-TimescaleDB: populated by scheduler job.
    """

    __tablename__ = "cycle_detector_arrival"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    detector_channel: Mapped[int] = mapped_column(Integer, primary_key=True)
    arrival_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    green_start: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    time_in_cycle_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    phase_state: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        Index("idx_cda_signal_phase_arrival", "signal_id", "phase", "arrival_time",
              postgresql_ops={"arrival_time": "DESC"}),
        Index("idx_cda_green_start", "green_start",
              postgresql_ops={"green_start": "DESC"}),
        Index("idx_cda_arrival_time", "arrival_time",
              postgresql_ops={"arrival_time": "DESC"}),
        {"schema": tsigma_schema("aggregation")},
    )


class CycleSummary15Min(Base):
    """
    15-minute binned cycle and arrival summary.

    Aggregated from CycleBoundary and CycleDetectorArrival. One row
    per signal/phase/15-minute bin. Feeds dashboard widgets, trend
    charts, and corridor-level arrival-on-green metrics.

    TimescaleDB continuous aggregate on cycle_boundary + cycle_detector_arrival.
    Non-TimescaleDB: populated by scheduler job.
    """

    __tablename__ = "cycle_summary_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    total_cycles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_cycle_length_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_green_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_arrivals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    arrivals_on_green: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    arrivals_on_yellow: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    arrivals_on_red: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    arrival_on_green_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    __table_args__ = (
        Index("idx_cs15_signal_phase_bin", "signal_id", "phase", "bin_start",
              postgresql_ops={"bin_start": "DESC"}),
        Index("idx_cs15_bin", "bin_start",
              postgresql_ops={"bin_start": "DESC"}),
        {"schema": tsigma_schema("aggregation")},
    )
