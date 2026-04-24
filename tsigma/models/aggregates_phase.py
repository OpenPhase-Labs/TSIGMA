"""
Phase-scoped aggregate tables for TSIGMA analytics.

Companion module to ``tsigma.models.aggregates`` for the second batch of
aggregations (approach speed, phase cycle, left-turn gap, pedestrian,
priority, yellow/red activation).  Kept separate so no single model file
crosses the 1000-line hard cap.

Population follows the same dual-path rule as the first batch:
  - **PostgreSQL + TimescaleDB**: Continuous aggregates (initial migration).
  - **All other databases**: APScheduler jobs in
    ``tsigma.scheduler.jobs.aggregate_phase``.

All tables use the composite PK ``(signal_id, phase, bin_start)`` except
``ApproachSpeed15Min`` which is approach-scoped rather than phase-scoped.
"""

from datetime import datetime

from sqlalchemy import Float, Index, Integer, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class ApproachSpeed15Min(Base):
    """
    15-minute approach-speed percentile bins.

    Populated from ``roadside_event`` rows with ``event_type = SPEED``
    (1) joined through ``roadside_sensor_lane`` to an approach.  Speed
    samples are per-vehicle ``mph`` readings emitted by roadside radar
    / LiDAR / video sensors (Wavetronix, Iteris, FLIR, Houston Radar,
    Quanergy, etc.) — the cabinet controller does not emit an mph
    value for any Indiana Hi-Res event code TSIGMA currently decodes,
    so controller-only deployments leave this table empty.

    See ``tsigma.scheduler.jobs.aggregate_phase.agg_approach_speed``
    for the 15-minute refresh job that populates this table.
    """

    __tablename__ = "approach_speed_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    approach_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    p15: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    p50: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    p85: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_as15_signal_bin", "signal_id", "bin_start"),
        Index("idx_as15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class PhaseCycle15Min(Base):
    """
    15-minute phase green/yellow/red durations and cycle counts.

    Sum of green (event 1), yellow (event 8), red (event 9) interval
    durations per 15-minute bin, with the number of green starts as a
    cycle count.
    """

    __tablename__ = "phase_cycle_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    green_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    yellow_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    red_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cycle_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_pc15_signal_bin", "signal_id", "bin_start"),
        Index("idx_pc15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class PhaseLeftTurnGap15Min(Base):
    """
    15-minute left-turn gap distribution per phase.

    Eleven gap-duration bins (0-1s through 10+s) derived from consecutive
    detector-ON events on left-turn channels during the phase green
    interval.
    """

    __tablename__ = "phase_left_turn_gap_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    bin_1s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_2s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_3s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_4s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_5s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_6s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_7s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_8s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_9s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_10s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bin_10plus: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_pltg15_signal_bin", "signal_id", "bin_start"),
        Index("idx_pltg15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class PhasePedestrian15Min(Base):
    """
    15-minute pedestrian call/walk counts and ped delay accumulator.

    ``ped_delay_sum_seconds`` and ``ped_delay_count`` store the sum and
    count of ped-delay samples so consumers can derive mean delay without
    storing per-sample data.
    """

    __tablename__ = "phase_pedestrian_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    ped_walk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ped_call_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ped_delay_sum_seconds: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0,
    )
    ped_delay_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_pp15_signal_bin", "signal_id", "bin_start"),
        Index("idx_pp15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class Priority15Min(Base):
    """
    15-minute transit signal priority counts per phase.

    Derived from TSP events (112=check-in, 113=early-green, 114=extended-
    green, 115=check-out).
    """

    __tablename__ = "priority_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    early_green_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    extended_green_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    check_in_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    check_out_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_pr15_signal_bin", "signal_id", "bin_start"),
        Index("idx_pr15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )


class YellowRedActivation15Min(Base):
    """
    15-minute yellow/red detector-activation counts per phase.

    ``yellow_activation_count`` / ``red_activation_count`` count detector
    ON events during the corresponding interval.  ``red_duration_sum_seconds``
    accumulates the total red-interval length (event 9 to event 1) so that
    an activation rate per red-second can be derived downstream.
    """

    __tablename__ = "yellow_red_activation_15min"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    phase: Mapped[int] = mapped_column(Integer, primary_key=True)
    bin_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    yellow_activation_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0,
    )
    red_activation_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0,
    )
    red_duration_sum_seconds: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0,
    )

    __table_args__ = (
        Index("idx_yra15_signal_bin", "signal_id", "bin_start"),
        Index("idx_yra15_bin", "bin_start"),
        {"schema": tsigma_schema("aggregation")},
    )
