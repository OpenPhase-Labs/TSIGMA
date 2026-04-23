"""
Signal timing plan model.

Stores structured timing plan parameters captured from controller event
codes (Indiana 131-149).  The controller is the source of truth — TSIGMA
never authors plans, it records what the controller reported.

Each row represents a single plan activation period: from the moment
event code 131 (CoordPatternChange) fires until the next 131 event
for the same signal.  Cycle length, offset, and per-phase splits are
stored alongside the plan boundary so reports can do before/after
retiming comparisons without re-scanning the raw event log.

Splits are stored as JSONB: ``{"2": 35, "4": 25, "6": 40, "8": 20}``
(keys are phase numbers as strings, values are split seconds).  JSONB
is natively queryable on PostgreSQL and supported via JSON functions
on MSSQL, Oracle, and MySQL.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import ForeignKey, Index, SmallInteger, Text, text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class SignalPlan(Base):
    """
    Timing plan activation record.

    Populated by a background job that scans controller_event_log for
    event codes 131 (plan change), 132 (cycle length), 133 (offset),
    and 134-149 (split changes).  One row per plan activation period.

    effective_from = timestamp of event code 131 that started this plan.
    effective_to   = timestamp of the next 131 event (NULL if still active).
    """

    __tablename__ = "signal_plan"

    signal_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("signal.signal_id"),
        primary_key=True,
    )
    effective_from: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        primary_key=True,
    )
    effective_to: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
    )
    plan_number: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    cycle_length: Mapped[Optional[int]] = mapped_column(SmallInteger)
    offset: Mapped[Optional[int]] = mapped_column(SmallInteger)
    splits: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (
        Index("idx_signal_plan_signal", "signal_id", "effective_from",
              postgresql_ops={"effective_from": "DESC"}),
        Index("idx_signal_plan_active", "signal_id",
              postgresql_where=text("effective_to IS NULL")),
        Index("idx_signal_plan_number", "signal_id", "plan_number"),
        {"schema": tsigma_schema("config")},
    )
