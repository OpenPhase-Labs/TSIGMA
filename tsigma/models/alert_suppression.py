"""
Alert suppression rules for watchdog data-quality checks.

An ``AlertSuppression`` row tells the watchdog to log-but-not-deliver any
alert matching ``(signal_id, check_name)`` until ``expires_at``. A NULL
``signal_id`` applies the suppression to every signal for that check; a
NULL ``expires_at`` makes the rule permanent (until explicitly removed).

The watchdog helper ``_is_suppressed`` queries this table before emitting
each notification.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import Index, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class AlertSuppression(Base):
    """
    Suppression rule for a watchdog data-quality check.

    ``check_name`` is one of the well-known identifiers used by the
    watchdog checks — e.g. ``"silent_signal"``, ``"stuck_detector"``,
    ``"low_event_count"``, ``"missing_data_window"``, ``"stuck_ped"``,
    ``"phase_termination_anomaly"``, ``"low_hit_count"``.
    """

    __tablename__ = "alert_suppression"

    suppression_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    signal_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    check_name: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        Index(
            "idx_alert_suppression_lookup",
            "check_name", "signal_id", "expires_at",
        ),
        {"schema": tsigma_schema("config")},
    )
