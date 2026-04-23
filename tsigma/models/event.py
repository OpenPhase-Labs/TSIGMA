"""
Event models for ATSPM time-series data.

ControllerEventLog stores raw event data from traffic signal controllers.
2M+ events per day for large deployments (9,000 signals).
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Index, Integer, SmallInteger, Text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class ControllerEventLog(Base):
    """
    Raw event log data from traffic signal controllers.

    Uses Indiana Traffic Signal Hi-Resolution Data Logger Enumerations.
    Reference: https://docs.lib.purdue.edu/jtrpdata/4/

    TimescaleDB hypertable - partitioned by event_time, compressed after 7 days.
    Composite primary key (signal_id, event_time, event_code, event_param) for ORM.

    Event codes 0-255 per Indiana spec, but real-world data can exceed 32767.
    Using INTEGER for compatibility.
    """

    __tablename__ = "controller_event_log"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True,
    )
    event_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_param: Mapped[int] = mapped_column(Integer, primary_key=True)
    device_id: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=1, server_default="1",
    )
    validation_metadata: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True, default=None,
    )

    __table_args__ = (
        Index(
            "idx_cel_signal_time", "signal_id", "event_time",
            postgresql_ops={"event_time": "DESC"},
        ),
        Index(
            "idx_cel_event_time", "event_code", "event_time",
            postgresql_ops={"event_time": "DESC"},
        ),
        {"schema": tsigma_schema("events")},
    )
