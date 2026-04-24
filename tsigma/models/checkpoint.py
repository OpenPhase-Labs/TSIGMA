"""
Polling checkpoint model for persistent ingestion state tracking.

Tracks last successful poll per device per method so TSIGMA:
- Never re-downloads files it has already ingested
- Survives service restarts without data loss or reprocessing
- Allows multiple independent consumers (no destructive reads)
- Enables crash recovery from the exact point of interruption

Device-polymorphic: a device is either a cabinet controller
(``device_type='controller'``, ``device_id`` = ``Signal.signal_id``) or
a roadside sensor (``device_type='sensor'``, ``device_id`` = stringified
``RoadsideSensor.sensor_id``).  No SQL-level FK is enforced on
``device_id`` because the target table varies with ``device_type``;
referential integrity is upheld at the application layer by the
``DeviceSource`` that emitted the checkpoint write.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, tsigma_schema

DEVICE_TYPE_CONTROLLER = "controller"
DEVICE_TYPE_SENSOR = "sensor"


class PollingCheckpoint(Base, TimestampMixin):
    """
    Persistent polling state per device per ingestion method.

    Composite PK ``(device_type, device_id, method)`` lets the same
    device be polled by multiple methods (e.g. FTP primary, HTTP
    fallback) with independent checkpoints, and keeps controller and
    sensor checkpoints cleanly namespaced in a single table.
    """

    __tablename__ = "polling_checkpoint"

    # Device-polymorphic identity.  No FK: the target table varies with
    # device_type (see module docstring).
    device_type: Mapped[str] = mapped_column(Text, primary_key=True)
    device_id: Mapped[str] = mapped_column(Text, primary_key=True)
    method: Mapped[str] = mapped_column(Text, primary_key=True)

    # File-based checkpoint (FTP/SFTP polling)
    last_filename: Mapped[Optional[str]] = mapped_column(Text)
    last_file_mtime: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
    )
    files_hash: Mapped[Optional[str]] = mapped_column(Text)

    # Event-based checkpoint (HTTP polling)
    last_event_timestamp: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
    )

    # Poll cycle metadata
    last_successful_poll: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
    )
    events_ingested: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0",
    )
    files_ingested: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0",
    )

    # Silent signal detection (zero events for N consecutive cycles)
    consecutive_silent_cycles: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0",
    )

    # Error tracking (for backoff and alerting)
    consecutive_errors: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0",
    )
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    last_error_time: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
    )

    __table_args__ = (
        Index(
            "idx_checkpoint_last_poll",
            "method",
            "last_successful_poll",
        ),
        Index(
            "idx_checkpoint_errors",
            "consecutive_errors",
            postgresql_where=text("consecutive_errors > 0"),
        ),
        {"schema": tsigma_schema("events")},
    )
