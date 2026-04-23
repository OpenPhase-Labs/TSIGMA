"""
Polling checkpoint model for persistent ingestion state tracking.

Tracks last successful poll per signal per method so TSIGMA:
- Never re-downloads files it has already ingested
- Survives service restarts without data loss or reprocessing
- Allows multiple independent consumers (no destructive reads)
- Enables crash recovery from the exact point of interruption

ATSPM 4.x deletes files from the controller after FTP download (destructive).
ATSPM 5.x excludes newest file by modification time (in-memory only).
TSIGMA uses this persistent checkpoint — non-destructive, restartable.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, tsigma_schema


class PollingCheckpoint(Base, TimestampMixin):
    """
    Persistent polling state per signal per ingestion method.

    Composite PK (signal_id, method) allows the same signal to be
    polled by different methods (e.g., FTP primary, HTTP fallback)
    with independent checkpoints.
    """

    __tablename__ = "polling_checkpoint"

    # Which signal and which ingestion method
    signal_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("signal.signal_id", ondelete="CASCADE"),
        primary_key=True,
    )
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
