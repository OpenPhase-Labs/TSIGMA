"""
Audit trail models for configuration change tracking.

Approach and detector audit tables mirror the signal_audit pattern:
PostgreSQL triggers capture INSERT/UPDATE/DELETE operations as
timestamped JSONB snapshots. The config_resolver module queries
these tables to reconstruct configuration at any historical date.

See also: SignalAudit in signal.py (same pattern for signal table).
See also: docs/dev/AUDITING.md for the full audit system design.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import BigInteger, Index, Text, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class ApproachAudit(Base):
    """
    Approach configuration change history.

    Populated by database trigger on the approach table.
    Stores JSONB snapshots of old and new values for all changes.
    """

    __tablename__ = "approach_audit"

    audit_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    approach_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    signal_id: Mapped[str] = mapped_column(Text, nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(),
    )
    changed_by: Mapped[Optional[str]] = mapped_column(Text)
    operation: Mapped[str] = mapped_column(Text, nullable=False)
    old_values: Mapped[Optional[dict]] = mapped_column(JSONB)
    new_values: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (
        Index("idx_approach_audit_approach", "approach_id", "changed_at",
              postgresql_ops={"changed_at": "DESC"}),
        Index("idx_approach_audit_signal", "signal_id", "changed_at",
              postgresql_ops={"changed_at": "DESC"}),
        Index("idx_approach_audit_time", "changed_at",
              postgresql_ops={"changed_at": "DESC"}),
        {"schema": tsigma_schema("config")},
    )


class DetectorAudit(Base):
    """
    Detector configuration change history.

    Populated by database trigger on the detector table.
    Stores JSONB snapshots of old and new values for all changes.
    """

    __tablename__ = "detector_audit"

    audit_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    detector_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    approach_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(),
    )
    changed_by: Mapped[Optional[str]] = mapped_column(Text)
    operation: Mapped[str] = mapped_column(Text, nullable=False)
    old_values: Mapped[Optional[dict]] = mapped_column(JSONB)
    new_values: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (
        Index("idx_detector_audit_detector", "detector_id", "changed_at",
              postgresql_ops={"changed_at": "DESC"}),
        Index("idx_detector_audit_approach", "approach_id", "changed_at",
              postgresql_ops={"changed_at": "DESC"}),
        Index("idx_detector_audit_time", "changed_at",
              postgresql_ops={"changed_at": "DESC"}),
        {"schema": tsigma_schema("config")},
    )


class AuthAuditLog(Base):
    """
    Authentication event log.

    Tracks login, logout, failed attempts, and password changes.
    Separate from configuration auditing — this is identity/access auditing.
    """

    __tablename__ = "auth_audit_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[Optional[UUID]] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    ip_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_auth_audit_user", "user_id", "timestamp",
              postgresql_ops={"timestamp": "DESC"}),
        Index("idx_auth_audit_type", "event_type", "timestamp",
              postgresql_ops={"timestamp": "DESC"}),
        Index("idx_auth_audit_time", "timestamp",
              postgresql_ops={"timestamp": "DESC"}),
        {"schema": tsigma_schema("identity")},
    )
