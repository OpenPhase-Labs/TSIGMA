"""
Signal (traffic intersection) models.

Includes Signal configuration and SignalAudit for change tracking.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from sqlalchemy import BigInteger, Boolean, Date, ForeignKey, Index, Integer, Text, func
from sqlalchemy.dialects.postgresql import INET, JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, tsigma_schema


class Signal(Base, TimestampMixin):
    """
    Traffic signal/intersection configuration.

    Primary entity representing a traffic signal controller and its configuration.
    Uses natural key (signal_id) instead of surrogate ID.
    """

    __tablename__ = "signal"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    primary_street: Mapped[str] = mapped_column(Text, nullable=False)
    secondary_street: Mapped[Optional[str]] = mapped_column(Text)
    latitude: Mapped[Optional[Decimal]] = mapped_column()
    longitude: Mapped[Optional[Decimal]] = mapped_column()
    jurisdiction_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("jurisdiction.jurisdiction_id"),
    )
    region_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("region.region_id"),
    )
    corridor_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("corridor.corridor_id"),
    )
    controller_type_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("controller_type.controller_type_id"),
    )
    ip_address: Mapped[Optional[str]] = mapped_column(INET)
    # First-class network port for FTP/HTTP/TCP/UDP/gRPC reachability.
    # Listener source-IP routing reads ip_address (B-tree indexed); polling
    # transports read host = ip_address with port from this column.
    port: Mapped[Optional[int]] = mapped_column(Integer)
    # First-class transport identifier (ftp, ftps, sftp, http, https, tcp,
    # udp, grpc, mqtt, nats).  Per-device protocol-specific routing
    # (remote_dir, topic, subject, instance, decoder) lives in
    # signal_metadata.collection JSONB.
    protocol: Mapped[Optional[str]] = mapped_column(Text)
    note: Mapped[Optional[str]] = mapped_column(Text)
    signal_metadata: Mapped[Optional[dict]] = mapped_column("metadata", JSONB)
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true",
    )
    start_date: Mapped[Optional[date]] = mapped_column(Date)

    __table_args__ = (
        Index("idx_signal_region", "region_id"),
        Index("idx_signal_corridor", "corridor_id"),
        Index("idx_signal_controller_type", "controller_type_id"),
        Index("idx_signal_metadata", "metadata", postgresql_using="gin"),
        # B-tree on ip_address for source-IP lookups by TCP/UDP listeners.
        Index(
            "idx_signal_ip_address", "ip_address",
            postgresql_where="ip_address IS NOT NULL",
        ),
        {"schema": tsigma_schema("config")},
    )


class SignalAudit(Base):
    """
    Signal configuration change history.

    Automatically populated by database trigger on signal table.
    Stores JSONB snapshots of old and new values for all changes.
    """

    __tablename__ = "signal_audit"

    audit_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    signal_id: Mapped[str] = mapped_column(Text, nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    changed_by: Mapped[Optional[str]] = mapped_column(Text)
    operation: Mapped[str] = mapped_column(Text, nullable=False)
    old_values: Mapped[Optional[dict]] = mapped_column(JSONB)
    new_values: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (
        Index(
            "idx_signal_audit_signal", "signal_id", "changed_at",
            postgresql_ops={"changed_at": "DESC"},
        ),
        Index(
            "idx_signal_audit_time", "changed_at",
            postgresql_ops={"changed_at": "DESC"},
        ),
        {"schema": tsigma_schema("config")},
    )
