"""
Plugin lifecycle audit-trail model.

Records every lifecycle event the plugin supervisor observes - launch,
handshake failure, health transition, restart, give-up, shutdown - so an
operator can reconstruct what a plugin did without reading a log file that
has already rotated. ADR-0019's Confirmation requires the table.

Event-log flavour (app-inserted, one row per event), mirroring
``SystemSettingAudit`` and ``AuthAuditLog`` rather than the trigger-populated
config-change tables in ``audit.py``.
"""

from datetime import datetime

from sqlalchemy import BigInteger, Index, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class PluginAudit(Base):
    """
    Append-only audit row for one plugin lifecycle event.

    ``event_type`` carries a ``PluginAuditEvent`` value and ``process_model`` a
    ``ProcessModel`` value; both are stored as text so an operator query is a
    string comparison and neither is pinned to a database enum that a new
    process model would have to migrate.
    """

    __tablename__ = "plugin_audit"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    plugin_name: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    process_model: Mapped[str] = mapped_column(Text, nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    changed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_plugin_audit_plugin", "plugin_name", "changed_at",
              postgresql_ops={"changed_at": "DESC"}),
        {"schema": tsigma_schema("config")},
    )
