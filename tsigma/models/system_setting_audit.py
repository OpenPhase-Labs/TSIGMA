"""
System settings audit-trail model.

Records every successful runtime-settings write so the history of who
changed which key, when, from what to what, and why is preserved. The
``system_setting.updated_by`` column captures only the last writer; this
table is the append-only history.
"""

from datetime import datetime

from sqlalchemy import BigInteger, Index, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class SystemSettingAudit(Base):
    """
    Append-only audit row for a runtime system-setting change.

    One row is inserted by ``PUT /api/v1/settings/{key}`` for every
    successful update. The row carries the prior value (or NULL for
    first-time writes), the new value, the user who made the change,
    and an optional free-text reason supplied by the operator.
    """

    __tablename__ = "system_setting_audit"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    old_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_value: Mapped[str] = mapped_column(Text, nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    changed_by: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_system_setting_audit_key", "key", "changed_at",
              postgresql_ops={"changed_at": "DESC"}),
        {"schema": tsigma_schema("identity")},
    )
