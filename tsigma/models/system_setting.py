"""
System settings model.

Stores runtime-configurable settings in the database.
Settings are updatable without requiring a restart.
"""

from datetime import datetime

from sqlalchemy import String, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class SystemSetting(Base):
    """
    Runtime-configurable system setting.

    Stored in the database so settings can be updated via the admin UI
    without requiring an application restart. A lightweight in-memory
    cache with TTL ensures low-latency reads.
    """

    __tablename__ = "system_setting"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    editable: Mapped[bool] = mapped_column(
        nullable=False, default=True, server_default="true",
    )

    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    updated_by: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default=None,
    )

    __table_args__ = {"schema": tsigma_schema("config")}
