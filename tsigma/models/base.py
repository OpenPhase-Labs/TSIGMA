"""
Base model for all TSIGMA database models.

Provides common fields and functionality for all models.

Schema resolution:
  PostgreSQL, MS-SQL, Oracle → four schemas (config, events, aggregation, identity)
  MySQL → single database (no schema prefix)

Models set their schema via `tsigma_schema()` in `__table_args__`.
"""

from datetime import datetime

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def tsigma_schema(logical_schema: str) -> str | None:
    """
    Resolve a logical schema name for use in model __table_args__.

    Reads db_type from settings at import time. Returns None for MySQL
    (all tables in default database), schema name for others.

    Args:
        logical_schema: One of "config", "events", "aggregation", "identity".

    Returns:
        Schema name string, or None for MySQL.
    """
    from ..config import settings
    if settings.db_type == "mysql":
        return None
    return logical_schema


class Base(DeclarativeBase):
    """Base class for all TSIGMA models."""

    pass


class TimestampMixin:
    """
    Mixin for models that need created_at and updated_at timestamps.

    Automatically managed by database triggers (not application code).
    """

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
