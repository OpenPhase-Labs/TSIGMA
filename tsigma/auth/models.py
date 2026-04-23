"""
Authentication database models.

AuthUser table for local username/password authentication.
"""

import enum
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import Boolean, Enum, ForeignKey, Index, Text, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from tsigma.models.base import Base, TimestampMixin, tsigma_schema


class UserRole(str, enum.Enum):
    """User roles for authorization."""

    ADMIN = "admin"
    VIEWER = "viewer"


class AuthUser(TimestampMixin, Base):
    """
    Local authentication user.

    Stores username, bcrypt password hash, and role for
    local auth mode. Uses UUID primary key.
    """

    __tablename__ = "auth_user"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    username: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[UserRole] = mapped_column(
        Enum(UserRole, name="user_role", create_constraint=True),
        nullable=False,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true",
    )
    external_id: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    external_provider: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)

    __table_args__ = (
        Index("idx_auth_user_username", "username", unique=True),
        Index(
            "idx_auth_user_external",
            "external_provider", "external_id",
            unique=True,
            postgresql_where=text("external_id IS NOT NULL"),
        ),
        {"schema": tsigma_schema("identity")},
    )


class ApiKey(TimestampMixin, Base):
    """
    API key for programmatic access.

    Keys are issued per-user and tied to a role. The plaintext key
    is returned exactly once at creation; only the bcrypt hash is stored.
    Keys can have optional expiration and can be revoked by an admin
    without affecting the user's interactive session.
    """

    __tablename__ = "api_key"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("auth_user.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    key_hash: Mapped[str] = mapped_column(Text, nullable=False)
    key_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[UserRole] = mapped_column(
        Enum(UserRole, name="user_role", create_constraint=False),
        nullable=False,
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True, default=None,
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True, default=None,
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True, default=None,
    )

    __table_args__ = (
        Index("idx_api_key_user_id", "user_id"),
        Index("idx_api_key_prefix", "key_prefix"),
        Index("idx_api_key_expires_at", "expires_at"),
        {"schema": tsigma_schema("identity")},
    )
