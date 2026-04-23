"""
Authentication audit logging.

Writes authentication events (login, logout, failure) to the
auth_audit_log table. Called from auth providers and the logout route.
"""

import logging
from uuid import UUID

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.audit import AuthAuditLog

logger = logging.getLogger(__name__)


async def log_auth_event(
    session: AsyncSession,
    event_type: str,
    username: str,
    request: Request,
    user_id: UUID | None = None,
) -> None:
    """
    Write an authentication audit record.

    Args:
        session: Database session (must be flushed/committed by caller).
        event_type: One of "login", "logout", "login_failed", "lockout".
        username: Username involved (even for failed attempts).
        request: FastAPI request (for IP and user agent).
        user_id: Resolved user ID (None for failed attempts).
    """
    entry = AuthAuditLog(
        event_type=event_type,
        user_id=user_id,
        username=username,
        ip_address=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )
    session.add(entry)
    await session.flush()
    logger.info(
        "Auth audit: %s user=%s ip=%s",
        event_type,
        username,
        request.client.host if request.client else "unknown",
    )
