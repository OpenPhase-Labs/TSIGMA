"""
Local username/password authentication provider.

Wraps the existing bcrypt-based login as an AuthProvider plugin.
This is the default auth_mode="local" provider.
"""

import logging
import threading
import time
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.auth.audit import log_auth_event
from tsigma.auth.dependencies import get_session_store
from tsigma.auth.models import AuthUser
from tsigma.auth.passwords import verify_password
from tsigma.auth.registry import AuthProviderRegistry, BaseAuthProvider
from tsigma.auth.schemas import LoginRequest
from tsigma.auth.sessions import BaseSessionStore
from tsigma.auth.utils import set_auth_cookie
from tsigma.dependencies import get_session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Login rate limiter — per-username failed-attempt tracking
#
# Protected by a lock for free-threaded Python (3.13+ no-GIL builds).
# On GIL builds the lock is essentially a no-op but keeps the code
# safe regardless of runtime configuration.
# ---------------------------------------------------------------------------
_MAX_ATTEMPTS = 5
_LOCKOUT_SECONDS = 300  # 5 minutes

_failed_attempts: dict[str, list[float]] = defaultdict(list)
_lock = threading.Lock()


def _is_locked_out(username: str) -> bool:
    """Check if a username is locked out due to too many failed attempts."""
    now = time.monotonic()
    cutoff = now - _LOCKOUT_SECONDS
    with _lock:
        attempts = _failed_attempts[username]
        _failed_attempts[username] = [t for t in attempts if t > cutoff]
        return len(_failed_attempts[username]) >= _MAX_ATTEMPTS


def _record_failure(username: str) -> None:
    """Record a failed login attempt."""
    with _lock:
        _failed_attempts[username].append(time.monotonic())


@AuthProviderRegistry.register("local")
class LocalAuthProvider(BaseAuthProvider):
    """Local username/password authentication provider."""

    name = "local"
    description = "Local bcrypt username/password authentication"

    async def initialize(self) -> None:
        """No initialization needed for local auth."""

    def get_router(self) -> APIRouter:
        """
        Return router with POST /login endpoint.

        Returns:
            APIRouter with local login route.
        """
        router = APIRouter()

        @router.post("/login")
        async def login(
            body: LoginRequest,
            request: Request,
            response: Response,
            session: AsyncSession = Depends(get_session),
            store: BaseSessionStore = Depends(get_session_store),
        ) -> dict:
            """
            Authenticate with username/password and create session.

            Args:
                body: Login credentials (username, password).
                response: FastAPI response (for setting cookies).
                session: Database session (injected).
                store: Session store (injected).

            Returns:
                Dict with username and role.

            Raises:
                HTTPException: 401 if credentials are invalid or user inactive.
            """
            # Validate CSRF nonce (one-time use, stored in Valkey)
            csrf_valid = await store.validate_csrf(body.csrf_token)
            if not csrf_valid:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Invalid or expired CSRF token",
                )

            if _is_locked_out(body.username):
                logger.warning(
                    "Login locked out for user '%s' — "
                    "too many failed attempts",
                    body.username,
                )
                await log_auth_event(
                    session, "lockout", body.username, request,
                )
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Too many failed attempts. Try again later.",
                )

            result = await session.execute(
                select(AuthUser).where(AuthUser.username == body.username)
            )
            user = result.scalar_one_or_none()

            if user is None or not verify_password(
                body.password, user.password_hash
            ):
                _record_failure(body.username)
                await log_auth_event(
                    session, "login_failed", body.username, request,
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid credentials",
                )

            if not user.is_active:
                await log_auth_event(
                    session, "login_failed", body.username, request,
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid credentials",
                )

            session_id = await store.create(
                user_id=user.id,
                username=user.username,
                role=user.role.value,
            )

            await log_auth_event(
                session, "login", user.username, request, user_id=user.id,
            )

            set_auth_cookie(response, session_id)

            return {"username": user.username, "role": user.role.value}

        return router
