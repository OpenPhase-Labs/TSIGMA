"""
Authentication FastAPI dependencies.

Provides dependency injection for session store access,
current user retrieval, role-based access control, and
configurable access policies.
"""

import logging
from typing import Callable

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.auth.api_keys import validate_api_key
from tsigma.auth.sessions import BaseSessionStore, SessionData
from tsigma.config import settings
from tsigma.dependencies import get_session
from tsigma.settings_service import LOCKED_CATEGORIES, settings_cache

logger = logging.getLogger(__name__)


def get_session_store(request: Request) -> BaseSessionStore:
    """
    Get the session store from app state.

    Args:
        request: FastAPI request (provides access to app.state).

    Returns:
        The application's session store instance.

    Raises:
        RuntimeError: If session store has not been initialized.
    """
    store = getattr(request.app.state, "session_store", None)
    if store is None:
        raise RuntimeError("Session store not initialized")
    return store


def _extract_api_key(request: Request) -> str | None:
    """
    Extract an API key from request headers.

    Checks X-API-Key first, then Authorization: Bearer.

    Args:
        request: FastAPI request.

    Returns:
        The plaintext API key string, or None.
    """
    # Check X-API-Key header first
    api_key = request.headers.get("x-api-key")
    if api_key:
        return api_key

    # Check Authorization: Bearer header
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()
        if token:
            return token

    return None


async def _get_db_for_api_key() -> AsyncSession:
    """
    Get a database session for API key validation.

    Uses the same get_session dependency but called manually
    to avoid circular dependency issues.
    """
    from tsigma.database.db import get_db_facade

    facade = get_db_facade()
    session = facade._session_factory()
    return session


async def get_current_user_optional(
    request: Request,
    store: BaseSessionStore = Depends(get_session_store),
) -> SessionData | None:
    """
    Get the current user from API key headers or session cookie.

    Checks X-API-Key and Authorization: Bearer headers first.
    Falls back to session cookie if no API key is present.

    Args:
        request: FastAPI request (for reading headers/cookies).
        store: Session store (injected).

    Returns:
        SessionData if authenticated, None otherwise.
    """
    # Check API key headers first
    api_key_value = _extract_api_key(request)
    if api_key_value:
        try:
            db_session = await _get_db_for_api_key()
            try:
                result = await validate_api_key(api_key_value, db_session)
                return result
            finally:
                await db_session.close()
        except Exception:
            logger.debug("API key validation failed")
            return None

    # Fall back to session cookie
    session_id = request.cookies.get(settings.auth_cookie_name)
    if not session_id:
        return None
    return await store.get(session_id)


_NOT_AUTHENTICATED = "Not authenticated"


def get_current_user(
    user: SessionData | None = Depends(get_current_user_optional),
) -> SessionData:
    """
    Require an authenticated user.

    Args:
        user: Current user from session (injected).

    Returns:
        SessionData for the authenticated user.

    Raises:
        HTTPException: 401 if not authenticated.
    """
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=_NOT_AUTHENTICATED,
        )
    return user


def require_admin(
    user: SessionData = Depends(get_current_user),
) -> SessionData:
    """
    Require the current user to have admin role.

    Args:
        user: Current authenticated user (injected).

    Returns:
        SessionData for the admin user.

    Raises:
        HTTPException: 403 if user is not admin.
    """
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required",
        )
    return user


def require_access(category: str) -> Callable:
    """
    Factory that returns a FastAPI dependency enforcing the access policy
    for the given category.

    If the category's policy is "public", the request passes through
    without authentication. If "authenticated" (or if the category is
    locked), the user must be logged in.

    Usage::

        @router.get("/analytics/something")
        async def my_endpoint(
            user: SessionData | None = Depends(require_access("analytics")),
        ):
            ...

    Args:
        category: Access policy category (e.g. "analytics", "reports").

    Returns:
        A FastAPI-compatible async dependency function.
    """
    policy_key = f"access_policy.{category}"

    async def _dependency(
        user: SessionData | None = Depends(get_current_user_optional),
        db_session: AsyncSession = Depends(_get_db_session),
    ) -> SessionData | None:
        # Locked categories always require auth
        if category in LOCKED_CATEGORIES:
            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=_NOT_AUTHENTICATED,
                )
            return user

        # Look up the policy from the cached settings
        policy = await settings_cache.get(policy_key, db_session)

        # Default to authenticated if setting is missing or unrecognised
        if policy != "public":
            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=_NOT_AUTHENTICATED,
                )
        return user

    return _dependency


def _get_db_session(
    session: AsyncSession = Depends(get_session),
) -> AsyncSession:
    """Pass through the DB session so dependency overrides propagate."""
    return session
