"""
Shared auth utilities.

Common helpers used across multiple authentication providers.
"""

from fastapi import Response

from tsigma.config import settings


def set_auth_cookie(response: Response, session_id: str) -> None:
    """
    Set the authentication session cookie on a response.

    Args:
        response: FastAPI Response object.
        session_id: Session identifier to store in the cookie.
    """
    response.set_cookie(
        key=settings.auth_cookie_name,
        value=session_id,
        httponly=True,
        secure=settings.auth_cookie_secure,
        samesite="strict",
        max_age=settings.auth_session_ttl_minutes * 60,
    )
