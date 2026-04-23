"""
Authentication API endpoints.

Shared routes (logout, me, provider info) that work with any auth provider.
Provider-specific login routes are mounted from the active provider's router.
"""

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.auth.api_keys import generate_api_key, list_user_keys, revoke_api_key
from tsigma.auth.audit import log_auth_event
from tsigma.auth.dependencies import get_current_user, get_session_store
from tsigma.auth.sessions import BaseSessionStore, SessionData
from tsigma.config import settings
from tsigma.dependencies import get_session

router = APIRouter()


@router.get("/csrf")
async def get_csrf_token(
    store: BaseSessionStore = Depends(get_session_store),
) -> dict:
    """
    Generate a CSRF nonce for the login form.

    The token is stored in the session store (Valkey in production)
    with a 5-minute TTL. It must be submitted with the login request
    and is consumed on use (one-time).

    Cross-server safe: token is stored in Valkey, not in-process memory.

    Returns:
        Dict with csrf_token string.
    """
    token = await store.create_csrf()
    return {"csrf_token": token}


@router.get("/provider")
async def provider_info() -> dict:
    """
    Return active authentication provider info.

    Used by frontends to discover which login flow to present.

    Returns:
        Dict with auth_mode string.
    """
    return {"auth_mode": settings.auth_mode}


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    store: BaseSessionStore = Depends(get_session_store),
    db_session: AsyncSession = Depends(get_session),
) -> dict:
    """
    End session and clear cookie.

    Args:
        request: FastAPI request (for reading cookie).
        response: FastAPI response (for clearing cookie).
        store: Session store (injected).
        db_session: Database session for audit logging.

    Returns:
        Dict with status message.
    """
    session_id = request.cookies.get(settings.auth_cookie_name)
    if session_id:
        session_data = await store.get(session_id)
        if session_data:
            await log_auth_event(
                db_session, "logout", session_data.username, request,
                user_id=session_data.user_id,
            )
        await store.delete(session_id)

    response.delete_cookie(
        key=settings.auth_cookie_name,
        httponly=True,
        secure=settings.auth_cookie_secure,
        samesite="strict",
    )

    return {"detail": "Logged out"}


@router.get("/me")
async def me(
    current_user: SessionData = Depends(get_current_user),
) -> dict:
    """
    Get current authenticated user info.

    Args:
        current_user: Current user session data (injected).

    Returns:
        Dict with user_id, username, and role.
    """
    return {
        "user_id": str(current_user.user_id),
        "username": current_user.username,
        "role": current_user.role,
    }


# ---------------------------------------------------------------------------
# API Key schemas
# ---------------------------------------------------------------------------


class ApiKeyCreateRequest(BaseModel):
    """Request body for creating an API key."""

    name: str = Field(..., min_length=1, description="Human-readable label")
    expires_at: datetime | None = Field(
        None, description="Optional expiration (ISO-8601 UTC)",
    )


class ApiKeyCreateResponse(BaseModel):
    """Response after creating an API key (includes plaintext once)."""

    key_id: str
    name: str
    plaintext_key: str


# ---------------------------------------------------------------------------
# API Key endpoints
# ---------------------------------------------------------------------------


@router.post("/api-keys", status_code=status.HTTP_201_CREATED)
async def create_api_key_endpoint(
    body: ApiKeyCreateRequest,
    current_user: SessionData = Depends(get_current_user),
    db_session: AsyncSession = Depends(get_session),
) -> ApiKeyCreateResponse:
    """
    Create a new API key for the current user.

    The plaintext key is returned exactly once in the response.
    Store it securely; it cannot be retrieved again.

    Args:
        body: Key name and optional expiration.
        current_user: Authenticated user (injected).
        db_session: Database session (injected).

    Returns:
        Key ID, name, and the one-time plaintext key.
    """
    key_id, plaintext = await generate_api_key(
        user_id=current_user.user_id,
        name=body.name,
        role=current_user.role,
        session=db_session,
        expires_at=body.expires_at,
    )
    return ApiKeyCreateResponse(
        key_id=str(key_id),
        name=body.name,
        plaintext_key=plaintext,
    )


@router.get("/api-keys")
async def list_api_keys_endpoint(
    current_user: SessionData = Depends(get_current_user),
    db_session: AsyncSession = Depends(get_session),
) -> list[dict]:
    """
    List all API keys for the current user.

    Returns metadata only -- no plaintext keys or hashes.

    Args:
        current_user: Authenticated user (injected).
        db_session: Database session (injected).

    Returns:
        List of key metadata dicts.
    """
    return await list_user_keys(current_user.user_id, db_session)


@router.delete("/api-keys/{key_id}")
async def revoke_api_key_endpoint(
    key_id: UUID,
    current_user: SessionData = Depends(get_current_user),
    db_session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Revoke an API key.

    Sets revoked_at without affecting the user's interactive session.

    Args:
        key_id: UUID of the key to revoke.
        current_user: Authenticated user (injected).
        db_session: Database session (injected).

    Returns:
        Confirmation dict.

    Raises:
        HTTPException: 404 if key not found or already revoked.
    """
    revoked = await revoke_api_key(key_id, db_session)
    if not revoked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Key not found or already revoked",
        )
    return {"detail": "Key revoked"}
