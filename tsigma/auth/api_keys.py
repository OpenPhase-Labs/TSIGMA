"""
API key service.

Handles generation, validation, revocation, and listing of API keys.
Keys use a "tsgm_" prefix for easy identification. The plaintext key
is returned exactly once at creation; only the bcrypt hash is persisted.
"""

import logging
import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

import bcrypt
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.auth.models import ApiKey, AuthUser
from tsigma.auth.sessions import SessionData

logger = logging.getLogger(__name__)

_KEY_PREFIX = "tsgm_"
_KEY_BYTES = 32  # 256-bit random token


def _make_plaintext_key() -> str:
    """Generate a plaintext API key with the tsgm_ prefix."""
    token = secrets.token_urlsafe(_KEY_BYTES)
    return f"{_KEY_PREFIX}{token}"


def _hash_key(plaintext: str) -> str:
    """Bcrypt-hash a plaintext API key."""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(plaintext.encode("utf-8"), salt).decode("utf-8")


def _verify_key(plaintext: str, hashed: str) -> bool:
    """Verify a plaintext key against its bcrypt hash."""
    return bcrypt.checkpw(plaintext.encode("utf-8"), hashed.encode("utf-8"))


async def _lookup_username(user_id: UUID, session: AsyncSession) -> str | None:
    """Look up a username by user_id."""
    result = await session.execute(
        select(AuthUser.username).where(AuthUser.id == user_id)
    )
    return result.scalar_one_or_none()


async def generate_api_key(
    user_id: UUID,
    name: str,
    role: str,
    session: AsyncSession,
    expires_at: datetime | None = None,
) -> tuple[UUID, str]:
    """
    Create a new API key for a user.

    Args:
        user_id: Owner's user ID.
        name: Human-readable label for the key.
        role: Role to assign (inherited from user at creation).
        session: Database session.
        expires_at: Optional expiration datetime.

    Returns:
        Tuple of (key_id, plaintext_key). The plaintext is returned
        exactly once and is never stored.
    """
    from uuid import uuid4

    plaintext = _make_plaintext_key()
    key_hash = _hash_key(plaintext)
    key_id = uuid4()

    api_key = ApiKey(
        id=key_id,
        user_id=user_id,
        name=name,
        key_hash=key_hash,
        key_prefix=plaintext[:12],
        role=role,
        expires_at=expires_at,
    )
    session.add(api_key)
    await session.flush()

    return key_id, plaintext


async def validate_api_key(
    plaintext: str,
    session: AsyncSession,
) -> SessionData | None:
    """
    Validate a plaintext API key and return session-equivalent data.

    Looks up candidate keys by prefix, then bcrypt-verifies against
    the stored hash. Rejects expired and revoked keys.

    Args:
        plaintext: The full plaintext API key.
        session: Database session.

    Returns:
        SessionData if valid, None otherwise.
    """
    if not plaintext or len(plaintext) < 12:
        return None

    prefix = plaintext[:12]
    now = datetime.now(timezone.utc)

    # Find candidate keys by prefix (typically 1, but bcrypt-verify to be sure)
    result = await session.execute(
        select(ApiKey).where(ApiKey.key_prefix == prefix)
    )
    candidates = result.scalars().all()

    for key in candidates:
        # Skip revoked keys
        if key.revoked_at is not None:
            continue

        # Skip expired keys
        if key.expires_at is not None and key.expires_at <= now:
            continue

        # Verify bcrypt hash
        if _verify_key(plaintext, key.key_hash):
            # Update last_used_at
            await session.execute(
                update(ApiKey)
                .where(ApiKey.id == key.id)
                .values(last_used_at=now)
            )

            # Look up username
            username = await _lookup_username(key.user_id, session)
            if username is None:
                logger.warning("API key %s references missing user %s", key.id, key.user_id)
                return None

            return SessionData(
                user_id=key.user_id,
                username=username,
                role=key.role.value if hasattr(key.role, "value") else key.role,
                created_at=key.created_at,
                expires_at=key.expires_at or now + timedelta(hours=8),
            )

    return None


async def revoke_api_key(
    key_id: UUID,
    session: AsyncSession,
) -> bool:
    """
    Revoke an API key by setting revoked_at.

    Args:
        key_id: The API key's UUID.
        session: Database session.

    Returns:
        True if a key was found and revoked, False if not found.
    """
    now = datetime.now(timezone.utc)
    result = await session.execute(
        update(ApiKey)
        .where(ApiKey.id == key_id, ApiKey.revoked_at.is_(None))
        .values(revoked_at=now)
    )
    return result.rowcount > 0


async def list_user_keys(
    user_id: UUID,
    session: AsyncSession,
) -> list[dict]:
    """
    List all API keys for a user (no hashes or plaintext).

    Args:
        user_id: The user's UUID.
        session: Database session.

    Returns:
        List of dicts with key metadata.
    """
    result = await session.execute(
        select(ApiKey).where(ApiKey.user_id == user_id)
    )
    keys = result.scalars().all()

    return [
        {
            "id": str(k.id),
            "name": k.name,
            "key_prefix": k.key_prefix,
            "role": k.role.value if hasattr(k.role, "value") else k.role,
            "created_at": k.created_at.isoformat() if k.created_at else None,
            "expires_at": k.expires_at.isoformat() if k.expires_at else None,
            "revoked_at": k.revoked_at.isoformat() if k.revoked_at else None,
            "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None,
        }
        for k in keys
    ]
