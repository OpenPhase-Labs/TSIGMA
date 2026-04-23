"""
Server-side session store.

Manages session lifecycle with configurable TTL.
Sessions are stored with session IDs used as httponly cookie values.

Two implementations:
- InMemorySessionStore: single-process fallback (dev/testing)
- ValkeySessionStore: production, multi-process, survives restarts
"""

import json
import logging
import secrets
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

logger = logging.getLogger(__name__)


@dataclass
class SessionData:
    """Data stored for each active session."""

    user_id: UUID
    username: str
    role: str
    created_at: datetime
    expires_at: datetime

    @property
    def is_expired(self) -> bool:
        """Check if this session has expired."""
        return datetime.now(timezone.utc) >= self.expires_at

    def to_dict(self) -> dict:
        """Serialize to JSON-safe dict for Valkey storage."""
        return {
            "user_id": str(self.user_id),
            "username": self.username,
            "role": self.role,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SessionData":
        """Deserialize from dict."""
        return cls(
            user_id=UUID(data["user_id"]),
            username=data["username"],
            role=data["role"],
            created_at=datetime.fromisoformat(data["created_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]),
        )


class BaseSessionStore(ABC):
    """Abstract base class for session stores."""

    @abstractmethod
    async def create(self, user_id: UUID, username: str, role: str) -> str:
        """Create a new session and return its ID."""
        ...

    @abstractmethod
    async def get(self, session_id: str) -> SessionData | None:
        """Get session data by ID, or None if expired/missing."""
        ...

    @abstractmethod
    async def delete(self, session_id: str) -> None:
        """Delete a session by ID."""
        ...

    @abstractmethod
    async def cleanup(self) -> None:
        """Remove all expired sessions."""
        ...

    @abstractmethod
    async def create_csrf(self) -> str:
        """
        Generate a CSRF nonce and store it with a short TTL.

        Returns:
            The CSRF token string.
        """
        ...

    @abstractmethod
    async def validate_csrf(self, token: str) -> bool:
        """
        Validate and consume a CSRF nonce (one-time use).

        Args:
            token: The CSRF token to validate.

        Returns:
            True if valid and not expired, False otherwise.
        """
        ...


_CSRF_TTL_SECONDS = 300  # 5 minutes
CSRF_KEY_PREFIX = "tsigma:csrf:"


class InMemorySessionStore(BaseSessionStore):
    """
    Dict-based in-memory session store with TTL expiry.

    Suitable for single-process development/testing. For production,
    use ValkeySessionStore.
    """

    def __init__(self, ttl_minutes: int = 480) -> None:
        self._sessions: dict[str, SessionData] = {}
        self._csrf_tokens: dict[str, datetime] = {}
        self._ttl = timedelta(minutes=ttl_minutes)

    async def create(self, user_id: UUID, username: str, role: str) -> str:
        session_id = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        self._sessions[session_id] = SessionData(
            user_id=user_id,
            username=username,
            role=role,
            created_at=now,
            expires_at=now + self._ttl,
        )
        return session_id

    async def get(self, session_id: str) -> SessionData | None:
        data = self._sessions.get(session_id)
        if data is None:
            return None
        if data.is_expired:
            del self._sessions[session_id]
            return None
        return data

    async def delete(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    async def cleanup(self) -> None:
        expired = [
            sid for sid, data in self._sessions.items()
            if data.is_expired
        ]
        for sid in expired:
            del self._sessions[sid]
        # Clean expired CSRF tokens
        now = datetime.now(timezone.utc)
        expired_csrf = [t for t, exp in self._csrf_tokens.items() if now >= exp]
        for t in expired_csrf:
            del self._csrf_tokens[t]

    async def create_csrf(self) -> str:
        token = secrets.token_urlsafe(32)
        self._csrf_tokens[token] = (
            datetime.now(timezone.utc) + timedelta(seconds=_CSRF_TTL_SECONDS)
        )
        return token

    async def validate_csrf(self, token: str) -> bool:
        expires = self._csrf_tokens.pop(token, None)
        if expires is None:
            return False
        return datetime.now(timezone.utc) < expires


SESSION_KEY_PREFIX = "tsigma:session:"


class ValkeySessionStore(BaseSessionStore):
    """
    Valkey-backed session store with TTL expiry.

    Sessions are JSON-serialized and stored with server-side TTL.
    Supports multi-process deployments and survives restarts.
    """

    def __init__(self, client, ttl_minutes: int = 480) -> None:
        """
        Args:
            client: An async valkey client (valkey.asyncio.Valkey instance).
            ttl_minutes: Session time-to-live in minutes.
        """
        self._client = client
        self._ttl_seconds = ttl_minutes * 60
        self._ttl = timedelta(minutes=ttl_minutes)

    async def create(self, user_id: UUID, username: str, role: str) -> str:
        session_id = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        session = SessionData(
            user_id=user_id,
            username=username,
            role=role,
            created_at=now,
            expires_at=now + self._ttl,
        )
        key = f"{SESSION_KEY_PREFIX}{session_id}"
        await self._client.setex(key, self._ttl_seconds, json.dumps(session.to_dict()))
        return session_id

    async def get(self, session_id: str) -> SessionData | None:
        key = f"{SESSION_KEY_PREFIX}{session_id}"
        raw = await self._client.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            session = SessionData.from_dict(json.loads(raw))
        except (KeyError, ValueError) as e:
            logger.error("Malformed session data for %s: %s", session_id, e)
            await self._client.delete(key)
            return None
        if session.is_expired:
            await self._client.delete(key)
            return None
        # Sliding expiry — refresh TTL on each access
        await self._client.expire(key, self._ttl_seconds)
        return session

    async def delete(self, session_id: str) -> None:
        key = f"{SESSION_KEY_PREFIX}{session_id}"
        await self._client.delete(key)

    async def cleanup(self) -> None:
        """No-op — Valkey TTL handles expiry automatically."""

    async def create_csrf(self) -> str:
        token = secrets.token_urlsafe(32)
        key = f"{CSRF_KEY_PREFIX}{token}"
        await self._client.setex(key, _CSRF_TTL_SECONDS, "1")
        return token

    async def validate_csrf(self, token: str) -> bool:
        key = f"{CSRF_KEY_PREFIX}{token}"
        result = await self._client.delete(key)
        return result > 0  # delete returns count of keys removed
