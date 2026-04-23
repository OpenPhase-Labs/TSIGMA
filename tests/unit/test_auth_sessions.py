"""
Unit tests for session store.

Tests in-memory session creation, retrieval, deletion, and TTL expiry.
"""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from tsigma.auth.sessions import (
    BaseSessionStore,
    InMemorySessionStore,
    SessionData,
)


class TestSessionData:
    """Tests for SessionData dataclass."""

    def test_create_session_data(self):
        """Test SessionData can be instantiated."""
        user_id = uuid4()
        now = datetime.now(timezone.utc)
        data = SessionData(
            user_id=user_id,
            username="admin",
            role="admin",
            created_at=now,
            expires_at=now + timedelta(hours=8),
        )
        assert data.user_id == user_id
        assert data.username == "admin"
        assert data.role == "admin"

    def test_is_expired_when_past(self):
        """Test is_expired returns True when expires_at is in the past."""
        now = datetime.now(timezone.utc)
        data = SessionData(
            user_id=uuid4(),
            username="admin",
            role="admin",
            created_at=now - timedelta(hours=9),
            expires_at=now - timedelta(hours=1),
        )
        assert data.is_expired is True

    def test_is_not_expired_when_future(self):
        """Test is_expired returns False when expires_at is in the future."""
        now = datetime.now(timezone.utc)
        data = SessionData(
            user_id=uuid4(),
            username="admin",
            role="admin",
            created_at=now,
            expires_at=now + timedelta(hours=8),
        )
        assert data.is_expired is False

    def test_to_dict_roundtrip(self):
        """Test to_dict/from_dict serialize and deserialize correctly."""
        user_id = uuid4()
        now = datetime.now(timezone.utc)
        original = SessionData(
            user_id=user_id,
            username="admin",
            role="admin",
            created_at=now,
            expires_at=now + timedelta(hours=8),
        )
        restored = SessionData.from_dict(original.to_dict())
        assert restored.user_id == original.user_id
        assert restored.username == original.username
        assert restored.role == original.role
        assert restored.created_at == original.created_at
        assert restored.expires_at == original.expires_at


class TestInMemorySessionStore:
    """Tests for InMemorySessionStore."""

    @pytest.mark.asyncio
    async def test_create_returns_session_id(self):
        """Test create() returns a string session ID."""
        store = InMemorySessionStore(ttl_minutes=480)
        session_id = await store.create(
            user_id=uuid4(), username="admin", role="admin",
        )
        assert isinstance(session_id, str)
        assert len(session_id) > 0

    @pytest.mark.asyncio
    async def test_get_returns_session_data(self):
        """Test get() returns SessionData for valid session ID."""
        store = InMemorySessionStore(ttl_minutes=480)
        user_id = uuid4()
        session_id = await store.create(
            user_id=user_id, username="admin", role="admin",
        )
        data = await store.get(session_id)
        assert data is not None
        assert data.user_id == user_id
        assert data.username == "admin"
        assert data.role == "admin"

    @pytest.mark.asyncio
    async def test_get_returns_none_for_unknown_id(self):
        """Test get() returns None for nonexistent session ID."""
        store = InMemorySessionStore(ttl_minutes=480)
        assert await store.get("nonexistent-id") is None

    @pytest.mark.asyncio
    async def test_get_returns_none_for_expired_session(self):
        """Test get() returns None and cleans up expired sessions."""
        store = InMemorySessionStore(ttl_minutes=0)
        session_id = await store.create(
            user_id=uuid4(), username="admin", role="admin",
        )
        assert await store.get(session_id) is None

    @pytest.mark.asyncio
    async def test_delete_removes_session(self):
        """Test delete() removes session so get() returns None."""
        store = InMemorySessionStore(ttl_minutes=480)
        session_id = await store.create(
            user_id=uuid4(), username="admin", role="admin",
        )
        await store.delete(session_id)
        assert await store.get(session_id) is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_does_not_raise(self):
        """Test delete() is safe for nonexistent session IDs."""
        store = InMemorySessionStore(ttl_minutes=480)
        await store.delete("nonexistent-id")

    @pytest.mark.asyncio
    async def test_cleanup_removes_expired_sessions(self):
        """Test cleanup() removes all expired sessions."""
        store = InMemorySessionStore(ttl_minutes=0)
        await store.create(user_id=uuid4(), username="a", role="admin")
        await store.create(user_id=uuid4(), username="b", role="viewer")
        await store.cleanup()
        assert len(store._sessions) == 0

    @pytest.mark.asyncio
    async def test_cleanup_preserves_valid_sessions(self):
        """Test cleanup() keeps sessions that have not expired."""
        store = InMemorySessionStore(ttl_minutes=480)
        session_id = await store.create(
            user_id=uuid4(), username="admin", role="admin",
        )
        await store.cleanup()
        assert await store.get(session_id) is not None

    @pytest.mark.asyncio
    async def test_create_sets_correct_expiry(self):
        """Test create() sets expires_at based on TTL."""
        store = InMemorySessionStore(ttl_minutes=60)
        session_id = await store.create(
            user_id=uuid4(), username="admin", role="admin",
        )
        data = await store.get(session_id)
        expected_delta = timedelta(minutes=60)
        actual_delta = data.expires_at - data.created_at
        assert abs(actual_delta.total_seconds() - expected_delta.total_seconds()) < 2

    def test_is_abstract_base(self):
        """Test BaseSessionStore cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseSessionStore()
