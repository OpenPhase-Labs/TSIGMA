"""
Unit tests for ValkeySessionStore.

Validates session CRUD, sliding expiry, CSRF token lifecycle, and
error handling with a fully mocked valkey async client.
"""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from tsigma.auth.sessions import (
    _CSRF_TTL_SECONDS,
    CSRF_KEY_PREFIX,
    SESSION_KEY_PREFIX,
    SessionData,
    ValkeySessionStore,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_client():
    """Return an AsyncMock that behaves like valkey.asyncio.Valkey."""
    client = AsyncMock()
    client.setex = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.delete = AsyncMock(return_value=0)
    client.expire = AsyncMock()
    return client


def _make_session_data(
    user_id=None, username="testuser", role="viewer", ttl_minutes=480,
):
    """Build a SessionData instance with reasonable defaults."""
    uid = user_id or uuid4()
    now = datetime.now(timezone.utc)
    return SessionData(
        user_id=uid,
        username=username,
        role=role,
        created_at=now,
        expires_at=now + timedelta(minutes=ttl_minutes),
    )


# ---------------------------------------------------------------------------
# create()
# ---------------------------------------------------------------------------

class TestValkeyCreate:
    """Tests for ValkeySessionStore.create()."""

    @pytest.mark.asyncio
    async def test_create_stores_session(self):
        """create() calls setex with correct prefix and TTL."""
        client = _mock_client()
        store = ValkeySessionStore(client, ttl_minutes=60)

        user_id = uuid4()
        session_id = await store.create(
            user_id=user_id, username="alice", role="admin",
        )

        assert isinstance(session_id, str)
        assert len(session_id) > 0

        # setex was called once
        client.setex.assert_awaited_once()
        call_args = client.setex.call_args
        key = call_args[0][0]
        ttl = call_args[0][1]
        payload = call_args[0][2]

        assert key.startswith(SESSION_KEY_PREFIX)
        assert key == f"{SESSION_KEY_PREFIX}{session_id}"
        assert ttl == 60 * 60  # 60 minutes in seconds

        # Payload is valid JSON with the expected fields
        data = json.loads(payload)
        assert data["user_id"] == str(user_id)
        assert data["username"] == "alice"
        assert data["role"] == "admin"


# ---------------------------------------------------------------------------
# get()
# ---------------------------------------------------------------------------

class TestValkeyGet:
    """Tests for ValkeySessionStore.get()."""

    @pytest.mark.asyncio
    async def test_get_returns_session(self):
        """get() returns deserialized SessionData for a valid key."""
        client = _mock_client()
        store = ValkeySessionStore(client, ttl_minutes=480)

        sd = _make_session_data(username="bob", role="admin")
        raw = json.dumps(sd.to_dict()).encode("utf-8")
        client.get.return_value = raw

        result = await store.get("some-session-id")

        assert result is not None
        assert result.username == "bob"
        assert result.role == "admin"
        client.get.assert_awaited_once_with(
            f"{SESSION_KEY_PREFIX}some-session-id"
        )

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self):
        """get() returns None when key does not exist."""
        client = _mock_client()
        client.get.return_value = None
        store = ValkeySessionStore(client)

        result = await store.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_expired_deletes(self):
        """get() deletes an expired session and returns None."""
        client = _mock_client()
        store = ValkeySessionStore(client, ttl_minutes=480)

        # Build a session that expired 1 hour ago
        now = datetime.now(timezone.utc)
        sd = SessionData(
            user_id=uuid4(),
            username="expired_user",
            role="viewer",
            created_at=now - timedelta(hours=10),
            expires_at=now - timedelta(hours=1),
        )
        client.get.return_value = json.dumps(sd.to_dict()).encode("utf-8")

        result = await store.get("expired-id")

        assert result is None
        client.delete.assert_awaited_once_with(
            f"{SESSION_KEY_PREFIX}expired-id"
        )

    @pytest.mark.asyncio
    async def test_get_malformed_deletes(self):
        """get() deletes malformed JSON and returns None."""
        client = _mock_client()
        store = ValkeySessionStore(client)

        client.get.return_value = b"this is not json"

        result = await store.get("bad-id")

        assert result is None
        client.delete.assert_awaited_once_with(
            f"{SESSION_KEY_PREFIX}bad-id"
        )

    @pytest.mark.asyncio
    async def test_sliding_expiry(self):
        """get() refreshes TTL via expire() on a valid session."""
        client = _mock_client()
        ttl_minutes = 120
        store = ValkeySessionStore(client, ttl_minutes=ttl_minutes)

        sd = _make_session_data(ttl_minutes=ttl_minutes)
        client.get.return_value = json.dumps(sd.to_dict()).encode("utf-8")

        result = await store.get("sliding-id")

        assert result is not None
        client.expire.assert_awaited_once_with(
            f"{SESSION_KEY_PREFIX}sliding-id",
            ttl_minutes * 60,
        )


# ---------------------------------------------------------------------------
# delete()
# ---------------------------------------------------------------------------

class TestValkeyDelete:
    """Tests for ValkeySessionStore.delete()."""

    @pytest.mark.asyncio
    async def test_delete_removes(self):
        """delete() calls client.delete with the correct key."""
        client = _mock_client()
        store = ValkeySessionStore(client)

        await store.delete("del-me")

        client.delete.assert_awaited_once_with(
            f"{SESSION_KEY_PREFIX}del-me"
        )


# ---------------------------------------------------------------------------
# CSRF
# ---------------------------------------------------------------------------

class TestValkeyCsrf:
    """Tests for CSRF token creation and validation."""

    @pytest.mark.asyncio
    async def test_create_csrf(self):
        """create_csrf() calls setex with csrf prefix and TTL."""
        client = _mock_client()
        store = ValkeySessionStore(client)

        token = await store.create_csrf()

        assert isinstance(token, str)
        assert len(token) > 0

        client.setex.assert_awaited_once()
        call_args = client.setex.call_args
        key = call_args[0][0]
        ttl = call_args[0][1]
        value = call_args[0][2]

        assert key == f"{CSRF_KEY_PREFIX}{token}"
        assert ttl == _CSRF_TTL_SECONDS
        assert value == "1"

    @pytest.mark.asyncio
    async def test_validate_csrf_valid(self):
        """validate_csrf() returns True when the key existed (delete returns 1)."""
        client = _mock_client()
        client.delete.return_value = 1
        store = ValkeySessionStore(client)

        result = await store.validate_csrf("valid-token")

        assert result is True
        client.delete.assert_awaited_once_with(
            f"{CSRF_KEY_PREFIX}valid-token"
        )

    @pytest.mark.asyncio
    async def test_validate_csrf_invalid(self):
        """validate_csrf() returns False when the key did not exist (delete returns 0)."""
        client = _mock_client()
        client.delete.return_value = 0
        store = ValkeySessionStore(client)

        result = await store.validate_csrf("unknown-token")

        assert result is False
