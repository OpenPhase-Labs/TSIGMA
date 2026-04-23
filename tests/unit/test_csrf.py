"""
Unit tests for CSRF token methods on InMemorySessionStore.

Tests creation, validation, one-time consumption, invalid tokens, and expiry.
"""

from datetime import datetime, timedelta, timezone

import pytest

from tsigma.auth.sessions import InMemorySessionStore


@pytest.mark.asyncio
async def test_create_csrf_returns_token():
    """create_csrf() returns a non-empty string."""
    store = InMemorySessionStore()
    token = await store.create_csrf()
    assert isinstance(token, str)
    assert len(token) > 0


@pytest.mark.asyncio
async def test_validate_csrf_success():
    """A freshly created token validates successfully."""
    store = InMemorySessionStore()
    token = await store.create_csrf()
    result = await store.validate_csrf(token)
    assert result is True


@pytest.mark.asyncio
async def test_validate_csrf_consumed():
    """Tokens are one-time use: second validation returns False."""
    store = InMemorySessionStore()
    token = await store.create_csrf()
    first = await store.validate_csrf(token)
    second = await store.validate_csrf(token)
    assert first is True
    assert second is False


@pytest.mark.asyncio
async def test_validate_csrf_invalid():
    """A token that was never created returns False."""
    store = InMemorySessionStore()
    result = await store.validate_csrf("nonexistent-token")
    assert result is False


@pytest.mark.asyncio
async def test_validate_csrf_expired():
    """An expired token returns False."""
    store = InMemorySessionStore()
    token = await store.create_csrf()
    # Patch the expiry to be in the past
    store._csrf_tokens[token] = datetime.now(timezone.utc) - timedelta(seconds=1)
    result = await store.validate_csrf(token)
    assert result is False
