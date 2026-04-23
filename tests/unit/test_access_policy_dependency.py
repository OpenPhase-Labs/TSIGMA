"""
Unit tests for the require_access() dependency factory.

Tests public passthrough, authenticated enforcement, locked categories,
and missing/unknown settings.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from tsigma.auth.dependencies import require_access
from tsigma.auth.sessions import SessionData


def _make_user(role: str = "viewer") -> SessionData:
    """Create a test SessionData."""
    now = datetime.now(timezone.utc)
    return SessionData(
        user_id=uuid4(),
        username=f"test_{role}",
        role=role,
        created_at=now,
        expires_at=now + timedelta(hours=8),
    )


class TestRequireAccessFactory:
    """Tests for require_access() factory function."""

    def test_returns_callable(self):
        """Test require_access returns a callable dependency."""
        dep = require_access("analytics")
        assert callable(dep)

    def test_different_categories_return_different_functions(self):
        """Test each category creates a distinct dependency."""
        dep_a = require_access("analytics")
        dep_b = require_access("reports")
        assert dep_a is not dep_b


class TestLockedCategory:
    """Tests for locked categories (management)."""

    @pytest.mark.asyncio
    async def test_locked_requires_auth(self):
        """Test locked category raises 401 when user is None."""
        dep = require_access("management")
        mock_db = AsyncMock()

        with pytest.raises(HTTPException) as exc_info:
            await dep(
                user=None,
                db_session=mock_db,
            )
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_locked_allows_authenticated_user(self):
        """Test locked category passes with authenticated user."""
        dep = require_access("management")
        user = _make_user()
        mock_db = AsyncMock()

        result = await dep(
            user=user,
            db_session=mock_db,
        )
        assert result is user

    @pytest.mark.asyncio
    async def test_locked_skips_cache_lookup(self):
        """Test locked categories don't even check the cache."""
        dep = require_access("management")
        user = _make_user()
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            await dep(
                user=user,
                db_session=mock_db,
            )
            mock_cache.get.assert_not_called()


class TestAuthenticatedPolicy:
    """Tests for categories with 'authenticated' policy."""

    @pytest.mark.asyncio
    async def test_authenticated_policy_rejects_anonymous(self):
        """Test 'authenticated' policy raises 401 for anonymous user."""
        dep = require_access("analytics")
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value="authenticated")
            with pytest.raises(HTTPException) as exc_info:
                await dep(
                    user=None,
                    db_session=mock_db,
                )
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_authenticated_policy_allows_user(self):
        """Test 'authenticated' policy passes with logged-in user."""
        dep = require_access("analytics")
        user = _make_user()
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value="authenticated")
            result = await dep(
                user=user,
                db_session=mock_db,
            )
        assert result is user


class TestPublicPolicy:
    """Tests for categories with 'public' policy."""

    @pytest.mark.asyncio
    async def test_public_allows_anonymous(self):
        """Test 'public' policy allows unauthenticated access."""
        dep = require_access("analytics")
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value="public")
            result = await dep(
                user=None,
                db_session=mock_db,
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_public_still_returns_user_if_present(self):
        """Test 'public' policy returns user when one is authenticated."""
        dep = require_access("reports")
        user = _make_user()
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value="public")
            result = await dep(
                user=user,
                db_session=mock_db,
            )
        assert result is user


class TestMissingOrUnknownPolicy:
    """Tests for missing or unrecognised policy values."""

    @pytest.mark.asyncio
    async def test_missing_policy_defaults_to_authenticated(self):
        """Test None from cache (key missing) defaults to requiring auth."""
        dep = require_access("analytics")
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value=None)
            with pytest.raises(HTTPException) as exc_info:
                await dep(
                    user=None,
                    db_session=mock_db,
                )
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_unknown_value_defaults_to_authenticated(self):
        """Test unrecognised policy value (e.g. 'open') defaults to requiring auth."""
        dep = require_access("analytics")
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value="open")
            with pytest.raises(HTTPException) as exc_info:
                await dep(
                    user=None,
                    db_session=mock_db,
                )
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_unknown_value_allows_authenticated_user(self):
        """Test unrecognised policy still allows logged-in users."""
        dep = require_access("reports")
        user = _make_user()
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value="garbage")
            result = await dep(
                user=user,
                db_session=mock_db,
            )
        assert result is user


class TestCategoryKeyMapping:
    """Tests for correct policy key construction."""

    @pytest.mark.asyncio
    async def test_correct_cache_key_for_analytics(self):
        """Test analytics category looks up 'access_policy.analytics'."""
        dep = require_access("analytics")
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value="public")
            await dep(
                user=None,
                db_session=mock_db,
            )
            mock_cache.get.assert_awaited_once_with(
                "access_policy.analytics", mock_db
            )

    @pytest.mark.asyncio
    async def test_correct_cache_key_for_signal_detail(self):
        """Test signal_detail category looks up 'access_policy.signal_detail'."""
        dep = require_access("signal_detail")
        mock_db = AsyncMock()

        with patch("tsigma.auth.dependencies.settings_cache") as mock_cache:
            mock_cache.get = AsyncMock(return_value="public")
            await dep(
                user=None,
                db_session=mock_db,
            )
            mock_cache.get.assert_awaited_once_with(
                "access_policy.signal_detail", mock_db
            )
