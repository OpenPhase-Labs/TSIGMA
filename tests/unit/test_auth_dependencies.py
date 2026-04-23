"""
Unit tests for authentication dependencies.

Tests session store retrieval, current user extraction from cookies,
and role-based access control.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from tsigma.auth.dependencies import (
    get_current_user,
    get_current_user_optional,
    get_session_store,
    require_admin,
)
from tsigma.auth.sessions import SessionData


class TestGetSessionStore:
    """Tests for get_session_store()."""

    def test_returns_store_from_request_app_state(self):
        """Test get_session_store returns store from app.state."""
        mock_store = MagicMock()
        mock_request = MagicMock()
        mock_request.app.state.session_store = mock_store

        result = get_session_store(mock_request)
        assert result is mock_store

    def test_raises_when_store_not_initialized(self):
        """Test get_session_store raises RuntimeError when missing."""
        mock_request = MagicMock()
        mock_request.app.state = MagicMock(spec=[])

        with pytest.raises(RuntimeError, match="Session store not initialized"):
            get_session_store(mock_request)


class TestGetCurrentUserOptional:
    """Tests for get_current_user_optional()."""

    @pytest.mark.asyncio
    async def test_returns_none_when_no_cookie(self):
        """Test returns None when session cookie is absent."""
        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.cookies = {}
        mock_store = MagicMock()

        with patch("tsigma.auth.dependencies.settings") as mock_settings:
            mock_settings.auth_cookie_name = "tsigma_session"
            result = await get_current_user_optional(mock_request, mock_store)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_session_expired(self):
        """Test returns None when session ID maps to expired session."""
        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.cookies = {"tsigma_session": "expired-id"}
        mock_store = AsyncMock()
        mock_store.get.return_value = None

        with patch("tsigma.auth.dependencies.settings") as mock_settings:
            mock_settings.auth_cookie_name = "tsigma_session"
            result = await get_current_user_optional(mock_request, mock_store)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_session_data_when_valid(self):
        """Test returns SessionData when cookie maps to valid session."""
        user_id = uuid4()
        now = datetime.now(timezone.utc)
        session_data = SessionData(
            user_id=user_id,
            username="admin",
            role="admin",
            created_at=now,
            expires_at=now + timedelta(hours=8),
        )

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.cookies = {"tsigma_session": "valid-session-id"}
        mock_store = AsyncMock()
        mock_store.get.return_value = session_data

        with patch("tsigma.auth.dependencies.settings") as mock_settings:
            mock_settings.auth_cookie_name = "tsigma_session"
            result = await get_current_user_optional(mock_request, mock_store)

        assert result is session_data
        assert result.username == "admin"


class TestGetCurrentUser:
    """Tests for get_current_user()."""

    def test_returns_user_when_authenticated(self):
        """Test returns SessionData when user is authenticated."""
        session_data = SessionData(
            user_id=uuid4(),
            username="admin",
            role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
        )
        result = get_current_user(session_data)
        assert result is session_data

    def test_raises_401_when_not_authenticated(self):
        """Test raises 401 HTTPException when user is None."""
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(None)
        assert exc_info.value.status_code == 401
        assert "Not authenticated" in exc_info.value.detail


class TestRequireAdmin:
    """Tests for require_admin()."""

    def test_returns_user_when_admin(self):
        """Test returns SessionData when user has admin role."""
        session_data = SessionData(
            user_id=uuid4(),
            username="admin",
            role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
        )
        result = require_admin(session_data)
        assert result is session_data

    def test_raises_403_when_viewer(self):
        """Test raises 403 HTTPException when user is viewer."""
        session_data = SessionData(
            user_id=uuid4(),
            username="reader",
            role="viewer",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
        )
        with pytest.raises(HTTPException) as exc_info:
            require_admin(session_data)
        assert exc_info.value.status_code == 403
        assert "Admin role required" in exc_info.value.detail
