"""
Unit tests for authentication API endpoints.

Tests login, logout, and me endpoints using
dependency-overridden mock sessions.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.auth.dependencies import get_current_user, get_session_store
from tsigma.auth.providers.local import LocalAuthProvider
from tsigma.auth.router import router as auth_router
from tsigma.auth.sessions import InMemorySessionStore, SessionData
from tsigma.dependencies import get_session


def _make_mock_user(**overrides):
    """Create a mock AuthUser ORM object."""
    defaults = {
        "id": uuid4(),
        "username": "admin",
        "password_hash": "$2b$12$fakehash",
        "role": MagicMock(value="admin"),
        "is_active": True,
    }
    defaults.update(overrides)
    user = MagicMock()
    for k, v in defaults.items():
        setattr(user, k, v)
    return user


def _create_login_app():
    """Create a minimal app with both shared auth and local login routes."""
    app = FastAPI()
    app.include_router(auth_router, prefix="/api/v1/auth")
    provider = LocalAuthProvider()
    app.include_router(provider.get_router(), prefix="/api/v1/auth")
    return app


@pytest.fixture
def mock_db_session():
    """Async mock database session."""
    return AsyncMock()


@pytest.fixture
def mock_store():
    """In-memory session store for testing."""
    return InMemorySessionStore(ttl_minutes=480)


@pytest.fixture
def test_client(mock_db_session, mock_store):
    """TestClient with auth dependencies overridden."""
    app = _create_login_app()
    app.dependency_overrides[get_session] = lambda: mock_db_session
    app.dependency_overrides[get_session_store] = lambda: mock_store
    return TestClient(app)


class TestLogin:
    """Tests for POST /api/v1/auth/login."""

    def _get_csrf(self, test_client):
        """Get a fresh CSRF token from the endpoint."""
        resp = test_client.get("/api/v1/auth/csrf")
        assert resp.status_code == 200
        return resp.json()["csrf_token"]

    def test_successful_login(self, test_client, mock_db_session, mock_store):
        """Test login returns 200 and sets session cookie."""
        csrf = self._get_csrf(test_client)
        user = _make_mock_user()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        mock_db_session.execute.return_value = mock_result

        with patch("tsigma.auth.providers.local.verify_password", return_value=True):
            response = test_client.post(
                "/api/v1/auth/login",
                json={"username": "admin", "password": "changeme", "csrf_token": csrf},
            )

        assert response.status_code == 200
        assert response.json()["username"] == "admin"
        assert "tsigma_session" in response.cookies

    def test_invalid_username_returns_401(self, test_client, mock_db_session):
        """Test login with unknown username returns 401."""
        csrf = self._get_csrf(test_client)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db_session.execute.return_value = mock_result

        response = test_client.post(
            "/api/v1/auth/login",
            json={"username": "nouser", "password": "pass", "csrf_token": csrf},
        )

        assert response.status_code == 401
        assert "Invalid credentials" in response.json()["detail"]

    def test_wrong_password_returns_401(self, test_client, mock_db_session):
        """Test login with wrong password returns 401."""
        csrf = self._get_csrf(test_client)
        user = _make_mock_user()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        mock_db_session.execute.return_value = mock_result

        with patch("tsigma.auth.providers.local.verify_password", return_value=False):
            response = test_client.post(
                "/api/v1/auth/login",
                json={"username": "admin", "password": "wrong", "csrf_token": csrf},
            )

        assert response.status_code == 401

    def test_inactive_user_returns_401(self, test_client, mock_db_session):
        """Test login with inactive user returns 401."""
        csrf = self._get_csrf(test_client)
        user = _make_mock_user(is_active=False)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        mock_db_session.execute.return_value = mock_result

        with patch("tsigma.auth.providers.local.verify_password", return_value=True):
            response = test_client.post(
                "/api/v1/auth/login",
                json={"username": "admin", "password": "changeme", "csrf_token": csrf},
            )

        assert response.status_code == 401

    def test_invalid_csrf_returns_403(self, test_client):
        """Test login with invalid CSRF token returns 403."""
        response = test_client.post(
            "/api/v1/auth/login",
            json={"username": "admin", "password": "changeme", "csrf_token": "bogus"},
        )
        assert response.status_code == 403
        assert "CSRF" in response.json()["detail"]

    def test_reused_csrf_returns_403(self, test_client, mock_db_session):
        """Test CSRF token cannot be reused (one-time)."""
        csrf = self._get_csrf(test_client)
        user = _make_mock_user()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        mock_db_session.execute.return_value = mock_result

        with patch("tsigma.auth.providers.local.verify_password", return_value=True):
            # First use — should succeed
            resp1 = test_client.post(
                "/api/v1/auth/login",
                json={"username": "admin", "password": "changeme", "csrf_token": csrf},
            )
            assert resp1.status_code == 200

            # Second use — same token should fail
            resp2 = test_client.post(
                "/api/v1/auth/login",
                json={"username": "admin", "password": "changeme", "csrf_token": csrf},
            )
            assert resp2.status_code == 403

    def test_missing_fields_returns_422(self, test_client):
        """Test login with missing fields returns 422 validation error."""
        response = test_client.post("/api/v1/auth/login", json={})
        assert response.status_code == 422


class TestLogout:
    """Tests for POST /api/v1/auth/logout."""

    @pytest.mark.asyncio
    async def test_logout_clears_session(self, test_client, mock_store):
        """Test logout deletes session from store."""
        session_id = await mock_store.create(
            user_id=uuid4(), username="admin", role="admin",
        )

        test_client.cookies.set("tsigma_session", session_id)
        response = test_client.post("/api/v1/auth/logout")

        assert response.status_code == 200
        assert await mock_store.get(session_id) is None

    def test_logout_without_session_returns_200(self, test_client):
        """Test logout without active session still returns 200."""
        response = test_client.post("/api/v1/auth/logout")
        assert response.status_code == 200


class TestMe:
    """Tests for GET /api/v1/auth/me."""

    def test_returns_current_user(self, test_client):
        """Test /me returns authenticated user info."""
        user_id = uuid4()
        session_data = SessionData(
            user_id=user_id,
            username="admin",
            role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
        )
        test_client.app.dependency_overrides[get_current_user] = lambda: session_data

        response = test_client.get("/api/v1/auth/me")

        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "admin"
        assert data["role"] == "admin"

    def test_unauthenticated_returns_401(self):
        """Test /me without auth returns 401."""
        app = _create_login_app()
        app.dependency_overrides[get_session_store] = lambda: InMemorySessionStore()
        client = TestClient(app)

        response = client.get("/api/v1/auth/me")
        assert response.status_code == 401
