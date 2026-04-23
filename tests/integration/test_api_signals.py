"""
Integration tests for Signals API.

Tests HTTP endpoints for signal CRUD operations using
dependency-overridden mock sessions.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from tsigma.app import create_app
from tsigma.auth.dependencies import get_session_store, require_admin
from tsigma.auth.sessions import InMemorySessionStore, SessionData
from tsigma.dependencies import get_session
from tsigma.models import Signal
from tsigma.settings_service import settings_cache


def _make_signal(**overrides) -> Signal:
    """
    Create a Signal model instance with sensible defaults.

    Args:
        **overrides: Fields to override on the default signal.

    Returns:
        Signal instance.
    """
    defaults = {
        "signal_id": "SIG-001",
        "primary_street": "Main St",
        "secondary_street": "1st Ave",
        "latitude": Decimal("33.7756"),
        "longitude": Decimal("-84.3963"),
        "enabled": True,
        "signal_metadata": None,
    }
    defaults.update(overrides)
    return Signal(**defaults)


def _admin_session_data() -> SessionData:
    """Create a SessionData for an admin user."""
    return SessionData(
        user_id=uuid4(),
        username="admin",
        role="admin",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _viewer_session_data() -> SessionData:
    """Create a SessionData for a viewer user."""
    return SessionData(
        user_id=uuid4(),
        username="viewer",
        role="viewer",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


@pytest.fixture(autouse=True)
def _warm_settings_cache():
    """Pre-fill the settings cache so require_access() never hits the DB."""
    settings_cache._cache = {
        "access_policy.analytics": "public",
        "access_policy.reports": "public",
        "access_policy.signal_detail": "public",
        "access_policy.health": "public",
        "access_policy.management": "authenticated",
    }
    settings_cache._last_refresh = float("inf")
    yield
    settings_cache.invalidate()


@pytest.fixture
def mock_session():
    """Async mock session for dependency injection."""
    session = AsyncMock()
    session.add = MagicMock()
    return session


@pytest.fixture
def test_client(mock_session):
    """
    TestClient with get_session overridden to use mock.

    Args:
        mock_session: Mocked async database session.

    Returns:
        FastAPI TestClient with dependency override.
    """
    app = create_app()
    app.dependency_overrides[get_session] = lambda: mock_session
    app.dependency_overrides[get_session_store] = lambda: InMemorySessionStore()
    return TestClient(app)


@pytest.fixture
def admin_client(mock_session):
    """TestClient authenticated as admin."""
    app = create_app()
    app.dependency_overrides[get_session] = lambda: mock_session
    app.dependency_overrides[require_admin] = lambda: _admin_session_data()
    return TestClient(app)


class TestListSignals:
    """Tests for GET /api/v1/signals."""

    def test_returns_signal_list(self, test_client, mock_session):
        """Test listing signals returns serialized signal data."""
        sig = _make_signal()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [sig]
        mock_session.execute.return_value = mock_result

        response = test_client.get("/api/v1/signals/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["signal_id"] == "SIG-001"
        assert data[0]["primary_street"] == "Main St"
        assert data[0]["enabled"] is True

    def test_returns_empty_list(self, test_client, mock_session):
        """Test listing signals returns empty list when none exist."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        response = test_client.get("/api/v1/signals/")

        assert response.status_code == 200
        assert response.json() == []

    def test_pagination_params_passed(self, test_client, mock_session):
        """Test skip and limit query params are accepted."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        response = test_client.get("/api/v1/signals/?skip=10&limit=5")

        assert response.status_code == 200

    def test_serializes_coordinates_as_strings(self, test_client, mock_session):
        """Test latitude/longitude are serialized as strings."""
        sig = _make_signal(
            latitude=Decimal("40.7128"),
            longitude=Decimal("-74.0060"),
        )
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [sig]
        mock_session.execute.return_value = mock_result

        response = test_client.get("/api/v1/signals/")

        data = response.json()[0]
        assert data["latitude"] == "40.7128"
        assert data["longitude"] == "-74.0060"

    def test_null_coordinates(self, test_client, mock_session):
        """Test signals without coordinates return null."""
        sig = _make_signal(latitude=None, longitude=None)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [sig]
        mock_session.execute.return_value = mock_result

        response = test_client.get("/api/v1/signals/")

        data = response.json()[0]
        assert data["latitude"] is None
        assert data["longitude"] is None

    def test_multiple_signals(self, test_client, mock_session):
        """Test listing multiple signals."""
        signals = [
            _make_signal(signal_id="SIG-001", primary_street="Main St"),
            _make_signal(signal_id="SIG-002", primary_street="Oak Ave"),
            _make_signal(signal_id="SIG-003", primary_street="Elm Blvd"),
        ]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = signals
        mock_session.execute.return_value = mock_result

        response = test_client.get("/api/v1/signals/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        assert data[1]["signal_id"] == "SIG-002"


class TestGetSignal:
    """Tests for GET /api/v1/signals/{signal_id}."""

    def test_returns_signal_detail(self, test_client, mock_session):
        """Test getting a signal by ID returns full detail."""
        sig = _make_signal()
        sig.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        sig.updated_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
        sig.metadata = {"firmware": "AXON-1.2"}

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sig
        mock_session.execute.return_value = mock_result

        response = test_client.get("/api/v1/signals/SIG-001")

        assert response.status_code == 200
        data = response.json()
        assert data["signal_id"] == "SIG-001"
        assert data["primary_street"] == "Main St"
        assert data["secondary_street"] == "1st Ave"
        assert data["latitude"] == "33.7756"
        assert data["metadata"] == {"firmware": "AXON-1.2"}

    def test_404_for_nonexistent_signal(self, test_client, mock_session):
        """Test 404 response when signal ID doesn't exist."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        response = test_client.get("/api/v1/signals/NONEXISTENT")

        assert response.status_code == 404
        assert "NONEXISTENT" in response.json()["detail"]


class TestCreateSignal:
    """Tests for POST /api/v1/signals."""

    def test_creates_signal(self, admin_client, mock_session):
        """Test creating a signal returns 201 with signal data."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        response = admin_client.post(
            "/api/v1/signals/",
            json={"signal_id": "SIG-NEW", "primary_street": "New St"},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["signal_id"] == "SIG-NEW"
        assert data["primary_street"] == "New St"
        assert data["enabled"] is True
        mock_session.add.assert_called_once()

    def test_duplicate_signal_id_returns_409(self, admin_client, mock_session):
        """Test creating signal with existing ID returns 409."""
        existing = _make_signal()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing
        mock_session.execute.return_value = mock_result

        response = admin_client.post(
            "/api/v1/signals/",
            json={"signal_id": "SIG-001", "primary_street": "Main St"},
        )

        assert response.status_code == 409
        assert "already exists" in response.json()["detail"]

    def test_missing_required_fields_returns_422(self, admin_client):
        """Test creating signal without required fields returns 422."""
        response = admin_client.post("/api/v1/signals/", json={})
        assert response.status_code == 422

    def test_no_auth_returns_401(self, test_client):
        """Test creating signal without auth returns 401."""
        response = test_client.post(
            "/api/v1/signals/",
            json={"signal_id": "SIG-NEW", "primary_street": "New St"},
        )
        assert response.status_code == 401

    def test_viewer_returns_403(self, mock_session):
        """Test creating signal as viewer returns 403."""
        from tsigma.auth.dependencies import get_current_user

        app = create_app()
        app.dependency_overrides[get_session] = lambda: mock_session
        app.dependency_overrides[get_current_user] = lambda: _viewer_session_data()
        client = TestClient(app)

        response = client.post(
            "/api/v1/signals/",
            json={"signal_id": "SIG-NEW", "primary_street": "New St"},
        )
        assert response.status_code == 403


class TestUpdateSignal:
    """Tests for PUT /api/v1/signals/{signal_id}."""

    def test_updates_signal(self, admin_client, mock_session):
        """Test updating a signal returns 200 with updated data."""
        sig = _make_signal()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sig
        mock_session.execute.return_value = mock_result

        response = admin_client.put(
            "/api/v1/signals/SIG-001",
            json={"primary_street": "Updated Main St"},
        )

        assert response.status_code == 200
        assert response.json()["primary_street"] == "Updated Main St"

    def test_partial_update(self, admin_client, mock_session):
        """Test partial update only changes provided fields."""
        sig = _make_signal()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sig
        mock_session.execute.return_value = mock_result

        response = admin_client.put(
            "/api/v1/signals/SIG-001",
            json={"enabled": False},
        )

        assert response.status_code == 200
        assert sig.enabled is False
        assert sig.primary_street == "Main St"

    def test_not_found_returns_404(self, admin_client, mock_session):
        """Test updating nonexistent signal returns 404."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        response = admin_client.put(
            "/api/v1/signals/NONEXISTENT",
            json={"primary_street": "Updated"},
        )

        assert response.status_code == 404

    def test_no_auth_returns_401(self, test_client):
        """Test updating signal without auth returns 401."""
        response = test_client.put(
            "/api/v1/signals/SIG-001",
            json={"primary_street": "Updated"},
        )
        assert response.status_code == 401


class TestDeleteSignal:
    """Tests for DELETE /api/v1/signals/{signal_id}."""

    def test_deletes_signal(self, admin_client, mock_session):
        """Test deleting a signal returns 204."""
        sig = _make_signal()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sig
        mock_session.execute.return_value = mock_result

        response = admin_client.delete("/api/v1/signals/SIG-001")

        assert response.status_code == 204
        mock_session.delete.assert_awaited_once_with(sig)

    def test_not_found_returns_404(self, admin_client, mock_session):
        """Test deleting nonexistent signal returns 404."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        response = admin_client.delete("/api/v1/signals/NONEXISTENT")

        assert response.status_code == 404

    def test_no_auth_returns_401(self, test_client):
        """Test deleting signal without auth returns 401."""
        response = test_client.delete("/api/v1/signals/SIG-001")
        assert response.status_code == 401
