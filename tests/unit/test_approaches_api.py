"""
Unit tests for approaches API endpoints.

Tests list, get, and create operations for approach configuration.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.approaches import router
from tsigma.auth.sessions import SessionData
from tsigma.models import Approach, Signal


def _create_test_app():
    """Create a minimal FastAPI app with the approaches router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return app


def _add_access_overrides(app):
    """Override require_access sub-dependencies so GET endpoints pass auth."""
    from tsigma.auth.dependencies import _get_db_session, get_current_user_optional

    app.dependency_overrides[get_current_user_optional] = lambda: SessionData(
        user_id=uuid4(),
        username="testuser",
        role="viewer",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )

    async def _mock_access_db():
        mock = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock.execute = AsyncMock(return_value=result)
        yield mock

    app.dependency_overrides[_get_db_session] = _mock_access_db


def _mock_approach(**overrides):
    """Create a mock Approach ORM object."""
    now = datetime.now(timezone.utc)
    defaults = {
        "approach_id": uuid4(),
        "signal_id": "SIG-001",
        "direction_type_id": 1,
        "description": "Northbound Through",
        "mph": 35,
        "protected_phase_number": 2,
        "is_protected_phase_overlap": False,
        "permissive_phase_number": None,
        "is_permissive_phase_overlap": False,
        "ped_phase_number": 4,
        "created_at": now,
        "updated_at": now,
    }
    defaults.update(overrides)
    mock = MagicMock(spec=Approach)
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


def _mock_signal():
    """Create a mock Signal ORM object."""
    mock = MagicMock(spec=Signal)
    mock.signal_id = "SIG-001"
    return mock


class TestListApproaches:
    """Tests for GET /api/v1/signals/{signal_id}/approaches."""

    def test_returns_approaches(self):
        """Test listing approaches for a signal."""
        approach = _mock_approach()
        mock_session = AsyncMock()
        # First query: signal exists
        signal_result = MagicMock()
        signal_result.scalar_one_or_none.return_value = _mock_signal()
        # Second query: approaches
        approach_result = MagicMock()
        approach_result.scalars.return_value.all.return_value = [approach]
        mock_session.execute = AsyncMock(side_effect=[signal_result, approach_result])

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/SIG-001/approaches")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["signal_id"] == "SIG-001"

    def test_signal_not_found(self):
        """Test 404 when signal doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/NOPE/approaches")
        assert resp.status_code == 404

    def test_empty_list(self):
        """Test empty approach list for signal with no approaches."""
        mock_session = AsyncMock()
        signal_result = MagicMock()
        signal_result.scalar_one_or_none.return_value = _mock_signal()
        approach_result = MagicMock()
        approach_result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(side_effect=[signal_result, approach_result])

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/SIG-001/approaches")
        assert resp.status_code == 200
        assert resp.json() == []


class TestGetApproach:
    """Tests for GET /api/v1/approaches/{approach_id}."""

    def test_returns_approach(self):
        """Test getting an approach by ID."""
        approach = _mock_approach()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = approach
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(f"/api/v1/approaches/{approach.approach_id}")
        assert resp.status_code == 200
        assert resp.json()["direction_type_id"] == 1

    def test_not_found(self):
        """Test 404 when approach doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(f"/api/v1/approaches/{uuid4()}")
        assert resp.status_code == 404


class TestCreateApproach:
    """Tests for POST /api/v1/signals/{signal_id}/approaches."""

    def test_creates_approach(self):
        """Test creating an approach under a signal."""
        approach = _mock_approach()
        mock_session = AsyncMock()
        signal_result = MagicMock()
        signal_result.scalar_one_or_none.return_value = _mock_signal()
        mock_session.execute = AsyncMock(return_value=signal_result)
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        with patch("tsigma.api.v1.approaches.Approach", return_value=approach):
            client = TestClient(app)
            resp = client.post(
                "/api/v1/signals/SIG-001/approaches",
                json={"direction_type_id": 1, "mph": 35},
            )

        assert resp.status_code == 201

    def test_signal_not_found(self):
        """Test 404 when parent signal doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.post(
            "/api/v1/signals/NOPE/approaches",
            json={"direction_type_id": 1},
        )
        assert resp.status_code == 404

    def test_validation_error(self):
        """Test 422 on invalid body."""
        mock_session = AsyncMock()

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.post(
            "/api/v1/signals/SIG-001/approaches",
            json={},
        )
        assert resp.status_code == 422


class TestUpdateApproach:
    """Tests for PUT /api/v1/approaches/{approach_id}."""

    def test_updates_approach(self):
        """Test updating an approach returns 200."""
        approach = _mock_approach()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = approach
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/approaches/{approach.approach_id}",
            json={"mph": 45},
        )
        assert resp.status_code == 200
        assert approach.mph == 45

    def test_partial_update(self):
        """Test partial update only changes provided fields."""
        approach = _mock_approach(mph=35, description="Northbound Through")
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = approach
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/approaches/{approach.approach_id}",
            json={"mph": 50},
        )
        assert resp.status_code == 200
        assert approach.mph == 50
        assert approach.description == "Northbound Through"

    def test_not_found(self):
        """Test 404 when approach doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/approaches/{uuid4()}",
            json={"mph": 45},
        )
        assert resp.status_code == 404

    def test_empty_body_returns_422(self):
        """Test 422 when no fields provided for update."""
        mock_session = AsyncMock()

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/approaches/{uuid4()}",
            json={},
        )
        assert resp.status_code == 422


class TestDeleteApproach:
    """Tests for DELETE /api/v1/approaches/{approach_id}."""

    def test_deletes_approach(self):
        """Test deleting an approach returns 204."""
        approach = _mock_approach()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = approach
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.delete(f"/api/v1/approaches/{approach.approach_id}")
        assert resp.status_code == 204
        mock_session.delete.assert_awaited_once_with(approach)

    def test_not_found(self):
        """Test 404 when approach doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.delete(f"/api/v1/approaches/{uuid4()}")
        assert resp.status_code == 404
