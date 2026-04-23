"""
Unit tests for Corridors API endpoints.

Tests list, get by ID, and create operations for corridor groupings.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.corridors import router
from tsigma.auth.sessions import SessionData
from tsigma.models import Corridor


def _create_test_app():
    """Create a minimal FastAPI app with the corridors router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/corridors")
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


def _add_session_override(app, mock_session):
    """Add a get_session dependency override."""
    from tsigma.dependencies import get_session

    async def override_session():
        yield mock_session

    app.dependency_overrides[get_session] = override_session


def _mock_corridor(**overrides):
    """Create a mock Corridor ORM object."""
    defaults = {
        "corridor_id": uuid4(),
        "name": "Peachtree Street Corridor",
        "description": "Main north-south corridor",
        "jurisdiction_id": None,
    }
    defaults.update(overrides)
    mock = MagicMock(spec=Corridor)
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


class TestListCorridors:
    """Tests for GET /api/v1/corridors/."""

    def test_returns_corridors(self):
        corridor = _mock_corridor()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = [corridor]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/corridors/")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "Peachtree Street Corridor"

    def test_empty_list(self):
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/corridors/")
        assert resp.status_code == 200
        assert resp.json() == []


class TestGetCorridor:
    """Tests for GET /api/v1/corridors/{corridor_id}."""

    def test_returns_corridor(self):
        corridor = _mock_corridor()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = corridor
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get(f"/api/v1/corridors/{corridor.corridor_id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "Peachtree Street Corridor"

    def test_not_found(self):
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get(f"/api/v1/corridors/{uuid4()}")
        assert resp.status_code == 404


class TestCreateCorridor:
    """Tests for POST /api/v1/corridors/."""

    def test_creates_corridor(self):
        expected_id = uuid4()
        added_objects = []
        mock_session = AsyncMock()
        mock_session.add = MagicMock(side_effect=lambda obj: added_objects.append(obj))

        async def fake_flush():
            for obj in added_objects:
                if hasattr(obj, "corridor_id") and obj.corridor_id is None:
                    object.__setattr__(obj, "corridor_id", expected_id)

        mock_session.flush = fake_flush

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
            "/api/v1/corridors/",
            json={"name": "Peachtree Street Corridor"},
        )
        assert resp.status_code == 201
        assert resp.json()["name"] == "Peachtree Street Corridor"

    def test_name_required(self):
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
        resp = client.post("/api/v1/corridors/", json={})
        assert resp.status_code == 422


def _add_admin_overrides(app):
    """Override admin dependency for POST/PUT/DELETE endpoints."""
    from tsigma.auth.dependencies import require_admin

    app.dependency_overrides[require_admin] = lambda: SessionData(
        user_id=str(uuid4()),
        username="admin",
        role="admin",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _add_audited_session_override(app, mock_session):
    """Add a get_audited_session dependency override."""
    from tsigma.dependencies import get_audited_session

    async def override_session():
        yield mock_session

    app.dependency_overrides[get_audited_session] = override_session


class TestUpdateCorridor:
    """Tests for PUT /api/v1/corridors/{corridor_id}."""

    def test_update_corridor(self):
        """PUT /corridors/{id} returns updated corridor data."""
        corridor = _mock_corridor(name="Old Name")

        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = corridor
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/corridors/{corridor.corridor_id}",
            json={"name": "New Name"},
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == "New Name"

    def test_update_corridor_not_found(self):
        """PUT /corridors/{id} returns 404 for unknown corridor."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/corridors/{uuid4()}",
            json={"name": "Updated"},
        )
        assert resp.status_code == 404


class TestDeleteCorridor:
    """Tests for DELETE /api/v1/corridors/{corridor_id} (via crud_factory)."""

    def test_delete_corridor(self):
        """DELETE /corridors/{id} returns 204 on success."""
        corridor = _mock_corridor()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = corridor
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.delete = AsyncMock()
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_audited_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.delete(f"/api/v1/corridors/{corridor.corridor_id}")
        assert resp.status_code == 204

    def test_delete_corridor_not_found(self):
        """DELETE /corridors/{id} returns 404 for unknown corridor."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_audited_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.delete(f"/api/v1/corridors/{uuid4()}")
        assert resp.status_code == 404
