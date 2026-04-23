"""
Unit tests for jurisdictions API endpoints.

Tests list, get, and create operations for jurisdiction configuration.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.jurisdictions import router
from tsigma.auth.sessions import SessionData
from tsigma.models import Jurisdiction


def _create_test_app():
    """Create a minimal FastAPI app with the jurisdictions router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/jurisdictions")
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


def _mock_jurisdiction(**overrides):
    """Create a mock Jurisdiction ORM object."""
    defaults = {
        "jurisdiction_id": uuid4(),
        "name": "City of Atlanta",
        "mpo_name": "Atlanta Regional Commission",
        "county_name": "Fulton County",
    }
    defaults.update(overrides)
    mock = MagicMock(spec=Jurisdiction)
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


class TestListJurisdictions:
    """Tests for GET /api/v1/jurisdictions."""

    def test_returns_jurisdictions(self):
        """Test listing jurisdictions."""
        jurisdiction = _mock_jurisdiction()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = [jurisdiction]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/jurisdictions/")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "City of Atlanta"

    def test_empty_list(self):
        """Test empty jurisdiction list."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/jurisdictions/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_pagination(self):
        """Test pagination parameters are accepted."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/jurisdictions/?skip=10&limit=5")
        assert resp.status_code == 200


class TestGetJurisdiction:
    """Tests for GET /api/v1/jurisdictions/{jurisdiction_id}."""

    def test_returns_jurisdiction(self):
        """Test getting a jurisdiction by ID."""
        jurisdiction = _mock_jurisdiction()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = jurisdiction
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(f"/api/v1/jurisdictions/{jurisdiction.jurisdiction_id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "City of Atlanta"

    def test_not_found(self):
        """Test 404 when jurisdiction doesn't exist."""
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
        resp = client.get(f"/api/v1/jurisdictions/{uuid4()}")
        assert resp.status_code == 404


class TestCreateJurisdiction:
    """Tests for POST /api/v1/jurisdictions."""

    def test_creates_jurisdiction(self):
        """Test creating a jurisdiction."""
        expected_id = uuid4()
        added_objects = []
        mock_session = AsyncMock()
        mock_session.add = MagicMock(side_effect=lambda obj: added_objects.append(obj))

        async def fake_flush():
            # Simulate the DB assigning a UUID on flush
            for obj in added_objects:
                if hasattr(obj, "jurisdiction_id") and obj.jurisdiction_id is None:
                    object.__setattr__(obj, "jurisdiction_id", expected_id)

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
            "/api/v1/jurisdictions/",
            json={"name": "City of Atlanta", "mpo_name": "ARC"},
        )

        assert resp.status_code == 201
        assert resp.json()["name"] == "City of Atlanta"

    def test_name_required(self):
        """Test 422 when name missing."""
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
        resp = client.post("/api/v1/jurisdictions/", json={})
        assert resp.status_code == 422


class TestUpdateJurisdiction:
    """Tests for PUT /api/v1/jurisdictions/{jurisdiction_id}."""

    def test_updates_jurisdiction(self):
        """Test updating a jurisdiction returns 200."""
        jurisdiction = _mock_jurisdiction()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = jurisdiction
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
            f"/api/v1/jurisdictions/{jurisdiction.jurisdiction_id}",
            json={"name": "City of Decatur"},
        )
        assert resp.status_code == 200
        assert jurisdiction.name == "City of Decatur"

    def test_partial_update(self):
        """Test partial update only changes provided fields."""
        jurisdiction = _mock_jurisdiction(
            name="City of Atlanta", mpo_name="Atlanta Regional Commission",
        )
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = jurisdiction
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
            f"/api/v1/jurisdictions/{jurisdiction.jurisdiction_id}",
            json={"county_name": "DeKalb County"},
        )
        assert resp.status_code == 200
        assert jurisdiction.county_name == "DeKalb County"
        assert jurisdiction.name == "City of Atlanta"

    def test_not_found(self):
        """Test 404 when jurisdiction doesn't exist."""
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
            f"/api/v1/jurisdictions/{uuid4()}",
            json={"name": "Updated"},
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
            f"/api/v1/jurisdictions/{uuid4()}",
            json={},
        )
        assert resp.status_code == 422


class TestDeleteJurisdiction:
    """Tests for DELETE /api/v1/jurisdictions/{jurisdiction_id}."""

    def test_deletes_jurisdiction(self):
        """Test deleting a jurisdiction returns 204."""
        jurisdiction = _mock_jurisdiction()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = jurisdiction
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
        resp = client.delete(
            f"/api/v1/jurisdictions/{jurisdiction.jurisdiction_id}",
        )
        assert resp.status_code == 204
        mock_session.delete.assert_awaited_once_with(jurisdiction)

    def test_not_found(self):
        """Test 404 when jurisdiction doesn't exist."""
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
        resp = client.delete(f"/api/v1/jurisdictions/{uuid4()}")
        assert resp.status_code == 404
