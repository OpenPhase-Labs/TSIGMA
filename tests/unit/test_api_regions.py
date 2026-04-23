"""
Unit tests for Regions API endpoints.

Tests list, get by ID, and create operations for regional hierarchy.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.regions import router
from tsigma.auth.sessions import SessionData
from tsigma.models import Region


def _create_test_app():
    """Create a minimal FastAPI app with the regions router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/regions")
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


def _mock_region(**overrides):
    """Create a mock Region ORM object."""
    defaults = {
        "region_id": uuid4(),
        "parent_region_id": None,
        "description": "District 7",
    }
    defaults.update(overrides)
    mock = MagicMock(spec=Region)
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


class TestListRegions:
    """Tests for GET /api/v1/regions/."""

    def test_returns_regions(self):
        region = _mock_region()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = [region]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/regions/")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["description"] == "District 7"

    def test_empty_list(self):
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/regions/")
        assert resp.status_code == 200
        assert resp.json() == []


class TestGetRegion:
    """Tests for GET /api/v1/regions/{region_id}."""

    def test_returns_region(self):
        region = _mock_region()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = region
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get(f"/api/v1/regions/{region.region_id}")
        assert resp.status_code == 200
        assert resp.json()["description"] == "District 7"

    def test_not_found(self):
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get(f"/api/v1/regions/{uuid4()}")
        assert resp.status_code == 404


class TestCreateRegion:
    """Tests for POST /api/v1/regions/."""

    def test_creates_region(self):
        expected_id = uuid4()
        added_objects = []
        mock_session = AsyncMock()
        mock_session.add = MagicMock(side_effect=lambda obj: added_objects.append(obj))

        async def fake_flush():
            for obj in added_objects:
                if hasattr(obj, "region_id") and obj.region_id is None:
                    object.__setattr__(obj, "region_id", expected_id)

        mock_session.flush = fake_flush

        # No parent lookup needed when parent_region_id is None
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
            "/api/v1/regions/",
            json={"description": "District 7"},
        )
        assert resp.status_code == 201
        assert resp.json()["description"] == "District 7"

    def test_description_required(self):
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
        resp = client.post("/api/v1/regions/", json={})
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


class TestUpdateRegion:
    """Tests for PUT /api/v1/regions/{region_id}."""

    def test_update_region(self):
        """PUT /regions/{id} returns updated region data."""
        region = _mock_region(description="Old Name")

        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = region
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/regions/{region.region_id}",
            json={"description": "New Name"},
        )
        assert resp.status_code == 200
        assert resp.json()["description"] == "New Name"

    def test_update_region_not_found(self):
        """PUT /regions/{id} returns 404 for unknown region."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/regions/{uuid4()}",
            json={"description": "Updated"},
        )
        assert resp.status_code == 404


class TestDeleteRegion:
    """Tests for DELETE /api/v1/regions/{region_id} (via crud_factory)."""

    def test_delete_region(self):
        """DELETE /regions/{id} returns 204 on success."""
        region = _mock_region()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = region
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.delete = AsyncMock()
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_audited_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.delete(f"/api/v1/regions/{region.region_id}")
        assert resp.status_code == 204

    def test_delete_region_not_found(self):
        """DELETE /regions/{id} returns 404 for unknown region."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_audited_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.delete(f"/api/v1/regions/{uuid4()}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# RegionUpdate validation (line 41)
# ---------------------------------------------------------------------------


class TestRegionUpdateValidation:
    """Tests for RegionUpdate at-least-one-field validator (line 40-41)."""

    def test_update_with_all_none_raises(self):
        """RegionUpdate with all None fields raises ValueError."""
        import pytest
        from pydantic import ValidationError

        from tsigma.api.v1.regions import RegionUpdate

        with pytest.raises(ValidationError, match="At least one field"):
            RegionUpdate(description=None, parent_region_id=None)


# ---------------------------------------------------------------------------
# List regions with parent filter (line 83)
# ---------------------------------------------------------------------------


class TestListRegionsWithParentFilter:
    """Tests for GET /regions/?parent_id= filter (line 82-83)."""

    def test_list_regions_with_parent_filter(self):
        """GET /regions/?parent_id=<id> filters by parent."""
        parent_id = uuid4()
        child_region = _mock_region(parent_region_id=parent_id, description="Zone A")

        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = [child_region]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get(f"/api/v1/regions/?parent_id={parent_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["description"] == "Zone A"


# ---------------------------------------------------------------------------
# Create region with parent validation (lines 100-106)
# ---------------------------------------------------------------------------


class TestCreateRegionWithParent:
    """Tests for POST /regions/ with parent_region_id (lines 99-108)."""

    def test_create_region_with_nonexistent_parent(self):
        """POST /regions/ with invalid parent returns 404."""
        parent_id = uuid4()
        mock_session = AsyncMock()
        # parent lookup returns None
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
            "/api/v1/regions/",
            json={"description": "Child", "parent_region_id": str(parent_id)},
        )
        assert resp.status_code == 404
        assert "Parent region" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Update region with parent validation (lines 149-155)
# ---------------------------------------------------------------------------


class TestUpdateRegionWithParent:
    """Tests for PUT /regions/{id} with parent_region_id change (lines 149-155)."""

    def test_update_region_with_nonexistent_parent(self):
        """PUT /regions/{id} with invalid new parent returns 404."""
        region = _mock_region(description="Zone B")
        parent_id = uuid4()

        mock_session = AsyncMock()
        # First call: region lookup (found); second call: parent lookup (not found)
        result_region = MagicMock()
        result_region.scalar_one_or_none.return_value = region

        result_parent = MagicMock()
        result_parent.scalar_one_or_none.return_value = None

        mock_session.execute = AsyncMock(
            side_effect=[result_region, result_parent]
        )
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/regions/{region.region_id}",
            json={"parent_region_id": str(parent_id)},
        )
        assert resp.status_code == 404
        assert "Parent region" in resp.json()["detail"]

    def test_update_region_partial_description_only(self):
        """PUT /regions/{id} with only description updates description."""
        region = _mock_region(description="Old")

        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = region
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/regions/{region.region_id}",
            json={"description": "Updated"},
        )
        assert resp.status_code == 200
        assert resp.json()["description"] == "Updated"
