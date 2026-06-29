from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.areas import router as areas_router
from tsigma.api.v1.corridors import router as corridors_router
from tsigma.api.v1.jurisdictions import router as jurisdictions_router
from tsigma.api.v1.regions import router as regions_router
from tsigma.auth.dependencies import _get_db_session, get_current_user_optional
from tsigma.auth.sessions import SessionData
from tsigma.dependencies import get_session


def _mock_scalars_result(rows):
    mock_scalars = MagicMock()
    mock_scalars.all = MagicMock(return_value=rows)
    mock_result = MagicMock()
    mock_result.scalars = MagicMock(return_value=mock_scalars)
    return mock_result


def _add_access_overrides(app):
    app.dependency_overrides[get_current_user_optional] = lambda: SessionData(
        user_id=uuid4(),
        username="viewer",
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

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(return_value=_mock_scalars_result([]))
    app.dependency_overrides[get_session] = lambda: mock_session


def _create_lookup_app(router, prefix):
    app = FastAPI()
    app.include_router(router, prefix=prefix)
    return app


# --- Jurisdictions ---

def test_jurisdictions_lookup_returns_200():
    app = _create_lookup_app(jurisdictions_router, "/api/v1/jurisdictions")
    _add_access_overrides(app)
    client = TestClient(app)
    res = client.get("/api/v1/jurisdictions/lookup")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_jurisdictions_lookup_maps_rows():
    app = _create_lookup_app(jurisdictions_router, "/api/v1/jurisdictions")
    _add_access_overrides(app)
    mock_session = app.dependency_overrides[get_session]()
    row1 = MagicMock()
    row1.jurisdiction_id = "id-1"
    row1.name = "Name 1"
    row2 = MagicMock()
    row2.jurisdiction_id = "id-2"
    row2.name = "Name 2"
    mock_session.execute.return_value = _mock_scalars_result([row1, row2])

    client = TestClient(app)
    res = client.get("/api/v1/jurisdictions/lookup")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 2
    assert data[0] == {"id": "id-1", "label": "Name 1"}
    assert data[1] == {"id": "id-2", "label": "Name 2"}


# --- Regions ---

def test_regions_lookup_returns_200():
    app = _create_lookup_app(regions_router, "/api/v1/regions")
    _add_access_overrides(app)
    client = TestClient(app)
    res = client.get("/api/v1/regions/lookup")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_regions_lookup_maps_rows_with_parent():
    app = _create_lookup_app(regions_router, "/api/v1/regions")
    _add_access_overrides(app)
    mock_session = app.dependency_overrides[get_session]()
    row1 = MagicMock()
    row1.region_id = "rid-1"
    row1.description = "Desc 1"
    row1.parent_region_id = "pid-1"
    row2 = MagicMock()
    row2.region_id = "rid-2"
    row2.description = "Desc 2"
    row2.parent_region_id = None
    mock_session.execute.return_value = _mock_scalars_result([row1, row2])

    client = TestClient(app)
    res = client.get("/api/v1/regions/lookup")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 2
    assert data[0] == {"id": "rid-1", "label": "Desc 1", "parent_id": "pid-1"}
    assert data[1] == {"id": "rid-2", "label": "Desc 2", "parent_id": None}


# --- Corridors ---

def test_corridors_lookup_returns_200():
    app = _create_lookup_app(corridors_router, "/api/v1/corridors")
    _add_access_overrides(app)
    client = TestClient(app)
    res = client.get("/api/v1/corridors/lookup")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_corridors_lookup_maps_rows():
    app = _create_lookup_app(corridors_router, "/api/v1/corridors")
    _add_access_overrides(app)
    mock_session = app.dependency_overrides[get_session]()
    row1 = MagicMock()
    row1.corridor_id = "cid-1"
    row1.name = "Corridor 1"
    row2 = MagicMock()
    row2.corridor_id = "cid-2"
    row2.name = "Corridor 2"
    mock_session.execute.return_value = _mock_scalars_result([row1, row2])

    client = TestClient(app)
    res = client.get("/api/v1/corridors/lookup")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 2
    assert data[0] == {"id": "cid-1", "label": "Corridor 1"}
    assert data[1] == {"id": "cid-2", "label": "Corridor 2"}


# --- Areas ---

def test_areas_lookup_returns_200():
    app = _create_lookup_app(areas_router, "/api/v1/areas")
    _add_access_overrides(app)
    client = TestClient(app)
    res = client.get("/api/v1/areas/lookup")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_areas_lookup_maps_rows():
    app = _create_lookup_app(areas_router, "/api/v1/areas")
    _add_access_overrides(app)
    mock_session = app.dependency_overrides[get_session]()
    row1 = MagicMock()
    row1.area_id = "aid-1"
    row1.name = "Area 1"
    row2 = MagicMock()
    row2.area_id = "aid-2"
    row2.name = "Area 2"
    mock_session.execute.return_value = _mock_scalars_result([row1, row2])

    client = TestClient(app)
    res = client.get("/api/v1/areas/lookup")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 2
    assert data[0] == {"id": "aid-1", "label": "Area 1"}
    assert data[1] == {"id": "aid-2", "label": "Area 2"}
