from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI, status
from fastapi.testclient import TestClient

from tests._helpers import make_mock_session
from tsigma.api.v1.jurisdictions import router as jurisdictions_router
from tsigma.auth.dependencies import _get_db_session, get_current_user_optional, require_admin
from tsigma.auth.sessions import SessionData
from tsigma.dependencies import get_audited_session, get_session
from tsigma.models import Jurisdiction


def _admin():
    return SessionData(
        user_id=uuid4(), username='admin', role='admin',
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _app():
    app = FastAPI()
    app.include_router(jurisdictions_router, prefix='/api/v1/jurisdictions')
    return app


def _overrides(app, mock_session):
    app.dependency_overrides[get_current_user_optional] = _admin
    app.dependency_overrides[require_admin] = _admin

    async def _access_db():
        m = AsyncMock()
        r = MagicMock()
        r.scalars.return_value.all.return_value = []
        m.execute = AsyncMock(return_value=r)
        yield m

    app.dependency_overrides[_get_db_session] = _access_db

    async def _sess():
        yield mock_session

    app.dependency_overrides[get_session] = _sess
    app.dependency_overrides[get_audited_session] = _sess


def test_create_with_other_partners():
    app = _app()
    mock_session = make_mock_session()
    _overrides(app, mock_session)

    created = []
    mock_session.add = MagicMock(side_effect=created.append)

    async def fake_flush():
        for o in created:
            if getattr(o, 'jurisdiction_id', None) is None:
                setattr(o, 'jurisdiction_id', uuid4())
    mock_session.flush = fake_flush

    client = TestClient(app)
    resp = client.post('/api/v1/jurisdictions/', json={'name': 'City', 'other_partners': 'GDOT, ARC'})

    assert resp.status_code == status.HTTP_201_CREATED
    assert resp.json()['other_partners'] == 'GDOT, ARC'


def test_create_without_other_partners_is_null():
    app = _app()
    mock_session = make_mock_session()
    _overrides(app, mock_session)

    created = []
    mock_session.add = MagicMock(side_effect=created.append)

    async def fake_flush():
        for o in created:
            if getattr(o, 'jurisdiction_id', None) is None:
                setattr(o, 'jurisdiction_id', uuid4())
    mock_session.flush = fake_flush

    client = TestClient(app)
    resp = client.post('/api/v1/jurisdictions/', json={'name': 'City'})

    assert resp.status_code == status.HTTP_201_CREATED
    assert resp.json()['other_partners'] is None


def test_update_other_partners():
    app = _app()
    mock_session = make_mock_session()
    _overrides(app, mock_session)

    j = MagicMock(spec=Jurisdiction)
    setattr(j, 'jurisdiction_id', uuid4())
    setattr(j, 'name', 'City')
    setattr(j, 'mpo_name', None)
    setattr(j, 'county_name', None)
    setattr(j, 'other_partners', None)

    result = MagicMock()
    result.scalar_one_or_none.return_value = j
    mock_session.execute = AsyncMock(return_value=result)
    mock_session.flush = AsyncMock()

    client = TestClient(app)
    resp = client.put(f'/api/v1/jurisdictions/{j.jurisdiction_id}', json={'other_partners': 'New Partner'})

    assert resp.status_code == status.HTTP_200_OK
    assert j.other_partners == 'New Partner'
    assert resp.json()['other_partners'] == 'New Partner'
