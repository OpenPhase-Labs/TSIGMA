from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI, status
from fastapi.testclient import TestClient

from tests._helpers import make_mock_session
from tsigma.api.v1.areas import router as areas_router
from tsigma.api.v1.signals import router as signals_router
from tsigma.auth.dependencies import _get_db_session, get_current_user_optional, require_admin
from tsigma.auth.sessions import SessionData
from tsigma.dependencies import get_audited_session, get_session


def _admin():
    return SessionData(
        user_id=uuid4(), username='admin', role='admin',
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _mk_app(router, prefix):
    app = FastAPI()
    app.include_router(router, prefix=prefix)
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


def _scalar(value):
    r = MagicMock()
    r.scalar_one_or_none.return_value = value
    return r


def _scalars(items):
    r = MagicMock()
    r.scalars.return_value.all.return_value = items
    return r


areas_app = _mk_app(areas_router, '/api/v1/areas')
signals_app = _mk_app(signals_router, '/api/v1/signals')


def test_create_area():
    mock_session = make_mock_session()
    created = []
    mock_session.add = MagicMock(side_effect=created.append)

    async def fake_flush():
        # Simulate the DB assigning the server-default UUID on flush.
        for obj in created:
            if getattr(obj, 'area_id', None) is None:
                obj.area_id = uuid4()

    mock_session.flush = fake_flush
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.post('/api/v1/areas/', json={'name': 'Downtown'})
    assert res.status_code == status.HTTP_201_CREATED
    assert res.json()['name'] == 'Downtown'


def test_get_area_200():
    mock_session = make_mock_session()
    area_mock = MagicMock(area_id=uuid4(), description='Desc')
    area_mock.name = 'Test'  # 'name' is a reserved MagicMock ctor kwarg; set it explicitly
    mock_session.execute = AsyncMock(return_value=_scalar(area_mock))
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.get(f'/api/v1/areas/{area_mock.area_id}')
    assert res.status_code == status.HTTP_200_OK
    assert res.json()['name'] == 'Test'


def test_get_area_404():
    mock_session = make_mock_session()
    mock_session.execute = AsyncMock(return_value=_scalar(None))
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.get(f'/api/v1/areas/{uuid4()}')
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_update_area():
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    area_mock = MagicMock(area_id=uuid4(), name='Old', description='Desc')
    mock_session.execute = AsyncMock(return_value=_scalar(area_mock))
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.put(f'/api/v1/areas/{area_mock.area_id}', json={'name': 'New'})
    assert res.status_code == status.HTTP_200_OK
    assert res.json()['name'] == 'New'


def test_delete_area():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    area_mock = MagicMock(area_id=uuid4(), name='Test', description='Desc')
    mock_session.execute = AsyncMock(return_value=_scalar(area_mock))
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.delete(f'/api/v1/areas/{area_mock.area_id}')
    assert res.status_code == status.HTTP_204_NO_CONTENT


def test_get_area_signals():
    mock_session = make_mock_session()
    area_mock = MagicMock(area_id=uuid4(), name='Test', description='Desc')
    sig1 = MagicMock(signal_id='SIG-1')
    sig2 = MagicMock(signal_id='SIG-2')
    mock_session.execute = AsyncMock(side_effect=[_scalar(area_mock), _scalars([sig1, sig2])])
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.get(f'/api/v1/areas/{area_mock.area_id}/signals')
    assert res.status_code == status.HTTP_200_OK
    assert res.json() == [{'signal_id': 'SIG-1'}, {'signal_id': 'SIG-2'}]


def test_get_area_signals_404():
    mock_session = make_mock_session()
    mock_session.execute = AsyncMock(side_effect=[_scalar(None)])
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.get(f'/api/v1/areas/{uuid4()}/signals')
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_post_area_signal():
    mock_session = make_mock_session()
    mock_session.flush = AsyncMock()
    area_mock = MagicMock(area_id=uuid4(), name='Test', description='Desc')
    signal_mock = MagicMock(signal_id='SIG-1')
    mock_session.execute = AsyncMock(side_effect=[_scalar(area_mock), _scalar(signal_mock)])
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.post(f'/api/v1/areas/{area_mock.area_id}/signals', json={'signal_id': 'SIG-1'})
    assert res.status_code == status.HTTP_201_CREATED
    assert res.json() == {'signal_id': 'SIG-1'}


def test_post_area_signal_signal_missing():
    mock_session = make_mock_session()
    area_mock = MagicMock(area_id=uuid4(), name='Test', description='Desc')
    mock_session.execute = AsyncMock(side_effect=[_scalar(area_mock), _scalar(None)])
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.post(f'/api/v1/areas/{area_mock.area_id}/signals', json={'signal_id': 'SIG-1'})
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_delete_area_signal():
    mock_session = make_mock_session()
    mock_session.delete = AsyncMock()
    sa_mock = MagicMock()
    mock_session.execute = AsyncMock(return_value=_scalar(sa_mock))
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.delete(f'/api/v1/areas/{uuid4()}/signals/SIG-1')
    assert res.status_code == status.HTTP_204_NO_CONTENT


def test_delete_area_signal_404():
    mock_session = make_mock_session()
    mock_session.execute = AsyncMock(return_value=_scalar(None))
    app = areas_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.delete(f'/api/v1/areas/{uuid4()}/signals/SIG-1')
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_get_signal_areas():
    mock_session = make_mock_session()
    signal_mock = MagicMock(signal_id='SIG-1')
    area_mock = MagicMock(area_id=uuid4(), description=None)
    area_mock.name = 'A1'  # 'name' is a reserved MagicMock ctor kwarg; set it explicitly
    mock_session.execute = AsyncMock(side_effect=[_scalar(signal_mock), _scalars([area_mock])])
    app = signals_app
    _overrides(app, mock_session)
    client = TestClient(app)
    res = client.get('/api/v1/signals/SIG-1/areas')
    assert res.status_code == status.HTTP_200_OK
    assert len(res.json()) == 1
    assert res.json()[0]['name'] == 'A1'
