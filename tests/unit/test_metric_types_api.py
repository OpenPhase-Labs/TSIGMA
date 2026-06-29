from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI, status
from fastapi.testclient import TestClient

from tests._helpers import make_mock_session
from tsigma.api.v1.metric_types import router as metric_types_router
from tsigma.auth.dependencies import _get_db_session, get_current_user_optional, require_admin
from tsigma.auth.sessions import SessionData
from tsigma.dependencies import get_session


def _admin_session():
    return SessionData(
        user_id=uuid4(), username='admin', role='admin',
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _add_overrides(app, mock_session):
    app.dependency_overrides[get_current_user_optional] = _admin_session
    app.dependency_overrides[require_admin] = _admin_session

    async def _mock_access_db():
        mock = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock.execute = AsyncMock(return_value=result)
        yield mock

    app.dependency_overrides[_get_db_session] = _mock_access_db

    async def _override_session():
        yield mock_session

    app.dependency_overrides[get_session] = _override_session


def test_get_metric_types_list():
    app = FastAPI()
    app.include_router(metric_types_router, prefix='/api/v1/metric-types', tags=['metric-types'])
    mock_session = make_mock_session()
    _add_overrides(app, mock_session)

    row1 = MagicMock(key='m1', label='Metric 1', display_order=1)
    row2 = MagicMock(key='m2', label='Metric 2', display_order=2)
    result = MagicMock()
    result.scalars.return_value.all.return_value = [row1, row2]
    mock_session.execute = AsyncMock(return_value=result)

    client = TestClient(app)
    resp = client.get('/api/v1/metric-types/')
    assert resp.status_code == status.HTTP_200_OK
    data = resp.json()
    assert len(data) == 2
    assert data[0]['key'] == 'm1'
    assert data[0]['label'] == 'Metric 1'
    assert data[0]['display_order'] == 1
    assert data[1]['key'] == 'm2'
    assert data[1]['label'] == 'Metric 2'
    assert data[1]['display_order'] == 2


def test_post_create_metric_type_valid_key():
    app = FastAPI()
    app.include_router(metric_types_router, prefix='/api/v1/metric-types', tags=['metric-types'])
    mock_session = make_mock_session()
    _add_overrides(app, mock_session)

    new_row = MagicMock(key='good-key', label='New Metric', display_order=10)
    result = MagicMock()
    result.scalar_one_or_none.return_value = new_row
    mock_session.execute = AsyncMock(return_value=result)
    mock_session.flush = AsyncMock()

    with patch('tsigma.api.v1.metric_types.ReportRegistry.list_all', return_value={'good-key': object()}):
        client = TestClient(app)
        resp = client.post(
            '/api/v1/metric-types/',
            json={'key': 'good-key', 'label': 'New Metric', 'display_order': 10},
        )
        assert resp.status_code == status.HTTP_201_CREATED
        assert resp.json()['key'] == 'good-key'


def test_post_create_metric_type_invalid_key():
    app = FastAPI()
    app.include_router(metric_types_router, prefix='/api/v1/metric-types', tags=['metric-types'])
    mock_session = make_mock_session()
    _add_overrides(app, mock_session)

    with patch('tsigma.api.v1.metric_types.ReportRegistry.list_all', return_value={'other-key': object()}):
        client = TestClient(app)
        resp = client.post(
            '/api/v1/metric-types/',
            json={'key': 'bogus', 'label': 'Bad Metric', 'display_order': 1},
        )
        assert resp.status_code == 422


def test_put_update_metric_type():
    app = FastAPI()
    app.include_router(metric_types_router, prefix='/api/v1/metric-types', tags=['metric-types'])
    mock_session = make_mock_session()
    _add_overrides(app, mock_session)

    existing_row = MagicMock(key='m1', label='Old', display_order=1)
    result = MagicMock()
    result.scalar_one_or_none.return_value = existing_row
    mock_session.execute = AsyncMock(return_value=result)
    mock_session.flush = AsyncMock()

    client = TestClient(app)
    resp = client.put('/api/v1/metric-types/m1', json={'label': 'Updated', 'display_order': 5})
    assert resp.status_code == status.HTTP_200_OK
    data = resp.json()
    assert data['key'] == 'm1'
    assert data['label'] == 'Updated'
    assert data['display_order'] == 5


def test_delete_metric_type():
    app = FastAPI()
    app.include_router(metric_types_router, prefix='/api/v1/metric-types', tags=['metric-types'])
    mock_session = make_mock_session()
    _add_overrides(app, mock_session)

    existing_row = MagicMock(key='m1', label='Old', display_order=1)
    result = MagicMock()
    result.scalar_one_or_none.return_value = existing_row
    mock_session.execute = AsyncMock(return_value=result)
    mock_session.delete = AsyncMock()
    mock_session.flush = AsyncMock()

    client = TestClient(app)
    resp = client.delete('/api/v1/metric-types/m1')
    assert resp.status_code == status.HTTP_204_NO_CONTENT
