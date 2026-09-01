from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.metric_comments import router as metric_comments_router
from tsigma.auth.dependencies import _get_db_session, get_current_user_optional
from tsigma.dependencies import get_session
from tsigma.settings_service import settings_cache


def test_anonymous_get_metric_comments_returns_200_when_policy_is_public():
    app = FastAPI()
    app.include_router(metric_comments_router, prefix='/api/v1/metric-comments')

    policy_rows = [SimpleNamespace(key='access_policy.comments', value='public')]

    app.dependency_overrides[get_current_user_optional] = lambda: None

    async def mock_policy_db():
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = policy_rows
        mock_session.execute = AsyncMock(return_value=mock_result)
        yield mock_session
    app.dependency_overrides[_get_db_session] = mock_policy_db

    async def mock_list_db():
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=mock_result)
        yield mock_session
    app.dependency_overrides[get_session] = mock_list_db

    settings_cache.invalidate()

    client = TestClient(app)
    response = client.get('/api/v1/metric-comments/')

    assert response.status_code == 200


def test_anonymous_get_metric_comments_returns_401_when_policy_is_authenticated():
    app = FastAPI()
    app.include_router(metric_comments_router, prefix='/api/v1/metric-comments')

    policy_rows = [SimpleNamespace(key='access_policy.comments', value='authenticated')]

    app.dependency_overrides[get_current_user_optional] = lambda: None

    async def mock_policy_db():
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = policy_rows
        mock_session.execute = AsyncMock(return_value=mock_result)
        yield mock_session
    app.dependency_overrides[_get_db_session] = mock_policy_db

    async def mock_list_db():
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=mock_result)
        yield mock_session
    app.dependency_overrides[get_session] = mock_list_db

    settings_cache.invalidate()

    client = TestClient(app)
    response = client.get('/api/v1/metric-comments/')

    assert response.status_code == 401
