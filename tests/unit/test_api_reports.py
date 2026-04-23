"""
Unit tests for Reports API endpoints.

Tests list, execute, and export report operations.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.reports import router
from tsigma.auth.sessions import SessionData


def _create_test_app():
    """Create a minimal FastAPI app with the reports router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return app


def _add_access_overrides(app):
    """Override require_access sub-dependencies so endpoints pass auth."""
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


def _make_fake_report_cls(execute_result=None, export_result=None):
    """Create a fake report class for mocking ReportRegistry.

    ``execute_result`` may be a DataFrame (matches the real Report
    contract) or a dict / list which will be wrapped in a DataFrame.
    """
    if execute_result is None:
        execute_result = pd.DataFrame([{"col": "val"}])
    elif isinstance(execute_result, dict):
        execute_result = pd.DataFrame([execute_result])
    elif isinstance(execute_result, list):
        execute_result = pd.DataFrame(execute_result)

    cls = MagicMock()
    instance = MagicMock()
    instance.execute = AsyncMock(return_value=execute_result)
    instance.export = AsyncMock(return_value=export_result or b"csv,data\n1,2")
    cls.return_value = instance
    cls.description = "Test report"
    cls.category = "standard"
    cls.estimated_time = "fast"
    cls.export_formats = ["csv", "json"]
    # The real preferred_http_status hook returns None for "use 200";
    # MagicMock's default return is another MagicMock, which would
    # confuse the API handler's isinstance check.
    cls.preferred_http_status = MagicMock(return_value=None)
    return cls


class TestListReports:
    """Tests for GET /api/v1/reports."""

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_returns_reports(self, mock_registry):
        fake_cls = _make_fake_report_cls()
        mock_registry.list_all.return_value = {"test-report": fake_cls}

        app = _create_test_app()
        _add_access_overrides(app)

        client = TestClient(app)
        resp = client.get("/api/v1/reports")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "test-report"
        assert data[0]["description"] == "Test report"
        assert data[0]["category"] == "standard"

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_empty_list(self, mock_registry):
        mock_registry.list_all.return_value = {}

        app = _create_test_app()
        _add_access_overrides(app)

        client = TestClient(app)
        resp = client.get("/api/v1/reports")
        assert resp.status_code == 200
        assert resp.json() == []


class TestRunReport:
    """Tests for POST /api/v1/reports/{report_name}."""

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_execute_report(self, mock_registry):
        fake_cls = _make_fake_report_cls(execute_result={"metric": 42})
        mock_registry.get.return_value = fake_cls

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/reports/test-report",
            json={"signal_id": "abc"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "complete"
        assert data["data"] == [{"metric": 42}]

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_report_not_found(self, mock_registry):
        mock_registry.get.side_effect = ValueError("Unknown report: nope")

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post("/api/v1/reports/nope", json={})
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_missing_param_returns_400(self, mock_registry):
        fake_cls = MagicMock()
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=KeyError("start"))
        fake_cls.return_value = instance
        mock_registry.get.return_value = fake_cls

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post("/api/v1/reports/test-report", json={})
        assert resp.status_code == 400
        assert "Missing required parameter" in resp.json()["detail"]

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_internal_error_returns_500(self, mock_registry):
        fake_cls = MagicMock()
        instance = MagicMock()
        instance.execute = AsyncMock(side_effect=RuntimeError("boom"))
        fake_cls.return_value = instance
        mock_registry.get.return_value = fake_cls

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post("/api/v1/reports/test-report", json={})
        assert resp.status_code == 500


class TestExportReport:
    """Tests for POST /api/v1/reports/{report_name}/export."""

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_export_csv(self, mock_registry):
        fake_cls = _make_fake_report_cls(export_result=b"a,b\n1,2")
        mock_registry.get.return_value = fake_cls

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/reports/test-report/export?format=csv",
            json={"signal_id": "abc"},
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        assert "Content-Disposition" in resp.headers
        assert resp.content == b"a,b\n1,2"

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_export_json(self, mock_registry):
        fake_cls = _make_fake_report_cls(export_result=b'{"key":"val"}')
        mock_registry.get.return_value = fake_cls

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/reports/test-report/export?format=json",
            json={},
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/json"

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_export_not_found(self, mock_registry):
        mock_registry.get.side_effect = ValueError("Unknown report")

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post("/api/v1/reports/nope/export", json={})
        assert resp.status_code == 404

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_export_missing_param_returns_400(self, mock_registry):
        """Test export with missing required param returns 400 (line 126)."""
        fake_cls = MagicMock()
        instance = MagicMock()
        instance.export = AsyncMock(side_effect=KeyError("signal_id"))
        fake_cls.return_value = instance
        mock_registry.get.return_value = fake_cls

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post("/api/v1/reports/test-report/export", json={})
        assert resp.status_code == 400
        assert "Missing required parameter" in resp.json()["detail"]

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_export_internal_error_returns_500(self, mock_registry):
        """Test export with unexpected exception returns 500 (lines 129-131)."""
        fake_cls = MagicMock()
        instance = MagicMock()
        instance.export = AsyncMock(side_effect=RuntimeError("disk full"))
        fake_cls.return_value = instance
        mock_registry.get.return_value = fake_cls

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post("/api/v1/reports/test-report/export", json={})
        assert resp.status_code == 500
        assert "export failed" in resp.json()["detail"].lower()

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_export_bad_format_returns_400(self, mock_registry):
        fake_cls = MagicMock()
        instance = MagicMock()
        instance.export = AsyncMock(side_effect=ValueError("Unsupported format: xml"))
        fake_cls.return_value = instance
        mock_registry.get.return_value = fake_cls

        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/reports/test-report/export?format=xml",
            json={},
        )
        assert resp.status_code == 400
