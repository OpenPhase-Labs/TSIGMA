"""
Unit tests for TSIGMA UI routes.

Tests server-rendered page routes return proper HTTP responses
with HTML content types. Authentication dependencies are overridden
so we can test the template rendering in isolation.

The base.html template references get_flashed_messages which is not
registered as a Jinja global in this test context, so we patch
TemplateResponse to return simple HTML.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient
from starlette.responses import HTMLResponse

from tsigma.app import create_app
from tsigma.auth.dependencies import _get_db_session, get_current_user_optional
from tsigma.auth.sessions import SessionData


def _make_client():
    """Create a TestClient with auth dependencies overridden."""
    app = create_app()

    # Override auth sub-dependencies so routes pass authentication
    app.dependency_overrides[get_current_user_optional] = lambda: SessionData(
        user_id=uuid4(),
        username="tester",
        role="admin",
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

    return TestClient(app, raise_server_exceptions=False)


def _patch_templates():
    """Patch TemplateResponse to skip Jinja rendering (avoids missing globals)."""
    def fake_template_response(template_name, context, **kwargs):
        return HTMLResponse(content=f"<html>{template_name}</html>")
    return patch("tsigma.api.ui.templates.TemplateResponse", side_effect=fake_template_response)


# ---------------------------------------------------------------------------
# Public routes
# ---------------------------------------------------------------------------

class TestLoginPage:
    def test_login_page_returns_html(self):
        with _patch_templates():
            client = _make_client()
            resp = client.get("/login")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


# ---------------------------------------------------------------------------
# Authenticated routes
# ---------------------------------------------------------------------------

class TestDashboard:
    def test_dashboard_returns_html(self):
        with _patch_templates():
            client = _make_client()
            resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


class TestSignalsPages:
    def test_signals_list_returns_html(self):
        with _patch_templates():
            client = _make_client()
            resp = client.get("/signals")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_signal_detail_returns_html(self):
        with _patch_templates():
            client = _make_client()
            resp = client.get("/signals/SIG-001")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


class TestReportsPages:
    def test_reports_list_returns_html(self):
        with _patch_templates():
            client = _make_client()
            resp = client.get("/reports")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_report_viewer_returns_html(self):
        with _patch_templates():
            client = _make_client()
            resp = client.get("/reports/left-turn-gap?signal_id=SIG-001")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

class TestAdminPages:
    def test_admin_users_returns_html(self):
        with _patch_templates():
            client = _make_client()
            resp = client.get("/admin/users")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_admin_settings_returns_html(self):
        with _patch_templates():
            client = _make_client()
            resp = client.get("/admin/settings")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
