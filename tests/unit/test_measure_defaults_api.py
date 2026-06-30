"""
Unit tests for the Measure Defaults API endpoints (Phase C).

Covers admin CRUD on the ``measure_default`` table plus a read endpoint
exposing the effective (model-default overlaid with admin-default) tunable
params per report. These are pure unit tests: a bare FastAPI app mounts the
``measure_defaults`` router, ``ReportRegistry`` is patched, and auth + session
dependencies are overridden so no live DB is touched.

The router module does not exist yet; these tests are RED by design.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from tests._helpers import make_mock_session
from tsigma.auth.sessions import SessionData
from tsigma.reports.registry import Report


# ---------------------------------------------------------------------------
# A small REAL report with a typed params model, used to exercise param
# validation, structural exclusion, and effective-default overlay.
# ---------------------------------------------------------------------------
class _TParams(BaseModel):
    """Params model for the fake report under test."""

    # Structural - always per-request, never admin-defaultable.
    signal_id: str
    start: str
    end: str
    # Tunables - admin-defaultable and surfaced in effective-defaults.
    green_occ_threshold: float = 0.79
    bin_size: str = "15min"


class _FakeReport(Report[_TParams]):
    """Minimal concrete Report parameterised by ``_TParams``."""

    async def execute(self, params, session):  # pragma: no cover - unused
        raise NotImplementedError


def _create_test_app():
    """Create a minimal FastAPI app with the measure_defaults router."""
    from tsigma.api.v1.measure_defaults import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return app


def _session_data(role: str) -> SessionData:
    return SessionData(
        user_id=uuid4(),
        username="testuser",
        role=role,
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _add_admin_overrides(app):
    """Override auth so require_admin and require_access both pass."""
    from tsigma.auth.dependencies import _get_db_session, get_current_user_optional

    app.dependency_overrides[get_current_user_optional] = lambda: _session_data("admin")

    async def _mock_access_db():
        mock = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock.execute = AsyncMock(return_value=result)
        yield mock

    app.dependency_overrides[_get_db_session] = _mock_access_db


def _add_session_override(app, mock_session):
    """Override get_session to yield the supplied mock session."""
    from tsigma.dependencies import get_session

    async def override_session():
        yield mock_session

    app.dependency_overrides[get_session] = override_session


# ---------------------------------------------------------------------------
# POST /api/v1/measure-defaults
# ---------------------------------------------------------------------------
class TestUpsertMeasureDefault:
    """Tests for POST /api/v1/measure-defaults (require_admin)."""

    @patch("tsigma.api.v1.measure_defaults.ReportRegistry")
    def test_valid_tunable_upserts(self, mock_registry):
        mock_registry.get.return_value = _FakeReport

        mock_session = make_mock_session()
        # No existing row -> treat as insert.
        existing = MagicMock()
        existing.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=existing)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/measure-defaults",
            json={
                "report_name": "fake-report",
                "param_name": "green_occ_threshold",
                "value": 0.5,
            },
        )
        assert resp.status_code in (200, 201)
        body = resp.json()
        assert body["report_name"] == "fake-report"
        assert body["param_name"] == "green_occ_threshold"
        assert body["value"] == 0.5
        # The row must have been persisted via add or merge.
        assert mock_session.add.called or mock_session.merge.called

    @patch("tsigma.api.v1.measure_defaults.ReportRegistry")
    def test_unknown_report_returns_404(self, mock_registry):
        mock_registry.get.side_effect = ValueError("Unknown report: nope")

        mock_session = make_mock_session()
        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/measure-defaults",
            json={
                "report_name": "nope",
                "param_name": "green_occ_threshold",
                "value": 0.5,
            },
        )
        assert resp.status_code == 404

    @patch("tsigma.api.v1.measure_defaults.ReportRegistry")
    def test_unknown_param_returns_400(self, mock_registry):
        mock_registry.get.return_value = _FakeReport

        mock_session = make_mock_session()
        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/measure-defaults",
            json={
                "report_name": "fake-report",
                "param_name": "nope",
                "value": 1,
            },
        )
        assert resp.status_code == 400

    @patch("tsigma.api.v1.measure_defaults.ReportRegistry")
    def test_structural_param_returns_400(self, mock_registry):
        mock_registry.get.return_value = _FakeReport

        mock_session = make_mock_session()
        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/measure-defaults",
            json={
                "report_name": "fake-report",
                "param_name": "signal_id",
                "value": "1234",
            },
        )
        assert resp.status_code == 400

    @patch("tsigma.api.v1.measure_defaults.ReportRegistry")
    def test_wrong_typed_value_returns_463(self, mock_registry):
        mock_registry.get.return_value = _FakeReport

        mock_session = make_mock_session()
        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/measure-defaults",
            json={
                "report_name": "fake-report",
                "param_name": "green_occ_threshold",
                "value": "abc",
            },
        )
        assert resp.status_code == 463
        detail = resp.json()["detail"]
        assert detail["error"] == "invalid_report_params"
        assert isinstance(detail["failures"], list)
        assert len(detail["failures"]) >= 1


# ---------------------------------------------------------------------------
# DELETE /api/v1/measure-defaults/{report_name}/{param_name}
# ---------------------------------------------------------------------------
class TestDeleteMeasureDefault:
    """Tests for DELETE /api/v1/measure-defaults/{report}/{param}."""

    def test_delete_existing_returns_204(self):
        mock_session = make_mock_session()
        existing = MagicMock()
        row = MagicMock()
        existing.scalar_one_or_none.return_value = row
        mock_session.execute = AsyncMock(return_value=existing)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.delete(
            "/api/v1/measure-defaults/fake-report/green_occ_threshold"
        )
        assert resp.status_code == 204

    def test_delete_missing_returns_404(self):
        mock_session = make_mock_session()
        existing = MagicMock()
        existing.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=existing)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.delete(
            "/api/v1/measure-defaults/fake-report/green_occ_threshold"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/v1/measure-defaults
# ---------------------------------------------------------------------------
class TestListMeasureDefaults:
    """Tests for GET /api/v1/measure-defaults (require_access reports)."""

    def test_list_returns_rows(self):
        mock_session = make_mock_session()
        row1 = MagicMock()
        row1.report_name = "fake-report"
        row1.param_name = "green_occ_threshold"
        row1.value = 0.5
        row2 = MagicMock()
        row2.report_name = "fake-report"
        row2.param_name = "bin_size"
        row2.value = "30min"

        result = MagicMock()
        result.scalars.return_value.all.return_value = [row1, row2]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/measure-defaults")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 2
        names = {d["param_name"] for d in data}
        assert names == {"green_occ_threshold", "bin_size"}
        by_name = {d["param_name"]: d for d in data}
        assert by_name["green_occ_threshold"]["report_name"] == "fake-report"
        assert by_name["green_occ_threshold"]["value"] == 0.5


# ---------------------------------------------------------------------------
# GET /api/v1/reports/{report_name}/effective-defaults
# ---------------------------------------------------------------------------
class TestEffectiveDefaults:
    """Tests for GET /api/v1/reports/{report_name}/effective-defaults."""

    @patch("tsigma.api.v1.measure_defaults.ReportRegistry")
    def test_effective_defaults_overlay(self, mock_registry):
        mock_registry.get.return_value = _FakeReport

        mock_session = make_mock_session()
        # One admin row overriding green_occ_threshold.
        row = MagicMock()
        row.report_name = "fake-report"
        row.param_name = "green_occ_threshold"
        row.value = 0.5
        result = MagicMock()
        result.scalars.return_value.all.return_value = [row]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/reports/fake-report/effective-defaults")
        assert resp.status_code == 200
        body = resp.json()
        assert body["report_name"] == "fake-report"
        eff = body["effective_defaults"]
        # Admin overlay wins for green_occ_threshold.
        assert eff["green_occ_threshold"] == 0.5
        # Untouched tunable falls back to the model field default.
        assert eff["bin_size"] == "15min"
        # Structural params are excluded entirely.
        assert "signal_id" not in eff
        assert "start" not in eff
        assert "end" not in eff

    @patch("tsigma.api.v1.measure_defaults.ReportRegistry")
    def test_effective_defaults_unknown_report_returns_404(self, mock_registry):
        mock_registry.get.side_effect = ValueError("Unknown report: nope")

        mock_session = make_mock_session()
        app = _create_test_app()
        _add_admin_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/reports/nope/effective-defaults")
        assert resp.status_code == 404
