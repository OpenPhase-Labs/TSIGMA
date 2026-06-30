"""
Unit tests for Reports API params validation at the chokepoint.

The ``run_report`` / ``export_report`` endpoints must validate the raw
params dict into the report's declared Pydantic params model before
calling ``execute`` / ``export``. Reports do attribute access on params
(``params.signal_id``), so a raw dict 500s; these tests define the
contract for the validation behavior:

- Typed report -> dict is validated into the params model.
- Validation failure -> custom status 463 with a structured body.
- Untyped/legacy report -> raw dict passes through unchanged.
- ``preferred_http_status`` gating stays distinct from 463.
- ``_params_cls_for`` is hardened against non-real classes (MagicMock).
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from tests._helpers import make_mock_session
from tsigma.api.v1.reports import router
from tsigma.auth.sessions import SessionData
from tsigma.reports.registry import Report, ReportMetadata

# Custom status code the chokepoint returns for invalid report params.
HTTP_INVALID_REPORT_PARAMS = 463


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


def _report_session():
    """A request session whose measure_default read yields no admin rows.

    The report chokepoint now resolves admin defaults from the
    ``measure_default`` table via ``session.execute(...).scalars().all()``
    before validation. These tests exercise validation/gating with no admin
    defaults configured, so the read returns an empty list (code/model
    defaults apply).
    """
    session = make_mock_session()
    result = MagicMock()
    result.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result)
    return session


def _make_fake_report_cls(execute_result=None, export_result=None):
    """Create a fake (untyped/legacy) report class for mocking ReportRegistry.

    Mirrors the helper in ``test_api_reports.py``. Because this is a
    MagicMock (not a real ``Report[TParams]`` subclass), ``_params_cls_for``
    must return ``None`` for it, so the raw dict is passed through.
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
    cls.metadata.description = "Test report"
    cls.metadata.category = "standard"
    cls.metadata.estimated_time = "fast"
    cls.metadata.export_formats = ["csv", "json"]
    cls.preferred_http_status = MagicMock(return_value=None)
    return cls


class _TParams(BaseModel):
    """Real Pydantic params model used by the typed test report."""

    signal_id: str
    start: str
    end: str
    threshold: float = 0.5


class _TReport(Report[_TParams]):
    """Typed report that proves it received a validated model, not a dict."""

    metadata = ReportMetadata(
        name="typed-report",
        description="Typed test report",
        category="standard",
        estimated_time="fast",
    )

    async def execute(self, params, session):
        # Attribute access only succeeds on the validated model; a raw
        # dict would AttributeError here.
        assert hasattr(params, "signal_id")
        return pd.DataFrame([{"v": params.threshold}])


class _GatingParams(BaseModel):
    """Params for the gating report."""

    signal_id: str
    start: str
    end: str


class _GatingReport(Report[_GatingParams]):
    """Typed report that gates with HTTP 422 even on valid params."""

    metadata = ReportMetadata(
        name="gating-report",
        description="Gating test report",
        category="standard",
        estimated_time="fast",
    )

    async def execute(self, params, session):
        return pd.DataFrame([{"ready": False}])

    @classmethod
    def preferred_http_status(cls, result):
        return 422


_VALID_PARAMS = {"signal_id": "S", "start": "2026-01-01", "end": "2026-01-02"}


class TestTypedReportRun:
    """POST /reports/{name} with a typed (Report[TParams]) report."""

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_valid_params_returns_200_with_validated_model(self, mock_registry):
        """Valid params -> 200; default threshold applied via the model."""
        mock_registry.get.return_value = _TReport

        mock_session = _report_session()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post("/api/v1/reports/typed-report", json=_VALID_PARAMS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "complete"
        # threshold defaulted to 0.5 by the params model -> proves a
        # validated model (not the raw dict) reached execute().
        assert data["data"] == [{"v": 0.5}]

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_missing_required_param_returns_463(self, mock_registry):
        """Missing signal_id -> 463 structured body naming signal_id."""
        mock_registry.get.return_value = _TReport

        mock_session = _report_session()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/reports/typed-report",
            json={"start": "2026-01-01", "end": "2026-01-02"},
        )
        assert resp.status_code == HTTP_INVALID_REPORT_PARAMS
        detail = resp.json()["detail"]
        assert detail["error"] == "invalid_report_params"
        fields = [f["field"] for f in detail["failures"]]
        assert "signal_id" in fields

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_wrong_type_returns_463(self, mock_registry):
        """Non-numeric threshold -> 463; failures name threshold."""
        mock_registry.get.return_value = _TReport

        mock_session = _report_session()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/reports/typed-report",
            json={
                "signal_id": "S",
                "start": "x",
                "end": "y",
                "threshold": "not-a-number",
            },
        )
        assert resp.status_code == HTTP_INVALID_REPORT_PARAMS
        detail = resp.json()["detail"]
        assert detail["error"] == "invalid_report_params"
        fields = [f["field"] for f in detail["failures"]]
        assert "threshold" in fields


class TestUntypedReportRun:
    """A legacy/untyped report -> dict passthrough, unchanged behavior."""

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_dict_passthrough_returns_200(self, mock_registry):
        fake_cls = _make_fake_report_cls(execute_result={"metric": 42})
        mock_registry.get.return_value = fake_cls

        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        payload = {"signal_id": "abc"}
        resp = client.post("/api/v1/reports/test-report", json=payload)
        assert resp.status_code == 200
        assert resp.json()["data"] == [{"metric": 42}]

        # _params_cls_for(MagicMock) is None -> the raw dict is forwarded.
        instance = fake_cls.return_value
        instance.execute.assert_awaited_once()
        forwarded = instance.execute.await_args.args[0]
        assert forwarded == payload
        assert isinstance(forwarded, dict)


class TestTypedReportExport:
    """POST /reports/{name}/export with a typed report."""

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_export_missing_param_returns_463(self, mock_registry):
        mock_registry.get.return_value = _TReport

        mock_session = _report_session()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/reports/typed-report/export?format=csv",
            json={"start": "2026-01-01", "end": "2026-01-02"},
        )
        assert resp.status_code == HTTP_INVALID_REPORT_PARAMS
        detail = resp.json()["detail"]
        assert detail["error"] == "invalid_report_params"
        fields = [f["field"] for f in detail["failures"]]
        assert "signal_id" in fields

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_export_valid_params_returns_200_bytes(self, mock_registry):
        mock_registry.get.return_value = _TReport

        mock_session = _report_session()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post(
            "/api/v1/reports/typed-report/export?format=csv",
            json=_VALID_PARAMS,
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        # The DataFrame from execute serialized to CSV bytes.
        assert b"0.5" in resp.content


class TestGatingStaysDistinct:
    """preferred_http_status gating is NOT conflated with 463 validation."""

    @patch("tsigma.api.v1.reports.ReportRegistry")
    def test_gating_422_on_valid_params(self, mock_registry):
        mock_registry.get.return_value = _GatingReport

        mock_session = _report_session()
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.post("/api/v1/reports/gating-report", json=_VALID_PARAMS)
        # Valid params (no 463); report gates with 422.
        assert resp.status_code == 422
        assert resp.status_code != HTTP_INVALID_REPORT_PARAMS


class TestParamsClsForResolution:
    """_params_cls_for returns None for non-typed (untyped/legacy) reports."""

    def test_magicmock_returns_none(self):
        from tsigma.api.v1.reports import _params_cls_for

        # A MagicMock does NOT synthesize the __orig_bases__ dunder, so
        # getattr falls back to the empty-tuple default and the function
        # returns None (no generic params model) - the dict-passthrough path.
        assert _params_cls_for(MagicMock()) is None

    def test_real_typed_report_returns_params_cls(self):
        from tsigma.api.v1.reports import _params_cls_for

        assert _params_cls_for(_TReport) is _TParams
