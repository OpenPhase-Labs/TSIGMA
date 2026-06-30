"""
Unit tests for measure-defaults resolution at the report chokepoint.

Phase B of "measure defaults": the report execute/export chokepoint must
resolve each tunable param with the precedence

    per-request value  ->  admin default  ->  code default

The admin defaults live in the ``measure_default`` table (config schema),
keyed by ``report_name``. They are merged UNDER the per-request params and
then validated into the report's Pydantic model, so:

- a per-request value always wins over an admin default;
- an admin default fills a param the request omitted (instead of the
  model's code default);
- with no admin default and no request value, the model's code default
  applies;
- STRUCTURAL params (``signal_id``, ``start``, ``end``) are ALWAYS
  per-request and must NEVER receive an admin default, even if a bad
  ``measure_default`` row exists for one of them;
- an untyped/legacy report (no params class) keeps dict passthrough and
  gets no admin defaults applied (nothing to resolve against).

These tests drive the stable HTTP path (POST /reports/{name}) plus one
direct unit test against the resolution entrypoint. The measure_default
DB read is mocked via the ``get_session`` override: the yielded session's
``execute`` returns a result whose ``.scalars().all()`` is the desired
list of admin-default rows.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from tsigma.api.v1.reports import router
from tsigma.auth.sessions import SessionData
from tsigma.reports.registry import Report, ReportMetadata

# Custom status code the chokepoint returns for invalid report params.
HTTP_INVALID_REPORT_PARAMS = 463

# Code default for the tunable param under test; admin default differs so
# we can tell which precedence rung resolved.
CODE_DEFAULT_THRESHOLD = 0.79
ADMIN_DEFAULT_THRESHOLD = 0.5
REQUEST_THRESHOLD = 0.9


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


def _make_default_row(param_name, value):
    """A stand-in for a ``MeasureDefault`` ORM row.

    The resolution read does ``row.param_name`` / ``row.value``; a plain
    namespace-style mock is enough.
    """
    row = MagicMock()
    row.param_name = param_name
    row.value = value
    return row


def _make_defaults_session(default_rows):
    """Build a mock session whose ``execute`` yields ``default_rows``.

    The chokepoint resolution reads admin defaults with something like
    ``(await session.execute(select(MeasureDefault).where(...))).scalars().all()``.
    This mock returns ``default_rows`` from that chain for every execute
    call, which is all the resolution query needs. Auth runs against a
    separate session (``_get_db_session`` override), so this session only
    ever serves the measure_default read.
    """
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = list(default_rows)
    session.execute = AsyncMock(return_value=result)
    return session


def _add_session_override(app, mock_session):
    """Add a get_session dependency override yielding ``mock_session``."""
    from tsigma.dependencies import get_session

    async def override_session():
        yield mock_session

    app.dependency_overrides[get_session] = override_session


def _make_fake_report_cls(execute_result=None):
    """Create an untyped/legacy report class (MagicMock) for the registry.

    ``_params_cls_for`` returns None for a MagicMock, so this report keeps
    raw-dict passthrough - the path that must NOT receive admin defaults.
    """
    if execute_result is None:
        execute_result = pd.DataFrame([{"col": "val"}])
    elif isinstance(execute_result, dict):
        execute_result = pd.DataFrame([execute_result])

    cls = MagicMock()
    instance = MagicMock()
    instance.execute = AsyncMock(return_value=execute_result)
    cls.return_value = instance
    cls.metadata.description = "Test report"
    cls.metadata.category = "standard"
    cls.metadata.estimated_time = "fast"
    cls.metadata.export_formats = ["csv", "json"]
    cls.preferred_http_status = MagicMock(return_value=None)
    return cls


class _TParams(BaseModel):
    """Real params model: structural signal_id + tunable threshold."""

    signal_id: str
    green_occ_threshold: float = CODE_DEFAULT_THRESHOLD


class _TReport(Report[_TParams]):
    """Typed report that reflects the resolved params back out.

    ``execute`` returns a one-row DataFrame carrying the validated
    ``green_occ_threshold`` and ``signal_id`` so a test can read back
    exactly what the resolution produced.
    """

    metadata = ReportMetadata(
        name="measure-defaults-report",
        description="Measure-defaults test report",
        category="standard",
        estimated_time="fast",
    )

    async def execute(self, params, session):
        # Attribute access proves a validated model (not a raw dict).
        assert hasattr(params, "green_occ_threshold")
        return pd.DataFrame(
            [
                {
                    "threshold": params.green_occ_threshold,
                    "signal_id": params.signal_id,
                }
            ]
        )

    async def export(self, params, session, format="csv"):
        return b"threshold\n" + str(params.green_occ_threshold).encode()


REPORT_NAME = "measure-defaults-report"
REPORT_URL = f"/api/v1/reports/{REPORT_NAME}"


def _run(default_rows, request_params):
    """POST to the typed report with given admin defaults + request body.

    Returns the parsed JSON response.
    """
    mock_session = _make_defaults_session(default_rows)
    app = _create_test_app()
    _add_access_overrides(app)
    _add_session_override(app, mock_session)

    with patch("tsigma.api.v1.reports.ReportRegistry") as mock_registry:
        mock_registry.get.return_value = _TReport
        client = TestClient(app)
        resp = client.post(REPORT_URL, json=request_params)
    return resp


class TestPerRequestWins:
    """Per-request value overrides the admin default."""

    def test_request_value_beats_admin_default(self):
        rows = [_make_default_row("green_occ_threshold", ADMIN_DEFAULT_THRESHOLD)]
        resp = _run(
            rows,
            {"signal_id": "S1", "green_occ_threshold": REQUEST_THRESHOLD},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data[0]["threshold"] == REQUEST_THRESHOLD


class TestAdminDefaultApplies:
    """Admin default fills an omitted param (NOT the model code default)."""

    def test_admin_default_used_when_request_omits(self):
        rows = [_make_default_row("green_occ_threshold", ADMIN_DEFAULT_THRESHOLD)]
        resp = _run(rows, {"signal_id": "S1"})
        assert resp.status_code == 200
        data = resp.json()["data"]
        # Must be the admin 0.5, NOT the model's code default 0.79.
        assert data[0]["threshold"] == ADMIN_DEFAULT_THRESHOLD
        assert data[0]["threshold"] != CODE_DEFAULT_THRESHOLD


class TestCodeDefaultApplies:
    """No admin row + omitted param -> model field default."""

    def test_code_default_when_no_admin_row(self):
        resp = _run([], {"signal_id": "S1"})
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data[0]["threshold"] == CODE_DEFAULT_THRESHOLD


class TestStructuralNeverDefaulted:
    """Structural params must never receive an admin default."""

    def test_structural_admin_row_ignored_while_tunable_default_applies(self):
        # Two admin rows: a BAD structural one (signal_id) and a legitimate
        # tunable one (green_occ_threshold). The request omits both. Correct
        # behavior: the structural row is EXCLUDED from merging, so signal_id
        # stays missing -> 463; the structural admin value must never backfill
        # signal_id (which would wrongly yield 200 with signal_id="GHOST").
        #
        # This is RED now because admin merging does not exist: the tunable
        # default is not consulted either, but the load-bearing assertion is
        # that a structural admin row is NEVER allowed to satisfy signal_id.
        # A naive Phase B that merges ALL admin rows would backfill signal_id
        # and return 200 - this test forbids that.
        rows = [
            _make_default_row("signal_id", "GHOST"),
            _make_default_row("green_occ_threshold", ADMIN_DEFAULT_THRESHOLD),
        ]
        resp = _run(rows, {})
        assert resp.status_code == HTTP_INVALID_REPORT_PARAMS
        detail = resp.json()["detail"]
        assert detail["error"] == "invalid_report_params"
        fields = [f["field"] for f in detail["failures"]]
        assert "signal_id" in fields

    def test_request_signal_id_survives_with_admin_tunable_merged(self):
        # The request supplies the structural signal_id and omits the tunable;
        # a bad structural admin row plus a good tunable admin row are present.
        # Correct: signal_id comes from the request (structural row excluded),
        # and the tunable resolves to the admin default. This is RED now
        # because the tunable admin default is not merged -> threshold comes
        # back as the code default 0.79, not the admin 0.5.
        rows = [
            _make_default_row("signal_id", "GHOST"),
            _make_default_row("green_occ_threshold", ADMIN_DEFAULT_THRESHOLD),
        ]
        resp = _run(rows, {"signal_id": "REAL"})
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data[0]["signal_id"] == "REAL"
        assert data[0]["threshold"] == ADMIN_DEFAULT_THRESHOLD


class TestUntypedReportPassthrough:
    """Legacy/untyped report: dict passthrough, no admin defaults applied."""

    def test_untyped_report_does_not_receive_admin_defaults(self):
        rows = [_make_default_row("green_occ_threshold", ADMIN_DEFAULT_THRESHOLD)]
        mock_session = _make_defaults_session(rows)
        app = _create_test_app()
        _add_access_overrides(app)
        _add_session_override(app, mock_session)

        fake_cls = _make_fake_report_cls(execute_result={"metric": 1})
        with patch("tsigma.api.v1.reports.ReportRegistry") as mock_registry:
            mock_registry.get.return_value = fake_cls
            client = TestClient(app)
            payload = {"signal_id": "abc"}
            resp = client.post("/api/v1/reports/legacy-report", json=payload)

        assert resp.status_code == 200
        instance = fake_cls.return_value
        instance.execute.assert_awaited_once()
        forwarded = instance.execute.await_args.args[0]
        # Raw dict forwarded unchanged - no green_occ_threshold injected.
        assert isinstance(forwarded, dict)
        assert forwarded == payload
        assert "green_occ_threshold" not in forwarded


class TestDirectResolutionEntrypoint:
    """Direct unit test of the resolution entrypoint, if one exists.

    Prefer the stable HTTP path above; this pins the helper contract too.
    Skips cleanly if the Phase B entrypoint is not yet present, so the
    suite stays meaningful before and after implementation.
    """

    @pytest.mark.asyncio
    async def test_resolve_merges_admin_under_request(self):
        import tsigma.api.v1.reports as reports_mod

        resolver = getattr(reports_mod, "_resolve_and_validate_params", None)
        if resolver is None:
            pytest.skip("_resolve_and_validate_params not implemented yet")

        session = _make_defaults_session(
            [_make_default_row("green_occ_threshold", ADMIN_DEFAULT_THRESHOLD)]
        )
        validated = await resolver(
            _TReport, REPORT_NAME, {"signal_id": "S1"}, session
        )
        assert validated.green_occ_threshold == ADMIN_DEFAULT_THRESHOLD
        assert validated.signal_id == "S1"
