"""
Unit tests for analytics API endpoints.

Tests all analytics endpoints with mocked database sessions.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests._helpers import make_mock_session
from tsigma.api.v1.analytics import router
from tsigma.auth.sessions import SessionData


def _create_test_app():
    """Create a minimal FastAPI app with the analytics router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/analytics")
    return app


def _add_access_overrides(app):
    """Override require_access sub-dependencies so GET endpoints pass auth."""
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


NOW = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
START = NOW - timedelta(hours=1)
END = NOW


# ----------------------------------------------------------------------
# Row-iteration analytics SDK wiring (fetch_events)
# ----------------------------------------------------------------------
#
# Analytics handlers in this group:
#   * resolve start/end via _default_start / _default_end,
#   * call require_max_lookback(t_start, session=session),
#   * call require_max_aggregation_days(t_start, t_end, session=session),
#   * read CEL rows via `await fetch_events(...)` (returns a DataFrame),
#   * iterate the DataFrame instead of SQLAlchemy named-tuple rows.
#
# The helpers below patch `fetch_events`, `require_max_lookback` and
# `require_max_aggregation_days` at the use-site of the analytics submodule
# under test, plus stub `tsigma.settings_service.get_int` to feed the
# guards.
#
# Each analytics submodule imports `fetch_events` independently, so a
# test patches the specific submodule it exercises (passed via
# `module_path`).
# ----------------------------------------------------------------------


def _wire_analytics_v2(
    app,
    monkeypatch,
    module_path,
    *,
    events_df=None,
    max_lookback_days=92,
    max_aggregation_days=92,
    session=None,
):
    """Wire fetch_events-based dependencies for a single analytics submodule.

    Args:
        app: FastAPI app under test.
        monkeypatch: pytest fixture for setattr.
        module_path: Dotted path of the analytics submodule whose handler
            will be invoked (e.g. ``tsigma.api.v1.analytics.coordination``).
            ``fetch_events`` and the two guards are patched at this module
            path so the handler picks them up at call time.
        events_df: DataFrame returned by the mocked ``fetch_events``.
            Defaults to an empty events frame with the SDK columns.
        max_lookback_days: Value returned by the stubbed
            ``settings_service.get_int`` for ``api.max_lookback_days``.
        max_aggregation_days: Same for ``api.max_aggregation_days``.
        session: Optional pre-built mock session. When ``None`` a fresh
            mock is created with an empty ``execute`` result; pass one
            when a test needs to control session.execute behaviour.

    Returns:
        dict of mocks: ``mock_session``, ``mock_fetch``, ``mock_lookback``,
        ``mock_aggregation``, ``mock_get_int``.
    """
    if events_df is None:
        events_df = pd.DataFrame(
            {
                "event_code": [],
                "event_param": [],
                "event_time": [],
            }
        )

    if session is None:
        session = make_mock_session()
        empty_result = MagicMock()
        empty_result.all.return_value = []
        empty_result.one.return_value = MagicMock()
        empty_result.scalar.return_value = 0
        session.execute = AsyncMock(return_value=empty_result)

    from tsigma.dependencies import get_session

    async def _override_session():
        yield session

    app.dependency_overrides[get_session] = _override_session

    mock_fetch = AsyncMock(return_value=events_df)
    mock_lookback = AsyncMock(return_value=None)
    mock_aggregation = AsyncMock(return_value=None)

    monkeypatch.setattr(
        f"{module_path}.fetch_events", mock_fetch, raising=False
    )
    monkeypatch.setattr(
        f"{module_path}.require_max_lookback", mock_lookback, raising=False
    )
    monkeypatch.setattr(
        f"{module_path}.require_max_aggregation_days",
        mock_aggregation,
        raising=False,
    )

    async def _get_int(key, session=None):
        mapping = {
            "api.max_lookback_days": max_lookback_days,
            "api.max_aggregation_days": max_aggregation_days,
            "api.max_page_size": 1000,
        }
        return mapping.get(key, 1000)

    mock_get_int = AsyncMock(side_effect=_get_int)
    monkeypatch.setattr(
        "tsigma.settings_service.get_int", mock_get_int, raising=False
    )

    return {
        "mock_session": session,
        "mock_fetch": mock_fetch,
        "mock_lookback": mock_lookback,
        "mock_aggregation": mock_aggregation,
        "mock_get_int": mock_get_int,
    }


def _events_df(rows):
    """Build an SDK-shape events DataFrame from a list of (code, param, time)."""
    return pd.DataFrame(
        {
            "event_code": [r[0] for r in rows],
            "event_param": [r[1] for r in rows],
            "event_time": [r[2] for r in rows],
        }
    )


# Default-window guard helpers ---------------------------------------------
#
# The 9 endpoints all default start/end (24h-ago → now) when query params are
# missing. To trigger the lookback / aggregation guards we use real-helper
# style HTTPExceptions and pass an explicit start/end the test asserts on.
def _raise_lookback_exc(start, *, session):
    from fastapi import HTTPException

    raise HTTPException(
        status_code=400,
        detail=(
            f"start={start.isoformat()} predates the "
            f"api.max_lookback_days ceiling (7 days)"
        ),
    )


def _raise_aggregation_exc(start, end, *, session):
    from fastapi import HTTPException

    window = (end - start).days
    raise HTTPException(
        status_code=400,
        detail=(
            f"requested window ({window} days) exceeds "
            f"api.max_aggregation_days (7)"
        ),
    )


class TestGapAnalysis:
    """Tests for GET /api/v1/analytics/detectors/gaps."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/detectors/gaps")
        assert resp.status_code == 422

    def test_returns_gap_analysis(self, monkeypatch):
        """Test gap analysis computation.

        Stage 2a: CEL rows come from `fetch_events` returning a DataFrame.
        """
        df = _events_df([
            (82, 5, NOW - timedelta(seconds=20)),
            (82, 5, NOW - timedelta(seconds=10)),
        ])

        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["total_actuations"] == 2
        assert data[0]["avg_gap_seconds"] == 10.0


class TestGapAnalysisEdgeCases:
    """Tests for detector gap analysis edge cases."""

    def test_gap_with_detector_channel_filter(self, monkeypatch):
        """Test gap analysis with specific detector_channel filter (line 166)."""
        df = _events_df([
            (82, 3, NOW - timedelta(seconds=20)),
            (82, 3, NOW - timedelta(seconds=10)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={"signal_id": "SIG-001", "detector_channel": 3},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["detector_channel"] == 3

    def test_gap_single_event_per_channel_skipped(self, monkeypatch):
        """Test channel with <2 events is skipped (line 185)."""
        df = _events_df([
            (82, 7, NOW - timedelta(seconds=10)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        assert resp.json() == []


class TestOccupancyEdgeCases:
    """Tests for detector occupancy edge cases (lines 255-269)."""

    def test_occupancy_on_event_before_bin_start(self, monkeypatch):
        """Test ON event before bin start carries into the bin (lines 255-258)."""
        # ON event before the bin, OFF event inside the bin
        df = _events_df([
            (82, 5, START - timedelta(seconds=30)),
            (81, 5, START + timedelta(seconds=60)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/occupancy",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
                "bin_minutes": 60,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["bins"]) >= 1
        # The first bin should have non-zero occupancy since ON carried in
        assert data["bins"][0]["occupancy_pct"] > 0

    def test_occupancy_on_without_off_carries_to_bin_end(self, monkeypatch):
        """Test ON event with no OFF extends to bin end (line 269)."""
        # ON event in the middle of the bin, no OFF event
        df = _events_df([
            (82, 5, START + timedelta(seconds=30)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/occupancy",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
                "bin_minutes": 60,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["bins"]) >= 1
        # ON at +30s to end of 3600s bin => occupancy > 0
        assert data["bins"][0]["occupancy_pct"] > 0


class TestDetectorOccupancy:
    """Tests for GET /api/v1/analytics/detectors/occupancy."""

    def test_requires_signal_id_and_channel(self):
        """Test required params."""
        mock_session = make_mock_session()

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/detectors/occupancy")
        assert resp.status_code == 422

    def test_returns_occupancy_bins(self, monkeypatch):
        """Test occupancy bin computation."""
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/occupancy",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
                "bin_minutes": 15,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signal_id"] == "SIG-001"
        assert data["detector_channel"] == 5
        assert "bins" in data


class TestSkippedPhases:
    """Tests for GET /api/v1/analytics/phases/skipped."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/phases/skipped")
        assert resp.status_code == 422


class TestSplitMonitorEdgeCases:
    """Tests for split monitor edge cases (lines 104, 147-152, 155)."""

    def test_split_monitor_with_phase_filter(self, monkeypatch):
        """Test split-monitor with phase query param (line 104)."""
        df = _events_df([
            (1, 4, NOW - timedelta(seconds=30)),
            (4, 4, NOW - timedelta(seconds=10)),
            (8, 4, NOW - timedelta(seconds=5)),
            (9, 4, NOW - timedelta(seconds=1)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/split-monitor",
            params={"signal_id": "SIG-001", "phase": 4},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["phase"] == 4
        assert data[0]["gap_out_pct"] > 0

    def test_split_monitor_all_termination_types(self, monkeypatch):
        """Test gap_out, max_out, force_off counting (lines 147-152)."""
        df = _events_df([
            (1, 2, NOW - timedelta(seconds=90)),
            (4, 2, NOW - timedelta(seconds=80)),
            (8, 2, NOW - timedelta(seconds=75)),
            (9, 2, NOW - timedelta(seconds=72)),
            (1, 2, NOW - timedelta(seconds=60)),
            (5, 2, NOW - timedelta(seconds=50)),
            (8, 2, NOW - timedelta(seconds=45)),
            (9, 2, NOW - timedelta(seconds=42)),
            (1, 2, NOW - timedelta(seconds=30)),
            (6, 2, NOW - timedelta(seconds=20)),
            (8, 2, NOW - timedelta(seconds=15)),
            (9, 2, NOW - timedelta(seconds=12)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/split-monitor",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        item = data[0]
        assert item["cycle_count"] == 3
        # 1 gap_out, 1 max_out, 1 force_off => each ~33.3%
        assert item["gap_out_pct"] > 0
        assert item["max_out_pct"] > 0
        assert item["force_off_pct"] > 0

    def test_split_monitor_zero_cycles_skipped(self, monkeypatch):
        """Test phase with no Green events (cycle_count==0) is skipped (line 155)."""
        # Only termination events, no Green (code 1)
        df = _events_df([
            (4, 6, NOW - timedelta(seconds=10)),
            (5, 6, NOW - timedelta(seconds=5)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/split-monitor",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        assert resp.json() == []


class TestSplitMonitor:
    """Tests for GET /api/v1/analytics/phases/split-monitor."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/phases/split-monitor")
        assert resp.status_code == 422

    def test_returns_split_data(self, monkeypatch):
        """Test split monitor with phase events."""
        # Green at t=0, Yellow at t=25, Red at t=29
        df = _events_df([
            (1, 2, NOW - timedelta(seconds=30)),
            (8, 2, NOW - timedelta(seconds=5)),
            (9, 2, NOW - timedelta(seconds=1)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/split-monitor",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["phase"] == 2
        assert data[0]["cycle_count"] == 1


class TestPhaseTerminations:
    """Tests for GET /api/v1/analytics/phases/terminations."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/phases/terminations")
        assert resp.status_code == 422


class TestOffsetDrift:
    """Tests for GET /api/v1/analytics/coordination/offset-drift."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/coordination/offset-drift")
        assert resp.status_code == 422

    def test_insufficient_data(self, monkeypatch):
        """Test 404 with insufficient data."""
        df = _events_df([(1, 2, NOW)])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/offset-drift",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 404

    def test_returns_drift_analysis(self, monkeypatch):
        """Test offset drift computation."""
        df = _events_df([
            (1, 2, NOW - timedelta(seconds=240)),
            (1, 2, NOW - timedelta(seconds=120)),
            (1, 2, NOW),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/offset-drift",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signal_id"] == "SIG-001"
        assert data["cycle_count"] == 2


class TestPatternHistory:
    """Tests for GET /api/v1/analytics/coordination/patterns."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/coordination/patterns")
        assert resp.status_code == 422

    def test_returns_pattern_changes(self, monkeypatch):
        """Test pattern change history."""
        df = _events_df([
            (131, 1, NOW - timedelta(hours=6)),
            (131, 2, NOW),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/patterns",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2


class TestCoordinationQuality:
    """Tests for GET /api/v1/analytics/coordination/quality."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/coordination/quality")
        assert resp.status_code == 422

    def test_insufficient_data_returns_404(self, monkeypatch):
        """Test 404 when fewer than 2 coord phase events."""
        df = _events_df([(1, 2, NOW)])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/quality",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 404
        assert "Insufficient data" in resp.json()["detail"]

    def test_returns_quality_score(self, monkeypatch):
        """Test coordination quality score computation (lines 138-172)."""
        # 3 events => 2 cycles, both 120s apart => avg=120, deviations=[0,0]
        df = _events_df([
            (1, 2, NOW - timedelta(seconds=240)),
            (1, 2, NOW - timedelta(seconds=120)),
            (1, 2, NOW),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/quality",
            params={
                "signal_id": "SIG-001",
                "tolerance_seconds": "5.0",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signal_id"] == "SIG-001"
        assert data["total_cycles"] == 2
        # Both cycles are 120s, avg=120, deviation=0 => all within tolerance
        assert data["cycles_within_tolerance"] == 2
        assert data["quality_pct"] == 100.0
        assert data["avg_offset_error_seconds"] == 0.0

    def test_quality_with_varied_cycles(self, monkeypatch):
        """Test quality score when some cycles exceed tolerance."""
        # Cycles: 120s, 125s => avg=122.5, devs=[2.5, 2.5]
        # tolerance=2.0 => 0 within tolerance
        df = _events_df([
            (1, 2, NOW - timedelta(seconds=245)),
            (1, 2, NOW - timedelta(seconds=125)),
            (1, 2, NOW),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/quality",
            params={
                "signal_id": "SIG-001",
                "tolerance_seconds": "2.0",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_cycles"] == 2
        assert data["cycles_within_tolerance"] == 0
        assert data["quality_pct"] == 0.0


class TestPreemptionSummary:
    """Tests for GET /api/v1/analytics/preemptions/summary."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/preemptions/summary")
        assert resp.status_code == 422

    def test_returns_summary(self, monkeypatch):
        """Test preemption summary with begin/end event pairs."""
        df = _events_df([
            (102, 1, NOW - timedelta(seconds=60)),
            (104, 1, NOW - timedelta(seconds=15)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/summary",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_preemptions"] == 1


class TestPreemptionRecovery:
    """Tests for GET /api/v1/analytics/preemptions/recovery."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/preemptions/recovery")
        assert resp.status_code == 422


class TestDetectorHealth:
    """Tests for GET /api/v1/analytics/health/detector."""

    def test_requires_signal_id_and_channel(self):
        """Test required params."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/health/detector")
        assert resp.status_code == 422


class TestSignalHealth:
    """Tests for GET /api/v1/analytics/health/signal."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = make_mock_session()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/health/signal")
        assert resp.status_code == 422


class TestPreemptionRecoveryData:
    """Tests for GET /api/v1/analytics/preemptions/recovery with mock data."""

    def test_returns_recovery_items(self, monkeypatch):
        """Test preemption recovery with end->green event pairs."""
        df = _events_df([
            (104, 1, NOW - timedelta(seconds=30)),
            (1, 2, NOW - timedelta(seconds=20)),
        ])
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/recovery",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["recovery_seconds"] == 10.0
        assert data["avg_recovery_seconds"] == 10.0

    def test_returns_empty_when_no_events(self, monkeypatch):
        """Test preemption recovery with no events returns empty items."""
        app = _create_test_app()
        _add_access_overrides(app)
        _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/recovery",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["avg_recovery_seconds"] == 0.0


# ============================================================================
# B12 Stage 2a — NEW tests: tier-aware fetch_events + request guards (RED).
# ============================================================================
#
# Each block tests one of the 9 row-iteration analytics endpoints with three
# common cases:
#   1. The handler invokes the tier-aware `fetch_events` (not session.execute)
#      with the correct signal_id / time window / event_codes / event_param_in.
#   2. A start that predates api.max_lookback_days yields 400.
#   3. A window that exceeds api.max_aggregation_days yields 400.
#
# Endpoint-specific extras live next to their block:
#   - preemption_recovery: where_sql_fragment OR-predicate assertion.
#   - signal_health: only the coord component swapped to fetch_events.


def _isoformat_old(days_ago):
    return (datetime.now(tz=timezone.utc) - timedelta(days=days_ago)).isoformat()


class TestOffsetDriftTierAware:
    """Stage 2a — offset_drift switches to fetch_events + guards."""

    def test_offset_drift_calls_tier_aware_fetch_events(self, monkeypatch):
        """fetch_events awaited with signal_id, window, event_codes=[1],
        event_param_in=[2]; session.execute is NOT used for the CEL query."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (1, 2, NOW - timedelta(seconds=240)),
            (1, 2, NOW - timedelta(seconds=120)),
            (1, 2, NOW),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/offset-drift",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert kwargs.get("event_codes") == [1]
        assert kwargs.get("event_param_in") == [2]
        assert kwargs.get("start") is not None
        assert kwargs.get("end") is not None
        assert mocks["mock_session"].execute.await_count == 0

    def test_offset_drift_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/offset-drift",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_offset_drift_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/offset-drift",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


class TestPatternHistoryTierAware:
    """Stage 2a — pattern_history switches to fetch_events + guards."""

    def test_pattern_history_calls_tier_aware_fetch_events(self, monkeypatch):
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (131, 1, NOW - timedelta(hours=6)),
            (131, 2, NOW),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/patterns",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert kwargs.get("event_codes") == [131]
        # pattern_history uses no event_param filter.
        assert kwargs.get("event_param_in") in (None, [])
        assert mocks["mock_session"].execute.await_count == 0

    def test_pattern_history_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/patterns",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_pattern_history_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/patterns",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


class TestCoordinationQualityTierAware:
    """Stage 2a — coordination_quality switches to fetch_events + guards."""

    def test_coordination_quality_calls_tier_aware_fetch_events(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (1, 2, NOW - timedelta(seconds=240)),
            (1, 2, NOW - timedelta(seconds=120)),
            (1, 2, NOW),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/quality",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert kwargs.get("event_codes") == [1]
        assert kwargs.get("event_param_in") == [2]
        assert mocks["mock_session"].execute.await_count == 0

    def test_coordination_quality_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/quality",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_coordination_quality_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.coordination",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/quality",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


class TestGapAnalysisTierAware:
    """Stage 2a — gap_analysis switches to fetch_events + guards."""

    def test_gap_analysis_calls_tier_aware_fetch_events(self, monkeypatch):
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (82, 5, NOW - timedelta(seconds=20)),
            (82, 5, NOW - timedelta(seconds=10)),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={"signal_id": "SIG-001", "detector_channel": 5},
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert kwargs.get("event_codes") == [82]
        assert kwargs.get("event_param_in") == [5]
        assert mocks["mock_session"].execute.await_count == 0

    def test_gap_analysis_no_channel_filter_passes_none(self, monkeypatch):
        """When detector_channel omitted, event_param_in must be None."""
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("event_param_in") is None

    def test_gap_analysis_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_gap_analysis_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


class TestDetectorOccupancyTierAware:
    """Stage 2a — detector_occupancy switches to fetch_events + guards."""

    def test_detector_occupancy_calls_tier_aware_fetch_events(self, monkeypatch):
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (82, 5, START + timedelta(seconds=30)),
            (81, 5, START + timedelta(seconds=90)),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/occupancy",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert sorted(kwargs.get("event_codes") or []) == [81, 82]
        assert kwargs.get("event_param_in") == [5]
        assert mocks["mock_session"].execute.await_count == 0

    def test_detector_occupancy_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/occupancy",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_detector_occupancy_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/occupancy",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


class TestSplitMonitorTierAware:
    """Stage 2a — split_monitor switches to fetch_events + guards."""

    def test_split_monitor_calls_tier_aware_fetch_events(self, monkeypatch):
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (1, 2, NOW - timedelta(seconds=30)),
            (8, 2, NOW - timedelta(seconds=5)),
            (9, 2, NOW - timedelta(seconds=1)),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/split-monitor",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert sorted(kwargs.get("event_codes") or []) == [1, 4, 5, 6, 8, 9]
        # No phase filter -> event_param_in must be None.
        assert kwargs.get("event_param_in") is None
        assert mocks["mock_session"].execute.await_count == 0

    def test_split_monitor_phase_filter_passes_event_param_in(self, monkeypatch):
        """phase query param flows into event_param_in=[phase]."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (1, 4, NOW - timedelta(seconds=30)),
            (8, 4, NOW - timedelta(seconds=5)),
            (9, 4, NOW - timedelta(seconds=1)),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/split-monitor",
            params={"signal_id": "SIG-001", "phase": 4},
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("event_param_in") == [4]

    def test_split_monitor_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/split-monitor",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_split_monitor_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/split-monitor",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


class TestPreemptionSummaryTierAware:
    """Stage 2a — preemption_summary switches to fetch_events + guards."""

    def test_preemption_summary_calls_tier_aware_fetch_events(self, monkeypatch):
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (102, 1, NOW - timedelta(seconds=60)),
            (104, 1, NOW - timedelta(seconds=15)),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/summary",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert sorted(kwargs.get("event_codes") or []) == [102, 104]
        assert kwargs.get("event_param_in") in (None, [])
        assert mocks["mock_session"].execute.await_count == 0

    def test_preemption_summary_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/summary",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_preemption_summary_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/summary",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


class TestPreemptionRecoveryTierAware:
    """Stage 2a — preemption_recovery uses fetch_events with where_sql_fragment.

    The OR predicate ``(event_code == 104) OR (event_code == 1 AND
    event_param == 2)`` cannot be expressed via the (event_codes,
    event_param_in) parameter pair. The implementer must build a
    ``where_sql_fragment`` string and pass ``event_codes=None,
    where_sql_fragment="<sql>"`` per the SDK-internal third predicate mode
    (B8 extension).
    """

    def test_preemption_recovery_calls_tier_aware_fetch_events(self, monkeypatch):
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (104, 1, NOW - timedelta(seconds=30)),
            (1, 2, NOW - timedelta(seconds=20)),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/recovery",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert mocks["mock_session"].execute.await_count == 0

    def test_calls_fetch_events_with_where_sql_fragment(self, monkeypatch):
        """OR predicate is pushed as a where_sql_fragment string, not the
        (event_codes, event_param_in) parameter pair."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = _events_df([
            (104, 1, NOW - timedelta(seconds=30)),
            (1, 2, NOW - timedelta(seconds=20)),
        ])
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            events_df=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/recovery",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        # event_codes must be None — the predicate flows via fragment.
        assert kwargs.get("event_codes") is None
        # where_sql_fragment must be a string containing the OR predicate.
        frag = kwargs.get("where_sql_fragment")
        assert isinstance(frag, str) and frag != ""
        # The fragment must mention event_code=104 and (event_code=1 AND
        # event_param=2). Allow loose whitespace and operator forms.
        normalized = frag.replace(" ", "").lower()
        assert "event_code=104" in normalized or "event_code in(104" in normalized
        assert "event_code=1" in normalized or "event_code in(1" in normalized
        assert "event_param=2" in normalized or "event_param in(2" in normalized

    def test_preemption_recovery_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/recovery",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_preemption_recovery_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2(
            app, monkeypatch,
            "tsigma.api.v1.analytics.preemption",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/preemptions/recovery",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


# ----------------------------------------------------------------------
# aggregate_events-based analytics SDK wiring
# ----------------------------------------------------------------------
#
# The SQL-aggregation analytics endpoints read CEL rows through the
# ``aggregate_events`` SDK (which returns a DataFrame) instead of issuing
# raw ``session.execute(select(...).group_by(...))`` SQL. Each handler:
#   * resolves start/end via _default_start / _default_end,
#   * calls require_max_lookback(t_start, session=session),
#   * calls require_max_aggregation_days(t_start, t_end, session=session),
#   * reads aggregated rows via ``await aggregate_events(...)``.
#
# Tests deliberately do NOT pin the implementation to a specific
# ``agg=[...tuple shape...]`` argument — that's impl-shaped, not
# contract-shaped. We feed the mocked SDK a known DataFrame and assert
# the endpoint maps it correctly to its Pydantic response.
# ----------------------------------------------------------------------


def _wire_analytics_v2b(
    app,
    monkeypatch,
    module_path,
    *,
    aggregate_returns=None,
    aggregate_side_effect=None,
    fetch_events_df=None,
    max_lookback_days=92,
    max_aggregation_days=92,
    session=None,
):
    """Wire aggregate_events-based dependencies for a single analytics submodule.

    Args:
        app: FastAPI app under test.
        monkeypatch: pytest fixture for setattr.
        module_path: Dotted path of the analytics submodule (e.g.
            ``tsigma.api.v1.analytics.detectors``). ``aggregate_events``
            and the two request guards are patched at this module path so
            the handler picks them up at call time.
        aggregate_returns: A DataFrame returned by the mocked
            ``aggregate_events``. Ignored when ``aggregate_side_effect``
            is set. Defaults to an empty DataFrame.
        aggregate_side_effect: Optional iterable of DataFrames or a
            callable. Use for endpoints that invoke ``aggregate_events``
            multiple times within one request (e.g. stuck_detectors fires
            it 3 times; signal_health fires it 3 times).
        fetch_events_df: DataFrame returned by the mocked
            ``fetch_events`` (still used alongside aggregate_events by
            signal_health for the coordination path). Only relevant for
            the health module.
        max_lookback_days: Value returned by the stubbed
            ``settings_service.get_int`` for ``api.max_lookback_days``.
        max_aggregation_days: Same for ``api.max_aggregation_days``.
        session: Optional pre-built mock session. When ``None`` a fresh
            mock is created with an empty ``execute`` result.

    Returns:
        dict of mocks: ``mock_session``, ``mock_aggregate``,
        ``mock_fetch``, ``mock_lookback``, ``mock_aggregation``,
        ``mock_get_int``.
    """
    if session is None:
        session = make_mock_session()
        empty_result = MagicMock()
        empty_result.all.return_value = []
        empty_result.one.return_value = MagicMock()
        empty_result.scalar.return_value = 0
        session.execute = AsyncMock(return_value=empty_result)

    from tsigma.dependencies import get_session

    async def _override_session():
        yield session

    app.dependency_overrides[get_session] = _override_session

    if aggregate_side_effect is not None:
        mock_aggregate = AsyncMock(side_effect=aggregate_side_effect)
    else:
        if aggregate_returns is None:
            aggregate_returns = pd.DataFrame()
        mock_aggregate = AsyncMock(return_value=aggregate_returns)

    if fetch_events_df is None:
        fetch_events_df = pd.DataFrame(
            {"event_code": [], "event_param": [], "event_time": []}
        )
    mock_fetch = AsyncMock(return_value=fetch_events_df)
    mock_lookback = AsyncMock(return_value=None)
    mock_aggregation = AsyncMock(return_value=None)

    monkeypatch.setattr(
        f"{module_path}.aggregate_events", mock_aggregate, raising=False
    )
    monkeypatch.setattr(
        f"{module_path}.fetch_events", mock_fetch, raising=False
    )
    monkeypatch.setattr(
        f"{module_path}.require_max_lookback", mock_lookback, raising=False
    )
    monkeypatch.setattr(
        f"{module_path}.require_max_aggregation_days",
        mock_aggregation,
        raising=False,
    )

    async def _get_int(key, session=None):
        mapping = {
            "api.max_lookback_days": max_lookback_days,
            "api.max_aggregation_days": max_aggregation_days,
            "api.max_page_size": 1000,
        }
        return mapping.get(key, 1000)

    mock_get_int = AsyncMock(side_effect=_get_int)
    monkeypatch.setattr(
        "tsigma.settings_service.get_int", mock_get_int, raising=False
    )

    return {
        "mock_session": session,
        "mock_aggregate": mock_aggregate,
        "mock_fetch": mock_fetch,
        "mock_lookback": mock_lookback,
        "mock_aggregation": mock_aggregation,
        "mock_get_int": mock_get_int,
    }


# ---------------------------------------------------------------------------
# stuck_detectors — 3 aggregations (last_on, last_off, on_count) joined
# ---------------------------------------------------------------------------


def _stuck_detectors_three_dfs(*, last_on_time, last_off_time, on_count):
    """Build the three DataFrames stuck_detectors' aggregations return:

    1) last_on:  group_by (signal_id, event_param), agg=max(event_time)
    2) last_off: group_by (signal_id, event_param), agg=max(event_time)
    3) on_count: group_by (signal_id, event_param), agg=count

    The implementation chooses concrete column names but the SDK contract
    is "group_by columns first then agg_<i>". We feed both styles so the
    test passes regardless of whether the implementer renames columns.
    """
    last_on_df = pd.DataFrame(
        [
            {
                "signal_id": "SIG-001",
                "event_param": 5,
                "agg_0": last_on_time,
            }
        ]
    )
    last_off_df = pd.DataFrame(
        [
            {
                "signal_id": "SIG-001",
                "event_param": 5,
                "agg_0": last_off_time,
            }
        ]
    )
    on_count_df = pd.DataFrame(
        [
            {
                "signal_id": "SIG-001",
                "event_param": 5,
                "agg_0": on_count,
            }
        ]
    )
    return [last_on_df, last_off_df, on_count_df]


class TestStuckDetectorsTierAware:
    """Stage 2b — stuck_detectors swaps the three SQL aggregations for
    three ``aggregate_events`` SDK calls and applies request guards."""

    def test_stuck_detectors_calls_aggregate_events(self, monkeypatch):
        """The endpoint must invoke aggregate_events for the three
        aggregations (last_on, last_off, on_count). We do not pin the
        agg-spec tuples — only that the SDK is invoked at least once with
        the right signal_id and time window."""
        app = _create_test_app()
        _add_access_overrides(app)
        # No stuck detectors: return empty DFs from every aggregation.
        empty_df = pd.DataFrame(
            {"signal_id": [], "event_param": [], "agg_0": []}
        )
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            aggregate_side_effect=[empty_df, empty_df, empty_df],
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        # Must have invoked aggregate_events (at least once); never
        # session.execute for the CEL aggregation queries.
        assert mocks["mock_aggregate"].await_count >= 1
        assert mocks["mock_session"].execute.await_count == 0
        # Signal_id propagated as positional or keyword.
        first_call = mocks["mock_aggregate"].await_args_list[0]
        first_args = list(first_call.args) + list(first_call.kwargs.values())
        assert "SIG-001" in first_args

    def test_stuck_detectors_empty_aggregation_returns_empty_list(
        self, monkeypatch
    ):
        """When every aggregation returns an empty DataFrame, the
        endpoint returns an empty list (existing contract)."""
        app = _create_test_app()
        _add_access_overrides(app)
        empty_df = pd.DataFrame(
            {"signal_id": [], "event_param": [], "agg_0": []}
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            aggregate_side_effect=[empty_df, empty_df, empty_df],
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        assert resp.json() == []

    def test_stuck_detectors_returns_stuck_when_last_on_after_last_off(
        self, monkeypatch
    ):
        """Detector is reported stuck when last_on > last_off and the
        duration since last_on >= threshold_minutes."""
        app = _create_test_app()
        _add_access_overrides(app)
        # last_on 2h before END, last_off 3h before END → duration is 2h
        # = 7200s. threshold=30min=1800s → STUCK.
        dfs = _stuck_detectors_three_dfs(
            last_on_time=END - timedelta(hours=2),
            last_off_time=END - timedelta(hours=3),
            on_count=10,
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            aggregate_side_effect=dfs,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
                "threshold_minutes": 30,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1
        item = data[0]
        assert item["signal_id"] == "SIG-001"
        assert item["detector_channel"] == 5
        assert item["status"] == "STUCK_ON"
        assert item["event_count"] == 10
        # Duration approximately 7200s (allow small float wiggle).
        assert 7100 < item["duration_seconds"] < 7300

    def test_stuck_detectors_threshold_filters_short_durations(
        self, monkeypatch
    ):
        """When stuck duration < threshold_minutes, item is filtered out."""
        app = _create_test_app()
        _add_access_overrides(app)
        # last_on only 10 minutes before END; threshold=30min → NOT stuck.
        dfs = _stuck_detectors_three_dfs(
            last_on_time=END - timedelta(minutes=10),
            last_off_time=END - timedelta(hours=2),
            on_count=3,
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            aggregate_side_effect=dfs,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
                "threshold_minutes": 30,
            },
        )
        assert resp.status_code == 200
        assert resp.json() == []

    def test_stuck_detectors_no_off_event_is_stuck(self, monkeypatch):
        """If last_off is absent for a channel that has last_on, the
        detector is treated as stuck (current behaviour: last_off_time
        is None → stuck if duration >= threshold)."""
        app = _create_test_app()
        _add_access_overrides(app)
        # Channel with last_on but no last_off — supply a last_on row and
        # an empty last_off DF.
        last_on_df = pd.DataFrame(
            [
                {
                    "signal_id": "SIG-001",
                    "event_param": 7,
                    "agg_0": END - timedelta(hours=2),
                }
            ]
        )
        empty_off_df = pd.DataFrame(
            {"signal_id": [], "event_param": [], "agg_0": []}
        )
        on_count_df = pd.DataFrame(
            [
                {
                    "signal_id": "SIG-001",
                    "event_param": 7,
                    "agg_0": 4,
                }
            ]
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            aggregate_side_effect=[last_on_df, empty_off_df, on_count_df],
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
                "threshold_minutes": 30,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["detector_channel"] == 7
        assert data[0]["status"] == "STUCK_ON"

    def test_stuck_detectors_passes_signal_id_filter_to_sdk(
        self, monkeypatch
    ):
        """When ``signal_id`` query param is given, the SDK call must
        receive it (as ``signal_id_or_ids``). Fleet-wide behaviour (no
        signal_id) is exercised in a separate test."""
        app = _create_test_app()
        _add_access_overrides(app)
        empty_df = pd.DataFrame(
            {"signal_id": [], "event_param": [], "agg_0": []}
        )
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            aggregate_side_effect=[empty_df, empty_df, empty_df],
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        # Every aggregate_events call must reference "SIG-001" somewhere
        # in its arguments (positional or kw).
        for call in mocks["mock_aggregate"].await_args_list:
            flat = list(call.args) + list(call.kwargs.values())
            assert "SIG-001" in flat

    def test_stuck_detectors_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_stuck_detectors_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]

    def test_stuck_detectors_no_signal_id_passes_all_to_sdk(
        self, monkeypatch
    ):
        """When the ``signal_id`` query param is omitted, the endpoint
        performs fleet-wide aggregation: every aggregate_events call must
        receive ``signal_id_or_ids="All"`` (positional or keyword)."""
        app = _create_test_app()
        _add_access_overrides(app)
        empty_df = pd.DataFrame(
            {"signal_id": [], "event_param": [], "agg_0": []}
        )
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.detectors",
            aggregate_side_effect=[empty_df, empty_df, empty_df],
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        # Spec pins 3 SDK calls (last_on, last_off, on_count). Pinning the
        # count guards against the vacuous-loop trap below.
        assert mocks["mock_aggregate"].await_count == 3
        # Every aggregate_events call must reference "All" somewhere in
        # its arguments (positional or kw) — the fleet-wide sentinel.
        for call in mocks["mock_aggregate"].await_args_list:
            flat = list(call.args) + list(call.kwargs.values())
            assert "All" in flat


# ---------------------------------------------------------------------------
# detector_health — 1 aggregation (on_count/off_count/last_on/last_off)
# ---------------------------------------------------------------------------


class TestDetectorHealthTierAware:
    """Stage 2b — detector_health swaps its one SQL aggregation for an
    ``aggregate_events`` SDK call and applies request guards."""

    @staticmethod
    def _single_row_df(*, on_count, off_count, last_on, last_off):
        """A one-row aggregation DF carrying on/off counts + last timestamps.

        The endpoint reads these four scalars off the (single) returned
        row; the SDK contract guarantees one row per group_by combination
        and detector_health has no group_by, so it gets a one-row frame.

        We name columns descriptively (rather than ``agg_0..3``) because
        the contract test just needs the response to be correct given the
        DF — the implementer is free to either read the named columns
        directly or map ``agg_<i>`` to its semantic name.
        """
        return pd.DataFrame(
            [
                {
                    "on_count": on_count,
                    "off_count": off_count,
                    "last_on": last_on,
                    "last_off": last_off,
                }
            ]
        )

    def test_detector_health_calls_aggregate_events(self, monkeypatch):
        """aggregate_events awaited exactly once with signal_id="SIG-001"
        and a non-empty time window; session.execute NOT used."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._single_row_df(
            on_count=100,
            off_count=100,
            last_on=END - timedelta(seconds=30),
            last_off=END - timedelta(seconds=20),
        )
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/detector",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        mocks["mock_aggregate"].assert_awaited()
        # Signal_id propagated.
        first_call = mocks["mock_aggregate"].await_args_list[0]
        flat = list(first_call.args) + list(first_call.kwargs.values())
        assert "SIG-001" in flat
        assert mocks["mock_session"].execute.await_count == 0

    def test_detector_health_healthy_response_shape(self, monkeypatch):
        """When counts are balanced and there is no stuck detector, the
        response carries score=100, grade=Excellent, HEALTHY status."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._single_row_df(
            on_count=10,
            off_count=10,
            last_on=END - timedelta(seconds=30),
            last_off=END - timedelta(seconds=20),  # last_off > last_on so not stuck
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/detector",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signal_id"] == "SIG-001"
        assert data["detector_channel"] == 5
        assert data["score"] == 100
        assert data["grade"] == "Excellent"
        assert data["status"] == "HEALTHY"
        # Pydantic schema field shape preserved.
        assert set(data["factors"].keys()) == {
            "stuck_penalty",
            "chatter_penalty",
            "variance_penalty",
            "activity_penalty",
            "balance_penalty",
        }

    def test_detector_health_empty_aggregation_returns_neutral_response(
        self, monkeypatch
    ):
        """When the SDK returns a row with zero counts and None
        timestamps (no events for the channel in the window), the
        endpoint still returns a 200 with the existing-contract response
        shape and a score derived from the empty data."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._single_row_df(
            on_count=0,
            off_count=0,
            last_on=None,
            last_off=None,
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/detector",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        # Schema preserved even when there is no data.
        assert "score" in data
        assert "factors" in data
        assert data["signal_id"] == "SIG-001"
        assert data["detector_channel"] == 5

    def test_detector_health_stuck_penalty(self, monkeypatch):
        """Ported from TestDetectorHealthScoring: detector with
        last_on > last_off and stuck > 30 min gets factors.stuck_penalty=-30.

        The test feeds the aggregate_events DataFrame the SQL→SDK swap now
        produces (on/off counts + last_on/last_off timestamps) so the
        endpoint reaches the same `_stuck_penalty` branch via the new path.
        """
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._single_row_df(
            on_count=100,
            off_count=99,
            # last ON 45 min before END, last OFF 50 min before END.
            # last_on > last_off and stuck_seconds = 2700 > 1800 → -30.
            last_on=END - timedelta(minutes=45),
            last_off=END - timedelta(minutes=50),
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/detector",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["factors"]["stuck_penalty"] == -30

    def test_detector_health_chatter_penalty(self, monkeypatch):
        """Ported from TestDetectorHealthScoring: detector with >2000
        actuations/hour gets factors.chatter_penalty=-20."""
        app = _create_test_app()
        _add_access_overrides(app)
        # 3000 ON events over a 1-hour window → 3000/h > 2000 → -20.
        df = self._single_row_df(
            on_count=3000,
            off_count=2998,
            last_on=END - timedelta(seconds=10),
            last_off=END - timedelta(seconds=5),
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/detector",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["factors"]["chatter_penalty"] == -20

    def test_detector_health_no_off_events_balance_penalty(self, monkeypatch):
        """Ported from TestDetectorHealthScoring: detector with ON events
        but zero OFF events gets factors.balance_penalty=-20."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._single_row_df(
            on_count=50,
            off_count=0,
            last_on=END - timedelta(seconds=10),
            last_off=None,
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/detector",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["factors"]["balance_penalty"] == -20

    def test_detector_health_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/detector",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_detector_health_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/detector",
            params={
                "signal_id": "SIG-001",
                "detector_channel": 5,
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# signal_health — 3 aggregations (det, phase, total) move to SDK; coord
# is already Stage 2a via fetch_events and stays put.
# ---------------------------------------------------------------------------


class TestSignalHealthStage2bTierAware:
    """Stage 2b — signal_health swaps three SQL aggregations for SDK calls.

    Coverage:
      * det_query (per-channel on_count/off_count)
      * phase_query (per-phase green_count)
      * comm_query (total event count over window)

    The coord_query is already Stage 2a tier-aware and continues to use
    ``fetch_events`` — those tests live in TestSignalHealthTierAware above
    and are NOT re-tested here.
    """

    @staticmethod
    def _det_df():
        return pd.DataFrame(
            [
                {"event_param": 1, "on_count": 10, "off_count": 10},
                {"event_param": 2, "on_count": 20, "off_count": 20},
            ]
        )

    @staticmethod
    def _phase_df():
        return pd.DataFrame(
            [
                {"event_param": 2, "green_count": 50},
                {"event_param": 4, "green_count": 50},
            ]
        )

    @staticmethod
    def _comm_df(total_events):
        """A one-row DF carrying the total event count over the window."""
        return pd.DataFrame([{"agg_0": total_events}])

    def test_signal_health_calls_aggregate_events_for_three_components(
        self, monkeypatch
    ):
        """All three SQL aggregations (det, phase, comm) move to
        aggregate_events → SDK awaited 3 times. The coord component stays
        on fetch_events → mock_fetch awaited once. session.execute is
        no longer used for the swapped aggregations (await_count == 0)."""
        app = _create_test_app()
        _add_access_overrides(app)
        det_df = self._det_df()
        phase_df = self._phase_df()
        comm_df = self._comm_df(1000)
        coord_df = _events_df([
            (1, 2, END - timedelta(seconds=240)),
            (1, 2, END - timedelta(seconds=120)),
            (1, 2, END),
        ])
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_side_effect=[det_df, phase_df, comm_df],
            fetch_events_df=coord_df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/signal",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        # 3 swapped aggregations → 3 SDK invocations.
        assert mocks["mock_aggregate"].await_count == 3
        # Coord component still tier-aware via fetch_events (Stage 2a).
        assert mocks["mock_fetch"].await_count == 1
        # No raw SQL aggregations left.
        assert mocks["mock_session"].execute.await_count == 0

    def test_signal_health_returns_full_response_shape(self, monkeypatch):
        """Response carries the documented 4-component shape: detector_health,
        phase_health, coordination_health, communication_health."""
        app = _create_test_app()
        _add_access_overrides(app)
        det_df = self._det_df()
        phase_df = self._phase_df()
        comm_df = self._comm_df(1000)
        coord_df = _events_df([
            (1, 2, END - timedelta(seconds=240)),
            (1, 2, END - timedelta(seconds=120)),
            (1, 2, END),
        ])
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_side_effect=[det_df, phase_df, comm_df],
            fetch_events_df=coord_df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/signal",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signal_id"] == "SIG-001"
        assert set(data["components"].keys()) == {
            "detector_health",
            "phase_health",
            "coordination_health",
            "communication_health",
        }
        # Each component shape: {score: int, weight: float}.
        for comp in data["components"].values():
            assert "score" in comp
            assert "weight" in comp
        assert "overall_score" in data
        assert "overall_grade" in data
        assert isinstance(data["issues"], list)

    def test_signal_health_low_event_rate_adds_comm_issue(self, monkeypatch):
        """When comm aggregation returns a very small event count, the
        response includes a 'Low event rate' issue (existing behaviour
        preserved across the SQL→SDK swap)."""
        app = _create_test_app()
        _add_access_overrides(app)
        det_df = self._det_df()
        phase_df = self._phase_df()
        # Only 5 events over a 1-hour window → rate is 5/hour < 10/hour.
        comm_df = self._comm_df(5)
        coord_df = _events_df([])
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_side_effect=[det_df, phase_df, comm_df],
            fetch_events_df=coord_df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/signal",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        joined = " ".join(data["issues"]).lower()
        assert "low event rate" in joined or "communication" in joined
        # Ported from removed TestSignalHealthIssues: comm score drops to 50.
        assert data["components"]["communication_health"]["score"] == 50

    def test_signal_health_empty_aggregations_return_valid_response(
        self, monkeypatch
    ):
        """When every aggregation comes back empty, the endpoint still
        produces a well-formed response (empty issues list, components
        present)."""
        app = _create_test_app()
        _add_access_overrides(app)
        empty_det_df = pd.DataFrame(
            {"event_param": [], "on_count": [], "off_count": []}
        )
        empty_phase_df = pd.DataFrame(
            {"event_param": [], "green_count": []}
        )
        comm_df = self._comm_df(0)
        coord_df = _events_df([])
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_side_effect=[empty_det_df, empty_phase_df, comm_df],
            fetch_events_df=coord_df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/signal",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert set(data["components"].keys()) == {
            "detector_health",
            "phase_health",
            "coordination_health",
            "communication_health",
        }
        assert isinstance(data["issues"], list)

    def test_signal_health_phase_skip_rate_drops_phase_score(
        self, monkeypatch
    ):
        """When one phase has dramatically fewer greens than the max
        across phases (skip rate > 10%), phase_health score drops below
        100 and an issue is logged. This exercises the SDK→Pydantic
        mapping path for the per-phase aggregation."""
        app = _create_test_app()
        _add_access_overrides(app)
        det_df = self._det_df()
        # Phase 2 = 100 greens, phase 4 = 50 greens → skip rate ~50%.
        phase_df = pd.DataFrame(
            [
                {"event_param": 2, "green_count": 100},
                {"event_param": 4, "green_count": 50},
            ]
        )
        comm_df = self._comm_df(1000)
        coord_df = _events_df([])
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            aggregate_side_effect=[det_df, phase_df, comm_df],
            fetch_events_df=coord_df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/signal",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        # phase_health component should have a score < 100 because of skip.
        assert data["components"]["phase_health"]["score"] < 100
        joined = " ".join(data["issues"]).lower()
        assert "skip" in joined

    def test_signal_health_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/signal",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_signal_health_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.health",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/health/signal",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# skipped_phases — 1 aggregation (per-phase green_count)
# ---------------------------------------------------------------------------


class TestSkippedPhasesTierAware:
    """Stage 2b — skipped_phases swaps its per-phase SQL aggregation for
    an ``aggregate_events`` SDK call and applies request guards."""

    @staticmethod
    def _phases_df(phase_greens):
        """phase_greens: list of (phase, green_count) tuples."""
        return pd.DataFrame(
            [
                {"event_param": phase, "green_count": green}
                for phase, green in phase_greens
            ]
        )

    def test_skipped_phases_calls_aggregate_events(self, monkeypatch):
        """aggregate_events awaited; session.execute NOT used."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._phases_df([(2, 100), (4, 90), (6, 80)])
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/skipped",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        mocks["mock_aggregate"].assert_awaited()
        first_call = mocks["mock_aggregate"].await_args_list[0]
        flat = list(first_call.args) + list(first_call.kwargs.values())
        assert "SIG-001" in flat
        assert mocks["mock_session"].execute.await_count == 0

    def test_skipped_phases_empty_aggregation_returns_empty_list(
        self, monkeypatch
    ):
        """When aggregate_events returns an empty DataFrame, response is []."""
        app = _create_test_app()
        _add_access_overrides(app)
        empty_df = pd.DataFrame({"event_param": [], "green_count": []})
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            aggregate_returns=empty_df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/skipped",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        assert resp.json() == []

    def test_skipped_phases_computes_skip_rate_from_max(
        self, monkeypatch
    ):
        """Phase with the most greens defines expected_cycles. Others
        receive skip_count = max - actual and skip_rate_pct ≈ that ratio."""
        app = _create_test_app()
        _add_access_overrides(app)
        # Phase 2 has 100 greens (max); phase 4 has 80 → skip 20, rate 20%.
        df = self._phases_df([(2, 100), (4, 80)])
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/skipped",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        by_phase = {item["phase"]: item for item in data}
        # Phase 4: 100 - 80 = 20 skipped, 20.0% rate.
        assert by_phase[4]["expected_cycles"] == 100
        assert by_phase[4]["actual_cycles"] == 80
        assert by_phase[4]["skip_count"] == 20
        assert by_phase[4]["skip_rate_pct"] == 20.0
        # Phase 2 (the max): 0 skipped, 0% rate.
        assert by_phase[2]["skip_count"] == 0
        assert by_phase[2]["skip_rate_pct"] == 0.0

    def test_skipped_phases_single_phase_returns_zero_skip(
        self, monkeypatch
    ):
        """When only one phase has greens, it is by definition the max,
        so skip_count = 0 and skip_rate_pct = 0.0."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._phases_df([(2, 50)])
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/skipped",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["phase"] == 2
        assert data[0]["skip_count"] == 0
        assert data[0]["skip_rate_pct"] == 0.0

    def test_skipped_phases_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/skipped",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_skipped_phases_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/skipped",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# phase_terminations — 1 aggregation with 4 count_if columns + total_cycles
# ---------------------------------------------------------------------------


class TestPhaseTerminationsTierAware:
    """Stage 2b — phase_terminations swaps its per-phase SQL aggregation
    (total_cycles + gap_outs + max_outs + force_offs) for an
    ``aggregate_events`` SDK call and applies request guards."""

    @staticmethod
    def _phase_term_df(rows):
        """rows: list of (phase, total_cycles, gap_outs, max_outs, force_offs)."""
        return pd.DataFrame(
            [
                {
                    "event_param": p,
                    "total_cycles": tc,
                    "gap_outs": go,
                    "max_outs": mo,
                    "force_offs": fo,
                }
                for p, tc, go, mo, fo in rows
            ]
        )

    def test_phase_terminations_calls_aggregate_events(self, monkeypatch):
        """aggregate_events awaited at least once with signal_id; no
        session.execute for the CEL aggregation."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._phase_term_df([(2, 100, 60, 20, 20)])
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/terminations",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        mocks["mock_aggregate"].assert_awaited()
        first_call = mocks["mock_aggregate"].await_args_list[0]
        flat = list(first_call.args) + list(first_call.kwargs.values())
        assert "SIG-001" in flat
        assert mocks["mock_session"].execute.await_count == 0

    def test_phase_terminations_response_shape(self, monkeypatch):
        """Each phase row maps to a PhaseTerminationItem carrying
        signal_id, phase, gap_outs, max_outs, force_offs, skips,
        total_cycles."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._phase_term_df(
            [
                (2, 100, 60, 20, 15),
                (4, 80, 50, 10, 20),
            ]
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            aggregate_returns=df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/terminations",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 2
        by_phase = {item["phase"]: item for item in data}
        assert by_phase[2]["signal_id"] == "SIG-001"
        assert by_phase[2]["total_cycles"] == 100
        assert by_phase[2]["gap_outs"] == 60
        assert by_phase[2]["max_outs"] == 20
        assert by_phase[2]["force_offs"] == 15
        # 'skips' is the current static-zero contract; preserved.
        assert by_phase[2]["skips"] == 0
        # Field-set parity with the Pydantic model.
        assert set(by_phase[2].keys()) == {
            "signal_id",
            "phase",
            "gap_outs",
            "max_outs",
            "force_offs",
            "skips",
            "total_cycles",
        }
        # Second row mapped correctly.
        assert by_phase[4]["total_cycles"] == 80
        assert by_phase[4]["gap_outs"] == 50

    def test_phase_terminations_empty_aggregation_returns_empty_list(
        self, monkeypatch
    ):
        """When aggregate_events returns an empty DataFrame, response is []."""
        app = _create_test_app()
        _add_access_overrides(app)
        empty_df = pd.DataFrame(
            {
                "event_param": [],
                "total_cycles": [],
                "gap_outs": [],
                "max_outs": [],
                "force_offs": [],
            }
        )
        _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            aggregate_returns=empty_df,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/terminations",
            params={
                "signal_id": "SIG-001",
                "start": START.isoformat(),
                "end": END.isoformat(),
            },
        )
        assert resp.status_code == 200
        assert resp.json() == []

    def test_phase_terminations_start_predates_max_lookback_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            max_lookback_days=7,
        )
        mocks["mock_lookback"].side_effect = _raise_lookback_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/terminations",
            params={
                "signal_id": "SIG-001",
                "start": _isoformat_old(30),
                "end": _isoformat_old(29),
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_phase_terminations_window_exceeds_max_aggregation_days_returns_400(
        self, monkeypatch
    ):
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = _wire_analytics_v2b(
            app, monkeypatch,
            "tsigma.api.v1.analytics.phases",
            max_aggregation_days=7,
        )
        mocks["mock_aggregation"].side_effect = _raise_aggregation_exc

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/terminations",
            params={
                "signal_id": "SIG-001",
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]
