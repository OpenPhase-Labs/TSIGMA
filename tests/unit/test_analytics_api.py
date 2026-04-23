"""
Unit tests for analytics API endpoints.

Tests all analytics endpoints with mocked database sessions.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

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


class TestStuckDetectors:
    """Tests for GET /api/v1/analytics/detectors/stuck."""

    def test_returns_stuck_detectors(self):
        """Test stuck detector detection."""
        # Row: last ON 2 hours ago, last OFF 3 hours ago
        row = MagicMock()
        row.signal_id = "SIG-001"
        row.detector_channel = 5
        row.last_on_time = NOW - timedelta(hours=2)
        row.last_off_time = NOW - timedelta(hours=3)
        row.event_count = 10

        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = [row]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/stuck",
            params={"signal_id": "SIG-001", "start": START.isoformat(), "end": END.isoformat()},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_empty_when_no_stuck(self):
        """Test empty list when no detectors are stuck."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/detectors/stuck")
        assert resp.status_code == 200
        assert resp.json() == []


class TestGapAnalysis:
    """Tests for GET /api/v1/analytics/detectors/gaps."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = AsyncMock()

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/detectors/gaps")
        assert resp.status_code == 422

    def test_returns_gap_analysis(self):
        """Test gap analysis computation."""
        # Two ON events with 10-second gap
        rows = [
            MagicMock(event_param=5, event_time=NOW - timedelta(seconds=20)),
            MagicMock(event_param=5, event_time=NOW - timedelta(seconds=10)),
        ]

        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = rows
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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

    def test_gap_with_detector_channel_filter(self):
        """Test gap analysis with specific detector_channel filter (line 166)."""
        rows = [
            MagicMock(event_param=3, event_time=NOW - timedelta(seconds=20)),
            MagicMock(event_param=3, event_time=NOW - timedelta(seconds=10)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = rows
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={"signal_id": "SIG-001", "detector_channel": 3},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["detector_channel"] == 3

    def test_gap_single_event_per_channel_skipped(self):
        """Test channel with <2 events is skipped (line 185)."""
        rows = [
            MagicMock(event_param=7, event_time=NOW - timedelta(seconds=10)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = rows
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/detectors/gaps",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        assert resp.json() == []


class TestOccupancyEdgeCases:
    """Tests for detector occupancy edge cases (lines 255-269)."""

    def test_occupancy_on_event_before_bin_start(self):
        """Test ON event before bin start carries into the bin (lines 255-258)."""
        # ON event before the bin, OFF event inside the bin
        events = [
            MagicMock(event_code=82, event_time=START - timedelta(seconds=30)),
            MagicMock(event_code=81, event_time=START + timedelta(seconds=60)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = events
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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

    def test_occupancy_on_without_off_carries_to_bin_end(self):
        """Test ON event with no OFF extends to bin end (line 269)."""
        # ON event in the middle of the bin, no OFF event
        events = [
            MagicMock(event_code=82, event_time=START + timedelta(seconds=30)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = events
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        mock_session = AsyncMock()

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/detectors/occupancy")
        assert resp.status_code == 422

    def test_returns_occupancy_bins(self):
        """Test occupancy bin computation."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/phases/skipped")
        assert resp.status_code == 422

    def test_returns_skipped_phases(self):
        """Test skip rate computation."""
        rows = [
            MagicMock(phase=2, green_count=100),
            MagicMock(phase=4, green_count=85),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = rows
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/skipped",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2


class TestSkippedPhasesEdgeCases:
    """Tests for skipped phases edge cases."""

    def test_empty_greens_returns_empty(self):
        """Test empty result returns [] (line 55)."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/skipped",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        assert resp.json() == []


class TestSplitMonitorEdgeCases:
    """Tests for split monitor edge cases (lines 104, 147-152, 155)."""

    def test_split_monitor_with_phase_filter(self):
        """Test split-monitor with phase query param (line 104)."""
        events = [
            MagicMock(event_code=1, event_param=4, event_time=NOW - timedelta(seconds=30)),
            MagicMock(event_code=4, event_param=4, event_time=NOW - timedelta(seconds=10)),
            MagicMock(event_code=8, event_param=4, event_time=NOW - timedelta(seconds=5)),
            MagicMock(event_code=9, event_param=4, event_time=NOW - timedelta(seconds=1)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = events
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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

    def test_split_monitor_all_termination_types(self):
        """Test gap_out, max_out, force_off counting (lines 147-152)."""
        events = [
            MagicMock(event_code=1, event_param=2, event_time=NOW - timedelta(seconds=90)),
            MagicMock(event_code=4, event_param=2, event_time=NOW - timedelta(seconds=80)),
            MagicMock(event_code=8, event_param=2, event_time=NOW - timedelta(seconds=75)),
            MagicMock(event_code=9, event_param=2, event_time=NOW - timedelta(seconds=72)),
            MagicMock(event_code=1, event_param=2, event_time=NOW - timedelta(seconds=60)),
            MagicMock(event_code=5, event_param=2, event_time=NOW - timedelta(seconds=50)),
            MagicMock(event_code=8, event_param=2, event_time=NOW - timedelta(seconds=45)),
            MagicMock(event_code=9, event_param=2, event_time=NOW - timedelta(seconds=42)),
            MagicMock(event_code=1, event_param=2, event_time=NOW - timedelta(seconds=30)),
            MagicMock(event_code=6, event_param=2, event_time=NOW - timedelta(seconds=20)),
            MagicMock(event_code=8, event_param=2, event_time=NOW - timedelta(seconds=15)),
            MagicMock(event_code=9, event_param=2, event_time=NOW - timedelta(seconds=12)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = events
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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

    def test_split_monitor_zero_cycles_skipped(self):
        """Test phase with no Green events (cycle_count==0) is skipped (line 155)."""
        # Only termination events, no Green (code 1)
        events = [
            MagicMock(event_code=4, event_param=6, event_time=NOW - timedelta(seconds=10)),
            MagicMock(event_code=5, event_param=6, event_time=NOW - timedelta(seconds=5)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = events
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/phases/split-monitor")
        assert resp.status_code == 422

    def test_returns_split_data(self):
        """Test split monitor with phase events."""
        # Green at t=0, Yellow at t=25, Red at t=29
        events = [
            MagicMock(event_code=1, event_param=2, event_time=NOW - timedelta(seconds=30)),
            MagicMock(event_code=8, event_param=2, event_time=NOW - timedelta(seconds=5)),
            MagicMock(event_code=9, event_param=2, event_time=NOW - timedelta(seconds=1)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = events
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/phases/terminations")
        assert resp.status_code == 422

    def test_returns_termination_counts(self):
        """Test termination count aggregation."""
        rows = [
            MagicMock(phase=2, total_cycles=60, gap_outs=39, max_outs=15, force_offs=6),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = rows
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/phases/terminations",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["gap_outs"] == 39


class TestOffsetDrift:
    """Tests for GET /api/v1/analytics/coordination/offset-drift."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/coordination/offset-drift")
        assert resp.status_code == 422

    def test_insufficient_data(self):
        """Test 404 with insufficient data."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = [MagicMock(event_time=NOW)]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/offset-drift",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 404

    def test_returns_drift_analysis(self):
        """Test offset drift computation."""
        times = [
            MagicMock(event_time=NOW - timedelta(seconds=240)),
            MagicMock(event_time=NOW - timedelta(seconds=120)),
            MagicMock(event_time=NOW),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = times
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/coordination/patterns")
        assert resp.status_code == 422

    def test_returns_pattern_changes(self):
        """Test pattern change history."""
        rows = [
            MagicMock(event_time=NOW - timedelta(hours=6), event_param=1),
            MagicMock(event_time=NOW, event_param=2),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = rows
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/coordination/quality")
        assert resp.status_code == 422

    def test_insufficient_data_returns_404(self):
        """Test 404 when fewer than 2 coord phase events."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = [MagicMock(event_time=NOW)]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(
            "/api/v1/analytics/coordination/quality",
            params={"signal_id": "SIG-001"},
        )
        assert resp.status_code == 404
        assert "Insufficient data" in resp.json()["detail"]

    def test_returns_quality_score(self):
        """Test coordination quality score computation (lines 138-172)."""
        # 3 events => 2 cycles, both 120s apart => avg=120, deviations=[0,0]
        times = [
            MagicMock(event_time=NOW - timedelta(seconds=240)),
            MagicMock(event_time=NOW - timedelta(seconds=120)),
            MagicMock(event_time=NOW),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = times
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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

    def test_quality_with_varied_cycles(self):
        """Test quality score when some cycles exceed tolerance."""
        # Cycles: 120s, 125s => avg=122.5, devs=[2.5, 2.5]
        # tolerance=2.0 => 0 within tolerance
        times = [
            MagicMock(event_time=NOW - timedelta(seconds=245)),
            MagicMock(event_time=NOW - timedelta(seconds=125)),
            MagicMock(event_time=NOW),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = times
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/preemptions/summary")
        assert resp.status_code == 422

    def test_returns_summary(self):
        """Test preemption summary with begin/end event pairs."""
        events = [
            MagicMock(event_code=102, event_param=1, event_time=NOW - timedelta(seconds=60)),
            MagicMock(event_code=104, event_param=1, event_time=NOW - timedelta(seconds=15)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = events
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        mock_session = AsyncMock()
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
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/health/detector")
        assert resp.status_code == 422

    def test_returns_health_score(self):
        """Test detector health scoring."""
        row = MagicMock()
        row.on_count = 500
        row.off_count = 498
        row.last_on = NOW - timedelta(seconds=10)
        row.last_off = NOW - timedelta(seconds=5)

        mock_session = AsyncMock()
        result = MagicMock()
        result.one.return_value = row
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        assert "score" in data
        assert "grade" in data
        assert "factors" in data


class TestSignalHealth:
    """Tests for GET /api/v1/analytics/health/signal."""

    def test_requires_signal_id(self):
        """Test signal_id is required."""
        mock_session = AsyncMock()
        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/analytics/health/signal")
        assert resp.status_code == 422

    def test_returns_signal_health(self):
        """Test signal health scoring."""
        # Detector query result
        det_rows = [MagicMock(channel=5, on_count=500, off_count=498)]
        # Phase query result
        phase_rows = [MagicMock(phase=2, green_count=100)]
        # Coord query result
        coord_rows = [
            MagicMock(event_time=NOW - timedelta(seconds=240)),
            MagicMock(event_time=NOW - timedelta(seconds=120)),
            MagicMock(event_time=NOW),
        ]
        # Comm query result
        comm_scalar = MagicMock()
        comm_scalar.scalar.return_value = 5000

        mock_session = AsyncMock()
        det_result = MagicMock()
        det_result.all.return_value = det_rows
        phase_result = MagicMock()
        phase_result.all.return_value = phase_rows
        coord_result = MagicMock()
        coord_result.all.return_value = coord_rows

        mock_session.execute = AsyncMock(
            side_effect=[det_result, phase_result, coord_result, comm_scalar]
        )

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        assert "overall_score" in data
        assert "overall_grade" in data
        assert "components" in data
        assert "issues" in data


class TestPreemptionRecoveryData:
    """Tests for GET /api/v1/analytics/preemptions/recovery with mock data."""

    def test_returns_recovery_items(self):
        """Test preemption recovery with end->green event pairs."""
        events = [
            MagicMock(event_code=104, event_param=1, event_time=NOW - timedelta(seconds=30)),
            MagicMock(event_code=1, event_param=2, event_time=NOW - timedelta(seconds=20)),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = events
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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

    def test_returns_empty_when_no_events(self):
        """Test preemption recovery with no events returns empty items."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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


class TestDetectorHealthScoring:
    """Tests for detector health edge cases (stuck, chatter, balance)."""

    def test_stuck_detector_penalty(self):
        """Detector with last_on > last_off and stuck > 30min gets -30."""
        row = MagicMock()
        row.on_count = 100
        row.off_count = 99
        # last ON was 45 min ago, last OFF was 50 min ago (stuck for 45 min)
        row.last_on = NOW - timedelta(minutes=45)
        row.last_off = NOW - timedelta(minutes=50)

        mock_session = AsyncMock()
        result = MagicMock()
        result.one.return_value = row
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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

    def test_chatter_detector_penalty(self):
        """Detector with >2000 actuations/hour gets chatter penalty."""
        row = MagicMock()
        row.on_count = 3000  # 3000 in 1 hour = chatter
        row.off_count = 2998
        row.last_on = NOW - timedelta(seconds=10)
        row.last_off = NOW - timedelta(seconds=5)

        mock_session = AsyncMock()
        result = MagicMock()
        result.one.return_value = row
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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

    def test_no_off_events_balance_penalty(self):
        """Detector with ON events but zero OFF events gets balance penalty."""
        row = MagicMock()
        row.on_count = 50
        row.off_count = 0
        row.last_on = NOW - timedelta(seconds=10)
        row.last_off = None

        mock_session = AsyncMock()
        result = MagicMock()
        result.one.return_value = row
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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


class TestSignalHealthIssues:
    """Tests for signal health with degraded components."""

    def test_low_event_rate_communication_issue(self):
        """Low event rate triggers communication health issue."""
        # Detector query - empty
        det_result = MagicMock()
        det_result.all.return_value = []
        # Phase query - empty
        phase_result = MagicMock()
        phase_result.all.return_value = []
        # Coord query - empty
        coord_result = MagicMock()
        coord_result.all.return_value = []
        # Comm query - very low event count
        comm_scalar = MagicMock()
        comm_scalar.scalar.return_value = 5  # 5 events in 1 hour = very low

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(
            side_effect=[det_result, phase_result, coord_result, comm_scalar]
        )

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

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
        assert any("communication" in issue.lower() for issue in data["issues"])
        assert data["components"]["communication_health"]["score"] == 50
