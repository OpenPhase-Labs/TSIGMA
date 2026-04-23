"""
Unit tests for tsigma.config_resolver.

Tests the dataclass snapshot types and SignalConfig logic.
Database-dependent functions (get_config_at, etc.) are tested with
mocked AsyncSession objects.
"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from tsigma.config_resolver import (
    ApproachSnapshot,
    DetectorSnapshot,
    SignalConfig,
    _has_changes_after,
    _load_audit_config,
    _load_live_config,
    _reconstruct_approaches,
    _reconstruct_detectors,
    get_config_at,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_approach_snap(
    approach_id="app-1",
    signal_id="sig-1",
    protected_phase=2,
    permissive_phase=None,
    ped_phase=8,
):
    return ApproachSnapshot(
        approach_id=approach_id,
        signal_id=signal_id,
        direction_type_id=1,
        protected_phase_number=protected_phase,
        permissive_phase_number=permissive_phase,
        is_protected_phase_overlap=False,
        is_permissive_phase_overlap=False,
        ped_phase_number=ped_phase,
        mph=35,
        description="NB Thru",
    )


def _make_detector_snap(
    detector_id="det-1",
    approach_id="app-1",
    channel=2,
):
    return DetectorSnapshot(
        detector_id=detector_id,
        approach_id=approach_id,
        detector_channel=channel,
        distance_from_stop_bar=400,
        min_speed_filter=None,
        lane_number=1,
    )


# ---------------------------------------------------------------------------
# ApproachSnapshot
# ---------------------------------------------------------------------------

class TestApproachSnapshotFromOrm:
    """Tests for ApproachSnapshot.from_orm()."""

    def test_basic_conversion(self):
        orm = MagicMock(
            approach_id="aaa",
            signal_id="sig-1",
            direction_type_id=3,
            protected_phase_number=2,
            permissive_phase_number=6,
            is_protected_phase_overlap=True,
            is_permissive_phase_overlap=False,
            ped_phase_number=8,
            mph=45,
            description="EB Left",
        )
        snap = ApproachSnapshot.from_orm(orm)
        assert snap.approach_id == "aaa"
        assert snap.signal_id == "sig-1"
        assert snap.direction_type_id == 3
        assert snap.protected_phase_number == 2
        assert snap.is_protected_phase_overlap is True
        assert snap.mph == 45

    def test_override_approach_id(self):
        orm = MagicMock(approach_id="original")
        snap = ApproachSnapshot.from_orm(orm, approach_id="override")
        assert snap.approach_id == "override"


class TestApproachSnapshotFromAudit:
    """Tests for ApproachSnapshot.from_audit()."""

    def test_full_values(self):
        vals = {
            "signal_id": "sig-2",
            "direction_type_id": 5,
            "protected_phase_number": 4,
            "permissive_phase_number": 8,
            "is_protected_phase_overlap": True,
            "is_permissive_phase_overlap": True,
            "ped_phase_number": 12,
            "mph": 25,
            "description": "WB Thru",
        }
        snap = ApproachSnapshot.from_audit("app-99", vals, "sig-fallback")
        assert snap.approach_id == "app-99"
        assert snap.signal_id == "sig-2"
        assert snap.direction_type_id == 5
        assert snap.description == "WB Thru"

    def test_defaults_on_empty_dict(self):
        snap = ApproachSnapshot.from_audit("app-1", {}, "sig-1")
        assert snap.signal_id == "sig-1"
        assert snap.direction_type_id == 0
        assert snap.protected_phase_number is None
        assert snap.is_protected_phase_overlap is False
        assert snap.is_permissive_phase_overlap is False
        assert snap.mph is None


# ---------------------------------------------------------------------------
# DetectorSnapshot
# ---------------------------------------------------------------------------

class TestDetectorSnapshotFromOrm:
    """Tests for DetectorSnapshot.from_orm()."""

    def test_basic_conversion(self):
        orm = MagicMock(
            detector_id="d1",
            approach_id="a1",
            detector_channel=5,
            distance_from_stop_bar=200,
            min_speed_filter=10,
            lane_number=2,
        )
        snap = DetectorSnapshot.from_orm(orm)
        assert snap.detector_id == "d1"
        assert snap.approach_id == "a1"
        assert snap.detector_channel == 5

    def test_override_detector_id(self):
        orm = MagicMock(detector_id="orig")
        snap = DetectorSnapshot.from_orm(orm, detector_id="new")
        assert snap.detector_id == "new"


class TestDetectorSnapshotFromAudit:
    """Tests for DetectorSnapshot.from_audit()."""

    def test_full_values(self):
        vals = {
            "detector_channel": 7,
            "distance_from_stop_bar": 350,
            "min_speed_filter": 15,
            "lane_number": 3,
        }
        snap = DetectorSnapshot.from_audit("det-1", "app-1", vals)
        assert snap.detector_channel == 7
        assert snap.distance_from_stop_bar == 350

    def test_defaults_on_empty_dict(self):
        snap = DetectorSnapshot.from_audit("det-1", "app-1", {})
        assert snap.detector_channel == 0
        assert snap.distance_from_stop_bar is None
        assert snap.lane_number is None


# ---------------------------------------------------------------------------
# SignalConfig
# ---------------------------------------------------------------------------

class TestSignalConfig:
    """Tests for SignalConfig methods."""

    def _make_config(self):
        approaches = [
            _make_approach_snap("app-1", protected_phase=2, permissive_phase=None),
            _make_approach_snap("app-2", protected_phase=4, permissive_phase=2),
            _make_approach_snap("app-3", protected_phase=6, permissive_phase=None, ped_phase=None),
        ]
        detectors = [
            _make_detector_snap("det-1", "app-1", channel=2),
            _make_detector_snap("det-2", "app-1", channel=18),
            _make_detector_snap("det-3", "app-2", channel=4),
            _make_detector_snap("det-4", "app-3", channel=6),
        ]
        return SignalConfig(
            signal_id="sig-1",
            as_of=datetime(2025, 1, 1),
            from_audit=False,
            approaches=approaches,
            detectors=detectors,
        )

    def test_detector_channels_for_phase_protected(self):
        cfg = self._make_config()
        # Phase 2 is protected on app-1, permissive on app-2
        channels = cfg.detector_channels_for_phase(2)
        assert channels == {2, 18, 4}

    def test_detector_channels_for_phase_no_match(self):
        cfg = self._make_config()
        channels = cfg.detector_channels_for_phase(99)
        assert channels == set()

    def test_detector_channels_for_phase_single_approach(self):
        cfg = self._make_config()
        channels = cfg.detector_channels_for_phase(6)
        assert channels == {6}

    def test_ped_phase_for_approach_found(self):
        cfg = self._make_config()
        assert cfg.ped_phase_for_approach("app-1") == 8

    def test_ped_phase_for_approach_none(self):
        cfg = self._make_config()
        assert cfg.ped_phase_for_approach("app-3") is None

    def test_ped_phase_for_approach_missing(self):
        cfg = self._make_config()
        assert cfg.ped_phase_for_approach("nonexistent") is None

    def test_approaches_for_signal(self):
        cfg = self._make_config()
        result = cfg.approaches_for_signal()
        assert len(result) == 3
        assert all(isinstance(a, ApproachSnapshot) for a in result)

    def test_detectors_for_approach(self):
        cfg = self._make_config()
        dets = cfg.detectors_for_approach("app-1")
        assert len(dets) == 2
        assert {d.detector_channel for d in dets} == {2, 18}

    def test_detectors_for_approach_empty(self):
        cfg = self._make_config()
        assert cfg.detectors_for_approach("nonexistent") == []


# ---------------------------------------------------------------------------
# get_config_at (mocked DB)
# ---------------------------------------------------------------------------

class TestGetConfigAt:
    """Tests for the async get_config_at function with mocked session."""

    def _run(self, coro):
        return asyncio.run(coro)

    @patch("tsigma.config_resolver._has_changes_after", new_callable=AsyncMock)
    @patch("tsigma.config_resolver._load_live_config", new_callable=AsyncMock)
    def test_fast_path_no_changes(self, mock_live, mock_has_changes):
        """When no changes after as_of, live config is returned."""
        mock_has_changes.return_value = False
        expected = SignalConfig("sig-1", datetime(2025, 1, 1), False, [], [])
        mock_live.return_value = expected

        session = AsyncMock()
        result = self._run(get_config_at(session, "sig-1", datetime(2025, 1, 1)))

        assert result is expected
        assert result.from_audit is False
        mock_live.assert_awaited_once()

    @patch("tsigma.config_resolver._has_changes_after", new_callable=AsyncMock)
    @patch("tsigma.config_resolver._load_audit_config", new_callable=AsyncMock)
    def test_slow_path_with_changes(self, mock_audit, mock_has_changes):
        """When changes exist after as_of, audit config is returned."""
        mock_has_changes.return_value = True
        expected = SignalConfig("sig-1", datetime(2024, 6, 1), True, [], [])
        mock_audit.return_value = expected

        session = AsyncMock()
        result = self._run(get_config_at(session, "sig-1", datetime(2024, 6, 1)))

        assert result is expected
        assert result.from_audit is True
        mock_audit.assert_awaited_once()


# ---------------------------------------------------------------------------
# _has_changes_after (mocked DB, lines 216-244)
# ---------------------------------------------------------------------------

class TestHasChangesAfter:
    """Tests for _has_changes_after with mocked session."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_scalar(self, value):
        result = MagicMock()
        result.scalar.return_value = value
        return result

    def test_returns_true_on_signal_audit_change(self):
        """Signal-level audit row after as_of triggers True."""
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=self._mock_scalar(1),  # signal audit found
        )

        result = self._run(
            _has_changes_after(session, "sig-1", datetime(2024, 1, 1))
        )
        assert result is True
        # Should short-circuit after first query
        assert session.execute.call_count == 1

    def test_returns_true_on_approach_audit_change(self):
        """Approach-level audit row triggers True when signal has none."""
        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalar(None),  # no signal audit
                self._mock_scalar(1),      # approach audit found
            ]
        )

        result = self._run(
            _has_changes_after(session, "sig-1", datetime(2024, 1, 1))
        )
        assert result is True
        assert session.execute.call_count == 2

    def test_returns_true_on_detector_audit_change(self):
        """Detector-level audit row triggers True when signal/approach have none."""
        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalar(None),  # no signal audit
                self._mock_scalar(None),  # no approach audit
                self._mock_scalar(1),      # detector audit found
            ]
        )

        result = self._run(
            _has_changes_after(session, "sig-1", datetime(2024, 1, 1))
        )
        assert result is True
        assert session.execute.call_count == 3

    def test_returns_false_when_no_changes(self):
        """All three queries return None -> False."""
        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalar(None),
                self._mock_scalar(None),
                self._mock_scalar(None),
            ]
        )

        result = self._run(
            _has_changes_after(session, "sig-1", datetime(2024, 1, 1))
        )
        assert result is False
        assert session.execute.call_count == 3


# ---------------------------------------------------------------------------
# _load_live_config (lines 254-271)
# ---------------------------------------------------------------------------

class TestLoadLiveConfig:
    """Tests for _load_live_config fast path."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_scalars_result(self, items):
        result = MagicMock()
        scalars = MagicMock()
        scalars.all.return_value = items
        result.scalars.return_value = scalars
        return result

    def _mock_all_result(self, items):
        """Mock for queries consumed via ``result.all()`` (no ``.scalars()``)."""
        result = MagicMock()
        result.all.return_value = items
        return result

    def test_loads_approaches_and_detectors(self):
        """Live config loads approaches then detectors for those approaches."""
        session = AsyncMock()

        fake_approach = MagicMock(
            approach_id="app-1",
            signal_id="sig-1",
            direction_type_id=1,
            protected_phase_number=2,
            permissive_phase_number=None,
            is_protected_phase_overlap=False,
            is_permissive_phase_overlap=False,
            ped_phase_number=8,
            mph=35,
            description="NB Thru",
        )
        fake_detector = MagicMock(
            detector_id="det-1",
            approach_id="app-1",
            detector_channel=2,
            distance_from_stop_bar=400,
            min_speed_filter=None,
            lane_number=1,
            movement_type_id=None,
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([fake_approach]),
                self._mock_all_result([]),  # movement_type lookup (empty is fine)
                self._mock_scalars_result([fake_detector]),
            ]
        )

        result = self._run(
            _load_live_config(session, "sig-1", datetime(2025, 1, 1))
        )

        assert result.signal_id == "sig-1"
        assert result.from_audit is False
        assert len(result.approaches) == 1
        assert len(result.detectors) == 1
        assert result.approaches[0].approach_id == "app-1"
        assert result.detectors[0].detector_channel == 2

    def test_no_approaches_skips_detector_query(self):
        """When no approaches exist, detector query is skipped."""
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=self._mock_scalars_result([]),
        )

        result = self._run(
            _load_live_config(session, "sig-1", datetime(2025, 1, 1))
        )

        assert result.approaches == []
        assert result.detectors == []
        # Only the approach query should have been executed (no movement
        # lookup when there are no approaches).
        assert session.execute.call_count == 1


# ---------------------------------------------------------------------------
# _reconstruct_approaches (lines 330-375)
# ---------------------------------------------------------------------------

class TestReconstructApproaches:
    """Tests for _reconstruct_approaches audit reconstruction."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_scalars_result(self, items):
        result = MagicMock()
        scalars = MagicMock()
        scalars.all.return_value = items
        result.scalars.return_value = scalars
        return result

    def test_reconstructs_from_audit_rows(self):
        """Audit rows with INSERT/UPDATE are reconstructed as snapshots."""
        session = AsyncMock()

        audit_row = MagicMock(
            approach_id="app-1",
            operation="UPDATE",
            new_values={
                "direction_type_id": 3,
                "protected_phase_number": 2,
                "mph": 45,
                "description": "EB Thru",
            },
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([audit_row]),   # audit query
                self._mock_scalars_result([]),              # live fallback (none needed)
            ]
        )

        result = self._run(
            _reconstruct_approaches(session, "sig-1", datetime(2024, 6, 1))
        )

        assert len(result) == 1
        assert result[0].approach_id == "app-1"
        assert result[0].direction_type_id == 3
        assert result[0].mph == 45

    def test_excludes_deleted_approaches(self):
        """Audit rows with DELETE operation are excluded from snapshots."""
        session = AsyncMock()

        deleted_row = MagicMock(
            approach_id="app-del",
            operation="DELETE",
            new_values=None,
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([deleted_row]),
                self._mock_scalars_result([]),
            ]
        )

        result = self._run(
            _reconstruct_approaches(session, "sig-1", datetime(2024, 6, 1))
        )

        assert len(result) == 0

    def test_falls_back_to_live_for_unaudited(self):
        """Approaches with no audit history use live data."""
        session = AsyncMock()

        live_approach = MagicMock(
            approach_id="app-live",
            signal_id="sig-1",
            direction_type_id=2,
            protected_phase_number=4,
            permissive_phase_number=None,
            is_protected_phase_overlap=False,
            is_permissive_phase_overlap=False,
            ped_phase_number=None,
            mph=30,
            description="SB Thru",
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([]),              # no audit rows
                self._mock_scalars_result([live_approach]),  # live fallback
            ]
        )

        result = self._run(
            _reconstruct_approaches(session, "sig-1", datetime(2024, 6, 1))
        )

        assert len(result) == 1
        assert result[0].approach_id == "app-live"
        assert result[0].direction_type_id == 2

    def test_deduplicates_audit_rows_keeps_latest(self):
        """When multiple audit rows exist for same approach, keep the latest."""
        session = AsyncMock()

        # Rows arrive ordered by changed_at DESC, so first seen wins
        newer_row = MagicMock(
            approach_id="app-1",
            operation="UPDATE",
            new_values={"direction_type_id": 5, "mph": 50},
        )
        older_row = MagicMock(
            approach_id="app-1",
            operation="INSERT",
            new_values={"direction_type_id": 1, "mph": 35},
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([newer_row, older_row]),
                self._mock_scalars_result([]),
            ]
        )

        result = self._run(
            _reconstruct_approaches(session, "sig-1", datetime(2024, 6, 1))
        )

        assert len(result) == 1
        assert result[0].direction_type_id == 5
        assert result[0].mph == 50


# ---------------------------------------------------------------------------
# _reconstruct_detectors (lines 389-440)
# ---------------------------------------------------------------------------

class TestReconstructDetectors:
    """Tests for _reconstruct_detectors audit reconstruction."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_scalars_result(self, items):
        result = MagicMock()
        scalars = MagicMock()
        scalars.all.return_value = items
        result.scalars.return_value = scalars
        return result

    def test_returns_empty_for_no_approaches(self):
        """Empty approach_ids list returns empty detector list."""
        session = AsyncMock()
        result = self._run(
            _reconstruct_detectors(session, [], datetime(2024, 6, 1))
        )
        assert result == []
        session.execute.assert_not_called()

    def test_reconstructs_from_audit_rows(self):
        """Detector audit rows with INSERT/UPDATE are reconstructed."""
        session = AsyncMock()

        audit_row = MagicMock(
            detector_id="det-1",
            approach_id="00000000-0000-0000-0000-000000000001",
            operation="UPDATE",
            new_values={
                "detector_channel": 7,
                "distance_from_stop_bar": 350,
                "lane_number": 2,
            },
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([audit_row]),
                self._mock_scalars_result([]),
            ]
        )

        result = self._run(
            _reconstruct_detectors(
                session,
                ["00000000-0000-0000-0000-000000000001"],
                datetime(2024, 6, 1),
            )
        )

        assert len(result) == 1
        assert result[0].detector_id == "det-1"
        assert result[0].detector_channel == 7

    def test_excludes_deleted_detectors(self):
        """Detector audit rows with DELETE are excluded."""
        session = AsyncMock()

        deleted_row = MagicMock(
            detector_id="det-del",
            approach_id="00000000-0000-0000-0000-000000000001",
            operation="DELETE",
            new_values=None,
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([deleted_row]),
                self._mock_scalars_result([]),
            ]
        )

        result = self._run(
            _reconstruct_detectors(
                session,
                ["00000000-0000-0000-0000-000000000001"],
                datetime(2024, 6, 1),
            )
        )

        assert len(result) == 0

    def test_falls_back_to_live_for_unaudited(self):
        """Detectors with no audit history use live data."""
        session = AsyncMock()

        live_det = MagicMock(
            detector_id="det-live",
            approach_id="00000000-0000-0000-0000-000000000001",
            detector_channel=3,
            distance_from_stop_bar=200,
            min_speed_filter=None,
            lane_number=1,
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([]),
                self._mock_scalars_result([live_det]),
            ]
        )

        result = self._run(
            _reconstruct_detectors(
                session,
                ["00000000-0000-0000-0000-000000000001"],
                datetime(2024, 6, 1),
            )
        )

        assert len(result) == 1
        assert result[0].detector_id == "det-live"
        assert result[0].detector_channel == 3

    def test_deduplicates_detector_audit_rows(self):
        """When multiple audit rows exist for same detector, keep the latest."""
        session = AsyncMock()

        newer = MagicMock(
            detector_id="det-1",
            approach_id="00000000-0000-0000-0000-000000000001",
            operation="UPDATE",
            new_values={"detector_channel": 9, "lane_number": 3},
        )
        older = MagicMock(
            detector_id="det-1",
            approach_id="00000000-0000-0000-0000-000000000001",
            operation="INSERT",
            new_values={"detector_channel": 2, "lane_number": 1},
        )

        session.execute = AsyncMock(
            side_effect=[
                self._mock_scalars_result([newer, older]),
                self._mock_scalars_result([]),
            ]
        )

        result = self._run(
            _reconstruct_detectors(
                session,
                ["00000000-0000-0000-0000-000000000001"],
                datetime(2024, 6, 1),
            )
        )

        assert len(result) == 1
        assert result[0].detector_channel == 9


# ---------------------------------------------------------------------------
# _load_audit_config (lines 296-303)
# ---------------------------------------------------------------------------

class TestLoadAuditConfig:
    """Tests for _load_audit_config orchestration."""

    def _run(self, coro):
        return asyncio.run(coro)

    @patch("tsigma.config_resolver._load_movement_type_map", new_callable=AsyncMock)
    @patch("tsigma.config_resolver._reconstruct_detectors", new_callable=AsyncMock)
    @patch("tsigma.config_resolver._reconstruct_approaches", new_callable=AsyncMock)
    def test_combines_approach_and_detector_snapshots(
        self, mock_approaches, mock_detectors, mock_movement_map,
    ):
        """Audit config combines reconstructed approaches and detectors."""
        app_snap = _make_approach_snap("app-1")
        det_snap = _make_detector_snap("det-1", "app-1")

        mock_approaches.return_value = [app_snap]
        mock_detectors.return_value = [det_snap]
        mock_movement_map.return_value = {}

        session = AsyncMock()
        result = self._run(
            _load_audit_config(session, "sig-1", datetime(2024, 6, 1))
        )

        assert result.signal_id == "sig-1"
        assert result.from_audit is True
        assert len(result.approaches) == 1
        assert len(result.detectors) == 1

        # Detectors called with approach IDs from reconstructed approaches
        # and the injected movement-type map.
        mock_detectors.assert_awaited_once_with(
            session, ["app-1"], datetime(2024, 6, 1), movement_map={},
        )
