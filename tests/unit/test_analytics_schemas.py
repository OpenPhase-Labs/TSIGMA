"""
Unit tests for analytics Pydantic schemas.

Tests serialization and field types for all analytics response models.
"""

from datetime import datetime, timezone

from tsigma.api.v1.analytics_schemas import (
    CoordinationQualityResponse,
    DetectorHealthFactors,
    DetectorHealthResponse,
    GapAnalysisItem,
    OccupancyBin,
    OccupancyResponse,
    OffsetDriftResponse,
    PatternChangeItem,
    PhaseTerminationItem,
    PreemptionRecoveryItem,
    PreemptionRecoveryResponse,
    PreemptionSummaryResponse,
    SignalHealthComponent,
    SignalHealthResponse,
    SkippedPhaseItem,
    SplitMonitorItem,
    StuckDetectorItem,
)

NOW = datetime.now(timezone.utc)


class TestDetectorAnalyticsSchemas:
    """Tests for detector analytics response schemas."""

    def test_stuck_detector_item(self):
        """Test StuckDetectorItem serializes correctly."""
        item = StuckDetectorItem(
            signal_id="SIG-001",
            detector_channel=5,
            status="STUCK_ON",
            duration_seconds=3600.5,
            last_event_time=NOW,
            event_count=0,
        )
        assert item.status == "STUCK_ON"
        assert item.duration_seconds == 3600.5
        assert item.event_count == 0

    def test_gap_analysis_item(self):
        """Test GapAnalysisItem serializes correctly."""
        item = GapAnalysisItem(
            signal_id="SIG-001",
            detector_channel=5,
            period_start=NOW,
            period_end=NOW,
            total_actuations=450,
            avg_gap_seconds=8.2,
            min_gap_seconds=0.5,
            max_gap_seconds=45.3,
            gap_out_count=12,
            max_out_count=3,
        )
        assert item.total_actuations == 450
        assert item.avg_gap_seconds == 8.2

    def test_occupancy_response(self):
        """Test OccupancyResponse with bins."""
        resp = OccupancyResponse(
            signal_id="SIG-001",
            detector_channel=5,
            bins=[
                OccupancyBin(bin_start=NOW, bin_end=NOW, occupancy_pct=23.5),
                OccupancyBin(bin_start=NOW, bin_end=NOW, occupancy_pct=45.2),
            ],
        )
        assert len(resp.bins) == 2
        assert resp.bins[0].occupancy_pct == 23.5


class TestPhaseAnalyticsSchemas:
    """Tests for phase analytics response schemas."""

    def test_skipped_phase_item(self):
        """Test SkippedPhaseItem serializes correctly."""
        item = SkippedPhaseItem(
            signal_id="SIG-001",
            phase=4,
            expected_cycles=100,
            actual_cycles=85,
            skip_count=15,
            skip_rate_pct=15.0,
            period_start=NOW,
            period_end=NOW,
        )
        assert item.skip_rate_pct == 15.0

    def test_split_monitor_item(self):
        """Test SplitMonitorItem serializes correctly."""
        item = SplitMonitorItem(
            signal_id="SIG-001",
            phase=2,
            period_start=NOW,
            period_end=NOW,
            cycle_count=60,
            avg_green_seconds=25.3,
            min_green_seconds=15.0,
            max_green_seconds=45.0,
            avg_yellow_seconds=4.0,
            avg_red_clearance_seconds=1.5,
            gap_out_pct=65.0,
            max_out_pct=25.0,
            force_off_pct=10.0,
        )
        assert item.cycle_count == 60
        assert item.avg_green_seconds == 25.3

    def test_phase_termination_item(self):
        """Test PhaseTerminationItem serializes correctly."""
        item = PhaseTerminationItem(
            signal_id="SIG-001",
            phase=2,
            gap_outs=39,
            max_outs=15,
            force_offs=6,
            skips=0,
            total_cycles=60,
        )
        assert item.gap_outs == 39
        assert item.total_cycles == 60


class TestCoordinationAnalyticsSchemas:
    """Tests for coordination analytics response schemas."""

    def test_offset_drift_response(self):
        """Test OffsetDriftResponse serializes correctly."""
        resp = OffsetDriftResponse(
            signal_id="SIG-001",
            period_start=NOW,
            period_end=NOW,
            expected_cycle_seconds=120,
            cycle_count=30,
            avg_drift_seconds=0.5,
            max_drift_seconds=2.3,
            drift_stddev=0.8,
        )
        assert resp.expected_cycle_seconds == 120
        assert resp.drift_stddev == 0.8

    def test_pattern_change_item(self):
        """Test PatternChangeItem serializes correctly."""
        item = PatternChangeItem(
            timestamp=NOW,
            from_pattern=1,
            to_pattern=2,
            duration_seconds=28800,
        )
        assert item.from_pattern == 1
        assert item.to_pattern == 2

    def test_pattern_change_item_optional_duration(self):
        """Test PatternChangeItem with no duration."""
        item = PatternChangeItem(
            timestamp=NOW,
            from_pattern=2,
            to_pattern=3,
        )
        assert item.duration_seconds is None

    def test_coordination_quality_response(self):
        """Test CoordinationQualityResponse serializes correctly."""
        resp = CoordinationQualityResponse(
            signal_id="SIG-001",
            period_start=NOW,
            period_end=NOW,
            total_cycles=30,
            cycles_within_tolerance=27,
            quality_pct=90.0,
            avg_offset_error_seconds=0.8,
        )
        assert resp.quality_pct == 90.0


class TestPreemptionAnalyticsSchemas:
    """Tests for preemption analytics response schemas."""

    def test_preemption_summary_response(self):
        """Test PreemptionSummaryResponse serializes correctly."""
        resp = PreemptionSummaryResponse(
            signal_id="SIG-001",
            period_start=NOW,
            period_end=NOW,
            total_preemptions=12,
            by_preempt_number={"1": 8, "2": 4},
            avg_duration_seconds=45.2,
            max_duration_seconds=120.0,
            total_preemption_time_seconds=542.4,
            pct_time_preempted=0.63,
        )
        assert resp.total_preemptions == 12
        assert resp.by_preempt_number["1"] == 8

    def test_preemption_recovery_response(self):
        """Test PreemptionRecoveryResponse serializes correctly."""
        resp = PreemptionRecoveryResponse(
            items=[
                PreemptionRecoveryItem(
                    preempt_end_time=NOW,
                    recovery_complete_time=NOW,
                    recovery_seconds=105.0,
                ),
            ],
            avg_recovery_seconds=95.5,
            max_recovery_seconds=180.0,
        )
        assert len(resp.items) == 1
        assert resp.avg_recovery_seconds == 95.5


class TestHealthAnalyticsSchemas:
    """Tests for health analytics response schemas."""

    def test_detector_health_response(self):
        """Test DetectorHealthResponse serializes correctly."""
        resp = DetectorHealthResponse(
            signal_id="SIG-001",
            detector_channel=5,
            score=85,
            grade="Good",
            factors=DetectorHealthFactors(
                stuck_penalty=0,
                chatter_penalty=-5,
                variance_penalty=-5,
                activity_penalty=0,
                balance_penalty=-5,
            ),
            status="HEALTHY",
        )
        assert resp.score == 85
        assert resp.factors.chatter_penalty == -5

    def test_signal_health_response(self):
        """Test SignalHealthResponse serializes correctly."""
        resp = SignalHealthResponse(
            signal_id="SIG-001",
            overall_score=78,
            overall_grade="Good",
            components={
                "detector_health": SignalHealthComponent(score=85, weight=0.35),
                "phase_health": SignalHealthComponent(score=70, weight=0.25),
            },
            issues=["Phase 4 has 15% skip rate"],
        )
        assert resp.overall_score == 78
        assert resp.components["detector_health"].score == 85
        assert len(resp.issues) == 1
