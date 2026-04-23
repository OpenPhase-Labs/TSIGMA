"""
Analytics API Pydantic schemas.

Response models for analytics endpoints (detector, phase,
coordination, preemption, and health analytics).
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Detector analytics
# ---------------------------------------------------------------------------


class StuckDetectorItem(BaseModel):
    """Single stuck detector result."""

    signal_id: str
    detector_channel: int
    status: str
    duration_seconds: float
    last_event_time: datetime
    event_count: int


class GapAnalysisItem(BaseModel):
    """Gap analysis result for one detector in one period."""

    signal_id: str
    detector_channel: int
    period_start: datetime
    period_end: datetime
    total_actuations: int
    avg_gap_seconds: float
    min_gap_seconds: float
    max_gap_seconds: float
    gap_out_count: int
    max_out_count: int


class OccupancyBin(BaseModel):
    """Single time bin in an occupancy response."""

    bin_start: datetime
    bin_end: datetime
    occupancy_pct: float


class OccupancyResponse(BaseModel):
    """Detector occupancy response with time bins."""

    signal_id: str
    detector_channel: int
    bins: list[OccupancyBin]


# ---------------------------------------------------------------------------
# Phase analytics
# ---------------------------------------------------------------------------


class SkippedPhaseItem(BaseModel):
    """Skipped phase analysis for one phase in one period."""

    signal_id: str
    phase: int
    expected_cycles: int
    actual_cycles: int
    skip_count: int
    skip_rate_pct: float
    period_start: datetime
    period_end: datetime


class SplitMonitorItem(BaseModel):
    """Split monitor result for one phase in one period."""

    signal_id: str
    phase: int
    period_start: datetime
    period_end: datetime
    cycle_count: int
    avg_green_seconds: float
    min_green_seconds: float
    max_green_seconds: float
    avg_yellow_seconds: float
    avg_red_clearance_seconds: float
    gap_out_pct: float
    max_out_pct: float
    force_off_pct: float


class PhaseTerminationItem(BaseModel):
    """Phase termination counts for one phase."""

    signal_id: str
    phase: int
    gap_outs: int
    max_outs: int
    force_offs: int
    skips: int
    total_cycles: int


# ---------------------------------------------------------------------------
# Coordination analytics
# ---------------------------------------------------------------------------


class OffsetDriftResponse(BaseModel):
    """Offset drift analysis result."""

    signal_id: str
    period_start: datetime
    period_end: datetime
    expected_cycle_seconds: int
    cycle_count: int
    avg_drift_seconds: float
    max_drift_seconds: float
    drift_stddev: float


class PatternChangeItem(BaseModel):
    """Single coordination pattern change event."""

    timestamp: datetime
    from_pattern: int
    to_pattern: int
    duration_seconds: Optional[float] = None


class CoordinationQualityResponse(BaseModel):
    """Coordination quality analysis result."""

    signal_id: str
    period_start: datetime
    period_end: datetime
    total_cycles: int
    cycles_within_tolerance: int
    quality_pct: float
    avg_offset_error_seconds: float


# ---------------------------------------------------------------------------
# Preemption analytics
# ---------------------------------------------------------------------------


class PreemptionSummaryResponse(BaseModel):
    """Preemption summary for a signal over a period."""

    signal_id: str
    period_start: datetime
    period_end: datetime
    total_preemptions: int
    by_preempt_number: dict[str, int]
    avg_duration_seconds: float
    max_duration_seconds: float
    total_preemption_time_seconds: float
    pct_time_preempted: float


class PreemptionRecoveryItem(BaseModel):
    """Single preemption recovery event."""

    preempt_end_time: datetime
    recovery_complete_time: datetime
    recovery_seconds: float


class PreemptionRecoveryResponse(BaseModel):
    """Preemption recovery time analysis."""

    items: list[PreemptionRecoveryItem]
    avg_recovery_seconds: float
    max_recovery_seconds: float


# ---------------------------------------------------------------------------
# Health analytics
# ---------------------------------------------------------------------------


class DetectorHealthFactors(BaseModel):
    """Individual health score penalty factors."""

    stuck_penalty: int
    chatter_penalty: int
    variance_penalty: int
    activity_penalty: int
    balance_penalty: int


class DetectorHealthResponse(BaseModel):
    """Detector health score result."""

    signal_id: str
    detector_channel: int
    score: int
    grade: str
    factors: DetectorHealthFactors
    status: str


class SignalHealthComponent(BaseModel):
    """Single component of a signal health score."""

    score: int
    weight: float


class SignalHealthResponse(BaseModel):
    """Signal-level health score with component breakdown."""

    signal_id: str
    overall_score: int
    overall_grade: str
    components: dict[str, SignalHealthComponent]
    issues: list[str]
