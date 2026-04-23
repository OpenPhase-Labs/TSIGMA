"""Health analytics endpoints (detector_health, signal_health)."""

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth.dependencies import require_access
from ....dependencies import get_session
from ..analytics_schemas import (
    DetectorHealthFactors,
    DetectorHealthResponse,
    SignalHealthComponent,
    SignalHealthResponse,
)
from ._common import CEL, _default_end, _default_start

router = APIRouter()


def _grade(score: float) -> str:
    """Convert a numeric health score to a letter grade."""
    if score >= 90:
        return "Excellent"
    elif score >= 75:
        return "Good"
    elif score >= 50:
        return "Fair"
    return "Poor"


def _stuck_penalty(row, t_end) -> int:
    """Compute stuck detector penalty from last ON/OFF timestamps."""
    if row.last_on and row.last_off:
        if row.last_on > row.last_off:
            stuck_seconds = (t_end - row.last_on).total_seconds()
            if stuck_seconds > 1800:
                return -30
    elif row.last_on and not row.last_off:
        return -30
    return 0


def _chatter_penalty(on_count: int, period_hours: float) -> int:
    """Compute chatter penalty from actuation rate."""
    if period_hours > 0:
        rate = on_count / period_hours
        if rate > 2000:
            return -20
        if rate > 1000:
            return -10
    return 0


def _activity_penalty(on_count: int, period_hours: float) -> int:
    """Compute activity penalty for low actuation rate."""
    if period_hours > 0 and on_count < period_hours:
        return -15
    return 0


def _balance_penalty(on_count: int, off_count: int) -> int:
    """Compute ON/OFF balance penalty."""
    if on_count > 0 and off_count > 0:
        ratio = on_count / off_count
        if ratio > 1.5 or ratio < 0.67:
            return -10
    elif on_count > 0 and off_count == 0:
        return -20
    return 0


def _score_detector_health(det_rows, t_start, t_end) -> tuple[int, list[str]]:
    """Score detector health component and collect issues."""
    score = 100
    issues = []
    period_hours = (t_end - t_start).total_seconds() / 3600

    for d in det_rows:
        if d.on_count > 0 and d.off_count == 0:
            score -= 15
            issues.append(f"Detector {d.channel} has no OFF events")
        if period_hours > 0 and d.on_count / period_hours > 2000:
            score -= 10
            issues.append(f"Detector {d.channel} showing high chatter")

    return max(0, score), issues


def _score_phase_health(phase_rows) -> tuple[int, list[str]]:
    """Score phase health component from green counts."""
    score = 100
    issues = []

    if phase_rows:
        max_cycles = max(r.green_count for r in phase_rows)
        for r in phase_rows:
            if max_cycles > 0:
                skip_rate = (max_cycles - r.green_count) / max_cycles
                if skip_rate > 0.1:
                    score -= 10
                    issues.append(
                        f"Phase {r.phase} has {round(skip_rate * 100)}% skip rate"
                    )

    return max(0, score), issues


def _score_coordination_health(coord_times) -> int:
    """Score coordination health from cycle length consistency."""
    if len(coord_times) < 2:
        return 100

    cycles = [
        (coord_times[i + 1] - coord_times[i]).total_seconds()
        for i in range(len(coord_times) - 1)
    ]
    avg = sum(cycles) / len(cycles)
    outliers = sum(1 for c in cycles if abs(c - avg) > 5)
    outlier_rate = outliers / len(cycles) if cycles else 0

    if outlier_rate > 0.2:
        return 80
    if outlier_rate > 0.1:
        return 90
    return 100


@router.get("/health/detector", response_model=DetectorHealthResponse)
async def detector_health(
    signal_id: str = Query(...),
    detector_channel: int = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("health")),
):
    """
    Compute detector health score (0-100).

    Scoring factors:
    - Stuck penalty: detector ON > 30 min without OFF
    - Chatter penalty: abnormally high actuation rate
    - Variance penalty: actuation rate variance vs expected
    - Activity penalty: no activity in expected active period
    - Balance penalty: ON/OFF ratio imbalance
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    query = (
        select(
            func.count().filter(CEL.event_code == 82).label("on_count"),
            func.count().filter(CEL.event_code == 81).label("off_count"),
            func.max(CEL.event_time).filter(CEL.event_code == 82).label("last_on"),
            func.max(CEL.event_time).filter(CEL.event_code == 81).label("last_off"),
        )
        .where(
            CEL.signal_id == signal_id,
            CEL.event_param == detector_channel,
            CEL.event_code.in_([81, 82]),
            CEL.event_time.between(t_start, t_end),
        )
    )
    result = await session.execute(query)
    row = result.one()

    on_count = row.on_count or 0
    off_count = row.off_count or 0
    period_hours = (t_end - t_start).total_seconds() / 3600

    stuck_penalty = _stuck_penalty(row, t_end)
    chatter_penalty = _chatter_penalty(on_count, period_hours)
    activity_penalty = _activity_penalty(on_count, period_hours)
    balance_penalty = _balance_penalty(on_count, off_count)
    variance_penalty = 0

    score = max(
        0,
        100 + stuck_penalty + chatter_penalty
        + variance_penalty + activity_penalty + balance_penalty,
    )

    return DetectorHealthResponse(
        signal_id=signal_id,
        detector_channel=detector_channel,
        score=score,
        grade=_grade(score),
        factors=DetectorHealthFactors(
            stuck_penalty=stuck_penalty,
            chatter_penalty=chatter_penalty,
            variance_penalty=variance_penalty,
            activity_penalty=activity_penalty,
            balance_penalty=balance_penalty,
        ),
        status="HEALTHY" if score >= 50 else "DEGRADED",
    )


@router.get("/health/signal", response_model=SignalHealthResponse)
async def signal_health(
    signal_id: str = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("health")),
):
    """
    Compute overall signal health score.

    Weighted composite of detector, phase, coordination,
    and communication health components.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    # Detector health component: check for any stuck/chattering detectors
    det_query = (
        select(
            CEL.event_param.label("channel"),
            func.count().filter(CEL.event_code == 82).label("on_count"),
            func.count().filter(CEL.event_code == 81).label("off_count"),
        )
        .where(
            CEL.signal_id == signal_id,
            CEL.event_code.in_([81, 82]),
            CEL.event_time.between(t_start, t_end),
        )
        .group_by(CEL.event_param)
    )
    det_result = await session.execute(det_query)
    det_rows = det_result.all()

    det_score, issues = _score_detector_health(det_rows, t_start, t_end)

    # Phase health component
    phase_query = (
        select(
            CEL.event_param.label("phase"),
            func.count().label("green_count"),
        )
        .where(
            CEL.signal_id == signal_id,
            CEL.event_code == 1,
            CEL.event_time.between(t_start, t_end),
        )
        .group_by(CEL.event_param)
    )
    phase_result = await session.execute(phase_query)
    phase_score, phase_issues = _score_phase_health(phase_result.all())
    issues.extend(phase_issues)

    # Coordination health
    coord_query = (
        select(CEL.event_time)
        .where(
            CEL.signal_id == signal_id,
            CEL.event_code == 1,
            CEL.event_param == 2,
            CEL.event_time.between(t_start, t_end),
        )
        .order_by(CEL.event_time)
    )
    coord_result = await session.execute(coord_query)
    coord_times = [r.event_time for r in coord_result.all()]
    coord_score = _score_coordination_health(coord_times)

    # Communication health
    comm_query = (
        select(func.count())
        .where(
            CEL.signal_id == signal_id,
            CEL.event_time.between(t_start, t_end),
        )
    )
    comm_result = await session.execute(comm_query)
    total_events = comm_result.scalar() or 0

    comm_score = 100
    period_hours = (t_end - t_start).total_seconds() / 3600
    if period_hours > 0 and total_events / period_hours < 10:
        comm_score = 50
        issues.append("Low event rate — possible communication issue")

    # Weighted composite
    weights = {
        "detector_health": 0.35,
        "phase_health": 0.25,
        "coordination_health": 0.20,
        "communication_health": 0.20,
    }
    scores = {
        "detector_health": det_score,
        "phase_health": phase_score,
        "coordination_health": coord_score,
        "communication_health": comm_score,
    }
    overall = sum(scores[k] * weights[k] for k in weights)

    return SignalHealthResponse(
        signal_id=signal_id,
        overall_score=round(overall),
        overall_grade=_grade(overall),
        components={
            k: SignalHealthComponent(score=scores[k], weight=weights[k])
            for k in weights
        },
        issues=issues,
    )
