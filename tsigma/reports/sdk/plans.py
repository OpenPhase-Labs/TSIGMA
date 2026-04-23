"""
SignalPlan helpers for reports that need programmed split times.
"""

from datetime import datetime

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import SignalPlan


async def fetch_plans(
    session: AsyncSession, signal_id: str, start: datetime, end: datetime
) -> list[SignalPlan]:
    """
    All plans overlapping [start, end], ordered by `effective_from`.
    """
    stmt = (
        select(SignalPlan)
        .where(SignalPlan.signal_id == signal_id)
        .where(SignalPlan.effective_from <= end)
        .where(
            or_(
                SignalPlan.effective_to.is_(None),
                SignalPlan.effective_to >= start,
            )
        )
        .order_by(SignalPlan.effective_from)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


def plan_at(plans: list[SignalPlan], moment: datetime) -> SignalPlan | None:
    """
    Return the plan active at `moment`, or None.

    Assumes `plans` is ordered by `effective_from`.
    """
    active: SignalPlan | None = None
    for plan in plans:
        if plan.effective_from > moment:
            break
        if plan.effective_to is None or plan.effective_to > moment:
            active = plan
    return active


def programmed_split(
    plans: list[SignalPlan], phase: int, moment: datetime
) -> float:
    """
    Programmed split seconds for `phase` at `moment`, or 0.0 if unknown.
    """
    plan = plan_at(plans, moment)
    if plan is None or not plan.splits:
        return 0.0
    value = plan.splits.get(str(phase))
    return float(value) if value is not None else 0.0
