"""
Signals API endpoints.

CRUD operations for traffic signals/intersections plus raw IHR event-log
reads.  GET endpoints respect the 'signal_detail' access policy, except
the metric-comment overlay, which respects 'comments' like the rest of
the metric-comment surface.  POST/PUT/DELETE require admin role.
"""

from datetime import datetime
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...crypto import encrypt_sensitive_fields, has_encryption_key, redact_metadata
from ...dependencies import get_audited_session, get_session
from ...models import (
    Area,
    MetricComment,
    MetricCommentMetricType,
    Signal,
    SignalArea,
    SignalAudit,
)
from ...reports.sdk.limits import (
    require_max_aggregation_days,
    require_max_lookback,
)
from ...reports.sdk.pagination import paginated_event_list
from ...reports.sdk.queries import fetch_events
from .helpers import get_or_404
from .metric_comments import MetricCommentResponse
from .schemas import AreaResponse, SignalCreate, SignalUpdate

router = APIRouter()

# Hard ceiling for raw IHR event reads.  GraphQL uses the same default.
# A single signal at typical event rates produces ~50–100k events/day, so
# 100k caps the response at "about a day's worth" — enough for ad-hoc
# reporting without materializing multi-day windows in app memory.
_RAW_EVENTS_DEFAULT_LIMIT = 10000
_RAW_EVENTS_MAX_LIMIT = 100000


@router.get("/", response_model=List[dict])
async def list_signals(
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """
    List all signals.

    Args:
        skip: Number of records to skip (pagination).
        limit: Maximum number of records to return.
        session: Database session (injected).

    Returns:
        List of signals.
    """
    result = await session.execute(
        select(Signal).offset(skip).limit(limit)
    )
    signals = result.scalars().all()
    return [
        {
            "signal_id": s.signal_id,
            "primary_street": s.primary_street,
            "secondary_street": s.secondary_street,
            "latitude": str(s.latitude) if s.latitude else None,
            "longitude": str(s.longitude) if s.longitude else None,
            "enabled": s.enabled,
        }
        for s in signals
    ]


@router.get("/{signal_id}", response_model=dict)
async def get_signal(
    signal_id: str,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """
    Get a specific signal by ID.

    Args:
        signal_id: Signal identifier.
        session: Database session (injected).

    Returns:
        Signal details.

    Raises:
        HTTPException: 404 if signal not found.
    """
    result = await session.execute(
        select(Signal).where(Signal.signal_id == signal_id)
    )
    signal = result.scalar_one_or_none()

    if not signal:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Signal {signal_id} not found",
        )

    return {
        "signal_id": signal.signal_id,
        "primary_street": signal.primary_street,
        "secondary_street": signal.secondary_street,
        "latitude": str(signal.latitude) if signal.latitude else None,
        "longitude": str(signal.longitude) if signal.longitude else None,
        "enabled": signal.enabled,
        "metadata": redact_metadata(signal.metadata),
        "created_at": signal.created_at.isoformat(),
        "updated_at": signal.updated_at.isoformat(),
    }


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_signal(
    body: SignalCreate,
    session: AsyncSession = Depends(get_audited_session),
    _: SessionData = Depends(require_admin),
) -> dict:
    """
    Create a new signal.

    Args:
        body: Signal creation data.
        session: Database session (injected).

    Returns:
        Created signal data.

    Raises:
        HTTPException: 409 if signal_id already exists.
    """
    existing = await session.execute(
        select(Signal).where(Signal.signal_id == body.signal_id)
    )
    if existing.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Signal {body.signal_id} already exists",
        )

    signal = Signal(
        signal_id=body.signal_id,
        primary_street=body.primary_street,
        secondary_street=body.secondary_street,
        latitude=body.latitude,
        longitude=body.longitude,
        jurisdiction_id=body.jurisdiction_id,
        region_id=body.region_id,
        corridor_id=body.corridor_id,
        controller_type_id=body.controller_type_id,
        ip_address=body.ip_address,
        note=body.note,
        enabled=body.enabled,
        start_date=body.start_date,
        signal_metadata=(
            encrypt_sensitive_fields(body.metadata)
            if body.metadata and has_encryption_key()
            else body.metadata
        ),
    )
    session.add(signal)
    await session.flush()

    return {
        "signal_id": signal.signal_id,
        "primary_street": signal.primary_street,
        "secondary_street": signal.secondary_street,
        "latitude": str(signal.latitude) if signal.latitude else None,
        "longitude": str(signal.longitude) if signal.longitude else None,
        "enabled": signal.enabled,
    }


@router.put("/{signal_id}")
async def update_signal(
    signal_id: str,
    body: SignalUpdate,
    session: AsyncSession = Depends(get_audited_session),
    _: SessionData = Depends(require_admin),
) -> dict:
    """
    Update an existing signal.

    Only provided fields are updated (partial update).

    Args:
        signal_id: Signal identifier.
        body: Fields to update.
        session: Database session (injected).

    Returns:
        Updated signal data.

    Raises:
        HTTPException: 404 if signal not found.
    """
    result = await session.execute(
        select(Signal).where(Signal.signal_id == signal_id)
    )
    signal = result.scalar_one_or_none()

    if signal is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Signal {signal_id} not found",
        )

    update_data = body.model_dump(exclude_unset=True)
    # Map schema 'metadata' field to model 'signal_metadata' attribute
    if "metadata" in update_data:
        metadata = update_data.pop("metadata")
        if metadata and has_encryption_key():
            encrypt_sensitive_fields(metadata)
        update_data["signal_metadata"] = metadata

    for field, value in update_data.items():
        setattr(signal, field, value)

    await session.flush()

    return {
        "signal_id": signal.signal_id,
        "primary_street": signal.primary_street,
        "secondary_street": signal.secondary_street,
        "latitude": str(signal.latitude) if signal.latitude else None,
        "longitude": str(signal.longitude) if signal.longitude else None,
        "enabled": signal.enabled,
    }


@router.delete("/{signal_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_signal(
    signal_id: str,
    session: AsyncSession = Depends(get_audited_session),
    _: SessionData = Depends(require_admin),
):
    """
    Delete a signal.

    Args:
        signal_id: Signal identifier.
        session: Database session (injected).

    Raises:
        HTTPException: 404 if signal not found.
    """
    result = await session.execute(
        select(Signal).where(Signal.signal_id == signal_id)
    )
    signal = result.scalar_one_or_none()

    if signal is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Signal {signal_id} not found",
        )

    await session.delete(signal)
    await session.flush()


@router.get("/{signal_id}/audit")
async def list_signal_audit(
    signal_id: str,
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """
    Get audit trail for a signal.

    Returns change history ordered by most recent first.

    Args:
        signal_id: Signal identifier.
        skip: Number of records to skip (pagination).
        limit: Maximum number of records to return.
        session: Database session (injected).

    Returns:
        List of audit records.

    Raises:
        HTTPException: 404 if signal not found.
    """
    signal_result = await session.execute(
        select(Signal).where(Signal.signal_id == signal_id)
    )
    if signal_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Signal {signal_id} not found",
        )

    result = await session.execute(
        select(SignalAudit)
        .where(SignalAudit.signal_id == signal_id)
        .order_by(SignalAudit.changed_at.desc())
        .offset(skip)
        .limit(limit)
    )
    rows = result.scalars().all()

    return [
        {
            "audit_id": row.audit_id,
            "signal_id": row.signal_id,
            "changed_at": row.changed_at.isoformat(),
            "changed_by": row.changed_by,
            "operation": row.operation,
            "old_values": row.old_values,
            "new_values": row.new_values,
        }
        for row in rows
    ]


@router.get("/{signal_id}/areas", response_model=list[AreaResponse])
async def list_signal_areas(
    signal_id: str,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all areas a signal belongs to."""
    await get_or_404(session, Signal, Signal.signal_id, signal_id, "Signal")

    result = await session.execute(
        select(Area)
        .join(SignalArea, SignalArea.area_id == Area.area_id)
        .where(SignalArea.signal_id == signal_id)
    )
    return result.scalars().all()


@router.get(
    "/{signal_id}/metric-comments",
    response_model=list[MetricCommentResponse],
)
async def list_signal_metric_comments(
    signal_id: str,
    metric_type: Optional[List[str]] = Query(
        None,
        description=(
            "Repeatable metric-type key (e.g. ``?metric_type=a&metric_type=b``). "
            "A comment matches when ANY of its metric types is requested.  Omit "
            "for no metric-type filtering at all, in which case a comment with "
            "no metric-type associations is still returned."
        ),
    ),
    start: Optional[datetime] = Query(
        None,
        description=(
            "Inclusive lower bound of the chart window (ISO-8601 datetime).  "
            "Omit ``start`` and ``end`` to return every comment on the signal."
        ),
    ),
    end: Optional[datetime] = Query(
        None,
        description=(
            "Inclusive upper bound of the chart window (ISO-8601 datetime)."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("comments")),
):
    """
    Metric comments to overlay on a signal's charts.

    This is a **side fetch**: the client asks for the chart window it is
    drawing and gets back the annotations for it.  Comments are never
    embedded in a report response -- reports are becoming gRPC plugins, and
    embedding would need either a broker RPC or an envelope outside the
    published contract.

    Reads are gated on the ``comments`` access policy, the same category the
    rest of the metric-comment surface uses -- not ``signal_detail``.

    A comment has exactly one of three anchor states, and matching follows
    from the state:

    - **unanchored** (both bounds NULL) -- always matches.  The note annotates
      the chart, not a moment on it.
    - **point** (``anchor_start`` only) -- matches when ``anchor_start`` falls
      within the window.
    - **range** (both bounds) -- matches when ``[anchor_start, anchor_end]``
      overlaps the window.

    A row with ``anchor_end`` and no ``anchor_start`` is rejected by a
    CheckConstraint and by the create/update validators, so no branch here
    accounts for it.  Point and range share one predicate by reading a NULL
    ``anchor_end`` as "ends where it starts" -- note this is the opposite of
    ``SignalPlan.effective_to``, where NULL means open-ended.

    Args:
        signal_id: Signal identifier.
        metric_type: Repeatable metric-type key; ANY-of semantics.
        start: Inclusive lower bound of the chart window.
        end: Inclusive upper bound of the chart window.
        session: Database session (injected).

    Returns:
        The signal's matching comments, oldest first.

    Raises:
        HTTPException: 404 if the signal does not exist.
    """
    await get_or_404(session, Signal, Signal.signal_id, signal_id, "Signal")

    stmt = select(MetricComment).where(MetricComment.signal_id == signal_id)

    # ANY-of over a many-to-many.  A subquery rather than a JOIN: joining
    # duplicates the comment row once per matching metric type.
    if metric_type:
        annotated = (
            select(MetricCommentMetricType.comment_id)
            .where(MetricCommentMetricType.metric_type_key.in_(metric_type))
            .scalar_subquery()
        )
        stmt = stmt.where(MetricComment.id.in_(annotated))

    if start is not None or end is not None:
        anchored = []
        if end is not None:
            anchored.append(MetricComment.anchor_start <= end)
        if start is not None:
            anchored.append(
                func.coalesce(
                    MetricComment.anchor_end, MetricComment.anchor_start
                )
                >= start
            )
        stmt = stmt.where(
            or_(MetricComment.anchor_start.is_(None), and_(*anchored))
        )

    result = await session.execute(stmt.order_by(MetricComment.created_at))
    return result.scalars().all()


@router.get("/{signal_id}/events")
async def list_signal_events(
    signal_id: str,
    start: datetime,
    end: datetime,
    event_codes: Optional[str] = Query(
        None,
        description=(
            "Comma-separated NTCIP/IHR event codes to filter on "
            "(e.g. ``1,82,9``).  Omit to return every code in the window."
        ),
    ),
    event_param: Optional[int] = Query(
        None,
        description=(
            "Optional exact-match filter on event_param "
            "(phase, detector channel, etc., depending on the event code)."
        ),
    ),
    after: Optional[str] = Query(
        None,
        description=(
            "Opaque cursor from a prior response's ``next_cursor``. "
            "Pass it back unchanged to fetch the next page; omit on the "
            "first request."
        ),
    ),
    limit: int = Query(
        _RAW_EVENTS_DEFAULT_LIMIT,
        ge=1,
        le=_RAW_EVENTS_MAX_LIMIT,
        description=(
            f"Max rows to return.  Default {_RAW_EVENTS_DEFAULT_LIMIT}, "
            f"hard ceiling {_RAW_EVENTS_MAX_LIMIT}.  The "
            "``api.max_page_size`` registry key is the final ceiling and "
            "clamps this value regardless of what the client requests."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """
    Raw IHR event log read for a single signal — tier-aware + paginated.

    Mirrors the GraphQL ``events`` resolver — same filters, same default
    limit, same ordering.  Use this for ad-hoc reporting, third-party
    tool integration, or any consumer that doesn't want to learn
    GraphQL.  ATSPM 4.x exposed ``/api/data/controllerEventLogs*`` for
    the same purpose; ATSPM 5.x removed raw event access entirely.

    Reads route through the tier-aware Report SDK
    (``tsigma.reports.sdk.queries.fetch_events``), which transparently
    unions hot and cold partitions when the requested window spans the
    ``cold_tier.threshold_days`` boundary.  The response is a paginated
    envelope ``{items, next_cursor}`` with rows ordered by ``event_time``
    ascending; pass ``next_cursor`` back as ``after`` to fetch the next
    page.

    Args:
        signal_id: Signal identifier.
        start: Inclusive lower bound (ISO-8601 datetime).
        end: Inclusive upper bound (ISO-8601 datetime).
        event_codes: Optional CSV of event codes to filter on.
        event_param: Optional exact-match filter on event_param.
        after: Opaque cursor from a prior response's ``next_cursor``.
        limit: Max rows per page (clamped to ``api.max_page_size``).

    Returns:
        ``{"items": [{signal_id, event_time, event_code, event_param}, ...],
        "next_cursor": <str|null>}``.  ``next_cursor`` is ``null`` when the
        page exhausts the remaining rows.

    Raises:
        HTTPException: 400 if ``end`` is before ``start``, the
            ``event_codes`` CSV is malformed, the ``after`` cursor is
            invalid, ``start`` predates ``api.max_lookback_days``, or the
            window exceeds ``api.max_aggregation_days``; 404 if the signal
            does not exist.
    """
    if end < start:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="end must be greater than or equal to start",
        )

    await require_max_lookback(start, session=session)
    await require_max_aggregation_days(start, end, session=session)

    signal_result = await session.execute(
        select(Signal.signal_id).where(Signal.signal_id == signal_id)
    )
    if signal_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Signal {signal_id} not found",
        )

    parsed_codes: Optional[list[int]] = None
    if event_codes:
        try:
            parsed_codes = [
                int(token.strip())
                for token in event_codes.split(",")
                if token.strip()
            ]
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"event_codes must be a comma-separated list of integers: {exc}",
            ) from exc

    df = await fetch_events(
        signal_id=signal_id,
        start=start,
        end=end,
        event_codes=parsed_codes if parsed_codes else None,
        event_param_in=[event_param] if event_param is not None else None,
    )

    # The SDK omits signal_id from its single-signal result; re-attach it
    # so the cursor encoder (which keys on event_time, signal_id,
    # event_code, event_param) has a value, and so the response rows
    # carry the field clients expect. Works on both empty and non-empty
    # DataFrames.
    if df.empty:
        df = df.assign(signal_id=pd.Series([], dtype=str))
    else:
        df = df.assign(signal_id=signal_id)

    items, next_cursor = await paginated_event_list(
        df, after=after, limit=limit, session=session
    )

    # Stringify event_time on each item — the response shape mirrors the
    # pre-B12.2 endpoint, which returned ISO-8601 strings. Use isinstance
    # so type-checkers can narrow (hasattr-based narrowing is unreliable
    # across checkers); runtime semantics are identical because pandas
    # Timestamp is a datetime subclass.
    for item in items:
        et = item.get("event_time")
        if isinstance(et, datetime):
            item["event_time"] = et.isoformat()

    return {"items": items, "next_cursor": next_cursor}
