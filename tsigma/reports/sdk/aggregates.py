"""
Shared aggregate / statistical helpers for report plugins.

Two flavours:

* **Per-row stats** (``safe_avg``, ``safe_min``, ``safe_max``, ``pct``,
  ``percentile_from_sorted``) — pure functions over numeric lists.
* **Tier-aware query** (``aggregate_events``) — single-call aggregation
  over ControllerEventLog that transparently spans the hot/cold tier
  boundary. The spanning path computes both halves separately and
  re-aggregates with the appropriate combinator per agg-spec.

See ``docs/developers/STORAGE.md#cold-tier-query`` for the agg-spec table
and ``docs/developers/ARCHITECTURE.md#tier-aware-query-routing`` for the
routing rule.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from sqlalchemy import and_, func, select, text

from tsigma import settings_service
from tsigma.database.cold_tier import (
    ColdTierQuery,
    _empty_aggregate_df,
    _validate_column,
)
from tsigma.database.db import db_facade
from tsigma.models.event import ControllerEventLog as CEL
from tsigma.storage.factory import get_cold_storage_backend


# Cross-tier combinator per agg-spec kind. Each per-tier op is associative
# & commutative, so the combinator is the same op (sum-of-sums for counts,
# max-of-maxes, min-of-mins).
_COMBINATOR: dict[str, str] = {
    "count": "sum",
    "count_if": "sum",
    "sum": "sum",
    "max": "max",
    "max_if": "max",
    "min": "min",
}


# ---------------------------------------------------------------------------
# Per-row stats helpers
# ---------------------------------------------------------------------------


def safe_avg(values: list[float], decimals: int = 2) -> float:
    """Return rounded average or 0.0 if *values* is empty."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), decimals)


def safe_min(values: list[float], decimals: int = 1) -> float:
    """Return rounded minimum or 0.0 if *values* is empty."""
    if not values:
        return 0.0
    return round(min(values), decimals)


def safe_max(values: list[float], decimals: int = 1) -> float:
    """Return rounded maximum or 0.0 if *values* is empty."""
    if not values:
        return 0.0
    return round(max(values), decimals)


def pct(count: int | float, total: int | float, decimals: int = 1) -> float:
    """Return percentage or 0.0 if *total* is zero."""
    if not total:
        return 0.0
    return round(count / total * 100, decimals)


def percentile_from_sorted(values: list[float], p: float) -> float:
    """
    Return the *p*-th percentile from an already-sorted list.

    Uses nearest-rank method. *p* should be in [0, 100].
    Returns 0.0 if *values* is empty.
    """
    if not values:
        return 0.0
    count = len(values)
    idx = int(count * p / 100)
    return values[min(idx, count - 1)]


# ---------------------------------------------------------------------------
# Tier-aware aggregate_events (B10)
# ---------------------------------------------------------------------------


async def aggregate_events(
    signal_id_or_ids: str | list[str] | None,
    start: datetime,
    end: datetime,
    *,
    agg: list[tuple],
    group_by: list[str] | None = None,
    filters: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Tier-aware aggregation over ``ControllerEventLog``.

    Routes per the standard B7 tier-routing rule (see
    ``cold_tier.query_enabled`` / ``cold_tier.threshold_days``). Spanning
    windows compute both halves separately then re-aggregate via pandas
    ``groupby(group_by).agg({col: combinator})`` with:

    * ``count`` / ``count_if`` / ``sum`` → ``sum``
    * ``max`` / ``max_if`` → ``max``
    * ``min`` → ``min``

    ``signal_id_or_ids`` supports four modes:

    * ``None`` or ``[]`` → no signals requested; returns an empty
      DataFrame immediately with the canonical column shape. No
      settings round-trip, no hot/cold dispatch, no I/O.
    * ``"All"`` → aggregates across every signal present in storage
      (fleet-wide mode). The hot path omits the ``signal_id IN (...)``
      WHERE clause entirely; the cold path enumerates all signal
      directories/prefixes before scanning. The contract is consistent
      across the SDK and ``ColdTierQuery`` levels.
    * ``str`` → single specific signal.
    * ``list[str]`` → specific signal subset.

    Returns a DataFrame with the same shape as
    ``ColdTierQuery.aggregate_events`` — ``group_by`` columns first
    (preserved order) then one ``agg_<i>`` column per spec.
    """
    group_by_list = list(group_by) if group_by else []

    # Empty-result short-circuit: None or [] means "no signals requested",
    # so return an empty-but-shaped DataFrame without any I/O or settings
    # round-trip. Stays in lock-step with ColdTierQuery.aggregate_events.
    if signal_id_or_ids is None or (
        isinstance(signal_id_or_ids, list) and len(signal_id_or_ids) == 0
    ):
        return _empty_aggregate_df(agg, group_by_list)

    async with db_facade.session() as session:
        cold_enabled = await settings_service.get_bool(
            "cold_tier.query_enabled", session
        )
        threshold_days = await settings_service.get_int(
            "cold_tier.threshold_days", session
        )

    if not cold_enabled:
        return await _aggregate_events_hot(
            signal_id_or_ids, start, end, agg, group_by_list, filters
        )

    now = datetime.now(timezone.utc)
    if start.tzinfo is None and end.tzinfo is None:
        now = now.replace(tzinfo=None)
    threshold = now - timedelta(days=int(threshold_days))

    if end < threshold:
        return await _aggregate_events_cold(
            signal_id_or_ids, start, end, agg, group_by_list, filters
        )
    if start >= threshold:
        return await _aggregate_events_hot(
            signal_id_or_ids, start, end, agg, group_by_list, filters
        )

    # Spanning — compute both halves, then cross-tier re-aggregate.
    cold_df = await _aggregate_events_cold(
        signal_id_or_ids, start, threshold, agg, group_by_list, filters
    )
    hot_df = await _aggregate_events_hot(
        signal_id_or_ids, threshold, end, agg, group_by_list, filters
    )
    return _combine_aggregations(cold_df, hot_df, agg, group_by_list)


async def _aggregate_events_hot(
    signal_id_or_ids: str | list[str] | None,
    start: datetime,
    end: datetime,
    agg: list[tuple],
    group_by: list[str],
    filters: dict[str, Any] | None,
) -> pd.DataFrame:
    """Hot-tier path — build one SELECT with all projections via SQLAlchemy."""
    if signal_id_or_ids == "All":
        signal_ids = None  # all signals — omit the signal_id IN filter
    elif isinstance(signal_id_or_ids, str):
        signal_ids = [signal_id_or_ids]
    else:
        signal_ids = list(signal_id_or_ids)

    projections: list = []
    group_cols: list = []
    for col_name in group_by:
        _validate_column(col_name)
        col_attr = getattr(CEL, col_name)
        projections.append(col_attr.label(col_name))
        group_cols.append(col_attr)

    for i, spec in enumerate(agg):
        projections.append(_hot_agg_expr(spec, i))

    conditions = [
        CEL.event_time >= start,
        CEL.event_time <= end,
    ]
    if signal_ids is not None:
        conditions.append(CEL.signal_id.in_(signal_ids))
    if filters:
        for col_name, val in filters.items():
            _validate_column(col_name)
            col_attr = getattr(CEL, col_name)
            if isinstance(val, list):
                conditions.append(col_attr.in_(val))
            else:
                conditions.append(col_attr == val)

    stmt = select(*projections).where(and_(*conditions))
    if group_cols:
        stmt = stmt.group_by(*group_cols)

    return await db_facade.get_dataframe(stmt)


def _hot_agg_expr(spec: tuple, i: int):
    """Translate an agg spec tuple into a labelled SQLAlchemy expression."""
    op = spec[0]
    label = f"agg_{i}"

    if op == "count" and len(spec) == 2 and spec[1] == "*":
        return func.count().label(label)
    if op == "count_if" and len(spec) == 2:
        return func.count().filter(text(spec[1])).label(label)
    if op == "max" and len(spec) == 2:
        _validate_column(spec[1])
        return func.max(getattr(CEL, spec[1])).label(label)
    if op == "max_if" and len(spec) == 3:
        _validate_column(spec[1])
        return func.max(getattr(CEL, spec[1])).filter(text(spec[2])).label(label)
    if op == "min" and len(spec) == 2:
        _validate_column(spec[1])
        return func.min(getattr(CEL, spec[1])).label(label)
    if op == "sum" and len(spec) == 2:
        _validate_column(spec[1])
        return func.sum(getattr(CEL, spec[1])).label(label)
    raise ValueError(f"unsupported aggregation spec: {spec!r}")


async def _aggregate_events_cold(
    signal_id_or_ids: str | list[str] | None,
    start: datetime,
    end: datetime,
    agg: list[tuple],
    group_by: list[str],
    filters: dict[str, Any] | None,
) -> pd.DataFrame:
    """Cold-tier path — delegate to ColdTierQuery (B5)."""
    backend = get_cold_storage_backend()
    cold = ColdTierQuery(backend)
    return await cold.aggregate_events(
        signal_id_or_ids,
        start,
        end,
        agg=agg,
        group_by=group_by,
        filters=filters,
    )


def _combine_aggregations(
    cold_df: pd.DataFrame,
    hot_df: pd.DataFrame,
    agg: list[tuple],
    group_by: list[str],
) -> pd.DataFrame:
    """Cross-tier re-aggregation for spanning windows.

    ``cold_df`` and ``hot_df`` each have the canonical
    ``[*group_by, agg_0, ..., agg_n]`` column shape from their respective
    backend. We concat them and apply per-spec combinators to collapse
    duplicate group keys.
    """
    combined = pd.concat([cold_df, hot_df], ignore_index=True)
    if combined.empty:
        return combined

    agg_funcs = {
        f"agg_{i}": _COMBINATOR[spec[0]]
        for i, spec in enumerate(agg)
        if spec[0] in _COMBINATOR
    }

    if not group_by:
        # Scalar: collapse to a single row.
        row = {col: combined[col].agg(fn) for col, fn in agg_funcs.items()}
        return pd.DataFrame([row])

    return combined.groupby(group_by, as_index=False).agg(agg_funcs)
