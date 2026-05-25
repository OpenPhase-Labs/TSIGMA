"""
Standard ControllerEventLog query helpers.

Every report needs events in a time window filtered by event code
(and sometimes by channel).  Centralising the query here means report
authors never hand-roll a SQL filter again.

All helpers return pandas DataFrames via db_facade.get_dataframe().

Tier routing
------------

``fetch_events`` is tier-aware: it consults the cold-tier query layer when
the requested window predates ``cold_tier.threshold_days``.  Callers see a
single DataFrame with the hot and cold rows union-ed and sorted by
``event_time``.  See ``docs/developers/ARCHITECTURE.md#tier-aware-query-routing``
for the decision tree.
"""

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import and_, or_, select, text

from tsigma import settings_service

from ...database.cold_tier import ColdTierQuery
from ...database.db import db_facade
from ...models.event import ControllerEventLog as CEL
from ...storage.factory import get_cold_storage_backend
from .events import EVENT_DETECTOR_ON


async def fetch_events(
    signal_id: str,
    start: datetime,
    end: datetime,
    event_codes: Iterable[int] | None = None,
    *,
    event_param_in: Iterable[int] | None = None,
    where_sql_fragment: str | None = None,
) -> pd.DataFrame:
    """
    Fetch ControllerEventLog rows for a signal in a time window, routing
    between hot and cold tiers based on runtime settings.

    Three predicate modes — at most one of ``event_codes`` and
    ``where_sql_fragment`` may be supplied:

    * **IN-list mode** (default): pass ``event_codes`` (and optionally
      ``event_param_in``). The WHERE clause is built as
      ``event_code IN (...) [AND event_param IN (...)]``.
    * **Fragment mode**: pass ``where_sql_fragment`` — a SQL expression
      that replaces the IN-list predicate entirely. Used by Stage 2a's
      ``preemption_recovery`` handler (and similar SDK callers) to
      express predicates that the ``(event_codes, event_param_in)`` pair
      cannot, e.g. ``event_code = 104 OR (event_code = 1 AND
      event_param = 2)``. The fragment is forwarded to the cold path via
      ``ColdTierQuery.fetch_events``'s ``where_sql_fragment`` kwarg, and
      injected into the hot path's SQLAlchemy ``WHERE`` clause via
      ``sqlalchemy.text(...)``.

      **Trust contract:** ``where_sql_fragment`` is SDK-internal code,
      NOT validated against an allowlist — same boundary as
      ``ColdTierQuery.fetch_events``'s fragment mode and
      ``aggregate_events``'s ``count_if`` / ``max_if`` expressions.
      Callers must build the fragment in Python from controlled
      constants; never interpolate API-surface input.
    * **No-filter mode**: pass neither. Returns every row in the
      ``[start, end]`` window for the signal, regardless of event code.

    Passing BOTH ``event_codes`` and ``where_sql_fragment`` raises
    ``ValueError`` (ambiguous).

    Args:
        signal_id: Signal ID to query.
        start: Window start (inclusive).
        end: Window end (inclusive).
        event_codes: Event codes to include. ``None`` means no event-code
            filter — every row in the window is returned.
        event_param_in: Optional additional filter on ``event_param``
            (e.g. restrict to a set of detector channels).
        where_sql_fragment: Optional SDK-internal SQL fragment that
            replaces the IN-list predicate; see "Fragment mode" above.

    Returns:
        DataFrame with columns: event_code, event_param, event_time.
        Ordered by event_time.

    Routing:
        - ``cold_tier.query_enabled = false`` → hot-only.
        - ``end < now - cold_tier.threshold_days`` → cold-only.
        - ``start >= now - cold_tier.threshold_days`` → hot-only.
        - Otherwise (spanning) → cold for ``[start, threshold)``, hot for
          ``[threshold, end]``, concatenated and sorted by ``event_time``.
    """
    if event_codes is not None and where_sql_fragment is not None:
        raise ValueError(
            "fetch_events: pass at most one of event_codes or "
            "where_sql_fragment, not both"
        )

    codes_list = list(event_codes) if event_codes is not None else None
    param_in_list = list(event_param_in) if event_param_in is not None else None

    async with db_facade.session() as session:
        cold_enabled = await settings_service.get_bool(
            "cold_tier.query_enabled", session
        )
        threshold_days = await settings_service.get_int(
            "cold_tier.threshold_days", session
        )

    if not cold_enabled:
        return await _fetch_events_hot(
            signal_id,
            start,
            end,
            codes_list,
            param_in_list,
            where_sql_fragment=where_sql_fragment,
        )

    # Build threshold matching the input tz-awareness so the comparison below
    # doesn't raise on mixed naive/aware operands. Callers in the codebase use
    # both conventions (cold-tier fixtures are naive UTC; REST endpoints typically
    # pass tz-aware UTC).
    now = datetime.now(timezone.utc)
    if start.tzinfo is None and end.tzinfo is None:
        now = now.replace(tzinfo=None)
    threshold = now - timedelta(days=int(threshold_days))

    if end < threshold:
        return await _fetch_events_cold(
            signal_id,
            start,
            end,
            codes_list,
            param_in_list,
            where_sql_fragment=where_sql_fragment,
        )
    if start >= threshold:
        return await _fetch_events_hot(
            signal_id,
            start,
            end,
            codes_list,
            param_in_list,
            where_sql_fragment=where_sql_fragment,
        )

    # Spanning — split at the threshold.
    cold_df = await _fetch_events_cold(
        signal_id,
        start,
        threshold,
        codes_list,
        param_in_list,
        where_sql_fragment=where_sql_fragment,
    )
    hot_df = await _fetch_events_hot(
        signal_id,
        threshold,
        end,
        codes_list,
        param_in_list,
        where_sql_fragment=where_sql_fragment,
    )
    combined = pd.concat([cold_df, hot_df], ignore_index=True)
    return combined.sort_values("event_time").reset_index(drop=True)


async def _fetch_events_hot(
    signal_id: str,
    start: datetime,
    end: datetime,
    event_codes: list[int] | None,
    event_param_in: list[int] | None,
    *,
    where_sql_fragment: str | None = None,
) -> pd.DataFrame:
    """Hot-tier path — original behaviour against ControllerEventLog.

    When ``where_sql_fragment`` is supplied it is injected into the WHERE
    clause via ``sqlalchemy.text(...)`` and renders inline in the
    compiled SQL. SDK-internal trust contract — see ``fetch_events``.
    """
    conditions = [
        CEL.signal_id == signal_id,
        CEL.event_time >= start,
        CEL.event_time <= end,
    ]
    if event_codes is not None:
        conditions.append(CEL.event_code.in_(event_codes))
    if event_param_in is not None:
        conditions.append(CEL.event_param.in_(event_param_in))
    if where_sql_fragment is not None:
        conditions.append(text(where_sql_fragment))

    stmt = (
        select(CEL.event_code, CEL.event_param, CEL.event_time)
        .where(and_(*conditions))
        .order_by(CEL.event_time)
    )
    return await db_facade.get_dataframe(stmt)


async def _fetch_events_cold(
    signal_id: str,
    start: datetime,
    end: datetime,
    event_codes: list[int] | None,
    event_param_in: list[int] | None,
    *,
    where_sql_fragment: str | None = None,
) -> pd.DataFrame:
    """Cold-tier path — DuckDB read via ColdTierQuery + storage factory.

    ``where_sql_fragment`` is forwarded to ``ColdTierQuery.fetch_events``;
    its both-set validation against ``event_codes`` is the cold-tier
    layer's responsibility (SDK ``fetch_events`` also validates earlier).
    """
    backend = get_cold_storage_backend()
    cold = ColdTierQuery(backend)
    return await cold.fetch_events(
        signal_id,
        start,
        end,
        event_codes,
        event_param_in=event_param_in,
        where_sql_fragment=where_sql_fragment,
    )


async def fetch_events_split(
    signal_id: str,
    start: datetime,
    end: datetime,
    *,
    phase_codes: Iterable[int],
    det_channels: Iterable[int],
    det_codes: Iterable[int] = (EVENT_DETECTOR_ON,),
) -> pd.DataFrame:
    """
    Fetch phase events AND detector events for specific channels in one query.

    Equivalent to:
        phase_codes OR (det_codes AND event_param IN det_channels)

    Tier-aware (B8): same routing rule as ``fetch_events``. The cold path
    uses a single DuckDB scan via ``ColdTierQuery.fetch_events``'s
    ``where_sql_fragment`` mode — the OR predicate is composed in Python
    and pushed into one query, not two.

    Args:
        signal_id: Signal ID to query.
        start: Window start (inclusive).
        end: Window end (inclusive).
        phase_codes: Phase event codes to include unconditionally.
        det_channels: Detector channels to include.
        det_codes: Detector event codes to include (default: DETECTOR_ON).

    Returns:
        DataFrame with columns: event_code, event_param, event_time.
        Ordered by event_time.
    """
    phase_codes_list = list(phase_codes)
    det_channels_list = list(det_channels)
    det_codes_list = list(det_codes)

    async with db_facade.session() as session:
        cold_enabled = await settings_service.get_bool(
            "cold_tier.query_enabled", session
        )
        threshold_days = await settings_service.get_int(
            "cold_tier.threshold_days", session
        )

    if not cold_enabled:
        return await _fetch_events_split_hot(
            signal_id, start, end, phase_codes_list, det_codes_list, det_channels_list
        )

    now = datetime.now(timezone.utc)
    if start.tzinfo is None and end.tzinfo is None:
        now = now.replace(tzinfo=None)
    threshold = now - timedelta(days=int(threshold_days))

    if end < threshold:
        return await _fetch_events_split_cold(
            signal_id, start, end, phase_codes_list, det_codes_list, det_channels_list
        )
    if start >= threshold:
        return await _fetch_events_split_hot(
            signal_id, start, end, phase_codes_list, det_codes_list, det_channels_list
        )

    cold_df = await _fetch_events_split_cold(
        signal_id, start, threshold, phase_codes_list, det_codes_list, det_channels_list
    )
    hot_df = await _fetch_events_split_hot(
        signal_id, threshold, end, phase_codes_list, det_codes_list, det_channels_list
    )
    combined = pd.concat([cold_df, hot_df], ignore_index=True)
    return combined.sort_values("event_time").reset_index(drop=True)


async def _fetch_events_split_hot(
    signal_id: str,
    start: datetime,
    end: datetime,
    phase_codes: list[int],
    det_codes: list[int],
    det_channels: list[int],
) -> pd.DataFrame:
    """Hot-tier split path — the original OR-of-IN-lists SQL."""
    stmt = (
        select(CEL.event_code, CEL.event_param, CEL.event_time)
        .where(
            and_(
                CEL.signal_id == signal_id,
                CEL.event_time >= start,
                CEL.event_time <= end,
                or_(
                    CEL.event_code.in_(phase_codes),
                    and_(
                        CEL.event_code.in_(det_codes),
                        CEL.event_param.in_(det_channels),
                    ),
                ),
            )
        )
        .order_by(CEL.event_time)
    )
    return await db_facade.get_dataframe(stmt)


async def _fetch_events_split_cold(
    signal_id: str,
    start: datetime,
    end: datetime,
    phase_codes: list[int],
    det_codes: list[int],
    det_channels: list[int],
) -> pd.DataFrame:
    """Cold-tier split path — single DuckDB scan via where_sql_fragment.

    Fragment is composed entirely in Python here (SDK-internal trust); the
    integer lists are int-cast and inlined, matching the safety pattern
    used throughout ColdTierQuery.
    """
    phases_sql = ",".join(str(int(c)) for c in phase_codes)
    det_codes_sql = ",".join(str(int(c)) for c in det_codes)
    channels_sql = ",".join(str(int(c)) for c in det_channels)
    fragment = (
        f"event_code IN ({phases_sql}) "
        f"OR (event_code IN ({det_codes_sql}) AND event_param IN ({channels_sql}))"
    )
    backend = get_cold_storage_backend()
    cold = ColdTierQuery(backend)
    return await cold.fetch_events(
        signal_id, start, end, where_sql_fragment=fragment
    )
