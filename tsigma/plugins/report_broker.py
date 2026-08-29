"""Host-served report broker services - the consume-side of the report contract.

A report plugin calls BACK into these for its data. It never receives database
credentials or schema; the host answers from its own session under the request's
tenant context.

The security boundary is `compile_predicate`: raw SQL NEVER crosses to a plugin
(contract ruling 3a). Conditional aggregates arrive as a structured
{column, comparator, values} and are compiled to SQL here, against an allowlist,
from integers only. The in-process `where_sql_fragment` / raw `count_if` filter
modes are deliberately not exposed - they are SDK-internal and would hand a
vendor binary arbitrary SQL against the host session.
"""

import logging
from datetime import timezone

import pandas as pd

from ..reports.sdk import aggregates as sdk_aggregates
from ..reports.sdk import config as sdk_config
from ..reports.sdk import cycles as sdk_cycles
from ..reports.sdk import plans as sdk_plans
from ..reports.sdk import queries as sdk_queries
from .broker import scoped_session_for_plugin
from .remote_report import dataframe_to_arrow_batch

logger = logging.getLogger(__name__)

# Columns a plugin may predicate on. Anything else is refused - this is the
# allowlist that makes a compiled predicate safe to interpolate.
PREDICATE_COLUMNS = frozenset({"event_code", "event_param"})

COMPARATORS = {
    "eq": "=",
    "gt": ">",
    "lt": "<",
    "ge": ">=",
    "le": "<=",
}

# Rows per Arrow batch on the wire. Bounded so a large result is streamed as a
# sequence of batches and never approaches the gRPC message cap.
BATCH_ROWS = 10_000


class PredicateError(ValueError):
    """A structured predicate was outside the allowlist."""


def compile_predicate(predicate) -> str:
    """Compile an AggPredicate into SQL. The only path from plugin input to SQL.

    Every element is validated: the column against an allowlist, the comparator
    against a fixed map, and the operands are protobuf int32s, so no string from
    a plugin ever reaches the generated SQL.
    """
    column = predicate.column
    if column not in PREDICATE_COLUMNS:
        raise PredicateError(
            f"column {column!r} is not predicable; allowed: {sorted(PREDICATE_COLUMNS)}"
        )

    comparator = predicate.comparator
    values = list(predicate.values)
    if not values:
        raise PredicateError(f"predicate on {column!r} has no values")

    # Ints by construction (proto int32), asserted so a future proto change
    # cannot quietly widen this to strings.
    if not all(isinstance(v, int) for v in values):
        raise PredicateError(f"predicate on {column!r} has non-integer values")

    if comparator == "in":
        joined = ", ".join(str(v) for v in values)
        return f"{column} IN ({joined})"

    if comparator not in COMPARATORS:
        raise PredicateError(
            f"comparator {comparator!r} is not supported; allowed: "
            f"{sorted([*COMPARATORS, 'in'])}"
        )
    if len(values) != 1:
        raise PredicateError(f"comparator {comparator!r} takes exactly one value")
    return f"{column} {COMPARATORS[comparator]} {values[0]}"


def compile_agg_spec(spec) -> tuple:
    """Turn one AggSpec into the tuple form aggregate_events expects."""
    op = spec.op
    if op in ("count_if", "max_if"):
        if not spec.HasField("filter"):
            raise PredicateError(f"{op} requires a filter predicate")
        condition = compile_predicate(spec.filter)
        return (op, condition) if op == "count_if" else (op, spec.field, condition)
    if op == "count":
        return ("count",)
    if op in ("sum", "max", "min"):
        if not spec.field:
            raise PredicateError(f"{op} requires a field")
        return (op, spec.field)
    raise PredicateError(f"unsupported aggregate op {op!r}")


def frame_to_batches(frame: pd.DataFrame, batch_rows: int = BATCH_ROWS):
    """Split a DataFrame into bounded Arrow IPC batches for streaming."""
    if frame.empty:
        yield dataframe_to_arrow_batch(frame)
        return
    for start in range(0, len(frame), batch_rows):
        yield dataframe_to_arrow_batch(frame.iloc[start : start + batch_rows])


def _dt(timestamp):
    """protobuf Timestamp -> aware UTC datetime."""
    return timestamp.ToDatetime().replace(tzinfo=timezone.utc)


class EventQueryService:
    """Mirrors tsigma/reports/sdk/queries.py - tier-aware event access."""

    async def fetch_events(self, request) -> pd.DataFrame:
        return await sdk_queries.fetch_events(
            request.signal_id,
            _dt(request.start),
            _dt(request.end),
            list(request.event_codes) or None,
            event_param_in=list(request.event_param_in) or None,
        )

    async def fetch_events_split(self, request) -> pd.DataFrame:
        kwargs = {
            "phase_codes": list(request.phase_codes),
            "det_channels": list(request.det_channels),
        }
        if request.det_codes:
            kwargs["det_codes"] = list(request.det_codes)
        return await sdk_queries.fetch_events_split(
            request.signal_id, _dt(request.start), _dt(request.end), **kwargs
        )


class AggregateQueryService:
    """Mirrors tsigma/reports/sdk/aggregates.py - tier-aware aggregation."""

    @staticmethod
    def _signals(selector):
        if selector.all_signals:
            return "All"
        ids = list(selector.signal_ids)
        return ids or None

    async def aggregate_events(self, request) -> pd.DataFrame:
        agg = [compile_agg_spec(spec) for spec in request.agg]
        filters = {k: list(v.values) for k, v in request.filters.items()} or None
        return await sdk_aggregates.aggregate_events(
            self._signals(request.signals),
            _dt(request.start),
            _dt(request.end),
            agg=agg,
            group_by=list(request.group_by) or None,
            filters=filters,
        )


class CycleAggregateService:
    """Mirrors tsigma/reports/sdk/cycles.py - pre-computed cycle aggregates.

    These helpers open their own sessions, like queries.py, so no scoped session
    is threaded through here.
    """

    async def fetch_cycle_boundaries(self, request) -> pd.DataFrame:
        return await sdk_cycles.fetch_cycle_boundaries(
            request.signal_id, request.phase, _dt(request.start), _dt(request.end)
        )

    async def fetch_cycle_arrivals(self, request) -> pd.DataFrame:
        return await sdk_cycles.fetch_cycle_arrivals(
            request.signal_id,
            request.phase,
            _dt(request.start),
            _dt(request.end),
            list(request.detector_channels) or None,
        )

    async def fetch_cycle_summary(self, request) -> pd.DataFrame:
        return await sdk_cycles.fetch_cycle_summary(
            request.signal_id, request.phase, _dt(request.start), _dt(request.end)
        )


class ConfigService:
    """Mirrors tsigma/reports/sdk/config.py and plans.py.

    Unlike the query helpers these take a session, so every call runs inside a
    fresh per-invocation scoped session (P4) rather than sharing one.
    """

    def __init__(self, session_factory, username: str | None = None):
        self._session_factory = session_factory
        self._username = username

    def _scope(self):
        return scoped_session_for_plugin(self._session_factory, self._username)

    async def load_channel_to_phase(self, request) -> dict[int, int]:
        async with self._scope() as session:
            return await sdk_config.load_channel_to_phase(
                session, request.signal_id, _dt(request.as_of)
            )

    async def load_channels_for_phase(self, request) -> set[int]:
        async with self._scope() as session:
            return await sdk_config.load_channels_for_phase(
                session, request.signal_id, request.phase, _dt(request.as_of)
            )

    async def load_channel_to_ped_phase(self, request) -> dict[int, int]:
        async with self._scope() as session:
            return await sdk_config.load_channel_to_ped_phase(
                session, request.signal_id, _dt(request.as_of)
            )

    async def load_channel_to_approach(self, request) -> dict[int, dict]:
        async with self._scope() as session:
            return await sdk_config.load_channel_to_approach(
                session, request.signal_id, _dt(request.as_of)
            )

    async def fetch_plans(self, request) -> list:
        async with self._scope() as session:
            return await sdk_plans.fetch_plans(
                session, request.signal_id, _dt(request.start), _dt(request.end)
            )


def to_int_int_map(mapping: dict[int, int]):
    """dict[int,int] -> IntIntMap."""
    from tsigma.report.v1 import report_pb2

    return report_pb2.IntIntMap(mapping=dict(mapping))


def to_int_set(values) -> "object":
    """set[int] -> IntSet, ordered for a deterministic wire form."""
    from tsigma.report.v1 import report_pb2

    return report_pb2.IntSet(values=sorted(values))


def to_channel_approach_map(mapping: dict[int, dict]):
    """dict[int, {approach_id, direction_type_id, distance_from_stop_bar}] -> proto."""
    from tsigma.report.v1 import report_pb2

    out = {}
    for channel, info in mapping.items():
        approach = report_pb2.ApproachInfo(
            approach_id=info["approach_id"],
            direction_type_id=info["direction_type_id"],
        )
        distance = info.get("distance_from_stop_bar")
        if distance is not None:
            approach.distance_from_stop_bar = int(distance)
        out[channel] = approach
    return report_pb2.ChannelApproachMap(mapping=out)


def to_signal_plan_list(plans: list):
    """SignalPlan rows -> SignalPlanList.

    cycle_length and offset are nullable in the store, so they are left UNSET
    rather than zeroed - a plugin must be able to tell "unknown" from "zero".
    """
    from tsigma.report.v1 import report_pb2

    out = []
    for plan in plans:
        msg = report_pb2.SignalPlan(plan_number=plan.plan_number)
        msg.effective_from.FromDatetime(plan.effective_from)
        if plan.effective_to is not None:
            msg.effective_to.FromDatetime(plan.effective_to)
        if plan.cycle_length is not None:
            msg.cycle_length = plan.cycle_length
        if plan.offset is not None:
            msg.offset = plan.offset
        for phase, seconds in (plan.splits or {}).items():
            msg.splits[str(phase)] = float(seconds)
        out.append(msg)
    return report_pb2.SignalPlanList(plans=out)
