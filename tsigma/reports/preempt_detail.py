"""
Preempt Detail report plugin.

Rich lifecycle analysis of each preemption cycle using a 10-event state
machine driven by Indiana Hi-Res codes 102-111. Pairs each preemption
request (102 / 105) with its downstream service events and derives
delay, time-to-service, dwell, track-clear, and max-presence timings.

This report coexists with the lightweight ``preemption`` report. The
older report returns raw 102/104 pairs; this one produces one row per
complete preemption cycle.

Reference: UDOT ATSPM 5.x ``PreemptDetail`` controller + MOE.Common
``PreemptSignalChart``; Indiana Hi-Res enumerations (Sturdevant et al.,
INDOT/Purdue 2012, doi:10.4231/K4RN35SH).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_PREEMPTION_BEGIN_DWELL,
    EVENT_PREEMPTION_BEGIN_EXIT,
    EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE,
    EVENT_PREEMPTION_CALL_INPUT_OFF,
    EVENT_PREEMPTION_CALL_INPUT_ON,
    EVENT_PREEMPTION_ENTRY_STARTED,
    EVENT_PREEMPTION_GATE_DOWN,
    EVENT_PREEMPTION_LINK_ACTIVE_OFF,
    EVENT_PREEMPTION_LINK_ACTIVE_ON,
    EVENT_PREEMPTION_MAX_PRESENCE,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)


# Event codes handled by the state machine, in the order documented in
# the spec.
_PREEMPT_EVENT_CODES: tuple[int, ...] = (
    EVENT_PREEMPTION_CALL_INPUT_ON,          # 102
    EVENT_PREEMPTION_GATE_DOWN,              # 103
    EVENT_PREEMPTION_CALL_INPUT_OFF,         # 104
    EVENT_PREEMPTION_ENTRY_STARTED,          # 105
    EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE,  # 106
    EVENT_PREEMPTION_BEGIN_DWELL,            # 107
    EVENT_PREEMPTION_LINK_ACTIVE_ON,         # 108
    EVENT_PREEMPTION_LINK_ACTIVE_OFF,        # 109
    EVENT_PREEMPTION_MAX_PRESENCE,           # 110
    EVENT_PREEMPTION_BEGIN_EXIT,             # 111
)

# Inactivity gap after which an open cycle is force-ended. Matches the
# 20-minute watchdog used by ATSPM 5.x.
_CYCLE_TIMEOUT = timedelta(minutes=20)

_OUTPUT_COLUMNS: list[str] = [
    "preempt_number",
    "cycle_start",
    "cycle_end",
    "input_on",
    "input_off",
    "gate_down",
    "entry_started",
    "begin_track_clearance",
    "begin_dwell_service",
    "max_presence_exceeded",
    "has_delay",
    "delay_seconds",
    "time_to_service_seconds",
    "dwell_time_seconds",
    "track_clear_seconds",
    "call_max_out_seconds",
    "terminated_by_timeout",
]

# Columns that may hold ``None``. Kept as object dtype so callers can use
# ``value is None`` rather than ``math.isnan``.
_NULLABLE_COLUMNS: tuple[str, ...] = (
    "input_off",
    "gate_down",
    "entry_started",
    "begin_track_clearance",
    "begin_dwell_service",
    "max_presence_exceeded",
    "call_max_out_seconds",
)


class PreemptDetailParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    preempt_number: int | None = Field(
        default=None,
        description="Optional preempt channel filter (event_param)",
    )


@ReportRegistry.register("preempt-detail")
class PreemptDetailReport(Report[PreemptDetailParams]):
    """Full preemption-cycle lifecycle with delay / dwell / track-clear metrics."""

    metadata = ReportMetadata(
        name="preempt-detail",
        description=(
            "Per-cycle preemption analysis pairing 102 requests with 105 entries "
            "and deriving delay, time-to-service, dwell, track-clear, and "
            "max-presence timings."
        ),
        category="detailed",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self,
        params: PreemptDetailParams,
        session: AsyncSession,
    ) -> pd.DataFrame:
        """Execute preempt detail analysis. Returns one row per cycle."""
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)

        logger.info(
            "Running preempt-detail for %s from %s to %s (channel=%s)",
            signal_id, start, end, params.preempt_number,
        )

        df = await fetch_events(signal_id, start, end, _PREEMPT_EVENT_CODES)

        if df.empty:
            logger.info("preempt-detail complete: 0 cycles (no events)")
            return _empty_result()

        channel_events = _group_by_channel(df, params.preempt_number)
        rows: list[dict[str, Any]] = []
        for channel, events in channel_events.items():
            cycles = _build_cycles_for_channel(events)
            for cycle in cycles:
                rows.append(_cycle_to_row(channel, cycle))

        if not rows:
            logger.info("preempt-detail complete: 0 cycles")
            return _empty_result()

        result = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
        result = result.sort_values(
            by=["preempt_number", "cycle_start"], kind="stable",
        ).reset_index(drop=True)

        # Preserve explicit ``None`` semantics in optional columns — pandas
        # otherwise coerces missing values to NaN for numeric columns, which
        # breaks ``value is None`` checks in callers.
        for col in _NULLABLE_COLUMNS:
            result[col] = result[col].astype(object).where(result[col].notna(), None)

        logger.info("preempt-detail complete: %d cycles", len(result))
        return result


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


@dataclass
class _Cycle:
    """Accumulator for a single preemption cycle."""

    cycle_start: datetime
    has_delay: bool
    input_on: list[datetime] = field(default_factory=list)
    input_off: list[datetime] = field(default_factory=list)
    gate_down: datetime | None = None
    entry_started: datetime | None = None
    begin_track_clearance: datetime | None = None
    begin_dwell_service: datetime | None = None
    link_active_on: datetime | None = None
    link_active_off: datetime | None = None
    max_presence_exceeded: datetime | None = None
    cycle_end: datetime | None = None
    terminated_by_timeout: bool = False


def _empty_result() -> pd.DataFrame:
    """Return an empty DataFrame with the full output schema."""
    return pd.DataFrame(columns=_OUTPUT_COLUMNS)


def _group_by_channel(
    df: pd.DataFrame,
    preempt_number: int | None,
) -> dict[int, list[tuple[datetime, int]]]:
    """Group events by preempt channel (event_param), preserving time order."""
    grouped: dict[int, list[tuple[datetime, int]]] = {}
    for _, row in df.iterrows():
        channel = int(row["event_param"])
        if preempt_number is not None and channel != preempt_number:
            continue
        grouped.setdefault(channel, []).append(
            (row["event_time"], int(row["event_code"])),
        )
    # Events already come out of fetch_events sorted by event_time, so
    # each per-channel list remains sorted. Sort defensively anyway.
    for channel in grouped:
        grouped[channel].sort(key=lambda pair: pair[0])
    return grouped


def _finalize_cycle(
    cycle: _Cycle,
    end_time: datetime,
    *,
    timed_out: bool = False,
) -> _Cycle:
    """Stamp end_time / timeout flag on a cycle and return it."""
    cycle.cycle_end = end_time
    cycle.terminated_by_timeout = timed_out
    return cycle


def _build_cycles_for_channel(
    events: list[tuple[datetime, int]],
) -> list[_Cycle]:
    """Walk one channel's events and return the completed cycles."""
    cycles: list[_Cycle] = []
    current: _Cycle | None = None
    last_time: datetime | None = None

    for event_time, code in events:
        # Timeout check BEFORE processing the current event.
        if (
            current is not None
            and last_time is not None
            and event_time - last_time > _CYCLE_TIMEOUT
        ):
            cycles.append(_finalize_cycle(current, last_time, timed_out=True))
            current = None

        current = _dispatch(current, event_time, code, cycles)
        last_time = event_time

    if current is not None and last_time is not None:
        cycles.append(_finalize_cycle(current, last_time))

    return cycles


def _dispatch(
    current: _Cycle | None,
    event_time: datetime,
    code: int,
    cycles: list[_Cycle],
) -> _Cycle | None:
    """Handle a single event against the current cycle. Returns the new current."""
    if code == EVENT_PREEMPTION_CALL_INPUT_ON:
        return _handle_input_on(current, event_time)

    if code == EVENT_PREEMPTION_ENTRY_STARTED:
        return _handle_entry_started(current, event_time, cycles)

    if code == EVENT_PREEMPTION_BEGIN_EXIT:
        if current is not None:
            cycles.append(_finalize_cycle(current, event_time))
        return None

    if current is None:
        # Stray event (103/104/106–110) with no cycle — ignore.
        return None

    _apply_inner_event(current, code, event_time)
    return current


def _handle_input_on(
    current: _Cycle | None,
    event_time: datetime,
) -> _Cycle:
    """Handle a 102 PreemptCallInputOn event."""
    if current is None:
        # A 102 always opens a delayed-request cycle. The presence or
        # absence of a following 105 only affects whether entry_started
        # (and delay_seconds) ends up populated — not whether the cycle
        # is categorised as having a delay phase.
        return _Cycle(cycle_start=event_time, has_delay=True, input_on=[event_time])

    current.input_on.append(event_time)
    return current


def _handle_entry_started(
    current: _Cycle | None,
    event_time: datetime,
    cycles: list[_Cycle],
) -> _Cycle:
    """Handle a 105 PreemptEntryStarted event."""
    if current is None:
        # Service without a preceding request.
        return _Cycle(cycle_start=event_time, has_delay=False, entry_started=event_time)

    if current.has_delay and current.entry_started is None:
        current.entry_started = event_time
        return current

    # A new 105 arrived while a cycle is still open — close the old one and
    # start a fresh one with has_delay=False.
    cycles.append(_finalize_cycle(current, event_time))
    return _Cycle(cycle_start=event_time, has_delay=False, entry_started=event_time)


def _apply_inner_event(cycle: _Cycle, code: int, event_time: datetime) -> None:
    """Apply codes 103/104/106–110 to the open cycle."""
    if code == EVENT_PREEMPTION_GATE_DOWN:
        if cycle.gate_down is None:
            cycle.gate_down = event_time
        return
    if code == EVENT_PREEMPTION_CALL_INPUT_OFF:
        cycle.input_off.append(event_time)
        return
    if code == EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE:
        if cycle.begin_track_clearance is None:
            cycle.begin_track_clearance = event_time
        return
    if code == EVENT_PREEMPTION_BEGIN_DWELL:
        if cycle.begin_dwell_service is None:
            cycle.begin_dwell_service = event_time
        return
    if code == EVENT_PREEMPTION_LINK_ACTIVE_ON:
        if cycle.link_active_on is None:
            cycle.link_active_on = event_time
        return
    if code == EVENT_PREEMPTION_LINK_ACTIVE_OFF:
        if cycle.link_active_off is None:
            cycle.link_active_off = event_time
        return
    if code == EVENT_PREEMPTION_MAX_PRESENCE:
        if cycle.max_presence_exceeded is None:
            cycle.max_presence_exceeded = event_time


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------


def _service_anchor(cycle: _Cycle) -> datetime:
    """Timestamp from which time-to-service is measured."""
    if cycle.has_delay and cycle.entry_started is not None:
        return cycle.entry_started
    return cycle.cycle_start


def _input_on_display(cycle: _Cycle) -> datetime:
    """The 'input_on' shown in the output row.

    For a cycle with a 102 request, this is the first 102. For a cycle that
    skipped the request (no-delay path), it falls back to entry_started /
    cycle_start so the column is never null.
    """
    if cycle.input_on:
        return cycle.input_on[0]
    if cycle.entry_started is not None:
        return cycle.entry_started
    return cycle.cycle_start


def _delay_seconds(cycle: _Cycle) -> float | None:
    """
    Delay from cycle start to entry-started.

    Returns ``0.0`` for a no-delay cycle (a 105 opens the cycle directly — by
    definition no delay to measure). Returns ``None`` for a delayed cycle
    where service never arrived (the 102 was recorded but no 105 followed),
    because "zero delay" and "never serviced" are operationally distinct.
    """
    if not cycle.has_delay:
        return 0.0
    if cycle.entry_started is None:
        return None
    return (cycle.entry_started - cycle.cycle_start).total_seconds()


def _time_to_service_seconds(cycle: _Cycle) -> float | None:
    """Time from service anchor to the later of track-clear or dwell start.

    ``None`` when neither track-clear (106) nor dwell (107) was observed —
    i.e., the preempt cycle never reached a service stage.
    """
    candidates = [t for t in (cycle.begin_track_clearance, cycle.begin_dwell_service) if t is not None]
    if not candidates:
        return None
    return (max(candidates) - _service_anchor(cycle)).total_seconds()


def _dwell_seconds(cycle: _Cycle) -> float | None:
    """Time from dwell start to cycle end. ``None`` when dwell never started."""
    if cycle.begin_dwell_service is None or cycle.cycle_end is None:
        return None
    return (cycle.cycle_end - cycle.begin_dwell_service).total_seconds()


def _track_clear_seconds(cycle: _Cycle) -> float | None:
    """Time from track-clear start to dwell start. ``None`` when either stage was skipped."""
    if cycle.begin_track_clearance is None or cycle.begin_dwell_service is None:
        return None
    return (cycle.begin_dwell_service - cycle.begin_track_clearance).total_seconds()


def _call_max_out_seconds(cycle: _Cycle) -> float | None:
    """Time from cycle start to max-presence-exceeded, if observed."""
    if cycle.max_presence_exceeded is None:
        return None
    return (cycle.max_presence_exceeded - cycle.cycle_start).total_seconds()


def _iso(ts: datetime | None) -> str | None:
    return ts.isoformat() if ts is not None else None


def _round_or_none(value: float | None, ndigits: int = 2) -> float | None:
    """Round to ``ndigits`` places, or return None if value is None."""
    return None if value is None else round(value, ndigits)


def _cycle_to_row(channel: int, cycle: _Cycle) -> dict[str, Any]:
    """Serialize a cycle into the output row shape."""
    return {
        "preempt_number": channel,
        "cycle_start": _iso(cycle.cycle_start),
        "cycle_end": _iso(cycle.cycle_end),
        "input_on": _iso(_input_on_display(cycle)),
        "input_off": _iso(cycle.input_off[0]) if cycle.input_off else None,
        "gate_down": _iso(cycle.gate_down),
        "entry_started": _iso(cycle.entry_started),
        "begin_track_clearance": _iso(cycle.begin_track_clearance),
        "begin_dwell_service": _iso(cycle.begin_dwell_service),
        "max_presence_exceeded": _iso(cycle.max_presence_exceeded),
        "has_delay": bool(cycle.has_delay),
        "delay_seconds": _round_or_none(_delay_seconds(cycle)),
        "time_to_service_seconds": _round_or_none(_time_to_service_seconds(cycle)),
        "dwell_time_seconds": _round_or_none(_dwell_seconds(cycle)),
        "track_clear_seconds": _round_or_none(_track_clear_seconds(cycle)),
        "call_max_out_seconds": _round_or_none(_call_max_out_seconds(cycle)),
        "terminated_by_timeout": bool(cycle.terminated_by_timeout),
    }
