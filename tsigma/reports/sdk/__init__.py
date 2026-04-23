"""
Report plugin SDK.

Shared helpers for TSIGMA report plugins.  Reports are plugins, and
plugins should be able to stand on a well-defined toolbox instead of
copy-pasting event codes, query patterns, time bucketing and config
lookups into every file.

`BaseReport` and `ReportRegistry` intentionally live in
`tsigma.reports.registry` — they define the *contract* between core
and plugins.  This package provides the *toolbox* plugins use to
implement that contract.
"""

from .aggregates import pct, percentile_from_sorted, safe_avg, safe_max, safe_min
from .config import (
    load_channel_to_approach,
    load_channel_to_ped_phase,
    load_channel_to_phase,
    load_channels_for_phase,
)
from .cycles import (
    fetch_cycle_arrivals,
    fetch_cycle_boundaries,
    fetch_cycle_summary,
)
from .events import (
    DETECTOR_EVENT_CODES,
    DIRECTION_MAP,
    EVENT_DETECTOR_OFF,
    EVENT_DETECTOR_ON,
    EVENT_FORCE_OFF,
    EVENT_GAP_OUT,
    EVENT_GREEN_TERMINATION,
    EVENT_MAX_OUT,
    EVENT_NAMES,
    EVENT_PED_CALL,
    EVENT_PED_CLEARANCE,
    EVENT_PED_DONT_WALK,
    EVENT_PED_WALK,
    EVENT_PHASE_END,
    EVENT_PHASE_GREEN,
    EVENT_PREEMPTION_ADVANCE_WARNING,
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
    EVENT_RED_CLEARANCE,
    EVENT_TSP_CHECK_IN,
    EVENT_TSP_CHECK_OUT,
    EVENT_TSP_EARLY_GREEN,
    EVENT_TSP_EXTEND_GREEN,
    EVENT_YELLOW_CLEARANCE,
    TERMINATION_CODES,
    TERMINATION_NAMES,
)
from .occupancy import accumulate_on_time, bin_occupancy_pct, calculate_occupancy
from .plans import fetch_plans, plan_at, programmed_split
from .queries import fetch_events, fetch_events_split
from .time_bins import bin_index, bin_timestamp, parse_time, total_bins

__all__ = [
    # aggregates
    "safe_avg",
    "safe_min",
    "safe_max",
    "pct",
    "percentile_from_sorted",
    # events
    "DETECTOR_EVENT_CODES",
    "DIRECTION_MAP",
    "EVENT_DETECTOR_OFF",
    "EVENT_DETECTOR_ON",
    "EVENT_FORCE_OFF",
    "EVENT_GAP_OUT",
    "EVENT_GREEN_TERMINATION",
    "EVENT_MAX_OUT",
    "EVENT_NAMES",
    "EVENT_PED_CALL",
    "EVENT_PED_CLEARANCE",
    "EVENT_PED_DONT_WALK",
    "EVENT_PED_WALK",
    "EVENT_PHASE_END",
    "EVENT_PHASE_GREEN",
    "EVENT_PREEMPTION_ADVANCE_WARNING",
    "EVENT_PREEMPTION_CALL_INPUT_ON",
    "EVENT_PREEMPTION_GATE_DOWN",
    "EVENT_PREEMPTION_CALL_INPUT_OFF",
    "EVENT_PREEMPTION_ENTRY_STARTED",
    "EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE",
    "EVENT_PREEMPTION_BEGIN_DWELL",
    "EVENT_PREEMPTION_LINK_ACTIVE_ON",
    "EVENT_PREEMPTION_LINK_ACTIVE_OFF",
    "EVENT_PREEMPTION_MAX_PRESENCE",
    "EVENT_PREEMPTION_BEGIN_EXIT",
    "EVENT_RED_CLEARANCE",
    "EVENT_TSP_CHECK_IN",
    "EVENT_TSP_EARLY_GREEN",
    "EVENT_TSP_EXTEND_GREEN",
    "EVENT_TSP_CHECK_OUT",
    "EVENT_YELLOW_CLEARANCE",
    "TERMINATION_CODES",
    "TERMINATION_NAMES",
    # time
    "parse_time",
    "bin_timestamp",
    "bin_index",
    "total_bins",
    # queries
    "fetch_events",
    "fetch_events_split",
    # config
    "load_channel_to_phase",
    "load_channel_to_ped_phase",
    "load_channel_to_approach",
    "load_channels_for_phase",
    # plans
    "fetch_plans",
    "plan_at",
    "programmed_split",
    # occupancy
    "calculate_occupancy",
    "accumulate_on_time",
    "bin_occupancy_pct",
    # cycles (pre-computed aggregates)
    "fetch_cycle_boundaries",
    "fetch_cycle_arrivals",
    "fetch_cycle_summary",
]
