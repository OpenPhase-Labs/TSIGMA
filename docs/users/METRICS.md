# TSIGMA Metrics & Analytics Reference

This document describes the traffic signal performance metrics calculated by TSIGMA, including algorithms, thresholds, and usage.

## Overview

TSIGMA calculates performance metrics from high-resolution event data collected from traffic signal controllers. These metrics help identify:

- **Operational issues** - Stuck detectors, communication failures
- **Timing problems** - Split failures, coordination drift
- **Safety concerns** - Yellow/red light violations
- **Efficiency opportunities** - Arrival on green optimization

---

## Metric Categories

| Category | Metrics | Purpose |
|----------|---------|---------|
| **Detector** | Stuck detection, Gap analysis, Occupancy | Detector health |
| **Phase** | Split monitor, Terminations, Skipped phases | Timing performance |
| **Coordination** | Offset drift, Pattern changes, Quality | System coordination |
| **Preemption** | Duration, Recovery time | Emergency response |
| **Volume** | Counts, PHF, K-Factor, D-Factor | Demand analysis |
| **Safety** | Arrivals on Green, Yellow/Red activations | Safety monitoring |
| **Health** | Composite scores | Overall assessment |

---

## Detector Metrics

### Stuck Detector Detection

Identifies detectors that are continuously ON or OFF, indicating malfunction.

**Algorithm:**
```sql
-- Find detectors stuck ON (no OFF event for 30+ minutes)
SELECT signal_id, event_param, MAX(event_time) as last_off
FROM controller_event_log
WHERE event_code = 81  -- Detector Off
GROUP BY signal_id, event_param
HAVING MAX(event_time) < NOW() - INTERVAL '30 minutes'
```

**Thresholds:**
| Status | Condition |
|--------|-----------|
| `HEALTHY` | Normal on/off cycling |
| `STUCK_ON` | Continuously ON > 30 minutes |
| `STUCK_OFF` | No activations > 30 minutes |
| `ERRATIC` | Rapid cycling (chatter) |
| `NO_DATA` | No events in analysis period |

**Event Codes Used:**
- 81: Detector Off
- 82: Detector On

**API Endpoint:** `GET /api/v1/analytics/detectors/stuck`

---

### Gap Analysis

Measures time gaps between successive detector actuations. Short gaps indicate congestion.

**Algorithm:**
```sql
SELECT
    signal_id, event_param,
    event_time,
    event_time - LAG(event_time) OVER (
        PARTITION BY signal_id, event_param ORDER BY event_time
    ) as gap_duration
FROM controller_event_log
WHERE event_code = 82  -- Detector On
```

**Metrics Calculated:**
| Metric | Description |
|--------|-------------|
| `avg_gap` | Average time between actuations |
| `min_gap` | Minimum gap (potential queue) |
| `max_gap` | Maximum gap (low demand period) |
| `gap_stddev` | Gap variability |

**API Endpoint:** `GET /api/v1/analytics/detectors/gaps`

---

### Detector Occupancy

Percentage of time a detector is occupied (ON state).

**Formula:**
```
Occupancy = (Total ON Time / Analysis Period) × 100
```

**Algorithm:**
```sql
SELECT
    signal_id, event_param,
    time_bucket('15 minutes', event_time) as bin,
    SUM(EXTRACT(EPOCH FROM (off_time - on_time))) / 900.0 * 100 as occupancy_pct
FROM detector_activations
GROUP BY signal_id, event_param, bin
```

**Interpretation:**
| Occupancy | Condition |
|-----------|-----------|
| < 10% | Low demand |
| 10-50% | Normal operation |
| 50-80% | Heavy demand |
| > 80% | Near capacity / potential queue |

**API Endpoint:** `GET /api/v1/analytics/detectors/occupancy`

---

## Phase Metrics

### Split Monitor

Tracks actual phase timing versus programmed splits.

**Metrics Per Phase:**
| Metric | Description |
|--------|-------------|
| `green_time` | Actual green duration |
| `yellow_time` | Yellow clearance duration |
| `red_clearance` | Red clearance duration |
| `total_split` | Total phase time |

**Termination Types:**
| Type | Event Code | Description |
|------|------------|-------------|
| Gap Out | 4 | Phase ended due to gap in detector actuations |
| Max Out | 5 | Phase reached maximum green time |
| Force Off | 6 | Phase terminated by coordination |
| Skip | (no event 1) | Phase was skipped entirely |

**Algorithm:**
```sql
SELECT
    event_param as phase,
    event_time as green_start,
    LEAD(event_time) FILTER (WHERE event_code = 8) as yellow_start,
    LEAD(event_time) FILTER (WHERE event_code = 10) as red_start,
    CASE
        WHEN EXISTS(event_code = 4) THEN 'gap_out'
        WHEN EXISTS(event_code = 5) THEN 'max_out'
        WHEN EXISTS(event_code = 6) THEN 'force_off'
    END as termination_type
FROM controller_event_log
WHERE event_code IN (1, 4, 5, 6, 8, 10)
```

**API Endpoint:** `GET /api/v1/analytics/phases/split-monitor`

---

### Split Failure Detection

Identifies when demand exceeds capacity during a phase.

**Definition:** A split failure occurs when:
1. **Green Occupancy ≥ 79%** at start of green (queue present)
2. **Red Occupancy ≥ 79%** in first 5 seconds of red (vehicles couldn't clear)

**Algorithm:**
```sql
WITH phase_cycles AS (
    SELECT
        phase,
        green_start,
        green_end,
        red_start
    FROM phase_timing
),
occupancy AS (
    SELECT
        cycle_id,
        -- Occupancy at green start (first 5 seconds)
        AVG(CASE WHEN event_time BETWEEN green_start AND green_start + INTERVAL '5 seconds'
            THEN occupancy END) as green_start_occ,
        -- Occupancy at red start (first 5 seconds)
        AVG(CASE WHEN event_time BETWEEN red_start AND red_start + INTERVAL '5 seconds'
            THEN occupancy END) as red_start_occ
    FROM detector_occupancy
    JOIN phase_cycles ON ...
)
SELECT *
FROM occupancy
WHERE green_start_occ >= 0.79
  AND red_start_occ >= 0.79
```

**Thresholds:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `green_occ_threshold` | 79% | Minimum green start occupancy |
| `red_occ_threshold` | 79% | Minimum red start occupancy |
| `evaluation_period` | 5 seconds | Time window for occupancy check |

**API Endpoint:** `GET /api/v1/analytics/phases/split-failures` (Available via the Reports API at `/api/v1/reports/split-failure`, not as an analytics endpoint)

---

### Skipped Phase Detection

Identifies phases that should have run but were skipped.

**Algorithm:**
```sql
WITH expected_phases AS (
    -- Phases that should run based on configuration
    SELECT phase_id FROM phase_config WHERE active = true
),
actual_phases AS (
    -- Phases that actually ran
    SELECT DISTINCT event_param as phase_id
    FROM controller_event_log
    WHERE event_code = 1  -- Phase On
      AND event_time BETWEEN :start AND :end
)
SELECT
    e.phase_id,
    CASE WHEN a.phase_id IS NULL THEN 'skipped' ELSE 'served' END as status
FROM expected_phases e
LEFT JOIN actual_phases a ON e.phase_id = a.phase_id
```

**API Endpoint:** `GET /api/v1/analytics/phases/skipped`

---

## Coordination Metrics

### Offset Drift Analysis

Measures deviation of actual cycle length from expected cycle length.

**Metrics:**
| Metric | Description |
|--------|-------------|
| `expected_cycle` | Programmed cycle length (mode) |
| `actual_cycles` | Measured cycle lengths |
| `drift_stddev` | Standard deviation of drift |
| `max_drift` | Maximum observed deviation |

**Algorithm:**
```sql
WITH cycles AS (
    SELECT
        event_time,
        event_time - LAG(event_time) OVER (ORDER BY event_time) as cycle_length
    FROM controller_event_log
    WHERE event_code = 151  -- Coordinated Phase Yield Point
),
expected AS (
    SELECT MODE() WITHIN GROUP (ORDER BY cycle_length) as expected_cycle
    FROM cycles
)
SELECT
    expected_cycle,
    STDDEV(EXTRACT(EPOCH FROM (cycle_length - expected_cycle))) as drift_stddev,
    MAX(ABS(EXTRACT(EPOCH FROM (cycle_length - expected_cycle)))) as max_drift
FROM cycles, expected
```

**Interpretation:**
| Drift StdDev | Assessment |
|--------------|------------|
| < 1 second | Excellent coordination |
| 1-3 seconds | Normal operation |
| > 3 seconds | Investigation needed |

**API Endpoint:** `GET /api/v1/analytics/coordination/offset-drift`

---

### Coordination Quality

Percentage of cycles where offset error is within tolerance.

**Formula:**
```
Quality = (Cycles Within Tolerance / Total Cycles) × 100
```

**Default Tolerance:** ±2.0 seconds

**API Endpoint:** `GET /api/v1/analytics/coordination/quality`

---

### Pattern History

Tracks coordination pattern changes over time.

**Event Code:** 131 (Coord Pattern Change)

**Output:**
```json
[
    {
        "timestamp": "2024-01-15T06:00:00",
        "from_pattern": 1,
        "to_pattern": 2,
        "duration": "PT8H"
    },
    {
        "timestamp": "2024-01-15T14:00:00",
        "from_pattern": 2,
        "to_pattern": 3,
        "duration": "PT4H"
    }
]
```

**API Endpoint:** `GET /api/v1/analytics/coordination/patterns`

---

### Time-Space Diagram Average

Typical-day corridor visualization built from the median cycle observed across matched weekdays within a calendar range.

**Definition:** For each signal on the corridor, the report selects the **median** green-to-green cycle (middle element after sorting by green duration) from every eligible weekday in the analysis range, then synthesises a repeating green-yellow-red pattern across the daily window. The median is used instead of the mean because a single day's preemption, incident, or transit priority event can skew the mean but cannot move the middle element of a sorted distribution. As a plan-match validation guard, the report refuses to mix cycles from days operating under different signal plans: if cycle length, offset, or splits differ across selected days, the report raises `ValueError` rather than emit a misleading "average".

**Algorithm:**
```
1. Enumerate eligible weekdays in [start_date, end_date]
   whose Python weekday() is in days_of_week.
2. Load the reference signal's plan for each day.
   If cycle_length / offset / splits differ across days, raise.
3. For each signal, for each eligible day:
       fetch phase events (green, yellow clearance, red clearance)
       for the coordinated phase during [start_time, end_time].
4. Build green -> yellow -> red -> next-green cycles.
   Skip partial cycles at the window boundary.
5. Sort that signal's cycles by green duration and pick the
   middle one. That (green, yellow, red) tuple is the signal's
   median cycle.
6. Compute the coordinated reference offset:
       ref_point = offset - (median_green + median_yellow
                             - programmed_split)
7. Synthesise a repeating cycle pattern from the window anchor
   through (end_time + 120s tail), emitting one row per phase
   interval boundary.
8. Project green arrivals downstream using distance_ft / speed
   in the visualiser.
```

**Output columns:**

| Column | Description |
|--------|-------------|
| `signal_id` | Corridor signal identifier. |
| `phase_number` | Coordinated phase for this signal. |
| `cycle_index` | 0-based index of the synthesised cycle within the window. |
| `event` | `green`, `yellow`, or `red` — the phase interval that begins at `event_time`. |
| `event_time` | ISO-8601 timestamp of the interval start. |
| `distance_ft` | Signal distance from the corridor origin, for downstream projection. |
| `cycle_length_seconds` | Synthesised cycle duration (broadcast per signal). |
| `median_green_seconds` | Median observed green (broadcast). |
| `median_yellow_seconds` | Median observed yellow clearance (broadcast). |
| `median_red_seconds` | Median observed red clearance (broadcast). |
| `programmed_split_seconds` | Plan's programmed split for this phase (broadcast). |
| `days_included` | Count of distinct days that contributed at least one cycle (broadcast). |
| `speed_limit_applied` | Speed (mph) used for downstream arrival projection (broadcast). |

The last seven columns are "broadcast": they hold the same value on every row for a given signal. Visualisers typically read them once per signal.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_ids` | `list[str]` | Ordered corridor signals, upstream to downstream. |
| `start_date` | `str` | Calendar range start, `YYYY-MM-DD`. |
| `end_date` | `str` | Calendar range end, `YYYY-MM-DD`. |
| `start_time` | `str` | Daily window start, `HH:MM`. |
| `end_time` | `str` | Daily window end, `HH:MM`. |
| `days_of_week` | `list[int]` | **Python weekday convention: Monday=0 .. Sunday=6.** |
| `direction_phase_map` | `dict[str,int]` | `signal_id` -> coordinated phase number. |
| `distances` | `dict[str,float]` | Optional. `signal_id` -> distance from origin in feet. |
| `speed_limit_mph` | `int` | Optional. Fallback speed (default 30) when config has none. |

**Weekday convention:** `days_of_week` uses Python's `datetime.weekday()` (Monday=0, Sunday=6). ATSPM 5.x's `DayOfWeek` enum uses Sunday=0 — callers porting from ATSPM must shift with `(day_of_week + 6) % 7`.

**API Endpoint:** `POST /api/v1/reports/time-space-diagram-average`

**When to use:** Choose **Time-Space Diagram Average** for baseline corridor performance characterisation and before/after coordination studies — the cross-day median filters out one-off disturbances so you see how the corridor runs on a typical day, not on the day you happened to pull. Choose the single-window **Time-Space Diagram** when you want raw observed phase intervals from one specific analysis window — incident reconstruction, complaint investigation, or any case where day-to-day variability is the subject of the study rather than something to suppress.

---

## Preemption Metrics

### Preemption Summary

Analyzes frequency and duration of preemption events.

**Event Codes:**
- 105: Preempt Entry Started
- 111: Preemption Begin Exit Interval

**Metrics:**
| Metric | Description |
|--------|-------------|
| `total_preemptions` | Count of preemption events |
| `by_preempt_number` | Breakdown by preempt 1, 2, etc. |
| `avg_duration` | Average preemption length |
| `max_duration` | Longest preemption |
| `pct_time_preempted` | Percentage of period in preemption |

**API Endpoint:** `GET /api/v1/analytics/preemptions/summary`

---

### Preemption Recovery Time

Time from preemption end to restoration of coordination.

**Algorithm:**
```sql
SELECT
    preempt_end.event_time as preempt_end,
    cycle_zero.event_time as recovery_complete,
    cycle_zero.event_time - preempt_end.event_time as recovery_time
FROM controller_event_log preempt_end
JOIN controller_event_log cycle_zero ON
    cycle_zero.event_code = 151  -- Coordinated Phase Yield Point
    AND cycle_zero.event_time > preempt_end.event_time
    AND cycle_zero.event_time = (
        SELECT MIN(event_time) FROM controller_event_log
        WHERE event_code = 151 AND event_time > preempt_end.event_time
    )
WHERE preempt_end.event_code = 111  -- Preemption Begin Exit Interval
```

**API Endpoint:** `GET /api/v1/analytics/preemptions/recovery`

---

### Preempt Detail

Full lifecycle analysis of every preemption cycle at a signal — pairs each request with its downstream service events and derives delay, time-to-service, dwell, track-clear, and max-presence timings on a per-cycle basis.

**Definition:** A **preempt cycle** is the full sequence of controller events triggered by one preemption request on one channel (preempt number). A cycle begins when the call input goes on (event 102) or, for "no-delay" paths, when entry is started directly (event 105). Within the cycle the controller may raise gates (103), drop the call input (104), start entry (105), begin track clearance (106), begin dwell service (107), activate/deactivate the preemption link (108/109), and flag max-presence (110). The cycle ends on the begin-exit-interval event (111), on the arrival of a new 105 that opens the next cycle, or when no further events arrive within a 20-minute watchdog window.

**Event lifecycle:**

| Code | SDK constant | Meaning |
|------|------|---------|
| 102 | `EVENT_PREEMPTION_CALL_INPUT_ON` | Preempt call input asserted — request received |
| 103 | `EVENT_PREEMPTION_GATE_DOWN` | Railroad / emergency gate reached down position |
| 104 | `EVENT_PREEMPTION_CALL_INPUT_OFF` | Preempt call input de-asserted |
| 105 | `EVENT_PREEMPTION_ENTRY_STARTED` | Controller began preempt service (entry interval) |
| 106 | `EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE` | Track-clearance interval started |
| 107 | `EVENT_PREEMPTION_BEGIN_DWELL` | Dwell (hold) interval started |
| 108 | `EVENT_PREEMPTION_LINK_ACTIVE_ON` | Preemption link asserted active |
| 109 | `EVENT_PREEMPTION_LINK_ACTIVE_OFF` | Preemption link deactivated |
| 110 | `EVENT_PREEMPTION_MAX_PRESENCE` | Call held longer than configured max-presence timer |
| 111 | `EVENT_PREEMPTION_BEGIN_EXIT` | Exit interval started — cycle complete |

**Derived metrics:**

| Metric | Formula | Meaning |
|--------|---------|---------|
| `has_delay` | `True` if cycle opened with a 102, `False` if it opened directly on 105 | Distinguishes requested service from direct-entry service |
| `delay_seconds` | `entry_started − cycle_start`, or `0.0` when `has_delay` is false or 105 never arrived | Time the request waited before the controller began serving it |
| `time_to_service_seconds` | `max(begin_track_clearance, begin_dwell_service) − service_anchor`, where `service_anchor = entry_started` for delayed cycles and `cycle_start` otherwise | Time from the start of service to when track-clearance or dwell actually begins — the observable "how long until the preempt is really running" |
| `dwell_time_seconds` | `cycle_end − begin_dwell_service` | Length of the dwell (hold) phase |
| `track_clear_seconds` | `begin_dwell_service − begin_track_clearance` | Duration of the track-clearance interval |
| `call_max_out_seconds` | `max_presence_exceeded − cycle_start`, `None` if 110 never fired | Elapsed time to max-presence flag; `None` means max-presence was not exceeded |
| `terminated_by_timeout` | `True` if the cycle was force-closed by the 20-minute watchdog | A cycle that never saw a 111 or a follow-on 105 |

All duration columns are reported in seconds, rounded to two decimals. The `cycle_start`, `cycle_end`, and raw event timestamp columns are ISO-8601 strings; optional timestamp columns remain `None` (not `NaN`) when the corresponding event was not observed.

**Algorithm:**
```
# Per-channel single-pass state machine over events ordered by time.
for each channel in group_by(event_param):
    current = None
    last_time = None
    for (event_time, code) in channel.events:
        # Watchdog: close an open cycle after 20 minutes of silence.
        if current and event_time - last_time > 20 minutes:
            finalize(current, end=last_time, timed_out=True)
            current = None

        if code == 102:                      # input on
            current = current or open_delayed_cycle(event_time)
            current.input_on.append(event_time)
        elif code == 105:                    # entry started
            if current is None:
                current = open_nodelay_cycle(event_time)
            elif current.has_delay and current.entry_started is None:
                current.entry_started = event_time
            else:                            # back-to-back 105s
                finalize(current, end=event_time)
                current = open_nodelay_cycle(event_time)
        elif code == 111:                    # begin exit
            finalize(current, end=event_time)
            current = None
        else:                                # 103, 104, 106-110
            if current: current.record(code, event_time)

        last_time = event_time

    if current: finalize(current, end=last_time)  # window-end close
```

Stray inner events (103, 104, 106-110) that arrive with no open cycle are ignored. Repeated occurrences of the same inner event within a cycle keep the *first* timestamp.

**Output columns** (one row per cycle, sorted by `preempt_number`, then `cycle_start`):

| Column | Type | Description |
|--------|------|-------------|
| `preempt_number` | int | Preempt channel (the event `event_param` value) |
| `cycle_start` | string (ISO-8601) | First event of the cycle — 102 for delayed, 105 for no-delay |
| `cycle_end` | string (ISO-8601) | Cycle-ending event time — 111, arrival of next 105, or timeout stamp |
| `input_on` | string (ISO-8601) | First 102 in the cycle, or fallback to `entry_started` / `cycle_start` for no-delay cycles |
| `input_off` | string (ISO-8601) \| null | First 104 observed, `None` if never asserted off |
| `gate_down` | string (ISO-8601) \| null | First 103, `None` if no gate event |
| `entry_started` | string (ISO-8601) \| null | 105 timestamp; `None` on delayed cycles that never entered service |
| `begin_track_clearance` | string (ISO-8601) \| null | 106 timestamp |
| `begin_dwell_service` | string (ISO-8601) \| null | 107 timestamp |
| `max_presence_exceeded` | string (ISO-8601) \| null | 110 timestamp; `None` when max-presence never tripped |
| `has_delay` | bool | Whether the cycle opened on a 102 request |
| `delay_seconds` | float | See derived metrics above |
| `time_to_service_seconds` | float | See derived metrics above |
| `dwell_time_seconds` | float | See derived metrics above |
| `track_clear_seconds` | float | See derived metrics above |
| `call_max_out_seconds` | float \| null | See derived metrics above |
| `terminated_by_timeout` | bool | Cycle was force-closed by the 20-minute watchdog |

**Params:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `signal_id` | string | *required* | Signal identifier |
| `start` | string (ISO-8601) | *required* | Analysis window start |
| `end` | string (ISO-8601) | *required* | Analysis window end |
| `preempt_number` | int \| null | `null` | Optional preempt channel filter; when set, only cycles on that `event_param` channel are returned |

**Thresholds:**

| Parameter | Value | Description |
|-----------|-------|-------------|
| cycle-inactivity timeout | 20 minutes | If no further preempt events arrive within 20 minutes of the last event in an open cycle, the cycle is force-closed with `cycle_end` set to the last event time and `terminated_by_timeout = true`. Matches the watchdog used by UDOT ATSPM 5.x. |

**API Endpoint:** `POST /api/v1/reports/preempt-detail` (executed via the generic Reports API — TSIGMA reports are not exposed as per-report analytics routes).

**When to use this vs. the simpler preemption reports:** Reach for **Preempt Detail** when you need service-quality metrics per cycle — delay, time-to-service, dwell duration, track-clearance duration, or max-presence flags — and you want one row per full lifecycle, including cycles that were force-closed by timeout. Use **Preempt Service** when you only need plan-level counts (how many preempts ran under each coordination plan). Use **Preempt Service Request** when you want the demand side of the story — request frequency and inter-arrival patterns independent of whether service actually happened. The older **`preemption`** report remains the right choice for a lightweight list of raw 102/104 entry/exit pairs with durations when you do not need the full event lifecycle or derived timings.

---

### Preempt Service

Answers "how many preempt services were granted under each active signal plan during the analysis window?"

**Definition:** A preempt *service* is a granted preemption, recorded by controller **event code 105 (Preempt Entry Started)**. This report counts every 105 event, buckets it under the signal plan that was active at the moment the event fired, and emits one timeline row per event. Plan boundaries are half-open intervals `[effective_from, effective_to)` — an event exactly at `effective_from` belongs to the new plan; an event exactly at `effective_to` belongs to the next plan. Events that fall outside any known plan are assigned the sentinel plan number `unknown`.

**Algorithm:**
```
events  = fetch_events(signal_id, start, end, codes=[105])
plans   = fetch_plans(signal_id, start, end)

if events is empty:
    return empty DataFrame

for each event in events:
    plan = active_plan_at(plans, event.event_time)   # half-open [from, to)
    emit row {
        event_time, event_param,
        plan_number, plan_start, plan_end,
        plan_preempt_count = 0            # filled in below
    }

# Broadcast the per-plan total onto every row
counts = group rows by plan_number, count
for each row: row.plan_preempt_count = counts[row.plan_number]
```

**Output columns:**
| Column | Description |
|--------|-------------|
| `event_time` | ISO-8601 timestamp of the 105 event |
| `event_param` | Preempt channel/number from the controller event |
| `plan_number` | Active plan number at `event_time`, or `"unknown"` if no plan covers it |
| `plan_start` | ISO-8601 start of that plan interval (`null` if unknown) |
| `plan_end` | ISO-8601 end of that plan interval (`null` if plan is open-ended or unknown) |
| `plan_preempt_count` | Total 105 events observed under this plan in the window (broadcast onto every row) |

**Params:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_id` | string | Signal identifier |
| `start` | string (ISO-8601) | Analysis window start |
| `end` | string (ISO-8601) | Analysis window end |

**API Endpoint:** `POST /api/v1/reports/preempt-service`

**Contrast with Preempt Service Request and Preempt Detail:**

Three related reports look at preemption from different angles. Use **Preempt Service** (this report, event 105) when you want to count how many preemptions were actually *granted* and see how that count breaks down by signal plan — useful for retiming reviews and enforcement reporting. Use **Preempt Service Request** (event 102, Preempt Call Input On) when you want to count how many preemptions were *requested*, regardless of whether they were serviced — the gap between 102 and 105 counts is your grant rate. Use **Preempt Detail** when you need the full lifecycle state machine (call, entry, hold, exit) for each individual preemption, including durations and recovery behavior. Preempt Service is intentionally the lightweight counter; it does not match entry/exit pairs or compute durations.

---

### Preempt Service Request

Measures preemption **demand** (calls raised) indexed by active timing plan — the "requested but maybe not served" counterpart to Preempt Service.

**Definition:** Each occurrence of event code **102** (`PreemptCallInputOn`) is counted once per active signal plan interval. The plan active at the moment the request was raised is attributed to that request; if no plan is active, the row is attributed to a sentinel plan labelled `unknown` bounded by the analysis window.

**Algorithm:**
```
1. Fetch all event-code-102 rows for the signal over [start, end].
2. Fetch signal plan history (effective_from / effective_to) over the same window.
3. For each 102 event:
     - Resolve the active plan via half-open interval [effective_from, effective_to).
     - Emit one timeline row: event_time, event_param, plan_number, plan_start, plan_end.
     - Increment plan_counts[plan_number].
4. Second pass: broadcast plan_counts[plan_number] onto every row as plan_request_count.
```

**Output Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `event_time` | ISO-8601 string | Timestamp of the 102 event |
| `event_param` | int | Raw event parameter (preempt number / source) |
| `plan_number` | string | Active plan number at request time, or `"unknown"` |
| `plan_start` | ISO-8601 string | `effective_from` of the active plan (or window start if unknown) |
| `plan_end` | ISO-8601 string | `effective_to` of the active plan (or window end if open/unknown) |
| `plan_request_count` | int | Total 102 events attributed to this plan in the window (broadcast) |

**Params (`PreemptServiceRequestParams`):**
| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_id` | string | Signal identifier |
| `start` | string (ISO-8601) | Analysis window start |
| `end` | string (ISO-8601) | Analysis window end |

**API Endpoint:** `POST /api/v1/reports/preempt-service-request`

**Demand vs. supply:** This report pairs directly with **Preempt Service** (event code 105, services *granted*). Compare `plan_request_count` here to `plan_preempt_count` from that report over the same window and plan: requests without a matching service indicate **failed, cancelled, or duplicate preemption attempts** — classic signatures of check-in/check-out priority calls that dropped, short-holds that never qualified, or EVP equipment chatter. A persistent gap on a given plan is a maintenance flag, not a timing defect.

---

## Volume Metrics

### Approach Volume

Vehicle counts per approach/time bin.

**Calculation:**
```sql
SELECT
    signal_id, event_param,
    time_bucket('15 minutes', event_time) as bin,
    COUNT(*) as volume
FROM controller_event_log
WHERE event_code = 82  -- Detector On
GROUP BY signal_id, event_param, bin
```

---

### Peak Hour Factor (PHF)

Ratio of peak hour volume to 4 times the peak 15-minute volume.

**Formula:**
```
PHF = V_hour / (4 × V_peak15)
```

Where:
- `V_hour` = Total volume in peak hour
- `V_peak15` = Volume in peak 15-minute interval

**Interpretation:**
| PHF | Traffic Pattern |
|-----|-----------------|
| < 0.70 | Highly peaked |
| 0.70-0.85 | Typical peak |
| 0.85-0.95 | Spread peak |
| > 0.95 | Nearly uniform |

---

### K-Factor

Peak hour volume as percentage of daily volume.

**Formula:**
```
K = V_peak_hour / V_daily × 100
```

**Typical Values:** 8-12% for urban areas

---

### D-Factor

Directional distribution during peak hour.

**Formula:**
```
D = V_peak_direction / V_total × 100
```

**Typical Values:** 55-65% for commuter corridors

---

## Left Turn Metrics

Metrics specific to left-turn movement analysis — eligibility screening, volume analysis, and phase-study decision support.

---

### Left Turn Gap Data Check

Pre-flight eligibility gate for the full **Left Turn Gap** report — a fast screen that answers "does this signal/approach have enough detector configuration and hi-res event data to produce a meaningful permissive-left-turn analysis?" BEFORE committing to the expensive full report.

**Definition:** A set of boolean readiness flags plus threshold checks computed across the AM peak window (**06:00–09:00**) and PM peak window (**15:00–19:00**) for the analysis date range. A single-row result; the `overall_ready` flag is the go/no-go signal for launching the full Left Turn Gap report.

**Readiness flags:**
| Flag | Description |
|------|-------------|
| `left_turn_volume_ok` | Peak-hour LT volume meets `volume_per_hour_threshold` in **at least one** window (AM or PM) |
| `gap_out_ok` | Through-phase gap-out rate is **at or below** `gap_out_threshold` in **both** windows |
| `ped_cycle_ok` | Ped-cycle rate is **at or below** `pedestrian_threshold` in **both** windows |
| `insufficient_detector_event_count` | No detector-ON events on LT channels in one or both windows |
| `insufficient_cycle_aggregation` | No through-phase green starts in one or both windows |
| `insufficient_phase_termination` | No gap-out / max-out / force-off events on the through phase in one or both windows |
| `insufficient_ped_aggregations` | Ped phase configured on approach, cycles observed, but zero ped calls in both windows (suppressed when no ped phase is configured) |
| `insufficient_split_fail_aggregations` | LT cycles or detector hits missing — split-failure metrics would be uncomputable |
| `insufficient_left_turn_gap_aggregations` | Through cycles or detector hits missing — gap aggregation would be uncomputable |
| `overall_ready` | Convenience: `volume_ok AND gap_ok AND ped_ok AND no insufficient flags` |

**Algorithm:**
```
1. Load signal config as of `start`; locate approach by `approach_id`.
2. Identify left-turn detector channels and the opposing through phase
   (plus protected LT phase and ped phase, if configured).
3. If approach is unknown or has no LT detectors: return a hard not-ready row
   (all insufficient flags True).
4. Fetch phase + detector events over [start, end] limited to LT channels,
   then filter by `days_of_week`.
5. Single pass over events → AM and PM per-window accumulators:
     - detector ONs on LT channels (+ 15-min bin counts for peak volume)
     - through-phase greens (cycle count)
     - through-phase terminations (4/5/6) and gap-outs (4)
     - LT-phase greens (for split-fail eligibility)
     - ped calls, tracked per through-phase cycle
6. Compute the six insufficient_* flags from per-window counts.
   `insufficient_ped_aggregations` is ONLY raised when the approach has a
   ped phase configured — intersections without ped phases are not penalized.
7. If every insufficient flag is True, short-circuit with metrics = None.
8. Otherwise compute:
     - AM/PM peak-hour LT volume = max rolling sum of 4 × 15-min bins per day
     - AM/PM gap-out % = gap_outs / cycles
     - AM/PM ped-cycle % = cycles_with_ped / cycles
9. Apply the three OK rules:
     - volume_ok  = AM peak ≥ threshold OR PM peak ≥ threshold  (either suffices)
     - gap_ok     = AM gap ≤ threshold AND PM gap ≤ threshold   (both required)
     - ped_ok     = AM ped ≤ threshold AND PM ped ≤ threshold   (both required)
10. overall_ready = volume_ok AND gap_ok AND ped_ok AND no insufficient flags.
```

**Thresholds:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `volume_per_hour_threshold` | 60 | Minimum acceptable peak-hour left-turn volume (veh/hr) |
| `gap_out_threshold` | 0.5 | Maximum acceptable gap-out rate per window (0.0–1.0) |
| `pedestrian_threshold` | 0.25 | Maximum acceptable ped-cycle rate per window (0.0–1.0) |

**Output Columns** (single row):
| Column | Type | Description |
|--------|------|-------------|
| `signal_id` | string | Signal identifier |
| `approach_id` | string | Left-turn approach identifier |
| `start` | ISO-8601 string | Analysis window start |
| `end` | ISO-8601 string | Analysis window end |
| `left_turn_volume_ok` | bool | Volume threshold satisfied (AM or PM) |
| `gap_out_ok` | bool | Gap-out threshold satisfied (AM and PM) |
| `ped_cycle_ok` | bool | Ped-cycle threshold satisfied (AM and PM) |
| `insufficient_detector_event_count` | bool | LT detector data missing in AM or PM |
| `insufficient_cycle_aggregation` | bool | Through-phase cycles missing in AM or PM |
| `insufficient_phase_termination` | bool | No 4/5/6 terminations on through phase in AM or PM |
| `insufficient_ped_aggregations` | bool | Ped phase configured but zero calls observed |
| `insufficient_split_fail_aggregations` | bool | LT cycles or detector hits missing |
| `insufficient_left_turn_gap_aggregations` | bool | Through cycles or detector hits missing |
| `am_peak_left_turn_volume` | int \| null | Peak-hour LT volume in AM window (veh/hr) |
| `pm_peak_left_turn_volume` | int \| null | Peak-hour LT volume in PM window (veh/hr) |
| `am_gap_out_pct` | float \| null | Gap-out rate in AM window (0.0–1.0, 4 dp) |
| `pm_gap_out_pct` | float \| null | Gap-out rate in PM window (0.0–1.0, 4 dp) |
| `am_ped_pct` | float \| null | Ped-cycle rate in AM window (0.0–1.0, 4 dp) |
| `pm_ped_pct` | float \| null | Ped-cycle rate in PM window (0.0–1.0, 4 dp) |
| `overall_ready` | bool | Go/no-go for running the full Left Turn Gap report |

**Params (`LeftTurnGapDataCheckParams`):**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `signal_id` | string | — | Signal identifier |
| `approach_id` | string | — | Left-turn approach identifier |
| `start` | string (ISO-8601) | — | Analysis window start |
| `end` | string (ISO-8601) | — | Analysis window end |
| `days_of_week` | list[int] | `[0,1,2,3,4]` | Python weekday numbers (0=Mon) to include |
| `volume_per_hour_threshold` | int | `60` | Minimum acceptable peak-hour LT volume (veh/hr) |
| `gap_out_threshold` | float | `0.5` | Max acceptable gap-out rate per window |
| `pedestrian_threshold` | float | `0.25` | Max acceptable ped-cycle rate per window |

**API Endpoint:** `POST /api/v1/reports/left-turn-gap-data-check`

**Workflow:** Always call this FIRST for a candidate signal/approach and inspect `overall_ready`. When `true`, launch the full **Left Turn Gap** report (`/api/v1/reports/left-turn-gap`) with confidence the result will be meaningful. When `false`, read the specific `insufficient_*` flags and the AM/PM metrics to decide whether the issue is a data gap (fix detector config, extend the window, include weekends) or a genuinely unsuitable candidate for permissive-LT analysis (low demand, excessive gap-outs, heavy ped conflicts) — then either remediate and re-check or skip to the next candidate. Running the expensive full report on a not-ready signal wastes compute and produces statistically weak output.

---

### Left Turn Volume

Quantifies left-turn vs opposing-through volumes on an approach and flags intersections that may warrant a dedicated left-turn phase study.

**Definition:** Two parallel reviews are run against the same window:

1. **Cross-product review** — the classic ATSPM check. Flags any approach whose `LT_V × OPP_V` product exceeds the lane-count limit (50,000 for a single opposing lane, 100,000 for two or more).
2. **HCM-style decision-boundary review** — applies one of six HCM formulas keyed on `approach_type` (permissive / permissive_protected / protected) and opposing lane count. If the calculated boundary exceeds the published HCM threshold, the approach is flagged as a candidate for a left-turn phase change.

**Algorithm:**
1. Resolve the target approach from the historical `SignalConfig`; use its `protected_phase_number` if present, else `permissive_phase_number`.
2. Map to the opposing phase via NEMA pairing: `1↔2, 3↔4, 5↔6, 7↔8`. The opposing approach is whichever approach owns that phase as protected or permissive.
3. Treat **every detector on the target approach** as a left-turn detector and **every detector on the opposing approach** as an opposing-through detector. `opposing_lanes` is the detector count on the opposing approach. (A `movement_type` field is not yet carried on `DetectorSnapshot`; when it lands, this classifier should be narrowed to `L` vs `T/TR/TL`.)
4. Fetch `Detector On` events (code 82) for both detector sets across the analysis window.
5. Filter to the configured days-of-week and time-of-day window, then bin into 15-minute intervals, counting LT and opposing events separately per bin.
6. Sum across all bins to get `LT_V` and `OPP_V`.
7. Compute `cross_product = LT_V × OPP_V` and compare to the lane-count limit.
8. Compute the HCM decision boundary using the approach-type formula below and compare to its threshold.
9. Compute the AM (06:00–09:00) and PM (15:00–18:00) peak hours via a 1-hour sliding window over the LT bins.

**Thresholds — HCM decision boundary formulas:**

| approach_type        | opposing_lanes | formula                        | threshold |
|----------------------|----------------|--------------------------------|-----------|
| permissive           | 1              | `LT_V × OPP_V^0.706`           | 9519      |
| permissive           | >1             | `2 × LT_V × OPP_V^0.642`       | 7974      |
| permissive_protected | 1              | `LT_V × OPP_V^0.500`           | 4638      |
| permissive_protected | >1             | `2 × LT_V × OPP_V^0.404`       | 3782      |
| protected            | 1              | `LT_V × OPP_V^0.425`           | 3693      |
| protected            | >1             | `2 × LT_V × OPP_V^0.404`       | 3782      |

**Thresholds — cross-product review:**

| opposing_lanes | limit   |
|----------------|---------|
| 1              | 50,000  |
| >1             | 100,000 |

Either review firing is grounds for follow-up; a signal that fires both is a strong candidate for the full Left Turn Gap study.

**Output columns:**

Per-bin row columns:

| Column                        | Description                                    |
|-------------------------------|------------------------------------------------|
| `bin_start`                   | ISO-8601 start of the 15-minute bin            |
| `left_turn_volume_bin`        | LT detector events in this bin                 |
| `opposing_through_volume_bin` | Opposing-through detector events in this bin   |

Broadcast summary columns (repeated on every row):

| Column                         | Description                                                 |
|--------------------------------|-------------------------------------------------------------|
| `approach_id`                  | Target (LT) approach identifier                             |
| `direction`                    | Compass direction of the LT approach                        |
| `opposing_direction`           | Compass direction of the opposing-through approach          |
| `left_turn_volume`             | `LT_V` — total LT events across the window                  |
| `opposing_through_volume`      | `OPP_V` — total opposing-through events across the window   |
| `opposing_lanes`               | Detector count on the opposing approach                     |
| `cross_product_value`          | `LT_V × OPP_V`                                              |
| `cross_product_review`         | `true` if cross-product exceeds the lane-count limit        |
| `calculated_volume_boundary`   | Value produced by the HCM formula for this approach type    |
| `decision_boundary_threshold`  | HCM threshold for this approach type and lane count         |
| `decision_boundaries_review`   | `true` if calculated boundary exceeds the HCM threshold     |
| `am_peak_hour`                 | ISO-8601 start of the 15-min bin that opens the AM peak     |
| `am_peak_left_turn_volume`     | LT volume in the AM peak hour                               |
| `pm_peak_hour`                 | ISO-8601 start of the 15-min bin that opens the PM peak     |
| `pm_peak_left_turn_volume`     | LT volume in the PM peak hour                               |
| `approach_type`                | Echoes the input parameter                                  |

**Params (`LeftTurnVolumeParams`):**

| Parameter        | Type        | Default          | Description                                                       |
|------------------|-------------|------------------|-------------------------------------------------------------------|
| `signal_id`      | str         | *(required)*     | Signal identifier                                                 |
| `approach_id`    | str         | *(required)*     | Approach whose left turn is analyzed                              |
| `start`          | str         | *(required)*     | Analysis window start (ISO-8601)                                  |
| `end`            | str         | *(required)*     | Analysis window end (ISO-8601)                                    |
| `days_of_week`   | list[int]   | `[0,1,2,3,4]`    | Python weekday numbers to include (0=Mon..6=Sun)                  |
| `start_hour`     | int         | `6`              | Time-of-day window start hour (0–23)                              |
| `start_minute`   | int         | `0`              | Time-of-day window start minute (0–59)                            |
| `end_hour`       | int         | `18`             | Time-of-day window end hour (0–23)                                |
| `end_minute`     | int         | `0`              | Time-of-day window end minute (0–59)                              |
| `approach_type`  | str         | `"permissive"`   | One of `permissive`, `permissive_protected`, `protected`          |

**API Endpoint:** `POST /api/v1/reports/left-turn-volume`

**Use in the Left Turn Gap workflow:**

The Left Turn Volume report feeds the cross-product and decision-boundary fields consumed by the Left Turn Gap study. Run this report first across a corridor: any approach where `cross_product_review` or `decision_boundaries_review` is `true` is a candidate for the full gap analysis. The Gap study then quantifies whether the observed opposing-through gaps actually support the permissive left-turn demand, closing the loop from "this intersection looks like it needs a phase study" to "here is the evidence for or against adding a protected phase."

---

## Arrivals on Green (AOG)

### Classification Logic

Vehicles are classified by signal state at arrival:

| Arrival Type | Condition |
|--------------|-----------|
| **Green** | Detector ON while phase green (after Event 1, before Event 8) |
| **Yellow** | Detector ON during yellow (after Event 8, before Event 10) |
| **Red** | Detector ON during red (after Event 10, before Event 1) |

**Algorithm:**
```sql
WITH signal_state AS (
    SELECT
        event_time,
        event_code,
        LEAD(event_time) OVER (PARTITION BY event_param ORDER BY event_time) as next_change
    FROM controller_event_log
    WHERE event_code IN (1, 8, 10)  -- Green, Yellow, Red
),
arrivals AS (
    SELECT
        event_time as arrival_time,
        signal_id, event_param
    FROM controller_event_log
    WHERE event_code = 82  -- Detector On
)
SELECT
    arrival_time,
    CASE
        WHEN s.event_code = 1 THEN 'green'
        WHEN s.event_code = 8 THEN 'yellow'
        WHEN s.event_code = 10 THEN 'red'
    END as arrival_type
FROM arrivals a
JOIN signal_state s ON
    a.arrival_time >= s.event_time
    AND a.arrival_time < s.next_change
```

**Metrics:**
| Metric | Description |
|--------|-------------|
| `arrivals_on_green` | Count of green arrivals |
| `arrivals_on_red` | Count of red arrivals |
| `arrivals_on_yellow` | Count of yellow arrivals |
| `pct_arrivals_on_green` | AOG percentage |

**Formula:**
```
% AOG = (Arrivals on Green / Total Arrivals) × 100
```

**Targets:**
| % AOG | Assessment |
|-------|------------|
| > 70% | Excellent coordination |
| 50-70% | Acceptable |
| < 50% | Poor coordination, needs attention |

---

### Arrival on Red

Quantifies what fraction of vehicle arrivals hit a red indication — the complement of Arrival on Green and a direct indicator of coordination loss.

**Definition:** A detector activation is classified as **arrival-on-red** when the target phase is not currently green at the event timestamp. Phase state is tracked per-phase across the analysis window using phase green, yellow-clearance, and red-clearance events. Detector channels are mapped to their approach's protected phase (or permissive phase when `include_permissive` is true and no protected phase exists).

**Algorithm:**
```
# Single-pass walk of merged phase + detector events, ordered by time.
phase_is_green[p] = False for each target phase p

for event in events:
    if event.code == PHASE_GREEN       and event.param in target_phases:
        phase_is_green[event.param] = True
    elif event.code == YELLOW_CLEARANCE and event.param in target_phases:
        phase_is_green[event.param] = False
    elif event.code == RED_CLEARANCE    and event.param in target_phases:
        phase_is_green[event.param] = False
    elif event.code == DETECTOR_ON:
        phase = channel_to_phase[event.param]
        is_aor = not phase_is_green[phase]
        emit(event.time, phase, is_aor)

# Bin by floor(event.time, bin_size_minutes), group by (phase, bin_start):
#   total_detections      = count(*)
#   arrivals_on_red       = sum(is_aor)
#   pct_arrivals_on_red   = 100 * arrivals_on_red / total_detections
#   total_vehicles_per_hour   = total_detections * (60 / bin_size_minutes)
#   arrivals_on_red_per_hour  = arrivals_on_red  * (60 / bin_size_minutes)
```

**Output columns** (one row per `(phase_number, bin_start)`):

| Column | Type | Description |
|--------|------|-------------|
| `bin_start` | datetime | Bin floor timestamp |
| `phase_number` | int | Target phase |
| `total_detections` | int | Detector activations in the bin on this phase |
| `arrivals_on_red` | int | Subset that arrived while the phase was not green |
| `pct_arrivals_on_red` | float | `arrivals_on_red / total_detections * 100`, rounded to 1 decimal (0.0 when denominator is 0) |
| `total_vehicles_per_hour` | float | `total_detections` scaled to hourly rate |
| `arrivals_on_red_per_hour` | float | `arrivals_on_red` scaled to hourly rate |
| `total_detector_hits` | int | Broadcast: sum of `total_detections` across all rows |
| `total_arrival_on_red` | int | Broadcast: sum of `arrivals_on_red` across all rows |
| `pct_arrival_on_red_overall` | float | Broadcast: overall percentage across the window |

The last three columns are constant on every row (broadcast summary) — same pattern as `dq_*` / `ped_*` / `peak_*` columns elsewhere in the SDK.

**Params:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `signal_id` | string | *required* | Signal identifier |
| `start` | string (ISO-8601) | *required* | Analysis window start |
| `end` | string (ISO-8601) | *required* | Analysis window end |
| `bin_size_minutes` | int | `15` | Width of each aggregation bin |
| `include_permissive` | bool | `false` | Fall back to the approach's permissive phase when no protected phase is configured |

**API Endpoint:** `POST /api/v1/reports/arrival-on-red` (executed via the generic Reports API — TSIGMA reports are not exposed as per-report analytics routes).

**Contrast with Arrival on Green:** Use **Arrivals on Green** when you want a single coordination-quality score per phase (one row per phase, no time binning). Use **Arrival on Red** when you need the time-resolved counterpart — per-phase, per-bin arrival counts and rates suitable for plotting coordination degradation over the day or correlating with demand.

---

## Health Scores

### Detector Health Score (0-100)

Composite score based on multiple factors:

| Factor | Weight | Penalty Conditions |
|--------|--------|-------------------|
| Stuck Detection | -40 | Stuck ON or OFF |
| Chatter | -20 | Rapid cycling > 10/min |
| High Variance | -15 | Gap variance > 3× average |
| Low Activity | -20 | < 5% expected activations |
| On/Off Balance | -10 | On time > 90% or < 10% |

**Grades:**
| Score | Grade |
|-------|-------|
| 90-100 | Excellent |
| 70-89 | Good |
| 50-69 | Fair |
| 30-49 | Poor |
| 0-29 | Critical |

**API Endpoint:** `GET /api/v1/analytics/health/detector`

---

### Signal Health Score (0-100)

Weighted composite of subsystem health:

| Component | Weight |
|-----------|--------|
| Detector Health | 35% |
| Phase Health | 25% |
| Coordination Health | 20% |
| Communication Health | 20% |

**Phase Health Factors:**
- Skipped phase count
- Underutilized phases
- Split failures

**Coordination Health Factors:**
- Offset drift
- Cycle variance
- Pattern stability

**Communication Health Factors:**
- Data freshness (time since last event)
- Missing data gaps

**API Endpoint:** `GET /api/v1/analytics/health/signal`

---

## Cycle Detection

### Red-to-Red Method (Standard)

A signal cycle is measured from Phase Begin Red (Event 10) to the next Phase Begin Red for the coordinated phase.

```sql
SELECT
    event_param as phase,
    event_time as cycle_start,
    LEAD(event_time) OVER (PARTITION BY event_param ORDER BY event_time) as cycle_end,
    LEAD(event_time) OVER (...) - event_time as cycle_length
FROM controller_event_log
WHERE event_code = 10  -- Phase Begin Red
  AND event_param = :coordinated_phase
```

### Green-to-Green Method (Alternative)

For some analyses, cycles are measured from Phase On (Event 1) to Phase On.

---

## Aggregation Strategy

TSIGMA uses a multi-level aggregation approach:

### Raw Events
- Full resolution (0.1 second)
- Retained in hot storage (PostgreSQL) for 3-4 weeks
- Used for detailed analysis and debugging

### Pre-Aggregated Tables
| Table | Time Bucket | Retention |
|-------|-------------|-----------|
| `approach_volume_*` | 15 minutes | 1 year |
| `phase_termination_*` | 15 minutes | 1 year |
| `split_failure_*` | 15 minutes | 1 year |
| `arrivals_on_green_*` | 15 minutes | 1 year |

### Rollup Aggregations
| Level | From | Aggregation |
|-------|------|-------------|
| Hourly | 15-min bins | SUM/AVG |
| Daily | Hourly | SUM/AVG |
| Weekly | Daily | SUM/AVG |

### Weighted Mean Rollups

For percentage metrics, use weighted averages:

```sql
-- Weighted % AOG rollup
SELECT
    date_trunc('hour', bin) as hour_bin,
    SUM(arrivals_on_green) / NULLIF(SUM(total_arrivals), 0) * 100 as pct_aog
FROM arrivals_on_green_15min
GROUP BY date_trunc('hour', bin)
```

---

## API Reference

All metrics are available via REST API:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/analytics/detectors/stuck` | GET | Find stuck detectors |
| `/api/v1/analytics/detectors/gaps` | GET | Gap analysis |
| `/api/v1/analytics/detectors/occupancy` | GET | Occupancy by time bin |
| `/api/v1/analytics/phases/split-monitor` | GET | Split timing |
| `/api/v1/analytics/phases/split-failures` | GET | Split failure detection (Available via the Reports API at `/api/v1/reports/split-failure`, not as an analytics endpoint) |
| `/api/v1/analytics/phases/skipped` | GET | Skipped phases |
| `/api/v1/analytics/phases/terminations` | GET | Termination summary |
| `/api/v1/analytics/coordination/offset-drift` | GET | Cycle variance |
| `/api/v1/analytics/coordination/patterns` | GET | Pattern history |
| `/api/v1/analytics/coordination/quality` | GET | Coordination quality |
| `/api/v1/analytics/preemptions/summary` | GET | Preemption analysis |
| `/api/v1/analytics/preemptions/recovery` | GET | Recovery times |
| `/api/v1/analytics/health/detector` | GET | Detector health score |
| `/api/v1/analytics/health/signal` | GET | Signal health score |

See [API_REFERENCE.md](API_REFERENCE.md) for full endpoint documentation.

---

## References

- NTCIP 1202 v3 - Object Definitions for Actuated Traffic Signal Controller Units
- FHWA Signal Timing Manual
- Indiana Traffic Signal Hi-Resolution Data Logger Enumerations
- ATSPM Metric Calculation Documentation
