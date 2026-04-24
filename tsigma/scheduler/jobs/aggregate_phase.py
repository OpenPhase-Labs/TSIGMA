"""
Phase-scoped aggregation jobs for non-TimescaleDB databases.

Companion module to ``tsigma.scheduler.jobs.aggregate`` (the "first batch"
of aggregations).  Kept in a separate file so neither module crosses the
1000-line hard cap.

Every job here follows the identical shape used by ``aggregate.py``:

  1. ``_should_skip(session)`` — short-circuit when aggregation is
     disabled, or when TimescaleDB continuous aggregates are active and
     handling the refresh natively.
  2. Build dialect-neutral INSERT SQL via ``db_facade`` helpers
     (``time_bucket``, ``lookback_predicate``, ``delete_window_sql``).
  3. Delegate the delete-and-reinsert to ``_refresh_aggregate``.

Jobs provided here (all on a 15-minute cron):

  - ``agg_approach_speed``        -> ``approach_speed_15min``
  - ``agg_phase_cycle``           -> ``phase_cycle_15min``
  - ``agg_phase_left_turn_gap``   -> ``phase_left_turn_gap_15min``
  - ``agg_phase_pedestrian``      -> ``phase_pedestrian_15min``
  - ``agg_priority``              -> ``priority_15min``
  - ``agg_yellow_red_activation`` -> ``yellow_red_activation_15min``

Event-code assumptions (Indiana Hi-Res / Purdue 2012):

  - Approach speed is derived from detector-ON (code 82) on detectors
    with a configured speed filter.  ``event_param`` on code 82 carries
    the observed speed bin in controllers that support Hi-Res speed
    logging; on controllers that do not, the percentile columns reduce
    to 0 but ``sample_count`` still tracks activations.  This assumption
    is documented in ``docs/developers/MULTI_DATABASE_AGGREGATES.md``.
  - Left-turn gap bins are approximate — the precise per-cycle gap
    computation requires window functions that are not portable across
    all four dialects; the scheduler fallback counts gap-out terminations
    (event 4) into the 10+s bin and leaves the shorter bins at zero for
    dialects without ``LAG()`` support.  Downstream report plugins compute
    the exact distribution when needed.
  - Preemption + TSP codes are drawn from ``tsigma.reports.sdk.events``.
"""

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.database.db import db_facade
from tsigma.models.roadside_event import ROADSIDE_EVENT_TYPE_SPEED
from tsigma.reports.sdk.events import (
    EVENT_DETECTOR_ON,
    EVENT_PED_CALL,
    EVENT_PED_WALK,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_TSP_CHECK_IN,
    EVENT_TSP_CHECK_OUT,
    EVENT_TSP_EARLY_GREEN,
    EVENT_TSP_EXTEND_GREEN,
    EVENT_YELLOW_CLEARANCE,
)
from tsigma.scheduler.jobs.aggregate import _refresh_aggregate, _should_skip
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fifteen_minute_bucket(col: str = "event_time") -> str:
    """Return the dialect-specific 15-minute time_bucket expression.

    ``col`` defaults to ``event_time`` but can be any timestamp column
    (used when bucketing by a window's ``interval_start`` rather than
    the raw event time).
    """
    if db_facade.db_type == "postgresql":
        return f"time_bucket('15 minutes', {col})"
    if db_facade.db_type == "mssql":
        return f"DATEADD(minute, (DATEDIFF(minute, 0, {col}) / 15) * 15, 0)"
    if db_facade.db_type == "oracle":
        return (
            f"TRUNC({col}, 'MI') - NUMTODSINTERVAL("
            f"MOD(EXTRACT(MINUTE FROM {col}), 15), 'MINUTE')"
        )
    # MySQL: 15-minute floor via DATE_FORMAT + modulo math.
    return (
        f"STR_TO_DATE(CONCAT(DATE_FORMAT({col}, '%Y-%m-%d %H:'), "
        f"LPAD(FLOOR(MINUTE({col})/15)*15, 2, '0'), ':00'), "
        f"'%Y-%m-%d %H:%i:%s')"
    )


def _seconds_diff(earlier: str, later: str) -> str:
    """Return a dialect-neutral SQL expression for ``(later - earlier)`` in seconds.

    Used by every aggregation that has to measure a duration (phase state
    durations, inter-arrival gaps, preempt + ped delays, red-interval
    totals).  The expression is always a non-negative numeric when
    ``later >= earlier``.
    """
    if db_facade.db_type == "postgresql":
        return f"EXTRACT(EPOCH FROM ({later} - {earlier}))"
    if db_facade.db_type == "mssql":
        # DATEDIFF(SECOND,...) is an INT so it wraps at ~68 years; for
        # intra-cycle durations that range is never approached.
        return f"DATEDIFF(SECOND, {earlier}, {later})"
    if db_facade.db_type == "oracle":
        # In Oracle, subtracting DATE/TIMESTAMP yields a number of days
        # (DATE) or an INTERVAL DAY TO SECOND (TIMESTAMP).  EXTRACT(SECOND
        # FROM) on an interval ignores higher fields, so compute via
        # explicit interval-to-seconds conversion.
        return (
            f"(EXTRACT(DAY FROM ({later} - {earlier})) * 86400 + "
            f"EXTRACT(HOUR FROM ({later} - {earlier})) * 3600 + "
            f"EXTRACT(MINUTE FROM ({later} - {earlier})) * 60 + "
            f"EXTRACT(SECOND FROM ({later} - {earlier})))"
        )
    # MySQL.
    return f"TIMESTAMPDIFF(SECOND, {earlier}, {later})"


# ---------------------------------------------------------------------------
# 1. Approach speed (15-min percentiles)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_approach_speed", trigger="cron", minute="*/15")
async def agg_approach_speed(session: AsyncSession) -> None:
    """Aggregate 15-minute approach speed percentiles from roadside sensors.

    Source:  ``roadside_event`` rows with ``event_type = SPEED`` (1) and
    a non-null ``mph``.  Per-vehicle detections emitted by roadside
    radar / LiDAR / video sensors (Wavetronix, Iteris, FLIR, Houston
    Radar, Quanergy, etc.) — not from the cabinet controller, which
    does not emit an mph value in any Indiana Hi-Res code TSIGMA
    currently decodes.

    Join path:  ``roadside_event.sensor_id + lane_number`` →
    ``roadside_sensor_lane`` → ``approach_id``.  A sensor that serves
    three lanes of an approach contributes three streams of detections
    which are pooled at the approach level before the percentile
    computation — so p15 / p50 / p85 are computed over the union of
    all mph samples hitting that approach in the 15-minute window.

    Percentile implementation varies per dialect:

      - PostgreSQL / Oracle — native ``PERCENTILE_CONT`` aggregate with
        ``WITHIN GROUP (ORDER BY mph)``.
      - MS-SQL — ``PERCENTILE_CONT`` is window-only; wrapped in
        ``SELECT DISTINCT`` over the partition.
      - MySQL 8 — no native ``PERCENTILE_CONT``; nearest-rank
        emulation via ``ROW_NUMBER() / COUNT() OVER`` inside a
        derived table.  Nearest-rank differs from linear interpolation
        at sub-sample resolution; for traffic speed distributions the
        bias is well inside measurement noise.

    If a second speed source ever comes online (e.g. a future decoder
    that surfaces an mph-bearing Indiana Hi-Res event), add a UNION ALL
    branch on the source rather than replacing this one.
    """
    if await _should_skip(session):
        return

    from tsigma.config import settings
    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)
    bucket = _fifteen_minute_bucket()
    sql = _approach_speed_insert_sql(
        db_type=db_facade.db_type,
        predicate=predicate,
        bucket=bucket,
    )

    await _refresh_aggregate(
        session,
        table="approach_speed_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed approach_speed_15min")


def _approach_speed_insert_sql(
    *, db_type: str, predicate: str, bucket: str,
) -> str:
    """Return the dialect-specific INSERT SQL for agg_approach_speed.

    Split out of ``agg_approach_speed`` so each dialect's syntax stays
    readable and the job function stays under the 50-line soft limit.
    """
    event_type = ROADSIDE_EVENT_TYPE_SPEED

    if db_type == "postgresql":
        return f"""
            INSERT INTO approach_speed_15min
                (signal_id, approach_id, bin_start, p15, p50, p85, sample_count)
            SELECT
                re.signal_id,
                CAST(rsl.approach_id AS TEXT) AS approach_id,
                {bucket} AS bin_start,
                COALESCE(PERCENTILE_CONT(0.15)
                    WITHIN GROUP (ORDER BY re.mph), 0) AS p15,
                COALESCE(PERCENTILE_CONT(0.50)
                    WITHIN GROUP (ORDER BY re.mph), 0) AS p50,
                COALESCE(PERCENTILE_CONT(0.85)
                    WITHIN GROUP (ORDER BY re.mph), 0) AS p85,
                COUNT(*) AS sample_count
            FROM roadside_event re
            JOIN roadside_sensor_lane rsl
              ON rsl.sensor_id = re.sensor_id
             AND rsl.lane_number = re.lane_number
            WHERE re.event_type = {event_type}
              AND re.mph IS NOT NULL
              AND {predicate}
            GROUP BY re.signal_id, rsl.approach_id, {bucket}
        """

    if db_type == "oracle":
        return f"""
            INSERT INTO approach_speed_15min
                (signal_id, approach_id, bin_start, p15, p50, p85, sample_count)
            SELECT
                re.signal_id,
                CAST(rsl.approach_id AS VARCHAR2(36)) AS approach_id,
                {bucket} AS bin_start,
                NVL(PERCENTILE_CONT(0.15)
                    WITHIN GROUP (ORDER BY re.mph), 0) AS p15,
                NVL(PERCENTILE_CONT(0.50)
                    WITHIN GROUP (ORDER BY re.mph), 0) AS p50,
                NVL(PERCENTILE_CONT(0.85)
                    WITHIN GROUP (ORDER BY re.mph), 0) AS p85,
                COUNT(*) AS sample_count
            FROM roadside_event re
            JOIN roadside_sensor_lane rsl
              ON rsl.sensor_id = re.sensor_id
             AND rsl.lane_number = re.lane_number
            WHERE re.event_type = {event_type}
              AND re.mph IS NOT NULL
              AND {predicate}
            GROUP BY re.signal_id, rsl.approach_id, {bucket}
        """

    if db_type == "mssql":
        # MS-SQL has PERCENTILE_CONT only as a window function; one row
        # per (signal_id, approach_id, bucket) falls out via DISTINCT
        # over the partition.
        partition = f"PARTITION BY re.signal_id, rsl.approach_id, {bucket}"
        return f"""
            INSERT INTO approach_speed_15min
                (signal_id, approach_id, bin_start, p15, p50, p85, sample_count)
            SELECT DISTINCT
                re.signal_id,
                CAST(rsl.approach_id AS NVARCHAR(36)) AS approach_id,
                {bucket} AS bin_start,
                ISNULL(PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY re.mph)
                    OVER ({partition}), 0) AS p15,
                ISNULL(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY re.mph)
                    OVER ({partition}), 0) AS p50,
                ISNULL(PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY re.mph)
                    OVER ({partition}), 0) AS p85,
                COUNT(*) OVER ({partition}) AS sample_count
            FROM roadside_event re
            JOIN roadside_sensor_lane rsl
              ON rsl.sensor_id = re.sensor_id
             AND rsl.lane_number = re.lane_number
            WHERE re.event_type = {event_type}
              AND re.mph IS NOT NULL
              AND {predicate}
        """

    if db_type == "mysql":
        # No PERCENTILE_CONT in MySQL 8 — nearest-rank via row indexes.
        # For a window of N samples ordered by mph ascending, the
        # q-percentile is the sample at rank CEIL(N * q) (clamped to 1).
        partition = f"PARTITION BY re.signal_id, rsl.approach_id, {bucket}"
        return f"""
            INSERT INTO approach_speed_15min
                (signal_id, approach_id, bin_start, p15, p50, p85, sample_count)
            SELECT
                signal_id, approach_id, bin_start,
                COALESCE(MAX(CASE WHEN rn =
                    GREATEST(1, CAST(CEIL(cnt * 0.15) AS UNSIGNED))
                    THEN mph END), 0) AS p15,
                COALESCE(MAX(CASE WHEN rn =
                    GREATEST(1, CAST(CEIL(cnt * 0.50) AS UNSIGNED))
                    THEN mph END), 0) AS p50,
                COALESCE(MAX(CASE WHEN rn =
                    GREATEST(1, CAST(CEIL(cnt * 0.85) AS UNSIGNED))
                    THEN mph END), 0) AS p85,
                cnt AS sample_count
            FROM (
                SELECT
                    re.signal_id,
                    CAST(rsl.approach_id AS CHAR) AS approach_id,
                    {bucket} AS bin_start,
                    re.mph,
                    ROW_NUMBER() OVER ({partition} ORDER BY re.mph) AS rn,
                    COUNT(*) OVER ({partition}) AS cnt
                FROM roadside_event re
                JOIN roadside_sensor_lane rsl
                  ON rsl.sensor_id = re.sensor_id
                 AND rsl.lane_number = re.lane_number
                WHERE re.event_type = {event_type}
                  AND re.mph IS NOT NULL
                  AND {predicate}
            ) ordered
            GROUP BY signal_id, approach_id, bin_start, cnt
        """

    raise ValueError(f"Unsupported db_type for agg_approach_speed: {db_type!r}")


# ---------------------------------------------------------------------------
# 2. Phase cycle (green/yellow/red time, cycle count)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_phase_cycle", trigger="cron", minute="*/15")
async def agg_phase_cycle(session: AsyncSession) -> None:
    """Aggregate 15-minute green/yellow/red seconds and cycle count.

    Durations are computed per-phase with ``LEAD()`` over the ordered
    state-transition events (green-start / yellow-start / red-start).
    Each transition event's duration is the time until the NEXT
    transition on that same ``(signal_id, phase)`` — i.e. how long the
    phase spent in that colour before flipping.  Windows are assigned
    to the 15-minute bucket covering the transition's start time.
    ``cycle_count`` counts green-starts (= cycle-begin events).
    """
    if await _should_skip(session):
        return

    from tsigma.config import settings
    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)
    bucket = _fifteen_minute_bucket()
    duration = _seconds_diff("event_time", "next_time")

    sql = f"""
        WITH state_events AS (
            SELECT
                signal_id,
                event_param AS phase,
                event_code,
                event_time,
                LEAD(event_time) OVER (
                    PARTITION BY signal_id, event_param
                    ORDER BY event_time
                ) AS next_time
            FROM controller_event_log
            WHERE event_code IN (
                {EVENT_PHASE_GREEN}, {EVENT_YELLOW_CLEARANCE}, {EVENT_RED_CLEARANCE}
            )
              AND {predicate}
        )
        INSERT INTO phase_cycle_15min
            (signal_id, phase, bin_start, green_seconds, yellow_seconds,
             red_seconds, cycle_count)
        SELECT
            signal_id,
            phase,
            {bucket} AS bin_start,
            SUM(CASE WHEN event_code = {EVENT_PHASE_GREEN}
                     THEN {duration} ELSE 0 END) AS green_seconds,
            SUM(CASE WHEN event_code = {EVENT_YELLOW_CLEARANCE}
                     THEN {duration} ELSE 0 END) AS yellow_seconds,
            SUM(CASE WHEN event_code = {EVENT_RED_CLEARANCE}
                     THEN {duration} ELSE 0 END) AS red_seconds,
            SUM(CASE WHEN event_code = {EVENT_PHASE_GREEN}
                     THEN 1 ELSE 0 END) AS cycle_count
        FROM state_events
        WHERE next_time IS NOT NULL
        GROUP BY signal_id, phase, {bucket}
    """

    await _refresh_aggregate(
        session,
        table="phase_cycle_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed phase_cycle_15min")


# ---------------------------------------------------------------------------
# 3. Phase left-turn gap (11 bins)
# ---------------------------------------------------------------------------


_LT_GAP_BIN_COLUMNS = (
    "bin_1s, bin_2s, bin_3s, bin_4s, bin_5s, "
    "bin_6s, bin_7s, bin_8s, bin_9s, bin_10s, bin_10plus"
)


def _lt_gap_bin_case(gap_expr: str) -> str:
    """Return the 11 ``SUM(CASE ...)`` expressions classifying gap values.

    Each bin is a half-open range ``[N-1, N)`` seconds; ``bin_10plus``
    captures everything ≥ 10 s.  Called with the dialect-appropriate
    seconds expression for the gap.
    """
    parts = []
    for n in range(1, 11):
        lower = n - 1
        upper = n
        parts.append(
            f"SUM(CASE WHEN {gap_expr} >= {lower} AND {gap_expr} < {upper} "
            f"THEN 1 ELSE 0 END) AS bin_{n}s"
        )
    parts.append(
        f"SUM(CASE WHEN {gap_expr} >= 10 THEN 1 ELSE 0 END) AS bin_10plus"
    )
    return ", ".join(parts)


@JobRegistry.register(
    name="agg_phase_left_turn_gap", trigger="cron", minute="*/15",
)
async def agg_phase_left_turn_gap(session: AsyncSession) -> None:
    """Aggregate 15-minute left-turn detector inter-arrival gap distribution.

    Gap = time between consecutive ``EVENT_DETECTOR_ON`` events on the
    same left-turn detector channel.  Left-turn detectors are identified
    via the ``movement_type`` reference table (abbreviation ``'L'``).
    Gaps are classified into 11 bins (``[0,1)`` .. ``[9,10)`` seconds
    and ``>= 10`` seconds) and counted per phase per 15-minute bin.
    """
    if await _should_skip(session):
        return

    from tsigma.config import settings
    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("cel.event_time", hours)
    bucket = _fifteen_minute_bucket("cel.event_time")
    gap_expr = _seconds_diff("prev_time", "cel.event_time")
    bin_clauses = _lt_gap_bin_case("gap_s")

    sql = f"""
        WITH lt_detector_events AS (
            SELECT
                cel.signal_id,
                a.protected_phase_number AS phase,
                cel.event_time,
                {bucket} AS bin_start,
                LAG(cel.event_time) OVER (
                    PARTITION BY cel.signal_id, cel.event_param
                    ORDER BY cel.event_time
                ) AS prev_time
            FROM controller_event_log cel
            JOIN detector d
              ON d.detector_channel = cel.event_param
            JOIN approach a
              ON a.approach_id = d.approach_id
            JOIN movement_type mt
              ON mt.movement_type_id = d.movement_type_id
            WHERE cel.event_code = {EVENT_DETECTOR_ON}
              AND mt.abbreviation = 'L'
              AND a.protected_phase_number IS NOT NULL
              AND {predicate}
        ),
        gaps AS (
            SELECT
                signal_id,
                phase,
                bin_start,
                {gap_expr} AS gap_s
            FROM lt_detector_events
            WHERE prev_time IS NOT NULL
        )
        INSERT INTO phase_left_turn_gap_15min
            (signal_id, phase, bin_start, {_LT_GAP_BIN_COLUMNS})
        SELECT signal_id, phase, bin_start, {bin_clauses}
        FROM gaps
        GROUP BY signal_id, phase, bin_start
    """

    await _refresh_aggregate(
        session,
        table="phase_left_turn_gap_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed phase_left_turn_gap_15min")


# ---------------------------------------------------------------------------
# 4. Phase pedestrian (walks, calls, delay)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_phase_pedestrian", trigger="cron", minute="*/15")
async def agg_phase_pedestrian(session: AsyncSession) -> None:
    """Aggregate 15-minute pedestrian counts + delay per phase.

    ``ped_delay_sum_seconds`` / ``ped_delay_count`` pair each
    ``EVENT_PED_CALL`` with the next ``EVENT_PED_WALK`` on the same
    ``(signal_id, phase)`` (correlated sub-query on ``MIN(walk_time)``
    after the call).  A call with no matching walk within the lookback
    contributes nothing to the delay sum/count — walk/call counts are
    unaffected.  ``mean = ped_delay_sum_seconds / ped_delay_count`` for
    bins with any paired calls.
    """
    if await _should_skip(session):
        return

    from tsigma.config import settings
    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)
    bucket = _fifteen_minute_bucket()
    delay_expr = _seconds_diff("call_time", "walk_time")

    # The ``paired`` CTE uses a correlated ``MIN()`` to find the first
    # walk at or after each call — portable across all four dialects.
    # The walk lookup is not time-bounded: a call whose walk arrives in
    # a later lookback window still gets paired.  Relies on the PK
    # ``(signal_id, event_time, event_code, event_param)`` for index
    # efficiency.
    sql = f"""
        WITH calls AS (
            SELECT signal_id, event_param AS phase, event_time AS call_time
            FROM controller_event_log
            WHERE event_code = {EVENT_PED_CALL}
              AND {predicate}
        ),
        paired AS (
            SELECT
                c.signal_id,
                c.phase,
                c.call_time,
                (
                    SELECT MIN(w.event_time)
                    FROM controller_event_log w
                    WHERE w.event_code = {EVENT_PED_WALK}
                      AND w.signal_id = c.signal_id
                      AND w.event_param = c.phase
                      AND w.event_time >= c.call_time
                ) AS walk_time
            FROM calls c
        ),
        call_delays AS (
            SELECT
                signal_id,
                phase,
                {_fifteen_minute_bucket("call_time")} AS bin_start,
                CASE WHEN walk_time IS NOT NULL
                     THEN {delay_expr} ELSE NULL END AS delay_seconds
            FROM paired
        ),
        counts AS (
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS bin_start,
                SUM(CASE WHEN event_code = {EVENT_PED_WALK}
                         THEN 1 ELSE 0 END) AS ped_walk_count,
                SUM(CASE WHEN event_code = {EVENT_PED_CALL}
                         THEN 1 ELSE 0 END) AS ped_call_count
            FROM controller_event_log
            WHERE event_code IN ({EVENT_PED_WALK}, {EVENT_PED_CALL})
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        ),
        delays AS (
            SELECT
                signal_id,
                phase,
                bin_start,
                SUM(CASE WHEN delay_seconds IS NOT NULL
                         THEN delay_seconds ELSE 0 END) AS ped_delay_sum_seconds,
                SUM(CASE WHEN delay_seconds IS NOT NULL
                         THEN 1 ELSE 0 END) AS ped_delay_count
            FROM call_delays
            GROUP BY signal_id, phase, bin_start
        )
        INSERT INTO phase_pedestrian_15min
            (signal_id, phase, bin_start, ped_walk_count, ped_call_count,
             ped_delay_sum_seconds, ped_delay_count)
        SELECT
            c.signal_id,
            c.phase,
            c.bin_start,
            c.ped_walk_count,
            c.ped_call_count,
            COALESCE(d.ped_delay_sum_seconds, 0),
            COALESCE(d.ped_delay_count, 0)
        FROM counts c
        LEFT JOIN delays d
          ON d.signal_id = c.signal_id
         AND d.phase = c.phase
         AND d.bin_start = c.bin_start
    """

    await _refresh_aggregate(
        session,
        table="phase_pedestrian_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed phase_pedestrian_15min")


# ---------------------------------------------------------------------------
# 5. Priority (TSP early/extended green + check-in/out)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_priority", trigger="cron", minute="*/15")
async def agg_priority(session: AsyncSession) -> None:
    """Aggregate 15-minute TSP counts per phase."""
    if await _should_skip(session):
        return

    from tsigma.config import settings
    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)
    bucket = _fifteen_minute_bucket()

    codes = (
        f"{EVENT_TSP_CHECK_IN}, {EVENT_TSP_EARLY_GREEN}, "
        f"{EVENT_TSP_EXTEND_GREEN}, {EVENT_TSP_CHECK_OUT}"
    )

    if db_facade.db_type == "postgresql":
        sql = f"""
            INSERT INTO priority_15min
                (signal_id, phase, bin_start, early_green_count,
                 extended_green_count, check_in_count, check_out_count)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS bin_start,
                COUNT(*) FILTER (WHERE event_code = {EVENT_TSP_EARLY_GREEN})
                    AS early_green_count,
                COUNT(*) FILTER (WHERE event_code = {EVENT_TSP_EXTEND_GREEN})
                    AS extended_green_count,
                COUNT(*) FILTER (WHERE event_code = {EVENT_TSP_CHECK_IN})
                    AS check_in_count,
                COUNT(*) FILTER (WHERE event_code = {EVENT_TSP_CHECK_OUT})
                    AS check_out_count
            FROM controller_event_log
            WHERE event_code IN ({codes})
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """
    else:
        sql = f"""
            INSERT INTO priority_15min
                (signal_id, phase, bin_start, early_green_count,
                 extended_green_count, check_in_count, check_out_count)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS bin_start,
                SUM(CASE WHEN event_code = {EVENT_TSP_EARLY_GREEN}
                         THEN 1 ELSE 0 END) AS early_green_count,
                SUM(CASE WHEN event_code = {EVENT_TSP_EXTEND_GREEN}
                         THEN 1 ELSE 0 END) AS extended_green_count,
                SUM(CASE WHEN event_code = {EVENT_TSP_CHECK_IN}
                         THEN 1 ELSE 0 END) AS check_in_count,
                SUM(CASE WHEN event_code = {EVENT_TSP_CHECK_OUT}
                         THEN 1 ELSE 0 END) AS check_out_count
            FROM controller_event_log
            WHERE event_code IN ({codes})
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """

    await _refresh_aggregate(
        session,
        table="priority_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed priority_15min")


# ---------------------------------------------------------------------------
# 6. Yellow/Red activation (detector ON during yellow/red interval)
# ---------------------------------------------------------------------------


@JobRegistry.register(
    name="agg_yellow_red_activation", trigger="cron", minute="*/15",
)
async def agg_yellow_red_activation(session: AsyncSession) -> None:
    """Aggregate 15-minute detector activations *during* yellow/red intervals.

    Builds yellow and red intervals per ``(signal_id, phase)`` via
    ``LEAD()`` on phase-state events (green-start / yellow-start /
    red-start).  Each interval runs from its start event until the next
    phase-state event on the same phase.  Detector-ON events (code 82)
    whose timestamps fall inside a yellow interval count as yellow
    activations; those inside a red interval count as red activations
    and contribute the interval's duration to ``red_duration_sum_seconds``.

    Note: the detector-ON's ``event_param`` is the detector channel;
    the interval's ``phase`` is matched by the signal alone (a detector
    on any approach that activates during phase N's red counts for
    phase N).  Consumers requiring approach-scoped counts should filter
    the underlying ``cycle_detector_arrival`` table instead.
    """
    if await _should_skip(session):
        return

    from tsigma.config import settings
    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)
    bucket = _fifteen_minute_bucket("pi.interval_start")
    duration = _seconds_diff("pi.interval_start", "pi.interval_end")

    sql = f"""
        WITH phase_intervals AS (
            SELECT
                signal_id,
                event_param AS phase,
                event_code,
                event_time AS interval_start,
                LEAD(event_time) OVER (
                    PARTITION BY signal_id, event_param
                    ORDER BY event_time
                ) AS interval_end
            FROM controller_event_log
            WHERE event_code IN (
                {EVENT_PHASE_GREEN}, {EVENT_YELLOW_CLEARANCE},
                {EVENT_RED_CLEARANCE}
            )
              AND {predicate}
        ),
        yr_intervals AS (
            SELECT
                signal_id, phase, event_code, interval_start, interval_end,
                {bucket} AS bin_start,
                {duration} AS duration_s
            FROM phase_intervals pi
            WHERE interval_end IS NOT NULL
              AND event_code IN ({EVENT_YELLOW_CLEARANCE}, {EVENT_RED_CLEARANCE})
        ),
        hits_per_interval AS (
            SELECT
                iv.signal_id,
                iv.phase,
                iv.event_code,
                iv.bin_start,
                iv.duration_s,
                (
                    SELECT COUNT(*)
                    FROM controller_event_log h
                    WHERE h.event_code = {EVENT_DETECTOR_ON}
                      AND h.signal_id = iv.signal_id
                      AND h.event_time >= iv.interval_start
                      AND h.event_time < iv.interval_end
                ) AS hit_count
            FROM yr_intervals iv
        )
        INSERT INTO yellow_red_activation_15min
            (signal_id, phase, bin_start, yellow_activation_count,
             red_activation_count, red_duration_sum_seconds)
        SELECT
            signal_id,
            phase,
            bin_start,
            SUM(CASE WHEN event_code = {EVENT_YELLOW_CLEARANCE}
                     THEN hit_count ELSE 0 END) AS yellow_activation_count,
            SUM(CASE WHEN event_code = {EVENT_RED_CLEARANCE}
                     THEN hit_count ELSE 0 END) AS red_activation_count,
            SUM(CASE WHEN event_code = {EVENT_RED_CLEARANCE}
                     THEN duration_s ELSE 0 END) AS red_duration_sum_seconds
        FROM hits_per_interval
        GROUP BY signal_id, phase, bin_start
    """

    await _refresh_aggregate(
        session,
        table="yellow_red_activation_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed yellow_red_activation_15min")
