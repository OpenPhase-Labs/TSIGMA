"""
Cycle aggregate jobs for PCD pre-computation.

Populates cycle_boundary, cycle_detector_arrival, and cycle_summary_15min
tables from raw ControllerEventLog events. Uses the same _should_skip()
pattern as the other aggregate jobs — if TimescaleDB continuous aggregates
are active, these jobs disable themselves.

Event codes (Indiana Hi-Res / Purdue 2012):
  1  = Phase Begin Green (cycle start)
  4  = Phase Gap Out
  5  = Phase Max Out
  6  = Phase Force Off
  8  = Phase Begin Yellow Clearance
  9  = Phase End Yellow Clearance
  10 = Phase Begin Red Clearance (used here as cycle-end marker)
  82 = Detector ON (arrival)
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.database.db import db_facade
from tsigma.scheduler.jobs.aggregate import _should_skip
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Cycle Boundary — one row per phase cycle
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_cycle_boundary", trigger="cron", minute="*/15")
async def agg_cycle_boundary(session: AsyncSession) -> None:
    """
    Compute cycle boundaries from raw phase events.

    Uses window functions to pair each Phase Begin Green (code 1) with
    the next Begin Yellow (8), End Yellow (9), Begin Red Clearance (10),
    and termination type (4=gap-out, 5=max-out, 6=force-off) within the
    same signal/phase.
    """
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)
    delete_sql = db_facade.delete_window_sql(
        "cycle_boundary", "green_start", hours
    )

    # PostgreSQL uses FILTER (WHERE ...) and EXTRACT(EPOCH FROM ...)
    if db_facade.db_type == "postgresql":
        insert_sql = f"""
            INSERT INTO cycle_boundary
                (signal_id, phase, green_start, yellow_start, red_start,
                 cycle_end, green_duration_seconds, yellow_duration_seconds,
                 red_duration_seconds, cycle_duration_seconds, termination_type)
            SELECT
                signal_id,
                event_param AS phase,
                event_time AS green_start,
                MIN(event_time) FILTER (WHERE event_code = 8) OVER w AS yellow_start,
                MIN(event_time) FILTER (WHERE event_code = 9) OVER w AS red_start,
                MIN(event_time) FILTER (WHERE event_code = 10) OVER w AS cycle_end,
                EXTRACT(EPOCH FROM
                    MIN(event_time) FILTER (WHERE event_code = 8) OVER w - event_time
                ) AS green_duration_seconds,
                EXTRACT(EPOCH FROM
                    MIN(event_time) FILTER (WHERE event_code = 9) OVER w -
                    MIN(event_time) FILTER (WHERE event_code = 8) OVER w
                ) AS yellow_duration_seconds,
                EXTRACT(EPOCH FROM
                    MIN(event_time) FILTER (WHERE event_code = 10) OVER w -
                    MIN(event_time) FILTER (WHERE event_code = 9) OVER w
                ) AS red_duration_seconds,
                EXTRACT(EPOCH FROM
                    MIN(event_time) FILTER (WHERE event_code = 10) OVER w - event_time
                ) AS cycle_duration_seconds,
                CASE
                    WHEN MIN(event_time) FILTER (WHERE event_code = 4) OVER w IS NOT NULL
                        THEN 'gap_out'
                    WHEN MIN(event_time) FILTER (WHERE event_code = 5) OVER w IS NOT NULL
                        THEN 'max_out'
                    WHEN MIN(event_time) FILTER (WHERE event_code = 6) OVER w IS NOT NULL
                        THEN 'force_off'
                    ELSE NULL
                END AS termination_type
            FROM controller_event_log
            WHERE event_code IN (1, 4, 5, 6, 8, 9, 10)
              AND {predicate}
              AND event_code = 1
            WINDOW w AS (
                PARTITION BY signal_id, event_param
                ORDER BY event_time
                ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING
            )
            ON CONFLICT (signal_id, phase, green_start) DO NOTHING
        """
    else:
        # Non-PostgreSQL: simpler approach using self-join
        insert_sql = f"""
            INSERT INTO cycle_boundary
                (signal_id, phase, green_start, yellow_start, red_start,
                 cycle_end, cycle_duration_seconds, termination_type)
            SELECT
                g.signal_id,
                g.event_param AS phase,
                g.event_time AS green_start,
                MIN(CASE WHEN n.event_code = 8 THEN n.event_time END) AS yellow_start,
                MIN(CASE WHEN n.event_code = 9 THEN n.event_time END) AS red_start,
                MIN(CASE WHEN n.event_code = 10 THEN n.event_time END) AS cycle_end,
                NULL AS cycle_duration_seconds,
                CASE
                    WHEN MIN(CASE WHEN n.event_code = 4 THEN n.event_time END) IS NOT NULL
                        THEN 'gap_out'
                    WHEN MIN(CASE WHEN n.event_code = 5 THEN n.event_time END) IS NOT NULL
                        THEN 'max_out'
                    WHEN MIN(CASE WHEN n.event_code = 6 THEN n.event_time END) IS NOT NULL
                        THEN 'force_off'
                    ELSE NULL
                END AS termination_type
            FROM controller_event_log g
            LEFT JOIN controller_event_log n
                ON n.signal_id = g.signal_id
                AND n.event_param = g.event_param
                AND n.event_time > g.event_time
                AND n.event_time < g.event_time + INTERVAL '300' SECOND
                AND n.event_code IN (4, 5, 6, 8, 9, 10)
            WHERE g.event_code = 1
              AND g.{predicate.replace('event_time', 'g.event_time')}
            GROUP BY g.signal_id, g.event_param, g.event_time
        """

    await session.execute(text(delete_sql))
    await session.execute(text(insert_sql))
    logger.info("Refreshed cycle_boundary")


# ---------------------------------------------------------------------------
# 2. Cycle Detector Arrival — one row per detector activation in a cycle
# ---------------------------------------------------------------------------


@JobRegistry.register(
    name="agg_cycle_detector_arrival", trigger="cron", minute="*/15"
)
async def agg_cycle_detector_arrival(session: AsyncSession) -> None:
    """
    Map detector activations to their containing phase cycle.

    Joins detector ON events (code 82) against cycle_boundary to
    determine which cycle each arrival belongs to and what phase
    state (green/yellow/red) was active at arrival time.
    """
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    delete_sql = db_facade.delete_window_sql(
        "cycle_detector_arrival", "arrival_time", hours
    )

    insert_sql = f"""
        INSERT INTO cycle_detector_arrival
            (signal_id, phase, detector_channel, arrival_time,
             green_start, time_in_cycle_seconds, phase_state)
        SELECT
            d.signal_id,
            cb.phase,
            d.event_param AS detector_channel,
            d.event_time AS arrival_time,
            cb.green_start,
            EXTRACT(EPOCH FROM d.event_time - cb.green_start) AS time_in_cycle_seconds,
            CASE
                WHEN d.event_time < cb.yellow_start THEN 'green'
                WHEN d.event_time < cb.red_start THEN 'yellow'
                ELSE 'red'
            END AS phase_state
        FROM controller_event_log d
        INNER JOIN cycle_boundary cb
            ON cb.signal_id = d.signal_id
            AND d.event_time >= cb.green_start
            AND d.event_time < COALESCE(cb.cycle_end, cb.green_start + INTERVAL '300 seconds')
        WHERE d.event_code = 82
          AND d.{db_facade.lookback_predicate('event_time', hours).replace('event_time', 'd.event_time')}
          AND cb.yellow_start IS NOT NULL
          AND cb.red_start IS NOT NULL
        ON CONFLICT (signal_id, phase, detector_channel, arrival_time) DO NOTHING
    """

    await session.execute(text(delete_sql))
    await session.execute(text(insert_sql))
    logger.info("Refreshed cycle_detector_arrival")


# ---------------------------------------------------------------------------
# 3. Cycle Summary 15-minute — binned arrival-on-green metrics
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_cycle_summary_15min", trigger="cron", minute="*/15")
async def agg_cycle_summary_15min(session: AsyncSession) -> None:
    """
    Aggregate cycle and arrival data into 15-minute bins.

    Computes total cycles, average cycle length, average green time,
    and arrival-on-green/yellow/red counts and percentages from
    cycle_boundary and cycle_detector_arrival tables.
    """
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    bucket = db_facade.time_bucket("cb.green_start", "15 minutes")
    delete_sql = db_facade.delete_window_sql(
        "cycle_summary_15min", "bin_start", hours
    )

    if db_facade.db_type == "postgresql":
        insert_sql = f"""
            INSERT INTO cycle_summary_15min
                (signal_id, phase, bin_start, total_cycles,
                 avg_cycle_length_seconds, avg_green_seconds,
                 total_arrivals, arrivals_on_green, arrivals_on_yellow,
                 arrivals_on_red, arrival_on_green_pct)
            SELECT
                cb.signal_id,
                cb.phase,
                {bucket} AS bin_start,
                COUNT(DISTINCT cb.green_start) AS total_cycles,
                AVG(cb.cycle_duration_seconds) AS avg_cycle_length_seconds,
                AVG(cb.green_duration_seconds) AS avg_green_seconds,
                COUNT(cda.arrival_time) AS total_arrivals,
                COUNT(cda.arrival_time) FILTER (WHERE cda.phase_state = 'green')
                    AS arrivals_on_green,
                COUNT(cda.arrival_time) FILTER (WHERE cda.phase_state = 'yellow')
                    AS arrivals_on_yellow,
                COUNT(cda.arrival_time) FILTER (WHERE cda.phase_state = 'red')
                    AS arrivals_on_red,
                CASE
                    WHEN COUNT(cda.arrival_time) > 0
                    THEN 100.0 * COUNT(cda.arrival_time)
                         FILTER (WHERE cda.phase_state = 'green')
                         / COUNT(cda.arrival_time)
                    ELSE 0.0
                END AS arrival_on_green_pct
            FROM cycle_boundary cb
            LEFT JOIN cycle_detector_arrival cda
                ON cda.signal_id = cb.signal_id
                AND cda.phase = cb.phase
                AND cda.green_start = cb.green_start
            WHERE cb.{db_facade.lookback_predicate('green_start', hours).replace('green_start', 'cb.green_start')}
            GROUP BY cb.signal_id, cb.phase, {bucket}
            ON CONFLICT (signal_id, phase, bin_start) DO UPDATE SET
                total_cycles = EXCLUDED.total_cycles,
                avg_cycle_length_seconds = EXCLUDED.avg_cycle_length_seconds,
                avg_green_seconds = EXCLUDED.avg_green_seconds,
                total_arrivals = EXCLUDED.total_arrivals,
                arrivals_on_green = EXCLUDED.arrivals_on_green,
                arrivals_on_yellow = EXCLUDED.arrivals_on_yellow,
                arrivals_on_red = EXCLUDED.arrivals_on_red,
                arrival_on_green_pct = EXCLUDED.arrival_on_green_pct
        """
    else:
        insert_sql = f"""
            INSERT INTO cycle_summary_15min
                (signal_id, phase, bin_start, total_cycles,
                 avg_cycle_length_seconds, avg_green_seconds,
                 total_arrivals, arrivals_on_green, arrivals_on_yellow,
                 arrivals_on_red, arrival_on_green_pct)
            SELECT
                cb.signal_id,
                cb.phase,
                {bucket} AS bin_start,
                COUNT(DISTINCT cb.green_start) AS total_cycles,
                AVG(cb.cycle_duration_seconds) AS avg_cycle_length_seconds,
                AVG(cb.green_duration_seconds) AS avg_green_seconds,
                COUNT(cda.arrival_time) AS total_arrivals,
                SUM(CASE WHEN cda.phase_state = 'green' THEN 1 ELSE 0 END)
                    AS arrivals_on_green,
                SUM(CASE WHEN cda.phase_state = 'yellow' THEN 1 ELSE 0 END)
                    AS arrivals_on_yellow,
                SUM(CASE WHEN cda.phase_state = 'red' THEN 1 ELSE 0 END)
                    AS arrivals_on_red,
                CASE
                    WHEN COUNT(cda.arrival_time) > 0
                    THEN 100.0 * SUM(CASE WHEN cda.phase_state = 'green' THEN 1 ELSE 0 END)
                         / COUNT(cda.arrival_time)
                    ELSE 0.0
                END AS arrival_on_green_pct
            FROM cycle_boundary cb
            LEFT JOIN cycle_detector_arrival cda
                ON cda.signal_id = cb.signal_id
                AND cda.phase = cb.phase
                AND cda.green_start = cb.green_start
            WHERE cb.{db_facade.lookback_predicate('green_start', hours).replace('green_start', 'cb.green_start')}
            GROUP BY cb.signal_id, cb.phase, {bucket}
        """

    await session.execute(text(delete_sql))
    await session.execute(text(insert_sql))
    logger.info("Refreshed cycle_summary_15min")
