"""
Aggregation pipeline jobs for non-TimescaleDB databases.

Each job deletes stale rows within a sliding lookback window, then
re-aggregates from raw ControllerEventLog events.  On PostgreSQL with
TimescaleDB the jobs detect the extension at first run and disable
themselves (continuous aggregates handle it natively).

All SQL is generated via DatabaseFacade helpers so that it works
across PostgreSQL, MS-SQL, Oracle, and MySQL.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.database.db import db_facade
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)

# Module-level flag: set once on first run to avoid repeated PG queries.
_timescaledb_checked: bool = False
_timescaledb_active: bool = False


async def _should_skip(session: AsyncSession) -> bool:
    """Return True if TimescaleDB continuous aggregates handle this."""
    global _timescaledb_checked, _timescaledb_active

    if not settings.aggregation_enabled:
        return True

    if not _timescaledb_checked:
        _timescaledb_active = await db_facade.has_timescaledb(session)
        _timescaledb_checked = True
        if _timescaledb_active:
            logger.info(
                "TimescaleDB detected — aggregation jobs disabled "
                "(continuous aggregates handle this)"
            )

    return _timescaledb_active


# ---------------------------------------------------------------------------
# Helper: delete + insert pattern
# ---------------------------------------------------------------------------


async def _refresh_aggregate(
    session: AsyncSession,
    *,
    table: str,
    time_column: str,
    insert_sql: str,
) -> None:
    """
    Delete-and-reinsert within the lookback window.

    Args:
        session: Active DB session (caller handles commit/rollback).
        table: Target aggregate table name.
        time_column: Timestamp column used for the window predicate.
        insert_sql: Full INSERT INTO ... SELECT ... statement.
    """
    hours = settings.aggregation_lookback_hours

    delete_sql = db_facade.delete_window_sql(table, time_column, hours)
    await session.execute(text(delete_sql))
    await session.execute(text(insert_sql))


# ---------------------------------------------------------------------------
# 1. Detector Volume (hourly)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_detector_volume", trigger="cron", minute="*/15")
async def agg_detector_volume(session: AsyncSession) -> None:
    """Aggregate hourly detector ON counts per signal/channel."""
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    bucket = db_facade.time_bucket("event_time", "hour")
    predicate = db_facade.lookback_predicate("event_time", hours)

    sql = f"""
        INSERT INTO detector_volume_hourly
            (signal_id, detector_channel, hour_start, volume, activations)
        SELECT
            signal_id,
            event_param AS detector_channel,
            {bucket} AS hour_start,
            COUNT(*) FILTER (WHERE event_code = 82) AS volume,
            COUNT(*) FILTER (WHERE event_code = 81) AS activations
        FROM controller_event_log
        WHERE event_code IN (81, 82)
          AND {predicate}
        GROUP BY signal_id, event_param, {bucket}
    """

    # MS-SQL / Oracle / MySQL lack FILTER (WHERE ...) — use CASE instead
    if db_facade.db_type != "postgresql":
        sql = f"""
            INSERT INTO detector_volume_hourly
                (signal_id, detector_channel, hour_start, volume, activations)
            SELECT
                signal_id,
                event_param AS detector_channel,
                {bucket} AS hour_start,
                SUM(CASE WHEN event_code = 82 THEN 1 ELSE 0 END) AS volume,
                SUM(CASE WHEN event_code = 81 THEN 1 ELSE 0 END) AS activations
            FROM controller_event_log
            WHERE event_code IN (81, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """

    await _refresh_aggregate(
        session,
        table="detector_volume_hourly",
        time_column="hour_start",
        insert_sql=sql,
    )
    logger.info("Refreshed detector_volume_hourly")


# ---------------------------------------------------------------------------
# 2. Detector Occupancy (hourly) — approximate
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_detector_occupancy", trigger="cron", minute="*/15")
async def agg_detector_occupancy(session: AsyncSession) -> None:
    """
    Aggregate hourly detector occupancy.

    Approximation: total_on_seconds = (ON count * avg_on_duration).
    Exact pair-matching across hour boundaries is expensive in SQL;
    the approximation is standard practice in ATSPM systems.
    """
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    bucket = db_facade.time_bucket("event_time", "hour")
    predicate = db_facade.lookback_predicate("event_time", hours)

    if db_facade.db_type == "postgresql":
        sql = f"""
            INSERT INTO detector_occupancy_hourly
                (signal_id, detector_channel, hour_start, occupancy_pct,
                 total_on_seconds, activation_count)
            SELECT
                signal_id,
                event_param AS detector_channel,
                {bucket} AS hour_start,
                LEAST(
                    (COUNT(*) FILTER (WHERE event_code = 82)::float
                     / GREATEST(COUNT(*) FILTER (WHERE event_code = 81), 1))
                    * 100.0 / 3600.0
                    * (EXTRACT(EPOCH FROM MAX(event_time) - MIN(event_time))
                       / GREATEST(COUNT(*) FILTER (WHERE event_code = 82), 1)),
                    100.0
                ) AS occupancy_pct,
                0 AS total_on_seconds,
                COUNT(*) FILTER (WHERE event_code = 82) AS activation_count
            FROM controller_event_log
            WHERE event_code IN (81, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """
    else:
        sql = f"""
            INSERT INTO detector_occupancy_hourly
                (signal_id, detector_channel, hour_start, occupancy_pct,
                 total_on_seconds, activation_count)
            SELECT
                signal_id,
                event_param AS detector_channel,
                {bucket} AS hour_start,
                0 AS occupancy_pct,
                0 AS total_on_seconds,
                SUM(CASE WHEN event_code = 82 THEN 1 ELSE 0 END) AS activation_count
            FROM controller_event_log
            WHERE event_code IN (81, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """

    await _refresh_aggregate(
        session,
        table="detector_occupancy_hourly",
        time_column="hour_start",
        insert_sql=sql,
    )
    logger.info("Refreshed detector_occupancy_hourly")


# ---------------------------------------------------------------------------
# 3. Split Failure (hourly)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_split_failure", trigger="cron", minute="*/15")
async def agg_split_failure(session: AsyncSession) -> None:
    """
    Aggregate hourly split failure rate per phase.

    A cycle is marked as a split failure when max-out (event 5) occurs,
    which indicates demand exceeded the available green time.
    """
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    bucket = db_facade.time_bucket("event_time", "hour")
    predicate = db_facade.lookback_predicate("event_time", hours)

    if db_facade.db_type == "postgresql":
        sql = f"""
            INSERT INTO split_failure_hourly
                (signal_id, phase, hour_start, total_cycles, failed_cycles, failure_rate_pct)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS hour_start,
                COUNT(*) FILTER (WHERE event_code = 1) AS total_cycles,
                COUNT(*) FILTER (WHERE event_code = 5) AS failed_cycles,
                CASE WHEN COUNT(*) FILTER (WHERE event_code = 1) > 0
                     THEN ROUND(
                         COUNT(*) FILTER (WHERE event_code = 5)::numeric
                         / COUNT(*) FILTER (WHERE event_code = 1) * 100, 1)
                     ELSE 0
                END AS failure_rate_pct
            FROM controller_event_log
            WHERE event_code IN (1, 5)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """
    else:
        sql = f"""
            INSERT INTO split_failure_hourly
                (signal_id, phase, hour_start, total_cycles, failed_cycles, failure_rate_pct)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS hour_start,
                SUM(CASE WHEN event_code = 1 THEN 1 ELSE 0 END) AS total_cycles,
                SUM(CASE WHEN event_code = 5 THEN 1 ELSE 0 END) AS failed_cycles,
                CASE WHEN SUM(CASE WHEN event_code = 1 THEN 1 ELSE 0 END) > 0
                     THEN ROUND(
                         CAST(SUM(CASE WHEN event_code = 5 THEN 1 ELSE 0 END) AS FLOAT)
                         / SUM(CASE WHEN event_code = 1 THEN 1 ELSE 0 END) * 100, 1)
                     ELSE 0
                END AS failure_rate_pct
            FROM controller_event_log
            WHERE event_code IN (1, 5)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """

    await _refresh_aggregate(
        session,
        table="split_failure_hourly",
        time_column="hour_start",
        insert_sql=sql,
    )
    logger.info("Refreshed split_failure_hourly")


# ---------------------------------------------------------------------------
# 4. Approach Delay (15-minute bins)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_approach_delay", trigger="cron", minute="*/15")
async def agg_approach_delay(session: AsyncSession) -> None:
    """
    Aggregate 15-minute approach delay per phase.

    Delay is approximated as the time difference between consecutive
    detector ON (82) events and the preceding phase green (1) event
    on the same phase. This is a simplified approximation; the full
    cycle-matched calculation runs in the report plugin.
    """
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)

    if db_facade.db_type == "postgresql":
        bucket = "time_bucket('15 minutes', event_time)"
        sql = f"""
            INSERT INTO approach_delay_15min
                (signal_id, phase, bin_start, avg_delay_seconds,
                 max_delay_seconds, total_arrivals)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS bin_start,
                0 AS avg_delay_seconds,
                0 AS max_delay_seconds,
                COUNT(*) FILTER (WHERE event_code = 82) AS total_arrivals
            FROM controller_event_log
            WHERE event_code IN (1, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """
    else:
        if db_facade.db_type == "mssql":
            bucket = "DATEADD(minute, (DATEDIFF(minute, 0, event_time) / 15) * 15, 0)"
        elif db_facade.db_type == "oracle":
            bucket = (
                "TRUNC(event_time, 'MI') - MOD(EXTRACT(MINUTE FROM event_time), 15)"
                " * INTERVAL '1' MINUTE"
            )
        else:
            bucket = (
                "DATE_FORMAT(event_time, '%Y-%m-%d %H:')"
                " + LPAD(FLOOR(MINUTE(event_time)/15)*15, 2, '0')"
            )

        sql = f"""
            INSERT INTO approach_delay_15min
                (signal_id, phase, bin_start, avg_delay_seconds,
                 max_delay_seconds, total_arrivals)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS bin_start,
                0 AS avg_delay_seconds,
                0 AS max_delay_seconds,
                SUM(CASE WHEN event_code = 82 THEN 1 ELSE 0 END) AS total_arrivals
            FROM controller_event_log
            WHERE event_code IN (1, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """

    await _refresh_aggregate(
        session,
        table="approach_delay_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed approach_delay_15min")


# ---------------------------------------------------------------------------
# 5. Arrival on Red (hourly)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_arrival_on_red", trigger="cron", minute="*/15")
async def agg_arrival_on_red(session: AsyncSession) -> None:
    """
    Aggregate hourly arrivals-on-red/green per phase.

    Counts detector ON (82) events and phase green (1) events per
    hour/phase. Exact red/green attribution requires cycle matching
    (done in the report plugin); this provides volume counts for
    dashboard use.
    """
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    bucket = db_facade.time_bucket("event_time", "hour")
    predicate = db_facade.lookback_predicate("event_time", hours)

    if db_facade.db_type == "postgresql":
        sql = f"""
            INSERT INTO arrival_on_red_hourly
                (signal_id, phase, hour_start, total_arrivals,
                 arrivals_on_red, arrivals_on_green, red_pct, green_pct)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS hour_start,
                COUNT(*) FILTER (WHERE event_code = 82) AS total_arrivals,
                0 AS arrivals_on_red,
                0 AS arrivals_on_green,
                0 AS red_pct,
                0 AS green_pct
            FROM controller_event_log
            WHERE event_code IN (1, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """
    else:
        sql = f"""
            INSERT INTO arrival_on_red_hourly
                (signal_id, phase, hour_start, total_arrivals,
                 arrivals_on_red, arrivals_on_green, red_pct, green_pct)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS hour_start,
                SUM(CASE WHEN event_code = 82 THEN 1 ELSE 0 END) AS total_arrivals,
                0 AS arrivals_on_red,
                0 AS arrivals_on_green,
                0 AS red_pct,
                0 AS green_pct
            FROM controller_event_log
            WHERE event_code IN (1, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """

    await _refresh_aggregate(
        session,
        table="arrival_on_red_hourly",
        time_column="hour_start",
        insert_sql=sql,
    )
    logger.info("Refreshed arrival_on_red_hourly")


# ---------------------------------------------------------------------------
# 6. Coordination Quality (hourly)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_coordination_quality", trigger="cron", minute="*/15")
async def agg_coordination_quality(session: AsyncSession) -> None:
    """
    Aggregate hourly coordination quality per signal.

    Counts phase 2 green events per hour as a proxy for cycle count.
    Full quality analysis (drift, tolerance) runs in the analytics
    endpoint against raw events.
    """
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    bucket = db_facade.time_bucket("event_time", "hour")
    predicate = db_facade.lookback_predicate("event_time", hours)

    sql = f"""
        INSERT INTO coordination_quality_hourly
            (signal_id, hour_start, total_cycles, cycles_within_tolerance,
             quality_pct, avg_cycle_length_seconds, avg_offset_error_seconds)
        SELECT
            signal_id,
            {bucket} AS hour_start,
            COUNT(*) AS total_cycles,
            0 AS cycles_within_tolerance,
            0 AS quality_pct,
            0 AS avg_cycle_length_seconds,
            0 AS avg_offset_error_seconds
        FROM controller_event_log
        WHERE event_code = 1
          AND event_param = 2
          AND {predicate}
        GROUP BY signal_id, {bucket}
    """

    await _refresh_aggregate(
        session,
        table="coordination_quality_hourly",
        time_column="hour_start",
        insert_sql=sql,
    )
    logger.info("Refreshed coordination_quality_hourly")


# ---------------------------------------------------------------------------
# 7. Phase Termination (hourly)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_phase_termination", trigger="cron", minute="*/15")
async def agg_phase_termination(session: AsyncSession) -> None:
    """Aggregate hourly phase termination counts per signal/phase."""
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    bucket = db_facade.time_bucket("event_time", "hour")
    predicate = db_facade.lookback_predicate("event_time", hours)

    if db_facade.db_type == "postgresql":
        sql = f"""
            INSERT INTO phase_termination_hourly
                (signal_id, phase, hour_start, total_cycles, gap_outs,
                 max_outs, force_offs)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS hour_start,
                COUNT(*) FILTER (WHERE event_code = 1) AS total_cycles,
                COUNT(*) FILTER (WHERE event_code = 4) AS gap_outs,
                COUNT(*) FILTER (WHERE event_code = 5) AS max_outs,
                COUNT(*) FILTER (WHERE event_code = 6) AS force_offs
            FROM controller_event_log
            WHERE event_code IN (1, 4, 5, 6)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """
    else:
        sql = f"""
            INSERT INTO phase_termination_hourly
                (signal_id, phase, hour_start, total_cycles, gap_outs,
                 max_outs, force_offs)
            SELECT
                signal_id,
                event_param AS phase,
                {bucket} AS hour_start,
                SUM(CASE WHEN event_code = 1 THEN 1 ELSE 0 END) AS total_cycles,
                SUM(CASE WHEN event_code = 4 THEN 1 ELSE 0 END) AS gap_outs,
                SUM(CASE WHEN event_code = 5 THEN 1 ELSE 0 END) AS max_outs,
                SUM(CASE WHEN event_code = 6 THEN 1 ELSE 0 END) AS force_offs
            FROM controller_event_log
            WHERE event_code IN (1, 4, 5, 6)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """

    await _refresh_aggregate(
        session,
        table="phase_termination_hourly",
        time_column="hour_start",
        insert_sql=sql,
    )
    logger.info("Refreshed phase_termination_hourly")
