"""
Signal-scoped aggregation jobs for non-TimescaleDB databases.

Companion module to ``tsigma.scheduler.jobs.aggregate`` /
``aggregate_phase``.  Hosts the aggregations whose grain is not phase-
scoped:

  - ``agg_preemption``          -> ``preemption_15min``
  - ``agg_signal_event_count``  -> ``signal_event_count_15min``

Both follow the same shape as the other aggregate jobs: guard with
``_should_skip``, build dialect-neutral SQL via ``db_facade`` helpers,
delete-and-reinsert the lookback window via ``_refresh_aggregate``.
"""

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.database.db import db_facade
from tsigma.reports.sdk.events import (
    EVENT_PREEMPTION_CALL_INPUT_ON,
    EVENT_PREEMPTION_ENTRY_STARTED,
)
from tsigma.scheduler.jobs.aggregate import _refresh_aggregate, _should_skip
from tsigma.scheduler.jobs.aggregate_phase import _fifteen_minute_bucket, _seconds_diff
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Preemption (requests, services, mean delay)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_preemption", trigger="cron", minute="*/15")
async def agg_preemption(session: AsyncSession) -> None:
    """Aggregate 15-minute preemption counts + mean request→service delay.

    Sources:
      - ``EVENT_PREEMPTION_CALL_INPUT_ON``  (102, request)
      - ``EVENT_PREEMPTION_ENTRY_STARTED`` (105, service)

    Every code-102 request is paired with the earliest code-105 service
    at-or-after it on the same ``(signal_id, preempt_channel)`` via a
    correlated ``MIN()`` sub-query.  The bin's ``mean_delay_seconds`` =
    sum of paired delays / count of paired delays.  Un-paired requests
    (service never arrived in the window) contribute to request_count
    only.
    """
    if await _should_skip(session):
        return

    from tsigma.config import settings
    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)
    bucket = _fifteen_minute_bucket()
    delay_expr = _seconds_diff("request_time", "service_time")

    codes = f"{EVENT_PREEMPTION_CALL_INPUT_ON}, {EVENT_PREEMPTION_ENTRY_STARTED}"

    sql = f"""
        WITH requests AS (
            SELECT signal_id, event_param AS preempt_channel,
                   event_time AS request_time
            FROM controller_event_log
            WHERE event_code = {EVENT_PREEMPTION_CALL_INPUT_ON}
              AND {predicate}
        ),
        paired AS (
            SELECT
                r.signal_id,
                r.preempt_channel,
                r.request_time,
                (
                    SELECT MIN(s.event_time)
                    FROM controller_event_log s
                    WHERE s.event_code = {EVENT_PREEMPTION_ENTRY_STARTED}
                      AND s.signal_id = r.signal_id
                      AND s.event_param = r.preempt_channel
                      AND s.event_time >= r.request_time
                ) AS service_time
            FROM requests r
        ),
        delays AS (
            SELECT
                signal_id,
                preempt_channel,
                {_fifteen_minute_bucket("request_time")} AS bin_start,
                SUM(CASE WHEN service_time IS NOT NULL
                         THEN {delay_expr} ELSE 0 END) AS delay_sum,
                SUM(CASE WHEN service_time IS NOT NULL
                         THEN 1 ELSE 0 END) AS paired_count
            FROM paired
            GROUP BY signal_id, preempt_channel,
                     {_fifteen_minute_bucket("request_time")}
        ),
        counts AS (
            SELECT
                signal_id,
                event_param AS preempt_channel,
                {bucket} AS bin_start,
                SUM(CASE WHEN event_code = {EVENT_PREEMPTION_CALL_INPUT_ON}
                         THEN 1 ELSE 0 END) AS request_count,
                SUM(CASE WHEN event_code = {EVENT_PREEMPTION_ENTRY_STARTED}
                         THEN 1 ELSE 0 END) AS service_count
            FROM controller_event_log
            WHERE event_code IN ({codes})
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        )
        INSERT INTO preemption_15min
            (signal_id, preempt_channel, bin_start, request_count,
             service_count, mean_delay_seconds)
        SELECT
            c.signal_id,
            c.preempt_channel,
            c.bin_start,
            c.request_count,
            c.service_count,
            CASE
                WHEN d.paired_count IS NULL OR d.paired_count = 0 THEN 0
                ELSE d.delay_sum / d.paired_count
            END AS mean_delay_seconds
        FROM counts c
        LEFT JOIN delays d
          ON d.signal_id = c.signal_id
         AND d.preempt_channel = c.preempt_channel
         AND d.bin_start = c.bin_start
    """

    await _refresh_aggregate(
        session,
        table="preemption_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed preemption_15min")


# ---------------------------------------------------------------------------
# 2. Signal event count (total events per signal per bin)
# ---------------------------------------------------------------------------


@JobRegistry.register(name="agg_signal_event_count", trigger="cron", minute="*/15")
async def agg_signal_event_count(session: AsyncSession) -> None:
    """Aggregate 15-minute total event counts per signal.

    Coarse heartbeat metric — the number of rows written to
    ``controller_event_log`` per signal per 15-minute bucket.  Feeds
    signal-health dashboards and silent-signal detection.
    """
    if await _should_skip(session):
        return

    from tsigma.config import settings
    hours = settings.aggregation_lookback_hours
    predicate = db_facade.lookback_predicate("event_time", hours)
    bucket = _fifteen_minute_bucket()

    sql = f"""
        INSERT INTO signal_event_count_15min
            (signal_id, bin_start, event_count)
        SELECT
            signal_id,
            {bucket} AS bin_start,
            COUNT(*) AS event_count
        FROM controller_event_log
        WHERE {predicate}
        GROUP BY signal_id, {bucket}
    """

    await _refresh_aggregate(
        session,
        table="signal_event_count_15min",
        time_column="bin_start",
        insert_sql=sql,
    )
    logger.info("Refreshed signal_event_count_15min")
