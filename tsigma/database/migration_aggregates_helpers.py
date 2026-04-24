"""
Helpers for the second-batch aggregate tables in the initial migration.

Separated from ``alembic/versions/000000000001_initial_schema.py`` to
keep individual files under the 1000-line cap.  Exposes two public
helpers consumed by the initial migration:

  - :func:`create_second_batch_aggregate_tables` — idempotent
    ``op.create_table`` calls for the 8 new aggregate tables.
  - :func:`second_batch_continuous_aggregates` — DDL blocks that
    convert each table into a TimescaleDB continuous aggregate when the
    extension is available (falls through cleanly otherwise).

All functions are called from within the initial migration's
``upgrade()`` body, so ``op`` must already have a bound context.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# ---------------------------------------------------------------------------
# Introspection helper (duplicated from the migration to avoid cross-import).
# ---------------------------------------------------------------------------


def _existing_index_names(table_name: str) -> set[str]:
    """Return the set of existing index names on ``table_name``.

    Returns an empty set if the table does not exist — callers combine
    this with ``inspect().has_table`` for idempotent index creation.
    """
    insp = sa.inspect(op.get_bind())
    if not insp.has_table(table_name):
        return set()
    return {idx["name"] for idx in insp.get_indexes(table_name)}


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------


def create_second_batch_aggregate_tables() -> None:
    """Create the 8 new second-batch aggregate tables idempotently."""
    insp = sa.inspect(op.get_bind())

    _create_approach_speed(insp)
    _create_phase_cycle(insp)
    _create_phase_left_turn_gap(insp)
    _create_phase_pedestrian(insp)
    _create_preemption(insp)
    _create_priority(insp)
    _create_signal_event_count(insp)
    _create_yellow_red_activation(insp)


def _create_approach_speed(insp) -> None:
    if not insp.has_table("approach_speed_15min"):
        op.create_table(
            "approach_speed_15min",
            sa.Column("signal_id", sa.Text, primary_key=True),
            sa.Column("approach_id", sa.Text, primary_key=True),
            sa.Column(
                "bin_start", postgresql.TIMESTAMP(timezone=True),
                primary_key=True,
            ),
            sa.Column("p15", sa.Float, nullable=False, server_default="0"),
            sa.Column("p50", sa.Float, nullable=False, server_default="0"),
            sa.Column("p85", sa.Float, nullable=False, server_default="0"),
            sa.Column(
                "sample_count", sa.Integer,
                nullable=False, server_default="0",
            ),
        )
    if "idx_as15_signal_bin" not in _existing_index_names("approach_speed_15min"):
        op.create_index(
            "idx_as15_signal_bin", "approach_speed_15min",
            ["signal_id", "bin_start"],
        )
        op.create_index("idx_as15_bin", "approach_speed_15min", ["bin_start"])


def _create_phase_cycle(insp) -> None:
    if not insp.has_table("phase_cycle_15min"):
        op.create_table(
            "phase_cycle_15min",
            sa.Column("signal_id", sa.Text, primary_key=True),
            sa.Column("phase", sa.Integer, primary_key=True),
            sa.Column(
                "bin_start", postgresql.TIMESTAMP(timezone=True),
                primary_key=True,
            ),
            sa.Column(
                "green_seconds", sa.Float, nullable=False, server_default="0",
            ),
            sa.Column(
                "yellow_seconds", sa.Float, nullable=False, server_default="0",
            ),
            sa.Column(
                "red_seconds", sa.Float, nullable=False, server_default="0",
            ),
            sa.Column(
                "cycle_count", sa.Integer, nullable=False, server_default="0",
            ),
        )
    if "idx_pc15_signal_bin" not in _existing_index_names("phase_cycle_15min"):
        op.create_index(
            "idx_pc15_signal_bin", "phase_cycle_15min",
            ["signal_id", "bin_start"],
        )
        op.create_index("idx_pc15_bin", "phase_cycle_15min", ["bin_start"])


def _create_phase_left_turn_gap(insp) -> None:
    if not insp.has_table("phase_left_turn_gap_15min"):
        op.create_table(
            "phase_left_turn_gap_15min",
            sa.Column("signal_id", sa.Text, primary_key=True),
            sa.Column("phase", sa.Integer, primary_key=True),
            sa.Column(
                "bin_start", postgresql.TIMESTAMP(timezone=True),
                primary_key=True,
            ),
            *[
                sa.Column(name, sa.Integer, nullable=False, server_default="0")
                for name in (
                    "bin_1s", "bin_2s", "bin_3s", "bin_4s", "bin_5s",
                    "bin_6s", "bin_7s", "bin_8s", "bin_9s", "bin_10s",
                    "bin_10plus",
                )
            ],
        )
    if "idx_pltg15_signal_bin" not in _existing_index_names(
        "phase_left_turn_gap_15min",
    ):
        op.create_index(
            "idx_pltg15_signal_bin", "phase_left_turn_gap_15min",
            ["signal_id", "bin_start"],
        )
        op.create_index(
            "idx_pltg15_bin", "phase_left_turn_gap_15min", ["bin_start"],
        )


def _create_phase_pedestrian(insp) -> None:
    if not insp.has_table("phase_pedestrian_15min"):
        op.create_table(
            "phase_pedestrian_15min",
            sa.Column("signal_id", sa.Text, primary_key=True),
            sa.Column("phase", sa.Integer, primary_key=True),
            sa.Column(
                "bin_start", postgresql.TIMESTAMP(timezone=True),
                primary_key=True,
            ),
            sa.Column(
                "ped_walk_count", sa.Integer, nullable=False, server_default="0",
            ),
            sa.Column(
                "ped_call_count", sa.Integer, nullable=False, server_default="0",
            ),
            sa.Column(
                "ped_delay_sum_seconds", sa.Float,
                nullable=False, server_default="0",
            ),
            sa.Column(
                "ped_delay_count", sa.Integer,
                nullable=False, server_default="0",
            ),
        )
    if "idx_pp15_signal_bin" not in _existing_index_names(
        "phase_pedestrian_15min",
    ):
        op.create_index(
            "idx_pp15_signal_bin", "phase_pedestrian_15min",
            ["signal_id", "bin_start"],
        )
        op.create_index(
            "idx_pp15_bin", "phase_pedestrian_15min", ["bin_start"],
        )


def _create_preemption(insp) -> None:
    if not insp.has_table("preemption_15min"):
        op.create_table(
            "preemption_15min",
            sa.Column("signal_id", sa.Text, primary_key=True),
            sa.Column("preempt_channel", sa.Integer, primary_key=True),
            sa.Column(
                "bin_start", postgresql.TIMESTAMP(timezone=True),
                primary_key=True,
            ),
            sa.Column(
                "request_count", sa.Integer, nullable=False, server_default="0",
            ),
            sa.Column(
                "service_count", sa.Integer, nullable=False, server_default="0",
            ),
            sa.Column(
                "mean_delay_seconds", sa.Float,
                nullable=False, server_default="0",
            ),
        )
    if "idx_pe15_signal_bin" not in _existing_index_names("preemption_15min"):
        op.create_index(
            "idx_pe15_signal_bin", "preemption_15min",
            ["signal_id", "bin_start"],
        )
        op.create_index("idx_pe15_bin", "preemption_15min", ["bin_start"])


def _create_priority(insp) -> None:
    if not insp.has_table("priority_15min"):
        op.create_table(
            "priority_15min",
            sa.Column("signal_id", sa.Text, primary_key=True),
            sa.Column("phase", sa.Integer, primary_key=True),
            sa.Column(
                "bin_start", postgresql.TIMESTAMP(timezone=True),
                primary_key=True,
            ),
            sa.Column(
                "early_green_count", sa.Integer,
                nullable=False, server_default="0",
            ),
            sa.Column(
                "extended_green_count", sa.Integer,
                nullable=False, server_default="0",
            ),
            sa.Column(
                "check_in_count", sa.Integer,
                nullable=False, server_default="0",
            ),
            sa.Column(
                "check_out_count", sa.Integer,
                nullable=False, server_default="0",
            ),
        )
    if "idx_pr15_signal_bin" not in _existing_index_names("priority_15min"):
        op.create_index(
            "idx_pr15_signal_bin", "priority_15min",
            ["signal_id", "bin_start"],
        )
        op.create_index("idx_pr15_bin", "priority_15min", ["bin_start"])


def _create_signal_event_count(insp) -> None:
    if not insp.has_table("signal_event_count_15min"):
        op.create_table(
            "signal_event_count_15min",
            sa.Column("signal_id", sa.Text, primary_key=True),
            sa.Column(
                "bin_start", postgresql.TIMESTAMP(timezone=True),
                primary_key=True,
            ),
            sa.Column(
                "event_count", sa.Integer, nullable=False, server_default="0",
            ),
        )
    if "idx_sec15_signal_bin" not in _existing_index_names(
        "signal_event_count_15min",
    ):
        op.create_index(
            "idx_sec15_signal_bin", "signal_event_count_15min",
            ["signal_id", "bin_start"],
        )
        op.create_index(
            "idx_sec15_bin", "signal_event_count_15min", ["bin_start"],
        )


def _create_yellow_red_activation(insp) -> None:
    if not insp.has_table("yellow_red_activation_15min"):
        op.create_table(
            "yellow_red_activation_15min",
            sa.Column("signal_id", sa.Text, primary_key=True),
            sa.Column("phase", sa.Integer, primary_key=True),
            sa.Column(
                "bin_start", postgresql.TIMESTAMP(timezone=True),
                primary_key=True,
            ),
            sa.Column(
                "yellow_activation_count", sa.Integer,
                nullable=False, server_default="0",
            ),
            sa.Column(
                "red_activation_count", sa.Integer,
                nullable=False, server_default="0",
            ),
            sa.Column(
                "red_duration_sum_seconds", sa.Float,
                nullable=False, server_default="0",
            ),
        )
    if "idx_yra15_signal_bin" not in _existing_index_names(
        "yellow_red_activation_15min",
    ):
        op.create_index(
            "idx_yra15_signal_bin", "yellow_red_activation_15min",
            ["signal_id", "bin_start"],
        )
        op.create_index(
            "idx_yra15_bin", "yellow_red_activation_15min", ["bin_start"],
        )


# ---------------------------------------------------------------------------
# Continuous aggregate DDL blocks
# ---------------------------------------------------------------------------


def _cagg_block(*, view: str, select_sql: str) -> str:
    """Build a DO-block that creates a continuous aggregate idempotently.

    The regular table created by ``create_second_batch_aggregate_tables``
    is dropped first so the CAGG can take over the name.  The policy
    call uses ``if_not_exists => true`` so a re-run is safe.  The whole
    block is wrapped in ``EXCEPTION WHEN undefined_function`` so non-
    TimescaleDB PostgreSQL installs fall through cleanly and the regular
    table is retained.
    """
    return (
        "DO $$ BEGIN "
        f"  DROP TABLE IF EXISTS {view}; "
        f"  CREATE MATERIALIZED VIEW IF NOT EXISTS {view} "
        "  WITH (timescaledb.continuous) AS "
        f"  {select_sql}; "
        f"  PERFORM add_continuous_aggregate_policy('{view}', "
        "    start_offset => INTERVAL '2 hours', "
        "    end_offset => INTERVAL '1 minute', "
        "    schedule_interval => INTERVAL '15 minutes', "
        "    if_not_exists => true "
        "  ); "
        "EXCEPTION WHEN undefined_function THEN "
        f"  RAISE NOTICE 'TimescaleDB not available — keeping regular {view} table'; "
        "END $$;"
    )


def second_batch_continuous_aggregates() -> list[str]:
    """Return the list of DDL blocks for the 8 continuous aggregates."""
    return [
        _cagg_block(
            view="approach_speed_15min",
            select_sql=(
                "SELECT "
                "  cel.signal_id, "
                "  CAST(d.approach_id AS TEXT) AS approach_id, "
                "  time_bucket('15 minutes', cel.event_time) AS bin_start, "
                "  COALESCE(PERCENTILE_CONT(0.15) "
                "    WITHIN GROUP (ORDER BY cel.event_param), 0) AS p15, "
                "  COALESCE(PERCENTILE_CONT(0.50) "
                "    WITHIN GROUP (ORDER BY cel.event_param), 0) AS p50, "
                "  COALESCE(PERCENTILE_CONT(0.85) "
                "    WITHIN GROUP (ORDER BY cel.event_param), 0) AS p85, "
                "  COUNT(*) AS sample_count "
                "FROM controller_event_log cel "
                "JOIN detector d ON d.detector_channel = cel.event_param "
                "JOIN approach a ON a.approach_id = d.approach_id "
                "WHERE cel.event_code = 82 "
                "  AND d.min_speed_filter IS NOT NULL "
                "  AND a.mph IS NOT NULL "
                "GROUP BY cel.signal_id, d.approach_id, "
                "  time_bucket('15 minutes', cel.event_time)"
            ),
        ),
        _cagg_block(
            view="phase_cycle_15min",
            select_sql=(
                "SELECT "
                "  signal_id, "
                "  event_param AS phase, "
                "  time_bucket('15 minutes', event_time) AS bin_start, "
                "  0::double precision AS green_seconds, "
                "  0::double precision AS yellow_seconds, "
                "  0::double precision AS red_seconds, "
                "  COUNT(*) FILTER (WHERE event_code = 1) AS cycle_count "
                "FROM controller_event_log "
                "WHERE event_code IN (1, 8, 9) "
                "GROUP BY signal_id, event_param, "
                "  time_bucket('15 minutes', event_time)"
            ),
        ),
        _cagg_block(
            view="phase_left_turn_gap_15min",
            select_sql=(
                "SELECT "
                "  signal_id, "
                "  event_param AS phase, "
                "  time_bucket('15 minutes', event_time) AS bin_start, "
                "  0 AS bin_1s, 0 AS bin_2s, 0 AS bin_3s, 0 AS bin_4s, "
                "  0 AS bin_5s, 0 AS bin_6s, 0 AS bin_7s, 0 AS bin_8s, "
                "  0 AS bin_9s, 0 AS bin_10s, "
                "  COUNT(*) FILTER (WHERE event_code = 4) AS bin_10plus "
                "FROM controller_event_log "
                "WHERE event_code IN (1, 4, 5, 6, 82) "
                "GROUP BY signal_id, event_param, "
                "  time_bucket('15 minutes', event_time)"
            ),
        ),
        _cagg_block(
            view="phase_pedestrian_15min",
            select_sql=(
                "SELECT "
                "  signal_id, "
                "  event_param AS phase, "
                "  time_bucket('15 minutes', event_time) AS bin_start, "
                "  COUNT(*) FILTER (WHERE event_code = 21) AS ped_walk_count, "
                "  COUNT(*) FILTER (WHERE event_code = 45) AS ped_call_count, "
                "  0::double precision AS ped_delay_sum_seconds, "
                "  0 AS ped_delay_count "
                "FROM controller_event_log "
                "WHERE event_code IN (21, 45) "
                "GROUP BY signal_id, event_param, "
                "  time_bucket('15 minutes', event_time)"
            ),
        ),
        _cagg_block(
            view="preemption_15min",
            select_sql=(
                "SELECT "
                "  signal_id, "
                "  event_param AS preempt_channel, "
                "  time_bucket('15 minutes', event_time) AS bin_start, "
                "  COUNT(*) FILTER (WHERE event_code = 102) AS request_count, "
                "  COUNT(*) FILTER (WHERE event_code = 105) AS service_count, "
                "  COALESCE(EXTRACT(EPOCH FROM ( "
                "    MIN(event_time) FILTER (WHERE event_code = 105) "
                "    - MIN(event_time) FILTER (WHERE event_code = 102) "
                "  )), 0) AS mean_delay_seconds "
                "FROM controller_event_log "
                "WHERE event_code IN (102, 105) "
                "GROUP BY signal_id, event_param, "
                "  time_bucket('15 minutes', event_time)"
            ),
        ),
        _cagg_block(
            view="priority_15min",
            select_sql=(
                "SELECT "
                "  signal_id, "
                "  event_param AS phase, "
                "  time_bucket('15 minutes', event_time) AS bin_start, "
                "  COUNT(*) FILTER (WHERE event_code = 113) AS early_green_count, "
                "  COUNT(*) FILTER (WHERE event_code = 114) "
                "    AS extended_green_count, "
                "  COUNT(*) FILTER (WHERE event_code = 112) AS check_in_count, "
                "  COUNT(*) FILTER (WHERE event_code = 115) AS check_out_count "
                "FROM controller_event_log "
                "WHERE event_code IN (112, 113, 114, 115) "
                "GROUP BY signal_id, event_param, "
                "  time_bucket('15 minutes', event_time)"
            ),
        ),
        _cagg_block(
            view="signal_event_count_15min",
            select_sql=(
                "SELECT "
                "  signal_id, "
                "  time_bucket('15 minutes', event_time) AS bin_start, "
                "  COUNT(*) AS event_count "
                "FROM controller_event_log "
                "GROUP BY signal_id, "
                "  time_bucket('15 minutes', event_time)"
            ),
        ),
        _cagg_block(
            view="yellow_red_activation_15min",
            select_sql=(
                "SELECT "
                "  signal_id, "
                "  event_param AS phase, "
                "  time_bucket('15 minutes', event_time) AS bin_start, "
                "  COUNT(*) FILTER (WHERE event_code = 8) "
                "    AS yellow_activation_count, "
                "  COUNT(*) FILTER (WHERE event_code = 9) "
                "    AS red_activation_count, "
                "  0::double precision AS red_duration_sum_seconds "
                "FROM controller_event_log "
                "WHERE event_code IN (1, 8, 9, 82) "
                "GROUP BY signal_id, event_param, "
                "  time_bucket('15 minutes', event_time)"
            ),
        ),
    ]
