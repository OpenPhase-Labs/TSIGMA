"""
Tests for the 8 new scheduler-populated aggregation jobs.

Covers:
  - agg_approach_speed        -> approach_speed_15min
  - agg_phase_cycle           -> phase_cycle_15min
  - agg_phase_left_turn_gap   -> phase_left_turn_gap_15min
  - agg_phase_pedestrian      -> phase_pedestrian_15min
  - agg_preemption            -> preemption_15min
  - agg_priority              -> priority_15min
  - agg_signal_event_count    -> signal_event_count_15min
  - agg_yellow_red_activation -> yellow_red_activation_15min

Each job follows the existing _should_skip / _refresh_aggregate pattern
from tsigma.scheduler.jobs.aggregate. We mock db_facade + settings and
assert the job issues exactly two execute() calls (DELETE then INSERT),
and that the INSERT SQL references the expected target table and event
codes.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

# Importing the jobs package wires up all @JobRegistry.register decorators.
import tsigma.scheduler.jobs  # noqa: F401  -- triggers auto-import
from tsigma.scheduler.registry import JobRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_session() -> AsyncMock:
    """Return an AsyncMock that behaves like an AsyncSession."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


def _reset_timescale_flags() -> None:
    """Reset module-level TimescaleDB detection so _should_skip re-runs."""
    import tsigma.scheduler.jobs.aggregate as agg_mod
    agg_mod._timescaledb_checked = False
    agg_mod._timescaledb_active = False


@pytest.fixture
def patched_agg_modules(monkeypatch):
    """Patch ``db_facade`` + ``settings`` in every aggregate module with
    shared mocks.

    The new jobs live in ``aggregate_phase`` and ``aggregate_signal``;
    each module keeps its own ``from ... import`` binding, so patching
    ``aggregate.db_facade`` alone is not enough — all three module
    references must be rebound to the same shared mocks.
    """
    from tsigma.scheduler.jobs import aggregate, aggregate_phase, aggregate_signal

    mock_facade = MagicMock()
    mock_settings = MagicMock()
    for mod in (aggregate, aggregate_phase, aggregate_signal):
        monkeypatch.setattr(mod, "db_facade", mock_facade, raising=False)
        monkeypatch.setattr(mod, "settings", mock_settings, raising=False)
    return mock_settings, mock_facade


def _configure_mocks(mock_settings, mock_facade, *, db_type: str = "postgresql",
                     delete_sql: str = "DELETE FROM tbl") -> None:
    mock_settings.aggregation_enabled = True
    mock_settings.aggregation_lookback_hours = 2
    mock_facade.has_timescaledb = AsyncMock(return_value=False)
    mock_facade.db_type = db_type
    mock_facade.time_bucket.return_value = (
        "time_bucket('15 minutes', event_time)" if db_type == "postgresql"
        else "DATEADD(minute, DATEDIFF(minute, 0, event_time), 0)"
    )
    mock_facade.lookback_predicate.return_value = (
        "event_time >= NOW() - INTERVAL '2 hours'"
        if db_type == "postgresql"
        else "event_time >= DATEADD(hour, -2, GETUTCDATE())"
    )
    mock_facade.delete_window_sql.return_value = delete_sql


# ---------------------------------------------------------------------------
# Registration sanity
# ---------------------------------------------------------------------------


class TestNewAggregationsRegistered:
    """All 8 new aggregation jobs are registered on a 15-minute cron."""

    NAMES = [
        "agg_approach_speed",
        "agg_phase_cycle",
        "agg_phase_left_turn_gap",
        "agg_phase_pedestrian",
        "agg_preemption",
        "agg_priority",
        "agg_signal_event_count",
        "agg_yellow_red_activation",
    ]

    def test_all_new_jobs_present(self):
        all_jobs = JobRegistry.list_all()
        for name in self.NAMES:
            assert name in all_jobs, f"Job {name!r} not registered"
            assert all_jobs[name]["trigger"] == "cron"
            assert all_jobs[name]["trigger_kwargs"].get("minute") == "*/15"


# ---------------------------------------------------------------------------
# Per-job behaviour
#
# Every job:
#   * returns early when aggregation_enabled is False
#   * returns early when TimescaleDB is detected
#   * on PostgreSQL with no TimescaleDB: two execute calls (DELETE, INSERT)
#   * INSERT SQL references the expected target table
# ---------------------------------------------------------------------------


_JOB_TABLE_EVENTS = [
    # agg_approach_speed is parametrised separately — it sources from
    # roadside_event, not controller_event_log, so the generic
    # event-code-in-SQL assertion doesn't fit it.
    ("agg_phase_cycle", "phase_cycle_15min", ("1", "8", "9")),
    ("agg_phase_left_turn_gap", "phase_left_turn_gap_15min", ("4",)),
    ("agg_phase_pedestrian", "phase_pedestrian_15min", ("21", "45")),
    ("agg_preemption", "preemption_15min", ("102", "105")),
    ("agg_priority", "priority_15min", ("112", "113", "114", "115")),
    ("agg_signal_event_count", "signal_event_count_15min", ()),
    ("agg_yellow_red_activation", "yellow_red_activation_15min", ("8", "9", "82")),
]


@pytest.mark.parametrize("job_name,target_table,event_codes", _JOB_TABLE_EVENTS)
@pytest.mark.asyncio
async def test_job_postgresql_runs_delete_and_insert(
    patched_agg_modules, job_name, target_table, event_codes,
):
    """Job issues DELETE + INSERT on PostgreSQL; INSERT names target table."""
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="postgresql",
        delete_sql=f"DELETE FROM {target_table}",
    )
    _reset_timescale_flags()

    func = JobRegistry.get(job_name)["func"]
    session = _mock_session()
    await func(session)

    assert session.execute.call_count == 2, (
        f"{job_name}: expected 2 execute calls (DELETE + INSERT), "
        f"got {session.execute.call_count}"
    )

    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    assert target_table in insert_sql, (
        f"{job_name}: INSERT does not target {target_table!r}: {insert_sql[:200]}"
    )
    for code in event_codes:
        assert code in insert_sql, (
            f"{job_name}: INSERT SQL missing event code {code}: {insert_sql[:200]}"
        )


@pytest.mark.parametrize("job_name,_target_table,_codes", _JOB_TABLE_EVENTS)
@pytest.mark.asyncio
async def test_job_skips_when_disabled(
    patched_agg_modules, job_name, _target_table, _codes,
):
    """Job short-circuits when aggregation_enabled is False."""
    mock_settings, _ = patched_agg_modules
    mock_settings.aggregation_enabled = False
    _reset_timescale_flags()

    func = JobRegistry.get(job_name)["func"]
    session = _mock_session()
    await func(session)

    session.execute.assert_not_called()


@pytest.mark.parametrize("job_name,_target_table,_codes", _JOB_TABLE_EVENTS)
@pytest.mark.asyncio
async def test_job_skips_when_timescaledb_active(
    patched_agg_modules, job_name, _target_table, _codes,
):
    """Job short-circuits when TimescaleDB is present."""
    mock_settings, mock_facade = patched_agg_modules
    mock_settings.aggregation_enabled = True
    mock_facade.has_timescaledb = AsyncMock(return_value=True)
    _reset_timescale_flags()

    func = JobRegistry.get(job_name)["func"]
    session = _mock_session()
    await func(session)

    session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Non-PostgreSQL dialect path: should use SUM(CASE ...) rather than FILTER,
# except where the aggregation has no conditional counts (signal_event_count).
# ---------------------------------------------------------------------------


_CASE_JOBS = [
    "agg_phase_cycle",
    "agg_phase_pedestrian",
    "agg_preemption",
    "agg_priority",
    "agg_yellow_red_activation",
]


@pytest.mark.parametrize("job_name", _CASE_JOBS)
@pytest.mark.asyncio
async def test_job_mssql_uses_case_when(
    patched_agg_modules, job_name,
):
    """Non-PostgreSQL aggregates emit SUM(CASE ...) in the INSERT SQL."""
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="mssql",
        delete_sql="DELETE FROM target",
    )
    _reset_timescale_flags()

    func = JobRegistry.get(job_name)["func"]
    session = _mock_session()
    await func(session)

    assert session.execute.call_count == 2
    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    assert "CASE WHEN" in insert_sql, (
        f"{job_name}: MS-SQL INSERT should use CASE WHEN: {insert_sql[:300]}"
    )
    assert "FILTER" not in insert_sql, (
        f"{job_name}: MS-SQL INSERT must not use FILTER: {insert_sql[:300]}"
    )


# ---------------------------------------------------------------------------
# Left-turn gap bins — sanity check on the 11-bin column list.
# The job may compute the bins as approximations (delegated to a helper),
# but all 11 bin columns must appear in the INSERT SQL since the model
# declares them as NOT NULL with default 0.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_left_turn_gap_has_11_bins(patched_agg_modules):
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="postgresql",
        delete_sql="DELETE FROM phase_left_turn_gap_15min",
    )
    _reset_timescale_flags()

    func = JobRegistry.get("agg_phase_left_turn_gap")["func"]
    session = _mock_session()
    await func(session)

    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    for col in [
        "bin_1s", "bin_2s", "bin_3s", "bin_4s", "bin_5s",
        "bin_6s", "bin_7s", "bin_8s", "bin_9s", "bin_10s", "bin_10plus",
    ]:
        assert col in insert_sql, f"bin column {col} missing from INSERT SQL"


# ---------------------------------------------------------------------------
# Real-computation assertions — verify the jobs actually compute what their
# output columns advertise, not just emit schema-shaped INSERT statements.
# (See feedback_done_means_verified_end_to_end.md rule #6: "Every output
# column actually computes.")
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phase_cycle_computes_real_durations(patched_agg_modules):
    """phase_cycle SQL must use LEAD() window to compute green/yellow/red
    seconds — not emit hardcoded 0s."""
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="postgresql",
        delete_sql="DELETE FROM phase_cycle_15min",
    )
    _reset_timescale_flags()

    func = JobRegistry.get("agg_phase_cycle")["func"]
    session = _mock_session()
    await func(session)

    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    # Real duration: LEAD + EXTRACT(EPOCH FROM ...) on PostgreSQL.
    assert "LEAD(event_time)" in insert_sql, (
        "phase_cycle INSERT must use LEAD() to find the next state event"
    )
    assert "EXTRACT(EPOCH FROM" in insert_sql, (
        "phase_cycle INSERT must compute duration via EXTRACT(EPOCH FROM ...)"
    )
    # Guard against regression to the prior 'hardcoded 0 AS green_seconds' stub.
    assert "0 AS green_seconds" not in insert_sql
    assert "0 AS yellow_seconds" not in insert_sql
    assert "0 AS red_seconds" not in insert_sql


@pytest.mark.asyncio
async def test_phase_left_turn_gap_computes_real_bins(patched_agg_modules):
    """Left-turn gap SQL must pair consecutive detector-ON events via LAG(),
    scope to left-turn detectors, and emit 11 distinct bin ranges — not
    10 hardcoded zeros + a single gap-out count in bin_10plus."""
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="postgresql",
        delete_sql="DELETE FROM phase_left_turn_gap_15min",
    )
    _reset_timescale_flags()

    func = JobRegistry.get("agg_phase_left_turn_gap")["func"]
    session = _mock_session()
    await func(session)

    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    assert "LAG(cel.event_time)" in insert_sql, (
        "left-turn-gap must use LAG() to find previous detector-ON"
    )
    assert "movement_type" in insert_sql
    assert "abbreviation = 'L'" in insert_sql, (
        "left-turn-gap must filter to left-turn movement type"
    )
    # Each of the 10 short bins must have its own range, not a hardcoded 0.
    for n in range(1, 11):
        lower = n - 1
        upper = n
        assert f">= {lower} AND" in insert_sql, (
            f"bin_{n}s range >= {lower} missing"
        )
        assert f"< {upper} " in insert_sql or f"< {upper}\n" in insert_sql, (
            f"bin_{n}s range < {upper} missing"
        )
    # bin_10plus has a different shape: >= 10 only.
    assert "bin_10plus" in insert_sql


@pytest.mark.asyncio
async def test_phase_pedestrian_computes_real_delay(patched_agg_modules):
    """Ped SQL must pair each PED_CALL with the next PED_WALK via a
    correlated MIN subquery — not hardcode ped_delay_sum_seconds = 0."""
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="postgresql",
        delete_sql="DELETE FROM phase_pedestrian_15min",
    )
    _reset_timescale_flags()

    func = JobRegistry.get("agg_phase_pedestrian")["func"]
    session = _mock_session()
    await func(session)

    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    assert "SELECT MIN(w.event_time)" in insert_sql, (
        "ped aggregation must find next walk via correlated MIN"
    )
    assert "0 AS ped_delay_sum_seconds" not in insert_sql
    assert "0 AS ped_delay_count" not in insert_sql
    # The delay sum should be computed from the paired walk_time.
    assert "EXTRACT(EPOCH FROM" in insert_sql


@pytest.mark.asyncio
async def test_yellow_red_activation_computes_interval_hits(patched_agg_modules):
    """Yellow/red activation SQL must count detector-ON events that fall
    INSIDE a yellow or red interval — not just count the phase-transition
    events themselves."""
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="postgresql",
        delete_sql="DELETE FROM yellow_red_activation_15min",
    )
    _reset_timescale_flags()

    func = JobRegistry.get("agg_yellow_red_activation")["func"]
    session = _mock_session()
    await func(session)

    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    assert "LEAD(event_time)" in insert_sql, (
        "yellow/red activation must build intervals via LEAD()"
    )
    # The hit count must be a correlated COUNT of code-82 detectors in-interval.
    assert "SELECT COUNT(*)" in insert_sql
    assert "h.event_time >= iv.interval_start" in insert_sql
    assert "h.event_time < iv.interval_end" in insert_sql
    # red_duration_sum_seconds must compute, not be hardcoded 0.
    assert "0 AS red_duration_sum_seconds" not in insert_sql


@pytest.mark.asyncio
async def test_preemption_computes_real_delay(patched_agg_modules):
    """Preemption SQL must pair each code-102 request with its next
    code-105 service via correlated MIN, and compute mean_delay_seconds
    as delay_sum / paired_count — not hardcode zero."""
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="postgresql",
        delete_sql="DELETE FROM preemption_15min",
    )
    _reset_timescale_flags()

    func = JobRegistry.get("agg_preemption")["func"]
    session = _mock_session()
    await func(session)

    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    assert "SELECT MIN(s.event_time)" in insert_sql, (
        "preemption aggregation must find next 105 via correlated MIN"
    )
    assert "0 AS mean_delay_seconds" not in insert_sql
    # delay_sum / paired_count computation must be present.
    assert "delay_sum / d.paired_count" in insert_sql or (
        "d.delay_sum / d.paired_count" in insert_sql
    )


# ---------------------------------------------------------------------------
# Signal event count — no phase column in the target table.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_signal_event_count_no_phase_column(patched_agg_modules):
    mock_settings, mock_facade = patched_agg_modules
    _configure_mocks(
        mock_settings, mock_facade, db_type="postgresql",
        delete_sql="DELETE FROM signal_event_count_15min",
    )
    _reset_timescale_flags()

    func = JobRegistry.get("agg_signal_event_count")["func"]
    session = _mock_session()
    await func(session)

    insert_sql = str(session.execute.call_args_list[1][0][0].text)
    # Columns in the INSERT list should not include 'phase'.
    # Match the start of the INSERT column list.
    assert "INSERT INTO signal_event_count_15min" in insert_sql
    header = insert_sql.split("SELECT", 1)[0]
    assert " phase" not in header.lower()
    assert "event_count" in insert_sql


# ---------------------------------------------------------------------------
# agg_approach_speed — dedicated tests, since it sources from roadside_event
# and has dialect-specific SQL shapes that don't fit the generic matrix.
# ---------------------------------------------------------------------------


class TestApproachSpeedSQL:
    """agg_approach_speed emits dialect-correct percentile SQL against
    roadside_event, not controller_event_log."""

    @pytest.mark.asyncio
    async def test_postgresql_uses_percentile_cont_aggregate(
        self, patched_agg_modules,
    ):
        mock_settings, mock_facade = patched_agg_modules
        _configure_mocks(
            mock_settings, mock_facade, db_type="postgresql",
            delete_sql="DELETE FROM approach_speed_15min",
        )
        _reset_timescale_flags()

        func = JobRegistry.get("agg_approach_speed")["func"]
        session = _mock_session()
        await func(session)

        assert session.execute.call_count == 2
        insert_sql = str(session.execute.call_args_list[1][0][0].text)
        assert "INSERT INTO approach_speed_15min" in insert_sql
        assert "roadside_event" in insert_sql
        assert "roadside_sensor_lane" in insert_sql
        # Controller table must NOT appear — the rewrite dropped that branch.
        assert "controller_event_log" not in insert_sql
        # event_type = SPEED (1), not controller event code 82.
        assert "event_type = 1" in insert_sql
        assert "re.mph" in insert_sql
        # Real percentile computation — not zero placeholders.
        assert "PERCENTILE_CONT(0.15)" in insert_sql
        assert "PERCENTILE_CONT(0.50)" in insert_sql
        assert "PERCENTILE_CONT(0.85)" in insert_sql
        assert "WITHIN GROUP" in insert_sql

    @pytest.mark.asyncio
    async def test_oracle_uses_percentile_cont_aggregate(
        self, patched_agg_modules,
    ):
        mock_settings, mock_facade = patched_agg_modules
        _configure_mocks(
            mock_settings, mock_facade, db_type="oracle",
            delete_sql="DELETE FROM approach_speed_15min",
        )
        _reset_timescale_flags()

        func = JobRegistry.get("agg_approach_speed")["func"]
        session = _mock_session()
        await func(session)

        insert_sql = str(session.execute.call_args_list[1][0][0].text)
        assert "roadside_event" in insert_sql
        assert "PERCENTILE_CONT(0.15)" in insert_sql
        assert "WITHIN GROUP" in insert_sql
        assert "NVL" in insert_sql

    @pytest.mark.asyncio
    async def test_mssql_uses_percentile_cont_window(
        self, patched_agg_modules,
    ):
        mock_settings, mock_facade = patched_agg_modules
        _configure_mocks(
            mock_settings, mock_facade, db_type="mssql",
            delete_sql="DELETE FROM approach_speed_15min",
        )
        _reset_timescale_flags()

        func = JobRegistry.get("agg_approach_speed")["func"]
        session = _mock_session()
        await func(session)

        insert_sql = str(session.execute.call_args_list[1][0][0].text)
        assert "roadside_event" in insert_sql
        assert "PERCENTILE_CONT(0.15)" in insert_sql
        # Window form — PERCENTILE_CONT used with OVER(PARTITION BY ...).
        assert "OVER" in insert_sql
        assert "PARTITION BY" in insert_sql
        assert "SELECT DISTINCT" in insert_sql
        assert "ISNULL" in insert_sql

    @pytest.mark.asyncio
    async def test_mysql_uses_row_number_emulation(
        self, patched_agg_modules,
    ):
        mock_settings, mock_facade = patched_agg_modules
        _configure_mocks(
            mock_settings, mock_facade, db_type="mysql",
            delete_sql="DELETE FROM approach_speed_15min",
        )
        _reset_timescale_flags()

        func = JobRegistry.get("agg_approach_speed")["func"]
        session = _mock_session()
        await func(session)

        insert_sql = str(session.execute.call_args_list[1][0][0].text)
        assert "roadside_event" in insert_sql
        # MySQL has no PERCENTILE_CONT — must use ROW_NUMBER nearest-rank.
        assert "PERCENTILE_CONT" not in insert_sql
        assert "ROW_NUMBER" in insert_sql
        assert "CEIL(cnt * 0.15)" in insert_sql
        assert "CEIL(cnt * 0.50)" in insert_sql
        assert "CEIL(cnt * 0.85)" in insert_sql

    @pytest.mark.asyncio
    async def test_skips_when_disabled(self, patched_agg_modules):
        mock_settings, _ = patched_agg_modules
        mock_settings.aggregation_enabled = False
        _reset_timescale_flags()

        func = JobRegistry.get("agg_approach_speed")["func"]
        session = _mock_session()
        await func(session)
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_timescaledb_active(self, patched_agg_modules):
        mock_settings, mock_facade = patched_agg_modules
        mock_settings.aggregation_enabled = True
        mock_facade.has_timescaledb = AsyncMock(return_value=True)
        _reset_timescale_flags()

        func = JobRegistry.get("agg_approach_speed")["func"]
        session = _mock_session()
        await func(session)
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Model import sanity — ensures tables are registered with Base.metadata
# so create_all() in integration tests picks them up.
# ---------------------------------------------------------------------------


def test_new_aggregate_models_importable():
    from tsigma.models import aggregates_phase, aggregates_signal  # noqa: F401
    from tsigma.models.aggregates_phase import (
        ApproachSpeed15Min,
        PhaseCycle15Min,
        PhaseLeftTurnGap15Min,
        PhasePedestrian15Min,
        Priority15Min,
        YellowRedActivation15Min,
    )
    from tsigma.models.aggregates_signal import (
        Preemption15Min,
        SignalEventCount15Min,
    )

    expected = {
        "approach_speed_15min": ApproachSpeed15Min,
        "phase_cycle_15min": PhaseCycle15Min,
        "phase_left_turn_gap_15min": PhaseLeftTurnGap15Min,
        "phase_pedestrian_15min": PhasePedestrian15Min,
        "priority_15min": Priority15Min,
        "yellow_red_activation_15min": YellowRedActivation15Min,
        "preemption_15min": Preemption15Min,
        "signal_event_count_15min": SignalEventCount15Min,
    }
    for tname, cls in expected.items():
        assert cls.__tablename__ == tname
