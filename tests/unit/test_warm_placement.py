"""
Unit tests for hot/warm/cold tiering - Phase A (PLACEMENT, SQL-gen only).

These tests pin the GENERATED SQL STRINGS for moving an old partition to a
cheaper "warm" tablespace / filegroup, plus the new config defaults and the
``move_to_warm`` scheduled job's registration + SQL-gen contract.

No database is touched - the DialectHelper methods are pure functions of
``db_type`` and the job tests mock the session + settings (mirroring the
existing ``manage_partitions`` / ``compress_chunks`` job unit tests).

Live per-engine behavior is validated post-boot; here we assert only the
stable tokens / schema-qualified identifiers each dialect emits.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.config import Settings
from tsigma.database.db import DialectHelper

# ---------------------------------------------------------------------------
# PART 1 - DialectHelper.move_partition_tablespace_sql
# ---------------------------------------------------------------------------


class TestMovePartitionTablespacePostgresql:
    """PostgreSQL emits ALTER TABLE ... SET TABLESPACE on the schema-qualified
    partition child table (mirroring drop_partition_sql's pg naming)."""

    def test_returns_single_set_tablespace_statement(self):
        helper = DialectHelper("postgresql")
        stmts = helper.move_partition_tablespace_sql(
            "controller_event_log", "p_20260423", "warm_ts",
        )
        assert len(stmts) == 1
        assert "ALTER TABLE" in stmts[0]
        assert "SET TABLESPACE" in stmts[0]
        assert "warm_ts" in stmts[0]

    def test_uses_same_schema_qualified_partition_identifier_as_drop(self):
        """The partition table identifier must match what drop_partition_sql
        targets: ``events.<table>_<partition_name>``."""
        helper = DialectHelper("postgresql")
        # Anchor on the exact identifier drop_partition_sql would emit.
        drop = helper.drop_partition_sql("controller_event_log", "p_20260423")
        assert drop == [
            "DROP TABLE IF EXISTS events.controller_event_log_p_20260423"
        ]
        stmts = helper.move_partition_tablespace_sql(
            "controller_event_log", "p_20260423", "warm_ts",
        )
        assert "events.controller_event_log_p_20260423" in stmts[0]


class TestMovePartitionTablespaceOracle:
    """Oracle emits ALTER TABLE <table> MOVE PARTITION <name> TABLESPACE <target>."""

    def test_move_partition_tokens(self):
        helper = DialectHelper("oracle")
        stmts = helper.move_partition_tablespace_sql(
            "controller_event_log", "P_20260101", "WARM_TS",
        )
        assert len(stmts) == 1
        assert "ALTER TABLE controller_event_log" in stmts[0]
        assert "MOVE PARTITION P_20260101" in stmts[0]
        assert "TABLESPACE WARM_TS" in stmts[0]


class TestMovePartitionTablespaceMssql:
    """MS-SQL is a best-effort index-rebuild onto a target filegroup.

    The exact DDL is validated live later, so assert only the stable tokens:
    an ALTER/REBUILD statement that references the target filegroup.
    """

    def test_rebuild_onto_filegroup_tokens(self):
        helper = DialectHelper("mssql")
        stmts = helper.move_partition_tablespace_sql(
            "controller_event_log", "p_20260423", "WARM_FG",
        )
        assert len(stmts) >= 1
        combined = " ".join(stmts)
        assert "ALTER" in combined
        assert "REBUILD" in combined
        assert "WARM_FG" in combined


class TestMovePartitionTablespaceMysql:
    """MySQL placement is at partition-creation via DATA DIRECTORY (deferred);
    move is a no-op here."""

    def test_returns_empty_list(self):
        helper = DialectHelper("mysql")
        stmts = helper.move_partition_tablespace_sql(
            "controller_event_log", "p_20260101", "warm_dir",
        )
        assert stmts == []


class TestMovePartitionTablespaceValidatesIdentifiers:
    """move_partition_tablespace_sql rejects injection in table/target."""

    def test_bad_table_name_raises(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            DialectHelper("postgresql").move_partition_tablespace_sql(
                "events; DROP TABLE x", "p_20260423", "warm_ts",
            )

    def test_bad_target_name_raises(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            DialectHelper("oracle").move_partition_tablespace_sql(
                "controller_event_log", "P_20260101", "warm'; DROP--",
            )


# ---------------------------------------------------------------------------
# PART 2 - Settings warm-placement config defaults
# ---------------------------------------------------------------------------


class TestWarmPlacementSettings:
    """Fresh Settings() carries the warm-placement defaults (opt-in, unset)."""

    def test_warm_placement_enabled_defaults_false(self):
        assert Settings().warm_placement_enabled is False

    def test_warm_tablespace_target_defaults_empty(self):
        assert Settings().warm_tablespace_target == ""


# ---------------------------------------------------------------------------
# PART 3 - move_to_warm scheduled job
# ---------------------------------------------------------------------------


def _mock_rows(rows: list[tuple[str, str]]) -> MagicMock:
    """Build a MagicMock ``execute`` result whose ``.all()`` returns rows with
    ``partition_name`` / ``boundary_iso`` attributes (mirrors manage_partitions
    test helper)."""
    row_mocks = []
    for name, boundary in rows:
        r = MagicMock()
        r.partition_name = name
        r.boundary_iso = boundary
        row_mocks.append(r)
    result = MagicMock()
    result.all.return_value = row_mocks
    return result


class TestMoveToWarmRegistration:
    """move_to_warm is registered in the JobRegistry under its name."""

    def test_registered(self):
        # Importing the jobs package triggers auto-registration of all jobs.
        import tsigma.scheduler.jobs  # noqa: F401
        from tsigma.scheduler.registry import JobRegistry

        reg = JobRegistry.get("move_to_warm")
        assert reg is not None
        assert callable(reg["func"])


class TestMoveToWarmGuard:
    """When warm_placement_enabled is False the job no-ops (no move SQL)."""

    @pytest.mark.asyncio
    async def test_disabled_does_not_execute_moves(self):
        from tests._helpers import make_mock_session
        from tsigma.scheduler.jobs import move_to_warm as mtw

        session = make_mock_session()
        session.execute = AsyncMock()

        with patch.object(mtw.settings, "warm_placement_enabled", False):
            await mtw.move_to_warm(session)

        session.execute.assert_not_called()


class TestMoveToWarmEnabled:
    """Enabled on a non-PG dialect with a target + one eligible old partition
    generates the move SQL via move_partition_tablespace_sql and executes it."""

    @pytest.mark.asyncio
    async def test_oracle_moves_one_eligible_partition(self):
        from datetime import date, timedelta

        from tests._helpers import make_mock_session
        from tsigma.scheduler.jobs import move_to_warm as mtw

        helper = DialectHelper("oracle")
        old = date.today() - timedelta(days=60)
        old_name = helper.partition_name(old, 1)
        # Oracle high_value as a TO_DATE literal, as list_partitions_sql yields.
        boundary = (
            "TO_DATE(' "
            + old.isoformat()
            + " 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"
        )

        session = make_mock_session()
        # First execute = list_partitions; subsequent = MOVE PARTITION DDL.
        session.execute = AsyncMock(side_effect=[
            _mock_rows([(old_name, boundary)]),
            MagicMock(),
        ])

        with patch.object(mtw.settings, "warm_placement_enabled", True), \
             patch.object(mtw.settings, "warm_tablespace_target", "WARM_TS"), \
             patch.object(mtw.settings, "db_type", "oracle"), \
             patch.object(mtw.settings, "storage_warm_after", "7 days"):
            await mtw.move_to_warm(session)

        sqls = [str(c.args[0]) for c in session.execute.call_args_list]
        assert any("MOVE PARTITION" in s and "WARM_TS" in s for s in sqls)


# ---------------------------------------------------------------------------
# PART 3 - adversarial-review regressions
# ---------------------------------------------------------------------------


class TestMovePartitionTablespaceMssqlNumericName:
    """MS-SQL partition names from list_partitions_sql are NUMERIC strings
    (CAST(boundary_id AS NVARCHAR), e.g. "3"). The mssql branch rebuilds by
    boundary_id and does NOT interpolate partition_name into an identifier
    position, so a numeric name must NOT be rejected by _validate_identifier."""

    def test_numeric_partition_name_does_not_raise(self):
        helper = DialectHelper("mssql")
        stmts = helper.move_partition_tablespace_sql(
            "controller_event_log", "3", "WARM_FG",
        )
        assert stmts  # non-empty
        combined = " ".join(stmts)
        assert "WARM_FG" in combined


class TestMoveToWarmMssqlNumericPartition:
    """The MS-SQL move path is not dead: a numeric partition_name from
    list_partitions_sql must still produce an executed move (REBUILD onto the
    target filegroup), not be silently swallowed by per-partition error
    handling."""

    @pytest.mark.asyncio
    async def test_mssql_moves_numeric_named_partition(self):
        from datetime import date, timedelta

        from tests._helpers import make_mock_session
        from tsigma.scheduler.jobs import move_to_warm as mtw

        old = date.today() - timedelta(days=60)
        # MS-SQL list_partitions_sql yields numeric names + ISO datetime boundary.
        boundary = old.isoformat() + "T00:00:00"

        session = make_mock_session()
        # First execute = list_partitions; subsequent = REBUILD move DDL.
        # Default AsyncMock return for any further calls keeps the mock from
        # raising StopIteration if the implementation issues extra statements.
        session.execute = AsyncMock(side_effect=[
            _mock_rows([("3", boundary)]),
            MagicMock(),
            MagicMock(),
        ])

        with patch.object(mtw, "_MANAGED_TABLES", ("controller_event_log",)), \
             patch.object(mtw.settings, "warm_placement_enabled", True), \
             patch.object(mtw.settings, "warm_tablespace_target", "WARM_FG"), \
             patch.object(mtw.settings, "db_type", "mssql"), \
             patch.object(mtw.settings, "storage_warm_after", "7 days"):
            await mtw.move_to_warm(session)

        sqls = [str(c.args[0]) for c in session.execute.call_args_list]
        assert any("REBUILD" in s and "WARM_FG" in s for s in sqls)


class TestMoveToWarmMalformedWindowIsConservative:
    """A malformed ``storage_warm_after`` must NOT collapse the cutoff to today
    and demote every historical partition. An unparseable window must fall back
    to a conservative (non-zero) default so a recent partition is left in place."""

    @pytest.mark.asyncio
    async def test_garbage_window_does_not_demote_recent_partition(self):
        from datetime import date, timedelta

        from tests._helpers import make_mock_session
        from tsigma.scheduler.jobs import move_to_warm as mtw

        # Only 2 days old - newer than the intended 7-day default window.
        recent = date.today() - timedelta(days=2)
        helper = DialectHelper("oracle")
        recent_name = helper.partition_name(recent, 1)
        boundary = (
            "TO_DATE(' "
            + recent.isoformat()
            + " 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"
        )

        session = make_mock_session()
        session.execute = AsyncMock(side_effect=[
            _mock_rows([(recent_name, boundary)]),
            MagicMock(),
        ])

        with patch.object(mtw, "_MANAGED_TABLES", ("controller_event_log",)), \
             patch.object(mtw.settings, "warm_placement_enabled", True), \
             patch.object(mtw.settings, "warm_tablespace_target", "WARM_TS"), \
             patch.object(mtw.settings, "db_type", "oracle"), \
             patch.object(mtw.settings, "storage_warm_after", "garbage"):
            await mtw.move_to_warm(session)

        sqls = [str(c.args[0]) for c in session.execute.call_args_list]
        assert not any("MOVE PARTITION" in s for s in sqls)
