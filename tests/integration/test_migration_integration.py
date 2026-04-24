"""Alembic migration round-trip coverage.

Exercises ``upgrade head`` → ``downgrade base`` → ``upgrade head``
against every supported dialect.  The round-trip catches asymmetric
cleanup in the initial-schema migration (partition-function leftovers
on MS-SQL, TimescaleDB hypertable double-create on PostgreSQL, Oracle
schema-owner reuse, MySQL foreign-key ordering, etc.).

``dialect_engine`` is deliberately NOT used here: that fixture has
already run ``upgrade head`` before the test body sees it.  These
tests drive Alembic themselves via the sync URL.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, inspect

from tests.integration.conftest import (
    _run_alembic_downgrade,
    _run_alembic_upgrade,
)

pytestmark = pytest.mark.integration


# The tables we expect any clean ``upgrade head`` to create.  A subset
# of the full schema — enough to prove the migration ran end-to-end on
# every dialect without enumerating every reference table.
_EXPECTED_TABLES = (
    "controller_event_log",
    "roadside_event",
    "polling_checkpoint",
    "signal",
    "roadside_sensor",
    "roadside_sensor_vendor",
    "roadside_sensor_model",
)


def test_upgrade_then_downgrade_clean(
    dialect_sync_url: str, dialect_name: str,
) -> None:
    """upgrade → downgrade → upgrade must complete without exceptions.

    Leaves the database at ``head`` so the conftest teardown can run
    its own final downgrade without surprises.
    """
    _run_alembic_upgrade(dialect_sync_url)
    try:
        _run_alembic_downgrade(dialect_sync_url, "base")
    except Exception as exc:
        pytest.fail(
            f"{dialect_name}: downgrade after first upgrade failed: {exc!r}"
        )

    try:
        _run_alembic_upgrade(dialect_sync_url)
    except Exception as exc:
        pytest.fail(
            f"{dialect_name}: second upgrade after downgrade failed: {exc!r}"
        )


def test_upgrade_creates_expected_tables(
    dialect_sync_url: str, dialect_name: str,
) -> None:
    """After ``upgrade head`` the key tables must be visible via inspect.

    Uses ``sqlalchemy.inspect(engine).has_table`` against every logical
    schema the dialect exposes.  MySQL has no schemas so ``schema=None``;
    everything else may place tables in ``events`` / ``config`` /
    ``aggregation`` / ``identity``.  Any schema matching counts as
    present — ``has_table`` returns true if the table is in ANY of the
    probed schemas.
    """
    _run_alembic_upgrade(dialect_sync_url)

    engine = create_engine(dialect_sync_url)
    try:
        inspector = inspect(engine)
        schemas_to_probe: list[str | None]
        if dialect_name == "mysql":
            schemas_to_probe = [None]
        else:
            schemas_to_probe = [
                None, "events", "config", "aggregation", "identity",
            ]

        missing: list[str] = []
        for table in _EXPECTED_TABLES:
            found = False
            for schema in schemas_to_probe:
                try:
                    if inspector.has_table(table, schema=schema):
                        found = True
                        break
                except Exception:
                    continue
            if not found:
                missing.append(table)

        assert not missing, (
            f"{dialect_name}: expected tables not present after upgrade: "
            f"{missing}"
        )
    finally:
        engine.dispose()
