"""End-to-end integration test for the cold-tier export job.

Covers: insert events into the real DB → run ``export_cold`` → verify
Parquet partitions appear on disk and contain exactly the archived rows
→ verify the hot DB still has all rows (current master's
``export_cold`` does NOT delete archived rows; the docstring at
``tsigma/scheduler/jobs/export_cold.py:6-8`` notes manual cleanup is
expected after archive verification).

The fetch_events / aggregate_events tier-aware ROUTING logic is covered
by unit tests under ``tests/unit/database/test_cold_tier_*.py`` and
``tests/unit/test_fetch_events_tier_aware.py``.  This test focuses on
the EXPORT side of the tier — the write path from hot DB to Parquet —
where unit tests would be insufficient (interaction with a real DB
transaction, real DataFrame extraction via dialect-portable SQL, real
Parquet write to the configured cold path).

Dialect scope: all four supported dialects (postgresql, mssql, oracle,
mysql).  The SQL in ``export_cold`` uses a pre-computed ``:cutoff``
bound parameter that SQLAlchemy adapts per dialect.

Transaction note: we manage our own engine/sessions because
``dialect_session``'s rolled-back-transaction pattern interferes with
``export_cold`` reading rows through a separate session that needs to
see committed writes.  Same approach as ``test_collector_e2e.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from tsigma.config import settings
from tsigma.models.event import ControllerEventLog
from tsigma.scheduler.jobs.export_cold import export_cold

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_cold_tier_export_round_trip(
    dialect_async_url,
    dialect_engine,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Insert old + recent events → ``export_cold`` → verify Parquet on disk.

    The ``dialect_engine`` fixture migrates the schema (via Alembic)
    before the test runs; we then open our own connections against the
    same ``dialect_async_url`` because ``export_cold`` reads rows via
    its session and our verification reads happen on separate sessions.

    The recent events (1 day old) must NOT be archived — they're newer
    than ``storage.cold_after_days`` (30 in this test).  The old events
    (60 days old) must be archived.  Both groups remain in the hot DB
    because current ``export_cold`` does not delete after export.
    """
    # storage.cold_enabled and storage.cold_after_days are runtime-registry
    # keys — set the matching env-var overrides so settings_service.get_*
    # short-circuits the DB lookup.  storage_cold_path stays a Pydantic
    # config attribute (deploy-time, not runtime-tunable), so monkeypatch
    # it directly on the settings instance.
    monkeypatch.setenv("TSIGMA_STORAGE_COLD_ENABLED", "true")
    monkeypatch.setenv("TSIGMA_STORAGE_COLD_AFTER_DAYS", "30")
    monkeypatch.setattr(settings, "storage_cold_path", str(tmp_path))

    engine = create_async_engine(dialect_async_url)
    session_maker = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False,
    )

    signal_id = f"SIG-CT-{uuid4().hex[:8]}"
    old_time = datetime.now(timezone.utc) - timedelta(days=60)
    recent_time = datetime.now(timezone.utc) - timedelta(days=1)

    try:
        # Insert 100 archive-eligible (60 days old) + 50 hot-only (1 day old).
        async with session_maker() as session:
            for i in range(100):
                session.add(
                    ControllerEventLog(
                        signal_id=signal_id,
                        event_time=old_time + timedelta(seconds=i),
                        event_code=1,
                        event_param=0,
                    )
                )
            for i in range(50):
                session.add(
                    ControllerEventLog(
                        signal_id=signal_id,
                        event_time=recent_time + timedelta(seconds=i),
                        event_code=1,
                        event_param=0,
                    )
                )
            await session.commit()

        async with session_maker() as export_session:
            await export_cold(export_session)

        parquet_files = list(tmp_path.glob(f"{signal_id}/*/events.parquet"))
        assert len(parquet_files) >= 1, (
            f"Expected at least one Parquet partition under "
            f"{tmp_path}/{signal_id}/, found none"
        )

        total_archived = sum(len(pd.read_parquet(f)) for f in parquet_files)
        assert total_archived == 100, (
            f"Expected 100 archived rows across Parquet partitions, "
            f"got {total_archived}"
        )

        # Parquet schema sanity — canonical event columns must be present.
        sample = pd.read_parquet(parquet_files[0])
        expected_cols = {
            "signal_id", "event_time", "event_code", "event_param", "device_id",
        }
        assert expected_cols.issubset(set(sample.columns)), (
            f"Parquet missing canonical columns. Got: {list(sample.columns)}"
        )

        # Current export_cold does NOT delete archived rows from hot DB —
        # both groups (150 total) should still be present.
        async with session_maker() as verify_session:
            result = await verify_session.execute(
                select(func.count()).where(
                    ControllerEventLog.signal_id == signal_id,
                )
            )
            hot_count = result.scalar()

        assert hot_count == 150, (
            f"Expected all 150 rows still in hot DB (export_cold does not "
            f"delete on master per its docstring), got {hot_count}"
        )

    finally:
        async with session_maker() as cleanup_session:
            await cleanup_session.execute(
                text(
                    "DELETE FROM controller_event_log "
                    "WHERE signal_id = :sid"
                ),
                {"sid": signal_id},
            )
            await cleanup_session.commit()

        await engine.dispose()
