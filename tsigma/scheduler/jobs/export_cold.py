"""
Cold-tier data export job.

Exports aged event data to Parquet files on local storage for long-term
archival. Runs weekly and writes one Parquet file per signal per date.
Exported partitions are logged but NOT auto-dropped — manual cleanup
is expected after verifying archive integrity.
"""

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)


@JobRegistry.register(
    name="export_cold",
    trigger="cron",
    day_of_week="sun",
    hour="2",
    minute="0",
)
async def export_cold(session: AsyncSession) -> None:
    """Export old event data to Parquet files in the cold-storage directory."""
    if not settings.storage_cold_enabled:
        logger.debug("Skipping export_cold — cold storage is disabled")
        return

    cold_root = Path(settings.storage_cold_path)

    try:
        # Fetch events older than the cold-storage threshold
        result = await session.execute(
            text("""
                SELECT signal_id, event_time, event_code, event_param, device_id
                FROM controller_event_log
                WHERE event_time < now() - :cold_interval ::interval
                ORDER BY signal_id, event_time
            """),
            {"cold_interval": settings.storage_cold_after},
        )
        rows = result.all()

        if not rows:
            logger.info("No data eligible for cold export")
            return

        df = pd.DataFrame(
            [dict(r._mapping) for r in rows],
            columns=["signal_id", "event_time", "event_code", "event_param", "device_id"],
        )

        total_exported = 0

        # Group by signal_id and date, write one Parquet per group
        df["event_date"] = pd.to_datetime(df["event_time"]).dt.date
        for (signal_id, event_date), group in df.groupby(["signal_id", "event_date"]):
            out_dir = cold_root / str(signal_id) / str(event_date)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / "events.parquet"

            group.drop(columns=["event_date"]).to_parquet(out_path, index=False)
            total_exported += len(group)
            logger.info("Exported %d events to %s", len(group), out_path)

        logger.info(
            "Cold export complete: %d events across %d signals written to %s",
            total_exported,
            df["signal_id"].nunique(),
            cold_root,
        )
        logger.info(
            "Exported data covers events before now() - %s. "
            "Manual partition drop is recommended after verifying archive integrity.",
            settings.storage_cold_after,
        )

    except Exception:
        logger.exception("Cold export job failed")
        raise
