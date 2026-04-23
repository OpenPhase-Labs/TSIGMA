"""
TimescaleDB chunk compression job.

Scans for uncompressed hypertable chunks older than the warm-storage window
and triggers on-disk compression. Runs every 5 minutes so newly eligible
chunks are compressed promptly without a single large batch.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)


@JobRegistry.register(name="compress_chunks", trigger="interval", minutes=5)
async def compress_chunks(session: AsyncSession) -> None:
    """Compress TimescaleDB chunks older than the warm-storage threshold."""
    if settings.db_type != "postgresql":
        logger.debug("Skipping compress_chunks — not PostgreSQL (db_type=%s)", settings.db_type)
        return

    try:
        # Find uncompressed chunks older than the warm window
        result = await session.execute(
            text("""
                SELECT chunk_schema || '.' || chunk_name AS chunk_full_name
                FROM timescaledb_information.chunks
                WHERE is_compressed = false
                  AND range_end < now() - :warm_interval ::interval
                ORDER BY range_end ASC
            """),
            {"warm_interval": settings.storage_warm_after},
        )
        chunks = result.all()

        if not chunks:
            logger.debug("No chunks eligible for compression")
            return

        compressed = 0
        for row in chunks:
            chunk_name = row.chunk_full_name
            try:
                await session.execute(
                    text("SELECT compress_chunk(:chunk_name)"),
                    {"chunk_name": chunk_name},
                )
                compressed += 1
                logger.info("Compressed chunk: %s", chunk_name)
            except Exception:
                logger.exception("Failed to compress chunk: %s", chunk_name)

        logger.info("Chunk compression complete: %d/%d compressed", compressed, len(chunks))

    except Exception:
        logger.exception("Chunk compression job failed")
        raise
