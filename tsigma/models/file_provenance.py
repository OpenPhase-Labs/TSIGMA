"""
Per-file ingest provenance history (ASC/3 integrity program, Inc-2).

A ``FileIngestProvenance`` row records the header metadata of every
ingested file — source filename, the controller's reported network
identity (IP / MAC), log version, phases in use, the log's begin time and
its header time anchor, and which decoder produced the row — so the origin
and integrity of each ingested file can be audited after the fact.

This is an append-only history table: one row is inserted per ingested
file. It carries its own ``ingested_at`` timestamp (no TimestampMixin).
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import Index, Text, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, schema_fk, tsigma_schema


class FileIngestProvenance(Base):
    """
    Append-only provenance row for a single ingested file.

    ``device_ip`` is stored as raw Text (not INET) because it is verbatim
    header provenance — the value the controller reported in the file,
    preserved exactly as received rather than normalized.
    """

    __tablename__ = "file_ingest_provenance"

    provenance_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    signal_id: Mapped[str] = mapped_column(
        Text,
        schema_fk("config", "signal.signal_id"),
        nullable=False,
    )
    source_filename: Mapped[Optional[str]] = mapped_column(Text)
    device_ip: Mapped[Optional[str]] = mapped_column(Text)
    device_mac: Mapped[Optional[str]] = mapped_column(Text)
    log_version: Mapped[Optional[str]] = mapped_column(Text)
    phases_in_use: Mapped[Optional[dict]] = mapped_column(JSONB)
    log_begin: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    header_anchor: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    decoder_name: Mapped[Optional[str]] = mapped_column(Text)
    ingested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_file_provenance_signal", "signal_id"),
        {"schema": tsigma_schema("config")},
    )
