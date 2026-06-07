"""
General needs-review / correction worklist (ASC/3 integrity program, Inc-3).

An ``IngestReview`` row flags an ingested artifact that needs human (or
downstream) attention — a decode anomaly, an integrity discrepancy, a
correction candidate, and so on. It is a general-purpose worklist reusable
across decoders, not tied to any single file format.

The logical key of a review item is ``signal_id`` + ``reason`` +
``source_filename``. ``provenance_id`` is an optional link to the file
audit row (``file_ingest_provenance``) when one exists; it is left null for
reviews that do not originate from a single audited file. The table carries
its own ``detected_at`` timestamp (no TimestampMixin).
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import Index, Text, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, schema_fk, tsigma_schema


class IngestReview(Base):
    """
    A single needs-review item raised during or after ingest.

    Reusable across decoders: the ``reason`` text and optional ``detail``
    JSONB carry decoder-specific context, while the common columns
    (``signal_id``, ``source_filename``, ``severity``, ``status``) give a
    uniform worklist. ``provenance_id`` links to the file audit row when the
    review originates from a single audited file.
    """

    __tablename__ = "ingest_review"

    review_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    signal_id: Mapped[str] = mapped_column(
        Text,
        schema_fk("config", "signal.signal_id"),
        nullable=False,
    )
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    source_filename: Mapped[Optional[str]] = mapped_column(Text)
    provenance_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        schema_fk("config", "file_ingest_provenance.provenance_id"),
    )
    severity: Mapped[Optional[str]] = mapped_column(Text)
    summary: Mapped[Optional[str]] = mapped_column(Text)
    detail: Mapped[Optional[dict]] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="open",
    )
    detected_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    resolved_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True)
    )
    resolved_by: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        Index("idx_ingest_review_signal_status", "signal_id", "status"),
        {"schema": tsigma_schema("config")},
    )
