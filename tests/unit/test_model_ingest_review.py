"""
Unit tests for the IngestReview model.

Tests table name, schema placement, column presence, types, nullability,
foreign keys, server defaults, and the composite index for the general
needs-review / correction worklist table (Inc-3 of the ASC/3 integrity
program).

Pure in-process model introspection — no database, no network. Mirrors
the style of test_model_file_provenance.py: schema is asserted via the
``__table_args__`` schema dict, types are asserted with ``isinstance``
(or loosely via ``str(col.type)`` for the dialect-specific JSONB type),
foreign keys are asserted via ``col.foreign_keys`` target_fullname, and
the index is asserted via ``__table__.indexes``.
"""

from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from tsigma.models.base import tsigma_schema
from tsigma.models.ingest_review import IngestReview


class TestIngestReviewModel:
    """Tests for IngestReview ORM model."""

    def test_tablename(self):
        """Test the table name is correct."""
        assert IngestReview.__tablename__ == "ingest_review"

    def test_schema_is_config(self):
        """Test the table is placed in the config logical schema.

        ``__table_args__`` is a tuple ``(Index(...), {"schema": ...})`` to
        combine the index declaration with the schema kwarg — the standard
        SQLAlchemy idiom (mirrors FileIngestProvenance). The schema dict is
        always the last element of the tuple.
        """
        table_args = IngestReview.__table_args__
        assert isinstance(table_args, tuple), (
            "Expected __table_args__ to be a tuple (Index(...), {schema...})"
        )
        schema_kwargs = table_args[-1]
        assert isinstance(schema_kwargs, dict)
        assert schema_kwargs["schema"] == tsigma_schema("config")

    def test_primary_key_is_review_id(self):
        """Test that 'review_id' is the primary key column."""
        pk_cols = [c.name for c in IngestReview.__table__.primary_key.columns]
        assert pk_cols == ["review_id"]

    def test_review_id_is_uuid_pk(self):
        """Test review_id column is a UUID primary key."""
        col = IngestReview.__table__.columns["review_id"]
        assert isinstance(col.type, PG_UUID)
        assert col.primary_key is True

    def test_review_id_has_server_default(self):
        """review_id must carry a server-side default (gen_random_uuid)."""
        col = IngestReview.__table__.columns["review_id"]
        assert col.server_default is not None

    def test_signal_id_is_text_not_null(self):
        """Test signal_id column is Text and NOT NULL."""
        col = IngestReview.__table__.columns["signal_id"]
        assert isinstance(col.type, Text)
        assert col.nullable is False

    def test_signal_id_foreign_key_targets_signal(self):
        """signal_id must be a FK to signal.signal_id."""
        col = IngestReview.__table__.columns["signal_id"]
        targets = {fk.target_fullname for fk in col.foreign_keys}
        assert any(t.endswith("signal.signal_id") for t in targets), (
            f"signal_id missing FK to signal.signal_id; found {targets}"
        )

    def test_reason_is_text_not_null(self):
        """Test reason column is Text and NOT NULL."""
        col = IngestReview.__table__.columns["reason"]
        assert isinstance(col.type, Text)
        assert col.nullable is False

    def test_source_filename_is_text_nullable(self):
        """Test source_filename column is Text and nullable."""
        col = IngestReview.__table__.columns["source_filename"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_provenance_id_is_uuid_nullable(self):
        """Test provenance_id column is UUID and nullable."""
        col = IngestReview.__table__.columns["provenance_id"]
        assert isinstance(col.type, PG_UUID)
        assert col.nullable is True

    def test_provenance_id_foreign_key_targets_provenance(self):
        """provenance_id must be a FK to file_ingest_provenance.provenance_id."""
        col = IngestReview.__table__.columns["provenance_id"]
        targets = {fk.target_fullname for fk in col.foreign_keys}
        assert any(
            t.endswith("file_ingest_provenance.provenance_id") for t in targets
        ), (
            "provenance_id missing FK to file_ingest_provenance.provenance_id; "
            f"found {targets}"
        )

    def test_severity_is_text_nullable(self):
        """Test severity column is Text and nullable."""
        col = IngestReview.__table__.columns["severity"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_summary_is_text_nullable(self):
        """Test summary column is Text and nullable."""
        col = IngestReview.__table__.columns["summary"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_detail_is_jsonb_nullable(self):
        """Test detail column is JSONB and nullable."""
        col = IngestReview.__table__.columns["detail"]
        assert "JSONB" in str(col.type)
        assert col.nullable is True

    def test_status_is_text_not_null(self):
        """Test status column is Text and NOT NULL."""
        col = IngestReview.__table__.columns["status"]
        assert isinstance(col.type, Text)
        assert col.nullable is False

    def test_status_has_server_default(self):
        """status must carry a server-side default (the 'open' default)."""
        col = IngestReview.__table__.columns["status"]
        assert col.server_default is not None

    def test_detected_at_is_timestamptz_not_null(self):
        """Test detected_at column is timezone-aware timestamp and NOT NULL."""
        col = IngestReview.__table__.columns["detected_at"]
        assert isinstance(col.type, TIMESTAMP)
        assert col.type.timezone is True
        assert col.nullable is False

    def test_detected_at_has_server_default(self):
        """detected_at must carry a server-side default (now())."""
        col = IngestReview.__table__.columns["detected_at"]
        assert col.server_default is not None

    def test_resolved_at_is_timestamptz_nullable(self):
        """Test resolved_at column is timezone-aware timestamp and nullable."""
        col = IngestReview.__table__.columns["resolved_at"]
        assert isinstance(col.type, TIMESTAMP)
        assert col.type.timezone is True
        assert col.nullable is True

    def test_resolved_by_is_text_nullable(self):
        """Test resolved_by column is Text and nullable."""
        col = IngestReview.__table__.columns["resolved_by"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_signal_status_index_exists(self):
        """Test idx_ingest_review_signal_status index on (signal_id, status)."""
        indexes_by_name = {ix.name: ix for ix in IngestReview.__table__.indexes}
        assert "idx_ingest_review_signal_status" in indexes_by_name, (
            "Missing index idx_ingest_review_signal_status on (signal_id, status). "
            f"Found: {sorted(indexes_by_name)}"
        )
        ix = indexes_by_name["idx_ingest_review_signal_status"]
        assert [c.name for c in ix.columns] == ["signal_id", "status"]
