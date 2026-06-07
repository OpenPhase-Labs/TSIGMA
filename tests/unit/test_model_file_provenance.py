"""
Unit tests for the FileIngestProvenance model.

Tests table name, schema placement, column presence, types, and
nullability for the history table that records the header metadata of
every ingested file (Inc-2 of the ASC/3 integrity program).

Pure in-process model introspection — no database, no network. Mirrors
the style of test_system_setting_audit_model.py and
test_event_model_validation.py: schema is asserted via the
``__table_args__`` schema dict, types are asserted with ``isinstance``
(or loosely via ``str(col.type)`` for the dialect-specific JSONB type),
and the index is asserted via ``__table__.indexes``.
"""

from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from tsigma.models.base import tsigma_schema
from tsigma.models.file_provenance import FileIngestProvenance


class TestFileIngestProvenanceModel:
    """Tests for FileIngestProvenance ORM model."""

    def test_tablename(self):
        """Test the table name is correct."""
        assert FileIngestProvenance.__tablename__ == "file_ingest_provenance"

    def test_schema_is_config(self):
        """Test the table is placed in the config logical schema.

        ``__table_args__`` is a tuple ``(Index(...), {"schema": ...})`` to
        combine the index declaration with the schema kwarg — the standard
        SQLAlchemy idiom (mirrors SystemSettingAudit). The schema dict is
        always the last element of the tuple.
        """
        table_args = FileIngestProvenance.__table_args__
        assert isinstance(table_args, tuple), (
            "Expected __table_args__ to be a tuple (Index(...), {schema...})"
        )
        schema_kwargs = table_args[-1]
        assert isinstance(schema_kwargs, dict)
        assert schema_kwargs["schema"] == tsigma_schema("config")

    def test_primary_key_is_provenance_id(self):
        """Test that 'provenance_id' is the primary key column."""
        pk_cols = [c.name for c in FileIngestProvenance.__table__.primary_key.columns]
        assert pk_cols == ["provenance_id"]

    def test_provenance_id_is_uuid_pk(self):
        """Test provenance_id column is a UUID primary key."""
        col = FileIngestProvenance.__table__.columns["provenance_id"]
        assert isinstance(col.type, PG_UUID)
        assert col.primary_key is True

    def test_provenance_id_has_server_default(self):
        """provenance_id must carry a server-side default (gen_random_uuid)."""
        col = FileIngestProvenance.__table__.columns["provenance_id"]
        assert col.server_default is not None

    def test_signal_id_is_text_not_null(self):
        """Test signal_id column is Text and NOT NULL."""
        col = FileIngestProvenance.__table__.columns["signal_id"]
        assert isinstance(col.type, Text)
        assert col.nullable is False

    def test_signal_id_foreign_key_targets_signal(self):
        """signal_id must be a FK to signal.signal_id."""
        col = FileIngestProvenance.__table__.columns["signal_id"]
        targets = {fk.target_fullname for fk in col.foreign_keys}
        assert any(t.endswith("signal.signal_id") for t in targets), (
            f"signal_id missing FK to signal.signal_id; found {targets}"
        )

    def test_source_filename_is_text_nullable(self):
        """Test source_filename column is Text and nullable."""
        col = FileIngestProvenance.__table__.columns["source_filename"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_device_ip_is_text_nullable(self):
        """Test device_ip column is Text and nullable."""
        col = FileIngestProvenance.__table__.columns["device_ip"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_device_mac_is_text_nullable(self):
        """Test device_mac column is Text and nullable."""
        col = FileIngestProvenance.__table__.columns["device_mac"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_log_version_is_text_nullable(self):
        """Test log_version column is Text and nullable."""
        col = FileIngestProvenance.__table__.columns["log_version"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_phases_in_use_is_jsonb_nullable(self):
        """Test phases_in_use column is JSONB and nullable."""
        col = FileIngestProvenance.__table__.columns["phases_in_use"]
        assert "JSONB" in str(col.type)
        assert col.nullable is True

    def test_log_begin_is_timestamptz_nullable(self):
        """Test log_begin column is timezone-aware timestamp and nullable."""
        col = FileIngestProvenance.__table__.columns["log_begin"]
        assert isinstance(col.type, TIMESTAMP)
        assert col.type.timezone is True
        assert col.nullable is True

    def test_header_anchor_is_timestamptz_nullable(self):
        """Test header_anchor column is timezone-aware timestamp and nullable."""
        col = FileIngestProvenance.__table__.columns["header_anchor"]
        assert isinstance(col.type, TIMESTAMP)
        assert col.type.timezone is True
        assert col.nullable is True

    def test_decoder_name_is_text_nullable(self):
        """Test decoder_name column is Text and nullable."""
        col = FileIngestProvenance.__table__.columns["decoder_name"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_ingested_at_is_timestamptz_not_null(self):
        """Test ingested_at column is timezone-aware timestamp and NOT NULL."""
        col = FileIngestProvenance.__table__.columns["ingested_at"]
        assert isinstance(col.type, TIMESTAMP)
        assert col.type.timezone is True
        assert col.nullable is False

    def test_ingested_at_has_server_default(self):
        """ingested_at must carry a server-side default (now())."""
        col = FileIngestProvenance.__table__.columns["ingested_at"]
        assert col.server_default is not None

    def test_signal_index_exists(self):
        """Test that the idx_file_provenance_signal index exists on signal_id."""
        indexes_by_name = {ix.name: ix for ix in FileIngestProvenance.__table__.indexes}
        assert "idx_file_provenance_signal" in indexes_by_name, (
            "Missing index idx_file_provenance_signal on signal_id. "
            f"Found: {sorted(indexes_by_name)}"
        )
        ix = indexes_by_name["idx_file_provenance_signal"]
        assert [c.name for c in ix.columns] == ["signal_id"]
