"""
Unit tests for the ControllerClockOffset model.

Tests table name, schema placement, column presence, types, nullability,
foreign keys, server defaults, and the composite index for the raw
per-ingest clock-offset trending table (Inc-4 of the ASC/3 integrity
program).

Pure in-process model introspection -- no database, no network. Mirrors
the style of test_model_file_provenance.py and test_model_ingest_review.py:
schema is asserted via the ``__table_args__`` schema dict, types are
asserted with ``isinstance``, foreign keys are asserted via
``col.foreign_keys`` target_fullname, and the index is asserted via
``__table__.indexes``.
"""

from sqlalchemy import Float, Integer, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from tsigma.models.base import tsigma_schema
from tsigma.models.controller_clock_offset import ControllerClockOffset


class TestControllerClockOffsetModel:
    """Tests for ControllerClockOffset ORM model."""

    def test_tablename(self):
        """Test the table name is correct."""
        assert ControllerClockOffset.__tablename__ == "controller_clock_offset"

    def test_schema_is_config(self):
        """Test the table is placed in the config logical schema.

        ``__table_args__`` is a tuple ``(Index(...), {"schema": ...})`` to
        combine the index declaration with the schema kwarg -- the standard
        SQLAlchemy idiom (mirrors FileIngestProvenance). The schema dict is
        always the last element of the tuple.
        """
        table_args = ControllerClockOffset.__table_args__
        assert isinstance(table_args, tuple), (
            "Expected __table_args__ to be a tuple (Index(...), {schema...})"
        )
        schema_kwargs = table_args[-1]
        assert isinstance(schema_kwargs, dict)
        assert schema_kwargs["schema"] == tsigma_schema("config")

    def test_primary_key_is_offset_id(self):
        """Test that 'offset_id' is the primary key column."""
        pk_cols = [c.name for c in ControllerClockOffset.__table__.primary_key.columns]
        assert pk_cols == ["offset_id"]

    def test_offset_id_is_uuid_pk(self):
        """Test offset_id column is a UUID primary key."""
        col = ControllerClockOffset.__table__.columns["offset_id"]
        assert isinstance(col.type, PG_UUID)
        assert col.primary_key is True

    def test_offset_id_has_server_default(self):
        """offset_id must carry a server-side default (gen_random_uuid)."""
        col = ControllerClockOffset.__table__.columns["offset_id"]
        assert col.server_default is not None

    def test_signal_id_is_text_not_null(self):
        """Test signal_id column is Text and NOT NULL."""
        col = ControllerClockOffset.__table__.columns["signal_id"]
        assert isinstance(col.type, Text)
        assert col.nullable is False

    def test_signal_id_foreign_key_targets_signal(self):
        """signal_id must be a FK to signal.signal_id."""
        col = ControllerClockOffset.__table__.columns["signal_id"]
        targets = {fk.target_fullname for fk in col.foreign_keys}
        assert any(t.endswith("signal.signal_id") for t in targets), (
            f"signal_id missing FK to signal.signal_id; found {targets}"
        )

    def test_offset_seconds_is_float_not_null(self):
        """Test offset_seconds column is float/double and NOT NULL.

        The repo uses ``sqlalchemy.Float`` for all double/ratio columns
        (see aggregates.py, e.g. occupancy_pct, avg_offset_error_seconds),
        so an ``isinstance(col.type, Float)`` check is the right assertion;
        a tolerant string fallback covers DOUBLE_PRECISION just in case.
        """
        col = ControllerClockOffset.__table__.columns["offset_seconds"]
        assert isinstance(col.type, Float) or (
            "FLOAT" in str(col.type).upper() or "DOUBLE" in str(col.type).upper()
        )
        assert col.nullable is False

    def test_event_count_is_integer_nullable(self):
        """Test event_count column is Integer and nullable."""
        col = ControllerClockOffset.__table__.columns["event_count"]
        assert isinstance(col.type, Integer)
        assert col.nullable is True

    def test_observed_at_is_timestamptz_not_null(self):
        """Test observed_at column is timezone-aware timestamp and NOT NULL."""
        col = ControllerClockOffset.__table__.columns["observed_at"]
        assert isinstance(col.type, TIMESTAMP)
        assert col.type.timezone is True
        assert col.nullable is False

    def test_observed_at_has_server_default(self):
        """observed_at must carry a server-side default (now())."""
        col = ControllerClockOffset.__table__.columns["observed_at"]
        assert col.server_default is not None

    def test_signal_observed_index_exists(self):
        """Test idx_clock_offset_signal_observed on (signal_id, observed_at)."""
        indexes_by_name = {
            ix.name: ix for ix in ControllerClockOffset.__table__.indexes
        }
        assert "idx_clock_offset_signal_observed" in indexes_by_name, (
            "Missing index idx_clock_offset_signal_observed on "
            "(signal_id, observed_at). "
            f"Found: {sorted(indexes_by_name)}"
        )
        ix = indexes_by_name["idx_clock_offset_signal_observed"]
        assert [c.name for c in ix.columns] == ["signal_id", "observed_at"]
