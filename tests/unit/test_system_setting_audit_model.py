"""
Unit tests for the SystemSettingAudit model.

Tests model creation, column types, schema placement, nullability,
and server defaults for the audit-trail table that records every
successful PUT /api/v1/settings/{key} write.
"""

from sqlalchemy import BigInteger, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP

from tsigma.models.base import tsigma_schema
from tsigma.models.system_setting_audit import SystemSettingAudit


class TestSystemSettingAuditModel:
    """Tests for SystemSettingAudit ORM model."""

    def test_tablename(self):
        """Test the table name is correct."""
        assert SystemSettingAudit.__tablename__ == "system_setting_audit"

    def test_schema_is_identity(self):
        """Test the table is placed in the identity logical schema."""
        # __table_args__ is now a tuple (Index(...), {"schema": ...}) to
        # combine the composite-index declaration with the schema kwarg —
        # the standard SQLAlchemy idiom (mirrors AuthAuditLog in
        # tsigma/models/audit.py). The schema dict is always the last
        # element of the tuple.
        table_args = SystemSettingAudit.__table_args__
        assert isinstance(table_args, tuple), (
            "Expected __table_args__ to be a tuple (Index(...), {schema...})"
        )
        schema_kwargs = table_args[-1]
        assert isinstance(schema_kwargs, dict)
        assert schema_kwargs["schema"] == tsigma_schema("identity")

    def test_primary_key_is_id(self):
        """Test that 'id' is the primary key column."""
        pk_cols = [c.name for c in SystemSettingAudit.__table__.primary_key.columns]
        assert pk_cols == ["id"]

    def test_id_is_bigint_autoincrement(self):
        """Test id column is BigInteger with autoincrement (bigserial-equivalent)."""
        id_col = SystemSettingAudit.__table__.columns["id"]
        assert isinstance(id_col.type, BigInteger)
        # autoincrement may be the string "auto" (SA default) or True for
        # an explicitly-declared autoincrement integer PK — both indicate
        # the column is an autoincrementing PK.
        assert id_col.autoincrement in (True, "auto")

    def test_key_is_text_not_null(self):
        """Test key column is Text and NOT NULL."""
        key_col = SystemSettingAudit.__table__.columns["key"]
        assert isinstance(key_col.type, Text)
        assert key_col.nullable is False

    def test_old_value_is_text_nullable(self):
        """Test old_value column is Text and nullable (initial inserts have no prior value)."""
        col = SystemSettingAudit.__table__.columns["old_value"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_new_value_is_text_not_null(self):
        """Test new_value column is Text and NOT NULL."""
        col = SystemSettingAudit.__table__.columns["new_value"]
        assert isinstance(col.type, Text)
        assert col.nullable is False

    def test_changed_at_is_timestamptz_not_null(self):
        """Test changed_at column is timezone-aware timestamp and NOT NULL."""
        col = SystemSettingAudit.__table__.columns["changed_at"]
        # Mirror SystemSetting / AuthAuditLog: postgresql TIMESTAMP(timezone=True).
        assert isinstance(col.type, TIMESTAMP)
        assert col.type.timezone is True
        assert col.nullable is False

    def test_changed_at_server_default_is_now(self):
        """Test changed_at has a server-side default of now()."""
        col = SystemSettingAudit.__table__.columns["changed_at"]
        # The server_default's arg should be a func.now() expression — its
        # rendered string starts with "now" regardless of dialect rendering.
        rendered = str(col.server_default.arg).lower()
        assert "now" in rendered

    def test_changed_by_is_text_not_null(self):
        """Test changed_by column is Text and NOT NULL."""
        col = SystemSettingAudit.__table__.columns["changed_by"]
        assert isinstance(col.type, Text)
        assert col.nullable is False

    def test_reason_is_text_nullable(self):
        """Test reason column is Text and nullable (operators may omit it)."""
        col = SystemSettingAudit.__table__.columns["reason"]
        assert isinstance(col.type, Text)
        assert col.nullable is True

    def test_create_with_required_fields(self):
        """Test creating a SystemSettingAudit row with required fields."""
        audit = SystemSettingAudit(
            key="access_policy.analytics",
            new_value="authenticated",
            changed_by="admin",
        )
        assert audit.key == "access_policy.analytics"
        assert audit.new_value == "authenticated"
        assert audit.changed_by == "admin"
        # Optional fields default to None at the ORM level (server_default
        # populates them at INSERT time, not on the Python object).
        assert audit.old_value is None
        assert audit.reason is None

    def test_create_with_all_fields(self):
        """Test creating a SystemSettingAudit row with every field populated."""
        audit = SystemSettingAudit(
            key="api.max_page_size",
            old_value="500",
            new_value="1000",
            changed_by="admin",
            reason="raised limit for GDOT batch export",
        )
        assert audit.key == "api.max_page_size"
        assert audit.old_value == "500"
        assert audit.new_value == "1000"
        assert audit.changed_by == "admin"
        assert audit.reason == "raised limit for GDOT batch export"

    def test_key_changed_at_index_exists_with_desc_order(self):
        """
        Test that the (key, changed_at DESC) composite index exists.

        Required by Task A3's GET /api/v1/settings/{key}/audit endpoint,
        which retrieves the last 50 audit rows for a key. Mirrors the
        AuthAuditLog ``idx_auth_audit_user`` pattern: PostgreSQL ``DESC``
        ordering is declared via ``postgresql_ops`` rather than at the
        Index-column level (so ``Index.columns`` exposes the columns but
        not their direction — the descending direction is asserted via
        the dialect-specific ops dict).
        """
        indexes_by_name = {ix.name: ix for ix in SystemSettingAudit.__table__.indexes}
        assert "idx_system_setting_audit_key" in indexes_by_name, (
            "Missing composite index (key, changed_at DESC). "
            f"Found: {sorted(indexes_by_name)}"
        )

        ix = indexes_by_name["idx_system_setting_audit_key"]

        # Column order must be (key, changed_at) — leading-column key
        # supports the equality predicate on key, trailing changed_at
        # supports the ORDER BY.
        assert [c.name for c in ix.columns] == ["key", "changed_at"]

        # Descending order on changed_at is stored as a postgresql_ops
        # dialect kwarg. SQLAlchemy does NOT expose ASC/DESC at the
        # Index-column level for this idiom (it only appears in the
        # compiled DDL or in dialect_options), so we assert against the
        # dialect-options mapping directly.
        pg_opts = ix.dialect_options["postgresql"]
        assert pg_opts["ops"] == {"changed_at": "DESC"}
