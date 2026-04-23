"""
Unit tests for the SystemSetting model.

Tests model creation, field defaults, and constraints.
"""


from tsigma.models.system_setting import SystemSetting


class TestSystemSettingModel:
    """Tests for SystemSetting ORM model."""

    def test_tablename(self):
        """Test the table name is correct."""
        assert SystemSetting.__tablename__ == "system_setting"

    def test_create_with_required_fields(self):
        """Test creating a SystemSetting with required fields."""
        setting = SystemSetting(
            key="access_policy.analytics",
            value="authenticated",
            category="access_policy",
            description="Access level for analytics endpoints",
        )
        assert setting.key == "access_policy.analytics"
        assert setting.value == "authenticated"
        assert setting.category == "access_policy"
        assert setting.description == "Access level for analytics endpoints"

    def test_editable_defaults_to_true(self):
        """Test editable column has server_default of 'true'."""
        col = SystemSetting.__table__.columns["editable"]
        assert str(col.server_default.arg) == "true"

    def test_editable_can_be_false(self):
        """Test editable field can be set to False."""
        setting = SystemSetting(
            key="access_policy.management",
            value="authenticated",
            category="access_policy",
            description="Always authenticated",
            editable=False,
        )
        assert setting.editable is False

    def test_updated_by_defaults_to_none(self):
        """Test updated_by field defaults to None."""
        setting = SystemSetting(
            key="test.key",
            value="test",
            category="test",
            description="",
        )
        assert setting.updated_by is None

    def test_updated_by_can_be_set(self):
        """Test updated_by field can be set."""
        setting = SystemSetting(
            key="test.key",
            value="test",
            category="test",
            description="",
            updated_by="admin",
        )
        assert setting.updated_by == "admin"

    def test_primary_key_is_key(self):
        """Test that 'key' is the primary key column."""
        pk_cols = [c.name for c in SystemSetting.__table__.primary_key.columns]
        assert pk_cols == ["key"]

    def test_category_is_indexed(self):
        """Test that 'category' column has an index."""
        category_col = SystemSetting.__table__.columns["category"]
        assert category_col.index is True

    def test_key_max_length(self):
        """Test key column has max length 255."""
        key_col = SystemSetting.__table__.columns["key"]
        assert key_col.type.length == 255

    def test_category_max_length(self):
        """Test category column has max length 100."""
        category_col = SystemSetting.__table__.columns["category"]
        assert category_col.type.length == 100
