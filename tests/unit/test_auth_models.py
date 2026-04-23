"""
Unit tests for AuthUser database model.

Tests model instantiation, field types, and constraints.
"""

from uuid import uuid4

import pytest

from tsigma.auth.models import AuthUser, UserRole


class TestUserRole:
    """Tests for UserRole enum."""

    def test_admin_value(self):
        """Test admin role has correct string value."""
        assert UserRole.ADMIN.value == "admin"

    def test_viewer_value(self):
        """Test viewer role has correct string value."""
        assert UserRole.VIEWER.value == "viewer"

    def test_from_string(self):
        """Test UserRole can be created from string."""
        assert UserRole("admin") == UserRole.ADMIN
        assert UserRole("viewer") == UserRole.VIEWER

    def test_invalid_role_raises(self):
        """Test invalid role string raises ValueError."""
        with pytest.raises(ValueError):
            UserRole("superadmin")


class TestAuthUser:
    """Tests for AuthUser model."""

    def test_instantiate_admin(self):
        """Test AuthUser can be instantiated as admin."""
        user = AuthUser(
            username="admin",
            password_hash="$2b$12$fakehashhere",
            role=UserRole.ADMIN,
            is_active=True,
        )
        assert user.username == "admin"
        assert user.role == UserRole.ADMIN
        assert user.is_active is True

    def test_instantiate_viewer(self):
        """Test AuthUser can be instantiated as viewer."""
        user = AuthUser(
            username="reader",
            password_hash="$2b$12$fakehashhere",
            role=UserRole.VIEWER,
            is_active=True,
        )
        assert user.role == UserRole.VIEWER

    def test_has_tablename(self):
        """Test AuthUser has correct table name."""
        assert AuthUser.__tablename__ == "auth_user"

    def test_id_is_uuid(self):
        """Test AuthUser id field accepts UUID."""
        user_id = uuid4()
        user = AuthUser(
            id=user_id,
            username="test",
            password_hash="hash",
            role=UserRole.ADMIN,
        )
        assert user.id == user_id

    def test_is_active_server_default(self):
        """Test AuthUser has server_default for is_active."""
        col = AuthUser.__table__.c.is_active
        assert col.server_default is not None
        assert str(col.server_default.arg) == "true"

    def test_is_active_set_explicitly(self):
        """Test AuthUser is_active can be set explicitly."""
        user = AuthUser(
            username="test",
            password_hash="hash",
            role=UserRole.VIEWER,
            is_active=False,
        )
        assert user.is_active is False
