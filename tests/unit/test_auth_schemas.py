"""
Unit tests for authentication Pydantic schemas.

Tests serialization, validation, and field constraints.
"""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from tsigma.auth.schemas import LoginRequest, UserCreate, UserResponse


class TestLoginRequest:
    """Tests for LoginRequest schema."""

    def test_valid_login(self):
        """Test LoginRequest accepts valid username, password, and csrf_token."""
        req = LoginRequest(username="admin", password="secret", csrf_token="tok")
        assert req.username == "admin"
        assert req.password == "secret"
        assert req.csrf_token == "tok"

    def test_username_required(self):
        """Test LoginRequest rejects missing username."""
        with pytest.raises(ValidationError):
            LoginRequest(password="secret", csrf_token="tok")

    def test_password_required(self):
        """Test LoginRequest rejects missing password."""
        with pytest.raises(ValidationError):
            LoginRequest(username="admin", csrf_token="tok")

    def test_csrf_token_required(self):
        """Test LoginRequest rejects missing csrf_token."""
        with pytest.raises(ValidationError):
            LoginRequest(username="admin", password="secret")

    def test_username_min_length(self):
        """Test LoginRequest rejects empty username."""
        with pytest.raises(ValidationError):
            LoginRequest(username="", password="secret", csrf_token="tok")

    def test_password_min_length(self):
        """Test LoginRequest rejects empty password."""
        with pytest.raises(ValidationError):
            LoginRequest(username="admin", password="", csrf_token="tok")


class TestUserResponse:
    """Tests for UserResponse schema."""

    def test_serializes_user(self):
        """Test UserResponse includes expected fields."""
        user_id = uuid4()
        now = datetime.now(timezone.utc)
        resp = UserResponse(
            id=user_id,
            username="admin",
            role="admin",
            is_active=True,
            created_at=now,
        )
        assert resp.id == user_id
        assert resp.username == "admin"
        assert resp.role == "admin"
        assert resp.is_active is True

    def test_excludes_password_hash(self):
        """Test UserResponse does not have password_hash field."""
        assert "password_hash" not in UserResponse.model_fields

    def test_role_validation(self):
        """Test UserResponse rejects invalid role."""
        with pytest.raises(ValidationError):
            UserResponse(
                id=uuid4(),
                username="admin",
                role="superadmin",
                is_active=True,
                created_at=datetime.now(timezone.utc),
            )


class TestUserCreate:
    """Tests for UserCreate schema."""

    def test_valid_create(self):
        """Test UserCreate accepts valid data."""
        req = UserCreate(username="newuser", password="Str0ng!P@ss1", role="viewer")
        assert req.username == "newuser"
        assert req.password == "Str0ng!P@ss1"
        assert req.role == "viewer"

    def test_role_defaults_to_viewer(self):
        """Test UserCreate defaults role to viewer."""
        req = UserCreate(username="newuser", password="Str0ng!P@ss1")
        assert req.role == "viewer"

    def test_rejects_invalid_role(self):
        """Test UserCreate rejects invalid role value."""
        with pytest.raises(ValidationError):
            UserCreate(username="newuser", password="secret123", role="superadmin")
