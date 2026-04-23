"""
Authentication Pydantic schemas.

Request/response models for auth endpoints.
"""

import re
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

_PASSWORD_MIN_LENGTH = 8
_PASSWORD_PATTERN = re.compile(
    r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^a-zA-Z0-9]).+$"
)
_PASSWORD_HELP = (
    "Password must be at least 8 characters with at least one "
    "uppercase letter, one lowercase letter, one digit, and one "
    "special character."
)


def _validate_password(value: str) -> str:
    """Enforce minimum length and complexity requirements."""
    if len(value) < _PASSWORD_MIN_LENGTH:
        raise ValueError(_PASSWORD_HELP)
    if not _PASSWORD_PATTERN.match(value):
        raise ValueError(_PASSWORD_HELP)
    return value


class LoginRequest(BaseModel):
    """Login request with username, password, and CSRF token."""

    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)
    csrf_token: str = Field(..., min_length=1)


class UserResponse(BaseModel):
    """User data returned in API responses (no password_hash)."""

    id: UUID
    username: str
    role: Literal["admin", "viewer"]
    is_active: bool
    created_at: datetime


class UserCreate(BaseModel):
    """Schema for creating a new user (admin operation)."""

    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=_PASSWORD_MIN_LENGTH)
    role: Literal["admin", "viewer"] = "viewer"

    @field_validator("password")
    @classmethod
    def check_password_complexity(cls, v: str) -> str:
        return _validate_password(v)
