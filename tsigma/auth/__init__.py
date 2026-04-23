"""
TSIGMA Authentication.

Pluggable auth provider system with server-side sessions.
"""

from .api_keys import generate_api_key, list_user_keys, revoke_api_key, validate_api_key
from .dependencies import (
    get_current_user,
    get_current_user_optional,
    get_session_store,
    require_admin,
)
from .models import ApiKey, AuthUser, UserRole
from .passwords import hash_password, verify_password
from .registry import AuthProviderRegistry, BaseAuthProvider
from .schemas import LoginRequest, UserCreate, UserResponse
from .sessions import BaseSessionStore, InMemorySessionStore, SessionData

__all__ = [
    # Models
    "ApiKey",
    "AuthUser",
    "UserRole",
    # API Keys
    "generate_api_key",
    "list_user_keys",
    "revoke_api_key",
    "validate_api_key",
    # Schemas
    "LoginRequest",
    "UserCreate",
    "UserResponse",
    # Sessions
    "BaseSessionStore",
    "InMemorySessionStore",
    "SessionData",
    # Passwords
    "hash_password",
    "verify_password",
    # Dependencies
    "get_session_store",
    "get_current_user_optional",
    "get_current_user",
    "require_admin",
    # Registry
    "AuthProviderRegistry",
    "BaseAuthProvider",
]
