"""
Unit tests for auth provisioning (JIT user creation from external IdPs).

All database interactions are mocked — no real DB required.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from tsigma.auth.models import AuthUser, UserRole
from tsigma.auth.provisioning import _EXTERNAL_PASSWORD_HASH, provision_user


def _make_mock_session(scalar_result=None):
    """Build a mock AsyncSession that returns scalar_result from execute()."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = scalar_result
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _make_user(**overrides):
    """Build a minimal AuthUser-like object for test assertions."""
    defaults = {
        "id": uuid4(),
        "username": "alice@example.com",
        "password_hash": _EXTERNAL_PASSWORD_HASH,
        "role": UserRole.VIEWER,
        "is_active": True,
        "external_id": "ext-123",
        "external_provider": "oidc",
    }
    defaults.update(overrides)
    user = MagicMock(spec=AuthUser)
    for k, v in defaults.items():
        setattr(user, k, v)
    return user


# ---------------------------------------------------------------------------
# New user creation (path 3 — no match by external_id or username)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provision_creates_new_user():
    """When no existing user matches, a new AuthUser is created."""
    # Both lookups return None
    session = AsyncMock()
    result_none = MagicMock()
    result_none.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=result_none)

    user = await provision_user(
        session,
        external_id="ext-999",
        external_provider="oidc",
        username="newuser@example.com",
        role=UserRole.VIEWER,
    )

    # session.add() was called with the newly created AuthUser
    session.add.assert_called_once()
    added_user = session.add.call_args[0][0]
    assert isinstance(added_user, AuthUser)
    assert added_user.username == "newuser@example.com"
    assert added_user.external_id == "ext-999"
    assert added_user.external_provider == "oidc"
    assert added_user.role == UserRole.VIEWER
    assert added_user.password_hash == _EXTERNAL_PASSWORD_HASH
    assert added_user.is_active is True
    session.flush.assert_awaited()
    assert user is added_user


# ---------------------------------------------------------------------------
# Existing user by external_id (path 1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provision_finds_by_external_id():
    """When user exists by external_id, return it without creating."""
    existing = _make_user(
        username="alice@example.com",
        external_id="ext-123",
        external_provider="oidc",
        role=UserRole.VIEWER,
    )

    session = _make_mock_session(scalar_result=existing)

    user = await provision_user(
        session,
        external_id="ext-123",
        external_provider="oidc",
        username="alice@example.com",
        role=UserRole.VIEWER,
    )

    assert user is existing
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_provision_updates_username_on_external_id_match():
    """When external_id matches but username changed, update username."""
    existing = _make_user(
        username="old@example.com",
        external_id="ext-123",
        external_provider="oidc",
        role=UserRole.VIEWER,
    )

    session = _make_mock_session(scalar_result=existing)

    user = await provision_user(
        session,
        external_id="ext-123",
        external_provider="oidc",
        username="new@example.com",
        role=UserRole.VIEWER,
    )

    assert user.username == "new@example.com"


@pytest.mark.asyncio
async def test_provision_upgrades_role_to_admin():
    """When role is ADMIN and current role is VIEWER, upgrade to ADMIN."""
    existing = _make_user(
        username="alice@example.com",
        external_id="ext-123",
        external_provider="oidc",
        role=UserRole.VIEWER,
    )

    session = _make_mock_session(scalar_result=existing)

    user = await provision_user(
        session,
        external_id="ext-123",
        external_provider="oidc",
        username="alice@example.com",
        role=UserRole.ADMIN,
    )

    assert user.role == UserRole.ADMIN


@pytest.mark.asyncio
async def test_provision_does_not_downgrade_admin():
    """When existing user is ADMIN and incoming role is VIEWER, keep ADMIN."""
    existing = _make_user(
        username="alice@example.com",
        external_id="ext-123",
        external_provider="oidc",
        role=UserRole.ADMIN,
    )

    session = _make_mock_session(scalar_result=existing)

    user = await provision_user(
        session,
        external_id="ext-123",
        external_provider="oidc",
        username="alice@example.com",
        role=UserRole.VIEWER,
    )

    # Role should remain ADMIN — never downgraded
    assert user.role == UserRole.ADMIN


# ---------------------------------------------------------------------------
# Existing user by username (path 2 — link local user to external IdP)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provision_links_existing_local_user():
    """When external_id lookup fails but username matches, link the user."""
    local_user = _make_user(
        username="bob@example.com",
        external_id=None,
        external_provider=None,
        role=UserRole.VIEWER,
    )

    # First execute (by external_id) returns None; second (by username) returns user
    results = []

    result_none = MagicMock()
    result_none.scalar_one_or_none.return_value = None
    results.append(result_none)

    result_user = MagicMock()
    result_user.scalar_one_or_none.return_value = local_user
    results.append(result_user)

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=results)

    user = await provision_user(
        session,
        external_id="ext-456",
        external_provider="oauth2",
        username="bob@example.com",
        role=UserRole.VIEWER,
    )

    assert user is local_user
    assert user.external_id == "ext-456"
    assert user.external_provider == "oauth2"
    session.add.assert_not_called()
    session.flush.assert_awaited()
