"""
Unit tests for admin seeding.

Tests that the default admin user is created on first startup,
skipped if one already exists, and rejected if the password is insecure.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.auth.seed import FORBIDDEN_PASSWORDS, seed_admin


class TestSeedAdmin:
    """Tests for seed_admin()."""

    @pytest.mark.asyncio
    async def test_creates_admin_when_none_exists(self):
        """Test seed_admin creates admin user when table is empty."""
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        with patch("tsigma.auth.seed.settings") as mock_settings, \
             patch("tsigma.auth.seed.hash_password", return_value="$2b$12$hashed"):
            mock_settings.auth_mode = "local"
            mock_settings.auth_admin_user = "admin"
            mock_settings.auth_admin_password = "Str0ng!P@ssw0rd"

            await seed_admin(mock_session)

        mock_session.add.assert_called_once()
        added_user = mock_session.add.call_args[0][0]
        assert added_user.username == "admin"
        assert added_user.password_hash == "$2b$12$hashed"
        assert added_user.role.value == "admin"
        assert added_user.is_active is True
        mock_session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_when_admin_exists(self):
        """Test seed_admin does nothing when admin user already exists."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = MagicMock(username="admin")
        mock_session.execute.return_value = mock_result

        with patch("tsigma.auth.seed.settings") as mock_settings:
            mock_settings.auth_mode = "local"
            mock_settings.auth_admin_user = "admin"

            await seed_admin(mock_session)

        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_uses_settings_credentials(self):
        """Test seed_admin uses username/password from settings."""
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        with patch("tsigma.auth.seed.settings") as mock_settings, \
             patch("tsigma.auth.seed.hash_password", return_value="$2b$12$custom") as mock_hash:
            mock_settings.auth_mode = "local"
            mock_settings.auth_admin_user = "superadmin"
            mock_settings.auth_admin_password = "s3cret!"

            await seed_admin(mock_session)

        mock_hash.assert_called_once_with("s3cret!")
        added_user = mock_session.add.call_args[0][0]
        assert added_user.username == "superadmin"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_password", sorted(FORBIDDEN_PASSWORDS))
    async def test_rejects_insecure_default_password(self, bad_password):
        """Test seed_admin refuses to start with a forbidden password."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        with patch("tsigma.auth.seed.settings") as mock_settings:
            mock_settings.auth_mode = "local"
            mock_settings.auth_admin_user = "admin"
            mock_settings.auth_admin_password = bad_password

            with pytest.raises(SystemExit, match="must be changed"):
                await seed_admin(mock_session)

        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_insecure_password_skipped_when_admin_exists(self):
        """Test forbidden password check is skipped when admin already exists."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = MagicMock(username="admin")
        mock_session.execute.return_value = mock_result

        with patch("tsigma.auth.seed.settings") as mock_settings:
            mock_settings.auth_mode = "local"
            mock_settings.auth_admin_user = "admin"
            mock_settings.auth_admin_password = "changeme"

            # Should NOT raise — admin already exists, no seed needed
            await seed_admin(mock_session)

        mock_session.add.assert_not_called()
