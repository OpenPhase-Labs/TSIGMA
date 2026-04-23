"""
Unit tests for password hashing utilities.

Tests bcrypt-based password hashing and verification.
"""


from tsigma.auth.passwords import hash_password, verify_password


class TestHashPassword:
    """Tests for hash_password()."""

    def test_returns_string(self):
        """Test hash_password returns a string."""
        result = hash_password("mysecret")
        assert isinstance(result, str)

    def test_returns_bcrypt_format(self):
        """Test hash starts with bcrypt identifier $2b$."""
        result = hash_password("mysecret")
        assert result.startswith("$2b$")

    def test_different_inputs_produce_different_hashes(self):
        """Test different passwords produce different hashes."""
        h1 = hash_password("password1")
        h2 = hash_password("password2")
        assert h1 != h2

    def test_same_input_produces_different_hashes(self):
        """Test same password produces different hashes (salted)."""
        h1 = hash_password("mysecret")
        h2 = hash_password("mysecret")
        assert h1 != h2

    def test_empty_string_hashes(self):
        """Test empty string can be hashed without error."""
        result = hash_password("")
        assert isinstance(result, str)


class TestVerifyPassword:
    """Tests for verify_password()."""

    def test_correct_password_returns_true(self):
        """Test verify returns True for matching password."""
        hashed = hash_password("mysecret")
        assert verify_password("mysecret", hashed) is True

    def test_wrong_password_returns_false(self):
        """Test verify returns False for non-matching password."""
        hashed = hash_password("mysecret")
        assert verify_password("wrongpass", hashed) is False

    def test_empty_password_verifies(self):
        """Test empty password can be verified."""
        hashed = hash_password("")
        assert verify_password("", hashed) is True
        assert verify_password("notempty", hashed) is False
