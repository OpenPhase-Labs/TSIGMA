"""Tests for credential encryption module."""

import os
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

from tsigma.crypto import (
    CryptoError,
    decrypt,
    decrypt_sensitive_fields,
    encrypt,
    encrypt_sensitive_fields,
    has_encryption_key,
    is_encrypted,
)


@pytest.fixture(autouse=True)
def _clear_key_cache():
    """Clear the lru_cache between tests."""
    from tsigma.crypto import _load_key
    _load_key.cache_clear()
    yield
    _load_key.cache_clear()


@pytest.fixture
def test_key():
    """Generate a valid Fernet key for testing."""
    return Fernet.generate_key().decode()


@pytest.fixture
def _set_key(test_key):
    """Set the encryption key via settings."""
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = test_key
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = ""
        yield test_key


def test_encrypt_decrypt_roundtrip(_set_key):
    """Encrypting then decrypting returns the original value."""
    original = "my_secret_password"
    encrypted = encrypt(original)
    assert encrypted != original
    assert decrypt(encrypted) == original


def test_encrypted_value_looks_like_fernet(_set_key):
    """Encrypted values start with gAAAAA (Fernet token prefix)."""
    encrypted = encrypt("test")
    assert is_encrypted(encrypted)


def test_is_encrypted_false_for_plaintext():
    """Plaintext values are not detected as encrypted."""
    assert not is_encrypted("plain_password")
    assert not is_encrypted("")
    assert not is_encrypted("12345")


def test_no_key_raises():
    """Encrypt/decrypt raise CryptoError when no key is configured."""
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = ""
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = ""
        with pytest.raises(CryptoError, match="No encryption key configured"):
            encrypt("test")


def test_wrong_key_raises(_set_key):
    """Decrypting with the wrong key raises CryptoError."""
    encrypted = encrypt("test")

    # Clear cache and set a different key
    from tsigma.crypto import _load_key
    _load_key.cache_clear()

    wrong_key = Fernet.generate_key().decode()
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = wrong_key
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = ""
        with pytest.raises(CryptoError, match="Decryption failed"):
            decrypt(encrypted)


def test_encrypt_sensitive_fields(_set_key):
    """encrypt_sensitive_fields encrypts password and ssh_key_path."""
    metadata = {
        "collection": {
            "method": "ftp_pull",
            "username": "admin",
            "password": "secret123",
            "ssh_key_path": "/path/to/key",
        }
    }
    result = encrypt_sensitive_fields(metadata)
    assert is_encrypted(result["collection"]["password"])
    assert is_encrypted(result["collection"]["ssh_key_path"])
    assert result["collection"]["username"] == "admin"  # not encrypted
    assert result["collection"]["method"] == "ftp_pull"  # not encrypted


def test_decrypt_sensitive_fields(_set_key):
    """decrypt_sensitive_fields reverses encrypt_sensitive_fields."""
    metadata = {
        "collection": {
            "method": "ftp_pull",
            "password": "secret123",
        }
    }
    encrypt_sensitive_fields(metadata)
    assert is_encrypted(metadata["collection"]["password"])

    decrypt_sensitive_fields(metadata)
    assert metadata["collection"]["password"] == "secret123"


def test_encrypt_skips_already_encrypted(_set_key):
    """Already-encrypted values are not double-encrypted."""
    metadata = {
        "collection": {
            "password": "secret123",
        }
    }
    encrypt_sensitive_fields(metadata)
    first_encrypted = metadata["collection"]["password"]

    encrypt_sensitive_fields(metadata)
    assert metadata["collection"]["password"] == first_encrypted


def test_decrypt_skips_plaintext(_set_key):
    """Plaintext values pass through decrypt_sensitive_fields unchanged."""
    metadata = {
        "collection": {
            "password": "not_encrypted_yet",
        }
    }
    decrypt_sensitive_fields(metadata)
    assert metadata["collection"]["password"] == "not_encrypted_yet"


def test_no_collection_key():
    """Metadata without a collection key passes through unchanged."""
    metadata = {"some_other_key": "value"}
    assert encrypt_sensitive_fields(metadata) == metadata
    assert decrypt_sensitive_fields(metadata) == metadata


def test_has_encryption_key_false():
    """has_encryption_key returns False when nothing is configured."""
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = ""
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = ""
        assert has_encryption_key() is False


def test_has_encryption_key_true_env():
    """has_encryption_key returns True when env var is set."""
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = "some_key"
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = ""
        assert has_encryption_key() is True


def test_key_from_file(tmp_path, _clear_key_cache):
    """Key can be loaded from a file."""
    key = Fernet.generate_key().decode()
    key_file = tmp_path / "secret.key"
    key_file.write_text(key)

    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = ""
        mock_settings.secret_key_file = str(key_file)
        mock_settings.secret_key_vault_url = ""
        encrypted = encrypt("test_from_file")
        assert decrypt(encrypted) == "test_from_file"


def test_key_file_not_found(_clear_key_cache):
    """TSIGMA_SECRET_KEY_FILE pointing to a missing file raises CryptoError."""
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = ""
        mock_settings.secret_key_file = "/nonexistent/path/secret.key"
        mock_settings.secret_key_vault_url = ""
        with pytest.raises(CryptoError, match="Secret key file not found"):
            encrypt("test")


def test_vault_loads_key(_clear_key_cache):
    """Key is loaded from HashiCorp Vault when vault URL is configured."""
    valid_key = Fernet.generate_key().decode()

    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = ""
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = "https://vault.example.com"
        mock_settings.secret_key_vault_path = "secret/data/tsigma"
        mock_settings.secret_key_vault_field = "secret_key"

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "data": {"data": {"secret_key": valid_key}}
        }

        with patch.dict(os.environ, {"VAULT_TOKEN": "test-token"}):
            with patch("httpx.get", return_value=mock_response) as mock_get:
                encrypted = encrypt("vault_test")
                assert decrypt(encrypted) == "vault_test"

                mock_get.assert_called_once()
                call_kwargs = mock_get.call_args
                assert "X-Vault-Token" in call_kwargs[1]["headers"]


def test_vault_missing_token(_clear_key_cache):
    """Missing VAULT_TOKEN env var raises CryptoError."""
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = ""
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = "https://vault.example.com"
        mock_settings.secret_key_vault_path = "secret/data/tsigma"
        mock_settings.secret_key_vault_field = "secret_key"

        with patch.dict(os.environ, {}, clear=True):
            # Ensure VAULT_TOKEN is absent
            os.environ.pop("VAULT_TOKEN", None)
            with pytest.raises(CryptoError, match="VAULT_TOKEN"):
                encrypt("test")


def test_vault_http_error(_clear_key_cache):
    """httpx HTTP error from vault raises CryptoError."""
    import httpx

    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = ""
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = "https://vault.example.com"
        mock_settings.secret_key_vault_path = "secret/data/tsigma"
        mock_settings.secret_key_vault_field = "secret_key"

        with patch.dict(os.environ, {"VAULT_TOKEN": "test-token"}):
            with patch(
                "httpx.get",
                side_effect=httpx.HTTPStatusError(
                    "403",
                    request=MagicMock(),
                    response=MagicMock(),
                ),
            ):
                with pytest.raises(CryptoError, match="Vault request failed"):
                    encrypt("test")


def test_vault_missing_field(_clear_key_cache):
    """Vault response missing expected field raises CryptoError."""
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = ""
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = "https://vault.example.com"
        mock_settings.secret_key_vault_path = "secret/data/tsigma"
        mock_settings.secret_key_vault_field = "secret_key"

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "data": {"data": {"wrong_field": "value"}}
        }

        with patch.dict(os.environ, {"VAULT_TOKEN": "test-token"}):
            with patch("httpx.get", return_value=mock_response):
                with pytest.raises(CryptoError, match="not found in vault"):
                    encrypt("test")


def test_invalid_fernet_key(_clear_key_cache):
    """Invalid Fernet key string raises CryptoError."""
    with patch("tsigma.crypto.settings") as mock_settings:
        mock_settings.secret_key = "not-a-valid-fernet-key!!!"
        mock_settings.secret_key_file = ""
        mock_settings.secret_key_vault_url = ""
        with pytest.raises(CryptoError, match="Invalid Fernet key"):
            encrypt("test")
