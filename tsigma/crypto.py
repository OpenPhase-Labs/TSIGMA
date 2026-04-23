"""
Credential encryption for TSIGMA.

Encrypts sensitive fields (passwords, SSH keys) at rest using Fernet
symmetric encryption. Credentials are encrypted before writing to the
database and decrypted at poll time when the CollectorService needs them.

Key sources (checked in order):
1. TSIGMA_SECRET_KEY environment variable (Fernet key, base64-encoded)
2. TSIGMA_SECRET_KEY_FILE path to a file containing the key
3. TSIGMA_SECRET_KEY_VAULT_URL + TSIGMA_SECRET_KEY_VAULT_PATH (HashiCorp Vault)

If no key is configured, encrypt/decrypt raise RuntimeError.
Generate a key: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

import logging
from functools import lru_cache
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from .config import settings

logger = logging.getLogger(__name__)

# Fields in signal_metadata["collection"] that must be encrypted at rest.
SENSITIVE_FIELDS = frozenset({
    "password",
    "ssh_key_path",
    "snmp_auth_passphrase",
    "snmp_priv_passphrase",
})


class CryptoError(RuntimeError):
    """Raised when encryption/decryption fails."""

    pass


@lru_cache(maxsize=1)
def _load_key() -> bytes:
    """
    Load the Fernet encryption key from the configured source.

    Checks in order: env var, file, vault.

    Returns:
        Raw Fernet key bytes (base64-encoded 32-byte key).

    Raises:
        CryptoError: If no key source is configured or key is invalid.
    """
    # 1. Environment variable / settings
    if settings.secret_key:
        logger.info("Encryption key loaded from TSIGMA_SECRET_KEY")
        return settings.secret_key.encode()

    # 2. File
    if settings.secret_key_file:
        path = Path(settings.secret_key_file)
        if not path.is_file():
            raise CryptoError(
                f"Secret key file not found: {settings.secret_key_file}"
            )
        key = path.read_text().strip().encode()
        logger.info("Encryption key loaded from file: %s", settings.secret_key_file)
        return key

    # 3. Vault
    if settings.secret_key_vault_url:
        return _load_key_from_vault()

    raise CryptoError(
        "No encryption key configured. Set TSIGMA_SECRET_KEY, "
        "TSIGMA_SECRET_KEY_FILE, or TSIGMA_SECRET_KEY_VAULT_URL. "
        "Generate a key: python -c "
        '"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"'
    )


def _load_key_from_vault() -> bytes:
    """
    Load the encryption key from HashiCorp Vault.

    Uses the VAULT_TOKEN environment variable for authentication.

    Returns:
        Raw Fernet key bytes.

    Raises:
        CryptoError: If vault is unreachable or key not found.
    """
    import os

    import httpx

    vault_url = settings.secret_key_vault_url.rstrip("/")
    vault_path = settings.secret_key_vault_path
    vault_field = settings.secret_key_vault_field
    vault_token = os.environ.get("VAULT_TOKEN", "")

    if not vault_token:
        raise CryptoError(
            "VAULT_TOKEN environment variable required for vault key retrieval"
        )

    try:
        response = httpx.get(
            f"{vault_url}/v1/{vault_path}",
            headers={"X-Vault-Token": vault_token},
            timeout=10.0,
        )
        response.raise_for_status()
        data = response.json()
        # KV v2 nests under data.data, KV v1 under data
        secret_data = data.get("data", {})
        if "data" in secret_data:
            secret_data = secret_data["data"]
        key = secret_data.get(vault_field)
        if not key:
            raise CryptoError(
                f"Field '{vault_field}' not found in vault secret at {vault_path}"
            )
        logger.info("Encryption key loaded from vault: %s", vault_url)
        return key.encode()
    except httpx.HTTPStatusError as exc:
        raise CryptoError(
            f"Vault request failed: HTTP {exc.response.status_code}"
        ) from None
    except httpx.HTTPError as exc:
        raise CryptoError(
            f"Vault connection failed: {type(exc).__name__}"
        ) from None


def _get_fernet() -> Fernet:
    """
    Get a Fernet instance with the configured key.

    Returns:
        Fernet instance ready for encrypt/decrypt.

    Raises:
        CryptoError: If key is missing or invalid.
    """
    key = _load_key()
    try:
        return Fernet(key)
    except Exception as exc:
        raise CryptoError(f"Invalid Fernet key: {exc}") from exc


def encrypt(plaintext: str) -> str:
    """
    Encrypt a plaintext string.

    Args:
        plaintext: The string to encrypt.

    Returns:
        Encrypted string (Fernet token, base64-encoded).

    Raises:
        CryptoError: If no key is configured.
    """
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    """
    Decrypt a Fernet-encrypted string.

    Args:
        ciphertext: Fernet token (base64-encoded).

    Returns:
        Decrypted plaintext string.

    Raises:
        CryptoError: If decryption fails (wrong key, tampered data, or
                     plaintext value that was never encrypted).
    """
    f = _get_fernet()
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except InvalidToken as exc:
        raise CryptoError(
            "Decryption failed — wrong key, corrupted data, or "
            "value was never encrypted"
        ) from exc


def is_encrypted(value: str) -> bool:
    """
    Check if a string looks like a Fernet token.

    Fernet tokens are base64-encoded and start with 'gAAAAA'.
    This is a heuristic, not a guarantee.

    Args:
        value: String to check.

    Returns:
        True if the value appears to be a Fernet token.
    """
    return isinstance(value, str) and value.startswith("gAAAAA")


def encrypt_sensitive_fields(metadata: dict) -> dict:
    """
    Encrypt sensitive fields in a signal_metadata dict before DB write.

    Encrypts values in metadata["collection"] that are in SENSITIVE_FIELDS.
    Already-encrypted values are left unchanged.

    Args:
        metadata: Signal metadata dict (modified in place and returned).

    Returns:
        The metadata dict with sensitive fields encrypted.
    """
    collection = metadata.get("collection")
    if not isinstance(collection, dict):
        return metadata

    for field in SENSITIVE_FIELDS:
        value = collection.get(field)
        if value and isinstance(value, str) and not is_encrypted(value):
            collection[field] = encrypt(value)

    return metadata


def decrypt_sensitive_fields(metadata: dict) -> dict:
    """
    Decrypt sensitive fields in a signal_metadata dict for poll-time use.

    Decrypts values in metadata["collection"] that are in SENSITIVE_FIELDS.
    Plaintext values (not yet encrypted) are returned as-is for backward
    compatibility during migration.

    Args:
        metadata: Signal metadata dict (modified in place and returned).

    Returns:
        The metadata dict with sensitive fields decrypted.
    """
    collection = metadata.get("collection")
    if not isinstance(collection, dict):
        return metadata

    for field in SENSITIVE_FIELDS:
        value = collection.get(field)
        if value and isinstance(value, str) and is_encrypted(value):
            collection[field] = decrypt(value)

    return metadata


def has_encryption_key() -> bool:
    """
    Check if an encryption key is configured.

    Returns:
        True if any key source is configured (env, file, or vault).
    """
    return bool(
        settings.secret_key
        or settings.secret_key_file
        or settings.secret_key_vault_url
    )


def redact_metadata(metadata: dict | None) -> dict | None:
    """
    Remove sensitive fields from signal_metadata for API responses.

    Replaces values in metadata["collection"] that are in SENSITIVE_FIELDS
    with '***' so credentials are never returned to API consumers.

    Args:
        metadata: Signal metadata dict (or None).

    Returns:
        Deep copy with sensitive values masked, or the original if falsy.
    """
    import copy

    if not metadata:
        return metadata
    result = copy.deepcopy(metadata)
    collection = result.get("collection")
    if isinstance(collection, dict):
        for field in SENSITIVE_FIELDS:
            if field in collection:
                collection[field] = "***"
    return result
