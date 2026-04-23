"""
Unit tests for redact_metadata in the crypto module.

Tests credential redaction, field preservation, None handling, and immutability.
"""

import copy

from tsigma.crypto import redact_metadata as _redact_metadata


def test_redact_replaces_password():
    """password inside collection is replaced with '***'."""
    metadata = {"collection": {"method": "ftp", "password": "secret123"}}
    result = _redact_metadata(metadata)
    assert result["collection"]["password"] == "***"


def test_redact_replaces_ssh_key_path():
    """ssh_key_path inside collection is replaced with '***'."""
    metadata = {"collection": {"method": "sftp", "ssh_key_path": "/home/user/.ssh/id_rsa"}}
    result = _redact_metadata(metadata)
    assert result["collection"]["ssh_key_path"] == "***"


def test_redact_preserves_other_fields():
    """Non-sensitive fields (username, method, decoder) are not redacted."""
    metadata = {
        "collection": {
            "username": "admin",
            "method": "ftp",
            "decoder": "asc3",
            "password": "secret",
        }
    }
    result = _redact_metadata(metadata)
    assert result["collection"]["username"] == "admin"
    assert result["collection"]["method"] == "ftp"
    assert result["collection"]["decoder"] == "asc3"


def test_redact_none_metadata():
    """None input returns None."""
    result = _redact_metadata(None)
    assert result is None


def test_redact_no_collection_key():
    """Metadata without a 'collection' key passes through unchanged."""
    metadata = {"firmware": "v2.1", "notes": "test signal"}
    result = _redact_metadata(metadata)
    assert result == {"firmware": "v2.1", "notes": "test signal"}


def test_redact_does_not_modify_original():
    """Original dict is not mutated (deep copy)."""
    metadata = {"collection": {"password": "secret", "ssh_key_path": "/key"}}
    original = copy.deepcopy(metadata)
    _redact_metadata(metadata)
    assert metadata == original
