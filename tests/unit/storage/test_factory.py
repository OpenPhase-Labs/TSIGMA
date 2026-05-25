"""Tests for the cold storage backend factory."""

from pathlib import Path
from unittest.mock import patch

import pytest


def test_get_cold_storage_backend_filesystem(monkeypatch, tmp_path):
    """When storage_backend is 'filesystem', return FilesystemBackend rooted at storage_cold_path."""
    monkeypatch.setattr("tsigma.config.settings.storage_backend", "filesystem")
    monkeypatch.setattr("tsigma.config.settings.storage_cold_path", str(tmp_path))

    from tsigma.storage.factory import get_cold_storage_backend

    backend = get_cold_storage_backend()

    assert type(backend).__name__ == "FilesystemBackend"
    assert backend._base == tmp_path


def test_get_cold_storage_backend_s3(monkeypatch):
    """When storage_backend is 's3', return S3Backend configured from S3 settings."""
    monkeypatch.setattr("tsigma.config.settings.storage_backend", "s3")
    monkeypatch.setattr("tsigma.config.settings.storage_s3_bucket", "my-bucket")
    monkeypatch.setattr("tsigma.config.settings.storage_s3_region", "us-west-2")
    monkeypatch.setattr("tsigma.config.settings.storage_s3_endpoint", "http://localhost:9000")
    monkeypatch.setattr("tsigma.config.settings.storage_s3_access_key", "AKIAIOSFODNN7EXAMPLE")
    monkeypatch.setattr("tsigma.config.settings.storage_s3_secret_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

    with patch("tsigma.storage.s3.S3Backend") as MockS3Backend:
        from tsigma.storage.factory import get_cold_storage_backend

        backend = get_cold_storage_backend()

        MockS3Backend.assert_called_once_with(
            bucket="my-bucket",
            region="us-west-2",
            endpoint_url="http://localhost:9000",
            access_key="AKIAIOSFODNN7EXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
