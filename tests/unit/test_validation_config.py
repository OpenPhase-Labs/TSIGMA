"""Tests for validation configuration settings."""

import os

from tsigma.config import Settings


def test_validation_defaults():
    """Validation settings have sensible defaults."""
    s = Settings(_env_file=None, pg_password="test")
    assert s.validation_enabled is True
    assert s.validation_layer1_enabled is True
    assert s.validation_layer2_enabled is False
    assert s.validation_layer3_enabled is False
    assert s.validation_batch_size == 5000
    assert s.validation_interval == 60


def test_validation_layer_toggles():
    """Individual layers can be toggled via env vars."""
    env = {
        "TSIGMA_VALIDATION_LAYER2_ENABLED": "true",
        "TSIGMA_VALIDATION_LAYER3_ENABLED": "true",
        "TSIGMA_VALIDATION_BATCH_SIZE": "1000",
    }
    for k, v in env.items():
        os.environ[k] = v
    try:
        s = Settings(_env_file=None, pg_password="test")
        assert s.validation_layer2_enabled is True
        assert s.validation_layer3_enabled is True
        assert s.validation_batch_size == 1000
    finally:
        for k in env:
            os.environ.pop(k, None)
