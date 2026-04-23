"""Tests for the validation registry."""

from typing import Any, ClassVar

import pytest

from tsigma.validation.registry import (
    BaseValidator,
    ValidationLevel,
    ValidationRegistry,
)

# ---------------------------------------------------------------------------
# Fixtures — clear registry between tests so registrations don't leak
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_registry():
    """Reset the registry before each test."""
    saved = ValidationRegistry._validators.copy()
    ValidationRegistry._validators.clear()
    yield
    ValidationRegistry._validators = saved


# ---------------------------------------------------------------------------
# Helpers — concrete validator stubs
# ---------------------------------------------------------------------------

def _make_validator(validator_name: str, validator_level: ValidationLevel):
    """Create and register a minimal concrete validator."""

    @ValidationRegistry.register(validator_name)
    class _Validator(BaseValidator):
        name: ClassVar[str] = validator_name
        level: ClassVar[ValidationLevel] = validator_level
        description: ClassVar[str] = f"Test validator ({validator_name})"

        async def validate_events(
            self,
            events: list[dict[str, Any]],
            signal_id: str,
            session_factory,
        ) -> list[dict[str, Any]]:
            return []

    return _Validator


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestValidationRegistry:
    """Tests for ValidationRegistry."""

    def test_register_and_get(self):
        """Register a validator and retrieve it by name."""
        cls = _make_validator("schema_range", ValidationLevel.LAYER1)
        assert ValidationRegistry.get("schema_range") is cls

    def test_get_unknown_raises(self):
        """get() raises ValueError for an unregistered name."""
        with pytest.raises(ValueError, match="Unknown validator"):
            ValidationRegistry.get("does_not_exist")

    def test_list_available(self):
        """list_available() returns registered names."""
        _make_validator("v1", ValidationLevel.LAYER1)
        _make_validator("v2", ValidationLevel.LAYER2)
        assert sorted(ValidationRegistry.list_available()) == ["v1", "v2"]

    def test_get_by_level_filters_correctly(self):
        """get_by_level() returns only validators matching the level."""
        l1_cls = _make_validator("layer1_val", ValidationLevel.LAYER1)
        _make_validator("layer2_val", ValidationLevel.LAYER2)

        result = ValidationRegistry.get_by_level(ValidationLevel.LAYER1)
        assert result == {"layer1_val": l1_cls}

        result2 = ValidationRegistry.get_by_level(ValidationLevel.LAYER3)
        assert result2 == {}
