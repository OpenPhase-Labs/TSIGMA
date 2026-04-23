"""
Validation registry for TSIGMA.

Validators are self-registering plugins for post-ingestion event
validation across multiple layers (schema, temporal, cross-signal).
"""

import enum
from abc import ABC, abstractmethod
from typing import Any, ClassVar


class ValidationLevel(str, enum.Enum):
    """Validation layer level."""

    LAYER1 = "layer1"
    LAYER2 = "layer2"
    LAYER3 = "layer3"


class BaseValidator(ABC):
    """
    Base class for all validation plugins.

    Subclasses must declare name, level, and description as ClassVars
    and implement validate_events().
    """

    name: ClassVar[str]
    level: ClassVar[ValidationLevel]
    description: ClassVar[str]

    @abstractmethod
    async def validate_events(
        self,
        events: list[dict[str, Any]],
        signal_id: str,
        session_factory,
    ) -> list[dict[str, Any]]:
        """
        Validate a batch of events for a single signal.

        Args:
            events: List of event dicts to validate.
            signal_id: Traffic signal identifier.
            session_factory: Async session factory for DB reads.

        Returns:
            List of validation result dicts.
        """
        ...


class ValidationRegistry:
    """
    Central registry for all validation plugins.

    Validators self-register using the @ValidationRegistry.register decorator.
    """

    _validators: dict[str, type[BaseValidator]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Register a validation plugin.

        Usage:
            @ValidationRegistry.register("schema_range")
            class SchemaRangeValidator(BaseValidator):
                ...

        Args:
            name: Validator identifier (e.g., "schema_range").

        Returns:
            Decorator function.
        """
        def wrapper(validator_class: type[BaseValidator]) -> type[BaseValidator]:
            cls._validators[name] = validator_class
            return validator_class
        return wrapper

    @classmethod
    def get(cls, name: str) -> type[BaseValidator]:
        """
        Get a registered validator by name.

        Args:
            name: Validator identifier.

        Returns:
            Validator class.

        Raises:
            ValueError: If validator not found.
        """
        if name not in cls._validators:
            raise ValueError(f"Unknown validator: {name}")
        return cls._validators[name]

    @classmethod
    def list_available(cls) -> list[str]:
        """
        List all registered validator names.

        Returns:
            List of validator names.
        """
        return list(cls._validators.keys())

    @classmethod
    def get_by_level(cls, level: ValidationLevel) -> dict[str, type[BaseValidator]]:
        """
        Get all registered validators for a given level.

        Args:
            level: Validation level to filter by.

        Returns:
            Dictionary of name -> validator class.
        """
        return {
            name: validator_cls
            for name, validator_cls in cls._validators.items()
            if getattr(validator_cls, "level", None) == level
        }
