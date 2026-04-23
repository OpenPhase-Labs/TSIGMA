"""
Layer 1 Schema/Range Validator.

Deterministic validator that checks events against the event_code_definition
reference table. Validates event codes exist and parameters fall within
NTCIP 1202 ranges for their parameter type.
"""

from typing import Any, ClassVar

from sqlalchemy import select

from ...models.reference import EventCodeDefinition
from ..registry import BaseValidator, ValidationLevel, ValidationRegistry
from ..sdk import STATUS_CLEAN, STATUS_INVALID, build_result

# Practical NTCIP 1202 maximums by parameter type
PARAM_RANGE_BY_TYPE = {
    "phase": (0, 40),
    "detector": (0, 128),
    "overlap": (0, 16),
    "ring": (0, 8),
    "channel": (0, 64),
    "preempt": (0, 10),
    "coord_pattern": (0, 255),
    "unit": (0, 255),
    "other": (0, 65535),
}
_DEFAULT_RANGE = (0, 65535)


@ValidationRegistry.register("schema_range")
class SchemaRangeValidator(BaseValidator):
    """
    Layer 1 schema and range validator.

    Checks each event against three rules:
    - negative_param: event_param must not be negative
    - unknown_event_code: event_code must exist in event_code_definition
    - param_out_of_range: event_param must be within range for its param_type
    """

    name: ClassVar[str] = "schema_range"
    level: ClassVar[ValidationLevel] = ValidationLevel.LAYER1
    description: ClassVar[str] = (
        "Deterministic schema/range check against event_code_definition"
    )

    async def validate_events(
        self,
        events: list[dict[str, Any]],
        signal_id: str,
        session_factory,
    ) -> list[dict[str, Any]]:
        """
        Validate a batch of events for a single signal.

        Args:
            events: List of event dicts with event_code and event_param keys.
            signal_id: Traffic signal identifier.
            session_factory: Async session factory for DB reads.

        Returns:
            List of validation result dicts, one per event.
        """
        code_lookup = await self._load_code_lookup(session_factory)
        results = []

        for event in events:
            event_code = event.get("event_code")
            event_param = event.get("event_param")
            rules_failed = []

            # Rule 1: negative parameter
            if event_param is not None and event_param < 0:
                rules_failed.append("negative_param")

            # Rule 2: unknown event code
            if event_code not in code_lookup:
                rules_failed.append("unknown_event_code")
            else:
                # Rule 3: parameter out of range (only if code is known)
                param_type = code_lookup[event_code]
                lo, hi = PARAM_RANGE_BY_TYPE.get(param_type, _DEFAULT_RANGE)
                if event_param is not None and not (lo <= event_param <= hi):
                    rules_failed.append("param_out_of_range")

            if rules_failed:
                results.append(
                    build_result(
                        self.name,
                        STATUS_INVALID,
                        rules_failed=rules_failed,
                    )
                )
            else:
                results.append(
                    build_result(self.name, STATUS_CLEAN)
                )

        return results

    async def _load_code_lookup(self, session_factory) -> dict[int, str]:
        """Load event_code -> param_type mapping from the database."""
        async with session_factory() as session:
            stmt = select(
                EventCodeDefinition.event_code,
                EventCodeDefinition.param_type,
            )
            result = await session.execute(stmt)
            return {row.event_code: row.param_type for row in result.all()}
