"""Tests for the Layer 1 Schema/Range validator."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tsigma.validation.registry import ValidationLevel, ValidationRegistry
from tsigma.validation.sdk import STATUS_CLEAN, STATUS_INVALID
from tsigma.validation.validators.schema_range import SchemaRangeValidator


def _make_session_factory(code_rows):
    """Build a mock async session factory returning the given code rows."""
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = code_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(return_value=mock_session)


def _code_row(event_code, param_type):
    """Create a mock row with event_code and param_type attributes."""
    row = MagicMock()
    row.event_code = event_code
    row.param_type = param_type
    return row


class TestSchemaRangeValidator:
    """Tests for SchemaRangeValidator."""

    def test_registered(self):
        """Validator is registered and has correct level."""
        cls = ValidationRegistry.get("schema_range")
        assert cls is SchemaRangeValidator
        assert cls.level == ValidationLevel.LAYER1

    @pytest.mark.asyncio
    async def test_valid_event(self):
        """Valid event_code and in-range param returns STATUS_CLEAN."""
        factory = _make_session_factory([_code_row(1, "phase")])
        validator = SchemaRangeValidator()
        results = await validator.validate_events(
            [{"event_code": 1, "event_param": 3}],
            "signal-1",
            factory,
        )
        assert len(results) == 1
        assert results[0]["status"] == STATUS_CLEAN

    @pytest.mark.asyncio
    async def test_unknown_event_code(self):
        """Unknown event_code returns STATUS_INVALID with unknown_event_code."""
        factory = _make_session_factory([_code_row(1, "phase")])
        validator = SchemaRangeValidator()
        results = await validator.validate_events(
            [{"event_code": 9999, "event_param": 0}],
            "signal-1",
            factory,
        )
        assert len(results) == 1
        assert results[0]["status"] == STATUS_INVALID
        assert "unknown_event_code" in results[0]["rules_failed"]

    @pytest.mark.asyncio
    async def test_negative_event_param(self):
        """Negative event_param returns STATUS_INVALID with negative_param."""
        factory = _make_session_factory([_code_row(1, "phase")])
        validator = SchemaRangeValidator()
        results = await validator.validate_events(
            [{"event_code": 1, "event_param": -1}],
            "signal-1",
            factory,
        )
        assert len(results) == 1
        assert results[0]["status"] == STATUS_INVALID
        assert "negative_param" in results[0]["rules_failed"]

    @pytest.mark.asyncio
    async def test_phase_param_out_of_range(self):
        """Phase param exceeding max returns STATUS_INVALID with param_out_of_range."""
        factory = _make_session_factory([_code_row(1, "phase")])
        validator = SchemaRangeValidator()
        results = await validator.validate_events(
            [{"event_code": 1, "event_param": 99}],
            "signal-1",
            factory,
        )
        assert len(results) == 1
        assert results[0]["status"] == STATUS_INVALID
        assert "param_out_of_range" in results[0]["rules_failed"]

    @pytest.mark.asyncio
    async def test_batch_mixed_results(self):
        """Batch with one valid and one invalid event returns two results."""
        factory = _make_session_factory([_code_row(1, "phase")])
        validator = SchemaRangeValidator()
        results = await validator.validate_events(
            [
                {"event_code": 1, "event_param": 3},
                {"event_code": 9999, "event_param": 0},
            ],
            "signal-1",
            factory,
        )
        assert len(results) == 2
        assert results[0]["status"] == STATUS_CLEAN
        assert results[1]["status"] == STATUS_INVALID
        assert "unknown_event_code" in results[1]["rules_failed"]
