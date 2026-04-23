"""Test that ControllerEventLog has the validation_metadata column."""

from tsigma.models.event import ControllerEventLog


def test_validation_metadata_column_exists():
    """The model must declare a validation_metadata JSONB column."""
    col = ControllerEventLog.__table__.columns.get("validation_metadata")
    assert col is not None, "validation_metadata column missing"
    assert "JSONB" in str(col.type)
    assert col.nullable is True


def test_validation_metadata_default_is_none():
    """New events should default to None (not empty dict)."""
    event = ControllerEventLog(
        signal_id="9999",
        event_time="2026-01-01T00:00:00+00:00",
        event_code=1,
        event_param=1,
    )
    assert event.validation_metadata is None
