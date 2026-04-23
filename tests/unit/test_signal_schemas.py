"""
Unit tests for signal Pydantic schemas.

Tests validation, serialization, and field constraints for
signal create, update, and response schemas.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tsigma.api.v1.schemas import SignalCreate, SignalResponse, SignalUpdate


class TestSignalCreate:
    """Tests for SignalCreate schema."""

    def test_valid_create(self):
        """Test SignalCreate accepts valid required fields."""
        data = SignalCreate(signal_id="SIG-001", primary_street="Main St")
        assert data.signal_id == "SIG-001"
        assert data.primary_street == "Main St"
        assert data.enabled is True

    def test_all_fields(self):
        """Test SignalCreate accepts all optional fields."""
        data = SignalCreate(
            signal_id="SIG-002",
            primary_street="Oak Ave",
            secondary_street="1st St",
            latitude=Decimal("33.7756"),
            longitude=Decimal("-84.3963"),
            ip_address="192.168.1.100",
            note="Test signal",
            enabled=False,
            start_date=date(2026, 1, 15),
            metadata={"firmware": "AXON-1.2"},
        )
        assert data.secondary_street == "1st St"
        assert data.latitude == Decimal("33.7756")
        assert data.enabled is False
        assert data.metadata == {"firmware": "AXON-1.2"}

    def test_signal_id_required(self):
        """Test SignalCreate rejects missing signal_id."""
        with pytest.raises(ValidationError):
            SignalCreate(primary_street="Main St")

    def test_primary_street_required(self):
        """Test SignalCreate rejects missing primary_street."""
        with pytest.raises(ValidationError):
            SignalCreate(signal_id="SIG-001")

    def test_signal_id_min_length(self):
        """Test SignalCreate rejects empty signal_id."""
        with pytest.raises(ValidationError):
            SignalCreate(signal_id="", primary_street="Main St")

    def test_primary_street_min_length(self):
        """Test SignalCreate rejects empty primary_street."""
        with pytest.raises(ValidationError):
            SignalCreate(signal_id="SIG-001", primary_street="")

    def test_optional_fields_default_none(self):
        """Test optional fields default to None."""
        data = SignalCreate(signal_id="SIG-001", primary_street="Main St")
        assert data.secondary_street is None
        assert data.latitude is None
        assert data.longitude is None
        assert data.ip_address is None
        assert data.note is None
        assert data.start_date is None
        assert data.metadata is None


class TestSignalUpdate:
    """Tests for SignalUpdate schema."""

    def test_partial_update(self):
        """Test SignalUpdate accepts partial fields."""
        data = SignalUpdate(primary_street="New Main St")
        assert data.primary_street == "New Main St"
        assert data.secondary_street is None

    def test_update_enabled(self):
        """Test SignalUpdate can toggle enabled flag."""
        data = SignalUpdate(enabled=False)
        assert data.enabled is False

    def test_empty_update_rejected(self):
        """Test SignalUpdate rejects empty body (no fields set)."""
        with pytest.raises(ValidationError, match="At least one field"):
            SignalUpdate()

    def test_update_metadata(self):
        """Test SignalUpdate can set metadata."""
        data = SignalUpdate(metadata={"firmware": "2.0"})
        assert data.metadata == {"firmware": "2.0"}


class TestSignalResponse:
    """Tests for SignalResponse schema."""

    def test_from_attributes(self):
        """Test SignalResponse has from_attributes config."""
        assert SignalResponse.model_config.get("from_attributes") is True

    def test_serializes_all_fields(self):
        """Test SignalResponse serializes all signal fields."""
        now = datetime.now(timezone.utc)
        resp = SignalResponse(
            signal_id="SIG-001",
            primary_street="Main St",
            secondary_street="1st Ave",
            latitude=Decimal("33.7756"),
            longitude=Decimal("-84.3963"),
            enabled=True,
            created_at=now,
            updated_at=now,
        )
        assert resp.signal_id == "SIG-001"
        assert resp.latitude == Decimal("33.7756")

    def test_nullable_fields(self):
        """Test SignalResponse handles null optional fields."""
        now = datetime.now(timezone.utc)
        resp = SignalResponse(
            signal_id="SIG-001",
            primary_street="Main St",
            enabled=True,
            created_at=now,
            updated_at=now,
        )
        assert resp.secondary_street is None
        assert resp.latitude is None
        assert resp.longitude is None
