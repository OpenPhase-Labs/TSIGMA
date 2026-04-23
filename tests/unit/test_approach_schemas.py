"""
Unit tests for approach Pydantic schemas.

Tests validation and field constraints for approach create and response schemas.
"""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from tsigma.api.v1.schemas import ApproachCreate, ApproachResponse


class TestApproachCreate:
    """Tests for ApproachCreate schema."""

    def test_valid_create(self):
        """Test ApproachCreate accepts valid required fields."""
        data = ApproachCreate(direction_type_id=1)
        assert data.direction_type_id == 1
        assert data.is_protected_phase_overlap is False
        assert data.is_permissive_phase_overlap is False

    def test_all_fields(self):
        """Test ApproachCreate accepts all optional fields."""
        data = ApproachCreate(
            direction_type_id=3,
            description="Eastbound Through",
            mph=45,
            protected_phase_number=2,
            is_protected_phase_overlap=True,
            permissive_phase_number=6,
            is_permissive_phase_overlap=False,
            ped_phase_number=4,
        )
        assert data.description == "Eastbound Through"
        assert data.mph == 45
        assert data.protected_phase_number == 2
        assert data.is_protected_phase_overlap is True

    def test_direction_type_id_required(self):
        """Test ApproachCreate rejects missing direction_type_id."""
        with pytest.raises(ValidationError):
            ApproachCreate()

    def test_direction_type_id_min(self):
        """Test ApproachCreate rejects direction_type_id < 1."""
        with pytest.raises(ValidationError):
            ApproachCreate(direction_type_id=0)

    def test_mph_range(self):
        """Test ApproachCreate rejects mph > 100."""
        with pytest.raises(ValidationError):
            ApproachCreate(direction_type_id=1, mph=150)

    def test_phase_number_range(self):
        """Test ApproachCreate rejects phase_number > 16."""
        with pytest.raises(ValidationError):
            ApproachCreate(direction_type_id=1, protected_phase_number=17)

    def test_optional_fields_default_none(self):
        """Test optional fields default to None."""
        data = ApproachCreate(direction_type_id=1)
        assert data.description is None
        assert data.mph is None
        assert data.protected_phase_number is None
        assert data.permissive_phase_number is None
        assert data.ped_phase_number is None


class TestApproachResponse:
    """Tests for ApproachResponse schema."""

    def test_from_attributes(self):
        """Test ApproachResponse has from_attributes config."""
        assert ApproachResponse.model_config.get("from_attributes") is True

    def test_serializes_all_fields(self):
        """Test ApproachResponse serializes all fields."""
        now = datetime.now(timezone.utc)
        resp = ApproachResponse(
            approach_id=uuid4(),
            signal_id="SIG-001",
            direction_type_id=1,
            description="Northbound",
            mph=35,
            protected_phase_number=2,
            is_protected_phase_overlap=False,
            permissive_phase_number=None,
            is_permissive_phase_overlap=False,
            ped_phase_number=4,
            created_at=now,
            updated_at=now,
        )
        assert resp.signal_id == "SIG-001"
        assert resp.direction_type_id == 1
        assert resp.mph == 35

    def test_nullable_fields(self):
        """Test ApproachResponse handles null optional fields."""
        now = datetime.now(timezone.utc)
        resp = ApproachResponse(
            approach_id=uuid4(),
            signal_id="SIG-001",
            direction_type_id=1,
            is_protected_phase_overlap=False,
            is_permissive_phase_overlap=False,
            created_at=now,
            updated_at=now,
        )
        assert resp.description is None
        assert resp.mph is None
        assert resp.protected_phase_number is None
