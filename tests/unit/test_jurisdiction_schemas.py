"""
Unit tests for jurisdiction Pydantic schemas.

Tests validation and field constraints for jurisdiction create and response schemas.
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from tsigma.api.v1.schemas import JurisdictionCreate, JurisdictionResponse


class TestJurisdictionCreate:
    """Tests for JurisdictionCreate schema."""

    def test_valid_create(self):
        """Test JurisdictionCreate accepts valid required fields."""
        data = JurisdictionCreate(name="City of Atlanta")
        assert data.name == "City of Atlanta"

    def test_all_fields(self):
        """Test JurisdictionCreate accepts all optional fields."""
        data = JurisdictionCreate(
            name="City of Atlanta",
            mpo_name="Atlanta Regional Commission",
            county_name="Fulton County",
        )
        assert data.mpo_name == "Atlanta Regional Commission"
        assert data.county_name == "Fulton County"

    def test_name_required(self):
        """Test JurisdictionCreate rejects missing name."""
        with pytest.raises(ValidationError):
            JurisdictionCreate()

    def test_name_min_length(self):
        """Test JurisdictionCreate rejects empty name."""
        with pytest.raises(ValidationError):
            JurisdictionCreate(name="")

    def test_optional_fields_default_none(self):
        """Test optional fields default to None."""
        data = JurisdictionCreate(name="Test")
        assert data.mpo_name is None
        assert data.county_name is None


class TestJurisdictionResponse:
    """Tests for JurisdictionResponse schema."""

    def test_from_attributes(self):
        """Test JurisdictionResponse has from_attributes config."""
        assert JurisdictionResponse.model_config.get("from_attributes") is True

    def test_serializes_all_fields(self):
        """Test JurisdictionResponse serializes all fields."""
        jid = uuid4()
        resp = JurisdictionResponse(
            jurisdiction_id=jid,
            name="City of Atlanta",
            mpo_name="ARC",
            county_name="Fulton",
        )
        assert resp.jurisdiction_id == jid
        assert resp.name == "City of Atlanta"
        assert resp.mpo_name == "ARC"

    def test_nullable_fields(self):
        """Test JurisdictionResponse handles null optional fields."""
        resp = JurisdictionResponse(
            jurisdiction_id=uuid4(),
            name="Test",
        )
        assert resp.mpo_name is None
        assert resp.county_name is None
