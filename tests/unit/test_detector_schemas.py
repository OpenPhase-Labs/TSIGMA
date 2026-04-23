"""
Unit tests for detector Pydantic schemas.

Tests validation and field constraints for detector create and response schemas.
"""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from tsigma.api.v1.schemas import DetectorCreate, DetectorResponse


class TestDetectorCreate:
    """Tests for DetectorCreate schema."""

    def test_valid_create(self):
        """Test DetectorCreate accepts valid required fields."""
        data = DetectorCreate(detector_channel=5)
        assert data.detector_channel == 5

    def test_all_fields(self):
        """Test DetectorCreate accepts all optional fields."""
        data = DetectorCreate(
            detector_channel=3,
            distance_from_stop_bar=400,
            min_speed_filter=15,
            decision_point=350,
            movement_delay=5,
            lane_number=2,
            lane_type_id=uuid4(),
            movement_type_id=uuid4(),
            detection_hardware_id=uuid4(),
            lat_lon_distance=100,
        )
        assert data.distance_from_stop_bar == 400
        assert data.lane_number == 2

    def test_detector_channel_required(self):
        """Test DetectorCreate rejects missing detector_channel."""
        with pytest.raises(ValidationError):
            DetectorCreate()

    def test_detector_channel_min(self):
        """Test DetectorCreate rejects detector_channel < 1."""
        with pytest.raises(ValidationError):
            DetectorCreate(detector_channel=0)

    def test_distance_non_negative(self):
        """Test DetectorCreate rejects negative distance."""
        with pytest.raises(ValidationError):
            DetectorCreate(detector_channel=1, distance_from_stop_bar=-10)

    def test_optional_fields_default_none(self):
        """Test optional fields default to None."""
        data = DetectorCreate(detector_channel=1)
        assert data.distance_from_stop_bar is None
        assert data.min_speed_filter is None
        assert data.lane_number is None
        assert data.lane_type_id is None
        assert data.movement_type_id is None


class TestDetectorResponse:
    """Tests for DetectorResponse schema."""

    def test_from_attributes(self):
        """Test DetectorResponse has from_attributes config."""
        assert DetectorResponse.model_config.get("from_attributes") is True

    def test_serializes_all_fields(self):
        """Test DetectorResponse serializes all fields."""
        now = datetime.now(timezone.utc)
        approach_id = uuid4()
        resp = DetectorResponse(
            detector_id=uuid4(),
            approach_id=approach_id,
            detector_channel=5,
            distance_from_stop_bar=300,
            created_at=now,
            updated_at=now,
        )
        assert resp.approach_id == approach_id
        assert resp.detector_channel == 5
        assert resp.distance_from_stop_bar == 300

    def test_nullable_fields(self):
        """Test DetectorResponse handles null optional fields."""
        now = datetime.now(timezone.utc)
        resp = DetectorResponse(
            detector_id=uuid4(),
            approach_id=uuid4(),
            detector_channel=1,
            created_at=now,
            updated_at=now,
        )
        assert resp.distance_from_stop_bar is None
        assert resp.min_speed_filter is None
        assert resp.lane_type_id is None
