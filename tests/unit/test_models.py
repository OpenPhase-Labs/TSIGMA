"""
Unit tests for TSIGMA database models.

Tests model instantiation, field types, and constraints.
"""

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from tsigma.models import (
    Approach,
    ControllerEventLog,
    Detector,
    DirectionType,
    Route,
    RouteDistance,
    RouteSignal,
    Signal,
)


class TestDirectionType:
    """Tests for DirectionType model."""

    def test_instantiate(self):
        """Test DirectionType can be instantiated."""
        direction = DirectionType(
            direction_type_id=1,
            abbreviation="NB",
            description="Northbound",
        )
        assert direction.direction_type_id == 1
        assert direction.abbreviation == "NB"
        assert direction.description == "Northbound"


class TestSignal:
    """Tests for Signal model."""

    def test_instantiate(self):
        """Test Signal can be instantiated with required fields."""
        signal = Signal(
            signal_id="signal-001",
            primary_street="Main St",
            enabled=True,
        )
        assert signal.signal_id == "signal-001"
        assert signal.primary_street == "Main St"
        assert signal.enabled is True

    def test_with_coordinates(self):
        """Test Signal with latitude/longitude."""
        signal = Signal(
            signal_id="signal-002",
            primary_street="Peachtree St",
            secondary_street="10th St",
            latitude=Decimal("33.7756"),
            longitude=Decimal("-84.3963"),
        )
        assert signal.latitude == Decimal("33.7756")
        assert signal.longitude == Decimal("-84.3963")

    def test_with_metadata(self):
        """Test Signal with JSONB metadata."""
        signal = Signal(
            signal_id="signal-003",
            primary_street="Test St",
            signal_metadata={"location_type": "intersection", "priority": "high"},
        )
        assert signal.signal_metadata["location_type"] == "intersection"
        assert signal.signal_metadata["priority"] == "high"


class TestApproach:
    """Tests for Approach model."""

    def test_instantiate(self):
        """Test Approach can be instantiated."""
        approach = Approach(
            signal_id="signal-001",
            direction_type_id=1,  # NB
            protected_phase_number=2,
        )
        assert approach.signal_id == "signal-001"
        assert approach.direction_type_id == 1
        assert approach.protected_phase_number == 2


class TestDetector:
    """Tests for Detector model."""

    def test_instantiate(self):
        """Test Detector can be instantiated."""
        approach_id = uuid4()
        detector = Detector(
            approach_id=approach_id,
            detector_channel=5,
            distance_from_stop_bar=300,
        )
        assert detector.approach_id == approach_id
        assert detector.detector_channel == 5
        assert detector.distance_from_stop_bar == 300


class TestControllerEventLog:
    """Tests for ControllerEventLog model."""

    def test_instantiate(self):
        """Test ControllerEventLog can be instantiated."""
        event = ControllerEventLog(
            signal_id="signal-001",
            event_time=datetime.now(),
            event_code=82,  # Detector On
            event_param=5,  # Channel 5
            device_id=1,
        )
        assert event.signal_id == "signal-001"
        assert event.event_code == 82
        assert event.event_param == 5
        assert event.device_id == 1


class TestRoute:
    """Tests for Route models."""

    def test_route_instantiate(self):
        """Test Route can be instantiated."""
        route = Route(name="EB Progression - Main St")
        assert route.name == "EB Progression - Main St"

    def test_route_signal_instantiate(self):
        """Test RouteSignal can be instantiated."""
        route_id = uuid4()
        route_signal = RouteSignal(
            route_id=route_id,
            signal_id="signal-001",
            sequence_order=1,
        )
        assert route_signal.route_id == route_id
        assert route_signal.signal_id == "signal-001"
        assert route_signal.sequence_order == 1

    def test_route_distance_instantiate(self):
        """Test RouteDistance can be instantiated."""
        from_signal = uuid4()
        to_signal = uuid4()
        distance = RouteDistance(
            from_route_signal_id=from_signal,
            to_route_signal_id=to_signal,
            distance_feet=1500,
            travel_time_seconds=30,
        )
        assert distance.distance_feet == 1500
        assert distance.travel_time_seconds == 30
