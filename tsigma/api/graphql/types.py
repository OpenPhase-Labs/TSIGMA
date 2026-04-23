"""
Strawberry GraphQL type definitions for TSIGMA.

Maps SQLAlchemy ORM models to Strawberry types for GraphQL queries.
"""

import logging
from datetime import datetime

import strawberry

logger = logging.getLogger(__name__)


@strawberry.type
class DetectorType:
    """Vehicle or pedestrian detector."""

    detector_id: str
    approach_id: str
    detector_channel: int
    distance_from_stop_bar: int | None
    min_speed_filter: int | None
    decision_point: int | None
    movement_delay: int | None
    lane_number: int | None


@strawberry.type
class ApproachType:
    """Directional approach to an intersection."""

    approach_id: str
    signal_id: str
    direction_type_id: int
    description: str | None
    mph: int | None
    protected_phase_number: int | None
    is_protected_phase_overlap: bool
    permissive_phase_number: int | None
    is_permissive_phase_overlap: bool
    ped_phase_number: int | None
    detectors: list[DetectorType]


@strawberry.type
class SignalType:
    """Traffic signal / intersection configuration."""

    signal_id: str
    primary_street: str
    secondary_street: str | None
    latitude: float | None
    longitude: float | None
    enabled: bool
    note: str | None
    approaches: list[ApproachType]


@strawberry.type
class EventType:
    """Controller event log entry."""

    signal_id: str
    event_time: datetime
    event_code: int
    event_param: int


@strawberry.type
class RegionType:
    """Regional grouping."""

    region_id: str
    description: str
    parent_region_id: str | None


@strawberry.type
class JurisdictionType:
    """Jurisdictional boundary."""

    jurisdiction_id: str
    name: str
    mpo_name: str | None
    county_name: str | None


@strawberry.type
class CorridorType:
    """Signal corridor grouping."""

    corridor_id: str
    name: str
    description: str | None


@strawberry.type
class ReportInfoType:
    """Metadata about a registered report plugin."""

    name: str
    description: str
    category: str
    estimated_time: str
    export_formats: list[str]


@strawberry.type
class ReportResultType:
    """Result from executing a report."""

    status: str
    data: strawberry.scalars.JSON
