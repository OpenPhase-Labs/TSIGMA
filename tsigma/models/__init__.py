"""
TSIGMA Database Models.

SQLAlchemy ORM models for all TSIGMA database tables.
Based on ATSPM 4.x with PostgreSQL/TimescaleDB optimizations.
"""

from .aggregates import (
    ApproachDelay15Min,
    ArrivalOnRedHourly,
    CoordinationQualityHourly,
    CycleBoundary,
    CycleDetectorArrival,
    CycleSummary15Min,
    DetectorOccupancyHourly,
    DetectorVolumeHourly,
    PhaseTerminationHourly,
    SplitFailureHourly,
)
from .aggregates_phase import (
    ApproachSpeed15Min,
    PhaseCycle15Min,
    PhaseLeftTurnGap15Min,
    PhasePedestrian15Min,
    Priority15Min,
    YellowRedActivation15Min,
)
from .aggregates_signal import (
    Preemption15Min,
    SignalEventCount15Min,
)
from .alert_suppression import AlertSuppression
from .approach import Approach
from .audit import (
    ApproachAudit,
    AuthAuditLog,
    DetectorAudit,
    RoadsideSensorAudit,
    RoadsideSensorLaneAudit,
)
from .base import Base, TimestampMixin
from .checkpoint import PollingCheckpoint
from .detector import Detector
from .event import ControllerEventLog
from .reference import (
    ControllerType,
    Corridor,
    DetectionHardware,
    DirectionType,
    EventCodeDefinition,
    Jurisdiction,
    LaneType,
    MovementType,
    Region,
    RoadsideSensorModel,
    RoadsideSensorVendor,
)
from .roadside_event import (
    ROADSIDE_EVENT_TYPE_CLASSIFICATION,
    ROADSIDE_EVENT_TYPE_OCCUPANCY,
    ROADSIDE_EVENT_TYPE_QUEUE,
    ROADSIDE_EVENT_TYPE_SPEED,
    RoadsideEvent,
)
from .roadside_sensor import RoadsideSensor, RoadsideSensorLane
from .route import Route, RouteDistance, RoutePhase, RouteSignal
from .signal import Signal, SignalAudit
from .signal_plan import SignalPlan
from .system_setting import SystemSetting

__all__ = [
    # Base
    "Base",
    "TimestampMixin",
    # Core
    "AlertSuppression",
    "PollingCheckpoint",
    "Signal",
    "SignalAudit",
    "ApproachAudit",
    "DetectorAudit",
    "AuthAuditLog",
    "Approach",
    "Detector",
    "ControllerEventLog",
    "SignalPlan",
    # Roadside sensors (radar / LiDAR / video)
    "RoadsideSensor",
    "RoadsideSensorLane",
    "RoadsideSensorAudit",
    "RoadsideSensorLaneAudit",
    "RoadsideEvent",
    "ROADSIDE_EVENT_TYPE_SPEED",
    "ROADSIDE_EVENT_TYPE_CLASSIFICATION",
    "ROADSIDE_EVENT_TYPE_QUEUE",
    "ROADSIDE_EVENT_TYPE_OCCUPANCY",
    # Routes
    "Route",
    "RouteSignal",
    "RoutePhase",
    "RouteDistance",
    # Aggregates
    "ApproachDelay15Min",
    "ApproachSpeed15Min",
    "ArrivalOnRedHourly",
    "CoordinationQualityHourly",
    "CycleBoundary",
    "CycleDetectorArrival",
    "CycleSummary15Min",
    "DetectorOccupancyHourly",
    "DetectorVolumeHourly",
    "PhaseCycle15Min",
    "PhaseLeftTurnGap15Min",
    "PhasePedestrian15Min",
    "PhaseTerminationHourly",
    "Preemption15Min",
    "Priority15Min",
    "SignalEventCount15Min",
    "SplitFailureHourly",
    "YellowRedActivation15Min",
    # Reference
    "Corridor",
    "ControllerType",
    "DetectionHardware",
    "DirectionType",
    "EventCodeDefinition",
    "Jurisdiction",
    "LaneType",
    "MovementType",
    "Region",
    "RoadsideSensorModel",
    "RoadsideSensorVendor",
    # System
    "SystemSetting",
]
