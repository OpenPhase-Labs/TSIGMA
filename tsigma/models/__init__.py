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
from .approach import Approach
from .audit import ApproachAudit, AuthAuditLog, DetectorAudit
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
)
from .route import Route, RouteDistance, RoutePhase, RouteSignal
from .signal import Signal, SignalAudit
from .signal_plan import SignalPlan
from .system_setting import SystemSetting

__all__ = [
    # Base
    "Base",
    "TimestampMixin",
    # Core
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
    # Routes
    "Route",
    "RouteSignal",
    "RoutePhase",
    "RouteDistance",
    # Aggregates
    "ApproachDelay15Min",
    "ArrivalOnRedHourly",
    "CoordinationQualityHourly",
    "CycleBoundary",
    "CycleDetectorArrival",
    "CycleSummary15Min",
    "DetectorOccupancyHourly",
    "DetectorVolumeHourly",
    "PhaseTerminationHourly",
    "SplitFailureHourly",
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
    # System
    "SystemSetting",
]
