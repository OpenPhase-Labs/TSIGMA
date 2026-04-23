"""
Analytics API endpoints.

Read-only query endpoints for traffic signal performance analytics.
Detector/phase/coordination/preemption endpoints respect the 'analytics'
access policy. Health endpoints respect the 'health' access policy.
All endpoints require a signal_id and time range. Results are computed
from ControllerEventLog (ATSPM high-resolution data).

Event code reference (Indiana Hi-Res spec, Purdue/INDOT 2012):
    1   = Phase Begin Green
    4   = Phase Gap Out
    5   = Phase Max Out
    6   = Phase Force Off
    7   = Phase Green Termination
    8   = Phase Begin Yellow Clearance
    9   = Phase End Yellow Clearance
    10  = Phase Begin Red Clearance
    11  = Phase End Red Clearance
    12  = Phase Inactive
    81  = Detector Off
    82  = Detector On
    102 = Preempt Call Input On      (request begin)
    104 = Preempt Call Input Off     (request end)
    105 = Preempt Entry Started      (service begin)
    111 = Preemption Begin Exit Interval (service end)
    131 = Coord Pattern Change
    132 = Cycle Length Change
    133 = Offset Length Change
    150 = Coord Cycle State Change
    151 = Coordinated Phase Yield Point
"""

from fastapi import APIRouter

from .coordination import router as coordination_router
from .detectors import router as detectors_router
from .health import router as health_router
from .phases import router as phases_router
from .preemption import router as preemption_router

router = APIRouter()

for _sub in [
    detectors_router,
    phases_router,
    coordination_router,
    preemption_router,
    health_router,
]:
    router.include_router(_sub)
