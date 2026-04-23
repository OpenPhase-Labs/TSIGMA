"""
Detectors API endpoints.

CRUD operations for detector configuration.
List/create are nested under approaches. Get/update/delete are standalone.
GET endpoints respect the 'signal_detail' access policy. POST/PUT/DELETE require admin role.
"""

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_audited_session, get_session
from ...models import Approach, Detector
from .crud_factory import crud_router
from .helpers import get_or_404
from .schemas import DetectorCreate, DetectorResponse, DetectorUpdate

router = APIRouter()

# Factory handles get/update/delete; custom nested list/create below.
router.include_router(
    crud_router(
        model=Detector,
        update_schema=DetectorUpdate,
        response_schema=DetectorResponse,
        pk_field="detector_id",
        prefix="/detectors",
        resource_name="Detector",
        operations={"get", "update", "delete"},
    )
)


@router.get(
    "/approaches/{approach_id}/detectors",
    response_model=list[DetectorResponse],
)
async def list_detectors(
    approach_id: str,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all detectors for an approach."""
    await get_or_404(session, Approach, Approach.approach_id, approach_id, "Approach")

    result = await session.execute(
        select(Detector).where(Detector.approach_id == approach_id)
    )
    return result.scalars().all()


@router.post(
    "/approaches/{approach_id}/detectors",
    response_model=DetectorResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_detector(
    approach_id: str,
    body: DetectorCreate,
    session: AsyncSession = Depends(get_audited_session),
    _: SessionData = Depends(require_admin),
):
    """Create a detector under an approach."""
    await get_or_404(session, Approach, Approach.approach_id, approach_id, "Approach")

    detector = Detector(
        approach_id=approach_id,
        detector_channel=body.detector_channel,
        distance_from_stop_bar=body.distance_from_stop_bar,
        min_speed_filter=body.min_speed_filter,
        decision_point=body.decision_point,
        movement_delay=body.movement_delay,
        lane_number=body.lane_number,
        lane_type_id=body.lane_type_id,
        movement_type_id=body.movement_type_id,
        detection_hardware_id=body.detection_hardware_id,
        lat_lon_distance=body.lat_lon_distance,
    )
    session.add(detector)
    await session.flush()

    return detector
