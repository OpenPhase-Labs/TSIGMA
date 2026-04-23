"""
Approaches API endpoints.

CRUD operations for signal approaches (directional approach configuration).
List/create are nested under signals. Get/update/delete are standalone.
GET endpoints respect the 'signal_detail' access policy. POST/PUT/DELETE require admin role.
"""

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_audited_session, get_session
from ...models import Approach, Signal
from .crud_factory import crud_router
from .helpers import get_or_404
from .schemas import ApproachCreate, ApproachResponse, ApproachUpdate

router = APIRouter()

# Factory handles get/update/delete; custom nested list/create below.
router.include_router(
    crud_router(
        model=Approach,
        update_schema=ApproachUpdate,
        response_schema=ApproachResponse,
        pk_field="approach_id",
        prefix="/approaches",
        resource_name="Approach",
        operations={"get", "update", "delete"},
    )
)


@router.get(
    "/signals/{signal_id}/approaches",
    response_model=list[ApproachResponse],
)
async def list_approaches(
    signal_id: str,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all approaches for a signal."""
    await get_or_404(session, Signal, Signal.signal_id, signal_id, "Signal")

    result = await session.execute(
        select(Approach).where(Approach.signal_id == signal_id)
    )
    return result.scalars().all()


@router.post(
    "/signals/{signal_id}/approaches",
    response_model=ApproachResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_approach(
    signal_id: str,
    body: ApproachCreate,
    session: AsyncSession = Depends(get_audited_session),
    _: SessionData = Depends(require_admin),
):
    """Create an approach under a signal."""
    await get_or_404(session, Signal, Signal.signal_id, signal_id, "Signal")

    approach = Approach(
        signal_id=signal_id,
        direction_type_id=body.direction_type_id,
        description=body.description,
        mph=body.mph,
        protected_phase_number=body.protected_phase_number,
        is_protected_phase_overlap=body.is_protected_phase_overlap,
        permissive_phase_number=body.permissive_phase_number,
        is_permissive_phase_overlap=body.is_permissive_phase_overlap,
        ped_phase_number=body.ped_phase_number,
    )
    session.add(approach)
    await session.flush()

    return approach
