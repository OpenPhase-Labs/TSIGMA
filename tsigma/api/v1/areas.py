"""
Areas API endpoints.

CRUD for named operational area groupings (e.g., "Downtown District")
plus signal-membership management. GET endpoints respect the
'signal_detail' access policy; POST/DELETE require admin role.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_session
from ...models import Area, Signal, SignalArea
from .crud_factory import crud_router
from .helpers import get_or_404
from .schemas import (
    AreaCreate,
    AreaResponse,
    AreaSignalCreate,
    AreaUpdate,
    LookupItem,
)

router = APIRouter()


@router.get("/lookup", response_model=list[LookupItem])
async def lookup_areas(
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """Return compact id/label items for all areas."""
    rows = (await session.execute(select(Area))).scalars().all()
    return [LookupItem(id=str(a.area_id), label=a.name) for a in rows]


class AreaSignalResponse(BaseModel):
    """Area signal membership returned in API responses."""

    model_config = {"from_attributes": True}

    signal_id: str


# Registered after /lookup so the literal path is not shadowed by the
# factory's GET /{area_id} route.
router.include_router(
    crud_router(
        model=Area,
        create_schema=AreaCreate,
        update_schema=AreaUpdate,
        response_schema=AreaResponse,
        pk_field="area_id",
        prefix="",
        resource_name="Area",
    )
)


@router.get("/{area_id}/signals", response_model=list[AreaSignalResponse])
async def list_area_signals(
    area_id: UUID,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all signals belonging to an area."""
    await get_or_404(session, Area, Area.area_id, area_id, "Area")

    result = await session.execute(
        select(SignalArea).where(SignalArea.area_id == area_id)
    )
    return result.scalars().all()


@router.post(
    "/{area_id}/signals",
    response_model=AreaSignalResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_area_signal(
    area_id: UUID,
    body: AreaSignalCreate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Add a signal to an area."""
    await get_or_404(session, Area, Area.area_id, area_id, "Area")
    await get_or_404(session, Signal, Signal.signal_id, body.signal_id, "Signal")

    sa = SignalArea(area_id=area_id, signal_id=body.signal_id)
    session.add(sa)
    await session.flush()

    return sa


@router.delete(
    "/{area_id}/signals/{signal_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_area_signal(
    area_id: UUID,
    signal_id: str,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Remove a signal from an area."""
    result = await session.execute(
        select(SignalArea).where(
            SignalArea.area_id == area_id,
            SignalArea.signal_id == signal_id,
        )
    )
    sa = result.scalar_one_or_none()

    if sa is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Signal {signal_id} not in area {area_id}",
        )

    await session.delete(sa)
