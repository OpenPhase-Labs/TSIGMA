"""
Routes API endpoints.

CRUD operations for progression/coordination routes and their
constituent signals, phases, and inter-signal distances.
GET endpoints respect the 'signal_detail' access policy. POST/PUT/DELETE require admin role.
"""

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_session
from ...models import Route, RouteDistance, RoutePhase, RouteSignal, Signal
from .crud_factory import crud_router
from .helpers import get_or_404
from .schemas import UPDATE_REQUIRED_MSG

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route schemas
# ---------------------------------------------------------------------------


class RouteCreate(BaseModel):
    """Schema for creating a route."""

    name: str = Field(..., min_length=1)


class RouteUpdate(BaseModel):
    """Schema for updating a route (partial update)."""

    name: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "RouteUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class RouteResponse(BaseModel):
    """Route data returned in API responses."""

    model_config = {"from_attributes": True}

    route_id: UUID
    name: str


# ---------------------------------------------------------------------------
# RouteSignal schemas
# ---------------------------------------------------------------------------


class RouteSignalCreate(BaseModel):
    """Schema for adding a signal to a route."""

    signal_id: str = Field(..., min_length=1)
    sequence_order: int = Field(..., ge=1)


class RouteSignalUpdate(BaseModel):
    """Schema for updating a route signal (partial update)."""

    signal_id: Optional[str] = Field(None, min_length=1)
    sequence_order: Optional[int] = Field(None, ge=1)

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "RouteSignalUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class RouteSignalResponse(BaseModel):
    """Route signal data returned in API responses."""

    model_config = {"from_attributes": True}

    route_signal_id: UUID
    route_id: UUID
    signal_id: str
    sequence_order: int


# ---------------------------------------------------------------------------
# RoutePhase schemas
# ---------------------------------------------------------------------------


class RoutePhaseCreate(BaseModel):
    """Schema for adding a phase to a route signal."""

    phase_number: int = Field(..., ge=1)
    direction_type_id: int = Field(..., ge=1)
    is_overlap: bool = False
    is_primary_approach: bool = False


class RoutePhaseUpdate(BaseModel):
    """Schema for updating a route phase (partial update)."""

    phase_number: Optional[int] = Field(None, ge=1)
    direction_type_id: Optional[int] = Field(None, ge=1)
    is_overlap: Optional[bool] = None
    is_primary_approach: Optional[bool] = None

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "RoutePhaseUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class RoutePhaseResponse(BaseModel):
    """Route phase data returned in API responses."""

    model_config = {"from_attributes": True}

    route_phase_id: UUID
    route_signal_id: UUID
    phase_number: int
    direction_type_id: int
    is_overlap: bool
    is_primary_approach: bool


# ---------------------------------------------------------------------------
# RouteDistance schemas
# ---------------------------------------------------------------------------


class RouteDistanceCreate(BaseModel):
    """Schema for creating a distance between two route signals."""

    from_route_signal_id: UUID
    to_route_signal_id: UUID
    distance_feet: int = Field(..., ge=0)
    travel_time_seconds: Optional[int] = Field(None, ge=0)


class RouteDistanceUpdate(BaseModel):
    """Schema for updating a route distance (partial update)."""

    from_route_signal_id: Optional[UUID] = None
    to_route_signal_id: Optional[UUID] = None
    distance_feet: Optional[int] = Field(None, ge=0)
    travel_time_seconds: Optional[int] = Field(None, ge=0)

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "RouteDistanceUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class RouteDistanceResponse(BaseModel):
    """Route distance data returned in API responses."""

    model_config = {"from_attributes": True}

    route_distance_id: UUID
    from_route_signal_id: UUID
    to_route_signal_id: UUID
    distance_feet: int
    travel_time_seconds: Optional[int] = None


# ===========================================================================
# Assemble router — factory handles standard CRUD, custom endpoints below
# ===========================================================================

router = APIRouter()

# Route: full CRUD via factory
router.include_router(
    crud_router(
        model=Route,
        create_schema=RouteCreate,
        update_schema=RouteUpdate,
        response_schema=RouteResponse,
        pk_field="route_id",
        prefix="/routes",
        resource_name="Route",
    )
)

# RouteSignal: update/delete via factory, custom nested list/create below
router.include_router(
    crud_router(
        model=RouteSignal,
        update_schema=RouteSignalUpdate,
        response_schema=RouteSignalResponse,
        pk_field="route_signal_id",
        prefix="/route-signals",
        resource_name="RouteSignal",
        operations={"get", "update", "delete"},
    )
)

# RoutePhase: update/delete via factory, custom nested list/create below
router.include_router(
    crud_router(
        model=RoutePhase,
        update_schema=RoutePhaseUpdate,
        response_schema=RoutePhaseResponse,
        pk_field="route_phase_id",
        prefix="/route-phases",
        resource_name="RoutePhase",
        operations={"get", "update", "delete"},
    )
)

# RouteDistance: update/delete via factory, custom list/create below
router.include_router(
    crud_router(
        model=RouteDistance,
        update_schema=RouteDistanceUpdate,
        response_schema=RouteDistanceResponse,
        pk_field="route_distance_id",
        prefix="/route-distances",
        resource_name="RouteDistance",
        operations={"get", "update", "delete"},
    )
)


# ===========================================================================
# Custom nested endpoints (list/create with parent validation)
# ===========================================================================


@router.get(
    "/routes/{route_id}/signals",
    response_model=list[RouteSignalResponse],
)
async def list_route_signals(
    route_id: UUID,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all signals in a route, ordered by sequence."""
    await get_or_404(session, Route, Route.route_id, route_id, "Route")

    result = await session.execute(
        select(RouteSignal)
        .where(RouteSignal.route_id == route_id)
        .order_by(RouteSignal.sequence_order.asc())
    )
    return result.scalars().all()


@router.post(
    "/routes/{route_id}/signals",
    response_model=RouteSignalResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_route_signal(
    route_id: UUID,
    body: RouteSignalCreate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Add a signal to a route."""
    await get_or_404(session, Route, Route.route_id, route_id, "Route")
    await get_or_404(session, Signal, Signal.signal_id, body.signal_id, "Signal")

    route_signal = RouteSignal(
        route_id=route_id,
        signal_id=body.signal_id,
        sequence_order=body.sequence_order,
    )
    session.add(route_signal)
    await session.flush()

    return route_signal


@router.get(
    "/route-signals/{route_signal_id}/phases",
    response_model=list[RoutePhaseResponse],
)
async def list_route_phases(
    route_signal_id: UUID,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all phases for a route signal."""
    await get_or_404(session, RouteSignal, RouteSignal.route_signal_id, route_signal_id, "RouteSignal")

    result = await session.execute(
        select(RoutePhase).where(
            RoutePhase.route_signal_id == route_signal_id
        )
    )
    return result.scalars().all()


@router.post(
    "/route-signals/{route_signal_id}/phases",
    response_model=RoutePhaseResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_route_phase(
    route_signal_id: UUID,
    body: RoutePhaseCreate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Add a phase to a route signal."""
    await get_or_404(session, RouteSignal, RouteSignal.route_signal_id, route_signal_id, "RouteSignal")

    route_phase = RoutePhase(
        route_signal_id=route_signal_id,
        phase_number=body.phase_number,
        direction_type_id=body.direction_type_id,
        is_overlap=body.is_overlap,
        is_primary_approach=body.is_primary_approach,
    )
    session.add(route_phase)
    await session.flush()

    return route_phase


@router.get(
    "/routes/{route_id}/distances",
    response_model=list[RouteDistanceResponse],
)
async def list_route_distances(
    route_id: UUID,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all distances between signals in a route."""
    await get_or_404(session, Route, Route.route_id, route_id, "Route")

    from_signal = select(RouteSignal.route_signal_id).where(
        RouteSignal.route_id == route_id
    ).scalar_subquery()

    result = await session.execute(
        select(RouteDistance).where(
            RouteDistance.from_route_signal_id.in_(from_signal)
        )
    )
    return result.scalars().all()


@router.post(
    "/route-distances/",
    response_model=RouteDistanceResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_route_distance(
    body: RouteDistanceCreate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Create a distance record between two route signals."""
    await get_or_404(session, RouteSignal, RouteSignal.route_signal_id, body.from_route_signal_id, "RouteSignal")
    await get_or_404(session, RouteSignal, RouteSignal.route_signal_id, body.to_route_signal_id, "RouteSignal")

    route_distance = RouteDistance(
        from_route_signal_id=body.from_route_signal_id,
        to_route_signal_id=body.to_route_signal_id,
        distance_feet=body.distance_feet,
        travel_time_seconds=body.travel_time_seconds,
    )
    session.add(route_distance)
    await session.flush()

    return route_distance
