"""
Corridors API endpoints.

CRUD operations for corridor groupings (e.g., "Peachtree Street Corridor").
GET endpoints respect the 'signal_detail' access policy. POST/PUT/DELETE require admin role.
"""

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_session
from ...models import Corridor, Jurisdiction
from .crud_factory import crud_router
from .schemas import UPDATE_REQUIRED_MSG

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class CorridorCreate(BaseModel):
    name: str = Field(..., min_length=1)
    description: Optional[str] = None
    jurisdiction_id: Optional[UUID] = None


class CorridorUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1)
    description: Optional[str] = None
    jurisdiction_id: Optional[UUID] = None

    @model_validator(mode="before")
    @classmethod
    def at_least_one_field(cls, values):
        """Require at least one field to be provided for an update."""
        if not any(v is not None for k, v in values.items()):
            raise ValueError(UPDATE_REQUIRED_MSG)
        return values


class CorridorResponse(BaseModel):
    model_config = {"from_attributes": True}

    corridor_id: UUID
    name: str
    description: Optional[str] = None
    jurisdiction_id: Optional[UUID] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _validate_jurisdiction(
    session: AsyncSession,
    jurisdiction_id: UUID | None,
) -> None:
    """Raise 404 if *jurisdiction_id* is provided but does not exist."""
    if jurisdiction_id is None:
        return
    result = await session.execute(
        select(Jurisdiction).where(
            Jurisdiction.jurisdiction_id == jurisdiction_id
        )
    )
    if result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Jurisdiction {jurisdiction_id} not found",
        )


# ---------------------------------------------------------------------------
# Router — factory handles get/delete, custom list/create/update below
# ---------------------------------------------------------------------------

router = APIRouter()

router.include_router(
    crud_router(
        model=Corridor,
        update_schema=CorridorUpdate,
        response_schema=CorridorResponse,
        pk_field="corridor_id",
        prefix="",
        resource_name="Corridor",
        operations={"get", "delete"},
    )
)


@router.get("/", response_model=list[CorridorResponse])
async def list_corridors(
    jurisdiction_id: Optional[str] = Query(
        None, description="Filter corridors by jurisdiction UUID."
    ),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all corridors, optionally filtered by jurisdiction."""
    stmt = select(Corridor)
    if jurisdiction_id is not None:
        stmt = stmt.where(Corridor.jurisdiction_id == jurisdiction_id)
    result = await session.execute(stmt)
    return result.scalars().all()


@router.post(
    "/",
    response_model=CorridorResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_corridor(
    body: CorridorCreate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Create a new corridor (validates jurisdiction if provided)."""
    await _validate_jurisdiction(session, body.jurisdiction_id)

    corridor = Corridor(
        name=body.name,
        description=body.description,
        jurisdiction_id=body.jurisdiction_id,
    )
    session.add(corridor)
    await session.flush()

    return corridor


@router.put(
    "/{corridor_id}",
    response_model=CorridorResponse,
)
async def update_corridor(
    corridor_id: str,
    body: CorridorUpdate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Update an existing corridor (validates jurisdiction if changed)."""
    result = await session.execute(
        select(Corridor).where(Corridor.corridor_id == corridor_id)
    )
    corridor = result.scalar_one_or_none()

    if corridor is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Corridor {corridor_id} not found",
        )

    update_data = body.model_dump(exclude_unset=True)

    if "jurisdiction_id" in update_data:
        await _validate_jurisdiction(session, update_data["jurisdiction_id"])

    for field, value in update_data.items():
        setattr(corridor, field, value)

    await session.flush()

    return corridor
