"""
Regions API endpoints.

CRUD operations for regional hierarchy (State > District > Zone).
GET endpoints respect the 'signal_detail' access policy. POST/PUT/DELETE require admin role.
"""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_session
from ...models import Region
from .crud_factory import crud_router
from .schemas import UPDATE_REQUIRED_MSG

# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class RegionCreate(BaseModel):
    description: str = Field(..., min_length=1)
    parent_region_id: Optional[UUID] = None


class RegionUpdate(BaseModel):
    description: Optional[str] = Field(None, min_length=1)
    parent_region_id: Optional[UUID] = None

    @model_validator(mode="before")
    @classmethod
    def at_least_one_field(cls, values):
        """Ensure at least one field is provided for update."""
        if not any(v is not None for k, v in values.items()):
            raise ValueError(UPDATE_REQUIRED_MSG)
        return values


class RegionResponse(BaseModel):
    model_config = {"from_attributes": True}

    region_id: UUID
    parent_region_id: Optional[UUID] = None
    description: str


# ---------------------------------------------------------------------------
# Router — factory handles get/delete, custom list/create/update below
# ---------------------------------------------------------------------------

router = APIRouter()

router.include_router(
    crud_router(
        model=Region,
        update_schema=RegionUpdate,
        response_schema=RegionResponse,
        pk_field="region_id",
        prefix="",
        resource_name="Region",
        operations={"get", "delete"},
    )
)


@router.get("/", response_model=list[RegionResponse])
async def list_regions(
    parent_id: Optional[str] = Query(
        None, description="Filter by parent region ID"
    ),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all regions, optionally filtered by parent."""
    stmt = select(Region)
    if parent_id is not None:
        stmt = stmt.where(Region.parent_region_id == parent_id)
    result = await session.execute(stmt)
    return result.scalars().all()


@router.post(
    "/",
    response_model=RegionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_region(
    body: RegionCreate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Create a new region (validates parent if provided)."""
    if body.parent_region_id is not None:
        parent = await session.execute(
            select(Region).where(
                Region.region_id == body.parent_region_id
            )
        )
        if parent.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Parent region {body.parent_region_id} not found",
            )

    region = Region(
        description=body.description,
        parent_region_id=body.parent_region_id,
    )
    session.add(region)
    await session.flush()

    return region


@router.put(
    "/{region_id}",
    response_model=RegionResponse,
)
async def update_region(
    region_id: str,
    body: RegionUpdate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Update an existing region (validates parent if changed)."""
    result = await session.execute(
        select(Region).where(Region.region_id == region_id)
    )
    region = result.scalar_one_or_none()

    if region is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Region {region_id} not found",
        )

    update_data = body.model_dump(exclude_unset=True)

    if (
        "parent_region_id" in update_data
        and update_data["parent_region_id"] is not None
    ):
        parent = await session.execute(
            select(Region).where(
                Region.region_id == update_data["parent_region_id"]
            )
        )
        if parent.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"Parent region"
                    f" {update_data['parent_region_id']} not found"
                ),
            )

    for field, value in update_data.items():
        setattr(region, field, value)

    await session.flush()

    return region
