"""
Jurisdictions API endpoints.

CRUD operations for jurisdictional boundaries (cities, counties, MPOs).
GET endpoints respect the 'signal_detail' access policy. POST/PUT/DELETE require admin role.
"""

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access
from ...dependencies import get_session
from ...models import Jurisdiction
from .crud_factory import crud_router
from .schemas import JurisdictionCreate, JurisdictionResponse, JurisdictionUpdate

router = APIRouter()

# Factory handles get/create/update/delete; custom list below for pagination.
router.include_router(
    crud_router(
        model=Jurisdiction,
        create_schema=JurisdictionCreate,
        update_schema=JurisdictionUpdate,
        response_schema=JurisdictionResponse,
        pk_field="jurisdiction_id",
        prefix="",
        resource_name="Jurisdiction",
        operations={"get", "create", "update", "delete"},
    )
)


@router.get("/", response_model=list[JurisdictionResponse])
async def list_jurisdictions(
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("signal_detail")),
):
    """List all jurisdictions with pagination."""
    result = await session.execute(
        select(Jurisdiction).offset(skip).limit(limit)
    )
    return result.scalars().all()
