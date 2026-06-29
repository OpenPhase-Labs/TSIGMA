"""
Metric types API endpoints.

CRUD operations for the MetricType reference table (key, label, display_order).
GET endpoints respect the 'reports' access policy. POST/PUT/DELETE require admin role.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_session
from ...models import MetricType
from ...reports.registry import ReportRegistry
from .crud_factory import crud_router
from .schemas import UPDATE_REQUIRED_MSG, MetricTypeItem

# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class MetricTypeCreate(BaseModel):
    key: str = Field(..., min_length=1)
    label: str = Field(..., min_length=1)
    display_order: int


class MetricTypeUpdate(BaseModel):
    label: Optional[str] = Field(None, min_length=1)
    display_order: Optional[int] = None

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "MetricTypeUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


# ---------------------------------------------------------------------------
# Router — factory handles get/delete, custom list/create/update below
# ---------------------------------------------------------------------------

router = APIRouter()


router.include_router(
    crud_router(
        model=MetricType,
        response_schema=MetricTypeItem,
        pk_field="key",
        prefix="",
        resource_name="MetricType",
        operations={"get", "delete"},
        user_supplied_pk=True,
        read_scope="reports",
    )
)


@router.get("/", response_model=list[MetricTypeItem])
async def list_metric_types(
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("reports")),
):
    """List all metric types ordered by display order."""
    result = await session.execute(
        select(MetricType).order_by(MetricType.display_order)
    )
    return result.scalars().all()


@router.post(
    "/",
    response_model=MetricTypeItem,
    status_code=status.HTTP_201_CREATED,
)
async def create_metric_type(
    body: MetricTypeCreate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Create a new metric type (validates the key against the report registry)."""
    if body.key not in ReportRegistry.list_all():
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unknown metric type key '{body.key}'"
                " - no matching report in the registry"
            ),
        )

    metric = MetricType(
        key=body.key,
        label=body.label,
        display_order=body.display_order,
    )
    session.add(metric)
    await session.flush()

    return metric


@router.put("/{key}", response_model=MetricTypeItem)
async def update_metric_type(
    key: str,
    body: MetricTypeUpdate,
    session: AsyncSession = Depends(get_session),
    _: SessionData = Depends(require_admin),
):
    """Update an existing metric type."""
    result = await session.execute(
        select(MetricType).where(MetricType.key == key)
    )
    metric = result.scalar_one_or_none()

    if metric is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MetricType {key} not found",
        )

    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(metric, field, value)

    await session.flush()

    return metric
