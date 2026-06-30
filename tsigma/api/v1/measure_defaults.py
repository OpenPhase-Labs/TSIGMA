"""
Measure Defaults API endpoints.

Admin CRUD over the ``measure_default`` table plus a read endpoint exposing
the effective (model-default overlaid with admin-default) tunable params per
report. Admin-defaultable params exclude the structural per-request params
(see ``STRUCTURAL_PARAMS``).
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_audited_session, get_session
from ...models import MeasureDefault
from ...reports.registry import ReportRegistry
from .reports import (
    HTTP_INVALID_REPORT_PARAMS,
    STRUCTURAL_PARAMS,
    _params_cls_for,
)

logger = logging.getLogger(__name__)

router = APIRouter()


class MeasureDefaultUpsert(BaseModel):
    """Request body for upserting an admin default value."""

    report_name: str
    param_name: str
    value: Any


def _report_cls_or_404(report_name: str) -> type:
    """Resolve a report class by name or raise 404."""
    try:
        return ReportRegistry.get(report_name)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report not found: {report_name}",
        ) from None


@router.post("/measure-defaults")
async def upsert_measure_default(
    body: MeasureDefaultUpsert,
    session: AsyncSession = Depends(get_audited_session),
    _: SessionData = Depends(require_admin),
):
    """Create or update an admin default for a report's tunable param."""
    report_cls = _report_cls_or_404(body.report_name)

    params_cls = _params_cls_for(report_cls)
    if params_cls is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Report has no typed params to default",
        )

    if body.param_name not in params_cls.model_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown param: {body.param_name}",
        )

    if body.param_name in STRUCTURAL_PARAMS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot default a structural param: {body.param_name}",
        )

    annotation = params_cls.model_fields[body.param_name].annotation
    try:
        TypeAdapter(annotation).validate_python(body.value)
    except ValidationError as exc:
        failures = [
            {"field": body.param_name, "message": err["msg"]}
            for err in exc.errors()
        ]
        raise HTTPException(
            status_code=HTTP_INVALID_REPORT_PARAMS,
            detail={"error": "invalid_report_params", "failures": failures},
        ) from exc

    row = MeasureDefault(
        report_name=body.report_name,
        param_name=body.param_name,
        value=body.value,
    )
    await session.merge(row)
    await session.flush()

    return {
        "report_name": body.report_name,
        "param_name": body.param_name,
        "value": body.value,
    }


@router.delete("/measure-defaults/{report_name}/{param_name}")
async def delete_measure_default(
    report_name: str,
    param_name: str,
    session: AsyncSession = Depends(get_audited_session),
    _: SessionData = Depends(require_admin),
):
    """Delete an admin default for a report's param."""
    result = await session.execute(
        select(MeasureDefault).where(
            MeasureDefault.report_name == report_name,
            MeasureDefault.param_name == param_name,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Measure default not found: {report_name}/{param_name}",
        )

    await session.delete(row)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/measure-defaults")
async def list_measure_defaults(
    report_name: str | None = Query(None, description="Filter by report"),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("reports")),
):
    """List admin defaults, optionally filtered by report name."""
    stmt = select(MeasureDefault)
    if report_name is not None:
        stmt = stmt.where(MeasureDefault.report_name == report_name)
    rows = (await session.execute(stmt)).scalars().all()
    return [
        {
            "report_name": row.report_name,
            "param_name": row.param_name,
            "value": row.value,
        }
        for row in rows
    ]


@router.get("/reports/{report_name}/effective-defaults")
async def effective_defaults(
    report_name: str,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("reports")),
):
    """Return effective tunable defaults (admin overlay over model defaults)."""
    report_cls = _report_cls_or_404(report_name)

    params_cls = _params_cls_for(report_cls)
    if params_cls is None:
        return {"report_name": report_name, "effective_defaults": {}}

    rows = (
        await session.execute(
            select(MeasureDefault).where(
                MeasureDefault.report_name == report_name
            )
        )
    ).scalars().all()
    admin = {row.param_name: row.value for row in rows}

    effective: dict[str, Any] = {}
    for name, field in params_cls.model_fields.items():
        if name in STRUCTURAL_PARAMS:
            continue
        if name in admin:
            effective[name] = admin[name]
        elif not field.is_required():
            effective[name] = field.get_default()
        # else: required tunable with no admin/code default -> skip

    return {"report_name": report_name, "effective_defaults": effective}
