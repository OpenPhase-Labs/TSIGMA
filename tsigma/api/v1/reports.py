"""
Reports API endpoints.

Provides endpoints to list, execute, and export TSIGMA report plugins.
"""

import json
import logging
import typing

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access
from ...dependencies import get_session
from ...reports.registry import ReportRegistry, ReportResourceNotFoundError

logger = logging.getLogger(__name__)

router = APIRouter()


def _params_cls_for(report_cls: type) -> type[BaseModel] | None:
    """Extract the Pydantic params class declared by a ``Report[TParams]``.

    Returns ``None`` if the report did not parameterise its base class with
    a ``BaseModel`` subclass (e.g., a hypothetical report that takes raw
    dict params and does its own validation).
    """
    for base in getattr(report_cls, "__orig_bases__", ()):
        for arg in typing.get_args(base):
            if isinstance(arg, type) and issubclass(arg, BaseModel):
                return arg
    return None


@router.get("/reports")
async def list_reports(
    _access=Depends(require_access("reports")),
):
    """
    List all available reports with metadata.

    Returns:
        List of report descriptors including name, description, category,
        estimated_time, and export_formats.
    """
    reports = ReportRegistry.list_all()
    return [
        {
            "name": name,
            "description": cls.description,
            "category": cls.category,
            "estimated_time": cls.estimated_time,
            "export_formats": cls.export_formats,
        }
        for name, cls in reports.items()
    ]


@router.get("/reports/{report_name}/schema")
async def report_schema(
    report_name: str,
    _access=Depends(require_access("reports")),
):
    """
    Return the JSON schema for a report's parameters.

    Used by the UI to render a dynamic parameter form instead of the
    previous fixed ``signal_id + start_date + end_date + phase_number``
    shape which did not match most reports' actual param names or types.

    Response shape::

        {
            "name": "arrival-on-red",
            "schema": { ...Pydantic v2 JSON schema... }
        }

    ``schema`` is ``None`` when the report does not declare a Pydantic
    params class — callers should fall back to an unstructured text
    editor or skip the report.
    """
    try:
        report_cls = ReportRegistry.get(report_name)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report not found: {report_name}",
        )

    params_cls = _params_cls_for(report_cls)
    if params_cls is None:
        return {"name": report_name, "schema": None}
    return {"name": report_name, "schema": params_cls.model_json_schema()}


@router.post("/reports/{report_name}")
async def run_report(
    report_name: str,
    params: dict,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("reports")),
):
    """
    Execute a report and return its results.

    Args:
        report_name: Registered report identifier.
        params: Report-specific parameters (signal_id, start, end, etc.).
        session: Database session (injected).

    Returns:
        Dict with status and report data.
    """
    try:
        report_cls = ReportRegistry.get(report_name)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report not found: {report_name}",
        )

    try:
        report = report_cls()
        result = await report.execute(params, session)
    except ReportResourceNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Missing required parameter: {exc}",
        )
    except Exception:
        logger.exception("Report '%s' failed", report_name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Report execution failed: {report_name}",
        )

    # Report-level hook: let gating reports override the HTTP status
    # (e.g. the left-turn-gap-data-check uses 422 when overall_ready is
    # False so clients can route on status without parsing the body).
    preferred = report_cls.preferred_http_status(result)
    body = {
        "status": "complete",
        "data": json.loads(result.to_json(orient="records", date_format="iso")),
    }
    if isinstance(preferred, int) and preferred != status.HTTP_200_OK:
        return JSONResponse(status_code=preferred, content=body)
    return body


@router.post("/reports/{report_name}/export")
async def export_report(
    report_name: str,
    params: dict,
    format: str = Query("csv"),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("reports")),
):
    """
    Export report results as a downloadable file.

    Args:
        report_name: Registered report identifier.
        params: Report-specific parameters.
        format: Export format ('csv' or 'json').
        session: Database session (injected).

    Returns:
        Response with file content and appropriate headers.
    """
    try:
        report_cls = ReportRegistry.get(report_name)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report not found: {report_name}",
        )

    try:
        report = report_cls()
        data = await report.export(params, session, format=format)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Missing required parameter: {exc}",
        )
    except Exception:
        logger.exception("Report export '%s' failed", report_name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Report export failed: {report_name}",
        )

    content_types = {
        "csv": "text/csv",
        "json": "application/json",
        "ndjson": "application/x-ndjson",
    }
    content_type = content_types.get(format, "application/octet-stream")
    filename = f"{report_name}.{format}"

    return Response(
        content=data,
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
