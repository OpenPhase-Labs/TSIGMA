"""
Reports API endpoints.

Provides endpoints to list, execute, and export TSIGMA report plugins.
"""

import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access
from ...dependencies import get_session
from ...reports.registry import ReportRegistry, ReportResourceNotFoundError

logger = logging.getLogger(__name__)

router = APIRouter()


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
    body = {"status": "complete", "data": json.loads(result.to_json(orient="records", date_format="iso"))}
    if preferred is not None and preferred != status.HTTP_200_OK:
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
