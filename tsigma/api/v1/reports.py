"""
Reports API endpoints.

Provides endpoints to list, execute, and export TSIGMA report plugins.
"""

import json
import logging
import typing

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access
from ...auth.sessions import SessionData
from ...dependencies import get_session
from ...models import MeasureDefault
from ...plugins.credentials import set_invocation_caller
from ...reports.registry import ReportRegistry, ReportResourceNotFoundError

logger = logging.getLogger(__name__)

# Custom 4xx for report param-validation failure, kept distinct from the
# gating 422 a report may return via ``preferred_http_status``.
HTTP_INVALID_REPORT_PARAMS = 463

# Params that are ALWAYS supplied per-request and must NEVER be filled from
# an admin ``measure_default`` row, even if a stray row exists for one.
STRUCTURAL_PARAMS = frozenset({"signal_id", "start", "end"})

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


def _validate_params(report_cls: type, params):
    """Validate raw params into the report's declared Pydantic model.

    Returns a validated model instance for typed reports. Untyped/legacy
    reports (``_params_cls_for`` -> ``None``) get the raw dict passed
    through unchanged for backward compatibility. A validation failure
    raises ``HTTPException(463)`` with a structured failures body.
    """
    params_cls = _params_cls_for(report_cls)
    if params_cls is None:
        return params  # untyped/legacy report: pass the raw dict through
    try:
        return params_cls.model_validate(params)
    except ValidationError as exc:
        failures = [
            {"field": ".".join(str(p) for p in err["loc"]), "message": err["msg"]}
            for err in exc.errors()
        ]
        raise HTTPException(
            status_code=HTTP_INVALID_REPORT_PARAMS,
            detail={"error": "invalid_report_params", "failures": failures},
        ) from exc


def _remote_params_schema(report_cls) -> dict | None:
    """The plugin-declared param schema for a remote report, else ``None``.

    A gRPC report does not parameterise ``Report[TParams]`` - it carries the
    plugin's schema from ``Describe``. Duck-typed rather than imported so this
    module keeps no dependency on the plugin host.
    """
    schema = getattr(report_cls, "params_schema", None)
    return schema if isinstance(schema, dict) and schema else None


def _validate_remote_params(schema: dict, params, report_name: str):
    """Check required keys against the plugin's declared schema.

    The contract has the host check required keys before ``Generate``
    (report.proto, ``DescribeResponse.params``). Type and range checking stay
    with the plugin, which owns its typed params struct.
    """
    missing = [
        name for name in schema.get("required", ())
        if name not in params or params[name] is None
    ]
    if missing:
        raise HTTPException(
            status_code=HTTP_INVALID_REPORT_PARAMS,
            detail={
                "error": "invalid_report_params",
                "failures": [
                    {"field": name, "message": "field required"} for name in missing
                ],
            },
        )
    return params


async def _resolve_and_validate_params(report_cls, report_name, params, session):
    """Resolve admin defaults then validate at the report chokepoint.

    Precedence is ``per-request value -> admin default -> code default``:
    admin ``measure_default`` rows (keyed by ``report_name``) are merged
    UNDER the per-request ``params``, then validated. Structural params (see
    ``STRUCTURAL_PARAMS``) are never admin-defaulted.

    Three report shapes, one precedence rule:

    * typed in-process (``Report[TParams]``) - validated into the Pydantic
      model, whose field defaults supply the code-default layer;
    * remote gRPC (``params_schema`` from ``Describe``) - required keys checked
      host-side; the plugin's own struct supplies the code-default layer;
    * untyped/legacy - raw-dict passthrough, and NO admin defaults, since there
      is no declared param surface to key them against.

    Validation failures raise ``HTTPException(463)``.
    """
    params_cls = _params_cls_for(report_cls)
    remote_schema = _remote_params_schema(report_cls)
    if params_cls is None and remote_schema is None:
        return params  # untyped/legacy report: no declared params to default

    rows = (
        await session.execute(
            select(MeasureDefault).where(MeasureDefault.report_name == report_name)
        )
    ).scalars().all()
    admin_defaults = {
        row.param_name: row.value
        for row in rows
        if row.param_name not in STRUCTURAL_PARAMS
    }
    merged = {**admin_defaults, **params}
    if remote_schema is not None:
        return _validate_remote_params(remote_schema, merged, report_name)
    return _validate_params(report_cls, merged)


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
            "description": cls.metadata.description,
            "category": cls.metadata.category,
            "estimated_time": cls.metadata.estimated_time,
            "export_formats": cls.metadata.export_formats,
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
        ) from None

    params_cls = _params_cls_for(report_cls)
    if params_cls is None:
        return {"name": report_name, "schema": None}
    return {"name": report_name, "schema": params_cls.model_json_schema()}


@router.post("/reports/{report_name}")
async def run_report(
    report_name: str,
    params: dict,
    request: Request,
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
        ) from None

    # Resolve admin defaults and validate at the chokepoint BEFORE the broad
    # try/except so a 463 (invalid params) is not swallowed and re-raised as
    # a 500.
    params = await _resolve_and_validate_params(
        report_cls, report_name, params, session
    )

    # Lend this request's identity to a plugin, if the report turns out to be
    # one. An in-process report ignores it; a RemoteReport mints a credential
    # from it and revokes it when the stream ends.
    set_invocation_caller(
        _access if isinstance(_access, SessionData) else None,
        getattr(request.app.state, "session_store", None),
    )

    try:
        report = report_cls()
        result = await report.execute(params, session)
    except ReportResourceNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Missing required parameter: {exc}",
        ) from exc
    except Exception:
        logger.exception("Report '%s' failed", report_name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Report execution failed: {report_name}",
        ) from None

    # Report-level hook: let gating reports override the HTTP status
    # (e.g. the left-turn-gap-data-check uses 422 when overall_ready is
    # False so clients can route on status without parsing the body).
    preferred = report_cls.preferred_http_status(result)
    # to_json without path_or_buf always returns a str at runtime; the
    # `or "[]"` is a defensive default that also gives type-checkers a
    # non-None operand for json.loads.
    body = {
        "status": "complete",
        "data": json.loads(
            result.to_json(orient="records", date_format="iso") or "[]"
        ),
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
        ) from None

    # Resolve admin defaults and validate at the chokepoint BEFORE the broad
    # try/except so a 463 (invalid params) is not swallowed and re-raised as
    # a 500.
    params = await _resolve_and_validate_params(
        report_cls, report_name, params, session
    )

    try:
        report = report_cls()
        data = await report.export(params, session, format=format)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Missing required parameter: {exc}",
        ) from exc
    except Exception:
        logger.exception("Report export '%s' failed", report_name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Report export failed: {report_name}",
        ) from None

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
