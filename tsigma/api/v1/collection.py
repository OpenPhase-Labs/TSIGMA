"""
Collection and ingestion API endpoints.

On-demand poll triggers:
1. SOAP endpoint — backward-compatible with ATSPM 4.x WCF
   UploadControllerData contract.
2. REST endpoint — modern JSON interface for new integrations.

Ingestion status:
3. Polling checkpoint read endpoints for monitoring ingestion state.

Poll triggers fire-and-forget via asyncio.create_task() and return
immediately (202 Accepted / SOAP Accepted).
"""

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import defusedxml.ElementTree as ET
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...dependencies import get_session
from ...models.checkpoint import PollingCheckpoint
from ...models.event import ControllerEventLog
from ...models.signal import Signal

logger = logging.getLogger(__name__)

router = APIRouter()

# Background poll tasks — stored to prevent premature garbage collection.
# Tasks remove themselves from the set on completion.
_background_tasks: set[asyncio.Task] = set()


def _fire_and_forget(coro) -> None:
    """Schedule a coroutine as a background task, preventing GC."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


# SOAP namespace used by ATSPM 4.x WCF contract
_SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
_TEMPURI_NS = "http://tempuri.org/"


# ---------------------------------------------------------------------------
# In-memory rate limiter for poll trigger endpoints
# ---------------------------------------------------------------------------

class _RateLimiter:
    """
    Simple sliding-window rate limiter keyed by client IP.

    Not shared across processes — sufficient for preventing accidental
    rapid-fire clicks, not intended as a DDoS defence.
    """

    def __init__(self, max_calls: int, window_seconds: float) -> None:
        self._max_calls = max_calls
        self._window = window_seconds
        self._hits: dict[str, list[float]] = defaultdict(list)

    def check(self, key: str) -> bool:
        """Return True if the request is allowed, False if rate-limited."""
        now = time.monotonic()
        window_start = now - self._window
        # Prune expired timestamps
        self._hits[key] = [t for t in self._hits[key] if t > window_start]
        if len(self._hits[key]) >= self._max_calls:
            return False
        self._hits[key].append(now)
        return True


# 5 poll triggers per signal per 60 seconds — prevents click-spam
_poll_limiter = _RateLimiter(max_calls=5, window_seconds=60)


# ---------------------------------------------------------------------------
# REST schema
# ---------------------------------------------------------------------------


class PollRequest(BaseModel):
    """Request body for the REST on-demand poll endpoint."""

    method: str = Field(
        ...,
        min_length=1,
        description="Polling method name (e.g., 'ftp_pull', 'http_pull').",
    )


class BulkTimestampCorrectionRequest(BaseModel):
    """Request body for bulk timestamp correction of poisoned event data."""

    signal_id: str = Field(
        ..., min_length=1, description="Traffic signal identifier."
    )
    start_time: datetime = Field(
        ..., description="Start of affected time range (UTC, inclusive)."
    )
    end_time: datetime = Field(
        ..., description="End of affected time range (UTC, inclusive)."
    )
    offset_seconds: float = Field(
        ...,
        description=(
            "Offset to apply in seconds. Negative values shift timestamps "
            "backward (the common case for future-dated events)."
        ),
    )


class AnchorCorrectionRequest(BaseModel):
    """
    Convenience wrapper for bulk timestamp correction.

    Operator identifies a known-good event and its real-world timestamp.
    The system computes the offset and applies it to all events in the
    affected range.
    """

    signal_id: str = Field(
        ..., min_length=1, description="Traffic signal identifier."
    )
    event_time: datetime = Field(
        ..., description="The recorded (incorrect) timestamp of a known event."
    )
    actual_time: datetime = Field(
        ..., description="The real-world timestamp of that event."
    )
    start_time: datetime = Field(
        ..., description="Start of affected time range (UTC, inclusive)."
    )
    end_time: datetime = Field(
        ..., description="End of affected time range (UTC, inclusive)."
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_collector(request: Request):
    """
    Get CollectorService from app state.

    Args:
        request: FastAPI request (carries app reference).

    Returns:
        CollectorService instance.

    Raises:
        HTTPException: 503 if collector is not running.
    """
    collector = getattr(request.app.state, "collector", None)
    if collector is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Collector service is not running",
        )
    return collector


def _parse_soap_envelope(body: bytes) -> dict[str, str]:
    """
    Parse UploadControllerData parameters from a SOAP envelope.

    Extracts all child elements of <UploadControllerData> as a flat
    dict of tag -> text. Missing elements get empty strings.

    Args:
        body: Raw XML bytes from the request.

    Returns:
        Dict of parameter name -> value string.

    Raises:
        HTTPException: 400 if the XML is malformed or missing
            the UploadControllerData element.
    """
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Malformed XML: {exc}",
        )

    # Find <UploadControllerData> inside <s:Body>
    body_el = root.find(f"{{{_SOAP_NS}}}Body")
    if body_el is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing SOAP Body element",
        )

    upload_el = body_el.find(f"{{{_TEMPURI_NS}}}UploadControllerData")
    if upload_el is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing UploadControllerData element",
        )

    params: dict[str, str] = {}
    for child in upload_el:
        # Strip namespace prefix to get plain tag name
        tag = child.tag
        if "}" in tag:
            tag = tag.split("}", 1)[1]
        params[tag] = (child.text or "").strip()

    return params


def _build_soap_response(signal_id: str, soap_status: str) -> str:
    """
    Build a SOAP response envelope for UploadControllerData.

    Args:
        signal_id: Traffic signal identifier.
        soap_status: Status string (e.g., "Accepted", "Error").

    Returns:
        SOAP XML response string.
    """
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        f'<s:Envelope xmlns:s="{_SOAP_NS}">'
        "<s:Body>"
        f'<UploadControllerDataResponse xmlns="{_TEMPURI_NS}">'
        f"<Status>{soap_status}</Status>"
        f"<SignalID>{signal_id}</SignalID>"
        "</UploadControllerDataResponse>"
        "</s:Body>"
        "</s:Envelope>"
    )


# ---------------------------------------------------------------------------
# SOAP endpoint — ATSPM 4.x WCF compatible
# ---------------------------------------------------------------------------


_SOAP_MEDIA_TYPE = "text/xml"


@router.post(
    "/soap/GetControllerData",
    dependencies=[Depends(require_admin)],
)
async def soap_upload_controller_data(request: Request):
    """
    SOAP endpoint for ATSPM 4.x WCF UploadControllerData compatibility.

    Accepts the same SOAP envelope that ATSPM 4.x WCF expects. DOTs
    point their existing SOAP clients at TSIGMA with zero code changes.

    Requires admin authentication.

    Ignored parameters (accepted for compatibility, logged):
        DeleteFiles, SNMPRetry, SNMPTimeout, SNMPPort, LocalDir,
        WaitBetweenRecords, BulkCopyOptions.

    Args:
        request: FastAPI request containing SOAP XML body.

    Returns:
        SOAP XML response with status and signal ID.
    """
    collector = _get_collector(request)

    body = await request.body()
    params = _parse_soap_envelope(body)

    signal_id = params.get("SignalID", "")
    if not signal_id:
        return Response(
            content=_build_soap_response("", "Error: missing SignalID"),
            media_type=_SOAP_MEDIA_TYPE,
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    # Rate limit per signal — prevents click-spam
    if not _poll_limiter.check(f"soap:{signal_id}"):
        logger.warning("SOAP rate limited for signal %s", signal_id)
        return Response(
            content=_build_soap_response(signal_id, "Error: rate limited"),
            media_type=_SOAP_MEDIA_TYPE,
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )

    # Log ignored ATSPM 4.x parameters
    ignored = ["DeleteFiles", "SNMPRetry", "SNMPTimeout", "SNMPPort",
               "LocalDir", "WaitBetweenRecords", "BulkCopyOptions"]
    for key in ignored:
        if params.get(key):
            logger.debug("SOAP ignored parameter %s=%s", key, params[key])

    # Build TSIGMA-native config from SOAP parameters
    active_mode = params.get("ActiveMode", "false").lower() == "true"
    config = {
        "host": params.get("IPAddress", ""),
        "username": params.get("UserName", ""),
        "password": params.get("Password", ""),
        "remote_dir": params.get("RemoteDir", ""),
        "passive_mode": not active_mode,
        "protocol": "ftp",
    }

    try:
        method = collector.get_method("ftp_pull")
    except ValueError:
        return Response(
            content=_build_soap_response(signal_id, "Error: ftp_pull method not available"),
            media_type=_SOAP_MEDIA_TYPE,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    _fire_and_forget(
        method.poll_once(signal_id, config, collector.session_factory)
    )

    logger.info("SOAP poll triggered for signal %s from %s", signal_id, config["host"])

    return Response(
        content=_build_soap_response(signal_id, "Accepted"),
        media_type=_SOAP_MEDIA_TYPE,
    )


# ---------------------------------------------------------------------------
# REST endpoint — modern JSON interface
# ---------------------------------------------------------------------------


@router.post(
    "/signals/{signal_id}/poll",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_admin)],
)
async def trigger_poll(
    signal_id: str,
    body: PollRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """
    REST endpoint for on-demand poll trigger.

    Looks up the signal's collection config from the database, resolves
    the requested polling method, and fires poll_once() as a background
    task. Returns 202 Accepted immediately.

    Args:
        signal_id: Traffic signal identifier.
        body: Poll request with method name.
        request: FastAPI request (for app state access).
        session: Database session (injected).

    Returns:
        JSON with signal_id, method, status, and message.

    Raises:
        HTTPException: 404 if signal not found.
        HTTPException: 400 if method is not a registered polling method.
        HTTPException: 503 if collector is not running.
    """
    collector = _get_collector(request)

    # Rate limit per signal — prevents click-spam
    if not _poll_limiter.check(f"rest:{signal_id}"):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limited: too many poll requests for signal {signal_id}",
        )

    # Look up signal
    result = await session.execute(
        select(Signal).where(Signal.signal_id == signal_id)
    )
    signal = result.scalar_one_or_none()

    if signal is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Signal {signal_id} not found",
        )

    # Resolve polling method
    try:
        method = collector.get_method(body.method)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown polling method: {body.method}",
        )

    # Build config from signal metadata
    metadata = signal.signal_metadata or {}
    config = dict(metadata.get("collection", {}))
    config["host"] = str(signal.ip_address) if signal.ip_address else ""

    _fire_and_forget(
        method.poll_once(signal_id, config, collector.session_factory)
    )

    logger.info(
        "REST poll triggered for signal %s method %s", signal_id, body.method
    )

    return {
        "signal_id": signal_id,
        "method": body.method,
        "status": "started",
        "message": "Poll cycle triggered",
    }


# ---------------------------------------------------------------------------
# Polling checkpoint read endpoints
# ---------------------------------------------------------------------------


def _to_checkpoint_dict(cp) -> dict:
    """Convert a PollingCheckpoint row to a JSON-serialisable dict."""
    return {
        "signal_id": cp.signal_id,
        "method": cp.method,
        "last_filename": cp.last_filename,
        "last_file_mtime": (
            cp.last_file_mtime.isoformat()
            if cp.last_file_mtime else None
        ),
        "last_event_timestamp": (
            cp.last_event_timestamp.isoformat()
            if cp.last_event_timestamp else None
        ),
        "last_successful_poll": (
            cp.last_successful_poll.isoformat()
            if cp.last_successful_poll else None
        ),
        "events_ingested": cp.events_ingested,
        "files_ingested": cp.files_ingested,
        "consecutive_errors": cp.consecutive_errors,
        "last_error": cp.last_error,
        "last_error_time": (
            cp.last_error_time.isoformat()
            if cp.last_error_time else None
        ),
    }


@router.get("/checkpoints/")
async def list_checkpoints(
    method: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("management")),
):
    """
    List all polling checkpoints.

    Provides visibility into ingestion state across all signals and
    methods. Optionally filter by method name.

    Args:
        method: Filter by ingestion method name (e.g., "ftp_pull").
        session: Database session (injected).

    Returns:
        List of checkpoint records.
    """
    stmt = select(PollingCheckpoint)
    if method:
        stmt = stmt.where(PollingCheckpoint.method == method)
    stmt = stmt.order_by(PollingCheckpoint.signal_id, PollingCheckpoint.method)

    result = await session.execute(stmt)
    rows = result.scalars().all()

    return [_to_checkpoint_dict(cp) for cp in rows]


@router.get("/checkpoints/{signal_id}")
async def get_signal_checkpoints(
    signal_id: str,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("management")),
):
    """
    Get polling checkpoints for a specific signal.

    Returns all checkpoints for the signal, one per ingestion method.

    Args:
        signal_id: Traffic signal identifier.
        session: Database session (injected).

    Returns:
        List of checkpoint records for the signal.
    """
    result = await session.execute(
        select(PollingCheckpoint).where(
            PollingCheckpoint.signal_id == signal_id
        )
    )
    rows = result.scalars().all()

    return [_to_checkpoint_dict(cp) for cp in rows]


# ---------------------------------------------------------------------------
# Timestamp correction endpoints (admin only)
# ---------------------------------------------------------------------------


@router.post("/corrections/bulk", dependencies=[Depends(require_admin)])
async def bulk_timestamp_correction(
    body: BulkTimestampCorrectionRequest,
    session: AsyncSession = Depends(get_session),
):
    """
    Bulk-correct event timestamps for a signal within a time range.

    Applies a fixed offset to all event_time values in the specified
    window. Used to fix already-ingested data from controllers with
    misconfigured clocks.

    Admin only — modifies historical event data.

    Args:
        body: Correction parameters (signal_id, time range, offset).
        session: Database session (injected).

    Returns:
        Summary of corrected rows.
    """
    offset = timedelta(seconds=body.offset_seconds)

    stmt = (
        update(ControllerEventLog)
        .where(
            ControllerEventLog.signal_id == body.signal_id,
            ControllerEventLog.event_time >= body.start_time,
            ControllerEventLog.event_time <= body.end_time,
        )
        .values(event_time=ControllerEventLog.event_time + offset)
    )

    result = await session.execute(stmt)
    await session.commit()

    logger.info(
        "Bulk timestamp correction: signal %s, range %s to %s, "
        "offset %+.1fs, %d rows updated",
        body.signal_id,
        body.start_time.isoformat(),
        body.end_time.isoformat(),
        body.offset_seconds,
        result.rowcount,
    )

    return {
        "signal_id": body.signal_id,
        "start_time": body.start_time.isoformat(),
        "end_time": body.end_time.isoformat(),
        "offset_seconds": body.offset_seconds,
        "rows_updated": result.rowcount,
    }


@router.post("/corrections/anchor", dependencies=[Depends(require_admin)])
async def anchor_timestamp_correction(
    body: AnchorCorrectionRequest,
    session: AsyncSession = Depends(get_session),
):
    """
    Anchor-based timestamp correction (convenience wrapper).

    Operator identifies a known-good event by its recorded (incorrect)
    timestamp and provides the real-world timestamp. The system computes
    the offset and applies it to all events in the specified range.

    Admin only — modifies historical event data.

    Args:
        body: Anchor parameters (signal_id, event_time, actual_time,
              time range).
        session: Database session (injected).

    Returns:
        Summary of corrected rows including computed offset.
    """
    offset_seconds = (body.actual_time - body.event_time).total_seconds()
    offset = timedelta(seconds=offset_seconds)

    stmt = (
        update(ControllerEventLog)
        .where(
            ControllerEventLog.signal_id == body.signal_id,
            ControllerEventLog.event_time >= body.start_time,
            ControllerEventLog.event_time <= body.end_time,
        )
        .values(event_time=ControllerEventLog.event_time + offset)
    )

    result = await session.execute(stmt)
    await session.commit()

    logger.info(
        "Anchor timestamp correction: signal %s, anchor %s -> %s, "
        "computed offset %+.1fs, range %s to %s, %d rows updated",
        body.signal_id,
        body.event_time.isoformat(),
        body.actual_time.isoformat(),
        offset_seconds,
        body.start_time.isoformat(),
        body.end_time.isoformat(),
        result.rowcount,
    )

    return {
        "signal_id": body.signal_id,
        "anchor_event_time": body.event_time.isoformat(),
        "anchor_actual_time": body.actual_time.isoformat(),
        "computed_offset_seconds": offset_seconds,
        "start_time": body.start_time.isoformat(),
        "end_time": body.end_time.isoformat(),
        "rows_updated": result.rowcount,
    }
