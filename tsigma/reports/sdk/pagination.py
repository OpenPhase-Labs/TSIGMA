"""Cursor-based pagination + request-lookback enforcement for event-list endpoints.

Opaque cursor encodes ``(event_time, signal_id, event_code, event_param)`` as
base64url(JSON). The cursor is opaque to clients — they pass back whatever the
server returned in ``next_cursor``. Schema can evolve (e.g., add a tiebreaker
field) without breaking clients.
"""

from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma import settings_service


def _encode_cursor(row: dict[str, Any]) -> str:
    """Encode the cursor payload as opaque base64url JSON."""
    payload = {
        "t": _isoformat(row["event_time"]),
        "s": str(row.get("signal_id", "")),
        "c": int(row["event_code"]),
        "p": int(row["event_param"]),
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str) -> dict[str, Any]:
    """Decode an opaque cursor. Raises HTTPException(400) on malformed input."""
    try:
        # Re-add padding stripped on encode
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw)
        return {
            "event_time": datetime.fromisoformat(payload["t"]),
            "signal_id": str(payload["s"]),
            "event_code": int(payload["c"]),
            "event_param": int(payload["p"]),
        }
    except (
        ValueError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
        binascii.Error,
        UnicodeDecodeError,
    ) as exc:
        raise HTTPException(
            status_code=400, detail=f"invalid pagination cursor: {exc}"
        ) from exc


def _isoformat(value: Any) -> str:
    """Render a datetime / pandas Timestamp / ISO string into an ISO string."""
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


async def paginated_event_list(
    rows: pd.DataFrame,
    *,
    after: str | None,
    limit: int,
    session: AsyncSession | None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Apply opaque cursor + clamped limit; return (page_items, next_cursor).

    ``rows`` must be sorted by ``event_time`` ascending and must carry the
    columns referenced by the cursor (``event_time``, ``signal_id``,
    ``event_code``, ``event_param``). Single-signal callers should add a
    constant ``signal_id`` column before invoking.

    The effective limit is ``min(limit, api.max_page_size)`` — the registry
    key is the absolute ceiling regardless of what the client requests.

    Returns ``(records, next_cursor)``. ``next_cursor`` is ``None`` when the
    page exhausted the remaining rows.
    """
    max_page_size = await settings_service.get_int("api.max_page_size", session)
    effective_limit = max(0, min(int(limit), int(max_page_size)))

    if after is not None:
        cursor = _decode_cursor(after)
        rows = _apply_cursor(rows, cursor)

    if effective_limit == 0:
        return [], None

    page = rows.iloc[:effective_limit]
    if len(rows) > effective_limit:
        next_cursor = _encode_cursor(page.iloc[-1].to_dict())
    else:
        next_cursor = None

    return page.to_dict(orient="records"), next_cursor


def _apply_cursor(rows: pd.DataFrame, cursor: dict[str, Any]) -> pd.DataFrame:
    """Filter ``rows`` to entries strictly after the cursor tuple.

    Tuple order: (event_time, signal_id, event_code, event_param).
    Standard lexicographic comparison: stricly greater on the first column
    that differs.
    """
    et = rows["event_time"]
    sid = rows["signal_id"] if "signal_id" in rows.columns else None
    ec = rows["event_code"]
    ep = rows["event_param"]

    c_et = cursor["event_time"]
    c_sid = cursor["signal_id"]
    c_ec = cursor["event_code"]
    c_ep = cursor["event_param"]

    if sid is None:
        # No signal_id column — collapse to 3-element tuple comparison.
        mask = (et > c_et) | ((et == c_et) & ((ec > c_ec) | ((ec == c_ec) & (ep > c_ep))))
    else:
        mask = (
            (et > c_et)
            | (
                (et == c_et)
                & (
                    (sid > c_sid)
                    | (
                        (sid == c_sid)
                        & (
                            (ec > c_ec)
                            | ((ec == c_ec) & (ep > c_ep))
                        )
                    )
                )
            )
        )
    return rows[mask]


async def require_max_lookback(
    start: datetime,
    *,
    session: AsyncSession | None,
) -> None:
    """Reject requests asking for data older than the ``api.max_lookback_days`` ceiling.

    Raises ``HTTPException(400)`` when ``start`` predates ``now() - max_lookback_days``.
    """
    max_lookback_days = await settings_service.get_int(
        "api.max_lookback_days", session
    )
    now = datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(days=int(max_lookback_days))

    # Treat naive datetimes as UTC for the comparison.
    if start.tzinfo is None:
        start_cmp = start.replace(tzinfo=timezone.utc)
    else:
        start_cmp = start

    if start_cmp < cutoff:
        raise HTTPException(
            status_code=400,
            detail=(
                f"start={start.isoformat()} predates the api.max_lookback_days "
                f"ceiling ({max_lookback_days} days)"
            ),
        )
