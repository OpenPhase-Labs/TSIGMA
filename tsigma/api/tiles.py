"""
MapLibre raster tile-cache proxy.

Exposes ``GET /tiles/{z}/{x}/{y}.png`` as a bounded, caching proxy in
front of an upstream raster tile source (OpenStreetMap by default).

Caching semantics:

* **Fresh hit** - the cached tile is younger than ``tile_cache_ttl_days``;
  it is served directly with no upstream fetch.
* **Stale hit** - the cached tile is older than the TTL; the stale bytes
  are served immediately (stale-while-revalidate) while a background task
  refreshes the cache from upstream.
* **Miss** - the upstream is fetched, stored, and the bytes returned.

Concurrent misses for the same tile are coalesced by an in-process
single-flight lock so the upstream is fetched only once.
"""

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import Response

from ..config import settings
from ..storage.factory import get_tile_storage_backend

logger = logging.getLogger(__name__)

router = APIRouter()

# In-process single-flight state keyed by cache key. The first request for an
# uncached tile becomes the leader: it creates a Future, fetches/stores
# upstream once, and resolves the Future. Concurrent followers find the Future
# and await its result instead of re-fetching (the backend may not yet reflect
# the write). The leader clears the entry once resolved.
_inflight: dict[str, "asyncio.Future[bytes]"] = {}


def _source_slug() -> str:
    """Stable id for the configured upstream source (its host)."""
    host = urlparse(settings.tile_source_url).hostname
    return host or "tiles"


def _cache_key(z: int, x: int, y: int) -> str:
    """Build the storage key: ``tiles/{source}/{z}/{x}/{y}.png``."""
    return f"tiles/{_source_slug()}/{z}/{x}/{y}.png"


def _upstream_url(z: int, x: int, y: int) -> str:
    """Substitute z/x/y into the configured source URL template."""
    return settings.tile_source_url.format(z=z, x=x, y=y)


async def _last_modified(backend, key: str) -> datetime:
    """Best-effort last-modified time for a cached tile.

    Reads freshness via ``list_files(key)``. When the backend cannot
    report a timestamp (e.g. a bare mock), the tile is treated as fresh.
    """
    listing = backend.list_files(key)
    if not hasattr(listing, "__aiter__"):
        # Not an async iterator (e.g. a bare mock returning a coroutine);
        # close it to avoid an un-awaited-coroutine warning, treat as fresh.
        close = getattr(listing, "close", None)
        if close is not None:
            close()
        return datetime.now(timezone.utc)
    try:
        async for stored in listing:
            return stored.last_modified
    except Exception:
        # Any failure reading freshness -> treat as fresh.
        pass
    return datetime.now(timezone.utc)


async def _fetch_tile(url: str, user_agent: str) -> bytes:
    """Fetch raster tile bytes from ``url`` with the given User-Agent.

    Module-level so it can be patched as the outbound seam in tests.
    """
    headers = {"User-Agent": user_agent}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.content


async def _fetch_upstream(z: int, x: int, y: int) -> bytes:
    """Fetch the tile bytes from upstream with the configured user-agent."""
    return await _fetch_tile(_upstream_url(z, x, y), settings.tile_user_agent)


async def _refresh(backend, key: str, z: int, x: int, y: int) -> None:
    """Background refresh for a stale tile (stale-while-revalidate)."""
    try:
        data = await _fetch_upstream(z, x, y)
        await backend.put(key, data)
    except Exception:
        logger.warning("Tile background refresh failed for %s", key, exc_info=True)


def _png_response(data: bytes) -> Response:
    """Build the image/png response with cache headers and an ETag."""
    max_age = settings.tile_cache_ttl_days * 24 * 60 * 60
    etag = hashlib.sha256(data).hexdigest()
    return Response(
        content=data,
        media_type="image/png",
        headers={
            "Cache-Control": f"public, max-age={max_age}",
            "ETag": f'"{etag}"',
        },
    )


@router.get("/tiles/{z}/{x}/{y}.png")
async def get_tile(
    z: int, x: int, y: int, background_tasks: BackgroundTasks
) -> Response:
    """Serve a cached/proxied raster map tile."""
    if z > settings.tile_max_zoom:
        raise HTTPException(status_code=404, detail="Zoom out of range")
    if x < 0 or y < 0 or x >= 2 ** z or y >= 2 ** z:
        raise HTTPException(status_code=404, detail="Tile out of range")

    backend = get_tile_storage_backend()
    key = _cache_key(z, x, y)

    if await backend.exists(key):
        data = await backend.get(key)
        last_modified = await _last_modified(backend, key)
        age = datetime.now(timezone.utc) - last_modified
        if age.days > settings.tile_cache_ttl_days:
            # Stale-while-revalidate: serve stale now, refresh after the
            # response is sent (FastAPI BackgroundTasks - retained by the
            # framework, so no GC footgun and no orphaned task).
            background_tasks.add_task(_refresh, backend, key, z, x, y)
        return _png_response(data)

    # Miss: single-flight upstream fetch. A concurrent follower joins the
    # leader's in-flight Future instead of issuing a second upstream GET.
    existing = _inflight.get(key)
    if existing is not None:
        data = await existing
        return _png_response(data)

    future: "asyncio.Future[bytes]" = asyncio.get_running_loop().create_future()
    _inflight[key] = future
    try:
        data = await _fetch_upstream(z, x, y)
        await backend.put(key, data)
    except BaseException as exc:
        future.set_exception(exc)
        _inflight.pop(key, None)
        raise
    future.set_result(data)
    _inflight.pop(key, None)
    return _png_response(data)
