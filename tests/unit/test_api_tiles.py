"""
Unit tests for the MapLibre raster tile-cache proxy.

Covers three pieces:

A) Config settings on tsigma.config Settings (env prefix TSIGMA_).
B) The get_tile_storage_backend() factory in tsigma/storage/factory.py.
C) The tiles router tsigma/api/tiles.py exposing GET /tiles/{z}/{x}/{y}.png.

These are FAILING tests authored before the implementation exists. The
config tests fail on missing Settings attributes, the factory test on a
missing function, and the router tests on an ImportError for
tsigma.api.tiles. All assertions are offline and deterministic; the
upstream HTTP fetch and the storage backend are mocked.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.storage.base import StoredFile

PNG_BYTES = b"\x89PNG\r\n\x1a\nFRESH-TILE-DATA"
STALE_BYTES = b"\x89PNG\r\n\x1a\nSTALE-TILE-DATA"
UPSTREAM_BYTES = b"\x89PNG\r\n\x1a\nUPSTREAM-TILE-DATA"


# ---------------------------------------------------------------------------
# A) Config settings defaults
# ---------------------------------------------------------------------------


def test_tile_settings_defaults():
    """A fresh Settings() carries the tile-cache defaults."""
    from tsigma.config import Settings

    s = Settings()

    assert s.tile_cache_enabled is True
    assert s.tile_source_url == "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    assert s.tile_cache_ttl_days == 30
    assert s.tile_storage_path == "/var/lib/tsigma/tiles"
    assert s.tile_max_zoom == 19
    assert s.tile_user_agent == "TSIGMA/1.0"


# ---------------------------------------------------------------------------
# B) Tile storage backend factory
# ---------------------------------------------------------------------------


def test_get_tile_storage_backend_filesystem(monkeypatch, tmp_path):
    """Default returns a FilesystemBackend rooted at settings.tile_storage_path."""
    # Importing the factory symbol is the RED here until it exists.
    from tsigma.storage.factory import get_tile_storage_backend

    monkeypatch.setattr("tsigma.config.settings.storage_backend", "filesystem")
    monkeypatch.setattr("tsigma.config.settings.tile_storage_path", str(tmp_path))

    backend = get_tile_storage_backend()

    assert type(backend).__name__ == "FilesystemBackend"
    assert backend._base == tmp_path


# ---------------------------------------------------------------------------
# C) Tiles router
# ---------------------------------------------------------------------------


def _stored_file(key: str, last_modified: datetime) -> StoredFile:
    """Build a StoredFile with the given freshness timestamp."""
    return StoredFile(
        key=key,
        size=len(PNG_BYTES),
        last_modified=last_modified,
        content_type="image/png",
    )


def _fake_upstream_response(content: bytes) -> MagicMock:
    """A stand-in httpx.Response with .content and .raise_for_status()."""
    resp = MagicMock()
    resp.content = content
    resp.raise_for_status = MagicMock(return_value=None)
    resp.status_code = 200
    return resp


def _make_client(backend, raise_server_exceptions=True):
    """Mount the tiles router on a bare app, patch the backend factory.

    Returns (TestClient, backend). The caller patches the upstream fetch
    separately so each behavior controls its own HTTP mock.
    """
    from tsigma.api import tiles

    app = FastAPI()
    app.include_router(tiles.router)

    # Patch the module-level factory so the route uses our fake backend.
    patcher = patch.object(tiles, "get_tile_storage_backend", return_value=backend)
    patcher.start()

    client = TestClient(app, raise_server_exceptions=raise_server_exceptions)
    return client, patcher


def test_cache_hit_fresh_serves_cached_no_upstream(monkeypatch):
    """Fresh cached tile -> 200 cached bytes, no upstream fetch, cache headers."""
    fresh = datetime.now(timezone.utc)
    backend = AsyncMock()
    backend.exists.return_value = True
    backend.get.return_value = PNG_BYTES
    # If the implementation stats the object for freshness via list/get_url,
    # provide a StoredFile too; AsyncMock returns one regardless.
    backend.put.return_value = _stored_file("tiles/x/3/4/5.png", fresh)

    # _make_client imports tsigma.api.tiles -> ImportError is the RED here
    # until the router module exists.
    client, patcher = _make_client(backend)
    monkeypatch.setattr("tsigma.config.settings.tile_cache_ttl_days", 30)
    try:
        # Patch httpx.AsyncClient so any upstream attempt is observable.
        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(
                return_value=_fake_upstream_response(UPSTREAM_BYTES)
            )
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            resp = client.get("/tiles/3/4/5.png")

            assert resp.status_code == 200
            assert resp.content == PNG_BYTES
            # No upstream fetch on a fresh hit.
            mock_client.get.assert_not_called()

        # Cache headers present.
        cache_control = resp.headers.get("Cache-Control", "")
        assert "public" in cache_control
        assert "max-age=" in cache_control
        assert resp.headers.get("ETag")

        # Cache key shape: tiles/{source}/{z}/{x}/{y}.png -> ends with /3/4/5.png
        backend.get.assert_awaited()
        get_key = backend.get.await_args.args[0]
        assert get_key.endswith("/3/4/5.png")
        assert get_key.startswith("tiles/")
    finally:
        patcher.stop()


def test_cache_miss_fetches_upstream_and_stores(monkeypatch):
    """Cache miss -> upstream fetch with user-agent, put(key, bytes), 200 bytes."""
    backend = AsyncMock()
    backend.exists.return_value = False
    backend.put.return_value = _stored_file(
        "tiles/x/3/4/5.png", datetime.now(timezone.utc)
    )

    client, patcher = _make_client(backend)
    monkeypatch.setattr(
        "tsigma.config.settings.tile_source_url",
        "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
    )
    monkeypatch.setattr("tsigma.config.settings.tile_user_agent", "TSIGMA/1.0")
    try:
        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(
                return_value=_fake_upstream_response(UPSTREAM_BYTES)
            )
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            resp = client.get("/tiles/3/4/5.png")

            assert resp.status_code == 200
            assert resp.content == UPSTREAM_BYTES

            # Upstream fetched exactly once.
            mock_client.get.assert_awaited_once()
            call = mock_client.get.await_args
            # URL is tile_source_url with z/x/y substituted.
            url = call.args[0] if call.args else call.kwargs.get("url")
            assert url == "https://tile.openstreetmap.org/3/4/5.png"
            # User-Agent header was sent.
            headers = call.kwargs.get("headers") or {}
            ua = headers.get("User-Agent") or headers.get("user-agent")
            assert ua == "TSIGMA/1.0"

        # Stored under a tiles/.../3/4/5.png key with the fetched bytes.
        backend.put.assert_awaited()
        put_key = backend.put.await_args.args[0]
        put_data = backend.put.await_args.args[1]
        assert put_key.startswith("tiles/")
        assert put_key.endswith("/3/4/5.png")
        assert put_data == UPSTREAM_BYTES
    finally:
        patcher.stop()


def test_stale_serves_stale_while_revalidate(monkeypatch):
    """Stale cached tile -> 200 stale bytes served immediately (SWR)."""
    stale_ts = datetime.now(timezone.utc) - timedelta(days=90)
    backend = AsyncMock()
    backend.exists.return_value = True
    backend.get.return_value = STALE_BYTES
    backend.put.return_value = _stored_file("tiles/x/3/4/5.png", stale_ts)

    # Make the backend report the stored tile as stale. Implementations may
    # read freshness via list_files() yielding a StoredFile, or via a stat on
    # get_url; expose a stale StoredFile through list_files just in case.
    async def _stale_listing(prefix):
        yield _stored_file("tiles/x/3/4/5.png", stale_ts)

    backend.list_files = _stale_listing

    client, patcher = _make_client(backend)
    monkeypatch.setattr("tsigma.config.settings.tile_cache_ttl_days", 30)
    try:
        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(
                return_value=_fake_upstream_response(UPSTREAM_BYTES)
            )
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            resp = client.get("/tiles/3/4/5.png")

            # Stale bytes served immediately, regardless of refresh timing.
            assert resp.status_code == 200
            assert resp.content == STALE_BYTES
    finally:
        patcher.stop()


def test_zoom_above_max_returns_404(monkeypatch):
    """z greater than tile_max_zoom -> 404 (bounded proxy)."""
    backend = AsyncMock()
    backend.exists.return_value = True
    backend.get.return_value = PNG_BYTES
    backend.put.return_value = _stored_file(
        "tiles/x/20/4/5.png", datetime.now(timezone.utc)
    )

    client, patcher = _make_client(backend, raise_server_exceptions=False)
    monkeypatch.setattr("tsigma.config.settings.tile_max_zoom", 19)
    try:
        resp = client.get("/tiles/20/4/5.png")
        assert resp.status_code == 404
    finally:
        patcher.stop()


def test_xy_out_of_range_returns_404(monkeypatch):
    """x or y >= 2**z for the given zoom -> 404."""
    backend = AsyncMock()
    backend.exists.return_value = True
    backend.get.return_value = PNG_BYTES
    backend.put.return_value = _stored_file(
        "tiles/x/2/9/0.png", datetime.now(timezone.utc)
    )

    client, patcher = _make_client(backend, raise_server_exceptions=False)
    monkeypatch.setattr("tsigma.config.settings.tile_max_zoom", 19)
    try:
        # At z=2 the valid tile index range is 0..3 (2**2 == 4); x=9 is invalid.
        resp = client.get("/tiles/2/9/0.png")
        assert resp.status_code == 404
    finally:
        patcher.stop()


def test_disabled_off_switch_route_not_served():
    """When tile_cache_enabled is False, the tiles route is not mounted (404).

    Mirrors the app's conditional-mount contract: the router is only
    included when settings.tile_cache_enabled is True. We mount it behind
    that same guard with the flag patched off and assert GET 404s.
    """
    from tsigma.api import tiles

    with patch("tsigma.config.settings.tile_cache_enabled", False):
        app = FastAPI()
        # Conditional-mount contract: only include the router when enabled.
        from tsigma.config import settings

        if settings.tile_cache_enabled:
            app.include_router(tiles.router)

        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/tiles/3/4/5.png")
        assert resp.status_code == 404


@pytest.mark.anyio
async def test_single_flight_one_upstream_fetch_for_concurrent_misses(monkeypatch):
    """Two concurrent requests for the SAME uncached tile -> ONE upstream fetch.

    Drives two real requests through the app via a REAL httpx.AsyncClient over
    httpx.ASGITransport(app=app) and asyncio.gather. The patchable production
    seam tsigma.api.tiles._fetch_tile is replaced by an AsyncMock whose slow
    coroutine blocks both requests until they have BOTH entered (an
    asyncio.Event), guaranteeing overlap; the router's single-flight must then
    coalesce the two concurrent misses into exactly ONE _fetch_tile await.

    Note: we deliberately do NOT patch httpx.AsyncClient here. Patching it
    globally would turn our own ASGITransport driver into the mock, so the
    requests would never reach the app and single-flight would never run.
    """
    import httpx

    # Importing the router module is the RED here until it exists.
    from tsigma.api import tiles

    backend = AsyncMock()
    backend.exists.return_value = False
    backend.put = AsyncMock(
        return_value=_stored_file("tiles/x/3/4/5.png", datetime.now(timezone.utc))
    )

    app = FastAPI()
    app.include_router(tiles.router)

    # Slow upstream fetch: both requests must enter the leader path before the
    # fetch resolves, forcing them to overlap. Single-flight should let only
    # ONE request become the leader that calls _fetch_tile.
    both_in = asyncio.Event()
    entered = {"count": 0}

    async def _slow_fetch(url, user_agent):
        entered["count"] += 1
        if entered["count"] >= 2:
            both_in.set()
        try:
            await asyncio.wait_for(both_in.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
        await asyncio.sleep(0.05)
        return UPSTREAM_BYTES

    fetch_mock = AsyncMock(side_effect=_slow_fetch)

    with patch.object(tiles, "get_tile_storage_backend", return_value=backend):
        with patch.object(tiles, "_fetch_tile", fetch_mock):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as ac:
                r1, r2 = await asyncio.gather(
                    ac.get("/tiles/3/4/5.png"),
                    ac.get("/tiles/3/4/5.png"),
                )

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.content == UPSTREAM_BYTES
    assert r2.content == UPSTREAM_BYTES
    # Single-flight: only ONE upstream fetch for the two concurrent misses.
    assert fetch_mock.await_count == 1


@pytest.fixture
def anyio_backend():
    """Run anyio-marked async tests on asyncio only."""
    return "asyncio"
