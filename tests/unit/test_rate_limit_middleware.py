"""
Unit tests for rate limit middleware.

Tests the RateLimitMiddleware integration with FastAPI.
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.middleware import RateLimitMiddleware
from tsigma.rate_limiter import InMemoryRateLimiterBackend, RateLimiter


def _make_app(limits: dict[str, tuple[int, int]]) -> FastAPI:
    """Create a minimal FastAPI app with rate limit middleware."""
    app = FastAPI()

    backend = InMemoryRateLimiterBackend()
    limiter = RateLimiter(backend=backend, limits=limits)
    app.add_middleware(RateLimitMiddleware, limiter=limiter)

    @app.post("/api/v1/auth/login")
    async def login():
        return {"ok": True}

    @app.get("/api/v1/signals")
    async def get_signals():
        return {"signals": []}

    @app.post("/api/v1/signals")
    async def create_signal():
        return {"created": True}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


class TestRateLimitMiddleware:
    """Tests for the rate limit middleware."""

    def test_login_blocked_after_limit(self):
        """POST /auth/login is rate-limited by IP at the login limit."""
        app = _make_app({"login": (3, 60), "read": (100, 60), "write": (30, 60)})
        client = TestClient(app)

        for _ in range(3):
            resp = client.post("/api/v1/auth/login")
            assert resp.status_code == 200

        resp = client.post("/api/v1/auth/login")
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers

    def test_read_endpoint_uses_read_limit(self):
        """GET /api/v1/* uses the read category limit."""
        app = _make_app({"login": (5, 60), "read": (3, 60), "write": (30, 60)})
        client = TestClient(app)

        for _ in range(3):
            resp = client.get("/api/v1/signals")
            assert resp.status_code == 200

        resp = client.get("/api/v1/signals")
        assert resp.status_code == 429

    def test_write_endpoint_uses_write_limit(self):
        """POST /api/v1/* (non-login) uses the write category limit."""
        app = _make_app({"login": (5, 60), "read": (100, 60), "write": (2, 60)})
        client = TestClient(app)

        for _ in range(2):
            resp = client.post("/api/v1/signals")
            assert resp.status_code == 200

        resp = client.post("/api/v1/signals")
        assert resp.status_code == 429

    def test_429_includes_retry_after(self):
        """429 responses include a positive integer Retry-After header."""
        app = _make_app({"login": (1, 60), "read": (100, 60), "write": (30, 60)})
        client = TestClient(app)

        client.post("/api/v1/auth/login")
        resp = client.post("/api/v1/auth/login")
        assert resp.status_code == 429
        retry = int(resp.headers["Retry-After"])
        assert retry > 0

    def test_429_body_has_detail(self):
        """429 response body contains a detail message."""
        app = _make_app({"login": (1, 60), "read": (100, 60), "write": (30, 60)})
        client = TestClient(app)

        client.post("/api/v1/auth/login")
        resp = client.post("/api/v1/auth/login")
        assert resp.status_code == 429
        body = resp.json()
        assert "detail" in body
        assert "rate limit" in body["detail"].lower()

    def test_health_not_rate_limited(self):
        """Non-API paths like /health are not rate limited."""
        app = _make_app({"login": (1, 60), "read": (1, 60), "write": (1, 60)})
        client = TestClient(app)

        # Should always pass regardless of limits
        for _ in range(10):
            resp = client.get("/health")
            assert resp.status_code == 200
