"""
FastAPI middleware for TSIGMA.

Request/response processing, logging, error handling, rate limiting, etc.
"""

import logging
import time
import uuid
from typing import Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from tsigma.config import settings

from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Adds unique request ID to each request.

    Request ID is returned in X-Request-ID header and available in request.state.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and add request ID.

        Args:
            request: Incoming request.
            call_next: Next middleware/route handler.

        Returns:
            Response with X-Request-ID header.
        """
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

        return response


class TimingMiddleware(BaseHTTPMiddleware):
    """
    Measures and logs request processing time.

    Adds X-Process-Time header with duration in milliseconds.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and measure timing.

        Args:
            request: Incoming request.
            call_next: Next middleware/route handler.

        Returns:
            Response with X-Process-Time header.
        """
        start_time = time.perf_counter()

        response = await call_next(request)

        process_time = (time.perf_counter() - start_time) * 1000
        response.headers["X-Process-Time"] = f"{process_time:.2f}ms"

        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Logs all requests and responses.

    Log format controlled by TSIGMA_LOG_FORMAT (json or console).
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and log details.

        Args:
            request: Incoming request.
            call_next: Next middleware/route handler.

        Returns:
            Response after logging.
        """
        request_id = getattr(request.state, "request_id", "no-id")
        method = request.method
        path = request.url.path

        logger.info("[%s] %s %s", request_id, method, path)

        response = await call_next(request)

        logger.info("[%s] %s %s -> %d", request_id, method, path, response.status_code)

        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Adds security headers (CSP, X-Frame-Options, etc.).

    Protects against XSS, clickjacking, and other attacks.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and add security headers.

        Args:
            request: Incoming request.
            call_next: Next middleware/route handler.

        Returns:
            Response with security headers.
        """
        response = await call_next(request)

        # Content Security Policy
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self'; "
            "img-src 'self' data:; "
            "font-src 'self'; "
            "connect-src 'self'; "
            "frame-ancestors 'none'"
        )

        # Additional security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        hsts = "max-age=63072000; includeSubDomains"
        if settings.hsts_preload:
            hsts += "; preload"
        response.headers["Strict-Transport-Security"] = hsts
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )

        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Per-request rate limiting middleware.

    Classifies each request into a category (login, read, write) and
    checks against the configured RateLimiter. Returns 429 Too Many
    Requests with Retry-After header when limits are exceeded.

    Non-API paths (health, static, docs) are not rate limited.
    """

    def __init__(self, app, limiter: RateLimiter) -> None:
        super().__init__(app)
        self._limiter = limiter

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Check rate limits before forwarding the request.

        Args:
            request: Incoming request.
            call_next: Next middleware/route handler.

        Returns:
            429 JSONResponse if rate limited, otherwise the normal response.
        """
        category, key = self._classify(request)

        if category is not None and key is not None:
            allowed, retry_after = await self._limiter.check(category, key)
            if not allowed:
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded. Please try again later."},
                    headers={"Retry-After": str(retry_after)},
                )

        return await call_next(request)

    @staticmethod
    def _get_client_ip(request: Request) -> str:
        """Extract client IP from request, defaulting to 'unknown'."""
        return request.client.host if request.client else "unknown"

    @staticmethod
    def _get_session_or_ip(request: Request) -> str:
        """Extract session cookie or fall back to client IP."""
        session_id = request.cookies.get("tsigma_session", "")
        if session_id:
            return session_id
        return request.client.host if request.client else "unknown"

    @classmethod
    def _classify(cls, request: Request) -> tuple[str | None, str | None]:
        """
        Determine rate-limit category and key from the request.

        Returns:
            (category, key) or (None, None) if the path is not rate-limited.
        """
        path = request.url.path
        method = request.method.upper()

        if not path.startswith("/api/"):
            return (None, None)

        if path.rstrip("/").endswith("/auth/login") and method == "POST":
            return ("login", cls._get_client_ip(request))

        if method == "GET":
            return ("read", cls._get_session_or_ip(request))

        if method in ("POST", "PUT", "PATCH", "DELETE"):
            return ("write", cls._get_session_or_ip(request))

        return (None, None)

