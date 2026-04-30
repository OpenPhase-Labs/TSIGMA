"""
FastAPI middleware for TSIGMA.

Request/response processing, logging, error handling, rate limiting, etc.
"""

import csv
import io
import json
import logging
import time
import uuid
from typing import Any, Callable
from xml.etree import ElementTree as ET

from fastapi import Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
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


class ContentNegotiationMiddleware(BaseHTTPMiddleware):
    """
    Serialize successful GET JSON responses as CSV or XML on demand.

    Format selected by, in priority order:
      1. ``?format=json|csv|xml`` query parameter (case-insensitive).
         Wins on conflict with the Accept header.
      2. ``Accept`` header — ``application/json``, ``text/csv``,
         ``application/xml``, or ``text/xml``.
      3. Default: JSON.

    The middleware is bypassed for:
      - Non-GET requests
      - Paths outside ``/api/`` (UI HTML, ``/health``, ``/ready``)
      - GraphQL endpoint (``/api/graphql``)
      - Endpoints that already handle their own format
        (path ends with ``/export``)
      - Non-2xx responses (errors keep JSON)
      - Responses whose Content-Type is not ``application/json``

    For CSV, the response payload must tabularize cleanly:
      - Top level must be a JSON object or array of objects
      - Every value must be scalar (no nested objects or arrays)
    Anything else returns ``406 Not Acceptable``.

    XML serializes the JSON tree directly: dicts become elements,
    lists become repeated ``<item>`` children, scalars become text
    nodes.  Element names are sanitized to be XML-legal (replacing
    illegal characters with underscore).
    """

    _ALLOWED = ("json", "csv", "xml")

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Negotiate response format and re-serialize if needed."""
        if not self._in_scope(request):
            return await call_next(request)

        fmt = self._resolve_format(request)
        response = await call_next(request)

        # JSON requested → leave as-is (no work to do).
        if fmt == "json":
            return response

        # Errors keep JSON regardless of negotiation.
        if response.status_code >= 400:
            return response

        # Only re-serialize JSON responses; binary downloads etc. pass through.
        ctype = response.headers.get("content-type", "")
        if "application/json" not in ctype:
            return response

        # Drain the streaming body so we can re-encode it.
        body_bytes = b""
        async for chunk in response.body_iterator:
            body_bytes += chunk

        try:
            data = json.loads(body_bytes)
        except (ValueError, TypeError):
            # Wasn't valid JSON; pass the original bytes through unchanged.
            return Response(
                content=body_bytes,
                status_code=response.status_code,
                media_type=ctype,
            )

        if fmt == "csv":
            return self._to_csv(data, response.status_code)
        if fmt == "xml":
            return self._to_xml(data, response.status_code)
        # Defensive: unreachable given _resolve_format's allowed set.
        return Response(
            content=body_bytes,
            status_code=response.status_code,
            media_type=ctype,
        )

    @staticmethod
    def _in_scope(request: Request) -> bool:
        if request.method != "GET":
            return False
        path = request.url.path
        if not path.startswith("/api/"):
            return False
        if path.startswith("/api/graphql"):
            return False
        if path.rstrip("/").endswith("/export"):
            return False
        return True

    def _resolve_format(self, request: Request) -> str:
        """Query param wins; otherwise inspect Accept; otherwise JSON."""
        q = request.query_params.get("format", "").lower().strip()
        if q in self._ALLOWED:
            return q
        accept = request.headers.get("accept", "").lower()
        if "text/csv" in accept:
            return "csv"
        if "application/xml" in accept or "text/xml" in accept:
            return "xml"
        return "json"

    @staticmethod
    def _to_csv(data: Any, status_code: int) -> Response:
        """Render a list-of-flat-dicts (or single flat dict) as CSV.

        Returns 406 Not Acceptable if the payload doesn't tabularize.
        """
        if isinstance(data, dict):
            rows: list[dict] = [data]
        elif isinstance(data, list):
            rows = data
        else:
            return ContentNegotiationMiddleware._not_acceptable(
                "text/csv",
                "response is not an object or array of objects",
            )

        if not rows:
            return PlainTextResponse(
                "",
                status_code=status_code,
                media_type="text/csv; charset=utf-8",
            )

        if not all(isinstance(r, dict) for r in rows):
            return ContentNegotiationMiddleware._not_acceptable(
                "text/csv", "array elements must be objects",
            )

        for r in rows:
            for v in r.values():
                if isinstance(v, (dict, list)):
                    return ContentNegotiationMiddleware._not_acceptable(
                        "text/csv",
                        "rows contain nested values that don't tabularize",
                    )

        # Field order: first row's keys, then any keys that show up later.
        fieldnames: list[str] = []
        seen: set[str] = set()
        for r in rows:
            for k in r.keys():
                if k not in seen:
                    fieldnames.append(k)
                    seen.add(k)

        buf = io.StringIO(newline="")
        writer = csv.DictWriter(
            buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for r in rows:
            writer.writerow({
                k: ("" if r.get(k) is None else r.get(k))
                for k in fieldnames
            })
        return Response(
            content=buf.getvalue(),
            status_code=status_code,
            media_type="text/csv; charset=utf-8",
        )

    @staticmethod
    def _to_xml(data: Any, status_code: int) -> Response:
        """Render any JSON-shaped value as XML."""
        if isinstance(data, list):
            root = ET.Element("items")
            for item in data:
                child = ET.SubElement(root, "item")
                ContentNegotiationMiddleware._build_xml(child, item)
        elif isinstance(data, dict):
            root = ET.Element("item")
            ContentNegotiationMiddleware._build_xml(root, data)
        else:
            root = ET.Element("value")
            root.text = "" if data is None else str(data)

        body = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        return Response(
            content=body,
            status_code=status_code,
            media_type="application/xml; charset=utf-8",
        )

    @staticmethod
    def _build_xml(parent: ET.Element, value: Any) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                child = ET.SubElement(
                    parent, ContentNegotiationMiddleware._safe_tag(k),
                )
                ContentNegotiationMiddleware._build_xml(child, v)
        elif isinstance(value, list):
            for item in value:
                child = ET.SubElement(parent, "item")
                ContentNegotiationMiddleware._build_xml(child, item)
        elif value is None:
            parent.text = ""
        elif isinstance(value, bool):
            # Render bools as "true"/"false" (XML convention) rather than
            # Python's "True"/"False" so XML consumers can parse natively.
            parent.text = "true" if value else "false"
        else:
            parent.text = str(value)

    @staticmethod
    def _safe_tag(name: str) -> str:
        """Coerce an arbitrary key into a valid XML element name."""
        if not name:
            return "_"
        first = name[0]
        if not (first.isalpha() or first == "_"):
            name = "_" + name
        return "".join(
            c if (c.isalnum() or c in "-_.") else "_" for c in name
        )

    @staticmethod
    def _not_acceptable(media_type: str, reason: str) -> JSONResponse:
        return JSONResponse(
            status_code=406,
            content={
                "detail": (
                    f"Cannot tabularize response as {media_type}: {reason}"
                ),
            },
        )

