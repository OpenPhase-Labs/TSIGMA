"""
Rate limiter for TSIGMA.

Provides configurable per-category rate limiting with pluggable backends.

Categories:
- "login": keyed by client IP, protects authentication endpoints
- "read": keyed by session ID, protects GET /api/v1/*
- "write": keyed by session ID, protects POST/PUT/DELETE /api/v1/*

Two backends:
- InMemoryRateLimiterBackend: single-process fallback (dev/testing)
- ValkeyRateLimiterBackend: production, multi-instance via INCR + EXPIRE
"""

import logging
import math
import time
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

RATE_LIMIT_KEY_PREFIX = "tsigma:ratelimit:"


class RateLimiterBackend(ABC):
    """Abstract backend for rate limiter storage."""

    @abstractmethod
    async def increment(self, key: str, window_seconds: int) -> tuple[int, float]:
        """
        Increment counter for key within the given window.

        Args:
            key: Unique key (category + identifier).
            window_seconds: Window duration in seconds.

        Returns:
            Tuple of (current_count, seconds_remaining_in_window).
        """
        ...


class InMemoryRateLimiterBackend(RateLimiterBackend):
    """
    Dict-based in-memory rate limiter backend.

    Uses fixed-window counters. Suitable for single-process dev/testing.
    """

    def __init__(self) -> None:
        # key -> (count, window_start_time)
        self._buckets: dict[str, tuple[int, float]] = {}

    async def increment(self, key: str, window_seconds: int) -> tuple[int, float]:
        now = time.monotonic()
        entry = self._buckets.get(key)

        if entry is None or (now - entry[1]) >= window_seconds:
            # Window expired or first request — start new window
            self._buckets[key] = (1, now)
            return (1, float(window_seconds))

        count, window_start = entry
        new_count = count + 1
        self._buckets[key] = (new_count, window_start)
        remaining = window_seconds - (now - window_start)
        return (new_count, remaining)


class ValkeyRateLimiterBackend(RateLimiterBackend):
    """
    Valkey-backed rate limiter using INCR + EXPIRE.

    Each key gets a TTL equal to the window. INCR is atomic across instances.
    """

    def __init__(self, client) -> None:
        """
        Args:
            client: An async valkey client (valkey.asyncio.Valkey instance).
        """
        self._client = client

    async def increment(self, key: str, window_seconds: int) -> tuple[int, float]:
        full_key = f"{RATE_LIMIT_KEY_PREFIX}{key}"
        count = await self._client.incr(full_key)
        if count == 1:
            # First request in window — set expiry
            await self._client.expire(full_key, window_seconds)
        ttl = await self._client.ttl(full_key)
        # ttl can be -1 if no expiry set (race condition safeguard)
        if ttl < 0:
            await self._client.expire(full_key, window_seconds)
            ttl = window_seconds
        return (count, float(ttl))


class RateLimiter:
    """
    Configurable rate limiter with category-based limits.

    Args:
        backend: Storage backend (in-memory or Valkey).
        limits: Dict mapping category name to (max_requests, window_seconds).
                e.g. {"login": (5, 60), "read": (100, 60), "write": (30, 60)}
    """

    def __init__(
        self,
        backend: RateLimiterBackend,
        limits: dict[str, tuple[int, int]],
    ) -> None:
        self._backend = backend
        self._limits = limits

    def set_backend(self, backend: RateLimiterBackend) -> None:
        """Hot-swap the storage backend (e.g. switch to Valkey at startup)."""
        self._backend = backend

    async def check(self, category: str, identifier: str) -> tuple[bool, int | None]:
        """
        Check if a request is within rate limits.

        Args:
            category: Rate limit category ("login", "read", "write").
            identifier: Unique identifier (IP address or session ID).

        Returns:
            Tuple of (allowed, retry_after_seconds).
            - allowed=True, retry_after=None if within limits
            - allowed=False, retry_after=N if over limit
        """
        if category not in self._limits:
            return (True, None)

        max_requests, window_seconds = self._limits[category]
        key = f"{category}:{identifier}"

        count, remaining = await self._backend.increment(key, window_seconds)

        if count <= max_requests:
            return (True, None)

        retry_after = max(1, math.ceil(remaining))
        return (False, retry_after)


def create_rate_limiter(
    valkey_client=None,
    login_limit: int = 5,
    read_limit: int = 100,
    write_limit: int = 30,
) -> RateLimiter:
    """
    Factory function that creates a RateLimiter with the appropriate backend.

    Uses ValkeyRateLimiterBackend if a valkey client is provided,
    otherwise falls back to InMemoryRateLimiterBackend.

    Args:
        valkey_client: Optional async valkey client.
        login_limit: Max login attempts per minute per IP.
        read_limit: Max read requests per minute per session.
        write_limit: Max write requests per minute per session.

    Returns:
        Configured RateLimiter instance.
    """
    if valkey_client is not None:
        backend = ValkeyRateLimiterBackend(valkey_client)
        logger.info("Rate limiter: Valkey-backed")
    else:
        backend = InMemoryRateLimiterBackend()
        logger.warning("Rate limiter: in-memory (no Valkey configured)")

    limits = {
        "login": (login_limit, 60),
        "read": (read_limit, 60),
        "write": (write_limit, 60),
    }

    return RateLimiter(backend=backend, limits=limits)
