"""
Unit tests for rate limiter.

Tests rate limiting logic with in-memory backend.
Written TDD-style before the implementation.
"""

import asyncio

import pytest

from tsigma.rate_limiter import InMemoryRateLimiterBackend, RateLimiter


class TestRateLimiterAllowsUnderLimit:
    """Requests under the limit should pass through."""

    @pytest.mark.asyncio
    async def test_allows_under_limit(self):
        """Requests below the configured limit return True (allowed)."""
        backend = InMemoryRateLimiterBackend()
        limiter = RateLimiter(
            backend=backend,
            limits={"login": (5, 60)},  # 5 requests per 60 seconds
        )
        for _ in range(5):
            allowed, _retry = await limiter.check("login", "192.168.1.1")
            assert allowed is True


class TestRateLimiterBlocksOverLimit:
    """Requests over the limit should be blocked."""

    @pytest.mark.asyncio
    async def test_blocks_over_limit(self):
        """The 6th request in a 5/60s window returns False (blocked)."""
        backend = InMemoryRateLimiterBackend()
        limiter = RateLimiter(
            backend=backend,
            limits={"login": (5, 60)},
        )
        for _ in range(5):
            allowed, _ = await limiter.check("login", "192.168.1.1")
            assert allowed is True

        allowed, retry_after = await limiter.check("login", "192.168.1.1")
        assert allowed is False
        assert retry_after is not None


class TestRetryAfterHeader:
    """Blocked responses must include a Retry-After value."""

    @pytest.mark.asyncio
    async def test_retry_after_is_positive(self):
        """When blocked, retry_after is a positive integer (seconds)."""
        backend = InMemoryRateLimiterBackend()
        limiter = RateLimiter(
            backend=backend,
            limits={"login": (2, 60)},
        )
        await limiter.check("login", "10.0.0.1")
        await limiter.check("login", "10.0.0.1")

        allowed, retry_after = await limiter.check("login", "10.0.0.1")
        assert allowed is False
        assert isinstance(retry_after, int)
        assert retry_after > 0
        assert retry_after <= 60


class TestDifferentKeysIndependent:
    """Different IPs/sessions must have independent counters."""

    @pytest.mark.asyncio
    async def test_different_keys_independent(self):
        """Exhausting one key's limit does not affect another key."""
        backend = InMemoryRateLimiterBackend()
        limiter = RateLimiter(
            backend=backend,
            limits={"login": (2, 60)},
        )
        # Exhaust key A
        await limiter.check("login", "ip-a")
        await limiter.check("login", "ip-a")
        allowed_a, _ = await limiter.check("login", "ip-a")
        assert allowed_a is False

        # Key B should still be allowed
        allowed_b, _ = await limiter.check("login", "ip-b")
        assert allowed_b is True


class TestWindowResets:
    """After the window expires, requests are allowed again."""

    @pytest.mark.asyncio
    async def test_window_resets(self):
        """After the window elapses, the counter resets and allows requests."""
        backend = InMemoryRateLimiterBackend()
        limiter = RateLimiter(
            backend=backend,
            limits={"login": (2, 1)},  # 2 requests per 1 second
        )
        await limiter.check("login", "10.0.0.1")
        await limiter.check("login", "10.0.0.1")
        allowed, _ = await limiter.check("login", "10.0.0.1")
        assert allowed is False

        # Wait for the window to expire
        await asyncio.sleep(1.1)

        allowed, _ = await limiter.check("login", "10.0.0.1")
        assert allowed is True


class TestConfigurableLimits:
    """Different categories can have different limits."""

    @pytest.mark.asyncio
    async def test_configurable_limits(self):
        """Each category has its own configured limit and window."""
        backend = InMemoryRateLimiterBackend()
        limiter = RateLimiter(
            backend=backend,
            limits={
                "login": (2, 60),
                "read": (100, 60),
                "write": (5, 60),
            },
        )
        # Exhaust login limit
        await limiter.check("login", "key1")
        await limiter.check("login", "key1")
        allowed, _ = await limiter.check("login", "key1")
        assert allowed is False

        # Read limit should still have headroom
        allowed, _ = await limiter.check("read", "key1")
        assert allowed is True

        # Write limit should still have headroom
        allowed, _ = await limiter.check("write", "key1")
        assert allowed is True
