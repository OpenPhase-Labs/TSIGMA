"""
Unit tests for Task A4 — Valkey pub/sub cache invalidation.

Two surfaces under test:

1. Publisher inside ``settings_service.set()`` — when the dual gate
   (``valkey_settings_invalidation_enabled`` AND non-empty ``valkey_url``)
   passes, ``set()`` schedules a fire-and-forget publish to the
   ``tsigma:system_setting:invalidate`` channel carrying the changed key.
2. Subscriber handler — invalidates the local ``settings_cache`` when a
   matching message arrives on the channel.

Per the A4 plan: NO ``fakeredis``, NO new test dependencies. All Valkey
client behaviour is exercised via ``unittest.mock.AsyncMock``, matching
the existing ``TestValkeySessionStore`` pattern at
``tests/unit/test_app.py:511`` and the ``_mock_client()`` helper at
``tests/unit/test_valkey_sessions.py:29-34``.

These tests are written in the RED phase — Task A4 production code does
NOT exist yet at HEAD ``f99b99f``. Most cases are expected to fail until
the implementer wires up the publisher, the handler, and the lifespan
subscriber. Negative-path guards (publish-not-called when the flag is
False or when ``valkey_url`` is empty) may pass trivially at HEAD because
there is currently no publish call at all.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests._helpers import make_mock_session
from tsigma.models.system_setting import SystemSetting
from tsigma.settings_service import set as set_setting

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


CHANNEL = "tsigma:system_setting:invalidate"


def _make_session(existing_row: SystemSetting | None = None):
    """Build an AsyncSession mock for ``set()`` calls.

    Mirrors the ``_make_audit_mock_session()`` helper in
    ``test_settings_service.py`` so the publisher tests share the same DB
    shape used by the A3 audit-row tests.
    """
    mock_session = make_mock_session()
    result = MagicMock()
    result.scalar_one_or_none.return_value = existing_row
    result.scalars.return_value.all.return_value = (
        [existing_row] if existing_row is not None else []
    )
    mock_session.execute = AsyncMock(return_value=result)
    return mock_session


def _make_publisher_mock() -> AsyncMock:
    """Build the publisher-client AsyncMock used by ``set()``.

    ``publish`` is the only method the dual-gate-open path is expected to
    touch on the publisher client. Returning an explicit AsyncMock keeps
    the assertion shape (``assert_awaited_once_with`` / ``assert_not_called``)
    unambiguous.
    """
    pub = AsyncMock()
    pub.publish = AsyncMock(return_value=1)  # valkey publish returns subscriber count
    return pub


# ---------------------------------------------------------------------------
# Publisher: dual-gate behaviour inside ``set()``
# ---------------------------------------------------------------------------

class TestSetPublishesInvalidation:
    """Dual-gate behaviour of the new publisher inside ``set()``."""

    @pytest.mark.asyncio
    async def test_set_publishes_invalidation_when_dual_gate_open(self):
        """Both flag=True AND valkey_url set: publish is called with (channel, key)."""
        session = _make_session(existing_row=None)
        publisher = _make_publisher_mock()

        with patch("tsigma.settings_service.settings") as mock_settings, \
             patch(
                 "tsigma.settings_service._get_publisher_client",
                 new=AsyncMock(return_value=publisher),
             ):
            mock_settings.valkey_settings_invalidation_enabled = True
            mock_settings.valkey_url = "valkey://localhost:6379"

            await set_setting(
                "api.max_page_size",
                500,
                session,
                changed_by="alice",
                reason="dual-gate-open publish test",
            )

            # The publish is fire-and-forget via asyncio.create_task — yield
            # control so the scheduled task can run before we assert.
            for _ in range(5):
                await asyncio.sleep(0)

            publisher.publish.assert_awaited_once_with(
                CHANNEL, "api.max_page_size"
            )

    @pytest.mark.asyncio
    async def test_set_does_not_publish_when_flag_false(self):
        """Flag=False (first gate closed): publish must NOT be called."""
        session = _make_session(existing_row=None)
        publisher = _make_publisher_mock()

        with patch("tsigma.settings_service.settings") as mock_settings, \
             patch(
                 "tsigma.settings_service._get_publisher_client",
                 new=AsyncMock(return_value=publisher),
             ) as mock_get_pub:
            mock_settings.valkey_settings_invalidation_enabled = False
            # valkey_url is set, but the first gate (flag) is False — must
            # short-circuit BEFORE any publisher construction.
            mock_settings.valkey_url = "valkey://localhost:6379"

            await set_setting(
                "api.max_page_size",
                500,
                session,
                changed_by="alice",
            )

            for _ in range(5):
                await asyncio.sleep(0)

            publisher.publish.assert_not_called()
            # The publisher lazy-constructor must also be untouched — a
            # short-circuit at the dual gate happens BEFORE client lookup.
            mock_get_pub.assert_not_called()

    @pytest.mark.asyncio
    async def test_set_does_not_publish_when_valkey_url_empty(self):
        """valkey_url="" (second gate closed): publish must NOT be called."""
        session = _make_session(existing_row=None)
        publisher = _make_publisher_mock()

        with patch("tsigma.settings_service.settings") as mock_settings, \
             patch(
                 "tsigma.settings_service._get_publisher_client",
                 new=AsyncMock(return_value=publisher),
             ) as mock_get_pub:
            mock_settings.valkey_settings_invalidation_enabled = True
            mock_settings.valkey_url = ""

            await set_setting(
                "api.max_page_size",
                500,
                session,
                changed_by="alice",
            )

            for _ in range(5):
                await asyncio.sleep(0)

            publisher.publish.assert_not_called()
            mock_get_pub.assert_not_called()

    @pytest.mark.asyncio
    async def test_set_publish_is_fire_and_forget(self):
        """The publish is scheduled, not awaited inline — ``set()`` returns fast."""
        session = _make_session(existing_row=None)

        slow_publisher = AsyncMock()

        async def _slow_publish(channel, key):
            # Deliberately slow — if ``set()`` awaited this directly, the
            # whole helper would block ~0.2s; fire-and-forget keeps it fast.
            await asyncio.sleep(0.2)
            return 1

        slow_publisher.publish = AsyncMock(side_effect=_slow_publish)

        with patch("tsigma.settings_service.settings") as mock_settings, \
             patch(
                 "tsigma.settings_service._get_publisher_client",
                 new=AsyncMock(return_value=slow_publisher),
             ):
            mock_settings.valkey_settings_invalidation_enabled = True
            mock_settings.valkey_url = "valkey://localhost:6379"

            loop = asyncio.get_event_loop()
            t0 = loop.time()
            await set_setting(
                "api.max_page_size",
                500,
                session,
                changed_by="alice",
            )
            elapsed = loop.time() - t0

            # ``set()`` must return well before the 0.2s slow publish would
            # complete. Generous bound to avoid flakiness on slow CI.
            assert elapsed < 0.1, (
                f"set() took {elapsed:.3f}s — expected <0.1s. The publish "
                f"appears to be awaited inline rather than fire-and-forget "
                f"via asyncio.create_task."
            )


# ---------------------------------------------------------------------------
# Subscriber handler: drives the local cache from received messages
# ---------------------------------------------------------------------------

class TestSubscriberHandler:
    """Direct unit tests for the subscriber's per-message handler.

    The handler is invoked once per message yielded by ``pubsub.listen()``.
    Tests call the handler directly with synthesized message dicts — no
    real or fake broker round-trip — matching the plan A4.2 spec.
    """

    @pytest.mark.asyncio
    async def test_subscriber_handler_invalidates_cache_for_key(self):
        """A ``{"type": "message", "data": "<key>"}`` payload invalidates the cache."""
        from tsigma.settings_service import _handle_invalidate_message

        with patch("tsigma.settings_service.settings_cache") as mock_cache:
            mock_cache.invalidate = MagicMock()

            await _handle_invalidate_message(
                {"type": "message",
                 "channel": CHANNEL,
                 "data": "api.max_page_size"}
            )

            mock_cache.invalidate.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscriber_handler_ignores_non_message_types(self):
        """Subscribe-ack and pattern-subscribe messages do NOT invalidate the cache."""
        from tsigma.settings_service import _handle_invalidate_message

        with patch("tsigma.settings_service.settings_cache") as mock_cache:
            mock_cache.invalidate = MagicMock()

            # Real valkey pub/sub yields a ``subscribe`` ack as the FIRST
            # message after a successful ``subscribe()`` call. The handler
            # must skip it cleanly.
            await _handle_invalidate_message(
                {"type": "subscribe", "channel": CHANNEL, "data": 1}
            )
            await _handle_invalidate_message(
                {"type": "psubscribe", "channel": CHANNEL, "data": 1}
            )
            await _handle_invalidate_message(
                {"type": "unsubscribe", "channel": CHANNEL, "data": 0}
            )

            mock_cache.invalidate.assert_not_called()

    @pytest.mark.asyncio
    async def test_subscriber_handler_handles_decoded_bytes(self):
        """Bytes payloads (the valkey-py default with ``decode_responses=False``) decode cleanly."""
        from tsigma.settings_service import _handle_invalidate_message

        with patch("tsigma.settings_service.settings_cache") as mock_cache:
            mock_cache.invalidate = MagicMock()

            await _handle_invalidate_message(
                {"type": "message",
                 "channel": b"tsigma:system_setting:invalidate",
                 "data": b"api.max_page_size"}
            )

            # The cache should be invalidated regardless of payload encoding
            # — the handler decodes bytes to str (per the channel name,
            # which is utf-8 safe) before consulting the cache.
            mock_cache.invalidate.assert_called_once()
