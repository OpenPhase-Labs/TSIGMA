"""
Tests for the per-ingest controller clock-offset recording inside
``tsigma.collection.sdk.persist_events_with_drift_check`` (Inc-4 Task 3).

A new best-effort helper records each controller ingest's clock offset
(newest event timestamp minus server receive time) as a
``ControllerClockOffset`` row, for trending over time.  The recording:

* runs ONLY for controller-shaped batches (``DecodedEvent``); sensor
  detections carry no per-controller checkpoint to trend,
* never blocks ingest — any failure in the recording path is swallowed,
  and ``_upsert_events`` still runs,
* is skipped entirely on an empty batch (the function returns early).

These tests isolate the recording behavior by patching ``_upsert_events``
and ``_warn_on_drift`` so only the clock-offset path is exercised.  They
mirror the mocking idioms in ``tests/unit/test_collection_sdk.py`` and
``tests/unit/test_collection_sdk_type_dispatch.py`` (``make_mock_session``
plus an ``async with`` context-manager session factory, patching at the
``tsigma.collection.sdk`` module path).
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests._helpers import make_mock_session
from tsigma.collection.decoders.base import DecodedEvent, SensorDetection
from tsigma.collection.sdk import (
    ControllerClockOffset,
    persist_events_with_drift_check,
)
from tsigma.models.roadside_event import ROADSIDE_EVENT_TYPE_SPEED


def _make_session_factory():
    """Build a mock async session factory matching the ``async with`` pattern.

    The yielded session exposes ``add`` (MagicMock) and ``commit``
    (AsyncMock) — the surface the clock-offset recorder uses.
    """
    mock_session = make_mock_session()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_session)
    ctx.__aexit__ = AsyncMock(return_value=None)
    factory = MagicMock(return_value=ctx)
    return factory, mock_session


def _make_events(n=3, *, base_time=None):
    """Create a list of DecodedEvent objects ending at ``base_time``."""
    base = base_time or datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return [
        DecodedEvent(
            timestamp=base - timedelta(seconds=i),
            event_code=1,
            event_param=i,
        )
        for i in range(n)
    ]


def _added_clock_offsets(session):
    """Return every ControllerClockOffset passed to ``session.add``."""
    return [
        call.args[0]
        for call in session.add.call_args_list
        if call.args and isinstance(call.args[0], ControllerClockOffset)
    ]


class TestRecordClockOffset:
    @pytest.mark.asyncio
    async def test_records_offset_for_controller_batch(self):
        factory, session = _make_session_factory()
        newest = datetime.now(timezone.utc) - timedelta(seconds=100)
        events = _make_events(3, base_time=newest)

        with (
            patch(
                "tsigma.collection.sdk._upsert_events",
                new_callable=AsyncMock,
            ) as mock_upsert,
            patch(
                "tsigma.collection.sdk._warn_on_drift",
                new_callable=AsyncMock,
            ),
        ):
            await persist_events_with_drift_check(events, "SIG-001", factory)

        recorded = _added_clock_offsets(session)
        assert len(recorded) == 1
        offset = recorded[0]
        assert offset.event_count == len(events)
        # Newest event is ~100s behind server time -> offset ~= -100.
        assert -101 < offset.offset_seconds < -99
        session.commit.assert_awaited()
        mock_upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_events_records_nothing(self):
        factory, session = _make_session_factory()

        with (
            patch(
                "tsigma.collection.sdk._upsert_events",
                new_callable=AsyncMock,
            ) as mock_upsert,
            patch(
                "tsigma.collection.sdk._warn_on_drift",
                new_callable=AsyncMock,
            ),
        ):
            await persist_events_with_drift_check([], "SIG-001", factory)

        assert _added_clock_offsets(session) == []
        # Empty batch returns before _upsert_events (verified in source).
        mock_upsert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sensor_batch_records_no_clock_offset(self):
        factory, session = _make_session_factory()
        detections = [
            SensorDetection(
                timestamp=datetime.now(timezone.utc) - timedelta(seconds=100),
                vendor_tag="SIG001",
                event_type=ROADSIDE_EVENT_TYPE_SPEED,
                mph=35,
                kph=56,
            ),
        ]

        with (
            patch(
                "tsigma.collection.sdk._upsert_events",
                new_callable=AsyncMock,
            ) as mock_upsert,
            patch(
                "tsigma.collection.sdk._warn_on_drift",
                new_callable=AsyncMock,
            ),
        ):
            await persist_events_with_drift_check(detections, "ignored", factory)

        assert _added_clock_offsets(session) == []
        mock_upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recording_error_is_swallowed_ingest_proceeds(self):
        factory, session = _make_session_factory()
        session.commit = AsyncMock(side_effect=RuntimeError("db down"))
        newest = datetime.now(timezone.utc) - timedelta(seconds=100)
        events = _make_events(2, base_time=newest)

        with (
            patch(
                "tsigma.collection.sdk._upsert_events",
                new_callable=AsyncMock,
            ) as mock_upsert,
            patch(
                "tsigma.collection.sdk._warn_on_drift",
                new_callable=AsyncMock,
            ),
        ):
            # Must NOT raise — recording failure can never block ingest.
            await persist_events_with_drift_check(events, "SIG-001", factory)

        mock_upsert.assert_awaited_once()
