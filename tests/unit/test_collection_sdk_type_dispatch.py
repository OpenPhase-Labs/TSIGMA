"""
Type-dispatch tests for ``tsigma.collection.sdk._upsert_events`` and
``persist_events_with_drift_check``.

The persister inspects the first element of the ``events`` list and
routes accordingly:

    DecodedEvent      -> ``controller_event_log`` via pg_insert
    SensorDetection   -> ``roadside_event`` via pg_insert, with
                         vendor_tag -> (sensor_id UUID, signal_id)
                         lookup through roadside_sensor_lane JOIN
                         roadside_sensor.

Mixed or unknown types raise ``TypeError`` so upstream decoder bugs
surface loudly rather than silently corrupting one of the target
tables.

These tests exercise the logic at the Python level with mocked DB
sessions.  Real-database round-trips are covered by the integration
suite under ``tests/integration/``.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from tsigma.collection.decoders.base import DecodedEvent, SensorDetection
from tsigma.collection.sdk import (
    _upsert_controller_events,
    _upsert_events,
    _upsert_sensor_detections,
    persist_events_with_drift_check,
)
from tsigma.models.roadside_event import ROADSIDE_EVENT_TYPE_SPEED


def _make_session_factory(lookup_rows: list | None = None):
    """Mock session factory where ``.execute`` returns a resultable mock.

    If ``lookup_rows`` is provided, the first execute() call's result
    iterates those rows — used to simulate the vendor_tag -> sensor
    lookup.  Subsequent execute() calls return a generic MagicMock
    (the INSERT doesn't care about the result).
    """
    mock_session = AsyncMock()

    if lookup_rows is not None:
        call_idx = {"n": 0}

        def _execute_side_effect(*args, **kwargs):
            call_idx["n"] += 1
            if call_idx["n"] == 1:
                # First call: lookup SELECT -> iterable of rows
                return iter(lookup_rows)
            return MagicMock()

        mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_session)
    ctx.__aexit__ = AsyncMock(return_value=None)
    factory = MagicMock(return_value=ctx)
    return factory, mock_session


def _decoded_event(i: int = 0) -> DecodedEvent:
    return DecodedEvent(
        timestamp=datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc)
        + timedelta(seconds=i),
        event_code=1,
        event_param=i,
    )


def _sensor_detection(
    vendor_tag: str = "SIG001", mph: int = 35,
) -> SensorDetection:
    return SensorDetection(
        timestamp=datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc),
        vendor_tag=vendor_tag,
        event_type=ROADSIDE_EVENT_TYPE_SPEED,
        mph=mph,
        kph=round(mph * 1.609344),
    )


def _lookup_row(vendor_tag: str, sensor_uuid=None, signal_id: str = "SIG001"):
    """Mocked row from the vendor_tag -> (sensor_id, signal_id) lookup."""
    row = MagicMock()
    row.vendor_lane_id = vendor_tag
    row.sensor_id = sensor_uuid or uuid4()
    row.signal_id = signal_id
    return row


# ---------------------------------------------------------------------------
# _upsert_events — dispatch gate
# ---------------------------------------------------------------------------


class TestUpsertEventsDispatch:
    @pytest.mark.asyncio
    async def test_empty_list_is_noop(self):
        factory, session = _make_session_factory()
        await _upsert_events([], "SIG-001", factory)
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_decoded_events_route_to_controller(self):
        factory, session = _make_session_factory()
        await _upsert_events(
            [_decoded_event(0), _decoded_event(1)],
            "SIG-001",
            factory,
        )
        assert session.execute.await_count == 1
        session.flush.assert_awaited()
        stmt = session.execute.await_args.args[0]
        assert "controller_event_log" in str(stmt.compile())

    @pytest.mark.asyncio
    async def test_all_sensor_detections_route_to_roadside(self):
        factory, session = _make_session_factory(
            lookup_rows=[_lookup_row("SIG001")],
        )
        await _upsert_events(
            [_sensor_detection("SIG001")],
            "ignored-for-sensors",
            factory,
        )
        # Two execute calls: (1) vendor_tag lookup, (2) INSERT
        assert session.execute.await_count == 2
        insert_stmt = session.execute.await_args_list[1].args[0]
        assert "roadside_event" in str(insert_stmt.compile())

    @pytest.mark.asyncio
    async def test_mixed_types_raises_type_error(self):
        factory, _ = _make_session_factory()
        with pytest.raises(TypeError, match="mixed event types"):
            await _upsert_events(
                [_decoded_event(0), _sensor_detection("SIG001")],
                "SIG-001",
                factory,
            )

    @pytest.mark.asyncio
    async def test_unknown_type_raises_type_error(self):
        factory, _ = _make_session_factory()
        with pytest.raises(TypeError, match="unknown event type"):
            await _upsert_events(
                [object()],
                "SIG-001",
                factory,
            )


# ---------------------------------------------------------------------------
# _upsert_sensor_detections — vendor_tag lookup + insert shape
# ---------------------------------------------------------------------------


class TestUpsertSensorDetections:
    @pytest.mark.asyncio
    async def test_resolves_vendor_tag_and_inserts(self):
        sensor_uuid = uuid4()
        factory, session = _make_session_factory(
            lookup_rows=[_lookup_row("SIG001", sensor_uuid=sensor_uuid)],
        )
        await _upsert_sensor_detections(
            [_sensor_detection("SIG001", mph=42)],
            factory,
        )
        # Lookup + INSERT
        assert session.execute.await_count == 2
        session.flush.assert_awaited()

    @pytest.mark.asyncio
    async def test_drops_unresolved_vendor_tags(self, caplog):
        # Lookup returns nothing — every event is orphaned.
        factory, session = _make_session_factory(lookup_rows=[])
        with caplog.at_level("WARNING"):
            await _upsert_sensor_detections(
                [_sensor_detection("MISSING")],
                factory,
            )
        # One execute call for the lookup; no INSERT happened.
        assert session.execute.await_count == 1
        session.flush.assert_not_awaited()
        assert any(
            "unregistered vendor_tag" in rec.message
            for rec in caplog.records
        )

    @pytest.mark.asyncio
    async def test_partial_resolution_inserts_only_resolved(self):
        # One of two tags resolves; the other is dropped.
        factory, session = _make_session_factory(
            lookup_rows=[_lookup_row("SIG001")],
        )
        await _upsert_sensor_detections(
            [
                _sensor_detection("SIG001", mph=20),
                _sensor_detection("MISSING", mph=30),
            ],
            factory,
        )
        assert session.execute.await_count == 2  # lookup + INSERT
        session.flush.assert_awaited()

    @pytest.mark.asyncio
    async def test_empty_list_is_noop(self):
        factory, session = _make_session_factory(lookup_rows=[])
        await _upsert_sensor_detections([], factory)
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# _upsert_controller_events — existing controller path still works
# ---------------------------------------------------------------------------


class TestUpsertControllerEvents:
    @pytest.mark.asyncio
    async def test_inserts_rows(self):
        factory, session = _make_session_factory()
        events = [_decoded_event(i) for i in range(5)]
        await _upsert_controller_events(events, "SIG-001", factory)
        assert session.execute.await_count == 1
        session.flush.assert_awaited()
        stmt = session.execute.await_args.args[0]
        assert "controller_event_log" in str(stmt.compile())


# ---------------------------------------------------------------------------
# persist_events_with_drift_check — drift check skipped for sensors
# ---------------------------------------------------------------------------


class TestDriftCheckDispatch:
    @pytest.mark.asyncio
    async def test_drift_check_runs_for_controller_events(self):
        factory, _ = _make_session_factory()
        far_future = datetime.now(timezone.utc) + timedelta(hours=6)
        events = [
            DecodedEvent(
                timestamp=far_future, event_code=1, event_param=0,
            ),
        ]
        with pytest.MonkeyPatch.context() as mp:
            sent = {}

            async def _capture_notify(**kwargs):
                sent.update(kwargs)

            mp.setattr(
                "tsigma.collection.sdk.notify", _capture_notify,
            )
            await persist_events_with_drift_check(events, "SIG-001", factory)
        assert sent, "drift notification should have fired for far-future DecodedEvent"
        assert sent.get("subject", "").startswith("Clock drift")

    @pytest.mark.asyncio
    async def test_drift_check_skipped_for_sensor_detections(self):
        factory, _ = _make_session_factory(
            lookup_rows=[_lookup_row("SIG001")],
        )
        # Sensor detection with a far-future timestamp — should still
        # insert without firing a drift notification (sensors don't
        # carry a checkpoint to poison, so drift capping has no home).
        far_future = datetime.now(timezone.utc) + timedelta(hours=6)
        det = SensorDetection(
            timestamp=far_future,
            vendor_tag="SIG001",
            event_type=ROADSIDE_EVENT_TYPE_SPEED,
            mph=35, kph=56,
        )
        with pytest.MonkeyPatch.context() as mp:
            sent = {}

            async def _capture_notify(**kwargs):
                sent.update(kwargs)

            mp.setattr(
                "tsigma.collection.sdk.notify", _capture_notify,
            )
            await persist_events_with_drift_check([det], "irrelevant", factory)
        assert sent == {}, (
            "drift notification must NOT fire for SensorDetection — "
            f"fired with {sent}"
        )
