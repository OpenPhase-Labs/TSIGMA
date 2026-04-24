"""Tests for tsigma.collection.sdk helpers."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.collection.sdk import (
    load_checkpoint,
    persist_events,
    persist_events_with_drift_check,
    record_error,
    resolve_decoder_by_extension,
    resolve_decoder_by_name,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session_factory():
    """Build a mock async session factory matching the ``async with`` pattern."""
    mock_session = AsyncMock()
    mock_session_ctx = MagicMock()
    mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_ctx.__aexit__ = AsyncMock(return_value=None)
    session_factory = MagicMock(return_value=mock_session_ctx)
    return session_factory, mock_session


def _make_events(n=3, *, base_time=None):
    """Create a list of DecodedEvent objects."""
    base = base_time or datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return [
        DecodedEvent(
            timestamp=base + timedelta(seconds=i),
            event_code=1,
            event_param=i,
        )
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# load_checkpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_checkpoint_found():
    factory, session = _make_session_factory()
    sentinel = MagicMock(name="checkpoint_row")

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = sentinel
    session.execute.return_value = result_mock

    cp = await load_checkpoint("http_pull", "controller", "SIG-001", factory)

    assert cp is sentinel
    session.expunge.assert_called_once_with(sentinel)


@pytest.mark.asyncio
async def test_load_checkpoint_not_found():
    factory, session = _make_session_factory()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    session.execute.return_value = result_mock

    cp = await load_checkpoint("http_pull", "controller", "SIG-001", factory)

    assert cp is None
    session.expunge.assert_not_called()


# ---------------------------------------------------------------------------
# record_error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_error_creates_checkpoint():
    factory, session = _make_session_factory()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    session.execute.return_value = result_mock

    fake_cp = MagicMock()
    fake_cp.consecutive_errors = 0

    # PollingCheckpoint is used both as a SQLAlchemy model in select() and
    # as a constructor.  We need select() to accept it, so we patch select
    # to return a mock statement chain, and patch PollingCheckpoint only
    # for construction.
    mock_select = MagicMock()  # select(CP).where(...) chain
    with (
        patch("tsigma.collection.sdk.select", return_value=mock_select),
        patch("tsigma.collection.sdk.PollingCheckpoint", return_value=fake_cp),
    ):
        await record_error(
            "http_pull", "controller", "SIG-001", factory, "connection timeout",
        )

    session.add.assert_called_once_with(fake_cp)
    assert fake_cp.consecutive_errors == 1
    assert fake_cp.last_error == "connection timeout"
    session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_record_error_increments():
    factory, session = _make_session_factory()

    existing_cp = MagicMock()
    existing_cp.consecutive_errors = 2

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = existing_cp
    session.execute.return_value = result_mock

    await record_error(
        "http_pull", "controller", "SIG-001", factory, "timeout again",
    )

    session.add.assert_not_called()
    assert existing_cp.consecutive_errors == 3
    assert existing_cp.last_error == "timeout again"
    session.flush.assert_awaited_once()


# ---------------------------------------------------------------------------
# persist_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persist_events_empty():
    factory, session = _make_session_factory()

    await persist_events([], "SIG-001", factory)

    factory.assert_not_called()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_persist_events_inserts():
    factory, session = _make_session_factory()
    events = _make_events(2)

    with patch("tsigma.collection.sdk.pg_insert") as mock_pg_insert:
        mock_stmt = MagicMock()
        mock_values = MagicMock()
        mock_values.on_conflict_do_nothing.return_value = mock_stmt
        mock_pg_insert.return_value.values.return_value = mock_values

        await persist_events(events, "SIG-001", factory)

    mock_pg_insert.assert_called_once()
    session.execute.assert_awaited_once_with(mock_stmt)
    session.flush.assert_awaited_once()


# ---------------------------------------------------------------------------
# persist_events_with_drift_check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persist_events_with_drift_check_no_drift():
    factory, session = _make_session_factory()
    now = datetime.now(timezone.utc)
    events = _make_events(2, base_time=now - timedelta(minutes=5))

    with (
        patch("tsigma.collection.sdk.pg_insert") as mock_pg_insert,
        patch("tsigma.collection.sdk.notify", new_callable=AsyncMock) as mock_notify,
        patch("tsigma.collection.sdk.settings") as mock_settings,
    ):
        mock_settings.checkpoint_future_tolerance_seconds = 300
        mock_stmt = MagicMock()
        mock_values = MagicMock()
        mock_values.on_conflict_do_nothing.return_value = mock_stmt
        mock_pg_insert.return_value.values.return_value = mock_values

        await persist_events_with_drift_check(events, "SIG-001", factory)

    mock_notify.assert_not_awaited()
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_persist_events_with_drift_check_drift():
    factory, session = _make_session_factory()
    future_time = datetime.now(timezone.utc) + timedelta(hours=1)
    events = _make_events(2, base_time=future_time)

    with (
        patch("tsigma.collection.sdk.pg_insert") as mock_pg_insert,
        patch("tsigma.collection.sdk.notify", new_callable=AsyncMock) as mock_notify,
        patch("tsigma.collection.sdk.settings") as mock_settings,
    ):
        mock_settings.checkpoint_future_tolerance_seconds = 300
        mock_stmt = MagicMock()
        mock_values = MagicMock()
        mock_values.on_conflict_do_nothing.return_value = mock_stmt
        mock_pg_insert.return_value.values.return_value = mock_values

        await persist_events_with_drift_check(events, "SIG-001", factory)

    mock_notify.assert_awaited_once()
    call_kwargs = mock_notify.call_args
    positional_subject = call_kwargs[0][0] if call_kwargs[0] else ""
    subject = call_kwargs.kwargs.get("subject", call_kwargs[1].get("subject", positional_subject))
    assert "Clock drift" in subject
    session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# resolve_decoder_by_name
# ---------------------------------------------------------------------------


def test_resolve_decoder_by_name():
    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_cls.return_value = mock_instance

    with patch("tsigma.collection.sdk.DecoderRegistry") as MockReg:
        MockReg.get.return_value = mock_cls

        result = resolve_decoder_by_name("maxtime")

    MockReg.get.assert_called_once_with("maxtime")
    assert result is mock_instance


def test_resolve_decoder_by_name_unknown():
    with patch("tsigma.collection.sdk.DecoderRegistry") as MockReg:
        MockReg.get.side_effect = ValueError("Unknown decoder: bogus")

        with pytest.raises(ValueError, match="Unknown decoder"):
            resolve_decoder_by_name("bogus")


# ---------------------------------------------------------------------------
# resolve_decoder_by_extension
# ---------------------------------------------------------------------------


def test_resolve_by_extension_dat():
    """A .dat file resolves to a decoder via extension lookup."""
    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_cls.return_value = mock_instance

    with patch("tsigma.collection.sdk.DecoderRegistry") as MockReg:
        MockReg.get_for_extension.return_value = [mock_cls]

        result = resolve_decoder_by_extension("events.dat")

    MockReg.get_for_extension.assert_called_once_with(".dat")
    assert result is mock_instance


def test_resolve_by_extension_with_override():
    """Explicit decoder overrides extension-based lookup."""
    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_cls.return_value = mock_instance

    with patch("tsigma.collection.sdk.DecoderRegistry") as MockReg:
        MockReg.get.return_value = mock_cls

        result = resolve_decoder_by_extension("events.dat", explicit_decoder="maxtime")

    MockReg.get.assert_called_once_with("maxtime")
    MockReg.get_for_extension.assert_not_called()
    assert result is mock_instance


def test_resolve_by_extension_unknown():
    """Unknown extension raises ValueError."""
    with patch("tsigma.collection.sdk.DecoderRegistry") as MockReg:
        MockReg.get_for_extension.return_value = []

        with pytest.raises(ValueError, match="No decoder found for extension"):
            resolve_decoder_by_extension("events.xyz")
