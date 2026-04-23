"""Unit tests for ValidationService."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.scheduler.registry import JobRegistry
from tsigma.validation.registry import ValidationLevel, ValidationRegistry
from tsigma.validation.service import ValidationService


@pytest.fixture
def mock_settings():
    s = MagicMock()
    s.validation_enabled = True
    s.validation_layer1_enabled = True
    s.validation_layer2_enabled = False
    s.validation_layer3_enabled = False
    s.validation_batch_size = 100
    s.validation_interval = 60
    return s


@pytest.fixture
def mock_session_factory():
    """Return a callable that produces an async context manager yielding a mock session."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()

    @asynccontextmanager
    async def factory():
        yield session

    factory._session = session  # expose for assertions
    return factory


def test_get_enabled_levels_layer1_only(mock_settings):
    svc = ValidationService(MagicMock(), mock_settings)
    levels = svc._get_enabled_levels()
    assert ValidationLevel.LAYER1 in levels
    assert ValidationLevel.LAYER2 not in levels
    assert ValidationLevel.LAYER3 not in levels


def test_get_enabled_levels_all(mock_settings):
    mock_settings.validation_layer2_enabled = True
    mock_settings.validation_layer3_enabled = True
    svc = ValidationService(MagicMock(), mock_settings)
    levels = svc._get_enabled_levels()
    assert ValidationLevel.LAYER1 in levels
    assert ValidationLevel.LAYER2 in levels
    assert ValidationLevel.LAYER3 in levels


def test_disabled_returns_no_levels(mock_settings):
    mock_settings.validation_enabled = False
    svc = ValidationService(MagicMock(), mock_settings)
    levels = svc._get_enabled_levels()
    assert levels == []


# ---------------------------------------------------------------------------
# start / stop tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_registers_job(mock_settings, mock_session_factory):
    """start() registers the validation_cycle job with JobRegistry."""
    svc = ValidationService(mock_session_factory, mock_settings)

    with patch.object(ValidationRegistry, "get_by_level", return_value={}):
        with patch.object(
            JobRegistry, "register_func"
        ) as mock_register:
            await svc.start()

    mock_register.assert_called_once()
    call_kwargs = mock_register.call_args
    assert call_kwargs.kwargs.get("name") or call_kwargs[1].get("name") or call_kwargs[0][0] == "validation_cycle"


@pytest.mark.asyncio
async def test_start_instantiates_validators(mock_settings, mock_session_factory):
    """start() instantiates validators for enabled levels."""
    mock_validator_cls = MagicMock()
    mock_validator_cls.return_value = MagicMock()  # instance

    mock_settings.validation_layer1_enabled = True

    svc = ValidationService(mock_session_factory, mock_settings)

    with patch.object(
        ValidationRegistry, "get_by_level",
        return_value={"test_validator": mock_validator_cls},
    ):
        with patch.object(JobRegistry, "register_func"):
            await svc.start()

    mock_validator_cls.assert_called_once()
    assert "test_validator" in svc._validator_instances


@pytest.mark.asyncio
async def test_stop_unregisters_job(mock_settings, mock_session_factory):
    """stop() unregisters the validation_cycle job from JobRegistry."""
    svc = ValidationService(mock_session_factory, mock_settings)

    with patch.object(JobRegistry, "unregister") as mock_unregister:
        await svc.stop()

    mock_unregister.assert_called_once_with("validation_cycle")


@pytest.mark.asyncio
async def test_stop_is_idempotent(mock_settings, mock_session_factory):
    """stop() can be called multiple times safely."""
    svc = ValidationService(mock_session_factory, mock_settings)

    with patch.object(JobRegistry, "unregister") as mock_unregister:
        await svc.stop()
        await svc.stop()

    assert mock_unregister.call_count == 2


# ---------------------------------------------------------------------------
# Validation cycle tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_validation_cycle_no_events(mock_settings, mock_session_factory):
    """Validation cycle returns early when no unvalidated events exist."""
    inner_session = mock_session_factory._session

    # Query returns no rows
    query_result = MagicMock()
    query_result.all.return_value = []
    inner_session.execute = AsyncMock(return_value=query_result)

    svc = ValidationService(mock_session_factory, mock_settings)
    await svc._run_validation_cycle()

    # Execute called once (the SELECT), but no commit (no updates needed)
    inner_session.execute.assert_called_once()
    inner_session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_run_validation_cycle_with_events(mock_settings):
    """Validation cycle runs validators and writes metadata back."""
    # Create mock event rows returned by the query
    row1 = MagicMock()
    row1.signal_id = "SIG-001"
    row1.event_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    row1.event_code = 82
    row1.event_param = 3

    row2 = MagicMock()
    row2.signal_id = "SIG-001"
    row2.event_time = datetime(2025, 6, 1, 12, 0, 1, tzinfo=timezone.utc)
    row2.event_code = 81
    row2.event_param = 3

    # First session context (SELECT): returns rows
    select_result = MagicMock()
    select_result.all.return_value = [row1, row2]

    # Second session context (UPDATE + commit): just accept calls
    update_session = AsyncMock()
    update_session.execute = AsyncMock()
    update_session.commit = AsyncMock()

    call_count = 0

    @asynccontextmanager
    async def counting_factory():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First context: the SELECT query
            mock_s = AsyncMock()
            mock_s.execute = AsyncMock(return_value=select_result)
            yield mock_s
        else:
            # Subsequent contexts: UPDATE writes
            yield update_session

    # Set up a mock validator
    mock_validator = AsyncMock()
    mock_validator.validate_events = AsyncMock(
        return_value=[
            {"validator": "test_v", "status": "clean"},
            {"validator": "test_v", "status": "clean"},
        ]
    )

    svc = ValidationService(counting_factory, mock_settings)
    svc._validator_instances = {"test_v": mock_validator}

    await svc._run_validation_cycle()

    # Validator should have been called with the 2 events for SIG-001
    mock_validator.validate_events.assert_called_once()
    call_args = mock_validator.validate_events.call_args
    events_arg = call_args[0][0]
    assert len(events_arg) == 2
    assert events_arg[0]["signal_id"] == "SIG-001"

    # Update session should have had execute called for each event + commit
    assert update_session.execute.call_count == 2
    update_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_run_validation_cycle_groups_by_signal(mock_settings):
    """Validation cycle groups events by signal_id before running validators."""
    # Create rows from two different signals
    row1 = MagicMock()
    row1.signal_id = "SIG-001"
    row1.event_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    row1.event_code = 82
    row1.event_param = 3

    row2 = MagicMock()
    row2.signal_id = "SIG-002"
    row2.event_time = datetime(2025, 6, 1, 12, 0, 1, tzinfo=timezone.utc)
    row2.event_code = 81
    row2.event_param = 5

    select_result = MagicMock()
    select_result.all.return_value = [row1, row2]

    update_session = AsyncMock()
    update_session.execute = AsyncMock()
    update_session.commit = AsyncMock()

    call_count = 0

    @asynccontextmanager
    async def counting_factory():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            mock_s = AsyncMock()
            mock_s.execute = AsyncMock(return_value=select_result)
            yield mock_s
        else:
            yield update_session

    mock_validator = AsyncMock()
    # Return one result per event per call
    mock_validator.validate_events = AsyncMock(
        side_effect=[
            [{"validator": "test_v", "status": "clean"}],   # SIG-001 (1 event)
            [{"validator": "test_v", "status": "clean"}],   # SIG-002 (1 event)
        ]
    )

    svc = ValidationService(counting_factory, mock_settings)
    svc._validator_instances = {"test_v": mock_validator}

    await svc._run_validation_cycle()

    # Validator should have been called twice: once per signal
    assert mock_validator.validate_events.call_count == 2
    first_call_signal = mock_validator.validate_events.call_args_list[0][0][1]
    second_call_signal = mock_validator.validate_events.call_args_list[1][0][1]
    assert {first_call_signal, second_call_signal} == {"SIG-001", "SIG-002"}


@pytest.mark.asyncio
async def test_run_validation_cycle_handles_validator_error(mock_settings):
    """Validation cycle continues when a validator raises an exception."""
    row1 = MagicMock()
    row1.signal_id = "SIG-001"
    row1.event_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    row1.event_code = 82
    row1.event_param = 3

    select_result = MagicMock()
    select_result.all.return_value = [row1]

    update_session = AsyncMock()
    update_session.execute = AsyncMock()
    update_session.commit = AsyncMock()

    call_count = 0

    @asynccontextmanager
    async def counting_factory():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            mock_s = AsyncMock()
            mock_s.execute = AsyncMock(return_value=select_result)
            yield mock_s
        else:
            yield update_session

    # Validator that raises
    failing_validator = AsyncMock()
    failing_validator.validate_events = AsyncMock(
        side_effect=RuntimeError("boom")
    )

    svc = ValidationService(counting_factory, mock_settings)
    svc._validator_instances = {"failing_v": failing_validator}

    # Should not raise
    await svc._run_validation_cycle()

    # Update should still happen (with empty merged results)
    update_session.execute.assert_called()
    update_session.commit.assert_called_once()
