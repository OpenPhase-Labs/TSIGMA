"""
Unit tests for ``tsigma.collection.targets``.

Verifies that ``ControllerTarget`` satisfies the ``IngestionTarget``
protocol and that each method is a delegate onto the existing
collection SDK — the whole point of this target being a pure no-op
refactor.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.targets import ControllerTarget, IngestionTarget


def test_controller_target_satisfies_protocol():
    """Runtime-checkable Protocol: ControllerTarget is an IngestionTarget."""
    target = ControllerTarget()
    assert isinstance(target, IngestionTarget)
    assert target.device_type == "controller"


def test_resolve_decoder_by_name_delegates():
    target = ControllerTarget()
    sentinel = MagicMock(name="decoder_instance")
    with patch(
        "tsigma.collection.targets.controller.sdk.resolve_decoder_by_name",
        return_value=sentinel,
    ) as mock_resolve:
        result = target.resolve_decoder(decoder_name="asc3")
    assert result is sentinel
    mock_resolve.assert_called_once_with("asc3")


def test_resolve_decoder_by_filename_delegates():
    target = ControllerTarget()
    sentinel = MagicMock(name="decoder_instance")
    with patch(
        "tsigma.collection.targets.controller.sdk.resolve_decoder_by_extension",
        return_value=sentinel,
    ) as mock_resolve:
        result = target.resolve_decoder(filename="event1.dat")
    assert result is sentinel
    mock_resolve.assert_called_once_with("event1.dat")


def test_resolve_decoder_requires_hint():
    target = ControllerTarget()
    with pytest.raises(ValueError, match="decoder_name or filename"):
        target.resolve_decoder()


@pytest.mark.asyncio
async def test_persist_delegates():
    target = ControllerTarget()
    factory = MagicMock()
    events = [MagicMock()]
    with patch(
        "tsigma.collection.targets.controller.sdk.persist_events",
        new_callable=AsyncMock,
    ) as mock_persist:
        await target.persist(events, "SIG-001", factory)
    mock_persist.assert_awaited_once_with(events, "SIG-001", factory)


@pytest.mark.asyncio
async def test_persist_with_drift_check_delegates():
    target = ControllerTarget()
    factory = MagicMock()
    events = [MagicMock()]
    with patch(
        "tsigma.collection.targets.controller.sdk"
        ".persist_events_with_drift_check",
        new_callable=AsyncMock,
    ) as mock_persist:
        await target.persist_with_drift_check(
            events, "SIG-001", factory, source_label="ftp",
        )
    mock_persist.assert_awaited_once_with(
        events, "SIG-001", factory, source_label="ftp",
    )


@pytest.mark.asyncio
async def test_load_checkpoint_delegates_with_controller_device_type():
    target = ControllerTarget()
    factory = MagicMock()
    sentinel = MagicMock(name="checkpoint_row")
    with patch(
        "tsigma.collection.targets.controller.sdk.load_checkpoint",
        new_callable=AsyncMock,
    ) as mock_load:
        mock_load.return_value = sentinel
        result = await target.load_checkpoint("http_pull", "SIG-001", factory)
    assert result is sentinel
    mock_load.assert_awaited_once_with(
        "http_pull", "controller", "SIG-001", factory,
    )


@pytest.mark.asyncio
async def test_save_checkpoint_delegates_with_controller_device_type():
    target = ControllerTarget()
    factory = MagicMock()
    with patch(
        "tsigma.collection.targets.controller.sdk.save_checkpoint",
        new_callable=AsyncMock,
    ) as mock_save:
        await target.save_checkpoint(
            "http_pull", "SIG-001", factory,
            last_event_timestamp="ts",
            events_ingested=42,
        )
    mock_save.assert_awaited_once_with(
        "http_pull", "controller", "SIG-001", factory,
        last_event_timestamp="ts",
        events_ingested=42,
    )


@pytest.mark.asyncio
async def test_record_error_delegates_with_controller_device_type():
    target = ControllerTarget()
    factory = MagicMock()
    with patch(
        "tsigma.collection.targets.controller.sdk.record_error",
        new_callable=AsyncMock,
    ) as mock_record:
        await target.record_error(
            "http_pull", "SIG-001", factory, "connection timeout",
        )
    mock_record.assert_awaited_once_with(
        "http_pull", "controller", "SIG-001", factory, "connection timeout",
    )
