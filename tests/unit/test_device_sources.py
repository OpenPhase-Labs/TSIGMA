"""
Unit tests for ``tsigma.collection.sources``.

Verifies ``SignalDeviceSource`` satisfies ``DeviceSource`` and reproduces
the device-selection behaviour that used to live inline in
``CollectorService._run_poll_cycle``: enabled filter, method filter
from ``signal_metadata["collection"]["method"]``, host injection,
credential decryption.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.sources import DeviceSource, SignalDeviceSource
from tsigma.collection.targets import ControllerTarget


def _mock_signal_row(
    signal_id: str,
    ip_address: str | None,
    metadata: dict | None,
) -> MagicMock:
    row = MagicMock()
    row.signal_id = signal_id
    row.ip_address = ip_address
    row.signal_metadata = metadata
    return row


def _mock_session_with_rows(rows: list) -> AsyncMock:
    session = AsyncMock()
    result = MagicMock()
    result.all.return_value = rows
    session.execute = AsyncMock(return_value=result)
    return session


def test_signal_source_satisfies_protocol():
    source = SignalDeviceSource(
        poll_interval_seconds=900, target=ControllerTarget(),
    )
    assert isinstance(source, DeviceSource)
    assert source.device_type == "controller"
    assert source.poll_interval_seconds == 900
    assert isinstance(source.target, ControllerTarget)


@pytest.mark.asyncio
async def test_lists_only_signals_matching_method():
    source = SignalDeviceSource(
        poll_interval_seconds=900, target=ControllerTarget(),
    )
    session = _mock_session_with_rows([
        _mock_signal_row("SIG-FTP",  "10.0.0.1", {"collection": {"method": "ftp_pull"}}),
        _mock_signal_row("SIG-HTTP", "10.0.0.2", {"collection": {"method": "http_pull"}}),
        _mock_signal_row("SIG-OTHER","10.0.0.3", {"collection": {"method": "tcp_server"}}),
    ])

    with patch(
        "tsigma.collection.sources.signal.has_encryption_key",
        return_value=False,
    ):
        devices = await source.list_devices_for_method(session, "ftp_pull")

    assert [d[0] for d in devices] == ["SIG-FTP"]


@pytest.mark.asyncio
async def test_skips_signals_without_collection_metadata():
    """Enabled signals lacking collection metadata are dropped, not errored."""
    source = SignalDeviceSource(
        poll_interval_seconds=900, target=ControllerTarget(),
    )
    session = _mock_session_with_rows([
        _mock_signal_row("SIG-NO-META", "10.0.0.1", None),
        _mock_signal_row("SIG-NO-COL",  "10.0.0.2", {"other": "stuff"}),
        _mock_signal_row("SIG-GOOD",    "10.0.0.3", {"collection": {"method": "ftp_pull"}}),
    ])

    with patch(
        "tsigma.collection.sources.signal.has_encryption_key",
        return_value=False,
    ):
        devices = await source.list_devices_for_method(session, "ftp_pull")

    assert [d[0] for d in devices] == ["SIG-GOOD"]


@pytest.mark.asyncio
async def test_injects_host_from_ip_address():
    source = SignalDeviceSource(
        poll_interval_seconds=900, target=ControllerTarget(),
    )
    session = _mock_session_with_rows([
        _mock_signal_row(
            "SIG-IP", "192.168.1.42",
            {"collection": {"method": "ftp_pull", "port": 21}},
        ),
    ])

    with patch(
        "tsigma.collection.sources.signal.has_encryption_key",
        return_value=False,
    ):
        devices = await source.list_devices_for_method(session, "ftp_pull")

    assert len(devices) == 1
    _, config = devices[0]
    assert config["host"] == "192.168.1.42"
    assert config["port"] == 21


@pytest.mark.asyncio
async def test_host_empty_when_ip_missing():
    source = SignalDeviceSource(
        poll_interval_seconds=900, target=ControllerTarget(),
    )
    session = _mock_session_with_rows([
        _mock_signal_row(
            "SIG-NO-IP", None,
            {"collection": {"method": "ftp_pull"}},
        ),
    ])

    with patch(
        "tsigma.collection.sources.signal.has_encryption_key",
        return_value=False,
    ):
        devices = await source.list_devices_for_method(session, "ftp_pull")

    assert devices[0][1]["host"] == ""


@pytest.mark.asyncio
async def test_decrypts_credentials_when_key_present():
    """When an encryption key is configured, credentials get decrypted."""
    source = SignalDeviceSource(
        poll_interval_seconds=900, target=ControllerTarget(),
    )
    session = _mock_session_with_rows([
        _mock_signal_row(
            "SIG-ENC", "10.0.0.1",
            {"collection": {"method": "ftp_pull", "password": "encrypted"}},
        ),
    ])

    with (
        patch(
            "tsigma.collection.sources.signal.has_encryption_key",
            return_value=True,
        ),
        patch(
            "tsigma.collection.sources.signal.decrypt_sensitive_fields",
        ) as mock_decrypt,
    ):
        await source.list_devices_for_method(session, "ftp_pull")

    mock_decrypt.assert_called_once()
    # The config is passed wrapped so the existing decrypt helper can
    # find the collection subtree.
    args, _ = mock_decrypt.call_args
    assert "collection" in args[0]
