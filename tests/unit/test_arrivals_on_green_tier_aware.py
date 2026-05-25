"""
Stage 2c migration tests — arrivals_on_green._fetch_raw_events.

Pins the contract that ArrivalsOnGreenReport's raw-event path dispatches
through the tier-aware SDK helper ``fetch_events_split`` rather than
calling ``db_facade.get_dataframe`` directly. Three concerns:

1. The SDK helper is called with the correct keyword shape.
2. ``db_facade.get_dataframe`` is NOT called from this path after migration.
3. The downstream classification logic (``_classify_arrivals`` /
   ``_aggregate_aog``) is unchanged by the SDK swap — given the same
   event sequence, the report produces the same AOG numbers.

These tests target the public ``execute`` entry point and mock the SDK
call site, matching the convention used by the existing
``TestArrivalsOnGreenWithData`` suite.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tests._helpers import make_mock_session
from tsigma.reports.sdk.events import (
    EVENT_DETECTOR_ON,
    EVENT_PHASE_GREEN,
    EVENT_YELLOW_CLEARANCE,
)

# ---------------------------------------------------------------------------
# Minimal local helpers (kept private; mirror test_report_with_data.py shape)
# ---------------------------------------------------------------------------


def _event(code: int, param: int, time: datetime) -> SimpleNamespace:
    """Build a fake ControllerEventLog row (matches test_report_with_data._event)."""
    return SimpleNamespace(
        signal_id="SIG-001",
        event_code=code,
        event_param=param,
        event_time=time,
        device_id=1,
        validation_metadata=None,
    )


def _events_to_df(events: list[SimpleNamespace]) -> pd.DataFrame:
    """Convert events to the canonical DataFrame shape fetch_events_split returns."""
    if not events:
        return pd.DataFrame(columns=["event_code", "event_param", "event_time"])
    return pd.DataFrame([
        {
            "event_code": e.event_code,
            "event_param": e.event_param,
            "event_time": e.event_time,
        }
        for e in events
    ])


_START_ISO = "2025-06-15T08:00:00"
_END_ISO = "2025-06-15T09:00:00"


def _mock_session():
    """AsyncSession mock — not used by reports that get events via patched helpers."""
    session = make_mock_session()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)
    return session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("tsigma.reports.arrivals_on_green.fetch_events_split", new_callable=AsyncMock)
@patch("tsigma.reports.arrivals_on_green.load_channel_to_phase", new_callable=AsyncMock)
async def test_fetch_raw_events_calls_fetch_events_split_with_correct_args(
    mock_ch_map, mock_fetch,
):
    """
    The raw-event path must dispatch to fetch_events_split with:
        - signal_id, start, end positional args matching the parsed window
        - phase_codes containing {EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE}
        - det_channels containing exactly the keys of channel_to_phase
        - det_codes either not passed or equal to (EVENT_DETECTOR_ON,)
    """
    from tsigma.reports.arrivals_on_green import (
        ArrivalsOnGreenParams,
        ArrivalsOnGreenReport,
    )
    from tsigma.reports.sdk import parse_time

    channel_to_phase = {5: 2, 7: 4}
    mock_ch_map.return_value = channel_to_phase
    mock_fetch.return_value = pd.DataFrame(
        columns=["event_code", "event_param", "event_time"]
    )

    report = ArrivalsOnGreenReport()
    params = ArrivalsOnGreenParams(
        signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
    )
    await report.execute(params, _mock_session())

    assert mock_fetch.await_count == 1, (
        f"fetch_events_split should be awaited exactly once, "
        f"got {mock_fetch.await_count}"
    )

    call = mock_fetch.call_args
    args, kwargs = call.args, call.kwargs

    # signal_id, start, end may be passed positionally or by name.
    # Normalize to a dict for assertion.
    if len(args) >= 3:
        signal_id, start, end = args[0], args[1], args[2]
    else:
        signal_id = kwargs.get("signal_id", args[0] if args else None)
        start = kwargs.get("start")
        end = kwargs.get("end")

    expected_start = parse_time(_START_ISO)
    expected_end = parse_time(_END_ISO)

    assert signal_id == "SIG-001"
    assert start == expected_start
    assert end == expected_end

    # phase_codes is keyword-only on fetch_events_split — must come via kwargs.
    assert "phase_codes" in kwargs, "phase_codes must be passed as a keyword arg"
    assert set(kwargs["phase_codes"]) == {
        EVENT_PHASE_GREEN,
        EVENT_YELLOW_CLEARANCE,
    }

    assert "det_channels" in kwargs, "det_channels must be passed as a keyword arg"
    assert set(kwargs["det_channels"]) == set(channel_to_phase.keys())

    # det_codes is either omitted (default) or explicitly (EVENT_DETECTOR_ON,).
    if "det_codes" in kwargs:
        assert tuple(kwargs["det_codes"]) == (EVENT_DETECTOR_ON,)


def test_arrivals_on_green_module_no_longer_imports_db_facade():
    """Stage 2c removed the direct db_facade fallback from this report.

    The fetch path now goes through the tier-aware SDK helper
    fetch_events_split, so db_facade should no longer be a module-level
    name. Structural absence of the import is a stronger guarantee than
    a runtime mock — even an accidental future re-introduction would be
    caught here.
    """
    import tsigma.reports.arrivals_on_green as mod

    assert not hasattr(mod, "db_facade"), (
        "tsigma.reports.arrivals_on_green should not re-import db_facade "
        "after Stage 2c migration to fetch_events_split"
    )


@pytest.mark.asyncio
@patch("tsigma.reports.arrivals_on_green.fetch_events_split", new_callable=AsyncMock)
@patch("tsigma.reports.arrivals_on_green.load_channel_to_phase", new_callable=AsyncMock)
async def test_arrivals_classification_unchanged_with_split_sdk(
    mock_ch_map, mock_fetch,
):
    """
    Equivalence: with the same 5-event sequence that test_arrivals_during_green
    feeds the old db_facade mock, routing through fetch_events_split must yield
    identical AOG numbers (phase=2, total=3, on_green=2, pct=66.7).

    This proves the SDK swap is behavior-preserving for the classification path.
    """
    from tsigma.reports.arrivals_on_green import (
        ArrivalsOnGreenParams,
        ArrivalsOnGreenReport,
    )

    mock_ch_map.return_value = {5: 2}

    t0 = datetime(2025, 6, 15, 8, 0, 0)
    events = [
        _event(EVENT_PHASE_GREEN, 2, t0),
        _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
        _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),
        _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
        _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=35)),
    ]
    mock_fetch.return_value = _events_to_df(events)

    report = ArrivalsOnGreenReport()
    params = ArrivalsOnGreenParams(
        signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
    )
    result = await report.execute(params, _mock_session())

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    row = result.iloc[0]
    assert row["phase_number"] == 2
    assert row["total_arrivals"] == 3
    assert row["arrivals_on_green"] == 2
    assert row["aog_percentage"] == pytest.approx(66.7, abs=0.1)
