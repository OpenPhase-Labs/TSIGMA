"""
Unit tests for the report SDK utility modules.

Tests time_bins helpers, events constants, occupancy calculations,
plan lookups, and config helpers. Pure functions and constants — no
database mocking required (except plan_at / programmed_split which
use plain model objects).
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from tsigma.config_resolver import ApproachSnapshot, DetectorSnapshot, SignalConfig
from tsigma.reports.sdk.events import (
    DETECTOR_EVENT_CODES,
    DIRECTION_MAP,
    EVENT_DETECTOR_OFF,
    EVENT_DETECTOR_ON,
    EVENT_GAP_OUT,
    EVENT_NAMES,
    EVENT_PHASE_GREEN,
    TERMINATION_CODES,
    TERMINATION_NAMES,
)
from tsigma.reports.sdk.occupancy import (
    accumulate_on_time,
    bin_occupancy_pct,
    calculate_occupancy,
)
from tsigma.reports.sdk.plans import plan_at, programmed_split
from tsigma.reports.sdk.time_bins import (
    bin_index,
    bin_timestamp,
    parse_time,
    total_bins,
)

# ---------------------------------------------------------------------------
# parse_time
# ---------------------------------------------------------------------------


class TestParseTime:
    def test_parse_iso_string(self):
        """ISO-8601 string is parsed to datetime."""
        result = parse_time("2025-06-15T08:30:00")
        assert isinstance(result, datetime)
        assert result == datetime(2025, 6, 15, 8, 30, 0)

    def test_parse_iso_string_with_seconds(self):
        """ISO-8601 string with seconds is parsed correctly."""
        result = parse_time("2025-01-01T00:00:59")
        assert result.second == 59

    def test_passthrough_datetime(self):
        """datetime object is returned as-is."""
        dt = datetime(2025, 6, 15, 8, 30, 0)
        result = parse_time(dt)
        assert result is dt


# ---------------------------------------------------------------------------
# bin_timestamp
# ---------------------------------------------------------------------------


class TestBinTimestamp:
    def test_15min_bins(self):
        """Timestamps are truncated to 15-minute boundaries."""
        dt = datetime(2025, 6, 15, 8, 37, 45)
        result = bin_timestamp(dt, 15)
        assert result == "2025-06-15T08:30:00"

    def test_5min_bins(self):
        """Timestamps are truncated to 5-minute boundaries."""
        dt = datetime(2025, 6, 15, 8, 23, 10)
        result = bin_timestamp(dt, 5)
        assert result == "2025-06-15T08:20:00"

    def test_exact_boundary(self):
        """Timestamp on a bin boundary stays unchanged (ignoring seconds)."""
        dt = datetime(2025, 6, 15, 8, 30, 0)
        result = bin_timestamp(dt, 15)
        assert result == "2025-06-15T08:30:00"

    def test_1min_bins(self):
        """1-minute bins truncate seconds only."""
        dt = datetime(2025, 6, 15, 8, 42, 55)
        result = bin_timestamp(dt, 1)
        assert result == "2025-06-15T08:42:00"


# ---------------------------------------------------------------------------
# bin_index
# ---------------------------------------------------------------------------


class TestBinIndex:
    def test_first_bin(self):
        """Moment at the start time is bin 0."""
        start = datetime(2025, 6, 15, 8, 0, 0)
        moment = datetime(2025, 6, 15, 8, 0, 0)
        assert bin_index(moment, start, 15) == 0

    def test_second_bin(self):
        """Moment 15 minutes after start is bin 1."""
        start = datetime(2025, 6, 15, 8, 0, 0)
        moment = datetime(2025, 6, 15, 8, 15, 0)
        assert bin_index(moment, start, 15) == 1

    def test_mid_bin(self):
        """Moment in the middle of a bin returns the correct bin index."""
        start = datetime(2025, 6, 15, 8, 0, 0)
        moment = datetime(2025, 6, 15, 8, 7, 30)
        assert bin_index(moment, start, 15) == 0

    def test_5min_bins(self):
        """bin_index with 5-minute bins at 22 minutes = bin 4."""
        start = datetime(2025, 6, 15, 8, 0, 0)
        moment = datetime(2025, 6, 15, 8, 22, 0)
        assert bin_index(moment, start, 5) == 4


# ---------------------------------------------------------------------------
# total_bins
# ---------------------------------------------------------------------------


class TestTotalBins:
    def test_exact_division(self):
        """60-minute span / 15-min bins = 4 bins."""
        start = datetime(2025, 6, 15, 8, 0, 0)
        end = datetime(2025, 6, 15, 9, 0, 0)
        assert total_bins(start, end, 15) == 4

    def test_partial_bin_rounds_up(self):
        """61-minute span / 15-min bins = 5 bins (partial counts)."""
        start = datetime(2025, 6, 15, 8, 0, 0)
        end = datetime(2025, 6, 15, 9, 1, 0)
        assert total_bins(start, end, 15) == 5

    def test_zero_span_returns_one(self):
        """Zero-length span returns minimum of 1 bin."""
        start = datetime(2025, 6, 15, 8, 0, 0)
        assert total_bins(start, start, 15) == 1

    def test_zero_bin_size_returns_one(self):
        """Zero bin size returns 1 (guarded by code)."""
        start = datetime(2025, 6, 15, 8, 0, 0)
        end = datetime(2025, 6, 15, 9, 0, 0)
        assert total_bins(start, end, 0) == 1

    def test_24_hour_span(self):
        """24-hour span / 15-min bins = 96 bins."""
        start = datetime(2025, 6, 15, 0, 0, 0)
        end = datetime(2025, 6, 16, 0, 0, 0)
        assert total_bins(start, end, 15) == 96


# ---------------------------------------------------------------------------
# Event constants
# ---------------------------------------------------------------------------


class TestEventConstants:
    def test_detector_event_codes_tuple(self):
        """DETECTOR_EVENT_CODES contains ON and OFF."""
        assert EVENT_DETECTOR_ON in DETECTOR_EVENT_CODES
        assert EVENT_DETECTOR_OFF in DETECTOR_EVENT_CODES
        assert len(DETECTOR_EVENT_CODES) == 2

    def test_termination_codes_keys(self):
        """TERMINATION_CODES maps gap_out, max_out, force_off."""
        assert set(TERMINATION_CODES.values()) == {"gap_out", "max_out", "force_off"}

    def test_termination_names_match_codes(self):
        """TERMINATION_NAMES tuple matches TERMINATION_CODES values."""
        assert set(TERMINATION_NAMES) == set(TERMINATION_CODES.values())

    def test_event_names_has_all_standard_codes(self):
        """EVENT_NAMES contains human-readable names for key codes."""
        assert EVENT_NAMES[EVENT_PHASE_GREEN] == "Phase Green"
        assert EVENT_NAMES[EVENT_GAP_OUT] == "Gap Out"
        assert EVENT_NAMES[EVENT_DETECTOR_ON] == "Detector On"

    def test_direction_map(self):
        """DIRECTION_MAP maps 1-4 to compass abbreviations."""
        assert DIRECTION_MAP == {1: "NB", 2: "SB", 3: "EB", 4: "WB"}

    def test_event_code_values(self):
        """Spot-check NTCIP 1202 event code values."""
        assert EVENT_PHASE_GREEN == 1
        assert EVENT_DETECTOR_ON == 82
        assert EVENT_DETECTOR_OFF == 81


# ---------------------------------------------------------------------------
# calculate_occupancy
# ---------------------------------------------------------------------------


class TestCalculateOccupancy:
    def test_full_occupancy(self):
        """Detector on for the entire window yields 1.0."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0, EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=10), EVENT_DETECTOR_OFF),
        ]
        assert calculate_occupancy(events, t0, 10.0) == 1.0

    def test_half_occupancy(self):
        """Detector on for half the window yields 0.5."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0, EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=5), EVENT_DETECTOR_OFF),
        ]
        assert calculate_occupancy(events, t0, 10.0) == 0.5

    def test_zero_window_returns_zero(self):
        """Zero-length window returns 0.0 without error."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [(t0, EVENT_DETECTOR_ON)]
        assert calculate_occupancy(events, t0, 0.0) == 0.0

    def test_negative_window_returns_zero(self):
        """Negative window duration returns 0.0."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        assert calculate_occupancy([], t0, -5.0) == 0.0

    def test_no_events_returns_zero(self):
        """Empty event list produces 0.0 occupancy."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        assert calculate_occupancy([], t0, 10.0) == 0.0

    def test_on_before_window_start(self):
        """Detector on before window start — on-time counted from window start."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0 - timedelta(seconds=5), EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=5), EVENT_DETECTOR_OFF),
        ]
        # On-time is from t0 to t0+5 = 5s out of 10s
        assert calculate_occupancy(events, t0, 10.0) == 0.5

    def test_on_extends_past_window(self):
        """Detector on extends past window end — clamped to window."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0, EVENT_DETECTOR_ON),
            # No off event within window
        ]
        # Trailing on is closed at window_end -> full occupancy
        assert calculate_occupancy(events, t0, 10.0) == 1.0

    def test_multiple_on_off_pairs(self):
        """Multiple on/off pairs sum correctly."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0 + timedelta(seconds=0), EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=2), EVENT_DETECTOR_OFF),
            (t0 + timedelta(seconds=5), EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=8), EVENT_DETECTOR_OFF),
        ]
        # 2s + 3s = 5s out of 10s
        assert calculate_occupancy(events, t0, 10.0) == 0.5

    def test_off_without_on_ignored(self):
        """An off event with no preceding on is harmless."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0 + timedelta(seconds=2), EVENT_DETECTOR_OFF),
            (t0 + timedelta(seconds=5), EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=8), EVENT_DETECTOR_OFF),
        ]
        # Only the second on/off pair counts: 3s out of 10s
        assert calculate_occupancy(events, t0, 10.0) == 0.3

    def test_event_past_window_end_ignored(self):
        """Events after window end are ignored (break)."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0 + timedelta(seconds=2), EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=5), EVENT_DETECTOR_OFF),
            # This pair is past window
            (t0 + timedelta(seconds=15), EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=18), EVENT_DETECTOR_OFF),
        ]
        # 3s out of 10s
        assert calculate_occupancy(events, t0, 10.0) == 0.3


# ---------------------------------------------------------------------------
# accumulate_on_time
# ---------------------------------------------------------------------------


class TestAccumulateOnTime:
    def test_single_bin(self):
        """On-interval within one bin adds the correct seconds."""
        bins: dict[str, float] = defaultdict(float)
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        accumulate_on_time(bins, t0 + timedelta(seconds=10), t0 + timedelta(seconds=20), 15)
        assert len(bins) == 1
        assert list(bins.values())[0] == pytest.approx(10.0)

    def test_spans_two_bins(self):
        """On-interval crossing a bin boundary splits correctly."""
        bins: dict[str, float] = defaultdict(float)
        # On from 08:14:00 to 08:16:00 with 15-min bins
        t_on = datetime(2025, 6, 15, 8, 14, 0)
        t_off = datetime(2025, 6, 15, 8, 16, 0)
        accumulate_on_time(bins, t_on, t_off, 15)

        # Should split into bin 08:00 (1 minute) and bin 08:15 (1 minute)
        assert len(bins) == 2
        total = sum(bins.values())
        assert total == pytest.approx(120.0)  # 2 minutes

    def test_exact_bin_boundary(self):
        """On-interval that starts exactly at a bin boundary."""
        bins: dict[str, float] = defaultdict(float)
        t_on = datetime(2025, 6, 15, 8, 15, 0)
        t_off = datetime(2025, 6, 15, 8, 15, 30)
        accumulate_on_time(bins, t_on, t_off, 15)

        assert len(bins) == 1
        assert list(bins.values())[0] == pytest.approx(30.0)

    def test_zero_duration_no_change(self):
        """Zero-length on-interval adds nothing."""
        bins: dict[str, float] = defaultdict(float)
        t = datetime(2025, 6, 15, 8, 5, 0)
        accumulate_on_time(bins, t, t, 15)
        assert len(bins) == 0


# ---------------------------------------------------------------------------
# bin_occupancy_pct
# ---------------------------------------------------------------------------


class TestBinOccupancyPct:
    def test_single_on_off(self):
        """Single on/off pair produces correct percentage."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0 + timedelta(seconds=0), EVENT_DETECTOR_ON),
            (t0 + timedelta(seconds=450), EVENT_DETECTOR_OFF),  # 7.5 min
        ]
        end = t0 + timedelta(minutes=15)
        result = bin_occupancy_pct(events, end, 15)

        # 450s out of 900s = 50%
        assert len(result) == 1
        assert list(result.values())[0] == pytest.approx(50.0)

    def test_trailing_on_closed_at_end(self):
        """Detector on with no off is closed at end_time."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0, EVENT_DETECTOR_ON),
        ]
        end = t0 + timedelta(minutes=15)
        result = bin_occupancy_pct(events, end, 15)

        # Full 900s / 900s = 100%
        assert len(result) == 1
        assert list(result.values())[0] == pytest.approx(100.0)

    def test_empty_events_empty_result(self):
        """No events yields empty dict."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        result = bin_occupancy_pct([], t0 + timedelta(minutes=15), 15)
        assert result == {}

    def test_multi_bin_occupancy(self):
        """On-interval spanning two bins produces entries for both."""
        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            (t0 + timedelta(minutes=14), EVENT_DETECTOR_ON),
            (t0 + timedelta(minutes=16), EVENT_DETECTOR_OFF),
        ]
        end = t0 + timedelta(minutes=30)
        result = bin_occupancy_pct(events, end, 15)

        assert len(result) == 2
        total_on = sum(v * 900 / 100 for v in result.values())
        assert total_on == pytest.approx(120.0)  # 2 minutes


# ---------------------------------------------------------------------------
# plan_at / programmed_split
# ---------------------------------------------------------------------------


def _make_plan(effective_from, effective_to=None, splits=None, plan_number=1):
    """Build a fake SignalPlan object."""
    plan = type("FakePlan", (), {})()
    plan.signal_id = "SIG-001"
    plan.effective_from = effective_from
    plan.effective_to = effective_to
    plan.plan_number = plan_number
    plan.cycle_length = 120
    plan.offset = 0
    plan.splits = splits
    return plan


class TestPlanAt:
    def test_single_plan_in_range(self):
        """Returns the plan when moment is within its effective range."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [_make_plan(t0, t0 + timedelta(hours=4))]
        result = plan_at(plans, t0 + timedelta(hours=1))
        assert result is plans[0]

    def test_no_plan_before_effective(self):
        """Returns None when moment is before all plans."""
        t0 = datetime(2025, 6, 15, 10, 0, 0)
        plans = [_make_plan(t0, t0 + timedelta(hours=4))]
        result = plan_at(plans, t0 - timedelta(hours=1))
        assert result is None

    def test_open_ended_plan(self):
        """Plan with effective_to=None covers any future moment."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [_make_plan(t0, None)]
        result = plan_at(plans, t0 + timedelta(days=30))
        assert result is plans[0]

    def test_second_plan_overrides_first(self):
        """Later plan is returned when moment falls in both (sorted order)."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [
            _make_plan(t0, t0 + timedelta(hours=8)),
            _make_plan(t0 + timedelta(hours=4), None),
        ]
        result = plan_at(plans, t0 + timedelta(hours=5))
        assert result is plans[1]

    def test_empty_plans(self):
        """Empty plan list returns None."""
        assert plan_at([], datetime(2025, 6, 15, 8, 0, 0)) is None

    def test_plan_expired(self):
        """Moment after effective_to returns None."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [_make_plan(t0, t0 + timedelta(hours=2))]
        result = plan_at(plans, t0 + timedelta(hours=3))
        assert result is None


class TestProgrammedSplit:
    def test_known_phase(self):
        """Returns the split value for a known phase."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [_make_plan(t0, None, splits={"2": 35, "4": 25, "6": 40})]
        assert programmed_split(plans, 2, t0 + timedelta(hours=1)) == 35.0
        assert programmed_split(plans, 4, t0 + timedelta(hours=1)) == 25.0
        assert programmed_split(plans, 6, t0 + timedelta(hours=1)) == 40.0

    def test_unknown_phase_returns_zero(self):
        """Phase not in splits dict returns 0.0."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [_make_plan(t0, None, splits={"2": 35})]
        assert programmed_split(plans, 8, t0 + timedelta(hours=1)) == 0.0

    def test_no_active_plan_returns_zero(self):
        """No plan active at moment returns 0.0."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [_make_plan(t0, t0 + timedelta(hours=1), splits={"2": 35})]
        assert programmed_split(plans, 2, t0 + timedelta(hours=5)) == 0.0

    def test_plan_with_none_splits(self):
        """Plan with splits=None returns 0.0."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [_make_plan(t0, None, splits=None)]
        assert programmed_split(plans, 2, t0 + timedelta(hours=1)) == 0.0

    def test_empty_splits_dict(self):
        """Plan with empty splits dict returns 0.0."""
        t0 = datetime(2025, 6, 15, 6, 0, 0)
        plans = [_make_plan(t0, None, splits={})]
        assert programmed_split(plans, 2, t0 + timedelta(hours=1)) == 0.0


# ---------------------------------------------------------------------------
# SDK config helpers — with populated config
# ---------------------------------------------------------------------------


def _populated_config():
    """SignalConfig with two approaches, detectors, and ped phase."""
    approaches = [
        ApproachSnapshot(
            approach_id="APP-1",
            signal_id="SIG-001",
            direction_type_id=1,
            protected_phase_number=2,
            permissive_phase_number=None,
            is_protected_phase_overlap=False,
            is_permissive_phase_overlap=False,
            ped_phase_number=22,
            mph=45,
            description="Northbound",
        ),
        ApproachSnapshot(
            approach_id="APP-2",
            signal_id="SIG-001",
            direction_type_id=3,
            protected_phase_number=4,
            permissive_phase_number=None,
            is_protected_phase_overlap=False,
            is_permissive_phase_overlap=False,
            ped_phase_number=None,
            mph=35,
            description="Eastbound",
        ),
    ]
    detectors = [
        DetectorSnapshot(
            detector_id="DET-5",
            approach_id="APP-1",
            detector_channel=5,
            distance_from_stop_bar=400,
            min_speed_filter=5,
            lane_number=1,
        ),
        DetectorSnapshot(
            detector_id="DET-6",
            approach_id="APP-1",
            detector_channel=6,
            distance_from_stop_bar=None,
            min_speed_filter=None,
            lane_number=2,
        ),
        DetectorSnapshot(
            detector_id="DET-7",
            approach_id="APP-2",
            detector_channel=7,
            distance_from_stop_bar=350,
            min_speed_filter=5,
            lane_number=1,
        ),
    ]
    return SignalConfig(
        signal_id="SIG-001",
        as_of=datetime(2025, 6, 15),
        from_audit=False,
        approaches=approaches,
        detectors=detectors,
    )


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.config.get_config_at", new_callable=AsyncMock)
async def test_load_channel_to_phase_populated(mock_get_config):
    """With approaches, returns channel -> phase mapping."""
    from tsigma.reports.sdk.config import load_channel_to_phase

    mock_get_config.return_value = _populated_config()
    session = AsyncMock()

    result = await load_channel_to_phase(session, "SIG-001", datetime(2025, 6, 15))

    assert result == {5: 2, 6: 2, 7: 4}


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.config.get_config_at", new_callable=AsyncMock)
async def test_load_channels_for_phase_populated(mock_get_config):
    """Returns only channels assigned to the requested phase."""
    from tsigma.reports.sdk.config import load_channels_for_phase

    mock_get_config.return_value = _populated_config()
    session = AsyncMock()

    result = await load_channels_for_phase(session, "SIG-001", 2, datetime(2025, 6, 15))

    assert result == {5, 6}


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.config.get_config_at", new_callable=AsyncMock)
async def test_load_channel_to_ped_phase_populated(mock_get_config):
    """Only approaches with a ped phase produce entries."""
    from tsigma.reports.sdk.config import load_channel_to_ped_phase

    mock_get_config.return_value = _populated_config()
    session = AsyncMock()

    result = await load_channel_to_ped_phase(session, "SIG-001", datetime(2025, 6, 15))

    # APP-1 has ped_phase_number=22, APP-2 has None
    assert result == {5: 22, 6: 22}


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.config.get_config_at", new_callable=AsyncMock)
async def test_load_channel_to_approach_populated(mock_get_config):
    """Returns approach info dicts with distance where present."""
    from tsigma.reports.sdk.config import load_channel_to_approach

    mock_get_config.return_value = _populated_config()
    session = AsyncMock()

    result = await load_channel_to_approach(session, "SIG-001", datetime(2025, 6, 15))

    assert 5 in result
    assert result[5]["approach_id"] == "APP-1"
    assert result[5]["direction_type_id"] == 1
    assert result[5]["distance_from_stop_bar"] == 400

    # Channel 6 has no distance
    assert 6 in result
    assert "distance_from_stop_bar" not in result[6]

    # Channel 7
    assert result[7]["approach_id"] == "APP-2"
    assert result[7]["distance_from_stop_bar"] == 350


# ---------------------------------------------------------------------------
# fetch_plans
# ---------------------------------------------------------------------------


class TestFetchPlans:
    """Tests for fetch_plans async function."""

    @pytest.mark.asyncio
    async def test_fetch_plans_returns_results(self):
        """fetch_plans returns plans overlapping the time window."""
        from unittest.mock import MagicMock as MM

        from tsigma.reports.sdk.plans import fetch_plans

        t0 = datetime(2025, 6, 15, 6, 0, 0)
        t1 = datetime(2025, 6, 15, 18, 0, 0)

        fake_plan = _make_plan(t0, t1)
        mock_result = MM()
        mock_result.scalars.return_value.all.return_value = [fake_plan]

        session = AsyncMock()
        session.execute = AsyncMock(return_value=mock_result)

        plans = await fetch_plans(session, "SIG-001", t0, t1)

        assert len(plans) == 1
        assert plans[0] is fake_plan
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fetch_plans_empty(self):
        """fetch_plans returns empty list when no plans overlap."""
        from unittest.mock import MagicMock as MM

        from tsigma.reports.sdk.plans import fetch_plans

        t0 = datetime(2025, 6, 15, 6, 0, 0)
        t1 = datetime(2025, 6, 15, 18, 0, 0)

        mock_result = MM()
        mock_result.scalars.return_value.all.return_value = []

        session = AsyncMock()
        session.execute = AsyncMock(return_value=mock_result)

        plans = await fetch_plans(session, "SIG-001", t0, t1)

        assert plans == []


# ---------------------------------------------------------------------------
# fetch_events
# ---------------------------------------------------------------------------


class TestFetchEvents:
    """Tests for fetch_events SDK function."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    async def test_returns_dataframe(self, mock_facade):
        """fetch_events returns a DataFrame with expected columns."""
        from tsigma.reports.sdk.queries import fetch_events

        df = pd.DataFrame({
            "event_code": [1, 82],
            "event_param": [2, 5],
            "event_time": [
                datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc),
            ],
        })
        mock_facade.get_dataframe = AsyncMock(return_value=df)

        result = await fetch_events(
            "SIG-001",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            event_codes=[1, 82],
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        mock_facade.get_dataframe.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    async def test_with_event_param_in(self, mock_facade):
        """fetch_events with event_param_in filter passes additional condition."""
        from tsigma.reports.sdk.queries import fetch_events

        mock_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        await fetch_events(
            "SIG-001",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            event_codes=[82],
            event_param_in=[5, 6],
        )
        mock_facade.get_dataframe.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    async def test_empty_returns_empty_dataframe(self, mock_facade):
        """fetch_events returns empty DataFrame when no rows match."""
        from tsigma.reports.sdk.queries import fetch_events

        mock_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        result = await fetch_events(
            "SIG-001",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            event_codes=[1],
        )
        assert result.empty


# ---------------------------------------------------------------------------
# fetch_events_split
# ---------------------------------------------------------------------------


class TestFetchEventsSplit:
    """Tests for fetch_events_split SDK function."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    async def test_returns_dataframe(self, mock_facade):
        """fetch_events_split returns a DataFrame with expected columns."""
        from tsigma.reports.sdk.queries import fetch_events_split

        df = pd.DataFrame({
            "event_code": [1, 82],
            "event_param": [2, 5],
            "event_time": [
                datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc),
            ],
        })
        mock_facade.get_dataframe = AsyncMock(return_value=df)

        result = await fetch_events_split(
            "SIG-001",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            phase_codes=[1, 7, 8, 9, 10, 11],
            det_channels=[5, 6],
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        mock_facade.get_dataframe.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    async def test_empty_returns_empty_dataframe(self, mock_facade):
        """fetch_events_split returns empty DataFrame when no rows match."""
        from tsigma.reports.sdk.queries import fetch_events_split

        mock_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        result = await fetch_events_split(
            "SIG-001",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            phase_codes=[1],
            det_channels=[5],
        )
        assert result.empty

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    async def test_custom_det_codes(self, mock_facade):
        """fetch_events_split accepts custom det_codes parameter."""
        from tsigma.reports.sdk.queries import fetch_events_split

        mock_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        await fetch_events_split(
            "SIG-001",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            phase_codes=[1, 7],
            det_channels=[5],
            det_codes=[82, 81],
        )
        mock_facade.get_dataframe.assert_awaited_once()
