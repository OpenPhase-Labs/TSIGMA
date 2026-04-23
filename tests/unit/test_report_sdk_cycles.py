"""Tests for report SDK cycle aggregate query functions."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest


def _boundary_df(**kwargs):
    """Create a DataFrame mimicking fetch_cycle_boundaries output."""
    defaults = {
        "green_start": [datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
        "yellow_start": [datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)],
        "red_start": [datetime(2026, 1, 1, 12, 0, 34, tzinfo=timezone.utc)],
        "cycle_end": [datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)],
        "green_duration_seconds": [30.0],
        "yellow_duration_seconds": [4.0],
        "red_duration_seconds": [26.0],
        "cycle_duration_seconds": [60.0],
        "termination_type": ["gap_out"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _arrival_df(**kwargs):
    """Create a DataFrame mimicking fetch_cycle_arrivals output."""
    defaults = {
        "arrival_time": [datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)],
        "detector_channel": [5],
        "green_start": [datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
        "time_in_cycle_seconds": [10.0],
        "phase_state": ["green"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _summary_df(**kwargs):
    """Create a DataFrame mimicking fetch_cycle_summary output."""
    defaults = {
        "bin_start": [datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
        "total_cycles": [4],
        "avg_cycle_length_seconds": [60.0],
        "avg_green_seconds": [30.0],
        "total_arrivals": [20],
        "arrivals_on_green": [14],
        "arrivals_on_yellow": [2],
        "arrivals_on_red": [4],
        "arrival_on_green_pct": [70.0],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


class TestFetchCycleBoundaries:
    """Tests for fetch_cycle_boundaries SDK function."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_returns_boundaries(self, mock_facade):
        from tsigma.reports.sdk.cycles import fetch_cycle_boundaries

        df = _boundary_df()
        # Add a second row
        row2 = _boundary_df(
            green_start=[datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)],
        )
        df = pd.concat([df, row2], ignore_index=True)

        mock_facade.get_dataframe = AsyncMock(return_value=df)

        boundaries = await fetch_cycle_boundaries(
            "SIG-001", 2,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        assert len(boundaries) == 2
        mock_facade.get_dataframe.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_empty_returns_empty_dataframe(self, mock_facade):
        from tsigma.reports.sdk.cycles import fetch_cycle_boundaries

        mock_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        boundaries = await fetch_cycle_boundaries(
            "SIG-001", 2,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        assert boundaries.empty


class TestFetchCycleArrivals:
    """Tests for fetch_cycle_arrivals SDK function."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_returns_arrivals(self, mock_facade):
        from tsigma.reports.sdk.cycles import fetch_cycle_arrivals

        df = _arrival_df()
        row2 = _arrival_df(phase_state=["red"])
        df = pd.concat([df, row2], ignore_index=True)

        mock_facade.get_dataframe = AsyncMock(return_value=df)

        arrivals = await fetch_cycle_arrivals(
            "SIG-001", 2,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        assert len(arrivals) == 2

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_filters_by_detector_channels(self, mock_facade):
        from tsigma.reports.sdk.cycles import fetch_cycle_arrivals

        mock_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        await fetch_cycle_arrivals(
            "SIG-001", 2,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            detector_channels=[5, 6],
        )
        mock_facade.get_dataframe.assert_awaited_once()


class TestFetchCycleSummary:
    """Tests for fetch_cycle_summary SDK function."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_returns_summaries(self, mock_facade):
        from tsigma.reports.sdk.cycles import fetch_cycle_summary

        df = _summary_df()
        mock_facade.get_dataframe = AsyncMock(return_value=df)

        summaries = await fetch_cycle_summary(
            "SIG-001", 2,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        assert len(summaries) == 1
        assert summaries.iloc[0]["arrival_on_green_pct"] == 70.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_empty_returns_empty_dataframe(self, mock_facade):
        from tsigma.reports.sdk.cycles import fetch_cycle_summary

        mock_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        summaries = await fetch_cycle_summary(
            "SIG-001", 2,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        assert summaries.empty
