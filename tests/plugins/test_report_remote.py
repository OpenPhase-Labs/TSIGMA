"""Phase 5 gate: the first vertical slice - a report served by a real plugin.

Host calls Report.Generate over a real gRPC connection to a real subprocess; the
plugin streams a ViewModel and Arrow batches; the host reassembles the DataFrame
the rest of the app expects. Nothing is mocked.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

from tsigma.plugins.connection import LaunchedConnection
from tsigma.plugins.remote_report import (
    RemoteReport,
    RemoteReportError,
    arrow_batches_to_dataframe,
    dataframe_to_arrow_batch,
    metadata_from_describe,
)
from tsigma.reports.registry import Report, ReportRegistry

PLUGIN = str(Path(__file__).parent / "fake_report_plugin.py")


def _command(*args: str) -> list[str]:
    return [sys.executable, PLUGIN, *args]


async def _remote(*args: str) -> tuple[RemoteReport, LaunchedConnection]:
    conn = LaunchedConnection("fake-delay", _command(*args))
    await conn.connect()
    from tsigma.report.v1 import report_pb2, report_pb2_grpc

    stub = report_pb2_grpc.ReportStub(conn.channel)
    describe = await stub.Describe(report_pb2.DescribeRequest())
    return RemoteReport(conn, metadata_from_describe(describe)), conn


class TestArrowReassembly:
    def test_round_trips_a_dataframe(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        assert arrow_batches_to_dataframe([dataframe_to_arrow_batch(df)], ["a", "b"]).equals(df)

    def test_concatenates_multiple_batches_in_order(self):
        first = dataframe_to_arrow_batch(pd.DataFrame({"a": [1, 2]}))
        second = dataframe_to_arrow_batch(pd.DataFrame({"a": [3]}))
        assert list(arrow_batches_to_dataframe([first, second], ["a"])["a"]) == [1, 2, 3]

    def test_empty_result_keeps_declared_columns(self):
        # The in-process contract guarantees this and the exporters rely on it.
        out = arrow_batches_to_dataframe([], ["phase", "delay_seconds"])
        assert out.empty
        assert list(out.columns) == ["phase", "delay_seconds"]


class TestRemoteReportIsAReport:
    @pytest.mark.asyncio
    async def test_satisfies_the_in_process_interface(self):
        report, conn = await _remote()
        try:
            assert isinstance(report, Report)
            assert report.metadata.name == "fake-delay"
            assert report.metadata.category == "standard"
            assert report.metadata.estimated_time == "fast"
            assert report.metadata.export_formats == ["csv", "json"]
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_registers_alongside_in_process_reports(self):
        report, conn = await _remote()
        try:
            ReportRegistry.register_grpc("fake-delay", conn)
            assert ReportRegistry.is_remote("fake-delay")
            # In-process reports are untouched.
            assert len(ReportRegistry.list_all()) > 0
        finally:
            ReportRegistry.unregister_grpc("fake-delay")
            await conn.shutdown()


class TestGenerate:
    @pytest.mark.asyncio
    async def test_returns_the_rows_the_plugin_streamed(self):
        report, conn = await _remote()
        try:
            frame = await report.execute({"signal_id": "SIG-001"}, session=None)
            assert list(frame.columns) == ["phase", "delay_seconds"]
            assert len(frame) == 3                       # 2 batches + 1
            assert list(frame["phase"]) == [2, 4, 6]
            assert frame["delay_seconds"].tolist() == [12.5, 7.25, 3.0]
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_empty_report_yields_an_empty_frame_with_columns(self):
        report, conn = await _remote("--empty")
        try:
            frame = await report.execute({}, session=None)
            assert frame.empty
            assert list(frame.columns) == ["phase", "delay_seconds"]
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_preferred_http_status_crosses_the_wire(self):
        report, conn = await _remote("--status", "422")
        try:
            await report.execute({}, session=None)
            assert report.preferred_http_status_for_run() == 422
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_default_status_is_none_not_zero(self):
        report, conn = await _remote()
        try:
            await report.execute({}, session=None)
            assert report.preferred_http_status_for_run() is None
        finally:
            await conn.shutdown()


class TestMalformedStreams:
    """A misbehaving plugin must fail loudly, not silently produce wrong data."""

    @pytest.mark.asyncio
    async def test_batch_before_viewmodel_is_rejected(self):
        report, conn = await _remote("--no-viewmodel")
        try:
            with pytest.raises(RemoteReportError, match="before the ViewModel"):
                await report.execute({}, session=None)
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_two_viewmodels_are_rejected(self):
        report, conn = await _remote("--two-viewmodels")
        try:
            with pytest.raises(RemoteReportError, match="more than one ViewModel"):
                await report.execute({}, session=None)
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_executing_a_disconnected_plugin_is_an_error(self):
        report, conn = await _remote()
        await conn.shutdown()
        with pytest.raises(RemoteReportError, match="not connected"):
            await report.execute({}, session=None)
