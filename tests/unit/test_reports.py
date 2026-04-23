"""
Unit tests for the TSIGMA report registry and report plugin structure.

Tests registration, metadata, and structural requirements without
executing reports (which need real database sessions).
"""

import inspect

import pytest

from tsigma.reports import ReportRegistry

# Import all report classes for targeted tests
from tsigma.reports.approach_delay import ApproachDelayReport
from tsigma.reports.approach_speed import ApproachSpeedReport
from tsigma.reports.approach_volume import ApproachVolumeReport
from tsigma.reports.arrivals_on_green import ArrivalsOnGreenReport
from tsigma.reports.bike_volume import BikeVolumeReport
from tsigma.reports.green_time_utilization import GreenTimeUtilizationReport
from tsigma.reports.left_turn_gap import LeftTurnGapReport
from tsigma.reports.link_pivot import LinkPivotReport
from tsigma.reports.ped_delay import PedDelayReport
from tsigma.reports.phase_termination import PhaseTerminationReport
from tsigma.reports.preemption import PreemptionReport
from tsigma.reports.purdue_diagram import PurdueDiagramReport
from tsigma.reports.ramp_metering import RampMeteringReport
from tsigma.reports.red_light_monitor import RedLightMonitorReport
from tsigma.reports.registry import BaseReport
from tsigma.reports.split_failure import SplitFailureReport
from tsigma.reports.split_monitor import SplitMonitorReport
from tsigma.reports.time_space_diagram import TimeSpaceDiagramReport
from tsigma.reports.timing_and_actuations import TimingAndActuationsReport
from tsigma.reports.transit_signal_priority import TransitSignalPriorityReport
from tsigma.reports.turning_movement_counts import TurningMovementCountsReport
from tsigma.reports.wait_time import WaitTimeReport
from tsigma.reports.yellow_red_actuations import YellowRedActuationsReport

# ── Registry tests ──────────────────────────────────────────────────


class TestRegistryListAll:
    """Tests for ReportRegistry.list_all()."""

    def test_registry_list_all(self):
        """list_all() returns a dict of all 22 registered reports."""
        reports = ReportRegistry.list_all()
        assert isinstance(reports, dict)
        expected_names = {
            "approach-delay",
            "approach-speed",
            "approach-volume",
            "arrivals-on-green",
            "bike-volume",
            "green-time-utilization",
            "left-turn-gap",
            "link-pivot",
            "ped-delay",
            "phase-termination",
            "preemption",
            "purdue-diagram",
            "ramp-metering",
            "red-light-monitor",
            "split-failure",
            "split-monitor",
            "time-space-diagram",
            "timing-and-actuations",
            "transit-signal-priority",
            "turning-movement-counts",
            "wait-time",
            "yellow-red-actuations",
        }
        assert expected_names.issubset(set(reports.keys()))
        assert len(reports) >= 22

    def test_registry_list_all_returns_copy(self):
        """list_all() returns a copy, not the internal dict."""
        a = ReportRegistry.list_all()
        b = ReportRegistry.list_all()
        assert a is not b

    def test_registry_list_all_values_are_classes(self):
        """All values in list_all() are subclasses of BaseReport."""
        for name, cls in ReportRegistry.list_all().items():
            assert isinstance(cls, type), f"{name} value is not a class"
            assert issubclass(cls, BaseReport), f"{name} is not a BaseReport subclass"


class TestRegistryGet:
    """Tests for ReportRegistry.get()."""

    def test_registry_get(self):
        """get() returns the correct class for a known report."""
        cls = ReportRegistry.get("approach-delay")
        assert cls is ApproachDelayReport

    def test_registry_get_split_monitor(self):
        """get() returns SplitMonitorReport for 'split-monitor'."""
        cls = ReportRegistry.get("split-monitor")
        assert cls is SplitMonitorReport

    def test_registry_get_purdue_diagram(self):
        """get() returns PurdueDiagramReport for 'purdue-diagram'."""
        cls = ReportRegistry.get("purdue-diagram")
        assert cls is PurdueDiagramReport

    def test_registry_get_phase_termination(self):
        """get() returns PhaseTerminationReport for 'phase-termination'."""
        cls = ReportRegistry.get("phase-termination")
        assert cls is PhaseTerminationReport

    def test_registry_get_unknown_raises(self):
        """get() raises ValueError for an unknown report name."""
        with pytest.raises(ValueError, match="Unknown report"):
            ReportRegistry.get("nonexistent-report-xyz")


class TestAllReportsHaveRequiredAttrs:
    """Verify every registered report has the required metadata fields."""

    REQUIRED_METADATA_FIELDS = ("name", "description", "category", "estimated_time")

    def test_all_reports_have_required_attrs(self):
        """Every registered report must have metadata with name, description, category, estimated_time."""
        reports = ReportRegistry.list_all()
        assert len(reports) > 0, "No reports registered"

        for reg_name, cls in reports.items():
            assert hasattr(cls, "metadata"), (
                f"Report '{reg_name}' ({cls.__name__}) missing 'metadata' attribute"
            )
            meta = cls.metadata
            for field in self.REQUIRED_METADATA_FIELDS:
                assert hasattr(meta, field), (
                    f"Report '{reg_name}' ({cls.__name__}) metadata missing field '{field}'"
                )
                value = getattr(meta, field)
                assert value is not None, (
                    f"Report '{reg_name}' ({cls.__name__}) has None for metadata.{field}"
                )


# ── Report structure tests ──────────────────────────────────────────

REPORT_CASES = [
    ("approach-delay", ApproachDelayReport),
    ("approach-speed", ApproachSpeedReport),
    ("approach-volume", ApproachVolumeReport),
    ("arrivals-on-green", ArrivalsOnGreenReport),
    ("bike-volume", BikeVolumeReport),
    ("green-time-utilization", GreenTimeUtilizationReport),
    ("left-turn-gap", LeftTurnGapReport),
    ("link-pivot", LinkPivotReport),
    ("ped-delay", PedDelayReport),
    ("phase-termination", PhaseTerminationReport),
    ("preemption", PreemptionReport),
    ("purdue-diagram", PurdueDiagramReport),
    ("ramp-metering", RampMeteringReport),
    ("red-light-monitor", RedLightMonitorReport),
    ("split-failure", SplitFailureReport),
    ("split-monitor", SplitMonitorReport),
    ("time-space-diagram", TimeSpaceDiagramReport),
    ("timing-and-actuations", TimingAndActuationsReport),
    ("transit-signal-priority", TransitSignalPriorityReport),
    ("turning-movement-counts", TurningMovementCountsReport),
    ("wait-time", WaitTimeReport),
    ("yellow-red-actuations", YellowRedActuationsReport),
]


class TestReportHasExecuteMethod:
    """Verify each report has an async execute method."""

    @pytest.mark.parametrize("reg_name,cls", REPORT_CASES, ids=[c[0] for c in REPORT_CASES])
    def test_report_has_execute_method(self, reg_name, cls):
        """Report class has an 'execute' method."""
        assert hasattr(cls, "execute"), f"{cls.__name__} missing execute method"
        assert callable(getattr(cls, "execute")), f"{cls.__name__}.execute is not callable"

    @pytest.mark.parametrize("reg_name,cls", REPORT_CASES, ids=[c[0] for c in REPORT_CASES])
    def test_report_execute_is_async(self, reg_name, cls):
        """Report execute method is a coroutine function (async)."""
        execute = getattr(cls, "execute")
        assert inspect.iscoroutinefunction(execute), (
            f"{cls.__name__}.execute is not async"
        )


class TestReportMetadata:
    """Verify metadata fields are non-empty strings for each report."""

    @pytest.mark.parametrize("reg_name,cls", REPORT_CASES, ids=[c[0] for c in REPORT_CASES])
    def test_report_name_is_nonempty_string(self, reg_name, cls):
        """Report.metadata.name is a non-empty string."""
        assert isinstance(cls.metadata.name, str) and len(cls.metadata.name) > 0, (
            f"{cls.__name__}.metadata.name must be a non-empty string, got {cls.metadata.name!r}"
        )

    @pytest.mark.parametrize("reg_name,cls", REPORT_CASES, ids=[c[0] for c in REPORT_CASES])
    def test_report_description_is_nonempty_string(self, reg_name, cls):
        """Report.metadata.description is a non-empty string."""
        assert isinstance(cls.metadata.description, str) and len(cls.metadata.description) > 0, (
            f"{cls.__name__}.metadata.description must be a non-empty string, got {cls.metadata.description!r}"
        )

    @pytest.mark.parametrize("reg_name,cls", REPORT_CASES, ids=[c[0] for c in REPORT_CASES])
    def test_report_category_is_nonempty_string(self, reg_name, cls):
        """Report.metadata.category is a non-empty string."""
        assert isinstance(cls.metadata.category, str) and len(cls.metadata.category) > 0, (
            f"{cls.__name__}.metadata.category must be a non-empty string, got {cls.metadata.category!r}"
        )

    @pytest.mark.parametrize("reg_name,cls", REPORT_CASES, ids=[c[0] for c in REPORT_CASES])
    def test_report_category_is_valid(self, reg_name, cls):
        """Report.metadata.category is one of the allowed values."""
        valid_categories = {"dashboard", "standard", "detailed"}
        assert cls.metadata.category in valid_categories, (
            f"{cls.__name__}.metadata.category '{cls.metadata.category}' not in {valid_categories}"
        )

    @pytest.mark.parametrize("reg_name,cls", REPORT_CASES, ids=[c[0] for c in REPORT_CASES])
    def test_report_estimated_time_is_valid(self, reg_name, cls):
        """Report.metadata.estimated_time is one of the allowed values."""
        valid_times = {"fast", "medium", "slow"}
        assert cls.metadata.estimated_time in valid_times, (
            f"{cls.__name__}.metadata.estimated_time '{cls.metadata.estimated_time}' not in {valid_times}"
        )

    @pytest.mark.parametrize("reg_name,cls", REPORT_CASES, ids=[c[0] for c in REPORT_CASES])
    def test_report_name_matches_registration(self, reg_name, cls):
        """Report.metadata.name matches the registry key."""
        assert cls.metadata.name == reg_name, (
            f"{cls.__name__}.metadata.name '{cls.metadata.name}' does not match registry key '{reg_name}'"
        )
