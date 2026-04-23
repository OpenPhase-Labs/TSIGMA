"""
Unit tests for TSIGMA GraphQL schema and types.

Tests schema structure, type definitions, and resolver logic
with mocked database sessions.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from tsigma.api.graphql.schema import (
    Query,
    _approach_to_type,
    _decimal_to_float,
    _detector_to_type,
    _signal_to_type,
    _uuid_to_str,
    schema,
)
from tsigma.api.graphql.types import (
    ApproachType,
    CorridorType,
    DetectorType,
    JurisdictionType,
    RegionType,
    SignalType,
)

# ---------------------------------------------------------------------------
# Schema structure
# ---------------------------------------------------------------------------


class TestSchemaStructure:
    """Tests that the Strawberry schema is well-formed."""

    def test_schema_has_query_type(self):
        """The schema must have a query type."""
        assert schema.query is not None

    def test_schema_types_defined(self):
        """All key Strawberry types should be registered in the schema."""
        type_map = schema._schema.type_map
        expected = {
            "SignalType",
            "ApproachType",
            "DetectorType",
            "EventType",
            "RegionType",
            "JurisdictionType",
            "CorridorType",
            "ReportInfoType",
            "ReportResultType",
        }
        for name in expected:
            assert name in type_map, f"{name} not found in schema type map"

    def test_query_has_expected_fields(self):
        """The Query type should expose the expected field names."""
        query_type = schema._schema.query_type
        field_names = set(query_type.fields.keys())
        expected_fields = {
            "signals",
            "signal",
            "regions",
            "jurisdictions",
            "corridors",
            "events",
            "availableReports",
            "runReport",
        }
        for name in expected_fields:
            assert name in field_names, f"Query field '{name}' not found"


# ---------------------------------------------------------------------------
# Converter helpers
# ---------------------------------------------------------------------------

class TestDecimalToFloat:
    def test_converts_decimal(self):
        from decimal import Decimal
        result = _decimal_to_float(Decimal("33.7490"))
        assert abs(result - 33.749) < 1e-9

    def test_none_returns_none(self):
        assert _decimal_to_float(None) is None


class TestUuidToStr:
    def test_converts_uuid(self):
        from uuid import UUID
        u = UUID("12345678-1234-5678-1234-567812345678")
        assert _uuid_to_str(u) == "12345678-1234-5678-1234-567812345678"

    def test_none_returns_none(self):
        assert _uuid_to_str(None) is None


class TestDetectorToType:
    def test_conversion(self):
        det = MagicMock(
            detector_id="d-1",
            approach_id="a-1",
            detector_channel=3,
            distance_from_stop_bar=400,
            min_speed_filter=10,
            decision_point=5,
            movement_delay=2,
            lane_number=1,
        )
        result = _detector_to_type(det)
        assert isinstance(result, DetectorType)
        assert result.detector_id == "d-1"
        assert result.detector_channel == 3


class TestApproachToType:
    def test_conversion(self):
        app = MagicMock(
            approach_id="a-1",
            signal_id="sig-1",
            direction_type_id=1,
            description="NB Thru",
            mph=35,
            protected_phase_number=2,
            is_protected_phase_overlap=False,
            permissive_phase_number=None,
            is_permissive_phase_overlap=False,
            ped_phase_number=8,
        )
        result = _approach_to_type(app)
        assert isinstance(result, ApproachType)
        assert result.signal_id == "sig-1"
        assert result.detectors == []

    def test_with_detectors(self):
        app = MagicMock(
            approach_id="a-1", signal_id="sig-1", direction_type_id=1,
            description=None, mph=None, protected_phase_number=2,
            is_protected_phase_overlap=False, permissive_phase_number=None,
            is_permissive_phase_overlap=False, ped_phase_number=None,
        )
        det = DetectorType(
            detector_id="d-1", approach_id="a-1", detector_channel=3,
            distance_from_stop_bar=None, min_speed_filter=None,
            decision_point=None, movement_delay=None, lane_number=None,
        )
        result = _approach_to_type(app, [det])
        assert len(result.detectors) == 1


class TestSignalToType:
    def test_conversion(self):
        from decimal import Decimal
        sig = MagicMock(
            signal_id="sig-1",
            primary_street="Peachtree St",
            secondary_street="10th St",
            latitude=Decimal("33.7810"),
            longitude=Decimal("-84.3830"),
            enabled=True,
            note=None,
        )
        result = _signal_to_type(sig)
        assert isinstance(result, SignalType)
        assert result.signal_id == "sig-1"
        assert result.approaches == []


# ---------------------------------------------------------------------------
# Resolver tests (mocked DB)
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


def _make_info(session):
    """Build a minimal strawberry Info mock with session in context."""
    info = MagicMock()
    info.context = {"session": session}
    return info


class TestSignalsResolver:
    """Tests for Query.signals resolver."""

    def test_returns_signals(self):
        sig = MagicMock(
            signal_id="sig-1", primary_street="Main St",
            secondary_street="1st Ave", latitude=None, longitude=None,
            enabled=True, note=None,
        )
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [sig]
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock

        session = AsyncMock()
        session.execute.return_value = result_mock
        info = _make_info(session)

        query = Query()
        result = _run(query.signals(info))

        assert len(result) == 1
        assert result[0].signal_id == "sig-1"

    def test_returns_empty_list(self):
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = []
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock

        session = AsyncMock()
        session.execute.return_value = result_mock
        info = _make_info(session)

        query = Query()
        result = _run(query.signals(info))
        assert result == []


class TestSignalResolver:
    """Tests for Query.signal resolver."""

    def test_signal_not_found(self):
        session = AsyncMock()
        session.get.return_value = None
        info = _make_info(session)

        query = Query()
        result = _run(query.signal(info, signal_id="nonexistent"))
        assert result is None


class TestRegionsResolver:
    def test_returns_regions(self):
        from uuid import uuid4
        region = MagicMock(
            region_id=uuid4(), description="Region 1", parent_region_id=None,
        )
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [region]
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock

        session = AsyncMock()
        session.execute.return_value = result_mock
        info = _make_info(session)

        query = Query()
        result = _run(query.regions(info))
        assert len(result) == 1
        assert isinstance(result[0], RegionType)


class TestAvailableReportsResolver:
    @patch("tsigma.api.graphql.schema.ReportRegistry")
    def test_returns_report_list(self, mock_registry):
        mock_cls = MagicMock()
        mock_cls.description = "Test report"
        mock_cls.category = "volume"
        mock_cls.estimated_time = "30s"
        mock_cls.export_formats = ["csv", "json"]
        mock_registry.list_all.return_value = {"test_report": mock_cls}

        info = _make_info(AsyncMock())
        query = Query()
        result = _run(query.available_reports(info))

        assert len(result) == 1
        assert result[0].name == "test_report"
        assert result[0].category == "volume"


class TestRunReportResolver:
    @patch("tsigma.api.graphql.schema.ReportRegistry")
    def test_unknown_report(self, mock_registry):
        mock_registry.get.side_effect = ValueError("Unknown")

        session = AsyncMock()
        info = _make_info(session)
        query = Query()
        result = _run(query.run_report(info, report_name="bad", params={}))

        assert result.status == "error"
        assert "Unknown report" in result.data["error"]

    @patch("tsigma.api.graphql.schema.ReportRegistry")
    def test_successful_report(self, mock_registry):
        mock_report_instance = AsyncMock()
        mock_report_instance.execute.return_value = {"rows": 42}
        mock_cls = MagicMock(return_value=mock_report_instance)
        mock_registry.get.return_value = mock_cls

        session = AsyncMock()
        info = _make_info(session)
        query = Query()
        result = _run(query.run_report(info, report_name="vol", params={"signal_id": "s1"}))

        assert result.status == "ok"
        assert result.data == {"rows": 42}

    @patch("tsigma.api.graphql.schema.ReportRegistry")
    def test_report_execution_error(self, mock_registry):
        mock_report_instance = AsyncMock()
        mock_report_instance.execute.side_effect = RuntimeError("boom")
        mock_cls = MagicMock(return_value=mock_report_instance)
        mock_registry.get.return_value = mock_cls

        session = AsyncMock()
        info = _make_info(session)
        query = Query()
        result = _run(query.run_report(info, report_name="vol", params={}))

        assert result.status == "error"
        assert "failed" in result.data["error"]


# ---------------------------------------------------------------------------
# Additional resolver coverage (uncovered lines)
# ---------------------------------------------------------------------------


class TestSignalResolverWithData:
    """Tests for Query.signal resolver when a signal IS found."""

    def test_signal_found_with_approaches_and_detectors(self):
        from uuid import UUID

        sig = MagicMock(
            signal_id="sig-1", primary_street="Peachtree", secondary_street="10th",
            latitude=None, longitude=None, enabled=True, note=None,
        )
        app_mock = MagicMock(
            approach_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            signal_id="sig-1", direction_type_id=1, description="NB",
            mph=35, protected_phase_number=2, is_protected_phase_overlap=False,
            permissive_phase_number=None, is_permissive_phase_overlap=False,
            ped_phase_number=None,
        )
        det_mock = MagicMock(
            detector_id="d-1",
            approach_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            detector_channel=3, distance_from_stop_bar=400,
            min_speed_filter=10, decision_point=5, movement_delay=2,
            lane_number=1,
        )

        session = AsyncMock()
        session.get = AsyncMock(return_value=sig)

        # First execute call: approaches
        app_scalars = MagicMock()
        app_scalars.all.return_value = [app_mock]
        app_result = MagicMock()
        app_result.scalars.return_value = app_scalars

        # Second execute call: detectors
        det_scalars = MagicMock()
        det_scalars.all.return_value = [det_mock]
        det_result = MagicMock()
        det_result.scalars.return_value = det_scalars

        session.execute = AsyncMock(side_effect=[app_result, det_result])
        info = _make_info(session)

        query = Query()
        result = _run(query.signal(info, signal_id="sig-1"))

        assert result is not None
        assert result.signal_id == "sig-1"
        assert len(result.approaches) == 1
        assert len(result.approaches[0].detectors) == 1

    def test_signal_found_no_approaches(self):
        sig = MagicMock(
            signal_id="sig-2", primary_street="Main", secondary_street=None,
            latitude=None, longitude=None, enabled=True, note=None,
        )
        session = AsyncMock()
        session.get = AsyncMock(return_value=sig)

        app_scalars = MagicMock()
        app_scalars.all.return_value = []
        app_result = MagicMock()
        app_result.scalars.return_value = app_scalars
        session.execute = AsyncMock(return_value=app_result)

        info = _make_info(session)
        query = Query()
        result = _run(query.signal(info, signal_id="sig-2"))

        assert result is not None
        assert result.approaches == []


class TestJurisdictionsResolver:
    """Tests for Query.jurisdictions resolver."""

    def test_returns_jurisdictions(self):
        from uuid import uuid4

        j = MagicMock(
            jurisdiction_id=uuid4(),
            mpo_name="Atlanta", county_name="Fulton",
        )
        j.name = "Fulton County"
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [j]
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock

        session = AsyncMock()
        session.execute.return_value = result_mock
        info = _make_info(session)

        query = Query()
        result = _run(query.jurisdictions(info))

        assert len(result) == 1
        assert isinstance(result[0], JurisdictionType)
        assert result[0].name == "Fulton County"

    def test_returns_empty(self):
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = []
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock

        session = AsyncMock()
        session.execute.return_value = result_mock
        info = _make_info(session)

        query = Query()
        result = _run(query.jurisdictions(info))
        assert result == []


class TestCorridorsResolver:
    """Tests for Query.corridors resolver."""

    def test_returns_corridors(self):
        from uuid import uuid4

        c = MagicMock(
            corridor_id=uuid4(),
            description="North-south arterial",
        )
        c.name = "Peachtree Corridor"
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [c]
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock

        session = AsyncMock()
        session.execute.return_value = result_mock
        info = _make_info(session)

        query = Query()
        result = _run(query.corridors(info))

        assert len(result) == 1
        assert isinstance(result[0], CorridorType)
        assert result[0].name == "Peachtree Corridor"


class TestEventsResolver:
    """Tests for Query.events resolver."""

    def test_returns_events(self):
        from datetime import datetime

        evt = MagicMock(
            signal_id="sig-1",
            event_time=datetime(2025, 6, 15, 8, 0, 0),
            event_code=1,
            event_param=2,
        )
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [evt]
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock

        session = AsyncMock()
        session.execute.return_value = result_mock
        info = _make_info(session)

        query = Query()
        result = _run(query.events(
            info, signal_id="sig-1",
            start=datetime(2025, 6, 15, 7, 0, 0),
            end=datetime(2025, 6, 15, 9, 0, 0),
        ))

        assert len(result) == 1
        assert result[0].event_code == 1

    def test_with_event_code_filter(self):
        from datetime import datetime

        scalars_mock = MagicMock()
        scalars_mock.all.return_value = []
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock

        session = AsyncMock()
        session.execute.return_value = result_mock
        info = _make_info(session)

        query = Query()
        result = _run(query.events(
            info, signal_id="sig-1",
            start=datetime(2025, 6, 15, 7, 0, 0),
            end=datetime(2025, 6, 15, 9, 0, 0),
            event_codes=[1, 8],
        ))
        assert result == []
