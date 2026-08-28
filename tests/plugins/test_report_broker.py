"""Phase 5 gate: host-served report broker services.

The security-critical part is `compile_predicate` - the ONLY path from plugin
input to SQL. Raw SQL never crosses to a plugin (contract ruling 3a), so these
tests are as much a security boundary as a correctness one.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from tsigma.plugins.remote_report import arrow_batches_to_dataframe
from tsigma.plugins.report_broker import (
    PREDICATE_COLUMNS,
    AggregateQueryService,
    ConfigService,
    CycleAggregateService,
    EventQueryService,
    PredicateError,
    compile_agg_spec,
    compile_predicate,
    frame_to_batches,
    to_channel_approach_map,
    to_int_int_map,
    to_int_set,
    to_signal_plan_list,
)
from tsigma.report.v1 import report_pb2 as r


def _ts(dt: datetime) -> Timestamp:
    t = Timestamp()
    t.FromDatetime(dt)
    return t


START = datetime(2026, 8, 1, tzinfo=timezone.utc)
END = datetime(2026, 8, 2, tzinfo=timezone.utc)


class TestCompilePredicate:
    @pytest.mark.parametrize(
        "comparator,values,expected",
        [
            ("eq", [82], "event_code = 82"),
            ("gt", [80], "event_code > 80"),
            ("lt", [80], "event_code < 80"),
            ("ge", [80], "event_code >= 80"),
            ("le", [80], "event_code <= 80"),
            ("in", [1, 8, 82], "event_code IN (1, 8, 82)"),
        ],
    )
    def test_compiles_each_supported_comparator(self, comparator, values, expected):
        p = r.AggPredicate(column="event_code", comparator=comparator, values=values)
        assert compile_predicate(p) == expected

    def test_output_matches_what_the_app_passes_today(self):
        # health.py:209 passes ("count_if", "event_code = 82") in-process.
        p = r.AggPredicate(column="event_code", comparator="eq", values=[82])
        assert compile_predicate(p) == "event_code = 82"

    @pytest.mark.parametrize(
        "predicate,reason",
        [
            (r.AggPredicate(column="signal_id", comparator="eq", values=[1]), "column"),
            (r.AggPredicate(column="", comparator="eq", values=[1]), "empty column"),
            (r.AggPredicate(column="event_code", comparator="like", values=[1]), "comparator"),
            (r.AggPredicate(column="event_code", comparator="", values=[1]), "empty comparator"),
            (r.AggPredicate(column="event_code", comparator="eq", values=[]), "no values"),
            (r.AggPredicate(column="event_code", comparator="eq", values=[1, 2]), "eq arity"),
        ],
    )
    def test_refuses_anything_outside_the_allowlist(self, predicate, reason):
        with pytest.raises(PredicateError):
            compile_predicate(predicate)

    def test_column_allowlist_is_narrow(self):
        # A widened allowlist should be a deliberate, reviewed change.
        assert PREDICATE_COLUMNS == {"event_code", "event_param"}

    def test_no_plugin_string_can_reach_the_sql(self):
        """The column is the only string, and it must match the allowlist exactly."""
        attack = r.AggPredicate(
            column="event_code; DROP TABLE controller_event_log --",
            comparator="eq",
            values=[1],
        )
        with pytest.raises(PredicateError):
            compile_predicate(attack)


class TestCompileAggSpec:
    def test_count(self):
        assert compile_agg_spec(r.AggSpec(op="count")) == ("count",)

    def test_field_ops(self):
        assert compile_agg_spec(r.AggSpec(op="max", field="event_time")) == ("max", "event_time")

    def test_count_if_matches_the_in_process_tuple(self):
        spec = r.AggSpec(
            op="count_if",
            filter=r.AggPredicate(column="event_code", comparator="eq", values=[82]),
        )
        assert compile_agg_spec(spec) == ("count_if", "event_code = 82")

    def test_max_if_matches_the_in_process_tuple(self):
        spec = r.AggSpec(
            op="max_if",
            field="event_time",
            filter=r.AggPredicate(column="event_code", comparator="eq", values=[81]),
        )
        assert compile_agg_spec(spec) == ("max_if", "event_time", "event_code = 81")

    def test_conditional_op_without_a_filter_is_refused(self):
        with pytest.raises(PredicateError, match="requires a filter"):
            compile_agg_spec(r.AggSpec(op="count_if"))

    def test_field_op_without_a_field_is_refused(self):
        with pytest.raises(PredicateError, match="requires a field"):
            compile_agg_spec(r.AggSpec(op="sum"))

    def test_unknown_op_is_refused(self):
        with pytest.raises(PredicateError, match="unsupported aggregate op"):
            compile_agg_spec(r.AggSpec(op="median", field="x"))


class TestBatching:
    def test_large_frame_is_split_into_bounded_batches(self):
        frame = pd.DataFrame({"a": range(25)})
        batches = list(frame_to_batches(frame, batch_rows=10))
        assert len(batches) == 3
        assert list(arrow_batches_to_dataframe(batches, ["a"])["a"]) == list(range(25))

    def test_empty_frame_still_yields_one_batch_with_schema(self):
        batches = list(frame_to_batches(pd.DataFrame({"a": pd.Series(dtype="int64")})))
        assert len(batches) == 1
        assert arrow_batches_to_dataframe(batches, ["a"]).empty


class TestEventQueryService:
    @pytest.mark.asyncio
    async def test_forwards_in_list_mode_to_the_sdk(self):
        request = r.FetchEventsRequest(
            signal_id="SIG-001", start=_ts(START), end=_ts(END),
            event_codes=[1, 8], event_param_in=[2],
        )
        with patch("tsigma.reports.sdk.queries.fetch_events", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame({"event_code": [1]})
            await EventQueryService().fetch_events(request)
        args, kwargs = m.call_args
        assert args[0] == "SIG-001"
        assert args[3] == [1, 8]
        assert kwargs["event_param_in"] == [2]

    @pytest.mark.asyncio
    async def test_no_filter_mode_passes_none_not_empty_list(self):
        request = r.FetchEventsRequest(signal_id="S", start=_ts(START), end=_ts(END))
        with patch("tsigma.reports.sdk.queries.fetch_events", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame()
            await EventQueryService().fetch_events(request)
        assert m.call_args[0][3] is None

    @pytest.mark.asyncio
    async def test_split_covers_the_or_fragment_case(self):
        """preemption.py's raw fragment maps onto the parameterized split."""
        request = r.FetchEventsSplitRequest(
            signal_id="S", start=_ts(START), end=_ts(END),
            phase_codes=[104], det_codes=[1], det_channels=[2],
        )
        with patch("tsigma.reports.sdk.queries.fetch_events_split", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame()
            await EventQueryService().fetch_events_split(request)
        kwargs = m.call_args[1]
        assert kwargs["phase_codes"] == [104]
        assert kwargs["det_codes"] == [1]
        assert kwargs["det_channels"] == [2]


class TestAggregateQueryService:
    @pytest.mark.asyncio
    async def test_all_signals_selector(self):
        request = r.AggregateEventsRequest(
            signals=r.SignalSelector(all_signals=True), start=_ts(START), end=_ts(END),
            agg=[r.AggSpec(op="count")],
        )
        with patch("tsigma.reports.sdk.aggregates.aggregate_events", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame()
            await AggregateQueryService().aggregate_events(request)
        assert m.call_args[0][0] == "All"

    @pytest.mark.asyncio
    async def test_specific_signals_and_compiled_aggs(self):
        request = r.AggregateEventsRequest(
            signals=r.SignalSelector(signal_ids=["A", "B"]), start=_ts(START), end=_ts(END),
            agg=[
                r.AggSpec(op="count_if",
                          filter=r.AggPredicate(column="event_code", comparator="eq", values=[82])),
            ],
            group_by=["event_param"],
            filters={"event_code": r.IntList(values=[1, 8])},
        )
        with patch("tsigma.reports.sdk.aggregates.aggregate_events", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame()
            await AggregateQueryService().aggregate_events(request)
        args, kwargs = m.call_args
        assert args[0] == ["A", "B"]
        assert kwargs["agg"] == [("count_if", "event_code = 82")]
        assert kwargs["group_by"] == ["event_param"]
        assert kwargs["filters"] == {"event_code": [1, 8]}

    @pytest.mark.asyncio
    async def test_a_bad_predicate_never_reaches_the_sdk(self):
        request = r.AggregateEventsRequest(
            signals=r.SignalSelector(signal_ids=["A"]), start=_ts(START), end=_ts(END),
            agg=[r.AggSpec(op="count_if",
                           filter=r.AggPredicate(column="pwned", comparator="eq", values=[1]))],
        )
        with patch("tsigma.reports.sdk.aggregates.aggregate_events", new_callable=AsyncMock) as m:
            with pytest.raises(PredicateError):
                await AggregateQueryService().aggregate_events(request)
        m.assert_not_awaited()


class TestCycleAggregateService:
    """cycles.py helpers open their own sessions - no scoped session threaded."""

    @pytest.mark.asyncio
    async def test_boundaries_forwarded(self):
        req = r.CycleBoundariesRequest(signal_id="S", phase=2, start=_ts(START), end=_ts(END))
        with patch("tsigma.reports.sdk.cycles.fetch_cycle_boundaries", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame()
            await CycleAggregateService().fetch_cycle_boundaries(req)
        assert m.call_args[0][:2] == ("S", 2)

    @pytest.mark.asyncio
    async def test_arrivals_empty_channels_means_all(self):
        req = r.CycleArrivalsRequest(signal_id="S", phase=2, start=_ts(START), end=_ts(END))
        with patch("tsigma.reports.sdk.cycles.fetch_cycle_arrivals", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame()
            await CycleAggregateService().fetch_cycle_arrivals(req)
        assert m.call_args[0][4] is None

    @pytest.mark.asyncio
    async def test_arrivals_passes_specific_channels(self):
        req = r.CycleArrivalsRequest(
            signal_id="S", phase=2, start=_ts(START), end=_ts(END), detector_channels=[3, 4]
        )
        with patch("tsigma.reports.sdk.cycles.fetch_cycle_arrivals", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame()
            await CycleAggregateService().fetch_cycle_arrivals(req)
        assert m.call_args[0][4] == [3, 4]

    @pytest.mark.asyncio
    async def test_summary_forwarded(self):
        req = r.CycleSummaryRequest(signal_id="S", phase=6, start=_ts(START), end=_ts(END))
        with patch("tsigma.reports.sdk.cycles.fetch_cycle_summary", new_callable=AsyncMock) as m:
            m.return_value = pd.DataFrame()
            await CycleAggregateService().fetch_cycle_summary(req)
        assert m.call_args[0][1] == 6


def _session_factory():
    made = []

    def factory():
        session = AsyncMock()
        session.__aenter__.return_value = session
        session.__aexit__.return_value = False
        made.append(session)
        return session

    return factory, made


class TestConfigService:
    """config.py / plans.py take a session, so each call is scoped per invocation."""

    @pytest.mark.asyncio
    async def test_channel_to_phase_runs_in_a_scoped_session(self):
        factory, made = _session_factory()
        req = r.ChannelMapRequest(signal_id="S", as_of=_ts(START))
        with patch("tsigma.reports.sdk.config.load_channel_to_phase", new_callable=AsyncMock) as m:
            m.return_value = {1: 2}
            out = await ConfigService(factory).load_channel_to_phase(req)
        assert out == {1: 2}
        assert m.call_args[0][0] is made[0]      # the scoped session, not a shared one
        made[0].commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_each_call_gets_its_own_session(self):
        factory, made = _session_factory()
        req = r.ChannelMapRequest(signal_id="S", as_of=_ts(START))
        with patch("tsigma.reports.sdk.config.load_channel_to_phase", new_callable=AsyncMock) as m:
            m.return_value = {}
            svc = ConfigService(factory)
            await svc.load_channel_to_phase(req)
            await svc.load_channel_to_phase(req)
        assert len(made) == 2
        assert made[0] is not made[1]

    @pytest.mark.asyncio
    async def test_channels_for_phase_passes_the_phase(self):
        factory, _ = _session_factory()
        req = r.ChannelsForPhaseRequest(signal_id="S", phase=4, as_of=_ts(START))
        with patch("tsigma.reports.sdk.config.load_channels_for_phase", new_callable=AsyncMock) as m:
            m.return_value = {7}
            await ConfigService(factory).load_channels_for_phase(req)
        assert m.call_args[0][2] == 4

    @pytest.mark.asyncio
    async def test_fetch_plans_runs_scoped(self):
        factory, made = _session_factory()
        req = r.FetchPlansRequest(signal_id="S", start=_ts(START), end=_ts(END))
        with patch("tsigma.reports.sdk.plans.fetch_plans", new_callable=AsyncMock) as m:
            m.return_value = []
            await ConfigService(factory).fetch_plans(req)
        assert m.call_args[0][0] is made[0]


class TestConverters:
    def test_int_int_map(self):
        assert dict(to_int_int_map({1: 2, 3: 4}).mapping) == {1: 2, 3: 4}

    def test_int_set_is_ordered_for_a_deterministic_wire_form(self):
        assert list(to_int_set({5, 1, 3}).values) == [1, 3, 5]

    def test_channel_approach_map_carries_optional_distance(self):
        out = to_channel_approach_map(
            {
                1: {"approach_id": 10, "direction_type_id": 2, "distance_from_stop_bar": 300},
                2: {"approach_id": 11, "direction_type_id": 3, "distance_from_stop_bar": None},
            }
        )
        assert out.mapping[1].distance_from_stop_bar == 300
        assert out.mapping[2].HasField("distance_from_stop_bar") is False

    def test_signal_plan_list_carries_timestamps_and_splits(self):
        plan = SimpleNamespace(
            effective_from=START, effective_to=END, splits={"2": 30.5, "6": 22.0},
            plan_number=3, cycle_length=90, offset=12,
        )
        out = to_signal_plan_list([plan])
        assert len(out.plans) == 1
        assert dict(out.plans[0].splits) == {"2": 30.5, "6": 22.0}
        assert out.plans[0].HasField("effective_to")

    def test_open_ended_plan_has_no_effective_to(self):
        plan = SimpleNamespace(effective_from=START, effective_to=None, splits={})
        assert to_signal_plan_list([plan]).plans[0].HasField("effective_to") is False
