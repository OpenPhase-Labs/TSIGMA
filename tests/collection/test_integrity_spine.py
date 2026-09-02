"""Tests for the ingest seam (`tsigma.collection.targets`, `tsigma.plugins.method_broker`).

Covers R7b: `IngestionTarget.ingest` carries everything `ingest_raw` needs, each
target forwards its own `device_type` rather than letting the persist helper
infer one, a method that holds a checkpoint sends the backward-poison reference,
the out-of-process door forwards the same three header fields, and the
provenance/identity checks flag without blocking ingest.

Doubles are constrained to the interfaces the real objects expose: the target
double is `create_autospec(ControllerTarget, spec_set=True, instance=True)`, so a
call the real target would reject fails here too - `@runtime_checkable`
`isinstance` only proves the method is present, and both targets satisfied it
while the parameter was missing. The wire header is a real
`DecodeAndPersistHeader`, so proto presence semantics are the real ones, and the
decoder is `StubDecoder` from the sibling module rather than a second double.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, create_autospec, patch

import pytest

from tsigma.plugins.method_broker import (
    EventSinkService,
    device_type_of,
    stated_device_type_of,
)

from tests._helpers import make_mock_session_factory
from tests.collection.test_ingest_raw import StubDecoder, _event, _reviews, _utc
from tsigma.collection.decoders.base import DecoderRegistry, FileMetadata
from tsigma.collection.ingest import IngestOutcome, IngestResult
from tsigma.collection.methods.http_pull import HTTPPullMethod
from tsigma.collection.sdk import (
    DecodedEvent,
    SensorDetection,
    _element_type_for,
    _upsert_events,
)
from tsigma.collection.targets.controller import ControllerTarget
from tsigma.collection.targets.roadside import RoadsideTarget
from tsigma.method.v1 import method_pb2
from tsigma.models.file_provenance import FileIngestProvenance
from tsigma.notifications.suppression import CHECK_TEMPORAL_INTEGRITY

INGEST = "tsigma.collection.ingest"
BROKER = "tsigma.plugins.method_broker"

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decoding(events, *, inserted=2, metadata=None):
    """Patches that drive one in-process decode of `events` through to persist."""
    return (
        patch(f"{INGEST}.resolve_decoder_by_name", return_value=StubDecoder(events, metadata=metadata)),
        patch.object(DecoderRegistry, "is_remote", return_value=False),
        patch(f"{INGEST}.persist_events_with_drift_check", new=AsyncMock(return_value=inserted)),
    )


def _notifications(notify, alert_type: str) -> list[dict]:
    """The metadata dicts of the notifications raised for one alert type."""
    return [
        call.kwargs["metadata"] for call in notify.await_args_list
        if call.kwargs.get("metadata", {}).get("alert_type") == alert_type
    ]


def _provenance_rows(session) -> list[FileIngestProvenance]:
    """The FileIngestProvenance rows added through this session."""
    return [
        call.args[0] for call in session.add.call_args_list
        if call.args and isinstance(call.args[0], FileIngestProvenance)
    ]


def _no_suppressions(session) -> None:
    """Make the alert-suppression count query report 'nothing suppressed'."""
    result = MagicMock()
    result.scalar.return_value = 0
    result.first.return_value = None
    result.all.return_value = []
    session.execute.return_value = result


def _http_response(body: bytes):
    """A mock aiohttp response carrying `body` with a 200."""
    response = AsyncMock()
    response.status = 200
    response.read = AsyncMock(return_value=body)
    return response


def _http_session(body: bytes):
    """A mock `aiohttp.ClientSession` context manager answering every GET with `body`."""
    session = AsyncMock()
    session.get = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=_http_response(body)),
        __aexit__=AsyncMock(return_value=False),
    ))
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=False)
    return context


def _target_double(*, last_successful_poll=None):
    """A target double constrained to the real ControllerTarget interface."""
    double = create_autospec(ControllerTarget, spec_set=True, instance=True)
    double.device_type = "controller"
    checkpoint = MagicMock()
    checkpoint.last_successful_poll = last_successful_poll
    checkpoint.last_event_timestamp = None
    double.load_checkpoint.return_value = checkpoint
    return double


def _header(**fields) -> method_pb2.DecodeAndPersistHeader:
    """A real wire header, so proto presence semantics are the real ones."""
    poll = fields.pop("last_successful_poll", None)
    header = method_pb2.DecodeAndPersistHeader(**fields)
    if poll is not None:
        header.last_successful_poll.FromDatetime(poll)
    return header


# ---------------------------------------------------------------------------
# The seam - each target forwards its own device class and the poison reference
# ---------------------------------------------------------------------------


class TestSeamForwarding:
    """`IngestionTarget.ingest` carries everything `ingest_raw` needs."""

    @pytest.mark.asyncio
    async def test_controller_states_its_device_class_rather_than_leaving_it_inferred(self):
        """The controller target names itself, so persistence routes on a stated class."""
        session_factory, _ = make_mock_session_factory()

        with patch("tsigma.collection.targets.controller.ingest_raw", new=AsyncMock()) as spine:
            await ControllerTarget().ingest(b"raw", "SIG_001", session_factory)

        assert spine.await_args.kwargs["device_type"] == "controller"

    @pytest.mark.asyncio
    async def test_roadside_states_its_own_device_class(self):
        """The two ingest bodies must differ by device class; byte-identical is the bug."""
        session_factory, _ = make_mock_session_factory()

        with patch("tsigma.collection.targets.roadside.ingest_raw", new=AsyncMock()) as spine:
            await RoadsideTarget().ingest(b"raw", "SENSOR_001", session_factory)

        assert spine.await_args.kwargs["device_type"] == "sensor"

    @pytest.mark.asyncio
    async def test_an_explicit_device_type_overrides_the_targets_own(self):
        """A caller that states a class is obeyed; only silence falls back to the target."""
        session_factory, _ = make_mock_session_factory()

        with patch("tsigma.collection.targets.controller.ingest_raw", new=AsyncMock()) as spine:
            await ControllerTarget().ingest(
                b"raw", "SIG_001", session_factory, device_type="sensor",
            )

        assert spine.await_args.kwargs["device_type"] == "sensor"

    @pytest.mark.asyncio
    async def test_the_poison_reference_reaches_the_spine(self):
        """Without this the check fails OPEN: `is_backward_poisoned` returns False on None."""
        session_factory, _ = make_mock_session_factory()
        reference = _utc(2025, 6, 2, 12, 0)

        with patch("tsigma.collection.targets.controller.ingest_raw", new=AsyncMock()) as spine:
            await ControllerTarget().ingest(
                b"raw", "SIG_001", session_factory, last_successful_poll=reference,
            )

        assert spine.await_args.kwargs["last_successful_poll"] == reference


# ---------------------------------------------------------------------------
# Doubles are constrained to the real interface
# ---------------------------------------------------------------------------


class TestConstrainedDoubles:
    """A call the real target would reject must fail against the double too."""

    def test_a_constrained_double_rejects_an_argument_the_real_target_rejects(self):
        """`isinstance` proves presence only; the spec is what rejects a wrong call."""
        double = _target_double()
        session_factory, _ = make_mock_session_factory()

        with pytest.raises(TypeError):
            double.ingest(b"raw", "SIG_001", session_factory, poll_reference=_utc(2025, 6, 1, 12, 0))

    @pytest.mark.asyncio
    async def test_a_constrained_double_accepts_the_arguments_the_seam_now_carries(self):
        """The same double takes both new arguments - before R7b this raised TypeError."""
        double = _target_double()
        session_factory, _ = make_mock_session_factory()

        await double.ingest(
            b"raw", "SIG_001", session_factory,
            device_type="controller",
            last_successful_poll=_utc(2025, 6, 2, 12, 0),
        )

        assert double.ingest.await_count == 1


# ---------------------------------------------------------------------------
# Backward poison - end to end from a method, not only from the spine
# ---------------------------------------------------------------------------


class TestBackwardPoison:
    """A method that holds a checkpoint sends the reference; the spine flags on it."""

    @pytest.mark.asyncio
    async def test_http_pull_sends_the_reference_it_already_holds(self):
        """http_pull loads the checkpoint anyway; the reference is one forward away."""
        reference = _utc(2025, 6, 2, 12, 0)
        target = _target_double(last_successful_poll=reference)
        target.ingest.return_value = IngestResult(IngestOutcome.SUCCESS, 0)
        session_factory, _ = make_mock_session_factory()

        with patch("aiohttp.ClientSession", return_value=_http_session(b"<EventResponses/>")):
            await HTTPPullMethod().poll_once(
                "SIG_001", {"host": "10.0.0.5"}, session_factory, target=target,
            )

        assert target.ingest.await_args.kwargs["last_successful_poll"] == reference

    @pytest.mark.asyncio
    async def test_a_backward_poisoned_batch_is_flagged_and_still_ingested(self):
        """A slow or reset controller clock is flagged, never a reason to drop rows."""
        events = [_event(_utc(2025, 6, 1, 12, 0)), _event(_utc(2025, 6, 1, 12, 5))]
        session_factory, _ = make_mock_session_factory()
        resolve, is_remote, persist = _decoding(events)

        with resolve, is_remote, persist, \
                patch(f"{INGEST}.notify", new=AsyncMock()) as notify:
            result = await ControllerTarget().ingest(
                b"raw", "SIG_001", session_factory,
                decoder_name="maxtime",
                filename="poll-042.dat",
                last_successful_poll=_utc(2025, 6, 2, 12, 0),
            )

        alerts = _notifications(notify, "clock_backward_poison")
        assert len(alerts) == 1
        assert alerts[0]["file"] == "poll-042.dat"
        # Ingest is never withheld on the strength of the flag (ADR-0034).
        assert result.outcome is IngestOutcome.SUCCESS
        assert result.events_inserted == 2

    @pytest.mark.asyncio
    async def test_no_reference_leaves_the_check_open_rather_than_failing_the_batch(self):
        """A method holding no checkpoint still ingests; it simply cannot flag."""
        events = [_event(_utc(2025, 6, 1, 12, 0))]
        session_factory, _ = make_mock_session_factory()
        resolve, is_remote, persist = _decoding(events, inserted=1)

        with resolve, is_remote, persist, \
                patch(f"{INGEST}.notify", new=AsyncMock()) as notify:
            result = await ControllerTarget().ingest(
                b"raw", "SIG_001", session_factory, decoder_name="maxtime", filename="poll.dat",
            )

        assert _notifications(notify, "clock_backward_poison") == []
        assert result.outcome is IngestOutcome.SUCCESS
        assert result.events_inserted == 1


# ---------------------------------------------------------------------------
# Provenance and identity checks run host-side and never block ingest
# ---------------------------------------------------------------------------


class TestProvenanceChecksNeverBlockIngest:
    """The checks flag; the rows land regardless."""

    @pytest.mark.asyncio
    async def test_a_temporal_finding_writes_a_review_row_and_the_batch_still_persists(self):
        """The finding reaches the review queue and the events reach the table."""
        ahead = datetime.now(UTC) + timedelta(hours=1)
        events = [_event(ahead)]
        metadata = FileMetadata(source_filename="poll-042.dat", device_ip="10.0.0.5")
        session_factory, session = make_mock_session_factory()
        session.get = AsyncMock(return_value=None)
        _no_suppressions(session)
        resolve, is_remote, persist = _decoding(events, inserted=1, metadata=metadata)

        with resolve, is_remote, persist, patch(f"{INGEST}.notify", new=AsyncMock()):
            result = await ControllerTarget().ingest(
                b"raw", "SIG_001", session_factory, decoder_name="maxtime",
            )

        row = _reviews(session, CHECK_TEMPORAL_INTEGRITY)[0]
        assert row.source_filename == "poll-042.dat"
        assert row.status == "open"
        # The file is recorded whether or not it drew a finding.
        assert _provenance_rows(session)[0].source_filename == "poll-042.dat"
        assert result.outcome is IngestOutcome.SUCCESS
        assert result.events_inserted == 1

    @pytest.mark.asyncio
    async def test_a_failure_inside_the_checks_does_not_withhold_the_events(self):
        """never-lose-data outranks provenance: a raising check is logged, not fatal."""
        events = [_event(_utc(2025, 6, 1, 12, 0))]
        session_factory, _ = make_mock_session_factory()
        resolve, is_remote, persist = _decoding(events, inserted=1)

        with resolve, is_remote, persist, patch(
            f"{INGEST}.validate_and_record_provenance",
            new=AsyncMock(side_effect=RuntimeError("provenance exploded")),
        ) as validate:
            result = await ControllerTarget().ingest(
                b"raw", "SIG_001", session_factory, decoder_name="maxtime",
            )

        assert validate.await_count == 1
        assert result.outcome is IngestOutcome.SUCCESS
        assert result.events_inserted == 1


# ---------------------------------------------------------------------------
# The third door - an out-of-process method gets the same spine
# ---------------------------------------------------------------------------


class TestOutOfProcessDoor:
    """`EventSinkService.decode_and_persist` forwards the whole header."""

    @pytest.mark.asyncio
    async def test_the_review_row_can_name_the_source_file(self):
        """`filename` was discarded, so a plugin's review row named the decoder."""
        session_factory, _ = make_mock_session_factory()
        header = _header(
            device_id="SIG_001", decoder_name="vendor-plugin",
            source_label="plugin", filename="poll-042.dat", device_type=1,
        )

        with patch(f"{BROKER}.ingest_raw", new=AsyncMock()) as spine:
            spine.return_value = IngestResult(IngestOutcome.SUCCESS, 0)
            await EventSinkService(session_factory).decode_and_persist(header, b"raw")

        assert spine.await_args.kwargs["filename"] == "poll-042.dat"

    @pytest.mark.asyncio
    async def test_the_poison_reference_crosses_the_wire_as_tz_aware_utc(self):
        """`ToDatetime()` with no argument is naive, and a naive reference disables the check."""
        session_factory, _ = make_mock_session_factory()
        reference = datetime(2025, 6, 2, 12, 0, tzinfo=UTC)
        header = _header(
            device_id="SIG_001", decoder_name="vendor-plugin",
            source_label="plugin", device_type=1, last_successful_poll=reference,
        )

        with patch(f"{BROKER}.ingest_raw", new=AsyncMock()) as spine:
            spine.return_value = IngestResult(IngestOutcome.SUCCESS, 0)
            await EventSinkService(session_factory).decode_and_persist(header, b"raw")

        forwarded = spine.await_args.kwargs["last_successful_poll"]
        assert forwarded == reference
        assert forwarded.tzinfo is not None

    @pytest.mark.asyncio
    async def test_an_unset_poison_reference_stays_unset(self):
        """Proto absence means "no reference", not the epoch."""
        session_factory, _ = make_mock_session_factory()
        header = _header(
            device_id="SIG_001", decoder_name="vendor-plugin",
            source_label="plugin", device_type=1,
        )

        with patch(f"{BROKER}.ingest_raw", new=AsyncMock()) as spine:
            spine.return_value = IngestResult(IngestOutcome.SUCCESS, 0)
            await EventSinkService(session_factory).decode_and_persist(header, b"raw")

        assert spine.await_args.kwargs["last_successful_poll"] is None

    @pytest.mark.asyncio
    async def test_the_stated_device_class_is_forwarded_rather_than_discarded(self):
        """A sensor plugin's rows must not route as controller events."""
        session_factory, _ = make_mock_session_factory()
        header = _header(
            device_id="SENSOR_001", decoder_name="vendor-plugin",
            source_label="plugin", device_type=2,
        )

        with patch(f"{BROKER}.ingest_raw", new=AsyncMock()) as spine:
            spine.return_value = IngestResult(IngestOutcome.SUCCESS, 0)
            await EventSinkService(session_factory).decode_and_persist(header, b"raw")

        assert spine.await_args.kwargs["device_type"] == device_type_of(2) == "sensor"


# --------------------------------------------------- stated-vs-inferred routing
class TestRoutingByStatedDeviceType:
    """The stated device class routes the batch; absence falls back to inference.

    The dispatch itself is only reachable through `_upsert_events`, and every
    other test that reaches persist patches it out - so without these the whole
    routing change is untested production logic.
    """

    def test_a_stated_class_selects_the_element_type(self):
        assert _element_type_for("controller", None) is DecodedEvent
        assert _element_type_for("sensor", None) is SensorDetection

    def test_an_unknown_stated_class_is_refused_rather_than_guessed(self):
        with pytest.raises(TypeError, match="unknown device_type"):
            _element_type_for("streetlight", _event(_utc(2026, 1, 1, 0, 0)))

    def test_absence_falls_back_to_reading_the_batch(self):
        # Proto UNSPECIFIED, and every in-process caller before the seam change.
        assert _element_type_for(None, _event(_utc(2026, 1, 1, 0, 0))) is DecodedEvent
        assert _element_type_for("", _event(_utc(2026, 1, 1, 0, 0))) is DecodedEvent

    def test_absence_with_an_unrecognisable_element_is_refused(self):
        with pytest.raises(TypeError, match="unknown event type"):
            _element_type_for(None, object())

    @pytest.mark.asyncio
    async def test_a_batch_that_contradicts_the_stated_class_is_refused(self):
        # Stating 'sensor' over controller rows must not silently write them
        # into roadside_event.
        session_factory, _ = make_mock_session_factory()
        with pytest.raises(TypeError, match="does not match device_type"):
            await _upsert_events(
                [_event(_utc(2026, 1, 1, 0, 0))], "SIG-001", session_factory, device_type="sensor",
            )

    @pytest.mark.asyncio
    async def test_an_empty_batch_states_nothing_and_raises_nothing(self):
        session_factory, _ = make_mock_session_factory()
        assert await _upsert_events(
            [], "SIG-001", session_factory, device_type="sensor",
        ) == 0


class TestProtoAbsenceIsNotAnAssertion:
    """UNSPECIFIED means the plugin said nothing, not that it said 'controller'.

    A proto enum defaults to 0. Answering 'controller' there states on the
    plugin's behalf something it never said, and a sensor batch then fails the
    element-type check and persists nothing - rows lost on a path that routed
    correctly by inference before anyone stated anything.
    """

    def test_unset_device_type_states_nothing(self):
        assert stated_device_type_of(_header(device_id="d1")) is None

    @pytest.mark.parametrize("value,expected", [(1, "controller"), (2, "sensor")])
    def test_a_set_device_type_is_forwarded(self, value, expected):
        assert stated_device_type_of(_header(device_id="d1", device_type=value)) == expected

    def test_the_checkpoint_default_is_left_alone(self):
        # load/save/record_error need a string and have always defaulted to
        # controller; only the ingest path treats absence as absence.
        assert device_type_of(0) == "controller"

    @pytest.mark.asyncio
    async def test_a_plugin_that_states_nothing_lets_the_spine_infer(self):
        session_factory, _ = make_mock_session_factory()
        header = _header(device_id="SENSOR_001", decoder_name="vendor-plugin")

        with patch(f"{BROKER}.ingest_raw", new=AsyncMock()) as spine:
            spine.return_value = IngestResult(IngestOutcome.SUCCESS, 0)
            await EventSinkService(session_factory).decode_and_persist(header, b"raw")

        assert spine.await_args.kwargs["device_type"] is None, (
            "an unset enum must reach the spine as absence, or a sensor batch "
            "is routed as controller rows and persists nothing"
        )
