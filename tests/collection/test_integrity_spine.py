"""Host-owned integrity spine: identity/config validation and provenance.

Moved from tests/unit/test_ftp_pull.py in P7c - the code these exercise now lives
in tsigma/collection/ingest.py, not in the FTP method.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests._helpers import make_mock_session
from tsigma.collection.decoders.base import (
    DecodedEvent,
    DecodeResult,
    FileMetadata,
)
from tsigma.collection.ingest import validate_and_record_provenance
from tsigma.models.file_provenance import FileIngestProvenance
from tsigma.models.ingest_review import IngestReview
from tsigma.notifications.registry import WARNING

# Module path prefix for patching SDK imports used in ftp_pull.py
_MOD = "tsigma.collection.methods.ftp_pull"


def _make_metadata(**overrides):
    """Build a FileMetadata with sane provenance defaults."""
    defaults = {
        "device_ip": "10.0.0.5",
        "device_mac": "00:04:81:02:15:0d",
        "log_version": "2.4.4",
        "source_filename": "events.dat",
        "phases_in_use": [2, 4, 6, 8],
        "log_begin": datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
        "header_anchor": datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return FileMetadata(**defaults)


_PHASE_ROWS_2468 = [(2, None, None), (4, None, None), (6, None, None), (8, None, None)]


def _mock_client(files=None):
    """Create a mock _FileTransferClient."""
    client = AsyncMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.list_dir = AsyncMock(return_value=files or [])
    client.download = AsyncMock(return_value=b"")
    return client


def _mock_decoder(events=None):
    """Create a mock decoder instance.

    Production calls ``decoder.decode(data)`` and reads ``result.events``, so
    the envelope ``decode.return_value`` is what feeds the ingest path. We also
    keep ``decode_bytes.return_value`` configured (harmless) for any direct use.
    Metadata is ``None`` so ``_validate_and_record_provenance`` short-circuits.
    """
    resolved = events or []
    decoder = MagicMock()
    decoder.decode_bytes.return_value = resolved
    decoder.decode.return_value = DecodeResult(events=resolved, metadata=None)
    return decoder


def _mock_session_factory():
    """Create a mock async session factory with proper checkpoint support."""
    mock_session = make_mock_session()
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    mock_session.execute = AsyncMock(return_value=result_mock)
    mock_session.flush = AsyncMock()
    mock_session_ctx = MagicMock()
    mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock(return_value=mock_session_ctx)
    return factory, mock_session


def _make_mock_target() -> MagicMock:
    """Build a mock ``IngestionTarget`` with async SDK methods.

    Matches the ``IngestionTarget`` protocol: ``resolve_decoder`` is
    synchronous; persistence/checkpoint methods are async.
    """
    target = MagicMock()
    target.device_type = "controller"
    target.load_checkpoint = AsyncMock(return_value=None)
    target.save_checkpoint = AsyncMock()
    target.record_error = AsyncMock()
    target.persist = AsyncMock()
    target.persist_with_drift_check = AsyncMock()
    target.resolve_decoder = MagicMock()
    return target


# ---------------------------------------------------------------------------
# FTPPullMethod — registration and construction
# ---------------------------------------------------------------------------


def _make_signal(signal_metadata=None):
    """Build a fake Signal-like object with a mutable signal_metadata attr."""
    sig = SimpleNamespace()
    sig.signal_metadata = signal_metadata
    return sig


def _make_validation_session_factory(
    signal=None,
    phase_rows=None,
):
    """Build a session factory mock for _validate_and_record_provenance.

    Mirrors the async-context-manager session pattern in
    ``_mock_session_factory`` (tests/unit/test_ftp_pull.py) and
    ``make_mock_session`` (tests/_helpers.py), but exposes ``get`` and
    an ``execute(...).all()`` surface for the Signal + Approach loads.

    ``phase_rows`` is the list of row tuples returned by ``.all()`` — e.g.
    ``[(2, None, None), (4, None, None), (6, None, None), (8, None, None)]``
    (protected_phase_number, permissive_phase_number, ped_phase_number).
    """
    mock_session = make_mock_session()
    mock_session.get = AsyncMock(return_value=signal)
    result_mock = MagicMock()
    result_mock.all = MagicMock(return_value=phase_rows or [])
    mock_session.execute = AsyncMock(return_value=result_mock)
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session_ctx = MagicMock()
    mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock(return_value=mock_session_ctx)
    return factory, mock_session


def _make_metadata(**overrides):
    """Build a FileMetadata with sane provenance defaults."""
    defaults = {
        "device_ip": "10.0.0.5",
        "device_mac": "00:04:81:02:15:0d",
        "log_version": "2.4.4",
        "source_filename": "events.dat",
        "phases_in_use": [2, 4, 6, 8],
        "log_begin": datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
        "header_anchor": datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return FileMetadata(**defaults)


_PHASE_ROWS_2468 = [(2, None, None), (4, None, None), (6, None, None), (8, None, None)]


class TestIngestIdentityConfigValidation:
    """Inc-2 Task 4: _validate_and_record_provenance behavior.

    Validation/provenance is best-effort and must NEVER block ingest:
    findings notify (unless suppressed/seeding), a provenance row is always
    recorded, and any internal error is swallowed.
    """

    @pytest.mark.asyncio
    async def test_metadata_none_no_db_work_no_notify(self):
        """metadata=None short-circuits: no DB load, no add, no notify."""
        factory, session = _make_validation_session_factory(
            signal=_make_signal({}), phase_rows=_PHASE_ROWS_2468,
        )
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", None, "asc3",
            )
        session.get.assert_not_called()
        session.add.assert_not_called()
        mock_notify.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_seed_on_empty_registered_mac(self):
        """Empty registered MAC + header MAC -> seed silently, no notify."""
        signal = _make_signal({})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        metadata = _make_metadata(device_mac="00:04:81:02:15:0d")
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3",
            )
        # the controller_mac is seeded onto the signal's metadata
        assert signal.signal_metadata["controller_mac"] == "00:04:81:02:15:0d"
        # seeding is silent
        mock_notify.assert_not_awaited()
        # exactly one provenance row recorded
        assert session.add.call_count == 1
        added = session.add.call_args[0][0]
        assert isinstance(added, FileIngestProvenance)
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_mac_matches_no_notify(self):
        """Registered MAC == header MAC -> no replacement finding, no notify."""
        signal = _make_signal({"controller_mac": "00:04:81:02:15:0d"})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        # mixed case / separators normalize to equal
        metadata = _make_metadata(device_mac="00-04-81-02-15-0D")
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3",
            )
        mock_notify.assert_not_awaited()
        assert session.add.call_count == 1
        assert isinstance(session.add.call_args[0][0], FileIngestProvenance)

    @pytest.mark.asyncio
    async def test_mac_differs_replacement_notify(self):
        """Registered MAC != header MAC -> replacement notify + provenance."""
        signal = _make_signal({"controller_mac": "00:04:81:02:15:0d"})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        metadata = _make_metadata(device_mac="AA:BB:CC:DD:EE:FF")
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3",
            )
        mock_notify.assert_awaited()
        # the alert is WARNING severity and references this device
        call = mock_notify.await_args_list[0]
        assert call.kwargs.get("severity") == WARNING
        haystack = repr(call.args) + repr(call.kwargs)
        assert "SIG-001" in haystack
        # provenance still recorded
        assert session.add.call_count == 1
        assert isinstance(session.add.call_args[0][0], FileIngestProvenance)

    @pytest.mark.asyncio
    async def test_phase_drift_notify(self):
        """Configured phases differ from phases-in-use -> drift notify."""
        # MAC matches so the only finding is phase drift
        signal = _make_signal({"controller_mac": "00:04:81:02:15:0d"})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        # extra phase 5 in the file vs configured {2,4,6,8}
        metadata = _make_metadata(
            device_mac="00:04:81:02:15:0d",
            phases_in_use=[2, 4, 5, 6, 8],
        )
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3",
            )
        mock_notify.assert_awaited()
        assert session.add.call_count == 1
        assert isinstance(session.add.call_args[0][0], FileIngestProvenance)

    @pytest.mark.asyncio
    async def test_finding_suppressed_no_notify_but_provenance(self):
        """Suppression silences notify but provenance is still recorded."""
        signal = _make_signal({"controller_mac": "00:04:81:02:15:0d"})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        metadata = _make_metadata(device_mac="AA:BB:CC:DD:EE:FF")
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=True):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3",
            )
        mock_notify.assert_not_awaited()
        # provenance is recorded regardless of suppression
        assert session.add.call_count == 1
        assert isinstance(session.add.call_args[0][0], FileIngestProvenance)
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_notify_raises_swallowed_provenance_and_commit(self):
        """A notify() failure is swallowed; provenance + commit still happen."""
        signal = _make_signal({"controller_mac": "00:04:81:02:15:0d"})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        metadata = _make_metadata(device_mac="AA:BB:CC:DD:EE:FF")
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock,
                   side_effect=RuntimeError("notify down")), \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            # must not raise
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3",
            )
        assert session.add.call_count == 1
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_db_error_swallowed_never_blocks(self):
        """A DB/provenance error is swallowed — the helper never raises."""
        signal = _make_signal({"controller_mac": "00:04:81:02:15:0d"})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        session.commit.side_effect = RuntimeError("db down")
        metadata = _make_metadata(device_mac="00:04:81:02:15:0d")
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            # must not raise despite commit blowing up
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3",
            )

    @pytest.mark.asyncio
    async def test_persist_runs_even_if_validation_raises(self):
        """never-lose-data: a provenance failure must not block ingest."""
        from types import SimpleNamespace

        from tsigma.collection.ingest import IngestOutcome, ingest_raw

        now = datetime.now(timezone.utc)
        dec = SimpleNamespace(
            decode=lambda raw: DecodeResult(
                events=[DecodedEvent(timestamp=now, event_code=1, event_param=0)],
                metadata=_make_metadata(),
            ),
            name="asc3",
        )
        factory, _ = _mock_session_factory()
        with (
            patch("tsigma.collection.ingest.resolve_decoder_by_name", return_value=dec),
            patch("tsigma.collection.ingest.validate_and_record_provenance",
                  new_callable=AsyncMock, side_effect=RuntimeError("validation boom")),
            patch("tsigma.collection.ingest.persist_events_with_drift_check",
                  new_callable=AsyncMock, return_value=1) as persist,
        ):
            out = await ingest_raw(b"\x00", device_id="SIG-001",
                                   decoder_name="asc3", session_factory=factory)

        assert out.outcome is IngestOutcome.SUCCESS
        persist.assert_awaited_once()


# ---------------------------------------------------------------------------
# Inc-3 Task 3: temporal-integrity review during provenance recording
# ---------------------------------------------------------------------------


def _evt(ts):
    """Build a DecodedEvent at the given tz-aware timestamp."""
    return DecodedEvent(timestamp=ts, event_code=1, event_param=0)


def _added_reviews(session):
    """All IngestReview instances passed to session.add."""
    return [
        call.args[0]
        for call in session.add.call_args_list
        if call.args and isinstance(call.args[0], IngestReview)
    ]


def _added_provenance(session):
    """All FileIngestProvenance instances passed to session.add."""
    return [
        call.args[0]
        for call in session.add.call_args_list
        if call.args and isinstance(call.args[0], FileIngestProvenance)
    ]


# Deterministic extremes so the clock offset is unambiguous vs real wall-clock.
_FAR_PAST = datetime(2000, 1, 1, tzinfo=timezone.utc)
_FAR_FUTURE = datetime(2100, 1, 1, tzinfo=timezone.utc)


class TestTemporalIntegrityReview:
    """Inc-3 Task 3: temporal-integrity finding -> IngestReview + notify.

    When ``events`` are supplied, ``_validate_and_record_provenance`` runs the
    temporal-integrity check against server-now and, on a finding, appends it to
    the same suppressible notify loop AND records an ``IngestReview`` row. The
    provenance row + commit still happen and any failure is swallowed (ingest
    must never be blocked). Identity + phases are aligned in these tests so the
    temporal finding is the only one in play.
    """

    @pytest.mark.asyncio
    async def test_far_past_adds_review_and_notifies(self):
        """Far-past events -> temporal IngestReview added + notify awaited."""
        metadata = _make_metadata()
        signal = _make_signal({"controller_mac": metadata.device_mac})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        events = [_evt(_FAR_PAST)]
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3", events,
            )
        reviews = _added_reviews(session)
        assert len(reviews) == 1
        review = reviews[0]
        assert review.reason == "temporal_integrity"
        assert review.status == "open"
        assert review.signal_id == "SIG-001"
        mock_notify.assert_awaited()
        assert _added_provenance(session)
        provenances = _added_provenance(session)
        assert len(provenances) == 1
        assert reviews[0].provenance_id is not None
        assert reviews[0].provenance_id == provenances[0].provenance_id
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_far_future_adds_review_and_notifies(self):
        """Far-future events -> temporal IngestReview added + notify awaited."""
        metadata = _make_metadata()
        signal = _make_signal({"controller_mac": metadata.device_mac})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        events = [_evt(_FAR_FUTURE)]
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3", events,
            )
        reviews = _added_reviews(session)
        assert len(reviews) == 1
        assert reviews[0].reason == "temporal_integrity"
        mock_notify.assert_awaited()
        provenances = _added_provenance(session)
        assert len(provenances) == 1
        assert reviews[0].provenance_id is not None
        assert reviews[0].provenance_id == provenances[0].provenance_id

    @pytest.mark.asyncio
    async def test_within_tolerance_no_review(self):
        """Events at ~now -> no temporal review, no notify; provenance + commit."""
        metadata = _make_metadata()
        signal = _make_signal({"controller_mac": metadata.device_mac})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        events = [_evt(datetime.now(timezone.utc))]
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3", events,
            )
        assert _added_reviews(session) == []
        mock_notify.assert_not_awaited()
        assert _added_provenance(session)
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_temporal_suppressed_no_notify_but_review_added(self):
        """Suppression silences notify but the IngestReview is still recorded."""
        metadata = _make_metadata()
        signal = _make_signal({"controller_mac": metadata.device_mac})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        events = [_evt(_FAR_PAST)]
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock) as mock_notify, \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=True):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3", events,
            )
        mock_notify.assert_not_awaited()
        reviews = _added_reviews(session)
        assert len(reviews) == 1
        assert reviews[0].reason == "temporal_integrity"

    @pytest.mark.asyncio
    async def test_no_events_no_temporal_review(self):
        """events=None -> no temporal review; provenance still recorded."""
        metadata = _make_metadata()
        signal = _make_signal({"controller_mac": metadata.device_mac})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3", None,
            )
        assert _added_reviews(session) == []
        assert _added_provenance(session)
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_review_insert_error_swallowed(self):
        """A commit failure is swallowed — the helper never blocks ingest."""
        metadata = _make_metadata()
        signal = _make_signal({"controller_mac": metadata.device_mac})
        factory, session = _make_validation_session_factory(
            signal=signal, phase_rows=_PHASE_ROWS_2468,
        )
        session.commit.side_effect = RuntimeError("db down")
        events = [_evt(_FAR_PAST)]
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock), \
             patch("tsigma.collection.ingest.is_suppressed",
                   new_callable=AsyncMock, return_value=False):
            # must not raise despite commit blowing up
            await validate_and_record_provenance(
                factory, "SIG-001", metadata, "asc3", events,
            )


class TestBackwardPoisonFlag:
    """Moved from the FTP method in P7c - poison flagging is host-owned.

    ingest + flag, never withhold (ADR-0034): a slow or reset controller clock
    is surfaced for review, and the events are still persisted.
    """

    @pytest.mark.asyncio
    async def test_poisoned_batch_is_flagged(self):
        from tsigma.collection.ingest import flag_if_backward_poison

        last_poll = datetime(2026, 6, 10, tzinfo=timezone.utc)
        events = [_evt(datetime(2026, 6, 1, tzinfo=timezone.utc))]   # predates it
        with patch("tsigma.collection.ingest.notify", new_callable=AsyncMock) as notify_:
            await flag_if_backward_poison(events, "SIG-1", "events.dat", last_poll)
        notify_.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_clean_batch_is_not_flagged(self):
        from tsigma.collection.ingest import flag_if_backward_poison

        last_poll = datetime(2026, 6, 1, tzinfo=timezone.utc)
        events = [_evt(datetime(2026, 6, 10, tzinfo=timezone.utc))]
        with patch("tsigma.collection.ingest.notify", new_callable=AsyncMock) as notify_:
            await flag_if_backward_poison(events, "SIG-1", "events.dat", last_poll)
        notify_.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_first_poll_is_not_flagged(self):
        from tsigma.collection.ingest import flag_if_backward_poison

        events = [_evt(datetime(2026, 6, 1, tzinfo=timezone.utc))]
        with patch("tsigma.collection.ingest.notify", new_callable=AsyncMock) as notify_:
            await flag_if_backward_poison(events, "SIG-1", "events.dat", None)
        notify_.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_notify_failure_never_blocks(self):
        """The flag is advisory; ingest proceeds regardless."""
        from tsigma.collection.ingest import flag_if_backward_poison

        last_poll = datetime(2026, 6, 10, tzinfo=timezone.utc)
        events = [_evt(datetime(2026, 6, 1, tzinfo=timezone.utc))]
        with patch("tsigma.collection.ingest.notify",
                   new_callable=AsyncMock, side_effect=RuntimeError("smtp down")):
            await flag_if_backward_poison(events, "SIG-1", "events.dat", last_poll)
