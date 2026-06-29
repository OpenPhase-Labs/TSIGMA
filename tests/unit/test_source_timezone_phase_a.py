"""Phase A unit tests for per-signal timezone normalization (INFRA ONLY).

Phase A adds the storage + settings + resolver scaffolding for per-signal
source-timezone normalization. NO decoder changes are in scope here. The
four pieces under test:

1. ``Signal.source_timezone`` - a new nullable Text column on the Signal ORM
   model holding an IANA timezone name (e.g. ``"America/New_York"``) or NULL.
2. Alembic migration ``000000000009`` chained onto head ``000000000008``.
3. ``collection.default_timezone`` - a new registered system setting
   (python_type ``str``, default ``"UTC"``, category ``"collection"``).
4. ``resolve_source_timezone(signal_id, session)`` - a helper returning
   ``str | None`` resolving in this order: the Signal row's non-null
   ``source_timezone`` -> the ``collection.default_timezone`` system setting
   value -> ``None``.

Mocking notes (BLIND test-author decisions, documented for the reviewer):

* **Resolver import location.** The resolver is imported from
  ``tsigma.collection.sdk`` (the ingestion-method toolbox alongside
  ``persist_events`` / ``persist_events_with_drift_check``), which is the
  package Phase B's decode-and-persist path would call it from. If the
  implementer places it in a sibling helper module instead, that module must
  re-export it from ``tsigma.collection.sdk`` for these tests to pass.

* **Signal lookup mock.** Mirrors the ``scalar_one_or_none()`` idiom used by
  the SDK's own checkpoint loaders (``load_checkpoint`` etc.): the mocked
  ``session.execute`` returns a result whose ``scalar_one_or_none()`` yields
  either a Signal row (with a ``source_timezone`` attribute) or ``None``.

* **Settings read boundary.** Patched at the shared module-level singleton
  ``tsigma.settings_service.settings_cache.get`` - the untyped
  ``get(key, session) -> str | None`` contract (NOT the typed ``get_str``,
  which substitutes the registered default and can never yield ``None``).
  The resolver must consult the untyped cache so that "no signal tz AND no
  default row" can resolve to ``None`` per the Phase A resolution order. The
  patch covers both a direct ``settings_cache.get`` call and an indirect call
  routed through ``get_str`` (which itself calls ``settings_cache.get``),
  because both funnel through the same singleton method.
"""

import glob
import importlib.util
import os
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests._helpers import make_mock_session
from tsigma.models.signal import Signal

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session_with_signal(signal_row):
    """Mock AsyncSession whose execute().scalar_one_or_none() -> signal_row."""
    session = make_mock_session()
    result = MagicMock()
    result.scalar_one_or_none.return_value = signal_row
    session.execute = AsyncMock(return_value=result)
    return session


def _make_signal_row(source_timezone):
    """A stand-in Signal row exposing a ``source_timezone`` attribute."""
    row = MagicMock(spec=Signal)
    row.source_timezone = source_timezone
    return row


# ---------------------------------------------------------------------------
# 1. Signal.source_timezone column
# ---------------------------------------------------------------------------


class TestSignalSourceTimezoneColumn:
    """The Signal ORM model carries a nullable ``source_timezone`` column."""

    def test_source_timezone_column_registered_on_table(self):
        """``source_timezone`` appears in ``Signal.__table__.columns``."""
        assert "source_timezone" in Signal.__table__.columns

    def test_source_timezone_column_is_nullable(self):
        """The column is nullable (a signal may have no declared timezone)."""
        column = Signal.__table__.columns["source_timezone"]
        assert column.nullable is True

    def test_instance_round_trips_iana_timezone(self):
        """A Signal instance accepts and preserves an IANA timezone name."""
        signal = Signal(
            signal_id="SIG_001",
            primary_street="Main St",
            source_timezone="America/New_York",
        )
        assert signal.source_timezone == "America/New_York"

    def test_source_timezone_defaults_to_none_when_omitted(self):
        """Omitting ``source_timezone`` leaves it ``None``."""
        signal = Signal(signal_id="SIG_002", primary_street="Main St")
        assert signal.source_timezone is None


# ---------------------------------------------------------------------------
# 2. Alembic migration 000000000009
# ---------------------------------------------------------------------------


class TestAlembicMigration0009:
    """Migration ``000000000009`` exists and chains onto ``000000000008``."""

    def _versions_dir(self):
        return os.path.join(
            os.path.dirname(__file__), "..", "..", "alembic", "versions"
        )

    def test_migration_file_exists_with_correct_prefix(self):
        """Exactly one migration file starts with ``000000000009_``."""
        files = glob.glob(
            os.path.join(self._versions_dir(), "000000000009_*.py")
        )
        assert len(files) == 1, (
            f"Expected exactly one migration file starting with "
            f"000000000009_, found {files}"
        )

    def test_migration_exposes_correct_revision_and_down_revision(self):
        """The module sets ``revision``/``down_revision`` to 9 chained on 8."""
        files = glob.glob(
            os.path.join(self._versions_dir(), "000000000009_*.py")
        )
        assert len(files) == 1
        migration_file = files[0]
        spec = importlib.util.spec_from_file_location(
            "migration_0009_module", migration_file
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.revision == "000000000009"
        assert module.down_revision == "000000000008"


# ---------------------------------------------------------------------------
# 3. collection.default_timezone system setting
# ---------------------------------------------------------------------------


class TestCollectionDefaultTimezoneSetting:
    """``collection.default_timezone`` is a registered str setting (UTC)."""

    def test_key_registered_in_registry(self):
        """The key is present in the module-level settings registry."""
        from tsigma.settings_service import _REGISTRY

        assert "collection.default_timezone" in _REGISTRY

    def test_registry_entry_has_str_type_utc_default_collection_category(self):
        """The entry is str-typed, defaults to ``"UTC"``, category collection."""
        from tsigma.settings_service import _REGISTRY, RegistryEntry

        entry = _REGISTRY["collection.default_timezone"]
        assert isinstance(entry, RegistryEntry)
        assert entry.key == "collection.default_timezone"
        assert entry.python_type is str
        assert entry.default == "UTC"
        assert entry.category == "collection"


# ---------------------------------------------------------------------------
# 4. resolve_source_timezone(signal_id, session) helper
# ---------------------------------------------------------------------------


class TestResolveSourceTimezone:
    """Resolution order: signal tz -> default setting -> None."""

    @pytest.mark.asyncio
    async def test_returns_signal_source_timezone_when_present(self, monkeypatch):
        """A non-null Signal.source_timezone short-circuits (no settings read)."""
        from tsigma.collection.sdk import resolve_source_timezone

        # If the resolver consulted settings here it would be a bug; make any
        # settings read explode so the short-circuit is enforced, not assumed.
        boom = AsyncMock(side_effect=AssertionError("settings must not be read"))
        monkeypatch.setattr(
            "tsigma.settings_service.settings_cache.get", boom
        )

        session = _make_session_with_signal(
            _make_signal_row("America/Chicago")
        )
        result = await resolve_source_timezone("SIG_001", session)
        assert result == "America/Chicago"

    @pytest.mark.asyncio
    async def test_falls_back_to_default_setting_when_signal_tz_none(
        self, monkeypatch
    ):
        """Signal tz None -> the default setting value is returned."""
        from tsigma.collection.sdk import resolve_source_timezone

        monkeypatch.setattr(
            "tsigma.settings_service.settings_cache.get",
            AsyncMock(return_value="America/New_York"),
        )
        # No env-var override should interfere with an indirect get_str path.
        monkeypatch.delenv("TSIGMA_COLLECTION_DEFAULT_TIMEZONE", raising=False)

        session = _make_session_with_signal(_make_signal_row(None))
        result = await resolve_source_timezone("SIG_001", session)
        assert result == "America/New_York"

    @pytest.mark.asyncio
    async def test_returns_none_when_no_signal_tz_and_no_default(
        self, monkeypatch
    ):
        """Signal tz None AND settings returns None -> resolver returns None."""
        from tsigma.collection.sdk import resolve_source_timezone

        monkeypatch.setattr(
            "tsigma.settings_service.settings_cache.get",
            AsyncMock(return_value=None),
        )
        monkeypatch.delenv("TSIGMA_COLLECTION_DEFAULT_TIMEZONE", raising=False)

        session = _make_session_with_signal(_make_signal_row(None))
        result = await resolve_source_timezone("SIG_001", session)
        assert result is None
