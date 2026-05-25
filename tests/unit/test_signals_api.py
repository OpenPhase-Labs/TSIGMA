"""
Unit tests for signals API endpoints.

Tests GET (redaction), POST (encryption), and PUT (encryption) operations.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests._helpers import make_mock_session
from tsigma.api.v1.signals import router
from tsigma.auth.sessions import SessionData
from tsigma.models import Signal


def _create_test_app():
    """Create a minimal FastAPI app with the signals router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/signals")
    return app


def _add_access_overrides(app):
    """Override require_access sub-dependencies so GET endpoints pass auth."""
    from tsigma.auth.dependencies import _get_db_session, get_current_user_optional

    app.dependency_overrides[get_current_user_optional] = lambda: SessionData(
        user_id=uuid4(),
        username="testuser",
        role="viewer",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )

    async def _mock_access_db():
        mock = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock.execute = AsyncMock(return_value=result)
        yield mock

    app.dependency_overrides[_get_db_session] = _mock_access_db


def _add_admin_overrides(app):
    """Override admin dependency for POST/PUT/DELETE endpoints."""
    from tsigma.auth.dependencies import require_admin

    app.dependency_overrides[require_admin] = lambda: SessionData(
        user_id=str(uuid4()),
        username="admin",
        role="admin",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _mock_signal(**overrides):
    """Create a mock Signal ORM object."""
    now = datetime.now(timezone.utc)
    defaults = {
        "signal_id": "SIG-001",
        "primary_street": "Main St",
        "secondary_street": "1st Ave",
        "latitude": Decimal("33.7490"),
        "longitude": Decimal("-84.3880"),
        "enabled": True,
        "metadata": None,
        "created_at": now,
        "updated_at": now,
    }
    defaults.update(overrides)
    mock = MagicMock(spec=Signal)
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


class TestGetSignalRedactsPassword:
    """Tests for GET /api/v1/signals/{id} credential redaction."""

    def test_get_signal_redacts_password(self):
        """GET /signals/{id} returns metadata with password as '***'."""
        signal = _mock_signal(
            metadata={
                "collection": {
                    "method": "ftp",
                    "username": "admin",
                    "password": "supersecret",
                }
            }
        )
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = signal
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/SIG-001")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metadata"]["collection"]["password"] == "***"
        assert data["metadata"]["collection"]["username"] == "admin"

    def test_get_signal_redacts_ssh_key_path(self):
        """GET /signals/{id} returns metadata with ssh_key_path as '***'."""
        signal = _mock_signal(
            metadata={
                "collection": {
                    "method": "sftp",
                    "ssh_key_path": "/home/user/.ssh/id_rsa",
                }
            }
        )
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = signal
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/SIG-001")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metadata"]["collection"]["ssh_key_path"] == "***"

    def test_get_signal_no_metadata(self):
        """GET /signals/{id} with None metadata returns null."""
        signal = _mock_signal(metadata=None)
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = signal
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/SIG-001")
        assert resp.status_code == 200
        assert resp.json()["metadata"] is None


class TestCreateSignalEncryptsPassword:
    """Tests for POST /api/v1/signals/ encryption of sensitive fields."""

    @patch("tsigma.api.v1.signals.has_encryption_key", return_value=True)
    @patch("tsigma.api.v1.signals.encrypt_sensitive_fields")
    def test_create_signal_encrypts_password(
        self, mock_encrypt, mock_has_key
    ):
        """POST /signals/ with metadata.collection.password encrypts it."""
        metadata_input = {
            "collection": {"method": "ftp", "password": "secret123"}
        }
        encrypted_metadata = {
            "collection": {"method": "ftp", "password": "gAAAAA_encrypted"}
        }
        mock_encrypt.return_value = encrypted_metadata

        mock_session = make_mock_session()
        # First execute: check for existing signal (returns None)
        existing_result = MagicMock()
        existing_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=existing_result)
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)

        from tsigma.dependencies import get_audited_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_audited_session] = override_session

        client = TestClient(app)
        resp = client.post(
            "/api/v1/signals/",
            json={
                "signal_id": "SIG-NEW",
                "primary_street": "Main St",
                "metadata": metadata_input,
            },
        )

        assert resp.status_code == 201
        mock_encrypt.assert_called_once_with(metadata_input)

    @patch("tsigma.api.v1.signals.has_encryption_key", return_value=False)
    @patch("tsigma.api.v1.signals.encrypt_sensitive_fields")
    def test_create_signal_skips_encryption_without_key(
        self, mock_encrypt, mock_has_key
    ):
        """POST /signals/ without encryption key does not call encrypt."""
        metadata_input = {
            "collection": {"method": "ftp", "password": "secret123"}
        }

        mock_session = make_mock_session()
        existing_result = MagicMock()
        existing_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=existing_result)
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)

        from tsigma.dependencies import get_audited_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_audited_session] = override_session

        client = TestClient(app)
        resp = client.post(
            "/api/v1/signals/",
            json={
                "signal_id": "SIG-NEW",
                "primary_street": "Main St",
                "metadata": metadata_input,
            },
        )

        assert resp.status_code == 201
        mock_encrypt.assert_not_called()


class TestUpdateSignalEncryptsPassword:
    """Tests for PUT /api/v1/signals/{id} encryption of sensitive fields."""

    @patch("tsigma.api.v1.signals.has_encryption_key", return_value=True)
    @patch("tsigma.api.v1.signals.encrypt_sensitive_fields")
    def test_update_signal_encrypts_password(
        self, mock_encrypt, mock_has_key
    ):
        """PUT /signals/{id} with metadata.collection.password encrypts it."""
        signal = _mock_signal()
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = signal
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)

        from tsigma.dependencies import get_audited_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_audited_session] = override_session

        metadata_input = {
            "collection": {"method": "ftp", "password": "newpassword"}
        }

        client = TestClient(app)
        resp = client.put(
            "/api/v1/signals/SIG-001",
            json={"metadata": metadata_input},
        )

        assert resp.status_code == 200
        mock_encrypt.assert_called_once_with(metadata_input)

    @patch("tsigma.api.v1.signals.has_encryption_key", return_value=False)
    @patch("tsigma.api.v1.signals.encrypt_sensitive_fields")
    def test_update_signal_skips_encryption_without_key(
        self, mock_encrypt, mock_has_key
    ):
        """PUT /signals/{id} without encryption key does not call encrypt."""
        signal = _mock_signal()
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = signal
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)

        from tsigma.dependencies import get_audited_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_audited_session] = override_session

        metadata_input = {
            "collection": {"method": "ftp", "password": "newpassword"}
        }

        client = TestClient(app)
        resp = client.put(
            "/api/v1/signals/SIG-001",
            json={"metadata": metadata_input},
        )

        assert resp.status_code == 200
        mock_encrypt.assert_not_called()


class TestListSignals:
    """Tests for GET /api/v1/signals/ (list all signals)."""

    def test_list_signals(self):
        """GET /signals/ returns a list of signals."""
        sig1 = _mock_signal(signal_id="SIG-001")
        sig2 = _mock_signal(signal_id="SIG-002", primary_street="Oak Ave")

        mock_session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = [sig1, sig2]
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["signal_id"] == "SIG-001"
        assert data[1]["signal_id"] == "SIG-002"

    def test_list_signals_empty(self):
        """GET /signals/ returns empty list when no signals exist."""
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/")
        assert resp.status_code == 200
        assert resp.json() == []


class TestDeleteSignal:
    """Tests for DELETE /api/v1/signals/{signal_id}."""

    def test_delete_signal(self):
        """DELETE /signals/{id} returns 204 on success."""
        signal = _mock_signal()
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = signal
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.delete = AsyncMock()
        mock_session.flush = AsyncMock()

        app = _create_test_app()
        _add_admin_overrides(app)

        from tsigma.dependencies import get_audited_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_audited_session] = override_session

        client = TestClient(app)
        resp = client.delete("/api/v1/signals/SIG-001")
        assert resp.status_code == 204

    def test_delete_signal_not_found(self):
        """DELETE /signals/{id} returns 404 for unknown signal."""
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_admin_overrides(app)

        from tsigma.dependencies import get_audited_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_audited_session] = override_session

        client = TestClient(app)
        resp = client.delete("/api/v1/signals/UNKNOWN")
        assert resp.status_code == 404


class TestSignalAudit:
    """Tests for GET /api/v1/signals/{signal_id}/audit."""

    def test_get_signal_audit(self):
        """GET /signals/{id}/audit returns audit records."""
        signal = _mock_signal()
        now = datetime.now(timezone.utc)

        audit_row = MagicMock()
        audit_row.audit_id = 1
        audit_row.signal_id = "SIG-001"
        audit_row.changed_at = now
        audit_row.changed_by = "admin"
        audit_row.operation = "UPDATE"
        audit_row.old_values = {"enabled": True}
        audit_row.new_values = {"enabled": False}

        mock_session = make_mock_session()

        # First call: signal existence check
        signal_result = MagicMock()
        signal_result.scalar_one_or_none.return_value = signal
        # Second call: audit records
        audit_result = MagicMock()
        audit_result.scalars.return_value.all.return_value = [audit_row]

        mock_session.execute = AsyncMock(
            side_effect=[signal_result, audit_result]
        )

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/SIG-001/audit")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["signal_id"] == "SIG-001"
        assert data[0]["operation"] == "UPDATE"

    def test_get_signal_audit_not_found(self):
        """GET /signals/{id}/audit returns 404 for unknown signal."""
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get("/api/v1/signals/UNKNOWN/audit")
        assert resp.status_code == 404


class TestSignalEvents:
    """Tests for GET /api/v1/signals/{signal_id}/events (raw IHR read).

    Post-B12.2 the endpoint must:
      - read events via ``fetch_events`` (tier-aware SDK) instead of
        ``session.execute(select(ControllerEventLog))``
      - paginate via ``paginated_event_list`` returning ``{items, next_cursor}``
      - call ``require_max_lookback`` + ``require_max_aggregation_days``
        guards before doing any work
    """

    @staticmethod
    def _wire_v2(
        app,
        monkeypatch,
        *,
        signal_lookup_returns="SIG-001",
        events_df=None,
        max_page_size=1000,
        max_lookback_days=92,
        max_aggregation_days=92,
    ):
        """Wire post-B12.2 dependencies.

        - ``session.execute`` returns the signal-existence lookup ONLY
          (event reads go through ``fetch_events``).
        - Patches ``fetch_events`` at the use site (``tsigma.api.v1.signals``)
          with ``raising=False`` so setattr succeeds even while the import is
          not yet in the module (red phase) — but ``mock.assert_called_*``
          checks will fail because the handler still uses ``session.execute``.
        - Patches ``paginated_event_list`` to return a deterministic envelope
          based on the input DataFrame (uses real helper semantics so cursor
          round-trips work).
        - Patches the two guards as AsyncMocks that no-op by default; tests
          override behavior via the returned mock objects.

        Returns a dict of mocks: ``mock_session``, ``mock_fetch``,
        ``mock_paginate``, ``mock_lookback``, ``mock_aggregation``,
        ``mock_get_int``.
        """
        if events_df is None:
            events_df = pd.DataFrame(
                {
                    "event_time": [],
                    "event_code": [],
                    "event_param": [],
                }
            )

        signal_result = MagicMock()
        signal_result.scalar_one_or_none.return_value = signal_lookup_returns

        mock_session = make_mock_session()
        mock_session.execute = AsyncMock(return_value=signal_result)

        from tsigma.dependencies import get_session

        async def _override():
            yield mock_session

        app.dependency_overrides[get_session] = _override

        # Patch the SDK use-sites. raising=False so red-phase setattr does
        # not blow up before the imports are added in B12.2. The handler
        # still uses session.execute today, so any assert_called check on
        # these mocks will FAIL — which is the assertion-style RED we want.
        mock_fetch = AsyncMock(return_value=events_df)
        mock_paginate = AsyncMock()
        mock_lookback = AsyncMock(return_value=None)
        mock_aggregation = AsyncMock(return_value=None)

        # Default paginate behavior: take limit rows, emit cursor only when
        # there are more rows remaining. Tests that want a specific shape
        # can override mock_paginate.return_value directly.
        async def _default_paginate(rows, *, after, limit, session):
            effective_limit = min(int(limit), int(max_page_size))
            if after is not None:
                # Caller passed a non-None cursor — for the default impl
                # we just slice from row[limit:] to simulate "page 2".
                rows = rows.iloc[effective_limit:]
            page = rows.iloc[:effective_limit]
            if len(rows) > effective_limit:
                cursor = "OPAQUE_CURSOR_TOKEN"
            else:
                cursor = None
            items = []
            for _, row in page.iterrows():
                items.append(
                    {
                        "signal_id": str(row.get("signal_id", "SIG-001")),
                        "event_time": (
                            row["event_time"].isoformat()
                            if hasattr(row["event_time"], "isoformat")
                            else str(row["event_time"])
                        ),
                        "event_code": int(row["event_code"]),
                        "event_param": int(row["event_param"]),
                    }
                )
            return items, cursor

        mock_paginate.side_effect = _default_paginate

        monkeypatch.setattr(
            "tsigma.api.v1.signals.fetch_events", mock_fetch, raising=False
        )
        monkeypatch.setattr(
            "tsigma.api.v1.signals.paginated_event_list",
            mock_paginate,
            raising=False,
        )
        monkeypatch.setattr(
            "tsigma.api.v1.signals.require_max_lookback",
            mock_lookback,
            raising=False,
        )
        monkeypatch.setattr(
            "tsigma.api.v1.signals.require_max_aggregation_days",
            mock_aggregation,
            raising=False,
        )

        # settings_service.get_int — used by guards and paginate via their
        # own module paths. We patch the canonical module so any look-up
        # path sees our return values.
        async def _get_int(key, session=None):
            mapping = {
                "api.max_page_size": max_page_size,
                "api.max_lookback_days": max_lookback_days,
                "api.max_aggregation_days": max_aggregation_days,
            }
            return mapping.get(key, 1000)

        mock_get_int = AsyncMock(side_effect=_get_int)
        monkeypatch.setattr(
            "tsigma.settings_service.get_int", mock_get_int, raising=False
        )

        return {
            "mock_session": mock_session,
            "mock_fetch": mock_fetch,
            "mock_paginate": mock_paginate,
            "mock_lookback": mock_lookback,
            "mock_aggregation": mock_aggregation,
            "mock_get_int": mock_get_int,
        }

    @staticmethod
    def _make_df(n, *, signal_id="SIG-001", base_time=None, event_code=82):
        """Build a DataFrame of n events ordered by event_time."""
        if base_time is None:
            base_time = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
        return pd.DataFrame(
            {
                "event_time": [base_time + timedelta(seconds=i) for i in range(n)],
                "event_code": [event_code] * n,
                "event_param": list(range(n)),
            }
        )

    # ------------------------------------------------------------------
    # Modified existing tests — now expect envelope shape + fetch_events
    # ------------------------------------------------------------------

    def test_returns_events_for_window(self, monkeypatch):
        """B12.2: response shape is the {items, next_cursor} envelope, and
        event rows come from fetch_events (not session.execute)."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = pd.DataFrame(
            {
                "event_time": [
                    datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc),
                    datetime(2026, 4, 29, 12, 0, 1, tzinfo=timezone.utc),
                ],
                "event_code": [82, 82],
                "event_param": [1, 2],
            }
        )
        self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, dict), "response must be the envelope dict"
        assert set(body.keys()) == {"items", "next_cursor"}
        items = body["items"]
        assert len(items) == 2
        assert items[0]["signal_id"] == "SIG-001"
        assert items[0]["event_code"] == 82
        assert {row["event_param"] for row in items} == {1, 2}

    def test_event_codes_csv_filter(self, monkeypatch):
        """event_codes CSV is parsed and forwarded to fetch_events as a list.

        Post-B12.2 there is only ONE session.execute call (signal lookup);
        the event read is fetch_events. await_count must therefore be 1, and
        fetch_events must have been called once with the parsed code list.
        """
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._make_df(1)
        mocks = self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "event_codes": "1, 82, 9",
            },
        )
        assert resp.status_code == 200
        # Only one execute call now: signal existence lookup.
        assert mocks["mock_session"].execute.await_count == 1
        # fetch_events must be called once with parsed codes.
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("event_codes") == [1, 82, 9]

    def test_event_codes_malformed_returns_400(self, monkeypatch):
        """Malformed event_codes still rejected before any SDK call."""
        app = _create_test_app()
        _add_access_overrides(app)
        self._wire_v2(app, monkeypatch, signal_lookup_returns="SIG-001")

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "event_codes": "1,not-a-number,9",
            },
        )
        assert resp.status_code == 400
        assert "comma-separated list of integers" in resp.json()["detail"]

    def test_end_before_start_returns_400(self, monkeypatch):
        """end < start guard runs early; unchanged from pre-B12.2."""
        app = _create_test_app()
        _add_access_overrides(app)
        self._wire_v2(app, monkeypatch, signal_lookup_returns=None)

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T12:00:00+00:00",
                "end": "2026-04-29T11:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "end must be greater" in resp.json()["detail"]

    def test_unknown_signal_returns_404(self, monkeypatch):
        """Signal-existence lookup unchanged from pre-B12.2."""
        app = _create_test_app()
        _add_access_overrides(app)
        self._wire_v2(app, monkeypatch, signal_lookup_returns=None)

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/UNKNOWN/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
            },
        )
        assert resp.status_code == 404

    def test_limit_above_ceiling_rejected(self, monkeypatch):
        """FastAPI Query(le=100000) still rejects out-of-range limit."""
        app = _create_test_app()
        _add_access_overrides(app)
        self._wire_v2(app, monkeypatch, signal_lookup_returns=None)

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "limit": 999_999_999,
            },
        )
        assert resp.status_code == 422  # FastAPI Query validation

    def test_event_param_filter_passes_through(self, monkeypatch):
        """event_param is forwarded to fetch_events as event_param_in=[value]."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = pd.DataFrame(
            {
                "event_time": [datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)],
                "event_code": [82],
                "event_param": [42],
            }
        )
        mocks = self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "event_param": 42,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["items"][0]["event_param"] == 42
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("event_param_in") == [42]

    # ------------------------------------------------------------------
    # NEW B12.2 tests (must FAIL against current impl)
    # ------------------------------------------------------------------

    def test_returns_pagination_envelope(self, monkeypatch):
        """Response body must be {items: [...], next_cursor: str|None}."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._make_df(3)
        self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, dict), (
            f"expected envelope dict, got {type(body).__name__}: {body!r}"
        )
        assert "items" in body
        assert "next_cursor" in body
        assert isinstance(body["items"], list)
        assert body["next_cursor"] is None or isinstance(body["next_cursor"], str)

    def test_first_page_returns_cursor_when_more_remain(self, monkeypatch):
        """25-row DataFrame, limit=10 → 10 items + non-None cursor."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._make_df(25)
        self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "limit": 10,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["items"]) == 10
        assert body["next_cursor"] is not None
        assert isinstance(body["next_cursor"], str)
        assert body["next_cursor"] != ""

    def test_last_page_returns_null_cursor(self, monkeypatch):
        """5-row DataFrame, limit=10 → 5 items, cursor is null."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._make_df(5)
        self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "limit": 10,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["items"]) == 5
        assert body["next_cursor"] is None

    def test_after_cursor_continues_from_prior_position(self, monkeypatch):
        """Two sequential calls with the cursor return non-overlapping pages
        and the second handler invocation forwards after=<cursor> to
        paginated_event_list."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._make_df(25)
        mocks = self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        client = TestClient(app)
        resp1 = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "limit": 10,
            },
        )
        assert resp1.status_code == 200
        body1 = resp1.json()
        cursor = body1["next_cursor"]
        assert cursor is not None

        resp2 = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "limit": 10,
                "after": cursor,
            },
        )
        assert resp2.status_code == 200
        body2 = resp2.json()

        # Distinct event_params on the two pages (page1=0..9, page2=10..19).
        page1_params = {it["event_param"] for it in body1["items"]}
        page2_params = {it["event_param"] for it in body2["items"]}
        assert page1_params.isdisjoint(page2_params)

        # Verify the handler forwarded after=<cursor> on the second call.
        call_kwargs_list = [
            call.kwargs for call in mocks["mock_paginate"].await_args_list
        ]
        assert any(ck.get("after") == cursor for ck in call_kwargs_list), (
            f"paginate calls did not include after={cursor!r}: "
            f"{call_kwargs_list!r}"
        )

    def test_limit_clamps_to_api_max_page_size(self, monkeypatch):
        """api.max_page_size=5 must clamp limit=100 → 5 items in response."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._make_df(50)
        self._wire_v2(
            app,
            monkeypatch,
            signal_lookup_returns="SIG-001",
            events_df=df,
            max_page_size=5,
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "limit": 100,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["items"]) <= 5

    def test_invalid_cursor_returns_400(self, monkeypatch):
        """An after value that is not a valid base64 cursor → HTTP 400."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._make_df(10)
        mocks = self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        # Override paginate to raise the same HTTPException(400) the real
        # helper raises on malformed cursors.
        from fastapi import HTTPException

        async def _raise_invalid(rows, *, after, limit, session):
            raise HTTPException(
                status_code=400, detail="invalid pagination cursor: bad token"
            )

        mocks["mock_paginate"].side_effect = _raise_invalid

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "after": "not-a-valid-base64-cursor",
            },
        )
        assert resp.status_code == 400
        assert "invalid pagination cursor" in resp.json()["detail"]

    def test_start_predates_max_lookback_returns_400(self, monkeypatch):
        """api.max_lookback_days=7 with start 30 days ago → 400."""
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = self._wire_v2(
            app,
            monkeypatch,
            signal_lookup_returns="SIG-001",
            max_lookback_days=7,
        )
        # Wire the lookback guard to raise as the real helper would.
        from fastapi import HTTPException

        async def _raise_lookback(start, *, session):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"start={start.isoformat()} predates the "
                    f"api.max_lookback_days ceiling (7 days)"
                ),
            )

        mocks["mock_lookback"].side_effect = _raise_lookback

        now = datetime.now(tz=timezone.utc)
        old_start = (now - timedelta(days=30)).isoformat()
        old_end = (now - timedelta(days=29)).isoformat()

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": old_start,
                "end": old_end,
            },
        )
        assert resp.status_code == 400
        assert "api.max_lookback_days" in resp.json()["detail"]

    def test_window_exceeds_max_aggregation_days_returns_400(self, monkeypatch):
        """api.max_aggregation_days=7 with a 30-day window → 400."""
        app = _create_test_app()
        _add_access_overrides(app)
        mocks = self._wire_v2(
            app,
            monkeypatch,
            signal_lookup_returns="SIG-001",
            max_aggregation_days=7,
        )

        from fastapi import HTTPException

        async def _raise_aggregation(start, end, *, session):
            window = (end - start).days
            raise HTTPException(
                status_code=400,
                detail=(
                    f"requested window ({window} days) exceeds "
                    f"api.max_aggregation_days (7)"
                ),
            )

        mocks["mock_aggregation"].side_effect = _raise_aggregation

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-05-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 400
        assert "api.max_aggregation_days" in resp.json()["detail"]

    def test_handler_calls_tier_aware_fetch_events(self, monkeypatch):
        """Handler must call fetch_events (not session.execute) for events,
        forwarding signal_id, start, end, event_codes, event_param_in."""
        app = _create_test_app()
        _add_access_overrides(app)
        df = self._make_df(2)
        mocks = self._wire_v2(
            app, monkeypatch, signal_lookup_returns="SIG-001", events_df=df
        )

        client = TestClient(app)
        resp = client.get(
            "/api/v1/signals/SIG-001/events",
            params={
                "start": "2026-04-29T00:00:00+00:00",
                "end": "2026-04-29T23:59:59+00:00",
                "event_codes": "1,82",
                "event_param": 5,
            },
        )
        assert resp.status_code == 200

        # fetch_events must have been called exactly once.
        mocks["mock_fetch"].assert_awaited_once()
        kwargs = mocks["mock_fetch"].await_args.kwargs
        assert kwargs.get("signal_id") == "SIG-001"
        assert kwargs.get("event_codes") == [1, 82]
        assert kwargs.get("event_param_in") == [5]
        start_arg = kwargs.get("start")
        end_arg = kwargs.get("end")
        assert start_arg is not None and end_arg is not None
        assert end_arg >= start_arg

        # Only ONE session.execute call (signal lookup) — not the event read.
        assert mocks["mock_session"].execute.await_count == 1
