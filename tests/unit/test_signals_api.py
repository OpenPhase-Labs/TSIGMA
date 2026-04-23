"""
Unit tests for signals API endpoints.

Tests GET (redaction), POST (encryption), and PUT (encryption) operations.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

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
        mock_session = AsyncMock()
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
        mock_session = AsyncMock()
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
        mock_session = AsyncMock()
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

        mock_session = AsyncMock()
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

        mock_session = AsyncMock()
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
        mock_session = AsyncMock()
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
        mock_session = AsyncMock()
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

        mock_session = AsyncMock()
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
        mock_session = AsyncMock()
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
        mock_session = AsyncMock()
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
        mock_session = AsyncMock()
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

        mock_session = AsyncMock()

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
        mock_session = AsyncMock()
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
