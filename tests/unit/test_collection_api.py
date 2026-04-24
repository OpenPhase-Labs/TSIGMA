"""
Unit tests for collection API endpoints.

Tests SOAP envelope parsing and error handling.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.collection import _parse_soap_envelope, router
from tsigma.auth.sessions import SessionData

# SOAP namespace constants (match collection.py)
_SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
_TEMPURI_NS = "http://tempuri.org/"


def _valid_soap_envelope(
    signal_id="SIG-001",
    ip_address="10.0.0.1",
    username="admin",
    password="secret",
    remote_dir="/logs",
    active_mode="false",
):
    """Build a valid SOAP envelope for UploadControllerData."""
    return (
        f'<?xml version="1.0" encoding="utf-8"?>'
        f'<s:Envelope xmlns:s="{_SOAP_NS}">'
        f"<s:Body>"
        f'<UploadControllerData xmlns="{_TEMPURI_NS}">'
        f"<SignalID>{signal_id}</SignalID>"
        f"<IPAddress>{ip_address}</IPAddress>"
        f"<UserName>{username}</UserName>"
        f"<Password>{password}</Password>"
        f"<RemoteDir>{remote_dir}</RemoteDir>"
        f"<ActiveMode>{active_mode}</ActiveMode>"
        f"</UploadControllerData>"
        f"</s:Body>"
        f"</s:Envelope>"
    )


def _create_test_app():
    """Create a minimal FastAPI app with the collection router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return app


def _add_admin_overrides(app):
    """Override admin dependency for protected endpoints."""
    from tsigma.auth.dependencies import require_admin

    app.dependency_overrides[require_admin] = lambda: SessionData(
        user_id=str(uuid4()),
        username="admin",
        role="admin",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


class TestParseSoapEnvelope:
    """Direct tests for _parse_soap_envelope helper."""

    def test_soap_envelope_parses_parameters(self):
        """Valid SOAP XML extracts all UploadControllerData parameters."""
        xml = _valid_soap_envelope(
            signal_id="SIG-042",
            ip_address="192.168.1.10",
            username="ftpuser",
            password="p@ssw0rd",
            remote_dir="/data/logs",
            active_mode="true",
        )
        params = _parse_soap_envelope(xml.encode("utf-8"))

        assert params["SignalID"] == "SIG-042"
        assert params["IPAddress"] == "192.168.1.10"
        assert params["UserName"] == "ftpuser"
        assert params["Password"] == "p@ssw0rd"
        assert params["RemoteDir"] == "/data/logs"
        assert params["ActiveMode"] == "true"

    def test_soap_envelope_invalid_xml(self):
        """Garbage bytes raise HTTPException with 400 status."""
        from fastapi import HTTPException

        try:
            _parse_soap_envelope(b"this is not xml at all <><>!!!")
            assert False, "Should have raised HTTPException"
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Malformed XML" in exc.detail

    def test_soap_envelope_missing_body(self):
        """SOAP envelope without <Body> raises 400."""
        from fastapi import HTTPException

        xml = (
            f'<s:Envelope xmlns:s="{_SOAP_NS}">'
            f"<s:Header/>"
            f"</s:Envelope>"
        )
        try:
            _parse_soap_envelope(xml.encode("utf-8"))
            assert False, "Should have raised HTTPException"
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Missing SOAP Body" in exc.detail

    def test_soap_envelope_missing_upload_element(self):
        """SOAP Body without UploadControllerData raises 400."""
        from fastapi import HTTPException

        xml = (
            f'<s:Envelope xmlns:s="{_SOAP_NS}">'
            f"<s:Body>"
            f"<SomeOtherElement/>"
            f"</s:Body>"
            f"</s:Envelope>"
        )
        try:
            _parse_soap_envelope(xml.encode("utf-8"))
            assert False, "Should have raised HTTPException"
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Missing UploadControllerData" in exc.detail

    def test_soap_envelope_empty_elements(self):
        """Empty child elements yield empty strings."""
        xml = (
            f'<s:Envelope xmlns:s="{_SOAP_NS}">'
            f"<s:Body>"
            f'<UploadControllerData xmlns="{_TEMPURI_NS}">'
            f"<SignalID></SignalID>"
            f"<IPAddress/>"
            f"</UploadControllerData>"
            f"</s:Body>"
            f"</s:Envelope>"
        )
        params = _parse_soap_envelope(xml.encode("utf-8"))
        assert params["SignalID"] == ""
        assert params["IPAddress"] == ""


class TestSoapEndpoint:
    """Tests for POST /api/v1/soap/GetControllerData."""

    def test_soap_endpoint_parses_envelope(self):
        """POST /soap/GetControllerData with valid SOAP XML extracts parameters and returns accepted."""
        xml_body = _valid_soap_envelope(signal_id="SIG-100", ip_address="10.0.0.5")

        app = _create_test_app()
        _add_admin_overrides(app)

        # Mock the collector on app state
        mock_method = MagicMock()
        mock_method.poll_once = AsyncMock()
        mock_collector = MagicMock()
        mock_collector.get_method.return_value = mock_method
        mock_collector.session_factory = MagicMock()
        app.state.collector = mock_collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/soap/GetControllerData",
            content=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
        )

        assert resp.status_code == 200
        assert "Accepted" in resp.text
        assert "SIG-100" in resp.text

    def test_soap_endpoint_invalid_xml(self):
        """POST /soap/GetControllerData with garbage returns 400 error."""
        app = _create_test_app()
        _add_admin_overrides(app)

        # Collector must exist for the endpoint to proceed before parsing
        mock_collector = MagicMock()
        app.state.collector = mock_collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/soap/GetControllerData",
            content=b"NOT VALID XML {{{}}}",
            headers={"Content-Type": "text/xml"},
        )

        assert resp.status_code == 400

    def test_soap_endpoint_missing_signal_id(self):
        """POST /soap/GetControllerData with empty SignalID returns 400 error."""
        xml_body = _valid_soap_envelope(signal_id="")

        app = _create_test_app()
        _add_admin_overrides(app)

        mock_collector = MagicMock()
        app.state.collector = mock_collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/soap/GetControllerData",
            content=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
        )

        assert resp.status_code == 400
        assert "missing SignalID" in resp.text

    def test_soap_endpoint_no_collector_returns_503(self):
        """POST /soap/GetControllerData without collector returns 503."""
        xml_body = _valid_soap_envelope()

        app = _create_test_app()
        _add_admin_overrides(app)
        # Do NOT set app.state.collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/soap/GetControllerData",
            content=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
        )

        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# REST poll trigger endpoint tests
# ---------------------------------------------------------------------------


class TestTriggerPoll:
    """Tests for POST /signals/{signal_id}/poll."""

    def _make_app_with_overrides(self):
        """Create app with auth and session overrides for poll endpoint."""
        from tsigma.auth.dependencies import require_admin

        app = _create_test_app()
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()),
            username="admin",
            role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
        )
        return app

    def test_poll_signal_triggers_poll(self):
        """POST /signals/{id}/poll with valid signal returns 202."""
        from tsigma.dependencies import get_session as dep_get_session

        app = self._make_app_with_overrides()

        # Mock DB session that returns a signal
        fake_signal = MagicMock()
        fake_signal.signal_id = "SIG-001"
        fake_signal.ip_address = "10.0.0.1"
        fake_signal.signal_metadata = {"collection": {"username": "admin"}}

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = fake_signal
        mock_session.execute = AsyncMock(return_value=mock_result)

        async def override_session():
            yield mock_session

        app.dependency_overrides[dep_get_session] = override_session

        # Mock collector
        mock_method = MagicMock()
        mock_method.poll_once = AsyncMock()
        mock_collector = MagicMock()
        mock_collector.get_method.return_value = mock_method
        mock_collector.session_factory = MagicMock()
        app.state.collector = mock_collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/signals/SIG-001/poll",
            json={"method": "ftp_pull"},
        )

        assert resp.status_code == 202
        data = resp.json()
        assert data["signal_id"] == "SIG-001"
        assert data["method"] == "ftp_pull"
        assert data["status"] == "started"

    def test_poll_signal_not_found(self):
        """POST /signals/{id}/poll returns 404 for unknown signal."""
        from tsigma.dependencies import get_session as dep_get_session

        app = self._make_app_with_overrides()

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        async def override_session():
            yield mock_session

        app.dependency_overrides[dep_get_session] = override_session

        mock_collector = MagicMock()
        app.state.collector = mock_collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/signals/NOPE/poll",
            json={"method": "ftp_pull"},
        )

        assert resp.status_code == 404
        assert "NOPE" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Checkpoint listing endpoint tests
# ---------------------------------------------------------------------------


def _mock_scalars_result(items):
    """Build a mock execute result whose .scalars().all() returns items."""
    result = MagicMock()
    scalars = MagicMock()
    scalars.all.return_value = items
    result.scalars.return_value = scalars
    return result


class TestListCheckpoints:
    """Tests for GET /checkpoints/ (direct function call)."""

    def test_list_checkpoints_returns_list(self):
        """list_checkpoints returns checkpoint dicts."""
        import asyncio

        from tsigma.api.v1.collection import list_checkpoints

        fake_cp = MagicMock()
        fake_cp.device_type = "controller"
        fake_cp.device_id = "SIG-001"
        fake_cp.method = "ftp_pull"
        fake_cp.last_filename = "data_001.dat"
        fake_cp.last_file_mtime = None
        fake_cp.last_event_timestamp = None
        fake_cp.last_successful_poll = None
        fake_cp.events_ingested = 100
        fake_cp.files_ingested = 5
        fake_cp.consecutive_errors = 0
        fake_cp.last_error = None
        fake_cp.last_error_time = None

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(
            return_value=_mock_scalars_result([fake_cp]),
        )

        result = asyncio.run(
            list_checkpoints(method=None, session=mock_session, _access=None)
        )

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["device_type"] == "controller"
        assert result[0]["device_id"] == "SIG-001"
        assert result[0]["events_ingested"] == 100

    def test_list_checkpoints_filters_by_method(self):
        """list_checkpoints passes method filter to the query."""
        import asyncio

        from tsigma.api.v1.collection import list_checkpoints

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(
            return_value=_mock_scalars_result([]),
        )

        result = asyncio.run(
            list_checkpoints(method="ftp_pull", session=mock_session, _access=None)
        )

        assert result == []
        mock_session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# SOAP rate limiting (line 68: rate limiter returns False, lines 284-285)
# ---------------------------------------------------------------------------


class TestSoapRateLimiting:
    """Tests for SOAP endpoint rate limiting (lines 283-289)."""

    def test_soap_rate_limited(self):
        """SOAP endpoint returns 429 when rate limited."""
        app = _create_test_app()
        _add_admin_overrides(app)

        mock_method = MagicMock()
        mock_method.poll_once = AsyncMock()
        mock_collector = MagicMock()
        mock_collector.get_method.return_value = mock_method
        mock_collector.session_factory = MagicMock()
        app.state.collector = mock_collector

        from tsigma.api.v1.collection import _poll_limiter

        # Patch the rate limiter to always deny
        with patch.object(_poll_limiter, "check", return_value=False):
            client = TestClient(app)
            xml_body = _valid_soap_envelope(signal_id="SIG-RATE")
            resp = client.post(
                "/api/v1/soap/GetControllerData",
                content=xml_body.encode("utf-8"),
                headers={"Content-Type": "text/xml"},
            )

        assert resp.status_code == 429
        assert "rate limited" in resp.text


# ---------------------------------------------------------------------------
# SOAP ignored parameter logging (line 296)
# ---------------------------------------------------------------------------


class TestSoapIgnoredParams:
    """Tests for SOAP ignored parameter logging (line 296)."""

    def test_soap_logs_ignored_params(self):
        """SOAP endpoint logs ignored ATSPM 4.x parameters."""
        xml_body = (
            f'<?xml version="1.0" encoding="utf-8"?>'
            f'<s:Envelope xmlns:s="{_SOAP_NS}">'
            f"<s:Body>"
            f'<UploadControllerData xmlns="{_TEMPURI_NS}">'
            f"<SignalID>SIG-IGN</SignalID>"
            f"<IPAddress>10.0.0.1</IPAddress>"
            f"<DeleteFiles>true</DeleteFiles>"
            f"<SNMPRetry>3</SNMPRetry>"
            f"</UploadControllerData>"
            f"</s:Body>"
            f"</s:Envelope>"
        )

        app = _create_test_app()
        _add_admin_overrides(app)

        mock_method = MagicMock()
        mock_method.poll_once = AsyncMock()
        mock_collector = MagicMock()
        mock_collector.get_method.return_value = mock_method
        mock_collector.session_factory = MagicMock()
        app.state.collector = mock_collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/soap/GetControllerData",
            content=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
        )

        # Should still succeed despite ignored params
        assert resp.status_code == 200
        assert "Accepted" in resp.text


# ---------------------------------------------------------------------------
# SOAP ftp_pull method not available (lines 311-312)
# ---------------------------------------------------------------------------


class TestSoapMethodNotAvailable:
    """Tests for SOAP endpoint when ftp_pull method is not registered (lines 311-312)."""

    def test_soap_ftp_pull_not_available(self):
        """SOAP endpoint returns 503 when ftp_pull method is not registered."""
        xml_body = _valid_soap_envelope(signal_id="SIG-NO-FTP")

        app = _create_test_app()
        _add_admin_overrides(app)

        mock_collector = MagicMock()
        mock_collector.get_method.side_effect = ValueError("ftp_pull not registered")
        app.state.collector = mock_collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/soap/GetControllerData",
            content=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
        )

        assert resp.status_code == 503
        assert "ftp_pull method not available" in resp.text


# ---------------------------------------------------------------------------
# REST poll rate limiting (lines 370-374)
# ---------------------------------------------------------------------------


class TestRestPollRateLimiting:
    """Tests for REST poll endpoint rate limiting (lines 370-374)."""

    def test_rest_poll_rate_limited(self):
        """REST poll endpoint returns 429 when rate limited."""
        from tsigma.api.v1.collection import _poll_limiter
        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session as dep_get_session

        app = _create_test_app()
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()),
            username="admin",
            role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
        )

        mock_session = AsyncMock()
        async def override_session():
            yield mock_session
        app.dependency_overrides[dep_get_session] = override_session

        mock_collector = MagicMock()
        app.state.collector = mock_collector

        with patch.object(_poll_limiter, "check", return_value=False):
            client = TestClient(app)
            resp = client.post(
                "/api/v1/signals/SIG-RATE/poll",
                json={"method": "ftp_pull"},
            )

        assert resp.status_code == 429
        assert "Rate limited" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# REST poll unknown method (lines 391-392)
# ---------------------------------------------------------------------------


class TestRestPollUnknownMethod:
    """Tests for REST poll endpoint with unknown method (lines 391-392)."""

    def test_rest_poll_unknown_method(self):
        """REST poll endpoint returns 400 for unknown polling method."""
        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session as dep_get_session

        app = _create_test_app()
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()),
            username="admin",
            role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
        )

        fake_signal = MagicMock()
        fake_signal.signal_id = "SIG-001"
        fake_signal.ip_address = "10.0.0.1"
        fake_signal.signal_metadata = {}

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = fake_signal
        mock_session.execute = AsyncMock(return_value=mock_result)

        async def override_session():
            yield mock_session
        app.dependency_overrides[dep_get_session] = override_session

        mock_collector = MagicMock()
        mock_collector.get_method.side_effect = ValueError("unknown")
        app.state.collector = mock_collector

        client = TestClient(app)
        resp = client.post(
            "/api/v1/signals/SIG-001/poll",
            json={"method": "nonexistent"},
        )

        assert resp.status_code == 400
        assert "Unknown polling method" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Checkpoint per-signal endpoint (lines 498-505)
# ---------------------------------------------------------------------------


class TestGetSignalCheckpoints:
    """Tests for GET /checkpoints/{signal_id} (lines 498-505)."""

    def test_get_signal_checkpoints(self):
        """get_signal_checkpoints returns checkpoints for a specific signal."""
        import asyncio

        from tsigma.api.v1.collection import get_signal_checkpoints

        fake_cp = MagicMock()
        fake_cp.device_type = "controller"
        fake_cp.device_id = "SIG-042"
        fake_cp.method = "ftp_pull"
        fake_cp.last_filename = "data_042.dat"
        fake_cp.last_file_mtime = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        fake_cp.last_event_timestamp = datetime(2024, 1, 15, 11, 59, tzinfo=timezone.utc)
        fake_cp.last_successful_poll = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        fake_cp.events_ingested = 500
        fake_cp.files_ingested = 10
        fake_cp.consecutive_errors = 0
        fake_cp.last_error = None
        fake_cp.last_error_time = None

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(
            return_value=_mock_scalars_result([fake_cp]),
        )

        result = asyncio.run(
            get_signal_checkpoints(signal_id="SIG-042", session=mock_session, _access=None)
        )

        assert len(result) == 1
        assert result[0]["device_type"] == "controller"
        assert result[0]["device_id"] == "SIG-042"
        assert result[0]["events_ingested"] == 500
        assert result[0]["last_filename"] == "data_042.dat"
        assert result[0]["last_file_mtime"] is not None

    def test_get_signal_checkpoints_empty(self):
        """get_signal_checkpoints returns empty list for unknown signal."""
        import asyncio

        from tsigma.api.v1.collection import get_signal_checkpoints

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(
            return_value=_mock_scalars_result([]),
        )

        result = asyncio.run(
            get_signal_checkpoints(signal_id="MISSING", session=mock_session, _access=None)
        )

        assert result == []


# ---------------------------------------------------------------------------
# Bulk timestamp correction (lines 561-586)
# ---------------------------------------------------------------------------


class TestBulkTimestampCorrection:
    """Tests for POST /corrections/bulk (lines 561-586)."""

    def test_bulk_correction(self):
        """bulk_timestamp_correction updates rows and returns summary."""
        import asyncio

        from tsigma.api.v1.collection import BulkTimestampCorrectionRequest, bulk_timestamp_correction

        body = BulkTimestampCorrectionRequest(
            signal_id="SIG-001",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            offset_seconds=-3600.0,
        )

        mock_result = MagicMock()
        mock_result.rowcount = 42

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()

        result = asyncio.run(bulk_timestamp_correction(body=body, session=mock_session))

        assert result["signal_id"] == "SIG-001"
        assert result["rows_updated"] == 42
        assert result["offset_seconds"] == -3600.0
        mock_session.commit.assert_awaited_once()


# ---------------------------------------------------------------------------
# Anchor timestamp correction (lines 617-645)
# ---------------------------------------------------------------------------


class TestAnchorTimestampCorrection:
    """Tests for POST /corrections/anchor (lines 617-645)."""

    def test_anchor_correction(self):
        """anchor_timestamp_correction computes offset and updates rows."""
        import asyncio

        from tsigma.api.v1.collection import AnchorCorrectionRequest, anchor_timestamp_correction

        body = AnchorCorrectionRequest(
            signal_id="SIG-002",
            event_time=datetime(2024, 1, 15, 13, 0, tzinfo=timezone.utc),
            actual_time=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
            start_time=datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 16, 0, 0, tzinfo=timezone.utc),
        )

        mock_result = MagicMock()
        mock_result.rowcount = 100

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()

        result = asyncio.run(anchor_timestamp_correction(body=body, session=mock_session))

        assert result["signal_id"] == "SIG-002"
        assert result["rows_updated"] == 100
        assert result["computed_offset_seconds"] == -3600.0
        assert result["anchor_event_time"] is not None
        assert result["anchor_actual_time"] is not None
        mock_session.commit.assert_awaited_once()
