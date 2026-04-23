"""
Unit tests for detectors API endpoints.

Tests list, get, and create operations for detector configuration.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.detectors import router
from tsigma.auth.sessions import SessionData
from tsigma.models import Approach, Detector


def _create_test_app():
    """Create a minimal FastAPI app with the detectors router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
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


def _mock_detector(**overrides):
    """Create a mock Detector ORM object."""
    now = datetime.now(timezone.utc)
    defaults = {
        "detector_id": uuid4(),
        "approach_id": uuid4(),
        "detector_channel": 5,
        "distance_from_stop_bar": 300,
        "min_speed_filter": None,
        "decision_point": None,
        "movement_delay": None,
        "lane_number": 1,
        "lane_type_id": None,
        "movement_type_id": None,
        "detection_hardware_id": None,
        "lat_lon_distance": None,
        "created_at": now,
        "updated_at": now,
    }
    defaults.update(overrides)
    mock = MagicMock(spec=Detector)
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


def _mock_approach():
    """Create a mock Approach ORM object."""
    mock = MagicMock(spec=Approach)
    mock.approach_id = uuid4()
    return mock


class TestListDetectors:
    """Tests for GET /api/v1/approaches/{approach_id}/detectors."""

    def test_returns_detectors(self):
        """Test listing detectors for an approach."""
        approach = _mock_approach()
        detector = _mock_detector(approach_id=approach.approach_id)
        mock_session = AsyncMock()
        approach_result = MagicMock()
        approach_result.scalar_one_or_none.return_value = approach
        detector_result = MagicMock()
        detector_result.scalars.return_value.all.return_value = [detector]
        mock_session.execute = AsyncMock(side_effect=[approach_result, detector_result])

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(f"/api/v1/approaches/{approach.approach_id}/detectors")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["detector_channel"] == 5

    def test_approach_not_found(self):
        """Test 404 when approach doesn't exist."""
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
        resp = client.get(f"/api/v1/approaches/{uuid4()}/detectors")
        assert resp.status_code == 404


class TestGetDetector:
    """Tests for GET /api/v1/detectors/{detector_id}."""

    def test_returns_detector(self):
        """Test getting a detector by ID."""
        detector = _mock_detector()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = detector
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()
        _add_access_overrides(app)

        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session

        client = TestClient(app)
        resp = client.get(f"/api/v1/detectors/{detector.detector_id}")
        assert resp.status_code == 200
        assert resp.json()["detector_channel"] == 5

    def test_not_found(self):
        """Test 404 when detector doesn't exist."""
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
        resp = client.get(f"/api/v1/detectors/{uuid4()}")
        assert resp.status_code == 404


class TestCreateDetector:
    """Tests for POST /api/v1/approaches/{approach_id}/detectors."""

    def test_creates_detector(self):
        """Test creating a detector under an approach."""
        approach = _mock_approach()
        detector = _mock_detector(approach_id=approach.approach_id)
        mock_session = AsyncMock()
        approach_result = MagicMock()
        approach_result.scalar_one_or_none.return_value = approach
        mock_session.execute = AsyncMock(return_value=approach_result)
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        with patch("tsigma.api.v1.detectors.Detector", return_value=detector):
            client = TestClient(app)
            resp = client.post(
                f"/api/v1/approaches/{approach.approach_id}/detectors",
                json={"detector_channel": 5, "distance_from_stop_bar": 300},
            )

        assert resp.status_code == 201

    def test_approach_not_found(self):
        """Test 404 when parent approach doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.post(
            f"/api/v1/approaches/{uuid4()}/detectors",
            json={"detector_channel": 5},
        )
        assert resp.status_code == 404

    def test_validation_error(self):
        """Test 422 on invalid body."""
        mock_session = AsyncMock()

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.post(
            f"/api/v1/approaches/{uuid4()}/detectors",
            json={},
        )
        assert resp.status_code == 422


class TestUpdateDetector:
    """Tests for PUT /api/v1/detectors/{detector_id}."""

    def test_updates_detector(self):
        """Test updating a detector returns 200."""
        detector = _mock_detector()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = detector
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/detectors/{detector.detector_id}",
            json={"detector_channel": 10},
        )
        assert resp.status_code == 200
        assert detector.detector_channel == 10

    def test_partial_update(self):
        """Test partial update only changes provided fields."""
        detector = _mock_detector(detector_channel=5, distance_from_stop_bar=300)
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = detector
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/detectors/{detector.detector_id}",
            json={"detector_channel": 8},
        )
        assert resp.status_code == 200
        assert detector.detector_channel == 8
        assert detector.distance_from_stop_bar == 300

    def test_not_found(self):
        """Test 404 when detector doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/detectors/{uuid4()}",
            json={"detector_channel": 10},
        )
        assert resp.status_code == 404

    def test_empty_body_returns_422(self):
        """Test 422 when no fields provided for update."""
        mock_session = AsyncMock()

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/detectors/{uuid4()}",
            json={},
        )
        assert resp.status_code == 422


class TestDeleteDetector:
    """Tests for DELETE /api/v1/detectors/{detector_id}."""

    def test_deletes_detector(self):
        """Test deleting a detector returns 204."""
        detector = _mock_detector()
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = detector
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.delete(f"/api/v1/detectors/{detector.detector_id}")
        assert resp.status_code == 204
        mock_session.delete.assert_awaited_once_with(detector)

    def test_not_found(self):
        """Test 404 when detector doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[require_admin] = lambda: SessionData(
            user_id=str(uuid4()), username="admin", role="admin",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

        client = TestClient(app)
        resp = client.delete(f"/api/v1/detectors/{uuid4()}")
        assert resp.status_code == 404
