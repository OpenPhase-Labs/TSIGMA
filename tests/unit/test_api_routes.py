"""
Unit tests for the Routes API endpoints.

Tests the custom nested endpoints (list/create) defined in
tsigma.api.v1.routes with mocked database sessions and
overridden auth dependencies.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.routes import (
    RouteCreate,
    RouteDistanceCreate,
    RouteDistanceUpdate,
    RoutePhaseCreate,
    RoutePhaseUpdate,
    RouteSignalCreate,
    RouteSignalUpdate,
    RouteUpdate,
    create_route_distance,
    create_route_phase,
    create_route_signal,
    list_route_distances,
    list_route_phases,
    list_route_signals,
    router,
)
from tsigma.auth.sessions import SessionData

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    """Run an async function synchronously."""
    return asyncio.run(coro)


def _mock_scalar_result(value):
    """Build a mock execute result whose .scalar_one_or_none() returns value."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def _mock_scalars_result(items):
    """Build a mock execute result whose .scalars().all() returns items."""
    result = MagicMock()
    scalars = MagicMock()
    scalars.all.return_value = items
    result.scalars.return_value = scalars
    return result


# ---------------------------------------------------------------------------
# list_route_signals
# ---------------------------------------------------------------------------

class TestListRouteSignals:
    """Tests for GET /routes/{route_id}/signals."""

    def test_returns_signals_for_valid_route(self):
        route_id = uuid4()
        session = AsyncMock()

        fake_route = MagicMock()
        fake_rs1 = MagicMock(
            route_signal_id=uuid4(),
            route_id=route_id,
            signal_id="SIG-001",
            sequence_order=1,
        )
        fake_rs2 = MagicMock(
            route_signal_id=uuid4(),
            route_id=route_id,
            signal_id="SIG-002",
            sequence_order=2,
        )

        # First call: route lookup; second call: signals query
        session.execute = AsyncMock(
            side_effect=[
                _mock_scalar_result(fake_route),
                _mock_scalars_result([fake_rs1, fake_rs2]),
            ]
        )

        result = _run(list_route_signals(route_id, session, _access=None))

        assert len(result) == 2
        assert result[0].signal_id == "SIG-001"
        assert result[1].signal_id == "SIG-002"
        assert session.execute.call_count == 2

    def test_raises_404_when_route_not_found(self):
        route_id = uuid4()
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=_mock_scalar_result(None),
        )

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            _run(list_route_signals(route_id, session, _access=None))

        assert exc_info.value.status_code == 404
        assert str(route_id) in exc_info.value.detail


# ---------------------------------------------------------------------------
# create_route_signal
# ---------------------------------------------------------------------------

class TestCreateRouteSignal:
    """Tests for POST /routes/{route_id}/signals."""

    def test_creates_signal_in_route(self):
        route_id = uuid4()
        session = AsyncMock()

        fake_route = MagicMock()
        fake_signal = MagicMock()

        body = RouteSignalCreate(signal_id="SIG-001", sequence_order=1)

        # First call: route lookup; second: signal lookup
        session.execute = AsyncMock(
            side_effect=[
                _mock_scalar_result(fake_route),
                _mock_scalar_result(fake_signal),
            ]
        )

        admin_user = MagicMock()

        with patch("tsigma.api.v1.routes.RouteSignal") as MockRS:
            mock_instance = MagicMock(
                route_signal_id=uuid4(),
                route_id=route_id,
                signal_id="SIG-001",
                sequence_order=1,
            )
            MockRS.return_value = mock_instance

            result = _run(
                create_route_signal(route_id, body, session, _=admin_user)
            )

            assert result is mock_instance
            session.add.assert_called_once_with(mock_instance)
            session.flush.assert_awaited_once()

    def test_raises_404_when_route_not_found(self):
        route_id = uuid4()
        session = AsyncMock()
        body = RouteSignalCreate(signal_id="SIG-001", sequence_order=1)

        session.execute = AsyncMock(
            return_value=_mock_scalar_result(None),
        )

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            _run(create_route_signal(route_id, body, session, _=MagicMock()))

        assert exc_info.value.status_code == 404
        assert "Route" in exc_info.value.detail

    def test_raises_404_when_signal_not_found(self):
        route_id = uuid4()
        session = AsyncMock()
        body = RouteSignalCreate(signal_id="NOPE", sequence_order=1)

        # Route found, signal not found
        session.execute = AsyncMock(
            side_effect=[
                _mock_scalar_result(MagicMock()),  # route exists
                _mock_scalar_result(None),          # signal missing
            ]
        )

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            _run(create_route_signal(route_id, body, session, _=MagicMock()))

        assert exc_info.value.status_code == 404
        assert "Signal" in exc_info.value.detail


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class TestRouteSchemas:
    """Tests for Pydantic schema validation."""

    def test_route_create_requires_name(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            RouteCreate(name="")

    def test_route_create_valid(self):
        r = RouteCreate(name="EB Progression")
        assert r.name == "EB Progression"

    def test_route_signal_create_rejects_zero_order(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            RouteSignalCreate(signal_id="SIG-001", sequence_order=0)

    def test_route_signal_create_valid(self):
        rs = RouteSignalCreate(signal_id="SIG-001", sequence_order=3)
        assert rs.sequence_order == 3


# ---------------------------------------------------------------------------
# list_route_phases / create_route_phase
# ---------------------------------------------------------------------------

class TestListRoutePhases:
    """Tests for GET /route-signals/{route_signal_id}/phases."""

    def test_returns_phases_for_valid_route_signal(self):
        route_signal_id = uuid4()
        session = AsyncMock()

        fake_rs = MagicMock()
        fake_phase1 = MagicMock(
            route_phase_id=uuid4(),
            route_signal_id=route_signal_id,
            phase_number=2,
            direction_type_id=1,
            is_overlap=False,
            is_primary_approach=True,
        )
        fake_phase2 = MagicMock(
            route_phase_id=uuid4(),
            route_signal_id=route_signal_id,
            phase_number=6,
            direction_type_id=2,
            is_overlap=False,
            is_primary_approach=False,
        )

        session.execute = AsyncMock(
            side_effect=[
                _mock_scalar_result(fake_rs),
                _mock_scalars_result([fake_phase1, fake_phase2]),
            ]
        )

        result = _run(list_route_phases(route_signal_id, session, _access=None))

        assert len(result) == 2
        assert result[0].phase_number == 2
        assert result[1].phase_number == 6

    def test_raises_404_when_route_signal_not_found(self):
        route_signal_id = uuid4()
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=_mock_scalar_result(None),
        )

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            _run(list_route_phases(route_signal_id, session, _access=None))

        assert exc_info.value.status_code == 404
        assert "RouteSignal" in exc_info.value.detail


class TestCreateRoutePhase:
    """Tests for POST /route-signals/{route_signal_id}/phases."""

    def test_creates_phase(self):
        route_signal_id = uuid4()
        session = AsyncMock()

        fake_rs = MagicMock()
        session.execute = AsyncMock(
            return_value=_mock_scalar_result(fake_rs),
        )

        body = RoutePhaseCreate(
            phase_number=2,
            direction_type_id=1,
            is_overlap=False,
            is_primary_approach=True,
        )

        with patch("tsigma.api.v1.routes.RoutePhase") as MockRP:
            mock_instance = MagicMock(
                route_phase_id=uuid4(),
                route_signal_id=route_signal_id,
                phase_number=2,
                direction_type_id=1,
                is_overlap=False,
                is_primary_approach=True,
            )
            MockRP.return_value = mock_instance

            result = _run(
                create_route_phase(route_signal_id, body, session, _=MagicMock())
            )

            assert result is mock_instance
            session.add.assert_called_once_with(mock_instance)
            session.flush.assert_awaited_once()

    def test_raises_404_when_route_signal_not_found(self):
        route_signal_id = uuid4()
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=_mock_scalar_result(None),
        )

        body = RoutePhaseCreate(
            phase_number=2, direction_type_id=1,
        )

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            _run(create_route_phase(route_signal_id, body, session, _=MagicMock()))

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# list_route_distances / create_route_distance
# ---------------------------------------------------------------------------

class TestListRouteDistances:
    """Tests for GET /routes/{route_id}/distances."""

    def test_returns_distances_for_valid_route(self):
        route_id = uuid4()
        session = AsyncMock()

        fake_route = MagicMock()
        fake_dist = MagicMock(
            route_distance_id=uuid4(),
            from_route_signal_id=uuid4(),
            to_route_signal_id=uuid4(),
            distance_feet=1200,
            travel_time_seconds=30,
        )

        session.execute = AsyncMock(
            side_effect=[
                _mock_scalar_result(fake_route),
                _mock_scalars_result([fake_dist]),
            ]
        )

        result = _run(list_route_distances(route_id, session, _access=None))

        assert len(result) == 1
        assert result[0].distance_feet == 1200

    def test_raises_404_when_route_not_found(self):
        route_id = uuid4()
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=_mock_scalar_result(None),
        )

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            _run(list_route_distances(route_id, session, _access=None))

        assert exc_info.value.status_code == 404


class TestCreateRouteDistance:
    """Tests for POST /route-distances/."""

    def test_creates_distance(self):
        from_rs_id = uuid4()
        to_rs_id = uuid4()
        session = AsyncMock()

        session.execute = AsyncMock(
            side_effect=[
                _mock_scalar_result(MagicMock()),  # from route signal exists
                _mock_scalar_result(MagicMock()),  # to route signal exists
            ]
        )

        body = RouteDistanceCreate(
            from_route_signal_id=from_rs_id,
            to_route_signal_id=to_rs_id,
            distance_feet=500,
            travel_time_seconds=15,
        )

        with patch("tsigma.api.v1.routes.RouteDistance") as MockRD:
            mock_instance = MagicMock(
                route_distance_id=uuid4(),
                from_route_signal_id=from_rs_id,
                to_route_signal_id=to_rs_id,
                distance_feet=500,
                travel_time_seconds=15,
            )
            MockRD.return_value = mock_instance

            result = _run(
                create_route_distance(body, session, _=MagicMock())
            )

            assert result is mock_instance
            session.add.assert_called_once_with(mock_instance)
            session.flush.assert_awaited_once()

    def test_raises_404_when_from_signal_not_found(self):
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=_mock_scalar_result(None),
        )

        body = RouteDistanceCreate(
            from_route_signal_id=uuid4(),
            to_route_signal_id=uuid4(),
            distance_feet=500,
        )

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            _run(create_route_distance(body, session, _=MagicMock()))

        assert exc_info.value.status_code == 404

    def test_raises_404_when_to_signal_not_found(self):
        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                _mock_scalar_result(MagicMock()),  # from exists
                _mock_scalar_result(None),          # to missing
            ]
        )

        body = RouteDistanceCreate(
            from_route_signal_id=uuid4(),
            to_route_signal_id=uuid4(),
            distance_feet=500,
        )

        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            _run(create_route_distance(body, session, _=MagicMock()))

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Route CRUD factory endpoints (lines 46-49, 82-85, 124-127, 168-171)
# ---------------------------------------------------------------------------


def _create_routes_app():
    """Create a minimal FastAPI app with the routes router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return app


def _add_route_auth_overrides(app):
    """Override auth dependencies for route tests."""
    from tsigma.auth.dependencies import _get_db_session, get_current_user_optional, require_admin

    app.dependency_overrides[require_admin] = lambda: SessionData(
        user_id=str(uuid4()),
        username="admin",
        role="admin",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )
    app.dependency_overrides[get_current_user_optional] = lambda: SessionData(
        user_id=uuid4(),
        username="viewer",
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


def _add_route_session_override(app, mock_session):
    from tsigma.dependencies import get_audited_session, get_session

    async def override_session():
        yield mock_session

    app.dependency_overrides[get_session] = override_session
    app.dependency_overrides[get_audited_session] = override_session


class TestRouteCrudFactory:
    """Tests for Route CRUD endpoints via crud_factory (lines 46-49)."""

    def test_get_route_by_pk(self):
        """GET /routes/{pk} returns a route."""
        route_id = uuid4()
        mock_route = MagicMock()
        mock_route.route_id = route_id
        mock_route.name = "EB Progression"

        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = mock_route
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_routes_app()
        _add_route_auth_overrides(app)
        _add_route_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get(f"/api/v1/routes/{route_id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "EB Progression"

    def test_get_route_not_found(self):
        """GET /routes/{pk} returns 404 for unknown route."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_routes_app()
        _add_route_auth_overrides(app)
        _add_route_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.get(f"/api/v1/routes/{uuid4()}")
        assert resp.status_code == 404

    def test_update_route(self):
        """PUT /routes/{pk} updates the route name."""
        route_id = uuid4()
        mock_route = MagicMock()
        mock_route.route_id = route_id
        mock_route.name = "Old Name"

        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = mock_route
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.flush = AsyncMock()

        app = _create_routes_app()
        _add_route_auth_overrides(app)
        _add_route_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.put(
            f"/api/v1/routes/{route_id}",
            json={"name": "New Name"},
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == "New Name"

    def test_delete_route(self):
        """DELETE /routes/{pk} returns 204."""
        route_id = uuid4()
        mock_route = MagicMock()
        mock_route.route_id = route_id
        mock_route.name = "Doomed"

        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = mock_route
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.delete = AsyncMock()
        mock_session.flush = AsyncMock()

        app = _create_routes_app()
        _add_route_auth_overrides(app)
        _add_route_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.delete(f"/api/v1/routes/{route_id}")
        assert resp.status_code == 204

    def test_delete_route_not_found(self):
        """DELETE /routes/{pk} returns 404 for unknown route."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _create_routes_app()
        _add_route_auth_overrides(app)
        _add_route_session_override(app, mock_session)

        client = TestClient(app)
        resp = client.delete(f"/api/v1/routes/{uuid4()}")
        assert resp.status_code == 404


class TestRouteUpdateSchemaValidation:
    """Tests for RouteUpdate empty-body validation (lines 46-49)."""

    def test_route_update_empty_raises(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="At least one field"):
            RouteUpdate()

    def test_route_signal_update_empty_raises(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="At least one field"):
            RouteSignalUpdate()

    def test_route_phase_update_empty_raises(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="At least one field"):
            RoutePhaseUpdate()

    def test_route_distance_update_empty_raises(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="At least one field"):
            RouteDistanceUpdate()
