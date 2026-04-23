"""
Integration tests for Signals API endpoints against a real PostgreSQL database.

Tests full request lifecycle: HTTP request -> FastAPI -> SQLAlchemy -> PostgreSQL.

Requirements:
    - Running PostgreSQL instance
    - TSIGMA_TEST_DB_URL environment variable set, e.g.:
        export TSIGMA_TEST_DB_URL="postgresql+asyncpg://user:pass@localhost:5432/tsigma_test"

Skipped automatically when TSIGMA_TEST_DB_URL is not set.
"""

import pytest
from fastapi.testclient import TestClient

from tsigma.app import create_app
from tsigma.dependencies import get_session
from tsigma.models import Signal

pytestmark = pytest.mark.integration


@pytest.fixture
def db_test_client(db_session):
    """
    TestClient wired to a real database session.

    The session is wrapped in a transaction that rolls back after each test,
    so no data persists between tests.

    Args:
        db_session: Real async database session from conftest.

    Returns:
        FastAPI TestClient with real database backing.
    """
    app = create_app()
    app.dependency_overrides[get_session] = lambda: db_session
    return TestClient(app)


class TestListSignalsDB:
    """Tests for GET /api/v1/signals against a real database."""

    @pytest.mark.asyncio
    async def test_empty_database_returns_empty_list(self, db_test_client, db_session):
        """Test listing signals on empty database returns []."""
        response = db_test_client.get("/api/v1/signals/")
        assert response.status_code == 200
        assert response.json() == []

    @pytest.mark.asyncio
    async def test_returns_inserted_signals(self, db_test_client, db_session):
        """Test listing signals returns data inserted via ORM."""
        sig = Signal(
            signal_id="INT-001",
            primary_street="Peachtree St",
            secondary_street="10th St",
            enabled=True,
        )
        db_session.add(sig)
        await db_session.flush()

        response = db_test_client.get("/api/v1/signals/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["signal_id"] == "INT-001"
        assert data[0]["primary_street"] == "Peachtree St"

    @pytest.mark.asyncio
    async def test_pagination(self, db_test_client, db_session):
        """Test skip/limit pagination against real data."""
        for i in range(5):
            db_session.add(Signal(
                signal_id=f"SIG-{i:03d}",
                primary_street=f"Street {i}",
                enabled=True,
            ))
        await db_session.flush()

        response = db_test_client.get("/api/v1/signals/?skip=2&limit=2")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2


class TestGetSignalDB:
    """Tests for GET /api/v1/signals/{signal_id} against a real database."""

    @pytest.mark.asyncio
    async def test_get_existing_signal(self, db_test_client, db_session):
        """Test retrieving a signal that exists in the database."""
        sig = Signal(
            signal_id="INT-100",
            primary_street="Main St",
            secondary_street="Oak Ave",
            enabled=True,
        )
        db_session.add(sig)
        await db_session.flush()

        response = db_test_client.get("/api/v1/signals/INT-100")

        assert response.status_code == 200
        data = response.json()
        assert data["signal_id"] == "INT-100"
        assert data["primary_street"] == "Main St"
        assert data["secondary_street"] == "Oak Ave"

    @pytest.mark.asyncio
    async def test_404_for_missing_signal(self, db_test_client, db_session):
        """Test 404 when signal ID does not exist in database."""
        response = db_test_client.get("/api/v1/signals/DOES-NOT-EXIST")

        assert response.status_code == 404
        assert "DOES-NOT-EXIST" in response.json()["detail"]
