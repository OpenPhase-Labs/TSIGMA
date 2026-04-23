"""
Pytest configuration and shared fixtures.

Provides test database, client, and other shared test utilities.

Integration tests require a running PostgreSQL database.
Set TSIGMA_TEST_DB_URL to enable them:

    export TSIGMA_TEST_DB_URL="postgresql+asyncpg://user:pass@localhost:5432/tsigma_test"

Integration tests are skipped automatically when this variable is not set.
"""

import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from tsigma.app import create_app
from tsigma.models.base import Base

# ---------------------------------------------------------------------------
# Unit test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def app():
    """
    FastAPI application fixture.

    Returns:
        Configured FastAPI app for testing.
    """
    return create_app()


@pytest.fixture
def client(app):
    """
    Test client fixture.

    Args:
        app: FastAPI app fixture.

    Returns:
        TestClient for making requests.
    """
    return TestClient(app)


# ---------------------------------------------------------------------------
# Integration test fixtures (require PostgreSQL)
# ---------------------------------------------------------------------------

def _get_test_db_url() -> str | None:
    """Read TSIGMA_TEST_DB_URL from environment."""
    return os.environ.get("TSIGMA_TEST_DB_URL")


requires_db = pytest.mark.integration


@pytest.fixture(scope="session")
def db_url():
    """
    PostgreSQL connection URL from environment.

    Skips the entire session if TSIGMA_TEST_DB_URL is not set.
    """
    url = _get_test_db_url()
    if not url:
        pytest.skip("TSIGMA_TEST_DB_URL not set — skipping database tests")
    return url


@pytest.fixture(scope="session")
def db_engine(db_url):
    """
    Async SQLAlchemy engine scoped to the test session.

    Creates all tables on startup, drops them on teardown.

    Args:
        db_url: PostgreSQL connection URL.

    Returns:
        AsyncEngine instance.
    """
    import asyncio

    engine = create_async_engine(db_url, echo=False)

    async def _setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _teardown():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()

    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_setup())
    yield engine
    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_teardown())


@pytest.fixture
async def db_session(db_engine) -> AsyncSession:
    """
    Per-test async database session.

    Wraps each test in a transaction that is rolled back on completion,
    so tests never leave data behind.

    Args:
        db_engine: Session-scoped async engine.

    Yields:
        AsyncSession bound to a rolled-back transaction.
    """
    async with db_engine.connect() as conn:
        transaction = await conn.begin()
        session_factory = async_sessionmaker(bind=conn, class_=AsyncSession, expire_on_commit=False)

        async with session_factory() as session:
            yield session

        await transaction.rollback()


@pytest.fixture
def db_facade(db_url):
    """
    Real DatabaseFacade connected to the test database.

    Args:
        db_url: PostgreSQL connection URL.

    Returns:
        Connected DatabaseFacade instance.
    """
    import asyncio

    # Parse URL parts for DatabaseFacade constructor
    # URL format: postgresql+asyncpg://user:pass@host:port/database
    from urllib.parse import urlparse

    from tsigma.database.db import DatabaseFacade
    parsed = urlparse(db_url.replace("postgresql+asyncpg://", "http://"))

    facade = DatabaseFacade(
        "postgresql",
        user=parsed.username or "tsigma",
        password=parsed.password or "",
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        database=parsed.path.lstrip("/") or "tsigma_test",
    )

    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(facade.connect())
    yield facade
    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(facade.disconnect())
