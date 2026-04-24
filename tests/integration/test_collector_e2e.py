"""End-to-end smoke test for ``CollectorService``.

Drives the full controller ingestion path — ``SignalDeviceSource`` →
``FakeFTPMethod`` (a test-local stand-in for a real transport) →
``ControllerTarget`` → ``controller_event_log`` — against a real
PostgreSQL database.

The real transports (FTP, HTTP, NATS) are exercised by their own unit
tests with mocked sockets.  This test's job is to prove the
orchestration layer wires source ↔ method ↔ target correctly and the
decoded events actually land in the DB.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from ipaddress import ip_address
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.collection.registry import (
    IngestionMethodRegistry,
    PollingIngestionMethod,
)
from tsigma.collection.service import CollectorService
from tsigma.collection.sources.signal import SignalDeviceSource
from tsigma.collection.targets import ControllerTarget
from tsigma.config import settings
from tsigma.models.event import ControllerEventLog
from tsigma.models.signal import Signal

pytestmark = pytest.mark.integration


_FAKE_METHOD_NAME = "fake_ftp_e2e"
_TEST_SIGNAL_ID = "SIG-TEST"


class _FakeFTPMethod(PollingIngestionMethod):
    """Minimal ``PollingIngestionMethod`` that emits three fabricated events.

    Does not touch the network — ``poll_once`` builds three
    ``DecodedEvent`` objects and hands them to the target's
    ``persist_with_drift_check`` so we can verify the orchestration
    wiring without any transport-layer concerns.
    """

    name = _FAKE_METHOD_NAME

    async def poll_once(
        self,
        device_id: str,
        config: dict[str, Any],
        session_factory,
        *,
        target: Any = None,
    ) -> None:
        base = datetime(2026, 4, 24, 14, 0, 0, tzinfo=timezone.utc)
        events = [
            DecodedEvent(
                timestamp=base + timedelta(seconds=i),
                event_code=10,
                event_param=i,
            )
            for i in range(3)
        ]
        await target.persist_with_drift_check(
            events, device_id, session_factory,
        )

    async def health_check(self) -> bool:
        return True


@pytest.fixture
def _registered_fake_method():
    """Register ``_FakeFTPMethod`` and unregister it on teardown.

    The registry is process-global; the ``pop`` in teardown protects
    other test modules that also touch ``IngestionMethodRegistry``.
    """
    IngestionMethodRegistry.register(_FAKE_METHOD_NAME)(_FakeFTPMethod)
    try:
        yield
    finally:
        IngestionMethodRegistry._methods.pop(_FAKE_METHOD_NAME, None)


@pytest.mark.asyncio
async def test_end_to_end_controller_ingestion(
    pg_engine, _registered_fake_method,
) -> None:
    """One poll cycle ingests three rows into ``controller_event_log``."""
    session_factory = async_sessionmaker(
        bind=pg_engine, class_=AsyncSession, expire_on_commit=False,
    )

    async with session_factory() as session:
        session.add(Signal(
            signal_id=_TEST_SIGNAL_ID,
            primary_street="Test St",
            ip_address=str(ip_address("10.0.0.1")),
            signal_metadata={"collection": {"method": _FAKE_METHOD_NAME}},
            enabled=True,
        ))
        await session.commit()

    source = SignalDeviceSource(
        poll_interval_seconds=900,
        target=ControllerTarget(),
    )
    svc = CollectorService(session_factory, settings, sources=[source])
    svc._polling_instances[_FAKE_METHOD_NAME] = _FakeFTPMethod()

    await svc._run_poll_cycle(_FAKE_METHOD_NAME, source)

    async with session_factory() as session:
        result = await session.execute(
            select(ControllerEventLog).where(
                ControllerEventLog.signal_id == _TEST_SIGNAL_ID,
            )
        )
        rows = list(result.scalars())

    assert len(rows) == 3, (
        f"expected 3 events for {_TEST_SIGNAL_ID}, got {len(rows)}"
    )
    params = sorted(r.event_param for r in rows)
    assert params == [0, 1, 2]
    assert {r.event_code for r in rows} == {10}
