"""PollingCheckpoint device-polymorphism coverage.

Controllers and roadside sensors share the ``polling_checkpoint``
table; the composite PK ``(device_type, device_id, method)`` is the
only thing keeping their namespaces separate.  These tests prove the
collision surface stays flat: a controller and a sensor with the same
``device_id`` can coexist, and device-type-scoped queries see only
their own rows.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.models.checkpoint import (
    DEVICE_TYPE_CONTROLLER,
    DEVICE_TYPE_SENSOR,
    PollingCheckpoint,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_controller_and_sensor_checkpoints_coexist(
    dialect_session: AsyncSession,
) -> None:
    """Two rows with the same ``method`` but different ``device_type`` survive.

    The PK includes ``device_type`` so identical ``device_id`` values
    across classes do not collide — we verify that by using the same
    string in both rows.
    """
    shared_id = "SIG-001"
    controller_cp = PollingCheckpoint(
        device_type=DEVICE_TYPE_CONTROLLER,
        device_id=shared_id,
        method="ftp_pull",
    )
    sensor_cp = PollingCheckpoint(
        device_type=DEVICE_TYPE_SENSOR,
        device_id=shared_id,
        method="ftp_pull",
    )
    dialect_session.add_all([controller_cp, sensor_cp])
    await dialect_session.flush()

    result = await dialect_session.execute(
        select(PollingCheckpoint).where(
            PollingCheckpoint.device_id == shared_id,
        )
    )
    rows = list(result.scalars())
    types = {r.device_type for r in rows}
    assert types == {DEVICE_TYPE_CONTROLLER, DEVICE_TYPE_SENSOR}
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_query_by_device_type_partitions_results(
    dialect_session: AsyncSession,
) -> None:
    """``WHERE device_type=...`` isolates each device class."""
    controller_ids = [f"CTRL-{i:03d}" for i in range(3)]
    sensor_ids = [str(uuid4()) for _ in range(2)]

    for cid in controller_ids:
        dialect_session.add(PollingCheckpoint(
            device_type=DEVICE_TYPE_CONTROLLER,
            device_id=cid,
            method="ftp_pull",
        ))
    for sid in sensor_ids:
        dialect_session.add(PollingCheckpoint(
            device_type=DEVICE_TYPE_SENSOR,
            device_id=sid,
            method="ftp_pull",
        ))
    await dialect_session.flush()

    controllers = await dialect_session.execute(
        select(PollingCheckpoint).where(
            PollingCheckpoint.device_type == DEVICE_TYPE_CONTROLLER,
        )
    )
    sensors = await dialect_session.execute(
        select(PollingCheckpoint).where(
            PollingCheckpoint.device_type == DEVICE_TYPE_SENSOR,
        )
    )

    controller_rows = list(controllers.scalars())
    sensor_rows = list(sensors.scalars())

    assert len(controller_rows) == 3
    assert len(sensor_rows) == 2
    assert {r.device_id for r in controller_rows} == set(controller_ids)
    assert {r.device_id for r in sensor_rows} == set(sensor_ids)
