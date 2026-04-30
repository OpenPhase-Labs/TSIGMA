"""
RoadsideSensorDeviceSource — device source for radar / LiDAR / video
sensors deployed at the roadway edge.

Queries the ``roadside_sensor`` table for active rows, filters by the
``collection.method`` stored in the sensor's ``metadata`` JSONB, injects
the sensor's IP address as ``host`` and its dedicated ``port`` /
``protocol`` columns as the network triple, and decrypts any encrypted
credentials in the collection config (when an encryption key is
configured).

Paired with ``RoadsideTarget`` so that events decoded from these
devices land in ``roadside_event`` and checkpoints are written with
``device_type='sensor'``.

The returned ``device_id`` is the stringified ``sensor_id`` UUID — the
same value used as ``polling_checkpoint.device_id`` for sensor rows.
"""

from typing import Any

from sqlalchemy import select, true
from sqlalchemy.ext.asyncio import AsyncSession

from ...crypto import decrypt_sensitive_fields, has_encryption_key
from ...models.roadside_sensor import RoadsideSensor
from ..targets import IngestionTarget


class RoadsideSensorDeviceSource:
    """Device source backed by the ``roadside_sensor`` table."""

    device_type: str = "sensor"

    def __init__(
        self,
        *,
        poll_interval_seconds: int,
        target: IngestionTarget,
    ) -> None:
        self.poll_interval_seconds = poll_interval_seconds
        self.target = target

    async def list_devices_for_method(
        self,
        session: AsyncSession,
        method_name: str,
    ) -> list[tuple[str, dict[str, Any]]]:
        stmt = select(
            RoadsideSensor.sensor_id,
            RoadsideSensor.ip_address,
            RoadsideSensor.port,
            RoadsideSensor.protocol,
            RoadsideSensor.username,
            RoadsideSensor.password,
            RoadsideSensor.metadata_,
        ).where(RoadsideSensor.is_active == true())

        result = await session.execute(stmt)
        rows = result.all()

        devices: list[tuple[str, dict[str, Any]]] = []
        for row in rows:
            metadata = row.metadata_
            if not metadata:
                continue

            collection = metadata.get("collection")
            if not collection:
                continue

            if collection.get("method") != method_name:
                continue

            config = dict(collection)
            # First-class network triple is on the row, not in JSONB.
            config["host"] = str(row.ip_address) if row.ip_address else ""
            if row.port is not None:
                config.setdefault("port", row.port)
            if row.protocol:
                config.setdefault("protocol", row.protocol)
            # Network credentials are first-class on roadside_sensor; the
            # username/password columns are encrypted at rest via crypto.py.
            if row.username:
                config.setdefault("username", row.username)
            if row.password:
                config.setdefault("password", row.password)

            if has_encryption_key():
                decrypt_sensitive_fields({"collection": config})

            devices.append((str(row.sensor_id), config))

        return devices
