"""
SignalDeviceSource — device source for cabinet-controller signals.

Queries the ``signal`` table for enabled rows, filters by the
``collection.method`` stored in ``signal_metadata``, injects the
signal's IP address as ``host``, and decrypts any encrypted
credentials in the collection config (when an encryption key is
configured).

Paired with ``ControllerTarget`` so that events decoded from these
devices land in ``controller_event_log`` and checkpoints are written
with ``device_type='controller'``.
"""

from typing import Any

from sqlalchemy import select, true
from sqlalchemy.ext.asyncio import AsyncSession

from ...crypto import decrypt_sensitive_fields, has_encryption_key
from ...models.signal import Signal
from ..targets import IngestionTarget


class SignalDeviceSource:
    """Device source backed by the ``signal`` table."""

    device_type: str = "controller"

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
            Signal.signal_id,
            Signal.ip_address,
            Signal.signal_metadata,
        ).where(Signal.enabled == true())

        result = await session.execute(stmt)
        rows = result.all()

        devices: list[tuple[str, dict[str, Any]]] = []
        for row in rows:
            metadata = row.signal_metadata
            if not metadata:
                continue

            collection = metadata.get("collection")
            if not collection:
                continue

            if collection.get("method") != method_name:
                continue

            config = dict(collection)
            config["host"] = str(row.ip_address) if row.ip_address else ""

            if has_encryption_key():
                decrypt_sensitive_fields({"collection": config})

            devices.append((row.signal_id, config))

        return devices
