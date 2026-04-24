"""
DeviceSource protocol — the contract between ``CollectorService`` and
a class of devices that share a polling cadence and an ingestion
target.

A device source answers *"which devices of my kind should be polled
right now, and with what config?"*.  ``SignalDeviceSource`` queries
the ``signal`` table; ``RoadsideSensorDeviceSource`` (future) queries
``roadside_sensor``.  Because each source owns its own poll cadence
and ingestion target, controllers and sensors can run at different
intervals and write to different event tables without the orchestrator
needing to know about either.
"""

from typing import Any, Protocol, runtime_checkable

from sqlalchemy.ext.asyncio import AsyncSession

from ..targets import IngestionTarget


@runtime_checkable
class DeviceSource(Protocol):
    """A class of devices with its own poll cadence and target.

    Attributes:
        device_type: Stable discriminator — matches the ``device_type``
            column written to ``polling_checkpoint`` and to any target-
            side event rows.  Values: ``"controller"``, ``"sensor"``.
        poll_interval_seconds: Interval, in seconds, between
            ``CollectorService`` poll cycles for this source.  Drawn
            from settings at app startup (``collector_poll_interval``
            for controllers, ``sensor_poll_interval`` for sensors).
        target: The ``IngestionTarget`` decoded events from these
            devices get fed into.  Fixed for the lifetime of the
            source so that ``(source, target)`` is a stable pair.
    """

    device_type: str
    poll_interval_seconds: int
    target: IngestionTarget

    async def list_devices_for_method(
        self,
        session: AsyncSession,
        method_name: str,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Return ``(device_id, collection_config)`` pairs eligible
        for ``method_name``.

        Devices are "eligible" when they are enabled for collection
        and their stored collection configuration names this
        ``method_name`` as the transport.  The returned ``config``
        dict is ready to hand to a transport's ``poll_once`` (host
        injected, credentials decrypted).
        """
        ...
