"""
RoadsideTarget — the ingestion target for radar / LiDAR / video sensors
that live at the roadway edge and feed TSIGMA outside the controller's
event log.

Parallel to ``ControllerTarget``: same shape, same SDK calls, but every
checkpoint operation is scoped to ``device_type='sensor'`` and
``device_id`` is the stringified ``RoadsideSensor.sensor_id`` UUID.
Events are routed to ``roadside_event`` rather than
``controller_event_log``; the SDK's ``_upsert_events`` already dispatches
on event element type (``SensorDetection`` vs ``DecodedEvent``), so this
target's ``persist`` / ``persist_with_drift_check`` calls are identical
to the controller path — the events themselves carry their destination.

Decoder resolution is unchanged from the controller path: decoders that
emit ``SensorDetection`` are resolved the same way decoders that emit
``DecodedEvent`` are.  A listener serving sensors picks up its decoder
from per-device ``roadside_sensor.metadata.collection.decoder``.
"""

from typing import Any, Optional

from ...models.checkpoint import PollingCheckpoint
from .. import sdk


class RoadsideTarget:
    """Ingestion target for radar / LiDAR / video roadside sensors."""

    device_type: str = "sensor"

    def resolve_decoder(
        self,
        *,
        decoder_name: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Any:
        """Resolve a decoder by explicit name or by filename extension.

        Exactly one of ``decoder_name`` / ``filename`` must be given.
        """
        if decoder_name:
            return sdk.resolve_decoder_by_name(decoder_name)
        if filename:
            return sdk.resolve_decoder_by_extension(filename)
        raise ValueError(
            "resolve_decoder requires decoder_name or filename",
        )

    async def persist(
        self, events, device_id: str, session_factory,
    ) -> None:
        await sdk.persist_events(events, device_id, session_factory)

    async def persist_with_drift_check(
        self,
        events,
        device_id: str,
        session_factory,
        *,
        source_label: str = "sensor",
    ) -> None:
        await sdk.persist_events_with_drift_check(
            events,
            device_id,
            session_factory,
            source_label=source_label,
        )

    async def load_checkpoint(
        self, method_name: str, device_id: str, session_factory,
    ) -> Optional[PollingCheckpoint]:
        return await sdk.load_checkpoint(
            method_name, self.device_type, device_id, session_factory,
        )

    async def save_checkpoint(
        self,
        method_name: str,
        device_id: str,
        session_factory,
        **kwargs,
    ) -> None:
        await sdk.save_checkpoint(
            method_name,
            self.device_type,
            device_id,
            session_factory,
            **kwargs,
        )

    async def record_error(
        self,
        method_name: str,
        device_id: str,
        session_factory,
        error_msg: str,
    ) -> None:
        await sdk.record_error(
            method_name,
            self.device_type,
            device_id,
            session_factory,
            error_msg,
        )
