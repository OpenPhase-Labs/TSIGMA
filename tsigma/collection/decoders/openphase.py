"""
OpenPhase Protobuf decoder.

Decodes OpenPhase v1 protobuf messages into DecodedEvent objects.
Supports three message formats:

1. **IntersectionUpdate** — single event in the master envelope
   (oneof payload = AtspmEvent). Used for real-time NATS/MQTT streaming.

2. **CompactEventBatch** — delta-encoded batch of events with a
   shared base timestamp. Optimized for bandwidth-constrained backhaul.

3. **IntersectionUpdateBatch** — batch of full IntersectionUpdate
   envelopes. Used for bulk transport.

OpenPhase EventType enum values are Indiana Hi-Resolution Data Logger
event codes (Purdue/INDOT 2012); no translation is needed. AtspmEvent
carries the IHR (code, param) pair directly.

Proto definitions sourced from the OPENPHASE repository and compiled
with protoc into tsigma/collection/decoders/proto/.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

from .base import BaseDecoder, DecodedEvent, DecoderRegistry

logger = logging.getLogger(__name__)

# Add proto output directory to sys.path so generated modules can
# resolve their cross-imports (e.g., common_pb2 imports ihr_events_pb2).
_PROTO_DIR = str(Path(__file__).parent / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from openphase.v1 import (  # noqa: E402  # generated proto modules require sys.path.insert above
    common_pb2,
    ihr_events_pb2,
)


@DecoderRegistry.register
class OpenPhaseDecoder(BaseDecoder):
    """
    Decoder for OpenPhase v1 protobuf messages.

    Handles IntersectionUpdate (single event), CompactEventBatch
    (delta-encoded batch), and IntersectionUpdateBatch (full batch).
    Auto-detects which format based on protobuf wire content.
    """

    name: ClassVar[str] = "openphase"
    extensions: ClassVar[list[str]] = [".pb", ".proto", ".bin"]
    description: ClassVar[str] = "OpenPhase v1 Protobuf (events and batches)"

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """
        Decode protobuf bytes into DecodedEvent objects.

        Tries each message type in order:
        1. CompactEventBatch (most common for streaming)
        2. IntersectionUpdateBatch (bulk transport)
        3. IntersectionUpdate (single event)

        Args:
            data: Raw protobuf bytes.

        Returns:
            List of DecodedEvent objects.
        """
        # Try CompactEventBatch first (most efficient format)
        events = self._try_compact_batch(data)
        if events is not None:
            return events

        # Try IntersectionUpdateBatch
        events = self._try_update_batch(data)
        if events is not None:
            return events

        # Try single IntersectionUpdate
        events = self._try_single_update(data)
        if events is not None:
            return events

        raise ValueError(
            "Unable to decode OpenPhase protobuf: "
            "data does not match any known message type"
        )

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """
        Check if data looks like an OpenPhase protobuf message.

        Attempts to parse as each supported message type.

        Args:
            data: Raw bytes to check.

        Returns:
            True if any OpenPhase message type parses successfully.
        """
        instance = cls()
        for try_fn in (
            instance._try_compact_batch,
            instance._try_update_batch,
            instance._try_single_update,
        ):
            try:
                result = try_fn(data)
            except Exception:
                continue
            if result is not None:
                return True
        return False

    def _try_compact_batch(self, data: bytes) -> list[DecodedEvent] | None:
        """Try to decode as CompactEventBatch."""
        try:
            batch = common_pb2.CompactEventBatch()
            batch.ParseFromString(data)
            # Validate: must have events and a base timestamp
            if not batch.events or batch.base_timestamp_ns == 0:
                return None
            return self._decode_compact_batch(batch)
        except Exception:
            return None

    def _try_update_batch(self, data: bytes) -> list[DecodedEvent] | None:
        """Try to decode as IntersectionUpdateBatch."""
        try:
            batch = common_pb2.IntersectionUpdateBatch()
            batch.ParseFromString(data)
            if not batch.updates:
                return None
            events = []
            for update in batch.updates:
                event = self._decode_intersection_update(update)
                if event is not None:
                    events.append(event)
            return events if events else None
        except Exception:
            return None

    def _try_single_update(self, data: bytes) -> list[DecodedEvent] | None:
        """Try to decode as a single IntersectionUpdate."""
        try:
            update = common_pb2.IntersectionUpdate()
            update.ParseFromString(data)
            if not update.HasField("payload"):
                return None
            event = self._decode_intersection_update(update)
            return [event] if event is not None else None
        except Exception:
            return None

    def _decode_intersection_update(
        self, update: common_pb2.IntersectionUpdate
    ) -> DecodedEvent | None:
        """
        Extract a DecodedEvent from an IntersectionUpdate envelope.

        Only processes AtspmEvent payloads. Other payload types
        (spat, health, security, etc.) are skipped.

        Args:
            update: Parsed IntersectionUpdate message.

        Returns:
            DecodedEvent or None if not an AtspmEvent.
        """
        payload_type = update.WhichOneof("payload")
        if payload_type != "event":
            return None

        atspm_event = update.event
        ts = update.ts.ToDatetime().replace(tzinfo=timezone.utc)

        event_code, event_param = self._resolve_event_code(atspm_event)

        return DecodedEvent(
            timestamp=ts,
            event_code=event_code,
            event_param=event_param,
        )

    def _decode_compact_batch(
        self, batch: common_pb2.CompactEventBatch
    ) -> list[DecodedEvent]:
        """
        Decode a CompactEventBatch into DecodedEvent objects.

        Reconstructs absolute timestamps from the base timestamp
        and per-event millisecond offsets.

        Args:
            batch: Parsed CompactEventBatch message.

        Returns:
            List of DecodedEvent objects.
        """
        base_ns = batch.base_timestamp_ns
        base_dt = datetime.fromtimestamp(base_ns / 1_000_000_000, tz=timezone.utc)

        events = []
        for compact in batch.events:
            # Reconstruct absolute timestamp from base + offset
            offset_us = compact.offset_ms * 1000
            ts = base_dt + _timedelta_us(offset_us)

            # EventType enum integer == Indiana Hi-Res event code (no translation)
            event_code = compact.code
            event_param = compact.param

            events.append(DecodedEvent(
                timestamp=ts,
                event_code=event_code,
                event_param=event_param,
            ))

        return events

    @staticmethod
    def _resolve_event_code(
        atspm_event: ihr_events_pb2.AtspmEvent,
    ) -> tuple[int, int]:
        """
        Resolve (event_code, event_param) from an AtspmEvent.

        EventType enum integer == Indiana Hi-Res event code, so `code` and
        `param` are the canonical IHR pair directly.

        Args:
            atspm_event: Parsed AtspmEvent message.

        Returns:
            Tuple of (event_code, event_param).
        """
        return atspm_event.code, atspm_event.param


def _timedelta_us(microseconds: int):
    """Create a timedelta from microseconds without importing timedelta at module level."""
    from datetime import timedelta
    return timedelta(microseconds=microseconds)
