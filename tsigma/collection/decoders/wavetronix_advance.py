"""
Wavetronics SmartSensor Advance radar decoder.

Roadside radar that pushes per-vehicle speed detections as fixed-size
19-byte binary packets, historically over UDP port 10088 (TCP listener
in the 4.x source is buggy and not used in practice).  The packet is
emitted at detection time — one vehicle, one packet, no buffering at
the sensor.

Wire format (19 bytes):

    field              | length | notes
    -------------------|-------:|---------------------------------------
    dataIdentifier     |   2    | purpose unclear per 4.x source comment
    DeviceID           |   4    | last 4 digits of radar serial
    Header             |   2    | message type; ignored in this decoder
    SpeedMPH           |   1    | vehicle speed, miles per hour
    SpeedKPH           |   1    | vehicle speed, kilometres per hour
    6-digit ASCII tag  |   6    | operator-configured: signal_id + channel
    Closing flags      |   3    | 0x7E 0x0D 0x0D ("~\\r\\r")

Validation:

  * ``len(packet) == _PACKET_STRUCT.size``
  * packet ends with ``_CLOSING_FLAGS``
  * ``kph == round(mph * _MI_TO_KM)``  — international-mile conversion,
    matches the 4.x sanity check byte-for-byte
  * sensor tag is alphanumeric  — matches the 4.x DetectorID regex

Packets failing any validation are dropped (debug-logged, not errored —
malformed packets are expected in the wild).

Ported from ATSPM 4.x:
    WavetronicsSpeedListener/WavetronicsSpeedLibrary/Packet.cs
"""

import logging
import re
import struct
from datetime import datetime, timezone

from tsigma.models.roadside_event import ROADSIDE_EVENT_TYPE_SPEED

from .base import BaseSensorDecoder, DecoderRegistry, SensorDetection

logger = logging.getLogger(__name__)

# International mile -> kilometre conversion (SI-defined, exact).  Kept
# as a named constant so the mph/kph sanity check below matches the
# 4.x C# source byte-for-byte and so intent is obvious.
_MI_TO_KM = 1.609344

# Wavetronics Advance 19-byte packet layout — declarative via struct:
#   8x   = dataIdentifier (2) + DeviceID (4) + Header (2) — all skipped
#   B    = SpeedMPH   (1 unsigned byte)
#   B    = SpeedKPH   (1 unsigned byte)
#   6s   = 6-byte ASCII sensor tag
#   3s   = 3-byte closing flags (validated against _CLOSING_FLAGS)
#   Total = 8 + 1 + 1 + 6 + 3 = 19 bytes, matches the documented size.
_PACKET_STRUCT = struct.Struct(">8xBB6s3s")

_CLOSING_FLAGS = b"\x7e\x0d\x0d"
_SENSOR_TAG_RE = re.compile(r"^[A-Za-z0-9]+$")


@DecoderRegistry.register
class WavetronixAdvanceDecoder(BaseSensorDecoder):
    """Decode 19-byte Wavetronics SmartSensor Advance push packets."""

    name = "wavetronix_advance"
    description = "Wavetronics SmartSensor Advance radar (19-byte binary push)"

    def decode_bytes(self, data: bytes) -> list[SensorDetection]:
        if not self.can_decode(data):
            return []

        try:
            mph, kph, tag_bytes, _closing = _PACKET_STRUCT.unpack(data)
            tag = tag_bytes.decode("ascii")
        except (struct.error, UnicodeDecodeError) as exc:
            logger.warning("wavetronix_advance: packet parse failed: %s", exc)
            return []

        # Per Packet.cs:124 — kph should be the rounded mi->km conversion
        # of mph.  Packets where the two don't match were never saved in
        # 4.x either; inherit that behaviour.
        if kph != round(mph * _MI_TO_KM):
            logger.debug(
                "wavetronix_advance: mph/kph mismatch (mph=%d kph=%d); dropped",
                mph, kph,
            )
            return []

        # 4.x regex-filtered DetectorID to alphanumeric before SQL insert.
        # We keep the same filter to stay round-trip compatible with any
        # deployment also running the 4.x listener.
        if not _SENSOR_TAG_RE.match(tag):
            logger.debug(
                "wavetronix_advance: non-alphanumeric tag %r; dropped", tag,
            )
            return []

        return [SensorDetection(
            timestamp=datetime.now(timezone.utc),
            vendor_tag=tag,
            event_type=ROADSIDE_EVENT_TYPE_SPEED,
            mph=mph,
            kph=kph,
        )]

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Shape check: exact size + correct closing flags."""
        return len(data) == _PACKET_STRUCT.size and data.endswith(_CLOSING_FLAGS)
