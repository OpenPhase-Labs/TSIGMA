"""
Device sources — classes of devices that share a polling cadence and
ingestion target.

``SignalDeviceSource`` queries the ``signal`` table and routes to a
``ControllerTarget``; a future ``RoadsideSensorDeviceSource`` will
query ``roadside_sensor`` and route to a ``RoadsideTarget``.
``CollectorService`` iterates registered sources and schedules one
poll-cycle job per ``(transport_method × source)`` pair at the
source's configured interval.

See ``base.DeviceSource`` for the protocol contract.
"""

from .base import DeviceSource
from .signal import SignalDeviceSource

__all__ = ["DeviceSource", "SignalDeviceSource"]
