"""
Device sources — classes of devices that share a polling cadence and
ingestion target.

``SignalDeviceSource`` queries the ``signal`` table and routes to a
``ControllerTarget``; ``RoadsideSensorDeviceSource`` queries
``roadside_sensor`` and routes to a ``RoadsideTarget``.
``CollectorService`` (polling) and ``ListenerService`` (listener +
event-driven) both iterate registered sources to dispatch each method
against every device class it serves.

See ``base.DeviceSource`` for the protocol contract.
"""

from .base import DeviceSource
from .sensor import RoadsideSensorDeviceSource
from .signal import SignalDeviceSource

__all__ = ["DeviceSource", "RoadsideSensorDeviceSource", "SignalDeviceSource"]
