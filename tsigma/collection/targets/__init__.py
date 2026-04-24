"""
Ingestion targets — destinations for ingested events.

A target abstracts the "where does the data go" question away from the
transport layer.  ``ControllerTarget`` writes to
``controller_event_log``; a future ``RoadsideTarget`` will write to
``roadside_event``.  Transport methods accept an ``IngestionTarget``
parameter so one transport can feed either event stream without
duplication.

See ``base.IngestionTarget`` for the protocol contract.
"""

from .base import IngestionTarget
from .controller import ControllerTarget

__all__ = ["ControllerTarget", "IngestionTarget"]
