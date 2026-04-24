"""
Collection (data ingestion) system.

Includes decoders (format parsers) and methods (data sources).
"""

# Import methods to trigger auto-discovery
from . import methods
from .decoders import BaseDecoder, DecodedEvent, DecoderRegistry
from .registry import (
    BaseIngestionMethod,
    ExecutionMode,
    IngestionMethodRegistry,
    PollingIngestionMethod,
)
from .service import CollectorService

__all__ = [
    "BaseDecoder",
    "CollectorService",
    "DecodedEvent",
    "DecoderRegistry",
    "BaseIngestionMethod",
    "ExecutionMode",
    "IngestionMethodRegistry",
    "PollingIngestionMethod", "methods",
]
