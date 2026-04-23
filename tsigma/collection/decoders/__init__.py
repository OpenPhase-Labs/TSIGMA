"""
Decoder plugins auto-discovery.

Automatically imports all decoder modules to trigger @DecoderRegistry.register decorators.
"""

from pathlib import Path

from .base import BaseDecoder, DecodedEvent, DecoderRegistry

# Auto-discover and import all decoder modules
decoders_dir = Path(__file__).parent
for module_file in decoders_dir.glob("*.py"):
    if module_file.stem not in ("__init__", "base"):
        __import__(f"tsigma.collection.decoders.{module_file.stem}")

__all__ = ["BaseDecoder", "DecodedEvent", "DecoderRegistry"]
