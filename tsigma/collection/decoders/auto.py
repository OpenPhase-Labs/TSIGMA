"""
Auto-detect decoder wrapper.

Probes registered decoders in priority order and delegates
to the first one that claims it can decode the data.

Priority: asc3 -> peek -> maxtime -> siemens -> csv
"""

from .base import BaseDecoder, DecodedEvent, DecoderRegistry

_PRIORITY = ["asc3", "peek", "maxtime", "siemens", "csv"]


@DecoderRegistry.register
class AutoDecoder(BaseDecoder):
    """Decoder that auto-detects the format and delegates."""

    name = "auto"
    extensions = [".*"]
    description = "Auto-detect event log format"

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Always returns True; auto will try all decoders.

        Args:
            data: Raw file bytes.

        Returns:
            True always.
        """
        return True

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Probe decoders in priority order and decode with first match.

        Args:
            data: Raw file bytes.

        Returns:
            List of decoded events.

        Raises:
            ValueError: If no decoder can handle the data.
        """
        for name in _PRIORITY:
            try:
                decoder_cls = DecoderRegistry.get(name)
            except ValueError:
                continue
            if decoder_cls.can_decode(data):
                return decoder_cls().decode_bytes(data)

        raise ValueError("No decoder found for the provided data")
