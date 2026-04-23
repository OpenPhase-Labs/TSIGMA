"""
Base decoder class and registry for event log format decoders.

Decoders are self-registering plugins that convert vendor-specific
event log formats to TSIGMA events.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar


@dataclass
class DecodedEvent:
    """
    Single decoded event from a controller event log.

    All decoders output events in this standardized format.
    """

    timestamp: datetime
    event_code: int
    event_param: int


class BaseDecoder(ABC):
    """
    Base class for all decoder plugins.

    Subclass this and decorate with @DecoderRegistry.register to create
    a new decoder plugin.
    """

    name: ClassVar[str]
    extensions: ClassVar[list[str]]
    description: ClassVar[str]

    @abstractmethod
    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """
        Decode raw bytes into events.

        Args:
            data: Raw file bytes.

        Returns:
            List of decoded events.
        """
        ...

    @classmethod
    @abstractmethod
    def can_decode(cls, data: bytes) -> bool:
        """
        Check if this decoder can handle the data.

        Args:
            data: Raw file bytes (typically first 1KB for magic byte check).

        Returns:
            True if this decoder can decode the data.
        """
        ...


class DecoderRegistry:
    """
    Central registry for all decoder plugins.

    Decoders self-register using the @DecoderRegistry.register decorator.
    """

    _decoders: dict[str, type[BaseDecoder]] = {}

    @classmethod
    def register(cls, decoder_cls: type[BaseDecoder]) -> type[BaseDecoder]:
        """
        Register a decoder plugin.

        Usage:
            @DecoderRegistry.register
            class ASC3Decoder(BaseDecoder):
                name = "asc3"
                ...

        Args:
            decoder_cls: Decoder class to register.

        Returns:
            The decoder class (unchanged, for decorator chaining).
        """
        cls._decoders[decoder_cls.name] = decoder_cls
        return decoder_cls

    @classmethod
    def get(cls, name: str) -> type[BaseDecoder]:
        """
        Get a registered decoder by name.

        Args:
            name: Decoder name (e.g., "asc3", "siemens").

        Returns:
            Decoder class.

        Raises:
            ValueError: If decoder not found.
        """
        if name not in cls._decoders:
            raise ValueError(f"Unknown decoder: {name}")
        return cls._decoders[name]

    @classmethod
    def get_for_extension(cls, extension: str) -> list[type[BaseDecoder]]:
        """
        Get all decoders that support a file extension.

        Args:
            extension: File extension (e.g., ".dat", ".csv").

        Returns:
            List of decoder classes that support this extension.
        """
        extension = extension.lower()
        return [
            decoder_cls
            for decoder_cls in cls._decoders.values()
            if extension in decoder_cls.extensions
        ]

    @classmethod
    def list_all(cls) -> dict[str, type[BaseDecoder]]:
        """
        List all registered decoders.

        Returns:
            Dictionary of decoder name -> decoder class.
        """
        return cls._decoders.copy()
