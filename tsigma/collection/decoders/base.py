"""
Base decoder classes and registry for event / detection format decoders.

Decoders are self-registering plugins that convert vendor-specific
wire bytes into TSIGMA events (controller side) or detections (roadside
sensor side).  Two parallel output types share the same registry:

    ``DecodedEvent``      — controller event-log events (event_code,
                            event_param) bound for ``controller_event_log``.
    ``SensorDetection``   — roadside-sensor per-vehicle detections (mph,
                            lane_number, vehicle_class, ...) bound for
                            ``roadside_event``.

The persister dispatches on the decoder's output type — no ``target``
parameter or per-listener refactor needed; the data itself carries its
destination.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Union


@dataclass
class DecodedEvent:
    """
    Single decoded event from a controller event log.

    Controller-side output; routes to ``controller_event_log`` via the
    type-dispatched persister.
    """

    timestamp: datetime
    event_code: int
    event_param: int


@dataclass
class SensorDetection:
    """
    Single decoded detection from a roadside sensor.

    Fields map 1:1 onto ``RoadsideEvent`` columns with one indirection:
    ``vendor_tag`` carries the vendor-native identifier from the wire
    (e.g. Wavetronics' 6-character ASCII tag of signal_id + detector
    channel).  The persister maps ``vendor_tag`` to the internal
    ``roadside_sensor.sensor_id`` UUID via ``roadside_sensor_lane.vendor_lane_id``
    before INSERT — decoders stay ignorant of TSIGMA's internal IDs.

    Which data fields are populated depends on ``event_type`` (see the
    constants in ``tsigma.models.roadside_event``):

        SPEED            -> mph, kph, length_feet, lane_number, direction_id
        CLASSIFICATION   -> vehicle_class, length_feet, lane_number, direction_id
        QUEUE            -> queue_length_feet, lane_number
        OCCUPANCY        -> occupancy_pct, lane_number

    Routes to ``roadside_event`` via the type-dispatched persister.
    """

    timestamp: datetime
    vendor_tag: str
    event_type: int
    mph: int | None = None
    kph: int | None = None
    length_feet: int | None = None
    vehicle_class: int | None = None
    lane_number: int | None = None
    direction_id: int | None = None
    occupancy_pct: float | None = None
    queue_length_feet: float | None = None


# Convenience alias for the two output types a decoder can return.
AnyDecodedOutput = Union[DecodedEvent, SensorDetection]


class BaseDecoder(ABC):
    """
    Base class for controller-event decoder plugins.

    Subclass this and decorate with @DecoderRegistry.register to create
    a new decoder plugin.  Output is a list of ``DecodedEvent``; for
    sensor-side decoders subclass ``BaseSensorDecoder`` instead.
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


class BaseSensorDecoder(ABC):
    """
    Base class for roadside-sensor decoder plugins.

    Sibling of ``BaseDecoder`` — same registry-registration mechanism
    via ``@DecoderRegistry.register``, different output type.  Sensor
    decoders typically operate on fixed-size wire packets (UDP / TCP
    push) rather than on file blobs, so ``extensions`` is usually an
    empty list and ``can_decode`` is a fast byte-shape sanity check.
    """

    name: ClassVar[str]
    extensions: ClassVar[list[str]] = []
    description: ClassVar[str]

    @abstractmethod
    def decode_bytes(self, data: bytes) -> list[SensorDetection]:
        """Decode raw bytes into sensor detections."""
        ...

    @classmethod
    @abstractmethod
    def can_decode(cls, data: bytes) -> bool:
        """Return True if this decoder can handle the given byte shape."""
        ...


# Both decoder types live in the same registry, looked up by ``name``.
# Consumers (listeners, ftp_pull, etc.) call ``decoder.decode_bytes(...)``
# and hand the result to the persister; the persister dispatches on the
# element type (``DecodedEvent`` vs ``SensorDetection``).
AnyDecoder = Union["BaseDecoder", "BaseSensorDecoder"]


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
