"""
Ingestion method registry for TSIGMA.

Ingestion methods are self-registering plugins for collecting data
from traffic signal controllers via polling (FTP, HTTP, etc.).
"""

import enum
from abc import ABC, abstractmethod
from typing import Any, ClassVar


class ExecutionMode(str, enum.Enum):
    """Execution mode for ingestion methods."""

    POLLING = "polling"
    LISTENER = "listener"
    EVENT_DRIVEN = "event_driven"


class BaseIngestionMethod(ABC):
    """
    Base class for all ingestion method plugins.

    Subclass PollingIngestionMethod instead of this class directly.
    """

    name: ClassVar[str]
    execution_mode: ClassVar[ExecutionMode]

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if ingestion method is healthy.

        Returns:
            True if healthy, False otherwise.
        """
        ...


class PollingIngestionMethod(BaseIngestionMethod):
    """
    Base class for polling ingestion methods.

    Plugins that declare "schedule me" — ``CollectorService`` calls
    ``poll_once()`` on interval with per-device config pulled from the
    device source's backing table and a ``target`` that selects which
    event table the decoded events land in.
    """

    execution_mode: ClassVar[ExecutionMode] = ExecutionMode.POLLING

    @abstractmethod
    async def poll_once(
        self,
        device_id: str,
        config: dict[str, Any],
        session_factory,
        *,
        target: Any = None,
    ) -> None:
        """
        Execute one poll cycle for a single device.

        Args:
            device_id: Device identifier.  For controller devices this
                is ``Signal.signal_id``; for sensor devices it is the
                stringified ``RoadsideSensor.sensor_id``.
            config: Collection config from the source's backing table
                (e.g. ``signal_metadata['collection']`` for controllers).
            session_factory: Async session factory for DB writes.
            target: ``IngestionTarget`` that determines where decoded
                events are written (``controller_event_log`` /
                ``roadside_event``) and which ``device_type`` is used
                for checkpoint I/O.  ``None`` defaults to
                ``ControllerTarget()`` for backward compatibility.
        """
        ...


class ListenerIngestionMethod(BaseIngestionMethod):
    """
    Base class for push/listener ingestion methods.

    Long-lived async servers that receive data pushed by external
    devices. CollectorService manages start/stop lifecycle.
    """

    execution_mode: ClassVar[ExecutionMode] = ExecutionMode.LISTENER

    @abstractmethod
    async def start(self, config: dict[str, Any], session_factory) -> None:
        """
        Start listening for incoming data.

        Args:
            config: Listener config (port, bind address, etc.).
            session_factory: Async session factory for DB writes.
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop listening and release resources."""
        ...


class EventDrivenIngestionMethod(BaseIngestionMethod):
    """
    Base class for event-driven ingestion methods.

    Watches for external events (e.g., filesystem changes) and
    ingests data when triggered. CollectorService manages
    start/stop lifecycle.
    """

    execution_mode: ClassVar[ExecutionMode] = ExecutionMode.EVENT_DRIVEN

    @abstractmethod
    async def start(self, config: dict[str, Any], session_factory) -> None:
        """
        Start watching for events.

        Args:
            config: Watcher config (directory path, patterns, etc.).
            session_factory: Async session factory for DB writes.
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop watching and release resources."""
        ...


class IngestionMethodRegistry:
    """
    Central registry for all ingestion method plugins.

    Methods self-register using the @IngestionMethodRegistry.register decorator.
    """

    _methods: dict[str, type[BaseIngestionMethod]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Register an ingestion method plugin.

        Usage:
            @IngestionMethodRegistry.register("ftp_pull")
            class FTPPullMethod(PollingIngestionMethod):
                ...

        Args:
            name: Method identifier (e.g., "ftp_pull", "http_pull").

        Returns:
            Decorator function.
        """
        def wrapper(method_class: type[BaseIngestionMethod]) -> type[BaseIngestionMethod]:
            cls._methods[name] = method_class
            return method_class
        return wrapper

    @classmethod
    def get(cls, name: str) -> type[BaseIngestionMethod]:
        """
        Get a registered ingestion method by name.

        Args:
            name: Method identifier.

        Returns:
            Ingestion method class.

        Raises:
            ValueError: If method not found.
        """
        if name not in cls._methods:
            raise ValueError(f"Unknown ingestion method: {name}")
        return cls._methods[name]

    @classmethod
    def list_available(cls) -> list[str]:
        """
        List all registered ingestion method names.

        Returns:
            List of method names.
        """
        return list(cls._methods.keys())

    @classmethod
    def get_polling_methods(cls) -> dict[str, type[PollingIngestionMethod]]:
        """
        Get all registered polling methods.

        Returns:
            Dictionary of name -> polling method class.
        """
        return {
            name: method_cls
            for name, method_cls in cls._methods.items()
            if getattr(method_cls, "execution_mode", None) == ExecutionMode.POLLING
        }

    @classmethod
    def get_listener_methods(cls) -> dict[str, type[ListenerIngestionMethod]]:
        """
        Get all registered listener methods.

        Returns:
            Dictionary of name -> listener method class.
        """
        return {
            name: method_cls
            for name, method_cls in cls._methods.items()
            if getattr(method_cls, "execution_mode", None) == ExecutionMode.LISTENER
        }

    @classmethod
    def get_event_driven_methods(cls) -> dict[str, type[EventDrivenIngestionMethod]]:
        """
        Get all registered event-driven methods.

        Returns:
            Dictionary of name -> event-driven method class.
        """
        return {
            name: method_cls
            for name, method_cls in cls._methods.items()
            if getattr(method_cls, "execution_mode", None) == ExecutionMode.EVENT_DRIVEN
        }
