"""
Notification provider registry for TSIGMA.

Notification providers are self-registering plugins that deliver alerts
via external channels (email, Slack, Teams, etc.). Multiple providers
can be active simultaneously — the shared ``notify()`` function fans out
to every initialized provider whose minimum severity threshold is met.
"""

import logging
from abc import ABC, abstractmethod
from typing import ClassVar

logger = logging.getLogger(__name__)

# Severity levels (ordered lowest to highest)
INFO = "info"
WARNING = "warning"
CRITICAL = "critical"

_SEVERITY_ORDER: dict[str, int] = {
    INFO: 0,
    WARNING: 1,
    CRITICAL: 2,
}

# Module-level list populated during app startup via initialize_providers()
_active_providers: list["BaseNotificationProvider"] = []


class BaseNotificationProvider(ABC):
    """
    Base class for all notification provider plugins.

    Subclass this and decorate with @NotificationRegistry.register("name")
    to create a new notification provider plugin.
    """

    name: ClassVar[str]
    min_severity: str = WARNING

    @abstractmethod
    async def initialize(self, settings) -> None:
        """
        Configure the provider from application settings.

        Called once during app lifespan startup. Validate required config
        and prepare connection parameters. Should log a warning and return
        (not raise) if configuration is missing or invalid.

        Args:
            settings: TSIGMA Settings instance.
        """
        ...

    @abstractmethod
    async def send(
        self,
        subject: str,
        message: str,
        severity: str,
        metadata: dict | None = None,
    ) -> None:
        """
        Send a notification.

        Args:
            subject: Short summary / email subject line.
            message: Full notification body (plain text).
            severity: One of INFO, WARNING, CRITICAL.
            metadata: Optional structured data for the notification.
        """
        ...


class NotificationRegistry:
    """
    Central registry for all notification provider plugins.

    Providers self-register using the @NotificationRegistry.register decorator.
    """

    _providers: dict[str, type[BaseNotificationProvider]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Register a notification provider plugin.

        Usage:
            @NotificationRegistry.register("email")
            class EmailProvider(BaseNotificationProvider):
                ...

        Args:
            name: Provider identifier (e.g., "email", "slack", "teams").

        Returns:
            Decorator function.
        """
        def wrapper(
            provider_class: type[BaseNotificationProvider],
        ) -> type[BaseNotificationProvider]:
            cls._providers[name] = provider_class
            return provider_class
        return wrapper

    @classmethod
    def get(cls, name: str) -> type[BaseNotificationProvider]:
        """
        Get a registered provider by name.

        Args:
            name: Provider identifier.

        Returns:
            Provider class.

        Raises:
            ValueError: If provider not found.
        """
        if name not in cls._providers:
            available = ", ".join(cls._providers.keys()) or "(none)"
            raise ValueError(
                f"Unknown notification provider: {name!r}. Available: {available}"
            )
        return cls._providers[name]

    @classmethod
    def list_available(cls) -> list[str]:
        """
        List all registered provider names.

        Returns:
            List of provider name strings.
        """
        return list(cls._providers.keys())


async def initialize_providers(settings) -> None:
    """
    Instantiate and initialize all configured notification providers.

    Reads ``settings.notification_providers`` (comma-separated string),
    looks each up in the registry, calls ``initialize()``, and appends
    to the module-level ``_active_providers`` list.

    Args:
        settings: TSIGMA Settings instance.
    """
    global _active_providers
    _active_providers.clear()

    raw = getattr(settings, "notification_providers", "")
    if not raw or not raw.strip():
        logger.info("No notification providers configured")
        return

    names = [n.strip() for n in raw.split(",") if n.strip()]
    for name in names:
        try:
            provider_cls = NotificationRegistry.get(name)
        except ValueError:
            logger.warning("Skipping unknown notification provider: %s", name)
            continue

        provider = provider_cls()
        try:
            await provider.initialize(settings)
            _active_providers.append(provider)
            logger.info("Notification provider initialized: %s", name)
        except Exception:
            logger.exception(
                "Failed to initialize notification provider: %s", name
            )


async def notify(
    subject: str,
    message: str,
    severity: str = WARNING,
    metadata: dict | None = None,
) -> None:
    """
    Fan out a notification to all active providers.

    Providers whose ``min_severity`` is higher than the notification
    severity are silently skipped. Exceptions from individual providers
    are caught and logged so that one failure never blocks the others.

    This function never raises.

    Args:
        subject: Short summary / email subject line.
        message: Full notification body (plain text).
        severity: One of INFO, WARNING, CRITICAL.
        metadata: Optional structured data for the notification.
    """
    notification_level = _SEVERITY_ORDER.get(severity, 0)

    for provider in _active_providers:
        provider_level = _SEVERITY_ORDER.get(provider.min_severity, 0)
        if notification_level < provider_level:
            continue
        try:
            await provider.send(subject, message, severity, metadata)
        except Exception:
            logger.exception(
                "Notification provider %s failed to send", provider.name
            )
