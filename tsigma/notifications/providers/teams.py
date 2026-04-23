"""
Microsoft Teams notification provider.

Sends notifications to a Teams channel via Workflows/Power Automate
webhook using the Adaptive Card format.
"""

import logging
from datetime import datetime, timezone
from typing import ClassVar

import httpx

from tsigma.notifications.registry import (
    BaseNotificationProvider,
    NotificationRegistry,
)

logger = logging.getLogger(__name__)

_SEVERITY_COLORS: dict[str, str] = {
    "info": "good",
    "warning": "warning",
    "critical": "attention",
}


@NotificationRegistry.register("teams")
class TeamsProvider(BaseNotificationProvider):
    """Send notifications to Microsoft Teams via webhook."""

    name: ClassVar[str] = "teams"

    def __init__(self) -> None:
        self._webhook_url: str = ""

    async def initialize(self, settings) -> None:
        """
        Configure Teams webhook URL from application settings.

        Args:
            settings: TSIGMA Settings instance.
        """
        self._webhook_url = getattr(settings, "teams_webhook_url", "")
        if not self._webhook_url:
            logger.warning("Teams provider: teams_webhook_url not configured")

    async def send(
        self,
        subject: str,
        message: str,
        severity: str,
        metadata: dict | None = None,
    ) -> None:
        """
        Send a Teams notification via webhook using Adaptive Card format.

        Args:
            subject: Notification subject (used as card header).
            message: Notification body text.
            severity: Notification severity level.
            metadata: Ignored for Teams delivery.
        """
        if not self._webhook_url:
            logger.warning("Teams provider: skipping send — webhook URL not configured")
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        color_style = _SEVERITY_COLORS.get(severity, "default")

        card = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": subject,
                                "color": color_style,
                            },
                            {
                                "type": "TextBlock",
                                "text": message,
                                "wrap": True,
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {
                                        "title": "Severity",
                                        "value": severity.upper(),
                                    },
                                    {
                                        "title": "Timestamp",
                                        "value": timestamp,
                                    },
                                    {
                                        "title": "Source",
                                        "value": "TSIGMA",
                                    },
                                ],
                            },
                        ],
                    },
                }
            ],
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(self._webhook_url, json=card)
            resp.raise_for_status()

        logger.debug("Teams notification sent: %s", subject)
