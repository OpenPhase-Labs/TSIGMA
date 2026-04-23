"""
Slack notification provider.

Sends notifications to a Slack channel via incoming webhook using
Slack Block Kit formatting.
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
    "info": "#36a64f",      # green
    "warning": "#ffcc00",   # yellow
    "critical": "#ff0000",  # red
}


@NotificationRegistry.register("slack")
class SlackProvider(BaseNotificationProvider):
    """Send notifications to Slack via incoming webhook."""

    name: ClassVar[str] = "slack"

    def __init__(self) -> None:
        self._webhook_url: str = ""

    async def initialize(self, settings) -> None:
        """
        Configure Slack webhook URL from application settings.

        Args:
            settings: TSIGMA Settings instance.
        """
        self._webhook_url = getattr(settings, "slack_webhook_url", "")
        if not self._webhook_url:
            logger.warning("Slack provider: slack_webhook_url not configured")

    async def send(
        self,
        subject: str,
        message: str,
        severity: str,
        metadata: dict | None = None,
    ) -> None:
        """
        Send a Slack notification via webhook.

        Args:
            subject: Notification subject (used as header block).
            message: Notification body text.
            severity: Notification severity level.
            metadata: Ignored for Slack delivery.
        """
        if not self._webhook_url:
            logger.warning("Slack provider: skipping send — webhook URL not configured")
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        color = _SEVERITY_COLORS.get(severity, _SEVERITY_COLORS["info"])

        payload = {
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": subject,
                                "emoji": False,
                            },
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": message,
                            },
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": (
                                        f"*Severity:* {severity.upper()}"
                                        f"  |  *Time:* {timestamp}"
                                        f"  |  *Source:* TSIGMA"
                                    ),
                                },
                            ],
                        },
                    ],
                }
            ]
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(self._webhook_url, json=payload)
            resp.raise_for_status()

        logger.debug("Slack notification sent: %s", subject)
