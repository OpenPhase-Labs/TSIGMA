"""
Email notification provider.

Sends notifications via SMTP using aiosmtplib for async delivery.
"""

import logging
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import ClassVar

from tsigma.notifications.registry import (
    BaseNotificationProvider,
    NotificationRegistry,
)

logger = logging.getLogger(__name__)


@NotificationRegistry.register("email")
class EmailProvider(BaseNotificationProvider):
    """Send notifications via SMTP email."""

    name: ClassVar[str] = "email"

    def __init__(self) -> None:
        self._smtp_host: str = ""
        self._smtp_port: int = 587
        self._smtp_username: str = ""
        self._smtp_password: str = ""
        self._smtp_use_tls: bool = True
        self._from_email: str = ""
        self._to_emails: list[str] = []

    async def initialize(self, settings) -> None:
        """
        Configure SMTP connection parameters from application settings.

        Args:
            settings: TSIGMA Settings instance.
        """
        self._smtp_host = getattr(settings, "smtp_host", "")
        self._smtp_port = getattr(settings, "smtp_port", 587)
        self._smtp_username = getattr(settings, "smtp_username", "")
        self._smtp_password = getattr(settings, "smtp_password", "")
        self._smtp_use_tls = getattr(settings, "smtp_use_tls", True)
        self._from_email = getattr(settings, "notification_from_email", "")
        raw_to = getattr(settings, "notification_to_emails", "")
        self._to_emails = [
            e.strip() for e in raw_to.split(",") if e.strip()
        ]

        if not self._smtp_host:
            logger.warning("Email provider: smtp_host not configured")
        if not self._from_email:
            logger.warning("Email provider: notification_from_email not configured")
        if not self._to_emails:
            logger.warning("Email provider: notification_to_emails not configured")

    async def send(
        self,
        subject: str,
        message: str,
        severity: str,
        metadata: dict | None = None,
    ) -> None:
        """
        Send an email notification.

        Args:
            subject: Email subject (severity prefix is prepended).
            message: Plain-text email body.
            severity: Notification severity level.
            metadata: Ignored for email delivery.
        """
        if not self._smtp_host or not self._from_email or not self._to_emails:
            logger.warning("Email provider: skipping send — incomplete configuration")
            return

        import aiosmtplib

        tag = severity.upper()
        full_subject = f"[{tag}] {subject}"

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        body = f"{message}\n\n---\nSent by TSIGMA at {timestamp}"

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = full_subject
        msg["From"] = self._from_email
        msg["To"] = ", ".join(self._to_emails)

        await aiosmtplib.send(
            msg,
            hostname=self._smtp_host,
            port=self._smtp_port,
            username=self._smtp_username or None,
            password=self._smtp_password or None,
            use_tls=self._smtp_use_tls,
        )

        logger.debug("Email notification sent: %s", full_subject)
