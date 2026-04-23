"""Unit tests for the TSIGMA notification system."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.notifications.providers.email import EmailProvider
from tsigma.notifications.providers.slack import SlackProvider
from tsigma.notifications.providers.teams import TeamsProvider
from tsigma.notifications.registry import (
    CRITICAL,
    INFO,
    WARNING,
    BaseNotificationProvider,
    NotificationRegistry,
    _active_providers,
    initialize_providers,
    notify,
)

# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


class TestNotificationRegistry:
    """Tests for NotificationRegistry register / get / list."""

    def setup_method(self):
        """Snapshot registry state so tests are isolated."""
        self._original = dict(NotificationRegistry._providers)

    def teardown_method(self):
        """Restore registry state."""
        NotificationRegistry._providers.clear()
        NotificationRegistry._providers.update(self._original)

    def test_registry_register_and_get(self):
        """Register a provider class and retrieve it by name."""

        @NotificationRegistry.register("test_provider")
        class _TestProvider(BaseNotificationProvider):
            name = "test_provider"

            async def initialize(self, settings):
                pass

            async def send(self, subject, message, severity, metadata=None):
                pass

        retrieved = NotificationRegistry.get("test_provider")
        assert retrieved is _TestProvider

    def test_registry_unknown_raises(self):
        """Getting a non-existent provider raises ValueError."""
        with pytest.raises(ValueError, match="Unknown notification provider"):
            NotificationRegistry.get("does_not_exist")

    def test_registry_list_available(self):
        """list_available returns names of all registered providers."""
        available = NotificationRegistry.list_available()
        # email, slack, teams are registered at import time
        assert "email" in available
        assert "slack" in available
        assert "teams" in available


# ---------------------------------------------------------------------------
# Fan-out / severity filter tests
# ---------------------------------------------------------------------------


class TestNotifyFanOut:
    """Tests for the module-level notify() fan-out function."""

    def setup_method(self):
        self._saved = list(_active_providers)
        _active_providers.clear()

    def teardown_method(self):
        _active_providers.clear()
        _active_providers.extend(self._saved)

    @pytest.mark.asyncio
    async def test_notify_fans_out(self):
        """notify() delivers to every active provider."""
        provider_a = AsyncMock(spec=BaseNotificationProvider)
        provider_a.min_severity = WARNING
        provider_a.name = "mock_a"

        provider_b = AsyncMock(spec=BaseNotificationProvider)
        provider_b.min_severity = WARNING
        provider_b.name = "mock_b"

        _active_providers.extend([provider_a, provider_b])

        await notify("subj", "body", WARNING)

        provider_a.send.assert_awaited_once_with("subj", "body", WARNING, None)
        provider_b.send.assert_awaited_once_with("subj", "body", WARNING, None)

    @pytest.mark.asyncio
    async def test_notify_severity_filter(self):
        """Provider with min_severity=CRITICAL is skipped for WARNING."""
        provider = AsyncMock(spec=BaseNotificationProvider)
        provider.min_severity = CRITICAL
        provider.name = "strict"

        _active_providers.append(provider)

        await notify("subj", "body", WARNING)

        provider.send.assert_not_awaited()


# ---------------------------------------------------------------------------
# Email provider tests
# ---------------------------------------------------------------------------


class TestEmailProvider:
    """Tests for EmailProvider.send()."""

    @pytest.mark.asyncio
    async def test_email_provider_send(self):
        """EmailProvider.send() calls aiosmtplib.send with correct args."""
        provider = EmailProvider()
        provider._smtp_host = "smtp.example.com"
        provider._smtp_port = 587
        provider._smtp_username = "user"
        provider._smtp_password = "pass"
        provider._smtp_use_tls = True
        provider._from_email = "from@example.com"
        provider._to_emails = ["to@example.com"]

        with patch(
            "tsigma.notifications.providers.email.aiosmtplib",
            create=True,
        ) as mock_aiosmtplib:
            mock_aiosmtplib.send = AsyncMock()
            # aiosmtplib is imported inside send(), so we patch via
            # the module-level import mechanism
            with patch.dict(
                "sys.modules",
                {"aiosmtplib": mock_aiosmtplib},
            ):
                await provider.send("Alert", "Something happened", WARNING)

            mock_aiosmtplib.send.assert_awaited_once()
            call_kwargs = mock_aiosmtplib.send.call_args
            # First positional arg is the MIMEText message
            msg = call_kwargs[0][0]
            assert "[WARNING] Alert" in msg["Subject"]
            assert msg["From"] == "from@example.com"
            assert "to@example.com" in msg["To"]
            assert call_kwargs[1]["hostname"] == "smtp.example.com"
            assert call_kwargs[1]["port"] == 587
            assert call_kwargs[1]["username"] == "user"
            assert call_kwargs[1]["password"] == "pass"
            assert call_kwargs[1]["use_tls"] is True


# ---------------------------------------------------------------------------
# Slack provider tests
# ---------------------------------------------------------------------------


class TestSlackProvider:
    """Tests for SlackProvider.send()."""

    @pytest.mark.asyncio
    async def test_slack_provider_send(self):
        """SlackProvider.send() POSTs to the webhook URL."""
        provider = SlackProvider()
        provider._webhook_url = "https://hooks.slack.com/test"

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("tsigma.notifications.providers.slack.httpx.AsyncClient", return_value=mock_client):
            await provider.send("Alert", "Something happened", WARNING)

        mock_client.post.assert_awaited_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == "https://hooks.slack.com/test"
        payload = call_args[1]["json"]
        assert "attachments" in payload


# ---------------------------------------------------------------------------
# Teams provider tests
# ---------------------------------------------------------------------------


class TestTeamsProvider:
    """Tests for TeamsProvider.send()."""

    @pytest.mark.asyncio
    async def test_teams_provider_send(self):
        """TeamsProvider.send() POSTs to the webhook URL."""
        provider = TeamsProvider()
        provider._webhook_url = "https://outlook.office.com/webhook/test"

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("tsigma.notifications.providers.teams.httpx.AsyncClient", return_value=mock_client):
            await provider.send("Alert", "Something happened", WARNING)

        mock_client.post.assert_awaited_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == "https://outlook.office.com/webhook/test"
        payload = call_args[1]["json"]
        assert "attachments" in payload
        # Verify it's an Adaptive Card
        content = payload["attachments"][0]["content"]
        assert content["type"] == "AdaptiveCard"


# ---------------------------------------------------------------------------
# initialize_providers tests
# ---------------------------------------------------------------------------


class TestInitializeProviders:
    """Tests for initialize_providers()."""

    def setup_method(self):
        self._saved = list(_active_providers)
        _active_providers.clear()

    def teardown_method(self):
        _active_providers.clear()
        _active_providers.extend(self._saved)

    @pytest.mark.asyncio
    async def test_initialize_skips_unknown_provider(self):
        """initialize_providers logs warning for unknown provider (lines 156-157)."""
        mock_settings = MagicMock()
        mock_settings.notification_providers = "nonexistent_provider"

        await initialize_providers(mock_settings)

        # No providers should be active
        assert len(_active_providers) == 0

    @pytest.mark.asyncio
    async def test_initialize_empty_string_skips(self):
        """initialize_providers with empty string does nothing (lines 163-165)."""
        mock_settings = MagicMock()
        mock_settings.notification_providers = "   "

        await initialize_providers(mock_settings)

        assert len(_active_providers) == 0

    @pytest.mark.asyncio
    async def test_initialize_no_attribute_skips(self):
        """initialize_providers with missing attribute does nothing."""
        mock_settings = MagicMock(spec=[])

        await initialize_providers(mock_settings)

        assert len(_active_providers) == 0

    @pytest.mark.asyncio
    async def test_initialize_catches_provider_init_error(self):
        """initialize_providers catches exception from provider.initialize() (lines 172-173)."""
        mock_settings = MagicMock()
        mock_settings.notification_providers = "email"
        mock_settings.smtp_host = None  # Will be read but we'll force the error

        # Patch EmailProvider.initialize to raise
        with patch.object(
            EmailProvider, "initialize",
            new_callable=AsyncMock,
            side_effect=RuntimeError("SMTP connection failed"),
        ):
            await initialize_providers(mock_settings)

        # Provider failed to initialize, should not be in active list
        assert len(_active_providers) == 0

    @pytest.mark.asyncio
    async def test_initialize_providers_loads_email(self):
        """initialize_providers with 'email' instantiates and initializes the email provider."""
        mock_settings = MagicMock()
        mock_settings.notification_providers = "email"
        mock_settings.smtp_host = "smtp.example.com"
        mock_settings.smtp_port = 587
        mock_settings.smtp_username = ""
        mock_settings.smtp_password = ""
        mock_settings.smtp_use_tls = True
        mock_settings.notification_from_email = "test@example.com"
        mock_settings.notification_to_emails = "admin@example.com"

        await initialize_providers(mock_settings)

        assert len(_active_providers) == 1
        assert isinstance(_active_providers[0], EmailProvider)
        assert _active_providers[0]._smtp_host == "smtp.example.com"

    @pytest.mark.asyncio
    async def test_notify_no_providers(self):
        """notify() succeeds silently when no providers are configured."""
        _active_providers.clear()

        # Should not raise
        await notify("test subject", "test body", WARNING)


# ---------------------------------------------------------------------------
# EmailProvider.initialize tests
# ---------------------------------------------------------------------------


class TestEmailProviderInitialize:
    """Tests for EmailProvider.initialize()."""

    @pytest.mark.asyncio
    async def test_email_initialize_reads_settings(self):
        """EmailProvider.initialize() reads SMTP settings from the settings object."""
        provider = EmailProvider()
        mock_settings = MagicMock()
        mock_settings.smtp_host = "mail.example.com"
        mock_settings.smtp_port = 465
        mock_settings.smtp_username = "user@example.com"
        mock_settings.smtp_password = "secret"
        mock_settings.smtp_use_tls = False
        mock_settings.notification_from_email = "noreply@example.com"
        mock_settings.notification_to_emails = "a@b.com, c@d.com"

        await provider.initialize(mock_settings)

        assert provider._smtp_host == "mail.example.com"
        assert provider._smtp_port == 465
        assert provider._smtp_username == "user@example.com"
        assert provider._smtp_password == "secret"
        assert provider._smtp_use_tls is False
        assert provider._from_email == "noreply@example.com"
        assert provider._to_emails == ["a@b.com", "c@d.com"]


# ---------------------------------------------------------------------------
# Slack initialize tests
# ---------------------------------------------------------------------------


class TestSlackProviderInitialize:
    """Tests for SlackProvider.initialize()."""

    @pytest.mark.asyncio
    async def test_slack_initialize_sets_webhook(self):
        """SlackProvider.initialize() sets webhook URL from settings."""
        provider = SlackProvider()
        mock_settings = MagicMock()
        mock_settings.slack_webhook_url = "https://hooks.slack.com/services/T/B/X"

        await provider.initialize(mock_settings)

        assert provider._webhook_url == "https://hooks.slack.com/services/T/B/X"

    @pytest.mark.asyncio
    async def test_slack_initialize_empty_webhook(self):
        """SlackProvider.initialize() warns when webhook URL is empty."""
        provider = SlackProvider()
        mock_settings = MagicMock(spec=[])  # no attributes

        await provider.initialize(mock_settings)

        assert provider._webhook_url == ""

    @pytest.mark.asyncio
    async def test_slack_send_skips_when_no_webhook(self):
        """SlackProvider.send() skips when webhook URL is not configured."""
        provider = SlackProvider()
        provider._webhook_url = ""

        # Should not raise
        await provider.send("test", "body", WARNING)


# ---------------------------------------------------------------------------
# Teams initialize tests
# ---------------------------------------------------------------------------


class TestTeamsProviderInitialize:
    """Tests for TeamsProvider.initialize()."""

    @pytest.mark.asyncio
    async def test_teams_initialize_sets_webhook(self):
        """TeamsProvider.initialize() sets webhook URL from settings."""
        provider = TeamsProvider()
        mock_settings = MagicMock()
        mock_settings.teams_webhook_url = "https://outlook.office.com/webhook/abc"

        await provider.initialize(mock_settings)

        assert provider._webhook_url == "https://outlook.office.com/webhook/abc"

    @pytest.mark.asyncio
    async def test_teams_initialize_empty_webhook(self):
        """TeamsProvider.initialize() warns when webhook URL is empty."""
        provider = TeamsProvider()
        mock_settings = MagicMock(spec=[])

        await provider.initialize(mock_settings)

        assert provider._webhook_url == ""

    @pytest.mark.asyncio
    async def test_teams_send_skips_when_no_webhook(self):
        """TeamsProvider.send() skips when webhook URL is not configured."""
        provider = TeamsProvider()
        provider._webhook_url = ""

        # Should not raise
        await provider.send("test", "body", WARNING)


# ---------------------------------------------------------------------------
# Email initialize with TLS
# ---------------------------------------------------------------------------


class TestEmailInitializeWithTls:
    """Tests for EmailProvider.initialize() TLS configuration."""

    @pytest.mark.asyncio
    async def test_email_initialize_with_tls(self):
        """EmailProvider.initialize() reads smtp_use_tls=True correctly."""
        provider = EmailProvider()
        mock_settings = MagicMock()
        mock_settings.smtp_host = "smtp.gmail.com"
        mock_settings.smtp_port = 465
        mock_settings.smtp_username = "user@gmail.com"
        mock_settings.smtp_password = "apppassword"
        mock_settings.smtp_use_tls = True
        mock_settings.notification_from_email = "alerts@example.com"
        mock_settings.notification_to_emails = "admin@example.com"

        await provider.initialize(mock_settings)

        assert provider._smtp_use_tls is True
        assert provider._smtp_port == 465

    @pytest.mark.asyncio
    async def test_email_send_skips_incomplete_config(self):
        """EmailProvider.send() skips when config is incomplete."""
        provider = EmailProvider()
        provider._smtp_host = ""
        provider._from_email = ""
        provider._to_emails = []

        # Should not raise
        await provider.send("Alert", "Something happened", WARNING)


# ---------------------------------------------------------------------------
# Notify error handling
# ---------------------------------------------------------------------------


class TestNotifyErrorHandling:
    """Tests for notify() exception isolation."""

    def setup_method(self):
        self._saved = list(_active_providers)
        _active_providers.clear()

    def teardown_method(self):
        _active_providers.clear()
        _active_providers.extend(self._saved)

    @pytest.mark.asyncio
    async def test_notify_catches_provider_error(self):
        """notify() catches and logs exceptions from providers."""
        failing_provider = AsyncMock(spec=BaseNotificationProvider)
        failing_provider.min_severity = INFO
        failing_provider.name = "failing"
        failing_provider.send = AsyncMock(side_effect=RuntimeError("webhook down"))

        ok_provider = AsyncMock(spec=BaseNotificationProvider)
        ok_provider.min_severity = INFO
        ok_provider.name = "ok"

        _active_providers.extend([failing_provider, ok_provider])

        # Should not raise
        await notify("subj", "body", WARNING)

        # Both providers should have been attempted
        failing_provider.send.assert_awaited_once()
        ok_provider.send.assert_awaited_once()
