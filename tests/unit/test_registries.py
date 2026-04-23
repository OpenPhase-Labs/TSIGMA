"""
Unit tests for plugin registries.

Tests registration, retrieval, and auto-discovery of plugins.
"""

import pytest

from tsigma.collection import BaseDecoder, DecodedEvent, DecoderRegistry
from tsigma.collection.registry import (
    ExecutionMode,
    IngestionMethodRegistry,
    PollingIngestionMethod,
)
from tsigma.reports import BaseReport, ReportRegistry
from tsigma.scheduler.registry import JobRegistry


class TestDecoderRegistry:
    """Tests for DecoderRegistry."""

    def test_register_decorator(self):
        """Test @DecoderRegistry.register decorator."""

        @DecoderRegistry.register
        class TestDecoder(BaseDecoder):
            name = "test"
            extensions = [".test"]
            description = "Test decoder"

            def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
                return []

            @classmethod
            def can_decode(cls, data: bytes) -> bool:
                return True

        assert "test" in DecoderRegistry.list_all()
        retrieved = DecoderRegistry.get("test")
        assert retrieved.name == "test"

    def test_get_nonexistent_raises(self):
        """Test getting nonexistent decoder raises ValueError."""
        with pytest.raises(ValueError, match="Unknown decoder"):
            DecoderRegistry.get("nonexistent-decoder")

    def test_get_for_extension(self):
        """Test getting decoders by file extension."""

        @DecoderRegistry.register
        class CSVDecoder(BaseDecoder):
            name = "csv_test"
            extensions = [".csv", ".txt"]
            description = "CSV test"

            def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
                return []

            @classmethod
            def can_decode(cls, data: bytes) -> bool:
                return True

        decoders = DecoderRegistry.get_for_extension(".csv")
        assert any(d.name == "csv_test" for d in decoders)


class TestJobRegistry:
    """Tests for JobRegistry."""

    def test_register_job(self):
        """Test @JobRegistry.register decorator."""

        @JobRegistry.register(name="test_job", trigger="interval", minutes=15)
        async def test_job(session):
            return "executed"

        assert "test_job" in JobRegistry.list_all()
        job_config = JobRegistry.get("test_job")
        assert job_config["trigger"] == "interval"
        assert job_config["trigger_kwargs"]["minutes"] == 15

    def test_register_decorator_includes_needs_session(self):
        """Test decorator registration includes needs_session=True."""

        @JobRegistry.register(name="test_needs_session", trigger="cron", hour="4")
        async def test_ns_job(session):
            pass

        config = JobRegistry.get("test_needs_session")
        assert config["needs_session"] is True

    def test_register_func(self):
        """Test programmatic registration via register_func."""

        async def dynamic_job():
            pass

        JobRegistry.register_func(
            name="dynamic_test",
            func=dynamic_job,
            trigger="interval",
            seconds=30,
        )

        assert "dynamic_test" in JobRegistry.list_all()
        config = JobRegistry.get("dynamic_test")
        assert config["func"] is dynamic_job
        assert config["trigger"] == "interval"
        assert config["trigger_kwargs"]["seconds"] == 30

    def test_register_func_needs_session_false(self):
        """Test register_func with needs_session=False."""

        async def no_session_job():
            pass

        JobRegistry.register_func(
            name="no_session_test",
            func=no_session_job,
            trigger="interval",
            needs_session=False,
            seconds=60,
        )

        config = JobRegistry.get("no_session_test")
        assert config["needs_session"] is False

    def test_register_func_needs_session_default_true(self):
        """Test register_func defaults needs_session to True."""

        async def default_job():
            pass

        JobRegistry.register_func(
            name="default_session_test",
            func=default_job,
            trigger="cron",
            hour="5",
        )

        config = JobRegistry.get("default_session_test")
        assert config["needs_session"] is True

    def test_unregister(self):
        """Test unregister removes a registered job."""

        @JobRegistry.register(name="to_remove", trigger="interval", seconds=10)
        async def removable_job(session):
            pass

        assert "to_remove" in JobRegistry.list_all()
        JobRegistry.unregister("to_remove")
        assert "to_remove" not in JobRegistry.list_all()

    def test_unregister_nonexistent_is_noop(self):
        """Test unregister with nonexistent name does not raise."""
        JobRegistry.unregister("definitely_does_not_exist")

    def test_get_nonexistent_raises(self):
        """Test getting nonexistent job raises ValueError."""
        with pytest.raises(ValueError, match="Unknown job"):
            JobRegistry.get("nonexistent-job")


class TestReportRegistry:
    """Tests for ReportRegistry."""

    def test_register_report(self):
        """Test @ReportRegistry.register decorator."""

        @ReportRegistry.register("test_report")
        class TestReport(BaseReport):
            name = "test"
            description = "Test report"
            category = "standard"
            estimated_time = "fast"

            async def execute(self, params, session):
                return {"result": "test"}

        assert "test_report" in ReportRegistry.list_all()
        report_cls = ReportRegistry.get("test_report")
        assert report_cls.name == "test"

        # Cleanup — don't pollute the global registry for other tests
        ReportRegistry._reports.pop("test_report", None)

    def test_get_nonexistent_raises(self):
        """Test getting nonexistent report raises ValueError."""
        with pytest.raises(ValueError, match="Unknown report"):
            ReportRegistry.get("nonexistent-report")


class TestExecutionMode:
    """Tests for ExecutionMode enum."""

    def test_polling_value(self):
        """Test polling mode string value."""
        assert ExecutionMode.POLLING == "polling"


class TestIngestionMethodRegistry:
    """Tests for IngestionMethodRegistry."""

    def test_register_polling_method(self):
        """Test registering a PollingIngestionMethod subclass."""

        @IngestionMethodRegistry.register("test_polling")
        class TestPoller(PollingIngestionMethod):
            name = "test_polling"

            async def poll_once(self, signal_id, config, session_factory):
                pass

            async def health_check(self):
                return True

        assert "test_polling" in IngestionMethodRegistry.list_available()
        cls = IngestionMethodRegistry.get("test_polling")
        assert cls is TestPoller
        assert cls.execution_mode == ExecutionMode.POLLING

    def test_get_polling_methods(self):
        """Test get_polling_methods returns only polling methods."""
        polling = IngestionMethodRegistry.get_polling_methods()
        for cls in polling.values():
            assert cls.execution_mode == ExecutionMode.POLLING

    def test_get_nonexistent_raises(self):
        """Test getting nonexistent method raises ValueError."""
        with pytest.raises(ValueError, match="Unknown ingestion method"):
            IngestionMethodRegistry.get("nonexistent-method")


class TestAuthProviderRegistry:
    """Tests for AuthProviderRegistry."""

    def test_get_unknown_provider_raises(self):
        """AuthProviderRegistry.get('unknown') raises ValueError."""
        from tsigma.auth.registry import AuthProviderRegistry

        with pytest.raises(ValueError, match="Unknown auth provider"):
            AuthProviderRegistry.get("nonexistent-provider")

    def test_get_unknown_provider_lists_available(self):
        """Error message includes available providers."""
        from tsigma.auth.registry import AuthProviderRegistry

        with pytest.raises(ValueError, match="Available:"):
            AuthProviderRegistry.get("bogus")
