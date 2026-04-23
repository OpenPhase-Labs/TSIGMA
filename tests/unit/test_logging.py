"""
Unit tests for the logging module.

Tests JSON and console formatters, setup_logging configuration,
and third-party logger quieting.
"""

import json
import logging

from tsigma.logging import ConsoleFormatter, JSONFormatter, build_log_config, setup_logging


class TestJSONFormatter:
    """Tests for JSONFormatter."""

    def test_formats_as_valid_json(self):
        """Test output is valid JSON."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="tsigma.test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=None,
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert isinstance(parsed, dict)

    def test_includes_required_fields(self):
        """Test output contains timestamp, level, logger, and message."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="tsigma.collection",
            level=logging.WARNING,
            pathname="test.py",
            lineno=10,
            msg="something happened",
            args=None,
            exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert "timestamp" in parsed
        assert parsed["level"] == "WARNING"
        assert parsed["logger"] == "tsigma.collection"
        assert parsed["message"] == "something happened"

    def test_timestamp_is_iso_format(self):
        """Test timestamp is ISO 8601 with timezone."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="msg",
            args=None,
            exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert "T" in parsed["timestamp"]
        assert "+" in parsed["timestamp"] or "Z" in parsed["timestamp"]

    def test_formats_message_with_args(self):
        """Test %-style message formatting with arguments."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="signal %s polled in %dms",
            args=("SIG-001", 42),
            exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert parsed["message"] == "signal SIG-001 polled in 42ms"

    def test_includes_exception_info(self):
        """Test exception traceback is included when present."""
        formatter = JSONFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="failed",
            args=None,
            exc_info=exc_info,
        )
        parsed = json.loads(formatter.format(record))
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]
        assert "test error" in parsed["exception"]

    def test_no_exception_key_without_error(self):
        """Test exception key is absent when no exception."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="ok",
            args=None,
            exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert "exception" not in parsed

    def test_output_is_single_line(self):
        """Test JSON output has no embedded newlines."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="line one",
            args=None,
            exc_info=None,
        )
        output = formatter.format(record)
        assert "\n" not in output


class TestConsoleFormatter:
    """Tests for ConsoleFormatter."""

    def test_includes_level_and_logger(self):
        """Test output contains level name and logger name."""
        formatter = ConsoleFormatter()
        record = logging.LogRecord(
            name="tsigma.scheduler",
            level=logging.DEBUG,
            pathname="test.py",
            lineno=1,
            msg="tick",
            args=None,
            exc_info=None,
        )
        output = formatter.format(record)
        assert "DEBUG" in output
        assert "tsigma.scheduler" in output
        assert "tick" in output

    def test_level_is_fixed_width(self):
        """Test level name is padded for alignment."""
        formatter = ConsoleFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="msg",
            args=None,
            exc_info=None,
        )
        output = formatter.format(record)
        # "INFO    " padded to 8 chars within brackets
        assert "[INFO    ]" in output

    def test_includes_timestamp(self):
        """Test output starts with a timestamp."""
        formatter = ConsoleFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="msg",
            args=None,
            exc_info=None,
        )
        output = formatter.format(record)
        # Should contain date-like pattern YYYY-MM-DD
        assert "-" in output.split(" ")[0]


class TestSetupLogging:
    """Tests for setup_logging()."""

    def _reset_root_logger(self):
        """Reset root logger to default state."""
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_sets_root_level(self):
        """Test root logger level is set from parameter."""
        self._reset_root_logger()
        setup_logging(log_level="DEBUG", log_format="console")
        assert logging.getLogger().level == logging.DEBUG

    def test_sets_root_level_case_insensitive(self):
        """Test log level is case-insensitive."""
        self._reset_root_logger()
        setup_logging(log_level="warning", log_format="console")
        assert logging.getLogger().level == logging.WARNING

    def test_invalid_level_defaults_to_info(self):
        """Test invalid log level falls back to INFO."""
        self._reset_root_logger()
        setup_logging(log_level="NONEXISTENT", log_format="console")
        assert logging.getLogger().level == logging.INFO

    def test_json_format_uses_json_formatter(self):
        """Test JSON format installs JSONFormatter."""
        self._reset_root_logger()
        setup_logging(log_level="INFO", log_format="json")
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JSONFormatter)

    def test_console_format_uses_console_formatter(self):
        """Test console format installs ConsoleFormatter."""
        self._reset_root_logger()
        setup_logging(log_level="INFO", log_format="console")
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, ConsoleFormatter)

    def test_clears_existing_handlers(self):
        """Test setup clears any pre-existing handlers."""
        self._reset_root_logger()
        root = logging.getLogger()
        root.addHandler(logging.StreamHandler())
        root.addHandler(logging.StreamHandler())
        assert len(root.handlers) == 2

        setup_logging(log_level="INFO", log_format="json")
        assert len(root.handlers) == 1

    def test_quiets_uvicorn_access(self):
        """Test uvicorn.access logger is set to WARNING."""
        self._reset_root_logger()
        setup_logging(log_level="DEBUG", log_format="json")
        assert logging.getLogger("uvicorn.access").level == logging.WARNING

    def test_quiets_apscheduler(self):
        """Test apscheduler logger is set to WARNING."""
        self._reset_root_logger()
        setup_logging(log_level="DEBUG", log_format="json")
        assert logging.getLogger("apscheduler").level == logging.WARNING

    def test_quiets_sqlalchemy(self):
        """Test sqlalchemy.engine logger is set to WARNING."""
        self._reset_root_logger()
        setup_logging(log_level="DEBUG", log_format="json")
        assert logging.getLogger("sqlalchemy.engine").level == logging.WARNING

    def test_handler_writes_to_stderr(self):
        """Test handler outputs to stderr."""
        import sys

        self._reset_root_logger()
        setup_logging(log_level="INFO", log_format="json")
        handler = logging.getLogger().handlers[0]
        assert handler.stream is sys.stderr

    def test_child_loggers_inherit_level(self):
        """Test module loggers inherit root level."""
        self._reset_root_logger()
        setup_logging(log_level="DEBUG", log_format="json")
        child = logging.getLogger("tsigma.collection.service")
        assert child.getEffectiveLevel() == logging.DEBUG

    def test_default_parameters(self):
        """Test defaults are INFO level and JSON format."""
        self._reset_root_logger()
        setup_logging()
        root = logging.getLogger()
        assert root.level == logging.INFO
        assert isinstance(root.handlers[0].formatter, JSONFormatter)


class TestBuildLogConfig:
    """Tests for build_log_config() uvicorn dictConfig builder."""

    def test_returns_valid_dictconfig(self):
        """Test output has required dictConfig keys."""
        config = build_log_config()
        assert config["version"] == 1
        assert "formatters" in config
        assert "handlers" in config
        assert "loggers" in config
        assert "root" in config

    def test_json_format_uses_json_formatter(self):
        """Test JSON format references JSONFormatter class."""
        config = build_log_config(log_format="json")
        assert config["formatters"]["default"]["()"] == "tsigma.logging.JSONFormatter"
        assert config["formatters"]["access"]["()"] == "tsigma.logging.JSONFormatter"

    def test_console_format_uses_console_formatter(self):
        """Test console format references ConsoleFormatter class."""
        config = build_log_config(log_format="console")
        assert config["formatters"]["default"]["()"] == "tsigma.logging.ConsoleFormatter"
        assert config["formatters"]["access"]["()"] == "tsigma.logging.ConsoleFormatter"

    def test_sets_root_level(self):
        """Test root logger level matches parameter."""
        config = build_log_config(log_level="DEBUG")
        assert config["root"]["level"] == "DEBUG"

    def test_level_uppercased(self):
        """Test log level is uppercased in output."""
        config = build_log_config(log_level="debug")
        assert config["root"]["level"] == "DEBUG"

    def test_handlers_write_to_stderr(self):
        """Test all handlers use stderr stream."""
        config = build_log_config()
        assert config["handlers"]["default"]["stream"] == "ext://sys.stderr"
        assert config["handlers"]["access"]["stream"] == "ext://sys.stderr"

    def test_uvicorn_loggers_configured(self):
        """Test uvicorn loggers are present in config."""
        config = build_log_config(log_level="INFO")
        assert "uvicorn" in config["loggers"]
        assert "uvicorn.error" in config["loggers"]
        assert "uvicorn.access" in config["loggers"]

    def test_uvicorn_loggers_use_app_level(self):
        """Test uvicorn loggers inherit the application log level."""
        config = build_log_config(log_level="WARNING")
        assert config["loggers"]["uvicorn"]["level"] == "WARNING"
        assert config["loggers"]["uvicorn.access"]["level"] == "WARNING"

    def test_noisy_loggers_set_to_warning(self):
        """Test apscheduler and sqlalchemy are quieted."""
        config = build_log_config(log_level="DEBUG")
        assert config["loggers"]["apscheduler"]["level"] == "WARNING"
        assert config["loggers"]["sqlalchemy.engine"]["level"] == "WARNING"

    def test_disable_existing_loggers_false(self):
        """Test existing loggers are not disabled."""
        config = build_log_config()
        assert config["disable_existing_loggers"] is False

    def test_uvicorn_loggers_no_propagate(self):
        """Test uvicorn loggers don't propagate to root."""
        config = build_log_config()
        assert config["loggers"]["uvicorn"]["propagate"] is False
        assert config["loggers"]["uvicorn.access"]["propagate"] is False

    def test_default_parameters(self):
        """Test defaults are INFO level and JSON format."""
        config = build_log_config()
        assert config["root"]["level"] == "INFO"
        assert config["formatters"]["default"]["()"] == "tsigma.logging.JSONFormatter"
