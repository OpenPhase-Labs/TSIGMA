"""
Centralized logging configuration for TSIGMA.

Configures Python's logging system with JSON or console formatting
based on application settings. Call setup_logging() once at startup
before any other module logs.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JSONFormatter(logging.Formatter):
    """
    Structured JSON log formatter.

    Produces one JSON object per log line with consistent field ordering.
    Includes exception info and extra fields when present.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as a JSON string.

        Args:
            record: Log record to format.

        Returns:
            Single-line JSON string.
        """
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, default=str)


class ConsoleFormatter(logging.Formatter):
    """
    Human-readable console formatter.

    Fixed-width level name for aligned output.
    """

    def __init__(self) -> None:
        """Initialize with standard format string."""
        super().__init__(
            fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def setup_logging(log_level: str = "INFO", log_format: str = "json") -> None:
    """
    Configure the root logger for the TSIGMA application.

    Sets up a single stderr handler with the chosen formatter.
    Clears any existing handlers to avoid duplicate output.
    Quiets noisy third-party loggers.

    Use this for non-uvicorn contexts (tests, CLI scripts).
    When running under uvicorn, use build_log_config() instead.

    Args:
        log_level: Python log level name (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_format: Output format — "json" for structured JSON, "console" for
                    human-readable text.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    if log_format == "json":
        formatter: logging.Formatter = JSONFormatter()
    else:
        formatter = ConsoleFormatter()

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)

    # Quiet noisy third-party loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


def build_log_config(log_level: str = "INFO", log_format: str = "json") -> dict:
    """
    Build a uvicorn-compatible logging dictConfig.

    Plugs TSIGMA formatters into uvicorn's logging pipeline so all
    output (uvicorn, access, application) uses the same format while
    preserving uvicorn's handler and stream management.

    Args:
        log_level: Python log level name.
        log_format: Output format — "json" or "console".

    Returns:
        Dictionary suitable for uvicorn's log_config parameter.
    """
    formatter_class = (
        "tsigma.logging.JSONFormatter"
        if log_format == "json"
        else "tsigma.logging.ConsoleFormatter"
    )

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {"()": formatter_class},
            "access": {"()": formatter_class},
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["default"],
                "level": log_level.upper(),
                "propagate": False,
            },
            "uvicorn.error": {
                "level": log_level.upper(),
            },
            "uvicorn.access": {
                "handlers": ["access"],
                "level": log_level.upper(),
                "propagate": False,
            },
            "apscheduler": {
                "handlers": ["default"],
                "level": "WARNING",
                "propagate": False,
            },
            "sqlalchemy.engine": {
                "handlers": ["default"],
                "level": "WARNING",
                "propagate": False,
            },
        },
        "root": {
            "handlers": ["default"],
            "level": log_level.upper(),
        },
    }
