"""
TSIGMA Application Entrypoint.

Run with: python -m tsigma.main
Or: uvicorn tsigma.main:app --reload
"""

import uvicorn

from .config import settings
from .logging import build_log_config


def main() -> None:
    """
    Run the TSIGMA application.

    Starts uvicorn server with configuration from settings.
    """
    uvicorn.run(
        "tsigma.app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
        log_config=build_log_config(settings.log_level, settings.log_format),
    )


if __name__ == "__main__":
    main()
