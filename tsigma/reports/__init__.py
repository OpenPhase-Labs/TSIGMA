"""
Report plugins auto-discovery.

Automatically imports all report modules to trigger @ReportRegistry.register decorators.
"""

from pathlib import Path

from .registry import BaseReport, ReportRegistry

# Auto-discover and import all report modules
reports_dir = Path(__file__).parent
for module_file in reports_dir.glob("*.py"):
    if module_file.stem not in ("__init__", "registry"):
        __import__(f"tsigma.reports.{module_file.stem}")

__all__ = ["BaseReport", "ReportRegistry"]
