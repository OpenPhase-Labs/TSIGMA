"""
Background job plugins auto-discovery.

Automatically imports all job modules to trigger @JobRegistry.register decorators.
"""

from pathlib import Path

# Auto-discover and import all job modules
jobs_dir = Path(__file__).parent
for module_file in jobs_dir.glob("*.py"):
    if module_file.stem != "__init__":
        __import__(f"tsigma.scheduler.jobs.{module_file.stem}")
