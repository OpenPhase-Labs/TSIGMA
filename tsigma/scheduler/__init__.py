"""
Scheduler (background jobs) system.

Jobs are self-registering plugins that run on schedules.
SchedulerService provides the APScheduler abstraction layer.
"""

# Import jobs to trigger auto-discovery
from . import jobs
from .registry import JobRegistry
from .service import SchedulerService

__all__ = ["JobRegistry", "SchedulerService", "jobs"]
