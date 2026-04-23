"""
Scheduler service — APScheduler abstraction layer.

Provides a consistent interface for job lifecycle management.
A single SchedulerService instance runs all jobs (polling,
cron, etc.) loaded from JobRegistry.
"""

import logging

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .registry import JobRegistry

logger = logging.getLogger(__name__)


class SchedulerService:
    """
    Thin wrapper around APScheduler for consistent job lifecycle.

    Supports interval, cron, and date triggers. Optionally injects
    a DB session into job functions when a session_factory is provided.

    Args:
        session_factory: Optional async session factory for DB access.
            When set, load_registry() wraps job functions with session
            injection (open session, call job, commit/rollback).
    """

    def __init__(self, session_factory=None):
        self._scheduler = None
        self._session_factory = session_factory

    @property
    def running(self) -> bool:
        """Whether the scheduler is currently running."""
        return self._scheduler is not None and self._scheduler.running

    async def start(self) -> None:
        """
        Create and start the APScheduler.

        Safe to call multiple times — restarts if already running.
        """
        if self._scheduler and self._scheduler.running:
            return

        self._scheduler = AsyncIOScheduler()
        self._scheduler.start()
        logger.info("SchedulerService started")

    async def stop(self) -> None:
        """
        Shutdown the scheduler gracefully.

        Safe to call multiple times or before start.
        """
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("SchedulerService stopped")
        self._scheduler = None

    def add_job(
        self,
        func,
        trigger: str,
        *,
        job_id: str = None,
        name: str = None,
        max_instances: int = 1,
        args: list = None,
        **trigger_kwargs,
    ) -> None:
        """
        Add a job to the scheduler.

        Args:
            func: Callable to execute.
            trigger: Trigger type ("interval", "cron", "date").
            job_id: Unique job identifier.
            name: Human-readable job name.
            max_instances: Max concurrent instances of this job.
            args: Positional arguments passed to func.
            **trigger_kwargs: Trigger-specific arguments
                (e.g. seconds=60, hour=3, minute=0).
        """
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            name=name,
            max_instances=max_instances,
            args=args,
            **trigger_kwargs,
        )

    def remove_job(self, job_id: str) -> None:
        """
        Remove a job by ID.

        Args:
            job_id: Job identifier to remove.
        """
        try:
            self._scheduler.remove_job(job_id)
        except JobLookupError:
            logger.debug("Job not found for removal: %s", job_id)

    def load_registry(self) -> None:
        """
        Load all jobs from JobRegistry into the scheduler.

        Jobs with needs_session=True get wrapped with session injection
        (open session, call job, commit/rollback). Jobs with
        needs_session=False (e.g. polling cycles) are added directly.
        """
        for name, config in JobRegistry.list_all().items():
            func = config["func"]
            trigger = config["trigger"]
            trigger_kwargs = config["trigger_kwargs"]

            if self._session_factory and config.get("needs_session", True):
                func = self._wrap_with_session(func)

            self.add_job(
                func,
                trigger,
                job_id=name,
                name=name,
                **trigger_kwargs,
            )
            logger.info("Loaded job from registry: %s (%s)", name, trigger)

    def _wrap_with_session(self, func):
        """
        Wrap a job function to inject a DB session.

        Opens a session, passes it to the job function, and handles
        commit on success or rollback on failure.

        Args:
            func: Async job function expecting a session parameter.

        Returns:
            Wrapped async function.
        """
        session_factory = self._session_factory

        async def wrapper():
            async with session_factory() as session:
                try:
                    await func(session)
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise

        return wrapper
