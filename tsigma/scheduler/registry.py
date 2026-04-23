"""
Background job registry for TSIGMA.

Jobs are self-registering plugins that run on schedules (cron, interval, etc.).
"""

from typing import Callable


class JobRegistry:
    """
    Central registry for all background job plugins.

    Jobs self-register using the @JobRegistry.register decorator.
    """

    _jobs: dict[str, dict] = {}

    @classmethod
    def register(cls, name: str, trigger: str, **trigger_kwargs):
        """
        Register a background job plugin.

        Usage:
            @JobRegistry.register(name="refresh_detector_volume", trigger="cron", hour="3")
            async def refresh_detector_volume_job(session):
                # Job logic here
                ...

        Args:
            name: Job identifier.
            trigger: APScheduler trigger type ("cron", "interval", "date").
            **trigger_kwargs: Trigger-specific arguments (hour="3", minutes=15, etc.).

        Returns:
            Decorator function.
        """
        def wrapper(job_func: Callable) -> Callable:
            cls._jobs[name] = {
                "func": job_func,
                "trigger": trigger,
                "trigger_kwargs": trigger_kwargs,
                "needs_session": True,
            }
            return job_func
        return wrapper

    @classmethod
    def register_func(
        cls,
        name: str,
        func: Callable,
        trigger: str,
        *,
        needs_session: bool = True,
        **trigger_kwargs,
    ) -> None:
        """
        Register a job programmatically (non-decorator).

        Use this when the job function is created dynamically
        (e.g. bound methods, partials) rather than at import time.

        Args:
            name: Job identifier.
            func: Async callable to execute.
            trigger: APScheduler trigger type ("cron", "interval", "date").
            needs_session: Whether load_registry should inject a DB session.
            **trigger_kwargs: Trigger-specific arguments.
        """
        cls._jobs[name] = {
            "func": func,
            "trigger": trigger,
            "trigger_kwargs": trigger_kwargs,
            "needs_session": needs_session,
        }

    @classmethod
    def unregister(cls, name: str) -> None:
        """
        Remove a registered job by name.

        No-op if the job doesn't exist.

        Args:
            name: Job identifier.
        """
        cls._jobs.pop(name, None)

    @classmethod
    def get(cls, name: str) -> dict:
        """
        Get a registered job by name.

        Args:
            name: Job identifier.

        Returns:
            Job configuration dict.

        Raises:
            ValueError: If job not found.
        """
        if name not in cls._jobs:
            raise ValueError(f"Unknown job: {name}")
        return cls._jobs[name]

    @classmethod
    def list_all(cls) -> dict[str, dict]:
        """
        List all registered jobs.

        Returns:
            Dictionary of job name -> job config.
        """
        return cls._jobs.copy()
