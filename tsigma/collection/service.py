"""
Collector service — orchestration layer for ingestion methods.

Discovers polling methods from ``IngestionMethodRegistry`` and, for
each registered ``DeviceSource``, registers one interval job per
``(transport_method × source)`` pair at the source's own cadence.

This means controllers and roadside sensors can feed through the same
transport (``ftp_pull``, ``http_pull``, ...) at different poll
intervals and write to different event tables without the orchestrator
knowing about either — that's the job of the target the source is
paired with.

Uses an asyncio.Semaphore to bound concurrent poll operations across
all devices of all sources.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Optional

from sqlalchemy import select

from ..models.checkpoint import PollingCheckpoint
from ..notifications.registry import CRITICAL, WARNING, notify
from ..scheduler.registry import JobRegistry
from .registry import IngestionMethodRegistry, PollingIngestionMethod
from .sources import DeviceSource, SignalDeviceSource
from .targets import ControllerTarget

logger = logging.getLogger(__name__)


class CollectorService:
    """
    Orchestration service for ingestion method plugins.

    For every ``(polling_method × device_source)`` pair the service
    registers an interval job that triggers ``_run_poll_cycle(method,
    source)``.  Each cycle asks the source which of its devices are
    configured for this method, fans out to semaphore-bounded
    ``_process_device()`` calls, then runs silent-device detection
    scoped to the source's ``device_type``.
    """

    def __init__(
        self,
        session_factory,
        settings,
        *,
        sources: Optional[list[DeviceSource]] = None,
    ) -> None:
        """
        Initialize the collector service.

        Args:
            session_factory: Async session factory for DB access.
            settings: Application settings (collector_max_concurrent,
                      collector_poll_interval, sensor_poll_interval).
            sources: Device sources to poll.  When omitted, a default
                     ``[SignalDeviceSource(target=ControllerTarget(),
                     poll_interval_seconds=collector_poll_interval)]``
                     is built — preserving today's controller-only
                     behaviour.
        """
        self._session_factory = session_factory
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.collector_max_concurrent)
        self._polling_instances: dict[str, PollingIngestionMethod] = {}
        self._sources: list[DeviceSource] = (
            sources if sources is not None else [
                SignalDeviceSource(
                    poll_interval_seconds=settings.collector_poll_interval,
                    target=ControllerTarget(),
                ),
            ]
        )
        # Track registered jobs so stop() can unregister exactly what
        # start() registered (job names include the source's device_type
        # to keep (method × source) pairs distinct).
        self._registered_jobs: list[str] = []

    async def start(self) -> None:
        """
        Start the collector service.

        Instantiates all registered polling methods and registers one
        poll-cycle job per ``(method × source)`` pair with
        ``JobRegistry``.  The app-level scheduler picks them up via
        ``load_registry()``.
        """
        for name, cls in IngestionMethodRegistry.get_polling_methods().items():
            self._polling_instances[name] = cls()
            logger.info("Instantiated polling method: %s", name)

        for method_name in self._polling_instances:
            for source in self._sources:
                job_name = f"poll_cycle_{method_name}_{source.device_type}"
                JobRegistry.register_func(
                    name=job_name,
                    func=partial(self._run_poll_cycle, method_name, source),
                    trigger="interval",
                    needs_session=False,
                    seconds=source.poll_interval_seconds,
                )
                self._registered_jobs.append(job_name)
                logger.info(
                    "Registered poll cycle %s every %ds",
                    job_name, source.poll_interval_seconds,
                )

        logger.info("CollectorService started")

    async def stop(self) -> None:
        """
        Stop the collector service gracefully.

        Unregisters every poll-cycle job ``start()`` registered.  Safe
        to call multiple times.
        """
        for job_name in self._registered_jobs:
            JobRegistry.unregister(job_name)
        self._registered_jobs.clear()

    async def _run_poll_cycle(
        self, method_name: str, source: DeviceSource,
    ) -> None:
        """
        Execute one poll cycle for a ``(method, source)`` pair.

        Asks the source for the devices configured for this method,
        fans out to ``_process_device()`` bounded by the concurrency
        semaphore, then runs silent-device detection on the polled
        set.
        """
        method = self._polling_instances.get(method_name)
        if method is None:
            logger.error("No polling instance for method: %s", method_name)
            return

        async with self._session_factory() as session:
            devices = await source.list_devices_for_method(
                session, method_name,
            )

        tasks = [
            self._process_device(method, source, device_id, config)
            for device_id, config in devices
        ]

        if tasks:
            await asyncio.gather(*tasks)
            logger.info(
                "Poll cycle %s/%s: processed %d devices",
                method_name, source.device_type, len(tasks),
            )
        else:
            logger.debug(
                "Poll cycle %s/%s: no matching devices",
                method_name, source.device_type,
            )

        # Post-cycle: detect silent devices and poisoned checkpoints.
        polled_device_ids = [device_id for device_id, _ in devices]
        if polled_device_ids:
            await self._check_silent_signals(
                method_name, source, polled_device_ids,
            )

    async def _process_device(
        self,
        method: PollingIngestionMethod,
        source: DeviceSource,
        device_id: str,
        config: dict[str, Any],
    ) -> None:
        """
        Process a single device with semaphore-bounded concurrency.

        Threads the source's ``target`` into ``poll_once`` so the
        transport writes decoded events and checkpoints through the
        right destination for the source's device class.
        """
        async with self._semaphore:
            try:
                await method.poll_once(
                    device_id, config, self._session_factory,
                    target=source.target,
                )
            except Exception:
                logger.exception(
                    "Poll failed: %s/%s device %s",
                    method.name, source.device_type, device_id,
                )

    async def _check_silent_signals(
        self,
        method_name: str,
        source: DeviceSource,
        device_ids: list[str],
    ) -> None:
        """
        Detect devices that returned zero events and track silent cycles.

        After N consecutive silent cycles (configurable), notify the
        operator and auto-investigate for a poisoned checkpoint
        (checkpoint timestamp in the future relative to server time).

        Named "silent_signals" for historical ATSPM terminology — the
        logic applies equally to controllers and roadside sensors
        because both leave checkpoints behind.
        """
        threshold = self._settings.checkpoint_silent_cycles_threshold
        now = datetime.now(timezone.utc)

        async with self._session_factory() as session:
            stmt = select(PollingCheckpoint).where(
                PollingCheckpoint.method == method_name,
                PollingCheckpoint.device_type == source.device_type,
                PollingCheckpoint.device_id.in_(device_ids),
            )
            result = await session.execute(stmt)
            checkpoints = {cp.device_id: cp for cp in result.scalars()}

            for cp in checkpoints.values():
                # Silent cycle detection: consecutive_silent_cycles is
                # reset to 0 in save_checkpoint when events are
                # ingested.  It is only > 0 if save_checkpoint was NOT
                # called (no events).  We increment here for devices
                # that were polled but didn't call save_checkpoint
                # this cycle.
                #
                # Check: if last_successful_poll was NOT updated this
                # cycle (i.e., it's older than the source's poll
                # interval), this device produced zero events.
                poll_interval = timedelta(
                    seconds=source.poll_interval_seconds,
                )
                if (
                    cp.last_successful_poll is not None
                    and now - cp.last_successful_poll > poll_interval * 1.5
                ):
                    cp.consecutive_silent_cycles += 1
                    cp.updated_at = now

                    if cp.consecutive_silent_cycles >= threshold:
                        await self._handle_silent_signal(cp, now)

            await session.flush()

    async def _handle_silent_signal(
        self, checkpoint: PollingCheckpoint, now: datetime,
    ) -> None:
        """
        Handle a device that has been silent for N consecutive cycles.

        Checks if the checkpoint is poisoned (timestamp in the future)
        and auto-recovers if so.  Always notifies the operator.
        """
        device_id = checkpoint.device_id
        device_type = checkpoint.device_type
        method = checkpoint.method
        tolerance = timedelta(
            seconds=self._settings.checkpoint_future_tolerance_seconds,
        )

        # Check for poisoned checkpoint (timestamp in future)
        is_poisoned = (
            checkpoint.last_event_timestamp is not None
            and checkpoint.last_event_timestamp > now + tolerance
        )

        if is_poisoned:
            drift = checkpoint.last_event_timestamp - now
            logger.warning(
                "%s %s (%s): poisoned checkpoint detected — "
                "checkpoint %s is %s ahead of server time",
                device_type, device_id,
                method,
                checkpoint.last_event_timestamp.isoformat(),
                drift,
            )

            # Roll back checkpoint to server time (safe recovery point)
            old_checkpoint = checkpoint.last_event_timestamp.isoformat()
            checkpoint.last_event_timestamp = now
            checkpoint.consecutive_silent_cycles = 0
            checkpoint.updated_at = now

            await notify(
                subject=(
                    "Poisoned checkpoint auto-recovered: "
                    f"{device_type} {device_id}"
                ),
                message=(
                    f"{device_type.capitalize()} {device_id} ({method}) was "
                    f"silent for {checkpoint.consecutive_silent_cycles} "
                    f"consecutive cycles.  Investigation found a poisoned "
                    f"checkpoint.\n\n"
                    f"Checkpoint value: {old_checkpoint}\n"
                    f"Server time: {now.isoformat()}\n"
                    f"Drift: {drift}\n"
                    f"Rollback target: {now.isoformat()}\n\n"
                    f"The checkpoint has been rolled back automatically.  "
                    f"The next poll cycle should resume normal collection."
                ),
                severity=CRITICAL,
                metadata={
                    "device_type": device_type,
                    "device_id": device_id,
                    "method": method,
                    "old_checkpoint": old_checkpoint,
                    "server_time": now.isoformat(),
                    "drift_seconds": drift.total_seconds(),
                    "rollback_target": now.isoformat(),
                    "alert_type": "poisoned_checkpoint_recovery",
                },
            )
        else:
            # Not poisoned — just silent.  Notify operator to investigate.
            await notify(
                subject=f"Silent device detected: {device_type} {device_id}",
                message=(
                    f"{device_type.capitalize()} {device_id} ({method}) has "
                    f"produced zero events for "
                    f"{checkpoint.consecutive_silent_cycles} consecutive "
                    f"poll cycles.\n\n"
                    f"Last successful poll: "
                    f"{checkpoint.last_successful_poll.isoformat()
                       if checkpoint.last_successful_poll else 'never'}"
                    f"\n"
                    f"Last event timestamp: "
                    f"{checkpoint.last_event_timestamp.isoformat()
                       if checkpoint.last_event_timestamp else 'never'}"
                    f"\n\n"
                    f"Checkpoint is NOT in the future — this may indicate "
                    f"a device communication issue, not clock drift."
                ),
                severity=WARNING,
                metadata={
                    "device_type": device_type,
                    "device_id": device_id,
                    "method": method,
                    "silent_cycles": checkpoint.consecutive_silent_cycles,
                    "last_poll": (
                        checkpoint.last_successful_poll.isoformat()
                        if checkpoint.last_successful_poll
                        else None
                    ),
                    "alert_type": "silent_signal",
                },
            )

    def get_method(self, name: str) -> PollingIngestionMethod:
        """
        Get a running polling method instance by name.

        Args:
            name: Method identifier (e.g., "ftp_pull").

        Returns:
            Polling method instance.

        Raises:
            ValueError: If method is not registered or not running.
        """
        method = self._polling_instances.get(name)
        if method is None:
            raise ValueError(f"No polling instance for method: {name}")
        return method

    @property
    def session_factory(self):
        """Expose session factory for on-demand poll routes."""
        return self._session_factory

    async def health_check(self) -> dict[str, bool]:
        """
        Aggregate health status from all polling methods.

        Returns:
            Dictionary of method name -> health status.
        """
        results: dict[str, bool] = {}

        for name, instance in self._polling_instances.items():
            try:
                results[name] = await instance.health_check()
            except Exception:
                results[name] = False

        return results
