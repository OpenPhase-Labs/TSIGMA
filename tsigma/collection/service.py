"""
Collector service — orchestration layer for ingestion methods.

Discovers polling methods from IngestionMethodRegistry and registers
them as interval jobs with JobRegistry. The app-level SchedulerService
picks them up via load_registry().

Uses an asyncio.Semaphore to bound concurrent poll operations
across all signals.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any

from sqlalchemy import select, true

from ..crypto import decrypt_sensitive_fields, has_encryption_key
from ..models.checkpoint import PollingCheckpoint
from ..models.signal import Signal
from ..notifications.registry import CRITICAL, WARNING, notify
from ..scheduler.registry import JobRegistry
from .registry import IngestionMethodRegistry, PollingIngestionMethod

logger = logging.getLogger(__name__)


class CollectorService:
    """
    Orchestration service for ingestion method plugins.

    Polling methods get a SchedulerService interval job that triggers
    _run_poll_cycle(), which queries enabled signals and fans out
    to semaphore-bounded _process_signal() calls.
    """

    def __init__(self, session_factory, settings):
        """
        Initialize the collector service.

        Args:
            session_factory: Async session factory for DB access.
            settings: Application settings (collector_max_concurrent,
                      collector_poll_interval).
        """
        self._session_factory = session_factory
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.collector_max_concurrent)
        self._polling_instances: dict[str, PollingIngestionMethod] = {}

    async def start(self) -> None:
        """
        Start the collector service.

        Instantiates all registered polling methods and registers
        poll cycle jobs with JobRegistry for the app-level scheduler.
        """
        for name, cls in IngestionMethodRegistry.get_polling_methods().items():
            self._polling_instances[name] = cls()
            logger.info("Instantiated polling method: %s", name)

        for method_name in self._polling_instances:
            JobRegistry.register_func(
                name=f"poll_cycle_{method_name}",
                func=partial(self._run_poll_cycle, method_name),
                trigger="interval",
                needs_session=False,
                seconds=self._settings.collector_poll_interval,
            )
            logger.info(
                "Registered poll cycle for %s every %ds",
                method_name,
                self._settings.collector_poll_interval,
            )

        logger.info("CollectorService started")

    async def stop(self) -> None:
        """
        Stop the collector service gracefully.

        Unregisters poll cycle jobs from JobRegistry.
        Safe to call multiple times.
        """
        for method_name in self._polling_instances:
            JobRegistry.unregister(f"poll_cycle_{method_name}")

    async def _run_poll_cycle(self, method_name: str) -> None:
        """
        Execute one poll cycle for a polling method.

        Queries the signal table for all enabled signals configured
        for this method, then fans out to _process_signal() bounded
        by the concurrency semaphore.

        Args:
            method_name: Name of the polling method to run.
        """
        method = self._polling_instances.get(method_name)
        if method is None:
            logger.error("No polling instance for method: %s", method_name)
            return

        async with self._session_factory() as session:
            stmt = select(
                Signal.signal_id,
                Signal.ip_address,
                Signal.signal_metadata,
            ).where(Signal.enabled == true())

            result = await session.execute(stmt)
            rows = result.all()

        tasks = []
        for row in rows:
            metadata = row.signal_metadata
            if not metadata:
                continue

            collection = metadata.get("collection")
            if not collection:
                continue

            if collection.get("method") != method_name:
                continue

            config = dict(collection)
            config["host"] = str(row.ip_address) if row.ip_address else ""

            # Decrypt credentials at poll time (if encryption is configured)
            if has_encryption_key():
                decrypt_sensitive_fields({"collection": config})

            tasks.append(
                self._process_signal(method, row.signal_id, config)
            )

        if tasks:
            await asyncio.gather(*tasks)
            logger.info(
                "Poll cycle %s: processed %d signals", method_name, len(tasks)
            )
        else:
            logger.debug("Poll cycle %s: no matching signals", method_name)

        # Post-cycle: detect silent signals and poisoned checkpoints
        polled_signal_ids = [
            row.signal_id
            for row in rows
            if row.signal_metadata
            and row.signal_metadata.get("collection", {}).get("method")
            == method_name
        ]
        if polled_signal_ids:
            await self._check_silent_signals(method_name, polled_signal_ids)

    async def _process_signal(
        self,
        method: PollingIngestionMethod,
        signal_id: str,
        config: dict[str, Any],
    ) -> None:
        """
        Process a single signal with semaphore-bounded concurrency.

        Args:
            method: Polling method instance.
            signal_id: Traffic signal identifier.
            config: Collection config dict with host injected.
        """
        async with self._semaphore:
            try:
                await method.poll_once(signal_id, config, self._session_factory)
            except Exception:
                logger.exception(
                    "Poll failed: %s signal %s", method.name, signal_id
                )

    async def _check_silent_signals(
        self, method_name: str, signal_ids: list[str]
    ) -> None:
        """
        Detect signals that returned zero events and track silent cycles.

        After N consecutive silent cycles (configurable), notify the
        operator and auto-investigate for a poisoned checkpoint (checkpoint
        timestamp in the future relative to server time).

        Args:
            method_name: Name of the polling method.
            signal_ids: Signal IDs that were polled this cycle.
        """
        threshold = self._settings.checkpoint_silent_cycles_threshold
        now = datetime.now(timezone.utc)

        async with self._session_factory() as session:
            # device_type scoped to "controller" — this service currently
            # polls the Signal table only; a future DeviceSource refactor
            # will parameterize this.
            stmt = select(PollingCheckpoint).where(
                PollingCheckpoint.method == method_name,
                PollingCheckpoint.device_type == "controller",
                PollingCheckpoint.device_id.in_(signal_ids),
            )
            result = await session.execute(stmt)
            checkpoints = {cp.device_id: cp for cp in result.scalars()}

            for cp in checkpoints.values():
                # Silent cycle detection: consecutive_silent_cycles is
                # reset to 0 in _save_checkpoint when events are ingested.
                # It is only > 0 if _save_checkpoint was NOT called (no
                # events). We increment here for signals that were polled
                # but didn't call _save_checkpoint this cycle.
                #
                # Check: if last_successful_poll was NOT updated this cycle
                # (i.e., it's older than the poll interval), this signal
                # produced zero events.
                poll_interval = timedelta(
                    seconds=self._settings.collector_poll_interval,
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
        self, checkpoint: PollingCheckpoint, now: datetime
    ) -> None:
        """
        Handle a signal that has been silent for N consecutive cycles.

        Checks if the checkpoint is poisoned (timestamp in the future)
        and auto-recovers if so. Always notifies the operator.

        Args:
            checkpoint: The silent signal's checkpoint.
            now: Current server time (UTC).
        """
        signal_id = checkpoint.device_id
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
                "Signal %s (%s): poisoned checkpoint detected — "
                "checkpoint %s is %s ahead of server time",
                signal_id,
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
                    f"Poisoned checkpoint auto-recovered: signal {signal_id}"
                ),
                message=(
                    f"Signal {signal_id} ({method}) was silent for "
                    f"{checkpoint.consecutive_silent_cycles} consecutive "
                    f"cycles. Investigation found a poisoned checkpoint.\n\n"
                    f"Checkpoint value: {old_checkpoint}\n"
                    f"Server time: {now.isoformat()}\n"
                    f"Drift: {drift}\n"
                    f"Rollback target: {now.isoformat()}\n\n"
                    f"The checkpoint has been rolled back automatically. "
                    f"The next poll cycle should resume normal collection."
                ),
                severity=CRITICAL,
                metadata={
                    "signal_id": signal_id,
                    "method": method,
                    "old_checkpoint": old_checkpoint,
                    "server_time": now.isoformat(),
                    "drift_seconds": drift.total_seconds(),
                    "rollback_target": now.isoformat(),
                    "alert_type": "poisoned_checkpoint_recovery",
                },
            )
        else:
            # Not poisoned — just silent. Notify operator to investigate.
            await notify(
                subject=f"Silent signal detected: {signal_id}",
                message=(
                    f"Signal {signal_id} ({method}) has produced zero "
                    f"events for {checkpoint.consecutive_silent_cycles} "
                    f"consecutive poll cycles.\n\n"
                    f"Last successful poll: "
                    f"{checkpoint.last_successful_poll.isoformat()
                       if checkpoint.last_successful_poll else 'never'}"
                    f"\n"
                    f"Last event timestamp: "
                    f"{checkpoint.last_event_timestamp.isoformat()
                       if checkpoint.last_event_timestamp else 'never'}"
                    f"\n\n"
                    f"Checkpoint is NOT in the future — this may indicate "
                    f"a controller communication issue, not clock drift."
                ),
                severity=WARNING,
                metadata={
                    "signal_id": signal_id,
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
