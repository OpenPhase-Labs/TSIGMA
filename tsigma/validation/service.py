"""
Validation service — orchestration layer for post-ingestion validators.

Discovers enabled validators from ValidationRegistry, groups unvalidated
events by signal_id, runs validators, and writes merged results back to
the validation_metadata JSONB column on controller_event_log.
"""

import logging
from itertools import groupby
from operator import itemgetter
from typing import Any

from sqlalchemy import select, update

from ..config import settings as global_settings
from ..models.event import ControllerEventLog
from ..scheduler.registry import JobRegistry
from .registry import ValidationLevel, ValidationRegistry
from .sdk import merge_results

logger = logging.getLogger(__name__)


class ValidationService:
    """
    Orchestration service for validation plugins.

    Queries unvalidated events, groups by signal, runs all enabled
    validators, and writes merged results back to the DB.
    """

    def __init__(self, session_factory, settings=None):
        """
        Initialize the validation service.

        Args:
            session_factory: Async session factory for DB access.
            settings: Application settings. Defaults to global settings.
        """
        self._session_factory = session_factory
        self._settings = settings or global_settings
        self._validator_instances: dict[str, Any] = {}

    def _get_enabled_levels(self) -> list[ValidationLevel]:
        """
        Determine which validation levels are enabled.

        Checks the master toggle first, then per-layer toggles.

        Returns:
            List of enabled ValidationLevel values.
        """
        if not self._settings.validation_enabled:
            return []

        levels = []
        if self._settings.validation_layer1_enabled:
            levels.append(ValidationLevel.LAYER1)
        if self._settings.validation_layer2_enabled:
            levels.append(ValidationLevel.LAYER2)
        if self._settings.validation_layer3_enabled:
            levels.append(ValidationLevel.LAYER3)
        return levels

    async def start(self) -> None:
        """
        Start the validation service.

        Instantiates validators for all enabled levels and registers
        the validation cycle job with JobRegistry.
        """
        enabled_levels = self._get_enabled_levels()

        for level in enabled_levels:
            validators = ValidationRegistry.get_by_level(level)
            for name, cls in validators.items():
                self._validator_instances[name] = cls()
                logger.info("Instantiated validator: %s (%s)", name, level.value)

        JobRegistry.register_func(
            name="validation_cycle",
            func=self._run_validation_cycle,
            trigger="interval",
            needs_session=False,
            seconds=self._settings.validation_interval,
        )

        logger.info(
            "ValidationService started — %d validators, %d-second interval",
            len(self._validator_instances),
            self._settings.validation_interval,
        )

    async def stop(self) -> None:
        """
        Stop the validation service gracefully.

        Unregisters the validation cycle job from JobRegistry.
        Safe to call multiple times.
        """
        JobRegistry.unregister("validation_cycle")

    async def _run_validation_cycle(self) -> None:
        """
        Execute one validation cycle.

        Queries unvalidated events (validation_metadata IS NULL),
        groups them by signal_id, runs validators, and writes
        merged results back.
        """
        batch_size = self._settings.validation_batch_size

        async with self._session_factory() as session:
            stmt = (
                select(
                    ControllerEventLog.signal_id,
                    ControllerEventLog.event_time,
                    ControllerEventLog.event_code,
                    ControllerEventLog.event_param,
                )
                .where(ControllerEventLog.validation_metadata.is_(None))
                .order_by(ControllerEventLog.event_time.desc())
                .limit(batch_size)
            )

            result = await session.execute(stmt)
            rows = result.all()

        if not rows:
            logger.debug("Validation cycle: no unvalidated events")
            return

        # Convert rows to dicts and group by signal_id
        events = [
            {
                "signal_id": row.signal_id,
                "event_time": row.event_time,
                "event_code": row.event_code,
                "event_param": row.event_param,
            }
            for row in rows
        ]

        # Group by signal_id
        events.sort(key=itemgetter("signal_id"))
        for signal_id, group in groupby(events, key=itemgetter("signal_id")):
            signal_events = list(group)
            await self._validate_signal_events(signal_id, signal_events)

        logger.info(
            "Validation cycle: processed %d events", len(events)
        )

    async def _validate_signal_events(
        self,
        signal_id: str,
        events: list[dict[str, Any]],
    ) -> None:
        """
        Run all validators on a batch of events for one signal.

        Merges per-validator results and writes the merged
        validation_metadata back to each event row.

        Args:
            signal_id: Traffic signal identifier.
            events: List of event dicts for this signal.
        """
        # Collect results from all validators
        all_results: dict[int, list[dict[str, Any]]] = {
            i: [] for i in range(len(events))
        }

        for name, validator in self._validator_instances.items():
            try:
                validator_results = await validator.validate_events(
                    events, signal_id, self._session_factory
                )
                for i, result in enumerate(validator_results):
                    if result is not None:
                        all_results[i].append(result)
            except Exception:
                logger.exception(
                    "Validator %s failed for signal %s", name, signal_id
                )

        # Write merged results back to DB
        async with self._session_factory() as session:
            for i, event in enumerate(events):
                merged = merge_results(all_results[i])

                stmt = (
                    update(ControllerEventLog)
                    .where(
                        ControllerEventLog.signal_id == event["signal_id"],
                        ControllerEventLog.event_time == event["event_time"],
                        ControllerEventLog.event_code == event["event_code"],
                        ControllerEventLog.event_param == event["event_param"],
                    )
                    .values(validation_metadata=merged)
                )
                await session.execute(stmt)

            await session.commit()
