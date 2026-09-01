"""Plugin lifecycle audit trail - the vocabulary, the record, and the sinks.

A supervisor decision an operator cannot see afterwards is a decision that
gets rediscovered at 3am. Every lifecycle event is written to
``config.plugin_audit`` as well as logged, and the event vocabulary is fixed
so an operator query is not a guess at spellings (ADR-0019 Confirmation).

The sink is injectable: the database writer is the default, and
``RecordingPluginAuditSink`` is the in-memory seam a test asserts against
without a live pool.
"""

import enum
import logging
from dataclasses import dataclass, field
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from ..models.plugin_audit import PluginAudit
from .connection import ProcessModel

logger = logging.getLogger(__name__)


class PluginAuditEvent(str, enum.Enum):
    """Lifecycle events a supervisor records.

    The health transition is split by direction: "it went bad" and "it came
    back" are different operator facts, and an operator must not have to diff
    two rows to tell them apart.
    """

    LAUNCH = "launch"
    HANDSHAKE_FAILED = "handshake_failed"
    HEALTH_LOST = "health_lost"
    HEALTH_RESTORED = "health_restored"
    RESTART = "restart"
    GAVE_UP = "gave_up"
    SHUTDOWN = "shutdown"


@dataclass
class PluginAuditRecord:
    """One lifecycle event, before it reaches a sink."""

    plugin_name: str
    event_type: PluginAuditEvent
    process_model: ProcessModel
    detail: str = ""


class PluginAuditSink(Protocol):
    """Where a supervisor sends lifecycle records. One method, always awaited."""

    async def record(self, event: PluginAuditRecord) -> None: ...


async def log_plugin_event(session: AsyncSession, record: PluginAuditRecord) -> None:
    """
    Write one plugin lifecycle audit row.

    Args:
        session: Database session (must be flushed/committed by caller).
        record: The lifecycle event to persist.
    """
    entry = PluginAudit(
        plugin_name=record.plugin_name,
        event_type=record.event_type.value,
        process_model=record.process_model.value,
        detail=record.detail or None,
    )
    session.add(entry)
    await session.flush()
    logger.info(
        "Plugin audit: %s plugin=%s model=%s",
        record.event_type.value,
        record.plugin_name,
        record.process_model.value,
    )


class DatabasePluginAuditSink:
    """The default sink: one row per event in ``config.plugin_audit``.

    The facade is resolved per write, not at construction: a supervisor is
    built during app startup, before a pool exists, and a sink that connects
    eagerly would make importing the plugin host database-dependent.
    """

    async def record(self, event: PluginAuditRecord) -> None:
        from ..database.db import get_db_facade

        async with get_db_facade().session() as session:
            await log_plugin_event(session, event)


@dataclass
class RecordingPluginAuditSink:
    """In-memory sink. The test seam, and a usable fallback with no database."""

    records: list[PluginAuditRecord] = field(default_factory=list)

    async def record(self, event: PluginAuditRecord) -> None:
        self.records.append(event)
