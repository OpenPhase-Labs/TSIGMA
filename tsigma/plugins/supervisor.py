"""Plugin supervisor - discover, connect, supervise, shut down.

Mode-aware per ADR-0019, and the mode is the connection's rather than a flag
this file keeps:

  - restart-on-crash applies only where this host owns the lifecycle;
  - an externally-orchestrated plugin is reconnected and observed, never
    restarted - k8s or systemd owns it, and restarting it here would fight them;
  - a connection that reports ``idle`` is resting between scheduled runs, so it
    is neither relaunched off-schedule nor charged against the restart budget.

The restart budget is windowed. A lifetime counter retires a plugin that
crashed three times in three years, which is a plugin that works; only the
restarts still inside the window count, so sustained health restores the budget
by letting the old entries fall out of it.

Every lifecycle event is written to the plugin audit trail as well as logged
(ADR-0019 Confirmation), and a restart re-points every registry entry that held
the replaced connection so dispatch never reaches one this host shut down.
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from . import constants
from .audit import (
    DatabasePluginAuditSink,
    PluginAuditEvent,
    PluginAuditRecord,
    PluginAuditSink,
)
from .coexistence import GrpcCoexistenceMixin, RegistryConflictError
from .connection import (
    DiscoveredConnection,
    LaunchedConnection,
    PluginConnection,
    ProcessModel,
    ScheduledConnection,
)
from .protocol import HandshakeConfig

logger = logging.getLogger(__name__)

DEFAULT_MAX_RESTARTS = 3

# Restarts older than this fall out of the budget. Long enough that a crash
# loop is still caught, short enough that an occasional crash over a long
# uptime never accumulates into a retirement.
DEFAULT_RESTART_WINDOW_SECONDS = 600.0

# How long a freshly connected plugin has to start serving. A plugin's gRPC
# server is not listening the instant it prints its handshake, so a single
# probe would read a slow-starting plugin as a failed start.
DEFAULT_STARTUP_HEALTH_SECONDS = 5.0
STARTUP_HEALTH_POLL_SECONDS = 0.1


def default_registries() -> dict[str, type[GrpcCoexistenceMixin]]:
    """The registries that carry the coexistence mixin, keyed by subsystem.

    Imported lazily: the supervisor is part of the plugin host, and the
    registries pull in the report/collection/notification stacks behind them.

    ``GENERATED_SUBSYSTEMS`` is the one list of subsystem names, so a name added
    there and left unmapped here raises at startup rather than becoming a
    subsystem this host silently never serves.
    """
    from ..collection.decoders.base import DecoderRegistry
    from ..collection.registry import IngestionMethodRegistry
    from ..notifications.registry import NotificationRegistry
    from ..reports.registry import ReportRegistry

    by_subsystem: dict[str, type[GrpcCoexistenceMixin]] = {
        "decoder": DecoderRegistry,
        "method": IngestionMethodRegistry,
        "notify": NotificationRegistry,
        "report": ReportRegistry,
    }
    return {subsystem: by_subsystem[subsystem] for subsystem in constants.GENERATED_SUBSYSTEMS}


class PluginSpecError(ValueError):
    """A plugin spec is missing what its process model requires."""


@dataclass
class PluginSpec:
    """One plugin as declared by a manifest."""

    name: str
    process_model: ProcessModel
    command: list[str] | None = None
    handshake: HandshakeConfig | None = None
    subsystems: tuple[str, ...] = ()

    def __post_init__(self):
        if self.process_model in (ProcessModel.CHILD, ProcessModel.CRON):
            if not self.command:
                raise PluginSpecError(f"{self.name}: {self.process_model.value} requires a command")
        elif self.process_model is ProcessModel.EXTERNAL:
            if self.handshake is None:
                raise PluginSpecError(
                    f"{self.name}: external requires a handshake from the manifest"
                )

    def build(self) -> PluginConnection:
        if self.process_model is ProcessModel.CHILD:
            return LaunchedConnection(self.name, self.command)
        if self.process_model is ProcessModel.CRON:
            return ScheduledConnection(self.name, self.command)
        return DiscoveredConnection(self.name, self.handshake)


@dataclass
class PluginState:
    """Live state the supervisor tracks per plugin."""

    spec: PluginSpec
    connection: PluginConnection
    available: bool = False
    restart_times: list[float] = field(default_factory=list)
    retired: bool = False
    released: bool = False
    last_error: str = ""
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


class PluginSupervisor:
    """Owns the set of connected plugins and keeps the healthy ones available."""

    def __init__(
        self,
        max_restarts: int = DEFAULT_MAX_RESTARTS,
        *,
        restart_window_seconds: float = DEFAULT_RESTART_WINDOW_SECONDS,
        startup_health_seconds: float = DEFAULT_STARTUP_HEALTH_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        audit_sink: PluginAuditSink | None = None,
        registries: dict[str, type[GrpcCoexistenceMixin]] | None = None,
    ):
        self.max_restarts = max_restarts
        self.restart_window_seconds = restart_window_seconds
        self.startup_health_seconds = startup_health_seconds
        self.audit_sink: PluginAuditSink = audit_sink or DatabasePluginAuditSink()
        self.registries = default_registries() if registries is None else dict(registries)
        self._clock = clock
        self._plugins: dict[str, PluginState] = {}

    # ---------------------------------------------------------------- discovery
    def add(self, spec: PluginSpec) -> None:
        if spec.name in self._plugins:
            raise PluginSpecError(f"{spec.name}: already registered")
        self._plugins[spec.name] = PluginState(spec=spec, connection=spec.build())

    def names(self) -> list[str]:
        return list(self._plugins)

    def connection(self, name: str) -> PluginConnection:
        return self._plugins[name].connection

    def state(self, name: str) -> PluginState:
        return self._plugins[name]

    def available(self) -> list[str]:
        return [n for n, s in self._plugins.items() if s.available]

    # ------------------------------------------------------------------- start
    async def start(self, name: str) -> bool:
        """Connect one plugin. A failure marks it unavailable rather than raising."""
        state = self._plugins[name]
        async with state._lock:
            return await self._start(state)

    async def start_all(self) -> dict[str, bool]:
        results = await asyncio.gather(*(self.start(n) for n in self._plugins))
        return dict(zip(self._plugins, results, strict=True))

    async def _start(self, state: PluginState) -> bool:
        """Connect, health-check, register. The caller holds the plugin's lock.

        Every start opens a fresh release budget, whether it arrived through the
        public ``start`` or through a restart: the subprocess and channel this
        attempt is about to open are not the ones a previous attempt released.
        """
        name = state.spec.name
        state.released = False
        try:
            await state.connection.connect()
        except Exception as exc:
            # A refused handshake is an operator event, not a private field: it
            # reaches the log an operator already watches AND the audit table.
            state.available = False
            state.last_error = str(exc)
            logger.error("plugin %s failed to start: %s", name, exc)
            await self._record(state, PluginAuditEvent.HANDSHAKE_FAILED, str(exc))
            await self._release(state)
            return False

        if not await self._await_ready(state.connection):
            state.available = False
            if state.connection.idle:
                # Mode 3: the run started and has already ended. launch() opened a
                # channel before it exited, and connect() rebinds the process on the
                # next run, so the release has to happen here or every scheduled run
                # leaks one.
                state.last_error = ""
                await self._record(state, PluginAuditEvent.LAUNCH)
                await self._release(state)
                return False
            state.last_error = "unhealthy after connect"
            logger.warning("plugin %s connected but failed its health check", name)
            await self._release(state)
            return False

        try:
            self._register(state)
        except RegistryConflictError as exc:
            # The name resolves in-process. Swallowing this would report a start
            # as successful while dispatch still went to the in-process class.
            state.available = False
            state.last_error = str(exc)
            logger.error("plugin %s cannot serve its subsystems: %s", name, exc)
            self._unregister(state)
            await self._release(state)
            return False

        state.available = True
        state.last_error = ""
        await self._record(state, PluginAuditEvent.LAUNCH)
        return True

    async def _await_ready(self, connection: PluginConnection) -> bool:
        """Poll health until the plugin serves or its startup budget runs out.

        Real elapsed time, not the injected budget clock: a test that moves the
        restart window by hand is not also asking a subprocess to boot faster.
        Idle short-circuits - a cron run that has already ended is not going to
        start serving, and waiting on it would only delay the next run.
        """
        deadline = time.monotonic() + self.startup_health_seconds
        while True:
            if await connection.is_healthy():
                return True
            if connection.idle or time.monotonic() >= deadline:
                return False
            await asyncio.sleep(STARTUP_HEALTH_POLL_SECONDS)

    # -------------------------------------------------------------- supervision
    async def supervise_once(self) -> dict[str, bool]:
        """One supervision pass. Restarts only what this host owns."""
        return {n: await self._supervise(n) for n in list(self._plugins)}

    async def _supervise(self, name: str) -> bool:
        state = self._plugins[name]
        if state.retired:
            return False

        self._prune_budget(state)
        connection = state.connection

        if await connection.is_healthy():
            if not state.available:
                state.available = True
                state.last_error = ""
                await self._record(state, PluginAuditEvent.HEALTH_RESTORED)
            return True

        if connection.idle:
            # Resting between scheduled runs is not a failure and not an event.
            # The schedule starts the next run, not this supervisor.
            state.available = False
            return False

        if state.available:
            await self._record(state, PluginAuditEvent.HEALTH_LOST)
        state.available = False

        if not connection.host_owns_lifecycle:
            # Mode 2: the orchestrator restarts it; we re-dial and observe.
            state.last_error = "unhealthy; awaiting orchestrator"
            logger.warning("plugin %s unhealthy (external); not restarting", name)
            await self._redial(state)
            return False

        if len(state.restart_times) >= self.max_restarts:
            await self._give_up(state)
            return False

        state.restart_times.append(self._clock())
        logger.warning(
            "plugin %s unhealthy; restart %d of %d in the last %.0fs",
            name, len(state.restart_times), self.max_restarts, self.restart_window_seconds,
        )
        await self._record(state, PluginAuditEvent.RESTART)
        return await self._restart(state)

    def _prune_budget(self, state: PluginState) -> None:
        """Drop restarts that have aged out. Sustained health empties the list."""
        cutoff = self._clock() - self.restart_window_seconds
        state.restart_times[:] = [t for t in state.restart_times if t > cutoff]

    async def _restart(self, state: PluginState) -> bool:
        """Replace the connection, then re-point every entry that held the old one."""
        async with state._lock:
            old = state.connection
            stale = self._entries_for(old)
            for registry, name in stale:
                registry.unregister_grpc(name)
            await self._release(state)

            state.connection = state.spec.build()
            started = await self._start(state)
            if started:
                for registry, name in stale:
                    if registry.origin(name) is None:
                        with contextlib.suppress(RegistryConflictError):
                            registry.register_grpc(name, state.connection)
            return started

    async def _redial(self, state: PluginState) -> None:
        """Drop the stale channel and dial again (mode 2 only).

        A channel to a pod that was rescheduled is stale forever, and ``connect``
        on a connection that still holds one is a no-op - so a host that only
        observes never notices the replacement come back.
        """
        try:
            await state.connection.shutdown()
            await state.connection.connect()
        except Exception as exc:
            logger.debug("%s: re-dial failed with %r", state.spec.name, exc)

    async def _give_up(self, state: PluginState) -> None:
        """Retire a plugin: unregister it everywhere, then release it."""
        state.retired = True
        state.available = False
        state.last_error = (
            f"gave up after {self.max_restarts} restarts "
            f"within {self.restart_window_seconds:.0f}s"
        )
        logger.error(
            "plugin %s exceeded %d restarts within %.0fs; retiring it",
            state.spec.name, self.max_restarts, self.restart_window_seconds,
        )
        self._unregister(state)
        await self._release(state)
        await self._record(state, PluginAuditEvent.GAVE_UP)

    # -------------------------------------------------------------- registries
    def _register(self, state: PluginState) -> None:
        """Register the plugin with each subsystem its manifest names."""
        for subsystem in state.spec.subsystems:
            registry = self.registries.get(subsystem)
            if registry is None:
                logger.warning(
                    "plugin %s names subsystem %r, which this host does not serve",
                    state.spec.name, subsystem,
                )
                continue
            registry.register_grpc(state.spec.name, state.connection)

    def _unregister(self, state: PluginState) -> None:
        """Drop every entry this plugin holds - by name and by connection object."""
        for registry, name in self._entries_for(state.connection):
            registry.unregister_grpc(name)
        for subsystem in state.spec.subsystems:
            registry = self.registries.get(subsystem)
            if registry is not None:
                registry.unregister_grpc(state.spec.name)

    def _entries_for(
        self, connection: PluginConnection
    ) -> list[tuple[type[GrpcCoexistenceMixin], str]]:
        """Every registry entry pointing at this connection, whatever its key.

        One plugin can serve several names, so a restart leaves entries stale
        under keys that are not the plugin's own.
        """
        found: list[tuple[type[GrpcCoexistenceMixin], str]] = []
        for registry in self.registries.values():
            for name, registered in registry.list_grpc().items():
                if registered is connection:
                    found.append((registry, name))
        return found

    # ---------------------------------------------------------------- shutdown
    async def shutdown_all(self) -> None:
        for state in self._plugins.values():
            async with state._lock:
                self._unregister(state)
                await self._release(state)
                state.available = False
                await self._record(state, PluginAuditEvent.SHUTDOWN)

    async def _release(self, state: PluginState) -> None:
        """Release the subprocess and the channel, once.

        Delegates to the connection so there is exactly one kill/close path, and
        never twice for the same connection: a start that failed its health
        check has already released, and the restart behind it must not report a
        second shutdown of a plugin that is already gone.
        """
        if state.released:
            return
        state.released = True
        try:
            await state.connection.shutdown()
        except Exception as exc:  # a dying plugin must not break the sweep
            logger.debug("%s: shutdown raised %s", state.spec.name, exc)

    # ------------------------------------------------------------------- audit
    async def _record(
        self, state: PluginState, event: PluginAuditEvent, detail: str = ""
    ) -> None:
        """Send one lifecycle record to the sink.

        An audit trail that cannot be written is a problem; a plugin fleet that
        stops being supervised because of it is an outage.
        """
        record = PluginAuditRecord(
            plugin_name=state.spec.name,
            event_type=event,
            process_model=state.spec.process_model,
            detail=detail,
        )
        try:
            await self.audit_sink.record(record)
        except Exception as exc:
            logger.warning(
                "plugin audit sink refused %s for %s: %s", event.value, state.spec.name, exc
            )
