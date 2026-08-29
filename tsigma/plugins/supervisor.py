"""Plugin supervisor - discover, connect, supervise, shut down.

Mode-aware per ADR-0019: restart-on-crash applies only where this host owns the
lifecycle. An externally-orchestrated plugin is reconnected and observed, never
restarted - k8s or systemd owns it, and restarting it here would fight them.
"""

import asyncio
import logging
from dataclasses import dataclass, field

from .connection import (
    DiscoveredConnection,
    LaunchedConnection,
    PluginConnection,
    ProcessModel,
    ScheduledConnection,
)
from .protocol import HandshakeConfig, HandshakeError

logger = logging.getLogger(__name__)

DEFAULT_MAX_RESTARTS = 3


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
    restarts: int = 0
    last_error: str = ""
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


class PluginSupervisor:
    """Owns the set of connected plugins and keeps the healthy ones available."""

    def __init__(self, max_restarts: int = DEFAULT_MAX_RESTARTS):
        self.max_restarts = max_restarts
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
            try:
                await state.connection.connect()
            except (HandshakeError, OSError) as exc:
                state.available = False
                state.last_error = str(exc)
                logger.error("plugin %s failed to start: %s", name, exc)
                return False
            state.available = await state.connection.is_healthy()
            if not state.available:
                state.last_error = "unhealthy after connect"
            return state.available

    async def start_all(self) -> dict[str, bool]:
        results = await asyncio.gather(*(self.start(n) for n in self._plugins))
        return dict(zip(self._plugins, results, strict=True))

    # -------------------------------------------------------------- supervision
    async def supervise_once(self) -> dict[str, bool]:
        """One supervision pass. Restarts only what this host owns."""
        return {n: await self._supervise(n) for n in list(self._plugins)}

    async def _supervise(self, name: str) -> bool:
        state = self._plugins[name]
        if await state.connection.is_healthy():
            state.available = True
            return True

        state.available = False
        if not state.connection.host_owns_lifecycle:
            # Mode 2: the orchestrator restarts it; we reconnect and observe.
            state.last_error = "unhealthy; awaiting orchestrator"
            logger.warning("plugin %s unhealthy (external); not restarting", name)
            return False

        if state.restarts >= self.max_restarts:
            state.last_error = f"gave up after {state.restarts} restarts"
            logger.error("plugin %s exceeded %d restarts", name, self.max_restarts)
            return False

        state.restarts += 1
        logger.warning("plugin %s unhealthy; restart %d", name, state.restarts)
        async with state._lock:
            await self._quiet_shutdown(state)
            state.connection = state.spec.build()
        return await self.start(name)

    # ---------------------------------------------------------------- shutdown
    async def shutdown_all(self) -> None:
        for state in self._plugins.values():
            async with state._lock:
                await self._quiet_shutdown(state)
                state.available = False

    @staticmethod
    async def _quiet_shutdown(state: PluginState) -> None:
        try:
            await state.connection.shutdown()
        except Exception as exc:  # a dying plugin must not break the sweep
            logger.debug("%s: shutdown raised %s", state.spec.name, exc)
