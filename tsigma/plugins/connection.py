"""Plugin connection seam - one interface, three ADR-0019 process models.

Everything above this layer (supervisor, registries, remote wrappers) codes
against `PluginConnection` and never against a concrete mode. That is what lets
the same plugin run as a core-managed child in dev and a k8s pod in production
without any caller knowing the difference (ADR-0019).

  LaunchedConnection    mode 1  core-managed child; host owns the lifecycle
  DiscoveredConnection  mode 2  externally orchestrated; host only dials/observes
  ScheduledConnection   mode 3  cron; started per run, exits, no long-lived slot
"""

import logging
from enum import Enum
from typing import Protocol, runtime_checkable

from grpc import aio

from .protocol import HandshakeConfig, PluginProcess, check_health, validate_handshake
from .transport import TLSConfig, require_transport_security

logger = logging.getLogger(__name__)


class ProcessModel(str, Enum):
    """Deployment shape a plugin declares in its manifest (ADR-0019)."""

    CHILD = "child"        # core forks/execs and supervises
    EXTERNAL = "external"  # systemd / k8s owns the lifecycle
    CRON = "cron"          # started at interval, runs, exits


@runtime_checkable
class PluginConnection(Protocol):
    """What a supervisor or registry may rely on, whatever the process model."""

    name: str
    process_model: ProcessModel
    handshake: HandshakeConfig | None
    channel: aio.Channel | None

    async def connect(self) -> HandshakeConfig: ...

    async def is_healthy(self, timeout: float = 2.0) -> bool: ...

    async def shutdown(self) -> None: ...

    @property
    def host_owns_lifecycle(self) -> bool:
        """True when this host may start, restart, and stop the plugin."""
        ...

    @property
    def idle(self) -> bool:
        """True when the plugin is resting between runs rather than failing.

        Idle and unhealthy both read as "not serving", so health alone cannot
        separate a cron plugin that finished its work from one that died. Every
        mode answers this so a supervisor never has to special-case mode 3.
        """
        ...


class LaunchedConnection:
    """Mode 1 - core-managed child. Wraps the Phase 2 subprocess shim."""

    process_model = ProcessModel.CHILD

    def __init__(self, name: str, command: list[str]):
        self.name = name
        self.command = command
        self._proc = PluginProcess(name, command)

    @property
    def handshake(self) -> HandshakeConfig | None:
        return self._proc.handshake

    @property
    def channel(self) -> aio.Channel | None:
        return self._proc.channel

    @property
    def host_owns_lifecycle(self) -> bool:
        return True

    @property
    def idle(self) -> bool:
        """Never. A long-lived child is serving or it is not; there are no runs.

        Reporting idle here would tell the supervisor to leave a crashed child
        alone instead of restarting it.
        """
        return False

    async def connect(self) -> HandshakeConfig:
        return await self._proc.launch()

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        return await self._proc.is_healthy(timeout=timeout)

    async def shutdown(self) -> None:
        await self._proc.shutdown()


class DiscoveredConnection:
    """Mode 2 - externally orchestrated.

    There is no subprocess and no stdout, so the handshake is not read - it comes
    from the manifest/registry entry (ADR-0020). A manifest on disk can drift or
    lie, so those fields go through the same gate a stdout line does, at
    construction: a refused claim never becomes a connection something can dial.
    The host does not launch, restart, or stop this plugin; that belongs to
    systemd or k8s. ``shutdown`` closes this host's channel only.
    """

    process_model = ProcessModel.EXTERNAL

    def __init__(
        self,
        name: str,
        handshake: HandshakeConfig,
        tls: TLSConfig | None = None,
    ):
        self.name = name
        self._handshake = validate_handshake(handshake)
        self._tls = tls
        # Refused here, not at dial time: a deployment that forgot TLS fails at
        # startup naming the address, rather than carrying ingestion data and a
        # broker callback across a cluster in the clear.
        require_transport_security(self._handshake.target, tls)
        self.channel: aio.Channel | None = None

    @property
    def handshake(self) -> HandshakeConfig | None:
        return self._handshake

    @property
    def host_owns_lifecycle(self) -> bool:
        return False

    @property
    def idle(self) -> bool:
        """Never. An external plugin has no runs, so it is up or it is down."""
        return False

    async def connect(self) -> HandshakeConfig:
        if self.channel is None:
            target = self._handshake.target
            self.channel = (
                aio.secure_channel(
                    target,
                    self._tls.credentials(),
                    options=self._tls.channel_options(),
                )
                if self._tls is not None
                else aio.insecure_channel(target)
            )
        return self._handshake

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        return await check_health(self.channel, self.name, timeout)

    async def shutdown(self) -> None:
        # Closes our channel; the plugin keeps running under its orchestrator.
        if self.channel is not None:
            await self.channel.close()
            self.channel = None


class ScheduledConnection:
    """Mode 3 - cron. Launched per run, exits when the work is done.

    Health is per-run: healthy only while a run is in flight. No long-lived slot,
    so a plugin that is not currently running is not unhealthy - it is idle.
    """

    process_model = ProcessModel.CRON

    def __init__(self, name: str, command: list[str]):
        self.name = name
        self.command = command
        self._proc: PluginProcess | None = None

    @property
    def handshake(self) -> HandshakeConfig | None:
        return self._proc.handshake if self._proc else None

    @property
    def channel(self) -> aio.Channel | None:
        return self._proc.channel if self._proc else None

    @property
    def host_owns_lifecycle(self) -> bool:
        return True

    @property
    def idle(self) -> bool:
        """True between runs: none has started yet, or the last one has ended.

        A run that exited - cleanly or not - leaves its ``PluginProcess`` object
        behind, so the object's existence is not the state. The subprocess's
        return code is.
        """
        if self._proc is None or self._proc.process is None:
            return True
        return self._proc.process.returncode is not None

    @property
    def running(self) -> bool:
        return not self.idle

    async def connect(self) -> HandshakeConfig:
        """Start one run."""
        self._proc = PluginProcess(self.name, self.command)
        return await self._proc.launch()

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        if self._proc is None:
            return False
        return await self._proc.is_healthy(timeout=timeout)

    async def shutdown(self) -> None:
        """End the current run."""
        if self._proc is not None:
            await self._proc.shutdown()
            self._proc = None
