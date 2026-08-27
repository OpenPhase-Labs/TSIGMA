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

import grpc
from grpc import aio
from grpc_health.v1 import health_pb2, health_pb2_grpc

from .protocol import HandshakeConfig, PluginProcess

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


async def _check_health(channel: aio.Channel | None, name: str, timeout: float) -> bool:
    """Shared health poll. Unreachable counts as unhealthy; never raises."""
    if channel is None:
        return False
    stub = health_pb2_grpc.HealthStub(channel)
    try:
        response = await stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=timeout)
    except grpc.RpcError as exc:
        logger.debug("%s: health check failed: %s", name, exc)
        return False
    return response.status == health_pb2.HealthCheckResponse.SERVING


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

    async def connect(self) -> HandshakeConfig:
        return await self._proc.launch()

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        return await self._proc.is_healthy(timeout=timeout)

    async def shutdown(self) -> None:
        await self._proc.shutdown()


class DiscoveredConnection:
    """Mode 2 - externally orchestrated.

    There is no subprocess and no stdout, so the handshake is not read - it comes
    from the manifest/registry entry and is verified by dialling. The host does
    not launch, restart, or stop this plugin; that belongs to systemd or k8s.
    ``shutdown`` closes this host's channel only.
    """

    process_model = ProcessModel.EXTERNAL

    def __init__(self, name: str, handshake: HandshakeConfig):
        self.name = name
        self.handshake = handshake
        self.channel: aio.Channel | None = None

    @property
    def host_owns_lifecycle(self) -> bool:
        return False

    async def connect(self) -> HandshakeConfig:
        if self.channel is None:
            self.channel = aio.insecure_channel(self.handshake.target)
        return self.handshake

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        return await _check_health(self.channel, self.name, timeout)

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
    def running(self) -> bool:
        return self._proc is not None

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
