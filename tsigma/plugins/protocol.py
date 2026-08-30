"""TSIGMA Plugin Protocol v1 - handshake, health, lifecycle.

Normative spec: TSIGMA-Contract PROTOCOL.md sections 1-3. The host launches a
plugin subprocess with the magic cookie set, reads one handshake line from its
stdout, dials the advertised address, polls grpc.health.v1, and stops it via
GRPCController.Shutdown with a kill fallback.
"""

import asyncio
import logging
import os
from dataclasses import dataclass

import grpc
import grpc_broker_pb2  # bare names: resolved by the gen/ path insert in __init__
import grpc_broker_pb2_grpc
import grpc_controller_pb2
import grpc_controller_pb2_grpc
import grpc_stdio_pb2
import grpc_stdio_pb2_grpc
from grpc import aio
from grpc_health.v1 import health_pb2, health_pb2_grpc

from . import constants  # side effect: puts gen/ on sys.path

logger = logging.getLogger(__name__)

# Re-exported so callers (and plugin authors) never need a bare-name import of
# the generated modules, which only resolve after this package is imported.
__all__ = [
    "HandshakeConfig",
    "HandshakeError",
    "PluginProcess",
    "format_handshake_line",
    "grpc_broker_pb2",
    "grpc_broker_pb2_grpc",
    "grpc_controller_pb2",
    "grpc_controller_pb2_grpc",
    "grpc_stdio_pb2",
    "grpc_stdio_pb2_grpc",
    "parse_handshake_line",
    "plugin_env",
]

HANDSHAKE_FIELDS = 5
VALID_NETWORKS = ("tcp", "unix")
VALID_PROTOCOL = "grpc"

# How long a plugin gets to print its handshake line. Measured: a Python plugin
# needs ~5s on an idle machine just to import grpc, grpc_health, and pyarrow
# before it can serve, so a 10s budget left no headroom under load. 30s matches
# collector_poll_timeout_seconds, the house number for "an operation that talks
# to a device". A plugin that never prints one is hung and is killed.
HANDSHAKE_TIMEOUT_SECONDS = 30.0
SHUTDOWN_GRACE_SECONDS = 5.0


class HandshakeError(ValueError):
    """The plugin's handshake line was malformed or declared an unsupported version."""


@dataclass(frozen=True)
class HandshakeConfig:
    """One parsed handshake line."""

    core_version: int
    app_version: int
    network: str
    address: str
    protocol: str

    @property
    def target(self) -> str:
        """gRPC channel target for this plugin."""
        return self.address if self.network == "tcp" else f"unix:{self.address}"


def parse_handshake_line(line: str) -> HandshakeConfig:
    """Parse CORE|APP|NETWORK|ADDRESS|PROTOCOL, rejecting anything off-spec.

    Raises HandshakeError with the offending line; the caller surfaces it rather
    than treating an unparseable plugin as merely unhealthy.
    """
    raw = (line or "").strip()
    if not raw:
        raise HandshakeError("empty handshake line")

    parts = raw.split("|")
    if len(parts) != HANDSHAKE_FIELDS:
        raise HandshakeError(f"expected {HANDSHAKE_FIELDS} fields, got {len(parts)}: {raw!r}")

    core_s, app_s, network, address, protocol = (p.strip() for p in parts)

    try:
        core_version = int(core_s)
        app_version = int(app_s)
    except ValueError:
        raise HandshakeError(f"non-integer version in handshake: {raw!r}") from None

    if core_version != constants.CORE_PROTOCOL_VERSION:
        raise HandshakeError(
            f"unsupported CORE-PROTOCOL-VERSION {core_version} "
            f"(host pins {constants.CORE_PROTOCOL_VERSION}): {raw!r}"
        )
    if app_version != constants.PLUGIN_PROTOCOL_VERSION:
        raise HandshakeError(
            f"unsupported APP-PROTOCOL-VERSION {app_version} "
            f"(host speaks {constants.PLUGIN_PROTOCOL_VERSION}): {raw!r}"
        )
    if network not in VALID_NETWORKS:
        raise HandshakeError(f"NETWORK must be one of {VALID_NETWORKS}: {raw!r}")
    if not address:
        raise HandshakeError(f"empty ADDRESS: {raw!r}")
    if protocol != VALID_PROTOCOL:
        raise HandshakeError(f"PROTOCOL must be {VALID_PROTOCOL!r}: {raw!r}")

    return HandshakeConfig(core_version, app_version, network, address, protocol)


def format_handshake_line(config: HandshakeConfig) -> str:
    """Produce a handshake line. Used by test plugins and by any Python-authored plugin."""
    return "|".join(
        [
            str(config.core_version),
            str(config.app_version),
            config.network,
            config.address,
            config.protocol,
        ]
    )


def plugin_env(base: dict[str, str] | None = None) -> dict[str, str]:
    """Environment for a plugin subprocess, with the magic cookie set."""
    env = dict(base if base is not None else os.environ)
    env[constants.MAGIC_COOKIE_KEY] = constants.MAGIC_COOKIE_VALUE
    return env


class PluginProcess:
    """A launched plugin subprocess and its gRPC connection.

    Lifecycle: launch -> handshake -> (health / serve) -> shutdown.
    """

    def __init__(self, name: str, command: list[str]):
        self.name = name
        self.command = command
        self.process: asyncio.subprocess.Process | None = None
        self.handshake: HandshakeConfig | None = None
        self.channel: aio.Channel | None = None

    async def launch(self) -> HandshakeConfig:
        """Start the subprocess, read its handshake, and open the channel."""
        self.process = await asyncio.create_subprocess_exec(
            *self.command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=plugin_env(),
        )
        try:
            line = await asyncio.wait_for(
                self.process.stdout.readline(), timeout=HANDSHAKE_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            await self._kill()
            raise HandshakeError(
                f"{self.name}: no handshake line within {HANDSHAKE_TIMEOUT_SECONDS}s"
            ) from None

        if not line:
            await self._kill()
            stderr = b""
            if self.process.stderr is not None:
                stderr = await self.process.stderr.read()
            raise HandshakeError(
                f"{self.name}: exited before handshake: {stderr.decode(errors='replace')[:400]}"
            )

        try:
            self.handshake = parse_handshake_line(line.decode(errors="replace"))
        except HandshakeError:
            await self._kill()
            raise

        self.channel = aio.insecure_channel(self.handshake.target)
        return self.handshake

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        """Poll grpc.health.v1. Unreachable counts as unhealthy, never raises."""
        if self.channel is None:
            return False
        stub = health_pb2_grpc.HealthStub(self.channel)
        try:
            response = await stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=timeout)
        except grpc.RpcError as exc:
            logger.debug("%s: health check failed: %s", self.name, exc)
            return False
        return response.status == health_pb2.HealthCheckResponse.SERVING

    async def shutdown(self) -> None:
        """Graceful GRPCController.Shutdown, then kill if the process lingers."""
        if self.channel is not None:
            stub = grpc_controller_pb2_grpc.GRPCControllerStub(self.channel)
            try:
                await stub.Shutdown(grpc_controller_pb2.Empty(), timeout=SHUTDOWN_GRACE_SECONDS)
            except grpc.RpcError as exc:
                # A plugin that closes the connection as it exits is the normal path.
                logger.debug("%s: Shutdown RPC ended with %s", self.name, exc)
            await self.channel.close()
            self.channel = None

        if self.process is None:
            return
        try:
            await asyncio.wait_for(self.process.wait(), timeout=SHUTDOWN_GRACE_SECONDS)
        except asyncio.TimeoutError:
            logger.warning("%s: did not exit gracefully, killing", self.name)
            await self._kill()
        self.process = None

    async def _kill(self) -> None:
        if self.process is None or self.process.returncode is not None:
            return
        self.process.kill()
        await self.process.wait()
