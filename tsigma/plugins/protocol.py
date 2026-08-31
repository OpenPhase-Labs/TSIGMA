"""TSIGMA Plugin Protocol v1 - handshake, health, lifecycle.

Normative spec: TSIGMA-Contract PROTOCOL.md sections 1-3. The host launches a
plugin subprocess with the magic cookie set, reads one handshake line from its
stdout, dials the advertised address, polls grpc.health.v1, and stops it via
GRPCController.Shutdown with a kill fallback.
"""

import asyncio
import contextlib
import logging
import os
import re
from dataclasses import dataclass

import grpc_broker_pb2  # bare names: resolved by the gen/ path insert in __init__
import grpc_broker_pb2_grpc
import grpc_controller_pb2
import grpc_controller_pb2_grpc
import grpc_stdio_pb2
import grpc_stdio_pb2_grpc
from grpc import aio
from grpc_health.v1 import health_pb2, health_pb2_grpc

from . import constants

logger = logging.getLogger(__name__)

# Re-exported so callers (and plugin authors) never need a bare-name import of
# the generated modules, which only resolve after this package is imported.
__all__ = [
    "HandshakeConfig",
    "HandshakeError",
    "PluginProcess",
    "check_health",
    "format_handshake_line",
    "grpc_broker_pb2",
    "grpc_broker_pb2_grpc",
    "grpc_controller_pb2",
    "grpc_controller_pb2_grpc",
    "grpc_stdio_pb2",
    "grpc_stdio_pb2_grpc",
    "parse_handshake_line",
    "plugin_env",
    "validate_handshake",
]

HANDSHAKE_FIELDS = 5
HANDSHAKE_DELIMITER = "|"
VALID_NETWORKS = ("tcp", "unix")
VALID_PROTOCOL = "grpc"

# Plain decimal digits only. int() would also take "+1", "1_0" and non-ASCII
# digits such as U+0661, every one of which is off-spec on the wire.
DECIMAL_RE = re.compile(r"[0-9]+")

TCP_PORT_MIN = 1
TCP_PORT_MAX = 65535

# How long a plugin gets to print its handshake line. Measured: a Python plugin
# needs ~5s on an idle machine just to import grpc, grpc_health, and pyarrow
# before it can serve, so a 10s budget left no headroom under load. 30s matches
# collector_poll_timeout_seconds, the house number for "an operation that talks
# to a device". A plugin that never prints one is hung and is killed.
HANDSHAKE_TIMEOUT_SECONDS = 30.0
SHUTDOWN_GRACE_SECONDS = 5.0

# Pipe drain: read in blocks, never by line, so a plugin whose output has no
# newline for longer than the stream limit cannot wedge the drain the way it
# can wedge a readline().
DRAIN_CHUNK_BYTES = 65536
STDERR_TAIL_BYTES = 4096
STDERR_READ_TIMEOUT_SECONDS = 5.0


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


def _reject_delimiters(field: str, value: str) -> None:
    if HANDSHAKE_DELIMITER in value:
        raise HandshakeError(f"{field} must not contain {HANDSHAKE_DELIMITER!r}: {value!r}")
    if "\r" in value or "\n" in value:
        raise HandshakeError(f"{field} must not contain a line break: {value!r}")


def _validate_tcp_address(address: str) -> None:
    host, separator, port = address.rpartition(":")
    if not separator or not host:
        raise HandshakeError(f"tcp ADDRESS must be host:port: {address!r}")
    if any(character.isspace() for character in host):
        raise HandshakeError(f"tcp ADDRESS host must not contain whitespace: {address!r}")
    if not DECIMAL_RE.fullmatch(port):
        raise HandshakeError(f"tcp ADDRESS port must be plain decimal digits: {address!r}")
    if not TCP_PORT_MIN <= int(port) <= TCP_PORT_MAX:
        raise HandshakeError(
            f"tcp ADDRESS port must be {TCP_PORT_MIN}-{TCP_PORT_MAX}: {address!r}"
        )


def _validate_unix_address(address: str) -> None:
    if not address.strip():
        raise HandshakeError(f"unix ADDRESS must be a socket path: {address!r}")


def _decimal_version(field: str, value: str) -> int:
    if not DECIMAL_RE.fullmatch(value):
        raise HandshakeError(f"{field} must be plain decimal digits: {value!r}")
    return int(value)


def validate_handshake(config: HandshakeConfig) -> HandshakeConfig:
    """The one gate every handshake passes, wherever the fields came from.

    A stdout line and a manifest entry are the same claim made two ways, so both
    go through here rather than each growing its own half of the rules. Returns
    the config so a caller can wrap a construction in it; raises HandshakeError
    naming the offending field otherwise.
    """
    _reject_delimiters("NETWORK", config.network)
    _reject_delimiters("ADDRESS", config.address)
    _reject_delimiters("PROTOCOL", config.protocol)

    if config.core_version != constants.CORE_PROTOCOL_VERSION:
        raise HandshakeError(
            f"unsupported CORE-PROTOCOL-VERSION {config.core_version}; "
            f"this host pins {constants.CORE_PROTOCOL_VERSION}"
        )
    if config.app_version != constants.PLUGIN_PROTOCOL_VERSION:
        raise HandshakeError(
            f"unsupported plugin wire-protocol version {config.app_version}; "
            f"this host speaks version {constants.PLUGIN_PROTOCOL_VERSION}"
        )
    if config.network not in VALID_NETWORKS:
        raise HandshakeError(f"NETWORK must be one of {VALID_NETWORKS}: {config.network!r}")
    if config.protocol != VALID_PROTOCOL:
        raise HandshakeError(f"PROTOCOL must be {VALID_PROTOCOL!r}: {config.protocol!r}")

    if config.network == "tcp":
        _validate_tcp_address(config.address)
    else:
        _validate_unix_address(config.address)
    return config


def parse_handshake_line(line: str) -> HandshakeConfig:
    """Parse CORE|APP|NETWORK|ADDRESS|PROTOCOL, rejecting anything off-spec.

    Raises HandshakeError with the offending line; the caller surfaces it rather
    than treating an unparseable plugin as merely unhealthy.
    """
    raw = (line or "").strip()
    if not raw:
        raise HandshakeError("empty handshake line")

    parts = raw.split(HANDSHAKE_DELIMITER)
    if len(parts) != HANDSHAKE_FIELDS:
        raise HandshakeError(f"expected {HANDSHAKE_FIELDS} fields, got {len(parts)}: {raw!r}")

    core_s, app_s, network, address, protocol = (p.strip() for p in parts)
    config = HandshakeConfig(
        _decimal_version("CORE-PROTOCOL-VERSION", core_s),
        _decimal_version("APP-PROTOCOL-VERSION", app_s),
        network,
        address,
        protocol,
    )
    return validate_handshake(config)


def format_handshake_line(config: HandshakeConfig) -> str:
    """Produce a handshake line. Used by test plugins and by any Python-authored plugin.

    Emits only what parse_handshake_line takes back: a config this host would
    reject on the wire is refused here instead of being written out.
    """
    validate_handshake(config)
    return HANDSHAKE_DELIMITER.join(
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


async def check_health(channel: aio.Channel | None, name: str, timeout: float) -> bool:
    """Poll grpc.health.v1. Every failure counts as unhealthy; never raises.

    Health is a bool by contract, so a closed channel, a dead peer, a deadline,
    or a misbehaving stub all read the same way to a caller: not serving.
    """
    if channel is None:
        return False
    try:
        stub = health_pb2_grpc.HealthStub(channel)
        response = await stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=timeout)
    except Exception as exc:
        logger.debug("%s: health check failed: %r", name, exc)
        return False
    return response.status == health_pb2.HealthCheckResponse.SERVING


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
        self._drain_tasks: list[asyncio.Task] = []
        self._stderr_tail = b""

    @property
    def stderr_tail(self) -> str:
        """The last of the plugin's stderr, for a diagnostic after a failure."""
        return self._stderr_tail.decode(errors="replace")

    async def launch(self) -> HandshakeConfig:
        """Start the subprocess, read its handshake, and open the channel."""
        self.process = await asyncio.create_subprocess_exec(
            *self.command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=plugin_env(),
        )
        line = await self._read_handshake_line()
        try:
            self.handshake = parse_handshake_line(line.decode(errors="replace"))
        except HandshakeError as exc:
            await self._kill()
            raise HandshakeError(f"{self.name}: {exc}") from None

        self._start_drain()
        self.channel = aio.insecure_channel(self.handshake.target)
        return self.handshake

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        """Poll grpc.health.v1. Unreachable counts as unhealthy, never raises."""
        return await check_health(self.channel, self.name, timeout)

    async def shutdown(self) -> None:
        """Graceful GRPCController.Shutdown, then kill if the process lingers.

        The kill fallback is unconditional. Any failure of the graceful path -
        not only grpc.RpcError - still ends the process.
        """
        if self.channel is not None:
            channel, self.channel = self.channel, None
            try:
                stub = grpc_controller_pb2_grpc.GRPCControllerStub(channel)
                await stub.Shutdown(grpc_controller_pb2.Empty(), timeout=SHUTDOWN_GRACE_SECONDS)
            except Exception as exc:
                # A plugin that closes the connection as it exits is the normal
                # path; anything else falls through to the kill below.
                logger.debug("%s: Shutdown RPC ended with %r", self.name, exc)
            finally:
                with contextlib.suppress(Exception):
                    await channel.close()

        if self.process is not None:
            try:
                await asyncio.wait_for(self.process.wait(), timeout=SHUTDOWN_GRACE_SECONDS)
            except asyncio.TimeoutError:
                logger.warning("%s: did not exit gracefully, killing", self.name)
                await self._kill()
            except Exception as exc:
                logger.warning("%s: wait failed with %r, killing", self.name, exc)
                await self._kill()

        await self._stop_drain()
        self.process = None

    async def _read_handshake_line(self) -> bytes:
        """One line off stdout, or a HandshakeError and a dead subprocess."""
        stdout = self.process.stdout if self.process is not None else None
        if stdout is None:
            await self._kill()
            raise HandshakeError(f"{self.name}: subprocess has no stdout pipe")

        try:
            line = await asyncio.wait_for(stdout.readline(), timeout=HANDSHAKE_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            await self._kill()
            raise HandshakeError(
                f"{self.name}: no handshake line within {HANDSHAKE_TIMEOUT_SECONDS}s"
            ) from None
        except (asyncio.LimitOverrunError, ValueError) as exc:
            # A first line past the stream reader's buffer limit surfaces as a
            # bare ValueError. Left alone it escapes launch() and the child -
            # still blocked in write() - is never killed.
            await self._kill()
            raise HandshakeError(
                f"{self.name}: handshake line exceeds the stream read limit: {exc}"
            ) from None

        if not line:
            await self._kill()
            stderr = await self._read_stderr_after_exit()
            raise HandshakeError(f"{self.name}: exited before handshake: {stderr}")
        return line

    async def _read_stderr_after_exit(self) -> str:
        if self.process is None or self.process.stderr is None:
            return ""
        try:
            data = await asyncio.wait_for(
                self.process.stderr.read(), timeout=STDERR_READ_TIMEOUT_SECONDS
            )
        except Exception:
            return ""
        return data.decode(errors="replace")[:400]

    def _start_drain(self) -> None:
        """Read both pipes for the process lifetime.

        A plugin that writes more than one pipe buffer blocks in write() until
        someone reads, and a blocked plugin stops serving and fails its health
        check. Reading the handshake line is not enough.
        """
        if self.process is None:
            return
        for stream, label in ((self.process.stdout, "stdout"), (self.process.stderr, "stderr")):
            if stream is not None:
                self._drain_tasks.append(asyncio.create_task(self._drain(stream, label)))

    async def _drain(self, stream: asyncio.StreamReader, label: str) -> None:
        while True:
            try:
                chunk = await stream.read(DRAIN_CHUNK_BYTES)
            except Exception as exc:
                logger.debug("%s: %s drain ended with %r", self.name, label, exc)
                return
            if not chunk:
                return
            if label == "stderr":
                self._stderr_tail = (self._stderr_tail + chunk)[-STDERR_TAIL_BYTES:]

    async def _stop_drain(self) -> None:
        tasks, self._drain_tasks = self._drain_tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _kill(self) -> None:
        await self._stop_drain()
        process = self.process
        if process is None:
            return
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError, OSError):
                process.kill()
        with contextlib.suppress(Exception):
            await process.wait()
