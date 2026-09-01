"""Phase R2 gate: the wire-protocol shim - handshake, health, lifecycle.

PROTOCOL.md sections 1-3 are the spec; this file is the host's proof that it
obeys them. Four things are gated here, and the live ones run against a real
subprocess with real pipes and a real grpc.health.v1 server (tests/plugins/
fake_plugin.py) because every defect this phase targets only appears once an
actual process is on the other end of an actual pipe:

  - Parsing and producing ``CORE|APP|NETWORK|ADDRESS|PROTOCOL`` strictly. CORE
    is pinned; APP gates the wire-protocol major; NETWORK is exactly the set the
    contract permits and is never coerced to a default transport; numeric fields
    are plain decimal digits, so a value like ``+1`` or a non-ASCII digit that
    ``int()`` would happily accept is rejected instead of silently honoured; and
    ADDRESS is checked in the form its declared network requires. Round-tripping
    holds: ``format_handshake_line`` emits only what ``parse_handshake_line``
    takes back.
  - A version mismatch is an operator-visible handshake FAILURE - an exception
    naming both versions - not a plugin that merely reads as unhealthy. The two
    are distinguished by kind, not by log wording.
  - The pipes are drained for the process lifetime. A conforming plugin that
    writes more than one pipe buffer must keep running and keep answering health
    checks; a host that reads stdout once wedges it in ``write()``.
  - Shutdown always ends the process. The graceful RPC failing for ANY reason -
    not only ``grpc.RpcError`` - still falls back to the kill, and a first line
    too long for the stream reader kills the child rather than orphaning it.
    ``is_healthy`` never raises, whatever the channel does.

The contract-derived assertions skip when the sibling contract repo is absent.
"""

import asyncio
import contextlib
import os
import socket
import sys
import time
from pathlib import Path

import pytest
from grpc import aio

from tsigma.plugins import constants, protocol

from tests.plugins import _contract
from tests.plugins._spawn import KILL_SIGNAL

CORE = constants.CORE_PROTOCOL_VERSION
APP = constants.PLUGIN_PROTOCOL_VERSION

# Eight times a 64 KiB pipe buffer, on each of stdout and stderr: far past the
# point where an undrained plugin blocks in write() and stops serving.
BULK_BYTES = 512 * 1024
# Past asyncio's default 64 KiB StreamReader limit, with no newline anywhere.
OVER_LIMIT_BYTES = 200_000

# Ceiling on any single host call against a live plugin. Generous: it is a
# backstop that turns a wedged host into a failure instead of a hung suite.
LIVE_CALL_TIMEOUT = 60.0

# A signed form, non-ASCII digits (Arabic-Indic and fullwidth ONE, written as
# escapes to keep this file ASCII), and an underscore separator: int() accepts
# every one of these, and the first three even yield the pinned version 1, so a
# lenient parser lets an off-spec plugin through unnoticed.
NON_DECIMAL_NUMERICS = ["+1", "\u0661", "\uff11", "1_0", "1.0", "0x1", "-1", "", " "]

BAD_NETWORKS = ["udp", "TCP", "Unix", "tcp4", "tcp6", "unixpacket", "vsock", "", " "]

VALID_TCP_ADDRESSES = ["127.0.0.1:5000", "localhost:1", "0.0.0.0:65535", "[::1]:5000"]
INVALID_TCP_ADDRESSES = [
    "/tmp/plug.sock",     # a socket path is not a tcp address
    "127.0.0.1",          # no port at all
    "127.0.0.1:",         # empty port
    ":5000",              # empty host
    "127.0.0.1:0",        # port 0 is "pick one for me"; nothing is listening there
    "127.0.0.1:65536",    # out of range
    "127.0.0.1:abc",
    "127.0.0.1:+1",
    "127.0.0.1: 5000",
    "",
    "   ",
]

VALID_UNIX_ADDRESSES = ["/tmp/plug.sock", "/var/run/tsigma/decoder.sock"]
INVALID_UNIX_ADDRESSES = ["", "   "]


def _line(core=None, app=None, network="tcp", address="127.0.0.1:5000", protocol_field="grpc") -> str:
    core = CORE if core is None else core
    app = APP if app is None else app
    return f"{core}|{app}|{network}|{address}|{protocol_field}"


def _free_tcp_port() -> int:
    """A port nothing is listening on right now."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _liveness_is_observable() -> bool:
    return Path("/proc").is_dir() or os.name == "posix"


def _process_is_alive(pid: int) -> bool:
    """True while pid names a running process. A reaped or zombie child is not."""
    proc_root = Path("/proc")
    if proc_root.is_dir():
        stat = proc_root / str(pid) / "stat"
        try:
            data = stat.read_bytes()
        except OSError:
            return False
        # "pid (comm) STATE ..." - comm can contain spaces and parentheses, so
        # split from the right on the closing paren. Z = reaped-pending-wait.
        return data.rsplit(b")", 1)[1].split()[0] != b"Z"
    if os.name == "posix":
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:  # pragma: no cover - defensive
            return True
        return True
    pytest.skip("cannot observe process liveness on this platform")


async def _read_pid(pidfile: Path, timeout: float = 15.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            text = pidfile.read_text(encoding="utf-8").strip()
        except OSError:
            text = ""
        if text:
            return int(text)
        await asyncio.sleep(0.05)
    raise AssertionError(f"fake plugin never wrote {pidfile}")


async def _assert_stopped(pid: int, timeout: float = 15.0) -> None:
    if not _liveness_is_observable():  # pragma: no cover - platform guard
        pytest.skip("cannot observe process liveness on this platform")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_is_alive(pid):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"pid {pid} is still running: the host orphaned its plugin subprocess")


async def _bounded(awaitable, what: str, timeout: float = LIVE_CALL_TIMEOUT):
    """Fail loudly instead of hanging the suite when a host call never returns."""
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise AssertionError(f"{what} never returned within {timeout}s") from exc


async def _wait_healthy(plugin: protocol.PluginProcess, timeout: float = 25.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await _bounded(plugin.is_healthy(timeout=1.0), "is_healthy", timeout=15.0):
            return True
        await asyncio.sleep(0.2)
    return False


async def _force_stop(plugin: protocol.PluginProcess) -> None:
    channel = getattr(plugin, "channel", None)
    if channel is not None:
        with contextlib.suppress(Exception):
            await channel.close()
    process = getattr(plugin, "process", None)
    if process is None:
        return
    if process.returncode is None:
        with contextlib.suppress(Exception):
            process.kill()
    # A paused, undrained pipe keeps the subprocess transport from ever
    # finishing, so wait() would block forever: empty both pipes first. The
    # teardown has to survive the very defect these tests are here to catch.
    for stream in (process.stdout, process.stderr):
        if stream is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(stream.read(), timeout=10.0)
    with contextlib.suppress(Exception):
        await asyncio.wait_for(process.wait(), timeout=10.0)


class TestContractPermittedValues:
    """NETWORK and PROTOCOL are the contract's sets, read from the contract."""

    def test_permitted_networks_are_exactly_what_the_contract_publishes(self):
        assert set(protocol.VALID_NETWORKS) == _contract.permitted_networks()

    def test_permitted_protocol_is_exactly_what_the_contract_publishes(self):
        assert protocol.VALID_PROTOCOL == _contract.permitted_protocol()

    def test_contract_still_publishes_both_address_forms(self):
        # The per-network ADDRESS rule below is only legitimate while the
        # contract still defines ADDRESS as one of these two shapes.
        section = _contract.protocol_section_one()
        assert "host:port" in section
        assert "socket path" in section


class TestParseHandshakeLine:
    """One line in, one HandshakeConfig out - or a HandshakeError, never a guess."""

    def test_conforming_tcp_line_parses(self):
        config = protocol.parse_handshake_line(_line(address="127.0.0.1:5000"))
        assert config.core_version == CORE
        assert config.app_version == APP
        assert config.network == "tcp"
        assert config.address == "127.0.0.1:5000"
        assert config.protocol == "grpc"

    def test_conforming_unix_line_parses(self):
        config = protocol.parse_handshake_line(_line(network="unix", address="/tmp/plug.sock"))
        assert config.network == "unix"
        assert config.address == "/tmp/plug.sock"

    @pytest.mark.parametrize("terminator", ["", "\n", "\r\n"])
    def test_line_terminators_are_tolerated(self, terminator):
        assert protocol.parse_handshake_line(_line() + terminator).network == "tcp"

    @pytest.mark.parametrize(
        "raw",
        [
            "",
            "   ",
            "\n",
            f"{CORE}|{APP}|tcp|127.0.0.1:5000",
            f"{CORE}|{APP}|tcp|127.0.0.1:5000|grpc|extra",
            f"{CORE}|{APP}|tcp|127.0.0.1|5000|grpc",
            "not a handshake line at all",
        ],
    )
    def test_field_count_is_exact(self, raw):
        with pytest.raises(protocol.HandshakeError):
            protocol.parse_handshake_line(raw)

    @pytest.mark.parametrize("value", NON_DECIMAL_NUMERICS)
    def test_core_field_takes_plain_decimal_digits_only(self, value):
        with pytest.raises(protocol.HandshakeError):
            protocol.parse_handshake_line(_line(core=value))

    @pytest.mark.parametrize("value", NON_DECIMAL_NUMERICS)
    def test_app_field_takes_plain_decimal_digits_only(self, value):
        with pytest.raises(protocol.HandshakeError):
            protocol.parse_handshake_line(_line(app=value))

    def test_core_version_is_pinned(self):
        with pytest.raises(protocol.HandshakeError) as excinfo:
            protocol.parse_handshake_line(_line(core=CORE + 1))
        assert "CORE" in str(excinfo.value)

    def test_unsupported_app_major_is_rejected_naming_both_versions(self):
        plugin_app = APP + 6
        with pytest.raises(protocol.HandshakeError) as excinfo:
            protocol.parse_handshake_line(_line(app=plugin_app))
        message = str(excinfo.value)
        assert "version" in message.lower()
        assert str(plugin_app) in message, "the diagnostic must name the version the plugin declared"
        assert str(APP) in message, "the diagnostic must name the version this host speaks"
        assert "health" not in message.lower(), "a version mismatch is not a health failure"

    def test_supported_app_major_is_accepted(self):
        assert protocol.parse_handshake_line(_line(app=APP)).app_version == APP

    @pytest.mark.parametrize("network", BAD_NETWORKS)
    def test_unrecognised_network_is_rejected_not_coerced(self, network):
        with pytest.raises(protocol.HandshakeError):
            protocol.parse_handshake_line(_line(network=network))

    @pytest.mark.parametrize("protocol_field", ["", "netrpc", "net/rpc", "GRPC", "http"])
    def test_protocol_must_be_the_only_supported_value(self, protocol_field):
        with pytest.raises(protocol.HandshakeError):
            protocol.parse_handshake_line(_line(protocol_field=protocol_field))

    @pytest.mark.parametrize("address", VALID_TCP_ADDRESSES)
    def test_tcp_address_accepts_host_port(self, address):
        assert protocol.parse_handshake_line(_line(address=address)).address == address

    @pytest.mark.parametrize("address", INVALID_TCP_ADDRESSES)
    def test_tcp_address_rejects_anything_that_is_not_host_port(self, address):
        with pytest.raises(protocol.HandshakeError):
            protocol.parse_handshake_line(_line(address=address))

    @pytest.mark.parametrize("address", VALID_UNIX_ADDRESSES)
    def test_unix_address_accepts_a_socket_path(self, address):
        parsed = protocol.parse_handshake_line(_line(network="unix", address=address))
        assert parsed.address == address

    @pytest.mark.parametrize("address", INVALID_UNIX_ADDRESSES)
    def test_unix_address_rejects_an_empty_path(self, address):
        with pytest.raises(protocol.HandshakeError):
            protocol.parse_handshake_line(_line(network="unix", address=address))

    def test_handshake_error_is_the_module_error(self):
        with pytest.raises(protocol.HandshakeError):
            protocol.parse_handshake_line("garbage")


class TestHandshakeTarget:
    """The channel target follows the declared network, never a default."""

    def test_tcp_target_is_the_address(self):
        config = protocol.parse_handshake_line(_line(address="127.0.0.1:5000"))
        assert config.target == "127.0.0.1:5000"

    def test_unix_target_carries_the_scheme(self):
        config = protocol.parse_handshake_line(_line(network="unix", address="/tmp/plug.sock"))
        assert config.target == "unix:/tmp/plug.sock"


class TestFormatHandshakeLine:
    """Whatever this emits, parse_handshake_line must take back."""

    @pytest.mark.parametrize(
        "network,address",
        [("tcp", "127.0.0.1:5000"), ("tcp", "[::1]:5000"), ("unix", "/tmp/plug.sock")],
    )
    def test_round_trip(self, network, address):
        config = protocol.HandshakeConfig(CORE, APP, network, address, "grpc")
        line = protocol.format_handshake_line(config)
        assert protocol.parse_handshake_line(line) == config

    def test_emitted_line_has_five_fields_and_no_newline(self):
        line = protocol.format_handshake_line(protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:5000", "grpc"))
        assert line.count("|") == 4
        assert "\n" not in line

    @pytest.mark.parametrize(
        "config",
        [
            protocol.HandshakeConfig(CORE, APP, "udp", "127.0.0.1:5000", "grpc"),
            protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:5000", "netrpc"),
            protocol.HandshakeConfig(CORE, APP, "tcp", "a|b:5000", "grpc"),
            protocol.HandshakeConfig(CORE, APP, "tcp", "", "grpc"),
            protocol.HandshakeConfig(CORE, APP, "tcp", "/tmp/plug.sock", "grpc"),
            protocol.HandshakeConfig(CORE, APP, "unix", "", "grpc"),
            protocol.HandshakeConfig(CORE, -1, "tcp", "127.0.0.1:5000", "grpc"),
            protocol.HandshakeConfig(-1, APP, "tcp", "127.0.0.1:5000", "grpc"),
        ],
    )
    def test_refuses_to_emit_a_line_that_would_not_parse(self, config):
        with pytest.raises(protocol.HandshakeError):
            protocol.format_handshake_line(config)


class TestPluginEnv:
    """The magic cookie is a launch precondition, taken from the pinned constants."""

    def test_sets_the_cookie(self):
        env = protocol.plugin_env({"PATH": "/usr/bin"})
        assert env[constants.MAGIC_COOKIE_KEY] == constants.MAGIC_COOKIE_VALUE
        assert env["PATH"] == "/usr/bin"

    def test_does_not_mutate_the_base_mapping(self):
        base = {"PATH": "/usr/bin"}
        protocol.plugin_env(base)
        assert constants.MAGIC_COOKIE_KEY not in base


class TestLaunchHandshake:
    """A real subprocess, a real pipe, a real handshake line."""

    @pytest.mark.asyncio
    async def test_launch_reads_the_handshake_and_the_plugin_serves_health(self, spawn):
        port = _free_tcp_port()
        plugin, _pidfile = spawn(line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        try:
            config = await _bounded(plugin.launch(), "launch")
            assert config.network == "tcp"
            assert config.address == f"127.0.0.1:{port}"
            assert plugin.handshake == config
            assert await _wait_healthy(plugin), "a serving plugin must report healthy"
        finally:
            await _force_stop(plugin)

    @pytest.mark.asyncio
    async def test_unsupported_app_version_fails_the_launch_and_leaves_no_orphan(self, spawn):
        plugin, pidfile = spawn(line=_line(app=APP + 6))
        try:
            with pytest.raises(protocol.HandshakeError) as excinfo:
                await _bounded(plugin.launch(), "launch")
            message = str(excinfo.value)
            assert str(APP + 6) in message
            assert str(APP) in message
            pid = await _read_pid(pidfile)
            await _assert_stopped(pid)
        finally:
            await _force_stop(plugin)

    @pytest.mark.asyncio
    async def test_malformed_handshake_fails_the_launch_and_leaves_no_orphan(self, spawn):
        plugin, pidfile = spawn(line="hello, world")
        try:
            with pytest.raises(protocol.HandshakeError) as excinfo:
                await _bounded(plugin.launch(), "launch")
            assert plugin.name in str(excinfo.value)
            pid = await _read_pid(pidfile)
            await _assert_stopped(pid)
        finally:
            await _force_stop(plugin)

    @pytest.mark.asyncio
    async def test_over_long_first_line_raises_the_module_error_and_kills_the_child(self, spawn):
        # The stream reader gives up on a line past its limit with a bare
        # ValueError. HandshakeError subclasses ValueError, so this assertion is
        # only satisfied when the host catches that and re-raises as its own
        # error - and the child, blocked in write(), must not be left behind.
        plugin, pidfile = spawn(raw_bytes=OVER_LIMIT_BYTES)
        try:
            with pytest.raises(protocol.HandshakeError):
                await _bounded(plugin.launch(), "launch")
            pid = await _read_pid(pidfile)
            await _assert_stopped(pid)
        finally:
            await _force_stop(plugin)

    @pytest.mark.asyncio
    async def test_a_handshake_failure_is_not_a_health_failure(self, spawn):
        # Same plugin binary, two different operator-facing outcomes: an
        # off-version plugin RAISES at handshake, while a plugin that handshakes
        # cleanly and never serves merely reads as unhealthy. That difference in
        # kind is what makes the two distinguishable.
        bad, _ = spawn(line=_line(app=APP + 6))
        quiet, _ = spawn(line=_line(address=f"127.0.0.1:{_free_tcp_port()}"))
        try:
            with pytest.raises(protocol.HandshakeError):
                await _bounded(bad.launch(), "launch")
            await _bounded(quiet.launch(), "launch")
            assert await quiet.is_healthy(timeout=1.0) is False
        finally:
            await _force_stop(bad)
            await _force_stop(quiet)


class TestContinuousDrain:
    """Both pipes are drained for the process lifetime, not read once."""

    @pytest.mark.asyncio
    async def test_a_chatty_plugin_keeps_running_and_stays_answerable(self, spawn):
        # The plugin writes 512 KiB to stdout and 512 KiB to stderr BEFORE it
        # starts serving. With a 64 KiB pipe buffer, a host that reads stdout
        # once for the handshake and then stops leaves the plugin wedged in
        # write(): it never starts its health server and the poll below fails.
        port = _free_tcp_port()
        plugin, pidfile = spawn(
            line=_line(address=f"127.0.0.1:{port}"),
            bulk_bytes=BULK_BYTES,
            serve_port=port,
        )
        try:
            await _bounded(plugin.launch(), "launch")
            assert await _wait_healthy(plugin), (
                "a plugin that wrote more than one pipe buffer must still be serving: "
                "stdout and stderr are not being drained"
            )
            pid = await _read_pid(pidfile)
            assert _process_is_alive(pid)
        finally:
            await _force_stop(plugin)


class TestShutdown:
    """Graceful first, kill always."""

    @pytest.mark.asyncio
    async def test_graceful_shutdown_stops_the_plugin(self, spawn):
        port = _free_tcp_port()
        plugin, pidfile = spawn(
            line=_line(address=f"127.0.0.1:{port}"),
            serve_port=port,
            controller="graceful",
        )
        try:
            await _bounded(plugin.launch(), "launch")
            assert await _wait_healthy(plugin)
            pid = await _read_pid(pidfile)
            await _bounded(plugin.shutdown(), "shutdown")
            await _assert_stopped(pid)
            assert plugin.channel is None
        finally:
            await _force_stop(plugin)

    @pytest.mark.asyncio
    async def test_shutdown_kills_when_the_rpc_raises_a_non_grpc_error(self, spawn, monkeypatch):
        # The plugin below serves health but no GRPCController, and the stub is
        # replaced with one that fails in a way that is not grpc.RpcError. The
        # kill fallback must not be conditional on the exception type.
        class _ExplodingControllerStub:
            def __init__(self, channel):
                self.channel = channel

            async def Shutdown(self, request, timeout=None):
                raise RuntimeError("controller stub blew up")

        monkeypatch.setattr(
            protocol.grpc_controller_pb2_grpc, "GRPCControllerStub", _ExplodingControllerStub
        )
        monkeypatch.setattr(protocol, "SHUTDOWN_GRACE_SECONDS", 1.0)

        port = _free_tcp_port()
        plugin, pidfile = spawn(line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        try:
            await _bounded(plugin.launch(), "launch")
            assert await _wait_healthy(plugin)
            pid = await _read_pid(pidfile)
            await _bounded(plugin.shutdown(), "shutdown")
            await _assert_stopped(pid)
            assert plugin.channel is None
        finally:
            await _force_stop(plugin)

    @pytest.mark.asyncio
    async def test_shutdown_kills_when_the_channel_is_already_closed(self, spawn, monkeypatch):
        # A channel closed underneath the host raises grpc.aio.UsageError, which
        # is not an RpcError. The process must still be killed.
        monkeypatch.setattr(protocol, "SHUTDOWN_GRACE_SECONDS", 1.0)
        port = _free_tcp_port()
        plugin, pidfile = spawn(line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        try:
            await _bounded(plugin.launch(), "launch")
            assert await _wait_healthy(plugin)
            pid = await _read_pid(pidfile)
            await plugin.channel.close()
            await _bounded(plugin.shutdown(), "shutdown")
            await _assert_stopped(pid)
        finally:
            await _force_stop(plugin)

    @pytest.mark.asyncio
    async def test_shutdown_is_safe_before_a_launch(self):
        plugin = protocol.PluginProcess("never-launched", [sys.executable, "-c", "pass"])
        await plugin.shutdown()


class TestIsHealthyNeverRaises:
    """Health is a bool, in every failure mode. An exception here is a defect."""

    @pytest.mark.asyncio
    async def test_false_without_a_channel(self):
        plugin = protocol.PluginProcess("no-channel", [sys.executable, "-c", "pass"])
        assert await plugin.is_healthy(timeout=0.5) is False

    @pytest.mark.asyncio
    async def test_false_when_nothing_is_listening(self):
        plugin = protocol.PluginProcess("unreachable", [sys.executable, "-c", "pass"])
        plugin.channel = aio.insecure_channel(f"127.0.0.1:{_free_tcp_port()}")
        try:
            assert await plugin.is_healthy(timeout=1.0) is False
        finally:
            await plugin.channel.close()

    @pytest.mark.asyncio
    async def test_false_on_a_closed_channel(self):
        # grpc.aio raises UsageError, not RpcError, once the channel is closed.
        plugin = protocol.PluginProcess("closed-channel", [sys.executable, "-c", "pass"])
        plugin.channel = aio.insecure_channel(f"127.0.0.1:{_free_tcp_port()}")
        await plugin.channel.close()
        assert await plugin.is_healthy(timeout=1.0) is False

    @pytest.mark.parametrize(
        "boom",
        [
            lambda: asyncio.TimeoutError("deadline"),
            lambda: OSError("socket went away"),
            lambda: RuntimeError("stub is confused"),
        ],
    )
    @pytest.mark.asyncio
    async def test_false_when_the_health_stub_raises(self, monkeypatch, boom):
        class _RaisingHealthStub:
            def __init__(self, channel):
                self.channel = channel

            async def Check(self, request, timeout=None):
                raise boom()

        monkeypatch.setattr(protocol.health_pb2_grpc, "HealthStub", _RaisingHealthStub)
        plugin = protocol.PluginProcess("raising-stub", [sys.executable, "-c", "pass"])
        plugin.channel = aio.insecure_channel(f"127.0.0.1:{_free_tcp_port()}")
        try:
            assert await plugin.is_healthy(timeout=0.5) is False
        finally:
            await plugin.channel.close()

    @pytest.mark.asyncio
    async def test_false_after_the_plugin_dies(self, spawn):
        port = _free_tcp_port()
        plugin, pidfile = spawn(line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        try:
            await _bounded(plugin.launch(), "launch")
            assert await _wait_healthy(plugin)
            pid = await _read_pid(pidfile)
            os.kill(pid, KILL_SIGNAL)
            await _assert_stopped(pid)
            assert await plugin.is_healthy(timeout=1.0) is False
        finally:
            await _force_stop(plugin)
