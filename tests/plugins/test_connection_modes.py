"""Phase R3a gate: the connection seam and the three ADR-0019 process models.

`PluginConnection` is what the supervisor, the registries, and the remote report
and decoder wrappers all code against, so the seam - not any one concrete class -
is what has to hold. Four things are gated here:

  - Conformance is proven by exercising the seam. Every implementation's
    ``connect``, ``is_healthy`` and ``shutdown`` are coroutine functions carrying
    the signature the Protocol declares, ``host_owns_lifecycle`` is a property,
    and the declared ``process_model`` is the one ADR-0019 names for that mode.
    The ``@runtime_checkable`` ``isinstance()`` check is deliberately NOT the
    gate: it compares attribute names only, so a class with a synchronous
    ``connect`` and an ``is_healthy`` that takes no timeout satisfies it. An
    impostor built below holds that distinction in place.
  - Mode 1, core-managed child: the host launches the process, reads the
    handshake off its stdout, dials the address the plugin advertised, and owns
    the shutdown. A handshake the gate refuses fails the connect and leaves no
    orphan behind.
  - Mode 2, externally orchestrated: the fields arrive from a manifest instead of
    a pipe, and a manifest on disk can drift or lie (ADR-0020), so they pass the
    SAME gate a stdout line passes - protocol major, network, and address form
    included. Sameness is asserted as a property, not by inspecting the call: one
    table of configurations, refused both ways. The host dials and health-polls;
    it never launches, restarts, or stops the plugin, and ``shutdown`` closes
    this host's channel while the plugin keeps running under its orchestrator.
  - Mode 3, cron: a run starts, works, and exits. Between runs the plugin is
    IDLE, and idle is a state of its own - it is not unhealthy. Both read as "not
    serving", so ``is_healthy`` alone cannot separate them; the seam has to.
    Health is per-run.

The live cases run against real subprocesses, real pipes and a real
grpc.health.v1 server (tests/plugins/fake_plugin.py), because the failures this
phase targets only appear once a real process is on the other end of a real
socket. The fake plugin is reused for mode 2 exactly as it stands: the test
starts it itself and points a `DiscoveredConnection` at it, which is what "a
plugin this host did not launch" means.
"""

import asyncio
import contextlib
import inspect
import socket
import sys
import time
from pathlib import Path

import pytest

from tsigma.plugins import constants, protocol
from tsigma.plugins.connection import (
    DiscoveredConnection,
    LaunchedConnection,
    PluginConnection,
    ProcessModel,
    ScheduledConnection,
)

from tests.plugins import _contract
from tests.plugins.test_protocol_handshake import (
    _assert_stopped,
    _bounded,
    _force_stop,
    _free_tcp_port,
    _line,
    _process_is_alive,
    _read_pid,
    _wait_healthy,
)

CORE = constants.CORE_PROTOCOL_VERSION
APP = constants.PLUGIN_PROTOCOL_VERSION

# Long enough for a process to be started, observed and reaped on a loaded box;
# short enough that a seam which never changes state fails instead of hanging.
STATE_TIMEOUT = 30.0

# The three seam coroutines, with the parameters the Protocol declares. A
# consumer awaits each one and passes `timeout` by keyword, so name, kind and
# default are all part of the contract, not just the method's existence.
SEAM_COROUTINES = ("connect", "is_healthy", "shutdown")


def _manifest_handshake(port: int) -> protocol.HandshakeConfig:
    """What a well-formed ADR-0020 manifest entry yields for an external plugin."""
    return protocol.HandshakeConfig(CORE, APP, "tcp", f"127.0.0.1:{port}", "grpc")


# One claim, made two ways. Every entry is a handshake this host must refuse on
# the wire; a manifest is the same claim typed into a file, so mode 2 must refuse
# each of them too. Both directions are asserted from this single table, which is
# what "the same gate" has to mean if it is to mean anything.
REJECTED_HANDSHAKES = [
    ("core-version-ahead", protocol.HandshakeConfig(CORE + 1, APP, "tcp", "127.0.0.1:5000", "grpc")),
    ("core-version-behind", protocol.HandshakeConfig(CORE - 1, APP, "tcp", "127.0.0.1:5000", "grpc")),
    ("app-major-ahead", protocol.HandshakeConfig(CORE, APP + 6, "tcp", "127.0.0.1:5000", "grpc")),
    ("app-major-behind", protocol.HandshakeConfig(CORE, APP - 1, "tcp", "127.0.0.1:5000", "grpc")),
    ("network-udp", protocol.HandshakeConfig(CORE, APP, "udp", "127.0.0.1:5000", "grpc")),
    ("network-uppercase", protocol.HandshakeConfig(CORE, APP, "TCP", "127.0.0.1:5000", "grpc")),
    ("network-tcp4", protocol.HandshakeConfig(CORE, APP, "tcp4", "127.0.0.1:5000", "grpc")),
    ("network-empty", protocol.HandshakeConfig(CORE, APP, "", "127.0.0.1:5000", "grpc")),
    ("protocol-http", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:5000", "http")),
    ("protocol-netrpc", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:5000", "netrpc")),
    ("protocol-uppercase", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:5000", "GRPC")),
    ("tcp-address-no-port", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1", "grpc")),
    ("tcp-address-empty-port", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:", "grpc")),
    ("tcp-address-port-zero", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:0", "grpc")),
    ("tcp-address-port-too-high", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:65536", "grpc")),
    ("tcp-address-signed-port", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:+1", "grpc")),
    ("tcp-address-is-a-socket-path", protocol.HandshakeConfig(CORE, APP, "tcp", "/tmp/plug.sock", "grpc")),
    ("tcp-address-empty", protocol.HandshakeConfig(CORE, APP, "tcp", "", "grpc")),
    ("unix-address-empty", protocol.HandshakeConfig(CORE, APP, "unix", "   ", "grpc")),
    ("delimiter-injected", protocol.HandshakeConfig(CORE, APP, "tcp", "a|b:5000", "grpc")),
    ("newline-injected", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:5000\nx", "grpc")),
]

REJECTED_IDS = [name for name, _ in REJECTED_HANDSHAKES]
REJECTED_CONFIGS = [config for _, config in REJECTED_HANDSHAKES]

ACCEPTED_HANDSHAKES = [
    ("tcp", protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:5000", "grpc")),
    ("tcp-ipv6", protocol.HandshakeConfig(CORE, APP, "tcp", "[::1]:5000", "grpc")),
    ("unix", protocol.HandshakeConfig(CORE, APP, "unix", "/tmp/plug.sock", "grpc")),
]

ACCEPTED_IDS = [name for name, _ in ACCEPTED_HANDSHAKES]
ACCEPTED_CONFIGS = [config for _, config in ACCEPTED_HANDSHAKES]


def _one_shot_command(line: str) -> list[str]:
    """A cron plugin that does its work and exits, which is the whole of mode 3.

    Not the fake plugin: every fake-plugin path either serves forever or sleeps
    forever, and the state under test here is the process that came back.
    """
    program = "import sys; sys.stdout.write(sys.argv[1] + chr(10)); sys.stdout.flush()"
    return [sys.executable, "-c", program, line]


async def _release(connection) -> None:
    """Best-effort teardown. Never masks the assertion that actually failed."""
    with contextlib.suppress(Exception):
        await asyncio.wait_for(connection.shutdown(), timeout=STATE_TIMEOUT)
    inner = getattr(connection, "_proc", None)
    if inner is not None:
        await _force_stop(inner)


async def _wait_until(predicate, what: str, timeout: float = STATE_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"the seam never reported {what} (last value {last!r})")


async def _wait_for_port(port: int, timeout: float = STATE_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with contextlib.suppress(OSError):
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        await asyncio.sleep(0.1)
    raise AssertionError(f"the external plugin never listened on 127.0.0.1:{port}")


async def _start_external(command: list[str]) -> asyncio.subprocess.Process:
    """Start a plugin this host does not own - systemd or k8s, played by the test."""
    return await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        env=protocol.plugin_env(),
    )


def _argv(spawn, **kwargs) -> tuple[list[str], Path]:
    """Fake-plugin argv and its pidfile, from the shared ``spawn`` fixture.

    ``spawn`` (tests/plugins/conftest.py) hands back a ``PluginProcess``, which
    launches nothing until ``launch()`` is awaited - it only holds the argv.
    Here the mode under test does the launching, or in mode 2 does not launch at
    all, so the argv is what is taken; going through ``spawn`` is what puts every
    subprocess this module starts under that fixture's reaping teardown.
    """
    plugin, pidfile = spawn(**kwargs)
    return plugin.command, pidfile


def _child(spawn, **kwargs) -> tuple[LaunchedConnection, Path]:
    """A mode 1 connection over the fake plugin, plus its pidfile."""
    command, pidfile = _argv(spawn, **kwargs)
    return LaunchedConnection(f"child-{pidfile.stem}", command), pidfile


def _seam_signature(method_name: str) -> list[tuple[str, object, object]]:
    """(name, kind, default) per parameter, as the Protocol declares them."""
    declared = inspect.signature(getattr(PluginConnection, method_name))
    return [(p.name, p.kind, p.default) for p in declared.parameters.values()]


def assert_seam_conformance(implementation: type) -> None:
    """Exercise the seam on a class, rather than trusting an isinstance() check.

    Attribute presence is not conformance: a consumer awaits ``connect``, passes
    ``timeout`` to ``is_healthy`` by keyword, and reads ``host_owns_lifecycle``
    without calling it. Each of those is checked here.
    """
    for method_name in SEAM_COROUTINES:
        member = getattr(implementation, method_name, None)
        assert member is not None, f"{implementation.__name__} has no {method_name}"
        assert inspect.iscoroutinefunction(member), (
            f"{implementation.__name__}.{method_name} must be a coroutine function: every consumer awaits it"
        )
        actual = [(p.name, p.kind, p.default) for p in inspect.signature(member).parameters.values()]
        assert actual == _seam_signature(method_name), (
            f"{implementation.__name__}.{method_name}{tuple(n for n, _, _ in actual)} does not match "
            f"the signature PluginConnection declares"
        )

    lifecycle = inspect.getattr_static(implementation, "host_owns_lifecycle")
    assert isinstance(lifecycle, property), (
        f"{implementation.__name__}.host_owns_lifecycle must be a property, not a bare attribute"
    )

    assert isinstance(implementation.process_model, ProcessModel), (
        f"{implementation.__name__}.process_model must be a ProcessModel member"
    )


class _AttributePresenceImpostor:
    """Every name the Protocol declares, none of the behaviour behind them.

    Synchronous where the seam is async, an ``is_healthy`` that takes no timeout,
    a plain boolean where a property belongs, and a ``process_model`` that is a
    bare string. Nothing may dispatch to this, yet ``isinstance`` says yes.
    """

    name = "impostor"
    process_model = "child"
    handshake = None
    channel = None
    idle = False
    host_owns_lifecycle = True

    def connect(self):
        return None

    def is_healthy(self):
        return True

    def shutdown(self):
        return None


class _MissingIdle:
    """Conforms to the pre-idle seam only: everything except an idle report."""

    name = "no-idle"
    process_model = ProcessModel.CHILD
    handshake = None
    channel = None

    @property
    def host_owns_lifecycle(self) -> bool:
        return True

    async def connect(self):
        return None

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        return False

    async def shutdown(self) -> None:
        return None


class TestSeamConformance:
    """What every consumer may rely on, checked by exercising it."""

    @pytest.mark.parametrize(
        "implementation", [LaunchedConnection, DiscoveredConnection, ScheduledConnection]
    )
    def test_every_mode_conforms_to_the_seam(self, implementation):
        assert_seam_conformance(implementation)

    def test_isinstance_alone_is_not_conformance(self):
        # The reason this file introspects instead of asserting isinstance: the
        # impostor satisfies the runtime-checkable Protocol and would still break
        # every consumer, because a Protocol isinstance() check compares
        # attribute NAMES and nothing else.
        impostor = _AttributePresenceImpostor()
        assert isinstance(impostor, PluginConnection), (
            "the premise of this test has changed: attribute presence no longer satisfies the Protocol"
        )
        with pytest.raises(AssertionError):
            assert_seam_conformance(_AttributePresenceImpostor)

    def test_declared_process_models_are_the_three_adr_0019_modes(self):
        assert LaunchedConnection.process_model is ProcessModel.CHILD
        assert DiscoveredConnection.process_model is ProcessModel.EXTERNAL
        assert ScheduledConnection.process_model is ProcessModel.CRON

    def test_each_mode_declares_a_distinct_process_model(self):
        declared = [
            LaunchedConnection.process_model,
            DiscoveredConnection.process_model,
            ScheduledConnection.process_model,
        ]
        assert len(set(declared)) == len(declared)

    def test_lifecycle_ownership_follows_the_mode(self):
        assert LaunchedConnection("child", [sys.executable]).host_owns_lifecycle is True
        assert ScheduledConnection("cron", [sys.executable]).host_owns_lifecycle is True
        external = DiscoveredConnection("external", _manifest_handshake(5000))
        assert external.host_owns_lifecycle is False, (
            "the orchestrator owns an external plugin; this host may not start or stop it"
        )

    def test_the_seam_reports_idle(self):
        # remote_report and remote_decoder read the seam and nothing else, and
        # the supervisor must be able to ask any connection whether it is idle
        # rather than special-casing mode 3. A class without an idle report is
        # therefore not a PluginConnection.
        assert not isinstance(_MissingIdle(), PluginConnection), (
            "PluginConnection must declare `idle`: idle is a seam-level state, not a mode-3 detail"
        )

    @pytest.mark.parametrize(
        "connection_factory",
        [
            lambda: LaunchedConnection("child", [sys.executable, "-c", "pass"]),
            lambda: DiscoveredConnection("external", _manifest_handshake(5000)),
            lambda: ScheduledConnection("cron", [sys.executable, "-c", "pass"]),
        ],
        ids=["child", "external", "cron"],
    )
    def test_channel_and_name_are_readable_before_any_connect(self, connection_factory):
        # remote_report.py and remote_decoder.py both read `.channel` and branch
        # on None before building a stub, so reading it on an unconnected
        # connection must be safe in every mode and must say "not connected".
        connection = connection_factory()
        assert connection.channel is None
        assert isinstance(connection.name, str) and connection.name
        assert isinstance(connection.idle, bool)

    def test_only_an_external_connection_knows_its_handshake_before_connecting(self):
        assert LaunchedConnection("child", [sys.executable]).handshake is None
        assert ScheduledConnection("cron", [sys.executable]).handshake is None
        manifest = _manifest_handshake(5000)
        assert DiscoveredConnection("external", manifest).handshake == manifest


class TestModeOneLaunchedChild:
    """Mode 1: the host launches, reads the stdout handshake, and dials."""

    @pytest.mark.asyncio
    async def test_connect_launches_reads_the_handshake_and_dials(self, spawn):
        port = _free_tcp_port()
        connection, pidfile = _child(spawn, line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        try:
            config = await _bounded(connection.connect(), "connect")
            assert config.address == f"127.0.0.1:{port}", "the handshake must come off the plugin's stdout"
            assert connection.handshake == config
            assert connection.channel is not None, "connect must dial the address the plugin advertised"
            pid = await _read_pid(pidfile)
            assert _process_is_alive(pid), "mode 1 launches the plugin as a child of this host"
            assert await _wait_healthy(connection), "a serving child must report healthy through the seam"
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_a_launched_child_is_never_idle_while_it_runs(self, spawn):
        # Idle is mode 3's between-runs state. A long-lived child is either
        # serving or not serving; it is never idle, or the supervisor would stop
        # restarting a crashed one.
        port = _free_tcp_port()
        connection, _pidfile = _child(spawn, line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        try:
            await _bounded(connection.connect(), "connect")
            assert await _wait_healthy(connection)
            assert connection.idle is False
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_a_handshake_the_gate_refuses_fails_the_connect_and_leaves_no_orphan(self, spawn):
        connection, pidfile = _child(spawn, line=_line(app=APP + 6))
        try:
            with pytest.raises(protocol.HandshakeError) as excinfo:
                await _bounded(connection.connect(), "connect")
            assert str(APP + 6) in str(excinfo.value)
            pid = await _read_pid(pidfile)
            await _assert_stopped(pid)
            assert connection.channel is None, "a refused handshake must not leave a dialled channel"
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_shutdown_stops_the_child_this_host_launched(self, spawn):
        port = _free_tcp_port()
        connection, pidfile = _child(
            spawn,
            line=_line(address=f"127.0.0.1:{port}"),
            serve_port=port,
            controller="graceful",
        )
        try:
            await _bounded(connection.connect(), "connect")
            assert await _wait_healthy(connection)
            pid = await _read_pid(pidfile)
            await _bounded(connection.shutdown(), "shutdown")
            await _assert_stopped(pid)
            assert connection.channel is None
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_health_is_false_once_the_child_dies(self, spawn):
        port = _free_tcp_port()
        connection, pidfile = _child(spawn, line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        try:
            await _bounded(connection.connect(), "connect")
            assert await _wait_healthy(connection)
            pid = await _read_pid(pidfile)
            await _force_stop(connection._proc)
            await _assert_stopped(pid)
            assert await connection.is_healthy(timeout=1.0) is False
        finally:
            await _release(connection)


class TestModeTwoManifestHandshakeGate:
    """Mode 2's fields come from a manifest, and go through the wire's gate."""

    @pytest.mark.parametrize("config", REJECTED_CONFIGS, ids=REJECTED_IDS)
    def test_the_wire_form_of_each_claim_is_refused(self, config):
        # Establishes the premise of the test below: every configuration in the
        # table really is one this host refuses when a plugin prints it.
        with pytest.raises(protocol.HandshakeError):
            protocol.format_handshake_line(config)

    @pytest.mark.parametrize("config", REJECTED_CONFIGS, ids=REJECTED_IDS)
    def test_the_manifest_form_of_each_claim_is_refused_too(self, config):
        # A manifest on disk can drift or lie (ADR-0020). Accepting from a file
        # what would be refused from a pipe is a hole in the gate, not a
        # convenience: it is how an off-version or off-transport plugin gets
        # dialled anyway.
        with pytest.raises(protocol.HandshakeError):
            DiscoveredConnection("external", config)

    @pytest.mark.parametrize("config", ACCEPTED_CONFIGS, ids=ACCEPTED_IDS)
    def test_a_conforming_manifest_handshake_is_accepted_unchanged(self, config):
        connection = DiscoveredConnection("external", config)
        assert connection.handshake == config
        assert connection.channel is None

    def test_the_protocol_major_is_part_of_the_manifest_gate(self):
        with pytest.raises(protocol.HandshakeError) as excinfo:
            DiscoveredConnection("external", protocol.HandshakeConfig(CORE, APP + 6, "tcp", "127.0.0.1:5000", "grpc"))
        message = str(excinfo.value)
        assert str(APP + 6) in message, "the diagnostic must name the version the manifest declared"
        assert str(APP) in message, "the diagnostic must name the version this host speaks"
        assert "health" not in message.lower(), "a manifest version mismatch is a handshake failure, not a health one"

    def test_the_network_the_manifest_declares_is_the_contract_set(self):
        permitted = _contract.permitted_networks()
        assert "udp" not in permitted, "the contract changed; this test's premise is stale"
        for network in permitted:
            address = "127.0.0.1:5000" if network == "tcp" else "/tmp/plug.sock"
            config = protocol.HandshakeConfig(CORE, APP, network, address, "grpc")
            assert DiscoveredConnection("external", config).handshake.network == network
        with pytest.raises(protocol.HandshakeError):
            DiscoveredConnection("external", protocol.HandshakeConfig(CORE, APP, "udp", "127.0.0.1:5000", "grpc"))

    def test_a_refused_manifest_never_yields_a_connection_to_dial(self):
        # The failure has to land at construction, where the supervisor can mark
        # the plugin unavailable, and not at the first dispatch through a
        # half-built connection.
        bad = protocol.HandshakeConfig(CORE, APP, "udp", "127.0.0.1:5000", "grpc")
        with pytest.raises(protocol.HandshakeError):
            DiscoveredConnection("external", bad)


class TestModeTwoExternallyOrchestrated:
    """Mode 2: dial, poll, observe. Never launch, never restart, never stop."""

    @pytest.mark.asyncio
    async def test_connect_dials_a_plugin_this_host_did_not_launch(self, spawn):
        port = _free_tcp_port()
        command, pidfile = _argv(spawn, serve_port=port)
        external = await _start_external(command)
        connection = DiscoveredConnection("external", _manifest_handshake(port))
        try:
            await _wait_for_port(port)
            config = await _bounded(connection.connect(), "connect")
            assert config == _manifest_handshake(port)
            assert connection.channel is not None
            assert await _wait_healthy(connection), "a serving external plugin must poll healthy"
            pid = await _read_pid(pidfile)
            assert _process_is_alive(pid)
        finally:
            await _release(connection)
            with contextlib.suppress(Exception):
                external.kill()
                await external.wait()

    @pytest.mark.asyncio
    async def test_an_external_connection_is_never_idle(self):
        # Idle means "between scheduled runs". An external plugin has no runs;
        # it is up or it is down, and the supervisor must not mistake a down one
        # for a resting one.
        connection = DiscoveredConnection("external", _manifest_handshake(_free_tcp_port()))
        try:
            await _bounded(connection.connect(), "connect")
            assert await connection.is_healthy(timeout=1.0) is False
            assert connection.idle is False, "an unreachable external plugin is unhealthy, not idle"
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_nothing_in_mode_two_starts_a_process(self, monkeypatch):
        async def _forbidden(*args, **kwargs):
            raise AssertionError("mode 2 must not spawn a process: the orchestrator owns the lifecycle")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _forbidden)
        connection = DiscoveredConnection("external", _manifest_handshake(_free_tcp_port()))
        try:
            await _bounded(connection.connect(), "connect")
            assert await connection.is_healthy(timeout=1.0) is False
            await _bounded(connection.shutdown(), "shutdown")
            await _bounded(connection.connect(), "reconnect")
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_the_host_has_no_command_to_launch_an_external_plugin_with(self):
        connection = DiscoveredConnection("external", _manifest_handshake(5000))
        assert not hasattr(connection, "command"), (
            "an external plugin is not startable from here; holding a command invites a restart"
        )

    @pytest.mark.asyncio
    async def test_shutdown_closes_this_hosts_channel_and_leaves_the_plugin_running(self, spawn):
        port = _free_tcp_port()
        command, pidfile = _argv(spawn, serve_port=port)
        external = await _start_external(command)
        connection = DiscoveredConnection("external", _manifest_handshake(port))
        try:
            await _wait_for_port(port)
            await _bounded(connection.connect(), "connect")
            assert await _wait_healthy(connection)
            pid = await _read_pid(pidfile)

            await _bounded(connection.shutdown(), "shutdown")
            assert connection.channel is None, "shutdown must release this host's channel"
            assert _process_is_alive(pid), (
                "mode 2 shutdown must not stop the plugin: systemd or k8s owns it"
            )

            # Still there, and still dialable: the proof the host only detached.
            await _bounded(connection.connect(), "reconnect")
            assert await _wait_healthy(connection), "an external plugin must be reconnectable after a detach"
        finally:
            await _release(connection)
            with contextlib.suppress(Exception):
                external.kill()
                await external.wait()

    @pytest.mark.asyncio
    async def test_health_goes_false_when_the_orchestrated_plugin_goes_away(self, spawn):
        port = _free_tcp_port()
        command, pidfile = _argv(spawn, serve_port=port)
        external = await _start_external(command)
        connection = DiscoveredConnection("external", _manifest_handshake(port))
        try:
            await _wait_for_port(port)
            await _bounded(connection.connect(), "connect")
            assert await _wait_healthy(connection)
            pid = await _read_pid(pidfile)
            external.kill()
            await external.wait()
            await _assert_stopped(pid)
            assert await connection.is_healthy(timeout=2.0) is False, (
                "the host observes an absent external plugin as unhealthy"
            )
            assert connection.idle is False
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_health_is_false_before_the_dial(self):
        connection = DiscoveredConnection("external", _manifest_handshake(_free_tcp_port()))
        assert await connection.is_healthy(timeout=1.0) is False


class TestModeThreeScheduledRun:
    """Mode 3: start, run, exit. Between runs is idle, and idle is not unhealthy."""

    @pytest.mark.asyncio
    async def test_a_cron_plugin_that_has_never_run_is_idle(self):
        connection = ScheduledConnection("cron", _one_shot_command(_line()))
        assert connection.idle is True, "a cron plugin before its first run is idle, not failed"
        assert await connection.is_healthy(timeout=1.0) is False
        assert connection.channel is None
        assert connection.handshake is None

    @pytest.mark.asyncio
    async def test_a_run_in_flight_is_not_idle_and_reports_health(self, spawn):
        port = _free_tcp_port()
        command, pidfile = _argv(spawn, line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        connection = ScheduledConnection(f"cron-{pidfile.stem}", command)
        try:
            await _bounded(connection.connect(), "connect")
            assert await _wait_healthy(connection), "health is per-run: a run in flight answers"
            assert connection.idle is False, "a plugin that is working is not idle"
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_a_run_that_exited_normally_reports_idle_not_unhealthy(self):
        # The whole distinction, in one test. The process handshook, did its
        # work, and exited 0. `is_healthy` is False - there is nothing serving -
        # but nothing is wrong, so a supervisor must be able to tell this apart
        # from a crash before it restarts anything.
        line = _line(address=f"127.0.0.1:{_free_tcp_port()}")
        connection = ScheduledConnection("cron", _one_shot_command(line))
        try:
            await _bounded(connection.connect(), "connect")
            await _wait_until(lambda: connection.idle, "idle after a run exited normally")
            assert await connection.is_healthy(timeout=1.0) is False
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_idle_and_unhealthy_are_told_apart_by_more_than_health(self, spawn):
        # Two connections, both answering False to is_healthy, in two different
        # states: one exited cleanly and is resting, the other is alive and not
        # serving. A seam that only exposes health cannot separate them, and a
        # supervisor that cannot separate them either restarts a resting plugin
        # or ignores a broken one.
        rested = ScheduledConnection("rested", _one_shot_command(_line(address=f"127.0.0.1:{_free_tcp_port()}")))
        command, pidfile = _argv(spawn, line=_line(address=f"127.0.0.1:{_free_tcp_port()}"))
        stuck = ScheduledConnection(f"cron-{pidfile.stem}", command)
        try:
            await _bounded(rested.connect(), "connect rested")
            await _bounded(stuck.connect(), "connect stuck")
            await _wait_until(lambda: rested.idle, "idle after a clean exit")

            assert await rested.is_healthy(timeout=1.0) is False
            assert await stuck.is_healthy(timeout=1.0) is False
            assert rested.idle is True
            assert stuck.idle is False, (
                "a live run that is not serving is unhealthy, not idle; the seam must not conflate them"
            )
        finally:
            await _release(rested)
            await _release(stuck)

    @pytest.mark.asyncio
    async def test_shutdown_ends_the_run_and_returns_the_plugin_to_idle(self, spawn):
        port = _free_tcp_port()
        command, pidfile = _argv(
            spawn,
            line=_line(address=f"127.0.0.1:{port}"),
            serve_port=port,
            controller="graceful",
        )
        connection = ScheduledConnection(f"cron-{pidfile.stem}", command)
        try:
            await _bounded(connection.connect(), "connect")
            assert await _wait_healthy(connection)
            pid = await _read_pid(pidfile)
            await _bounded(connection.shutdown(), "shutdown")
            await _assert_stopped(pid)
            assert connection.idle is True, "with no run in flight, a cron plugin is idle"
            assert connection.channel is None
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_health_is_per_run_so_a_new_run_is_healthy_again(self, spawn):
        # An idle plugin is not a dead one: the next scheduled invocation starts
        # a fresh run against the same command, and health follows that run.
        port = _free_tcp_port()
        command, pidfile = _argv(
            spawn,
            line=_line(address=f"127.0.0.1:{port}"),
            serve_port=port,
            controller="graceful",
        )
        connection = ScheduledConnection(f"cron-{pidfile.stem}", command)
        try:
            await _bounded(connection.connect(), "first run")
            assert await _wait_healthy(connection)
            first_pid = await _read_pid(pidfile)
            await _bounded(connection.shutdown(), "end first run")
            await _assert_stopped(first_pid)
            assert connection.idle is True

            await _bounded(connection.connect(), "second run")
            assert await _wait_healthy(connection), "health is per-run: the next run answers on its own"
            assert connection.idle is False
            assert connection.handshake is not None
        finally:
            await _release(connection)

    @pytest.mark.asyncio
    async def test_a_cron_run_whose_handshake_is_refused_leaves_no_orphan(self, spawn):
        command, pidfile = _argv(spawn, line=_line(app=APP + 6))
        connection = ScheduledConnection(f"cron-{pidfile.stem}", command)
        try:
            with pytest.raises(protocol.HandshakeError):
                await _bounded(connection.connect(), "connect")
            pid = await _read_pid(pidfile)
            await _assert_stopped(pid)
            assert connection.idle is True, "a run that never started is idle again, not left mid-flight"
        finally:
            await _release(connection)
