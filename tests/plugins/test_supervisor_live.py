"""Phase R3b gate: releasing real subprocesses and real channels.

An orphan is only observable once a real process is on the other end, so these
cases run against tests/plugins/fake_plugin.py rather than a scripted fake. Split
out of test_supervisor.py, which is over the 1000-line cap with them in it
(STYLE_GUIDE.md section 4).

A start that connects but fails its health check RELEASES the subprocess and the
channel, and so does the give-up path. Neither may leave a live process or an open
channel behind.
"""

import asyncio
import contextlib

import pytest

from tsigma.plugins.audit import PluginAuditEvent
from tsigma.plugins.connection import ProcessModel
from tsigma.plugins.supervisor import PluginSpec

from tests.plugins._supervisor_fakes import APP, _events, _live_supervisor, _registry
from tests.plugins.test_protocol_handshake import (
    _assert_stopped,
    _bounded,
    _force_stop,
    _free_tcp_port,
    _line,
    _process_is_alive,
    _read_pid,
)

# Generous: a backstop that turns a wedged supervisor into a failure rather than a
# hung suite, not a performance assertion.
LIVE_TIMEOUT = 90.0


async def _release(connection) -> None:
    """Best-effort teardown that never masks the assertion that actually failed."""
    with contextlib.suppress(Exception):
        await asyncio.wait_for(connection.shutdown(), timeout=LIVE_TIMEOUT)
    inner = getattr(connection, "_proc", None)
    if inner is not None:
        await _force_stop(inner)


# ------------------------------------------------------------- live process release
class TestLiveProcessRelease:
    """An orphan is only observable against a real process on a real pipe."""

    @pytest.mark.asyncio
    async def test_a_start_that_fails_its_health_check_releases_the_subprocess(self, spawn):
        # The plugin handshakes correctly and then never serves: connect succeeds,
        # health fails. Returning early there leaves a live child and an open
        # channel that nothing will ever close.
        port = _free_tcp_port()
        plugin, pidfile = spawn(line=_line(address=f"127.0.0.1:{port}"))
        supervisor = _live_supervisor(registries={})
        supervisor.add(PluginSpec(name="p", process_model=ProcessModel.CHILD, command=plugin.command))

        try:
            assert await _bounded(supervisor.start("p"), "start", timeout=LIVE_TIMEOUT) is False
            pid = await _read_pid(pidfile)
            await _assert_stopped(pid)
            assert supervisor.connection("p").channel is None, (
                "a start that failed must release the channel it dialled"
            )
            assert supervisor.available() == []
        finally:
            await _release(supervisor.connection("p"))

    @pytest.mark.asyncio
    async def test_a_finished_cron_run_releases_the_channel_it_opened(self, spawn):
        # Mode 3 against a real subprocess. The plugin handshakes and exits, which
        # is a cron run completing normally - but launch() opened a channel before
        # it went, and connect() rebinds the process on the next run. A fake whose
        # channel is a string cannot see what that leaks; only a real one can.
        port = _free_tcp_port()
        plugin, pidfile = spawn(
            line=_line(address=f"127.0.0.1:{port}"), exit_after_handshake=True
        )
        supervisor = _live_supervisor(registries={})
        supervisor.add(PluginSpec(name="p", process_model=ProcessModel.CRON, command=plugin.command))

        try:
            assert await _bounded(supervisor.start("p"), "start", timeout=LIVE_TIMEOUT) is False
            connection = supervisor.connection("p")
            assert connection.idle, "a run that has exited is idle, not unhealthy"
            pid = await _read_pid(pidfile)
            await _assert_stopped(pid)
            assert connection.channel is None, (
                "every scheduled run that ends must release its channel, or a cron "
                "plugin leaks one per invocation for the life of the host"
            )
            assert PluginAuditEvent.LAUNCH in _events(supervisor)
        finally:
            await _release(supervisor.connection("p"))

    @pytest.mark.asyncio
    async def test_a_refused_handshake_leaves_no_orphan(self, spawn):
        plugin, pidfile = spawn(line=_line(app=APP + 6))
        supervisor = _live_supervisor(registries={})
        supervisor.add(PluginSpec(name="p", process_model=ProcessModel.CHILD, command=plugin.command))

        try:
            assert await _bounded(supervisor.start("p"), "start", timeout=LIVE_TIMEOUT) is False
            pid = await _read_pid(pidfile)
            await _assert_stopped(pid)
            assert PluginAuditEvent.HANDSHAKE_FAILED in _events(supervisor)
        finally:
            await _release(supervisor.connection("p"))

    @pytest.mark.asyncio
    async def test_a_restart_starts_a_new_process_and_stops_the_old_one(self, spawn):
        port = _free_tcp_port()
        plugin, pidfile = spawn(
            line=_line(address=f"127.0.0.1:{port}"), serve_port=port, controller="graceful"
        )
        supervisor = _live_supervisor(max_restarts=2, registries={})
        supervisor.add(PluginSpec(name="p", process_model=ProcessModel.CHILD, command=plugin.command))

        try:
            assert await _bounded(supervisor.start("p"), "start", timeout=LIVE_TIMEOUT) is True
            first_pid = await _read_pid(pidfile)
            assert _process_is_alive(first_pid)

            # Kill it the way a crash does: no controller RPC, no warning.
            first_connection = supervisor.connection("p")
            await _force_stop(first_connection._proc)
            await _assert_stopped(first_pid)

            await _bounded(supervisor.supervise_once(), "supervise", timeout=LIVE_TIMEOUT)

            assert supervisor.connection("p") is not first_connection
            second_pid = await _read_pid(pidfile)
            assert second_pid != first_pid, "a restart must launch a new process, not reuse a dead one"
            assert _process_is_alive(second_pid)
            assert supervisor.available() == ["p"]
        finally:
            await _release(supervisor.connection("p"))

    @pytest.mark.asyncio
    async def test_the_give_up_path_releases_the_channel(self, spawn):
        port = _free_tcp_port()
        plugin, pidfile = spawn(line=_line(address=f"127.0.0.1:{port}"), serve_port=port)
        decoders = _registry("Decoder")
        supervisor = _live_supervisor(max_restarts=0, registries={"decoder": decoders})
        supervisor.add(
            PluginSpec(
                name="p",
                process_model=ProcessModel.CHILD,
                command=plugin.command,
                subsystems=("decoder",),
            )
        )

        try:
            assert await _bounded(supervisor.start("p"), "start", timeout=LIVE_TIMEOUT) is True
            pid = await _read_pid(pidfile)
            connection = supervisor.connection("p")
            await _force_stop(connection._proc)
            await _assert_stopped(pid)

            await _bounded(supervisor.supervise_once(), "supervise", timeout=LIVE_TIMEOUT)

            assert connection.channel is None, "the give-up path must close the channel it holds"
            assert decoders.list_grpc() == {}, "dispatch must not reach a plugin the supervisor retired"
            assert PluginAuditEvent.GAVE_UP in _events(supervisor)
        finally:
            await _release(supervisor.connection("p"))

    @pytest.mark.asyncio
    async def test_shutdown_all_stops_every_live_child(self, spawn):
        commands = []
        pidfiles = []
        for _ in range(2):
            port = _free_tcp_port()
            plugin, pidfile = spawn(
                line=_line(address=f"127.0.0.1:{port}"), serve_port=port, controller="graceful"
            )
            commands.append(plugin.command)
            pidfiles.append(pidfile)

        supervisor = _live_supervisor(registries={})
        for index, command in enumerate(commands):
            supervisor.add(PluginSpec(name=f"p{index}", process_model=ProcessModel.CHILD, command=command))

        try:
            results = await _bounded(supervisor.start_all(), "start_all", timeout=LIVE_TIMEOUT)
            assert all(results.values()), f"both fake plugins must come up healthy: {results}"
            pids = [await _read_pid(pidfile) for pidfile in pidfiles]

            await _bounded(supervisor.shutdown_all(), "shutdown_all", timeout=LIVE_TIMEOUT)

            for pid in pids:
                await _assert_stopped(pid)
            for name in supervisor.names():
                assert supervisor.connection(name).channel is None
        finally:
            for name in supervisor.names():
                await _release(supervisor.connection(name))
