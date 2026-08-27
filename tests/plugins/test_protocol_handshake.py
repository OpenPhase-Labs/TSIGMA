"""Phase 2 gate: handshake parsing, launch, health, and graceful shutdown.

The lifecycle tests drive a REAL subprocess (tests/plugins/fake_plugin.py) over a
real gRPC connection - nothing here is mocked, so a break in the shim shows up as
a failing test rather than at first contact with a real plugin.
"""

import asyncio
import sys
from pathlib import Path

import pytest

from tsigma.plugins import constants
from tsigma.plugins.protocol import (
    HandshakeConfig,
    HandshakeError,
    PluginProcess,
    format_handshake_line,
    parse_handshake_line,
    plugin_env,
)

FAKE_PLUGIN = str(Path(__file__).parent / "fake_plugin.py")


def _command(*args: str) -> list[str]:
    return [sys.executable, FAKE_PLUGIN, *args]


class TestParseHandshakeLine:
    def test_parses_a_conforming_line(self):
        c = parse_handshake_line("1|1|tcp|127.0.0.1:5001|grpc")
        assert (c.core_version, c.app_version) == (1, 1)
        assert (c.network, c.address, c.protocol) == ("tcp", "127.0.0.1:5001", "grpc")

    def test_tolerates_surrounding_whitespace(self):
        assert parse_handshake_line("  1|1|tcp|127.0.0.1:1|grpc \n").address == "127.0.0.1:1"

    def test_tcp_target(self):
        assert parse_handshake_line("1|1|tcp|127.0.0.1:5001|grpc").target == "127.0.0.1:5001"

    def test_unix_target_gets_scheme(self):
        assert parse_handshake_line("1|1|unix|/tmp/p.sock|grpc").target == "unix:/tmp/p.sock"

    def test_round_trips(self):
        line = "1|1|tcp|127.0.0.1:5001|grpc"
        assert format_handshake_line(parse_handshake_line(line)) == line

    @pytest.mark.parametrize(
        "line,reason",
        [
            ("", "empty"),
            ("   ", "empty"),
            ("1|1|tcp|grpc", "too few fields"),
            ("1|1|tcp|a:1|grpc|extra", "too many fields"),
            ("a|1|tcp|a:1|grpc", "non-integer core"),
            ("1|b|tcp|a:1|grpc", "non-integer app"),
            ("2|1|tcp|a:1|grpc", "unsupported core version"),
            ("1|9|tcp|a:1|grpc", "unsupported app version"),
            ("1|1|http|a:1|grpc", "bad network"),
            ("1|1|tcp||grpc", "empty address"),
            ("1|1|tcp|a:1|netrpc", "bad protocol"),
        ],
    )
    def test_rejects_malformed(self, line, reason):
        with pytest.raises(HandshakeError):
            parse_handshake_line(line)


class TestPluginEnv:
    def test_sets_the_magic_cookie(self):
        env = plugin_env({"PATH": "/usr/bin"})
        assert env[constants.MAGIC_COOKIE_KEY] == constants.MAGIC_COOKIE_VALUE
        assert env["PATH"] == "/usr/bin"

    def test_does_not_mutate_the_caller_dict(self):
        base = {"PATH": "/usr/bin"}
        plugin_env(base)
        assert constants.MAGIC_COOKIE_KEY not in base


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_launch_handshake_health_shutdown(self):
        p = PluginProcess("fake", _command())
        handshake = await p.launch()
        try:
            assert isinstance(handshake, HandshakeConfig)
            assert handshake.network == "tcp"
            assert handshake.address.startswith("127.0.0.1:")
            assert await p.is_healthy() is True
        finally:
            await p.shutdown()
        assert p.process is None
        assert p.channel is None

    @pytest.mark.asyncio
    async def test_unhealthy_plugin_reports_unhealthy(self):
        p = PluginProcess("fake", _command("--unhealthy"))
        await p.launch()
        try:
            assert await p.is_healthy() is False
        finally:
            await p.shutdown()

    @pytest.mark.asyncio
    async def test_malformed_handshake_raises_and_kills(self):
        p = PluginProcess("fake", _command("--bad-line"))
        with pytest.raises(HandshakeError):
            await p.launch()
        assert p.process is None or p.process.returncode is not None

    @pytest.mark.asyncio
    async def test_missing_handshake_times_out(self, monkeypatch):
        monkeypatch.setattr("tsigma.plugins.protocol.HANDSHAKE_TIMEOUT_SECONDS", 1.0)
        p = PluginProcess("fake", _command("--no-line"))
        with pytest.raises(HandshakeError, match="no handshake line"):
            await p.launch()

    @pytest.mark.asyncio
    async def test_process_that_exits_before_handshake_raises(self):
        p = PluginProcess("dead", [sys.executable, "-c", "import sys; sys.exit(3)"])
        with pytest.raises(HandshakeError, match="exited before handshake"):
            await p.launch()

    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(self):
        p = PluginProcess("fake", _command())
        await p.launch()
        await p.shutdown()
        await p.shutdown()

    @pytest.mark.asyncio
    async def test_health_is_false_before_launch(self):
        assert await PluginProcess("fake", _command()).is_healthy() is False

    @pytest.mark.asyncio
    async def test_shutdown_actually_stops_the_process(self):
        p = PluginProcess("fake", _command())
        await p.launch()
        proc = p.process
        await p.shutdown()
        await asyncio.wait_for(proc.wait(), timeout=5)
        assert proc.returncode is not None
