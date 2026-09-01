"""Shared fake-plugin plumbing for the plugin tests.

``fake_plugin.py`` is the subprocess; this is what the host-side tests need in
order to build its argv and to guarantee nothing it started outlives a test.
It lives in a non-test module (the ``_contract.py`` pattern) so every phase
reaps spawned plugins the same way instead of keeping its own copy of the
teardown.
"""

import contextlib
import os
import signal
import sys
from pathlib import Path

from tsigma.plugins import constants

FAKE_PLUGIN = Path(__file__).resolve().parent / "fake_plugin.py"

# SIGKILL everywhere it exists; the fallback keeps the teardown importable.
KILL_SIGNAL = getattr(signal, "SIGKILL", signal.SIGTERM)

# What the fixtures name their pidfiles, and therefore what the reaper collects.
PIDFILE_GLOB = "plugin-*.pid"


def plugin_command(
    pidfile: Path,
    *,
    line: str | None = None,
    raw_bytes: int = 0,
    bulk_bytes: int = 0,
    serve_port: int = 0,
    controller: str = "none",
    exit_after_handshake: bool = False,
) -> list[str]:
    command = [
        sys.executable,
        str(FAKE_PLUGIN),
        "--pidfile",
        str(pidfile),
        "--cookie-key",
        constants.MAGIC_COOKIE_KEY,
        "--cookie-value",
        constants.MAGIC_COOKIE_VALUE,
    ]
    if line is not None:
        command += ["--emit-line", line]
    if raw_bytes:
        command += ["--emit-raw-bytes", str(raw_bytes)]
    if bulk_bytes:
        command += ["--bulk-bytes", str(bulk_bytes)]
    if serve_port:
        command += ["--serve-port", str(serve_port)]
    if controller != "none":
        command += ["--controller", controller]
    if exit_after_handshake:
        command += ["--exit-after-handshake"]
    return command


def reap_spawned(tmp_path: Path) -> None:
    """Kill every fake plugin started under `tmp_path`, however the test ended."""
    for pidfile in sorted(tmp_path.glob(PIDFILE_GLOB)):
        try:
            pid = int(pidfile.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):  # pragma: no cover - nothing was spawned
            continue
        with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
            os.kill(pid, KILL_SIGNAL)
