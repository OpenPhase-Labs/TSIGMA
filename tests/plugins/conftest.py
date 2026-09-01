"""Fixtures shared by the plugin tests."""

import itertools
from pathlib import Path

import pytest

from tsigma.plugins import protocol

from tests.plugins._spawn import plugin_command, reap_spawned


@pytest.fixture
def spawn(tmp_path):
    """Build PluginProcess handles; guarantee no fake plugin outlives the test.

    `PluginProcess.__init__` starts nothing - it only holds the argv until
    `launch()` is awaited - so a test that wants the argv the fake plugin would
    be started with reads `.command` off the handle and is still covered by the
    reaping teardown here.
    """
    counter = itertools.count()

    def _make(**kwargs) -> tuple[protocol.PluginProcess, Path]:
        pidfile = tmp_path / f"plugin-{next(counter)}.pid"
        command = plugin_command(pidfile, **kwargs)
        return protocol.PluginProcess(f"fake-{pidfile.stem}", command), pidfile

    yield _make

    reap_spawned(tmp_path)
