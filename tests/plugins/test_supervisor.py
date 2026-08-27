"""Phase 3 gate: supervision is mode-aware.

The load-bearing behaviour: restart-on-crash applies only where this host owns
the lifecycle. Restarting an externally-orchestrated plugin would fight k8s.
"""

import sys
from pathlib import Path

import pytest

from tsigma.plugins.connection import ProcessModel
from tsigma.plugins.protocol import PluginProcess
from tsigma.plugins.supervisor import PluginSpec, PluginSpecError, PluginSupervisor

FAKE_PLUGIN = str(Path(__file__).parent / "fake_plugin.py")


def _command(*args: str) -> list[str]:
    return [sys.executable, FAKE_PLUGIN, *args]


def _child(name="child", *args):
    return PluginSpec(name=name, process_model=ProcessModel.CHILD, command=_command(*args))


class TestSpecValidation:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"name": "a", "process_model": ProcessModel.CHILD},
            {"name": "b", "process_model": ProcessModel.CRON},
            {"name": "c", "process_model": ProcessModel.EXTERNAL},
        ],
        ids=["child-no-command", "cron-no-command", "external-no-handshake"],
    )
    def test_rejects_incomplete_specs(self, kwargs):
        with pytest.raises(PluginSpecError):
            PluginSpec(**kwargs)

    def test_rejects_duplicate_names(self):
        sup = PluginSupervisor()
        sup.add(_child())
        with pytest.raises(PluginSpecError, match="already registered"):
            sup.add(_child())


class TestStart:
    @pytest.mark.asyncio
    async def test_starts_and_marks_available(self):
        sup = PluginSupervisor()
        sup.add(_child())
        try:
            assert await sup.start("child") is True
            assert sup.available() == ["child"]
        finally:
            await sup.shutdown_all()

    @pytest.mark.asyncio
    async def test_unhealthy_plugin_starts_but_is_unavailable(self):
        sup = PluginSupervisor()
        sup.add(_child("sick", "--unhealthy"))
        try:
            assert await sup.start("sick") is False
            assert sup.available() == []
            assert "unhealthy" in sup.state("sick").last_error
        finally:
            await sup.shutdown_all()

    @pytest.mark.asyncio
    async def test_bad_handshake_is_recorded_not_raised(self):
        sup = PluginSupervisor()
        sup.add(_child("broken", "--bad-line"))
        assert await sup.start("broken") is False
        assert sup.state("broken").last_error
        await sup.shutdown_all()

    @pytest.mark.asyncio
    async def test_start_all_reports_per_plugin(self):
        sup = PluginSupervisor()
        sup.add(_child("good"))
        sup.add(_child("sick", "--unhealthy"))
        try:
            assert await sup.start_all() == {"good": True, "sick": False}
        finally:
            await sup.shutdown_all()


class TestSupervision:
    @pytest.mark.asyncio
    async def test_healthy_plugin_is_left_alone(self):
        sup = PluginSupervisor()
        sup.add(_child())
        try:
            await sup.start("child")
            pid = sup.connection("child")._proc.process.pid
            assert await sup.supervise_once() == {"child": True}
            assert sup.connection("child")._proc.process.pid == pid
            assert sup.state("child").restarts == 0
        finally:
            await sup.shutdown_all()

    @pytest.mark.asyncio
    async def test_crashed_child_is_restarted(self):
        sup = PluginSupervisor()
        sup.add(_child())
        try:
            await sup.start("child")
            original = sup.connection("child")._proc.process
            original.kill()
            await original.wait()

            assert await sup.supervise_once() == {"child": True}
            assert sup.state("child").restarts == 1
            assert sup.connection("child")._proc.process.pid != original.pid
            assert await sup.connection("child").is_healthy() is True
        finally:
            await sup.shutdown_all()

    @pytest.mark.asyncio
    async def test_restarts_are_capped(self):
        sup = PluginSupervisor(max_restarts=2)
        sup.add(_child("flaky", "--unhealthy"))
        try:
            await sup.start("flaky")
            for _ in range(4):
                await sup.supervise_once()
            assert sup.state("flaky").restarts == 2
            assert "gave up" in sup.state("flaky").last_error
        finally:
            await sup.shutdown_all()

    @pytest.mark.asyncio
    async def test_external_plugin_is_never_restarted(self):
        """The whole point of the mode split: k8s owns it, not us."""
        running = PluginProcess("ext", _command())
        advertised = await running.launch()
        sup = PluginSupervisor()
        sup.add(PluginSpec("ext", ProcessModel.EXTERNAL, handshake=advertised))
        try:
            assert await sup.start("ext") is True

            # Kill it as if the pod died; the orchestrator would replace it.
            running.process.kill()
            await running.process.wait()

            assert await sup.supervise_once() == {"ext": False}
            assert sup.state("ext").restarts == 0
            assert "orchestrator" in sup.state("ext").last_error
        finally:
            await sup.shutdown_all()
            await running.shutdown()

    @pytest.mark.asyncio
    async def test_external_plugin_recovers_when_orchestrator_replaces_it(self):
        running = PluginProcess("ext", _command())
        advertised = await running.launch()
        sup = PluginSupervisor()
        sup.add(PluginSpec("ext", ProcessModel.EXTERNAL, handshake=advertised))
        try:
            await sup.start("ext")
            assert await sup.supervise_once() == {"ext": True}
            assert sup.state("ext").restarts == 0
        finally:
            await sup.shutdown_all()
            await running.shutdown()


class TestShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_all_clears_availability(self):
        sup = PluginSupervisor()
        sup.add(_child("a"))
        sup.add(_child("b"))
        await sup.start_all()
        await sup.shutdown_all()
        assert sup.available() == []

    @pytest.mark.asyncio
    async def test_shutdown_all_is_safe_when_nothing_started(self):
        sup = PluginSupervisor()
        sup.add(_child())
        await sup.shutdown_all()
