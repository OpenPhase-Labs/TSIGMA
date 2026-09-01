"""Phase R3b gate: mode-aware supervision, a windowed restart budget, an audit trail.

`PluginSupervisor` is what turns three connection modes into one operable fleet, so
the things gated here are the ones an operator finds out about at 3am:

  - Supervision is MODE-AWARE, and the mode is the connection's, not a flag the
    supervisor keeps. Mode 1 is restarted when it crashes. Mode 2 is reconnected
    and observed - the orchestrator owns it, and restarting it here would fight
    systemd or k8s. Mode 3 runs per invocation: a cron plugin resting between runs
    reports `idle`, and an idle plugin is never relaunched off-schedule and never
    charged against the restart budget. `PluginConnection.idle` (R3a) is how that
    is known; a `process_model is CRON` branch in the supervisor would be the same
    knowledge written twice.
  - The restart budget is WINDOWED. A lifetime counter retires a plugin that
    crashed three times in three years, which is a plugin that works. Only the
    restarts inside the window count, so sustained health resets the budget by
    letting the old entries fall out of it.
  - A restart REPLACES the connection object, and every registry entry pointing at
    the old one is re-pointed or unregistered. This is where a supervisor leaks:
    the old connection is shut down, the registry still holds it, and dispatch
    keeps arriving at a closed channel. Proved through every accessor the
    coexistence mixin publishes, not just the one the implementation happens to
    use, and by object identity so an entry registered under some other name is
    caught too.
  - Lifecycle events are RECORDED: launch, handshake failure, health transitions,
    restart, give-up, shutdown. A malformed handshake in particular is surfaced
    twice - logged at error and written to the audit table - rather than parked in
    a `last_error` field with no reader.

Health transitions are transitions: a plugin that is unhealthy on ten consecutive
passes produces one record, not ten, or the table becomes a poll log.

The scripted cases drive a seam-conforming fake connection, which is checked
against R3a's conformance helper here so the fake cannot drift into something no
real mode could be. Two neighbours carry the rest of this phase's gate:
test_plugin_audit_table.py (the audit row and its migration) and
test_supervisor_live.py (release proved against real subprocesses).
"""

import asyncio
import logging

import pytest

from tsigma.plugins import constants, protocol
from tsigma.plugins.audit import PluginAuditEvent
from tsigma.plugins.coexistence import GrpcCoexistenceMixin, Origin
from tsigma.plugins.connection import (
    DiscoveredConnection,
    LaunchedConnection,
    ProcessModel,
    ScheduledConnection,
)
from tsigma.plugins.supervisor import (
    DEFAULT_RESTART_WINDOW_SECONDS,
    PluginSpec,
    PluginSpecError,
    PluginSupervisor,
    default_registries,
)

from tests.plugins._supervisor_fakes import (
    MANIFEST_HANDSHAKE,
    _Clock,
    _events,
    _FakeConnection,
    _records_for,
    _registry,
    _spec,
    _supervisor,
)
from tests.plugins.test_connection_modes import assert_seam_conformance
from tsigma.collection.decoders.base import DecoderRegistry
from tsigma.collection.registry import IngestionMethodRegistry
from tsigma.notifications.registry import NotificationRegistry
from tsigma.reports.registry import ReportRegistry


# ------------------------------------------------------------------- fake integrity
class TestTheFakeIsALegitimateStandIn:
    """A fake that no real mode could be would make every scripted test below vacuous."""

    def test_the_fake_connection_conforms_to_the_seam(self):
        assert_seam_conformance(_FakeConnection)

    def test_the_fake_reports_lifecycle_ownership_the_way_the_real_modes_do(self):
        assert _FakeConnection("a", ProcessModel.CHILD).host_owns_lifecycle is True
        assert _FakeConnection("a", ProcessModel.CRON).host_owns_lifecycle is True
        assert _FakeConnection("a", ProcessModel.EXTERNAL).host_owns_lifecycle is False

    def test_a_rebuild_past_the_end_of_the_script_is_still_the_broken_plugin(self):
        # The scripted spec falls back to relaunching the plugin as it last was.
        # A fallback that came up healthy would end every crash loop after one
        # restart and quietly make the windowed-budget cases below vacuous.
        spec = _spec("p", ProcessModel.CHILD, [_FakeConnection("p", healthy=False)])
        first = spec.build()
        second = spec.build()
        assert second is not first
        assert second.healthy is False
        assert second.process_model is first.process_model


# ------------------------------------------------------- the manifest's declaration
class TestManifestProcessModelIsHonored:
    """A manifest declares the deployment shape; the supervisor does not second-guess it."""

    @pytest.mark.parametrize(
        "model,expected",
        [
            (ProcessModel.CHILD, LaunchedConnection),
            (ProcessModel.CRON, ScheduledConnection),
            (ProcessModel.EXTERNAL, DiscoveredConnection),
        ],
        ids=["child", "cron", "external"],
    )
    def test_each_declared_model_yields_that_modes_connection(self, model, expected):
        kwargs = (
            {"handshake": MANIFEST_HANDSHAKE}
            if model is ProcessModel.EXTERNAL
            else {"command": ["fake-plugin"]}
        )
        supervisor = _supervisor()
        supervisor.add(PluginSpec(name="p", process_model=model, **kwargs))
        connection = supervisor.connection("p")
        assert isinstance(connection, expected)
        assert connection.process_model is model

    def test_a_declaration_its_model_cannot_satisfy_is_refused_not_defaulted(self):
        # Silently defaulting a child plugin with no command to "external", or an
        # external one with no handshake to "child", is how a manifest typo turns
        # into a plugin the host quietly never runs.
        with pytest.raises(PluginSpecError):
            PluginSpec(name="p", process_model=ProcessModel.CHILD)
        with pytest.raises(PluginSpecError):
            PluginSpec(name="p", process_model=ProcessModel.CRON)
        with pytest.raises(PluginSpecError):
            PluginSpec(name="p", process_model=ProcessModel.EXTERNAL)

    def test_the_declared_model_survives_a_restart(self):
        supervisor = _supervisor()
        supervisor.add(PluginSpec(name="p", process_model=ProcessModel.CRON, command=["fake-plugin"]))
        rebuilt = supervisor.state("p").spec.build()
        assert rebuilt.process_model is ProcessModel.CRON

    def test_the_audit_record_names_the_model_the_manifest_declared(self):
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.CRON, [_FakeConnection("p", ProcessModel.CRON)]))
        asyncio.run(supervisor.start("p"))
        launches = _records_for(supervisor, PluginAuditEvent.LAUNCH)
        assert [r.process_model for r in launches] == [ProcessModel.CRON]


# ------------------------------------------------------------- mode-aware supervision
class TestModeOneIsRestarted:
    """Mode 1 is this host's to keep alive."""

    @pytest.mark.asyncio
    async def test_a_crashed_child_is_replaced_by_a_freshly_built_connection(self):
        dead = _FakeConnection("p", healthy=False)
        fresh = _FakeConnection("p", healthy=True)
        supervisor = _supervisor()
        spec = _spec("p", ProcessModel.CHILD, [dead, fresh])
        supervisor.add(spec)
        await supervisor.start("p")

        assert await supervisor.supervise_once() == {"p": True}
        assert supervisor.connection("p") is fresh, "a restart must swap in the rebuilt connection"
        assert spec.built == [dead, fresh]
        assert dead.shutdowns == 1, "the crashed child must be released before the replacement starts"
        assert fresh.connects == 1
        assert supervisor.available() == ["p"]

    @pytest.mark.asyncio
    async def test_a_healthy_child_is_left_alone(self):
        connection = _FakeConnection("p")
        supervisor = _supervisor()
        spec = _spec("p", ProcessModel.CHILD, [connection])
        supervisor.add(spec)
        await supervisor.start("p")
        connects_after_start = connection.connects

        for _ in range(3):
            assert await supervisor.supervise_once() == {"p": True}

        assert spec.built == [connection], "a healthy plugin must never be rebuilt"
        assert connection.shutdowns == 0
        assert connection.connects == connects_after_start
        assert supervisor.state("p").restart_times == []


class TestModeTwoIsReconnectedNotRestarted:
    """Mode 2 belongs to systemd or k8s; this host dials and watches."""

    @pytest.mark.asyncio
    async def test_an_unhealthy_external_plugin_is_never_rebuilt(self):
        connection = _FakeConnection("p", ProcessModel.EXTERNAL, healthy=False)
        supervisor = _supervisor()
        spec = _spec("p", ProcessModel.EXTERNAL, [connection])
        supervisor.add(spec)
        await supervisor.start("p")

        for _ in range(4):
            assert await supervisor.supervise_once() == {"p": False}

        assert spec.built == [connection], "restarting an orchestrated plugin fights its orchestrator"
        assert supervisor.connection("p") is connection

    @pytest.mark.asyncio
    async def test_an_unhealthy_external_plugin_is_redialled(self):
        # A channel to a pod that was rescheduled is stale forever: `connect` on a
        # connection that still holds one is a no-op, so observing without ever
        # re-dialling means the host never notices the replacement come back.
        connection = _FakeConnection("p", ProcessModel.EXTERNAL, healthy=False)
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.EXTERNAL, [connection]))
        await supervisor.start("p")
        connects_after_start = connection.connects

        await supervisor.supervise_once()
        assert connection.connects > connects_after_start, (
            "mode 2 must re-dial an unhealthy external plugin rather than sit on a stale channel"
        )

    @pytest.mark.asyncio
    async def test_an_external_plugin_is_never_charged_against_the_restart_budget(self):
        connection = _FakeConnection("p", ProcessModel.EXTERNAL, healthy=False)
        supervisor = _supervisor(max_restarts=1)
        supervisor.add(_spec("p", ProcessModel.EXTERNAL, [connection]))
        await supervisor.start("p")

        for _ in range(5):
            await supervisor.supervise_once()

        assert supervisor.state("p").restart_times == []
        assert PluginAuditEvent.GAVE_UP not in _events(supervisor), (
            "there is nothing to give up on: the orchestrator is still trying"
        )

    @pytest.mark.asyncio
    async def test_an_external_plugin_that_comes_back_is_marked_available_again(self):
        connection = _FakeConnection("p", ProcessModel.EXTERNAL, healthy=False)
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.EXTERNAL, [connection]))
        await supervisor.start("p")
        await supervisor.supervise_once()
        assert supervisor.available() == []

        connection.healthy = True
        assert await supervisor.supervise_once() == {"p": True}
        assert supervisor.available() == ["p"]


class TestModeThreeRunsPerInvocation:
    """Idle is a state, not a failure. The schedule starts a cron plugin, not the supervisor."""

    @pytest.mark.asyncio
    async def test_a_start_that_finds_a_finished_run_releases_its_channel(self):
        # connect() opens a channel before the run can end, so the start that
        # observes an already-finished run is the one that has to release it.
        # Without this, every scheduled invocation leaks one.
        rested = _FakeConnection("p", ProcessModel.CRON, healthy=False, idle=True)
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.CRON, [rested]))

        assert await supervisor.start("p") is False
        assert rested.channel is None, "a finished cron run must not keep its channel"

    @pytest.mark.asyncio
    async def test_an_idle_cron_plugin_is_never_relaunched(self):
        rested = _FakeConnection("p", ProcessModel.CRON, healthy=False, idle=True)
        supervisor = _supervisor()
        spec = _spec("p", ProcessModel.CRON, [rested])
        supervisor.add(spec)
        await supervisor.start("p")
        connects_after_start = rested.connects

        for _ in range(4):
            await supervisor.supervise_once()

        assert spec.built == [rested], "an idle cron plugin must not be rebuilt off-schedule"
        assert rested.connects == connects_after_start, "the schedule starts the next run, not the supervisor"

    @pytest.mark.asyncio
    async def test_an_idle_cron_plugin_is_never_charged_against_the_restart_budget(self):
        rested = _FakeConnection("p", ProcessModel.CRON, healthy=False, idle=True)
        supervisor = _supervisor(max_restarts=1)
        supervisor.add(_spec("p", ProcessModel.CRON, [rested]))
        await supervisor.start("p")

        for _ in range(6):
            await supervisor.supervise_once()

        assert supervisor.state("p").restart_times == []
        assert PluginAuditEvent.GAVE_UP not in _events(supervisor), (
            "a cron plugin that rested six times has not failed once"
        )
        assert PluginAuditEvent.RESTART not in _events(supervisor)

    @pytest.mark.asyncio
    async def test_an_idle_cron_plugin_is_not_shut_down_by_supervision(self):
        rested = _FakeConnection("p", ProcessModel.CRON, healthy=False, idle=True)
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.CRON, [rested]))
        await supervisor.start("p")
        # Measured across supervision only. start() releases the channel the
        # finished run left open, which is a different property, tested live in
        # test_a_finished_cron_run_releases_the_channel_it_opened.
        before = rested.shutdowns
        await supervisor.supervise_once()
        assert rested.shutdowns == before, "the schedule owns the next run, not the supervisor"

    @pytest.mark.asyncio
    async def test_a_cron_run_that_died_mid_flight_is_restarted(self):
        # Not idle: the run is still live and has stopped serving. That is a crash,
        # and this host owns a cron plugin's lifecycle.
        crashed = _FakeConnection("p", ProcessModel.CRON, healthy=False, idle=False)
        fresh = _FakeConnection("p", ProcessModel.CRON, healthy=True)
        supervisor = _supervisor()
        spec = _spec("p", ProcessModel.CRON, [crashed, fresh])
        supervisor.add(spec)
        await supervisor.start("p")

        await supervisor.supervise_once()
        assert spec.built == [crashed, fresh]
        assert crashed.shutdowns == 1
        assert supervisor.connection("p") is fresh

    @pytest.mark.asyncio
    async def test_idleness_is_read_from_the_seam_not_from_the_process_model(self):
        # A supervisor that branches on `process_model is CRON` writes down what
        # the connection already knows, and then gets it wrong for the one case
        # that matters: a cron run that crashed while it was working.
        idle_child = _FakeConnection("p", ProcessModel.CHILD, healthy=False, idle=True)
        supervisor = _supervisor()
        spec = _spec("p", ProcessModel.CHILD, [idle_child])
        supervisor.add(spec)
        await supervisor.start("p")

        await supervisor.supervise_once()
        assert spec.built == [idle_child], (
            "the supervisor must consult `connection.idle`, whatever mode the connection declares"
        )


# ------------------------------------------------------------ windowed restart budget
class TestWindowedRestartBudget:
    """A lifetime counter retires a plugin that works. Only the window counts."""

    def test_the_window_is_a_declared_default(self):
        assert isinstance(DEFAULT_RESTART_WINDOW_SECONDS, (int, float))
        assert DEFAULT_RESTART_WINDOW_SECONDS > 0

    @pytest.mark.asyncio
    async def test_restarts_are_recorded_as_timestamps_not_a_bare_counter(self):
        clock = _Clock()
        supervisor = _supervisor(max_restarts=5, clock=clock)
        supervisor.add(_spec("p", ProcessModel.CHILD, [_FakeConnection("p", healthy=False)]))
        await supervisor.start("p")

        await supervisor.supervise_once()
        stamps = supervisor.state("p").restart_times
        assert stamps and all(isinstance(t, float) for t in stamps), (
            "a windowed budget needs the times, not a count"
        )
        assert stamps[-1] == clock.now

    @pytest.mark.asyncio
    async def test_the_budget_is_exhausted_inside_the_window(self):
        clock = _Clock()
        supervisor = _supervisor(max_restarts=2, restart_window_seconds=600.0, clock=clock)
        supervisor.add(_spec("p", ProcessModel.CHILD, [_FakeConnection("p", healthy=False)]))
        await supervisor.start("p")

        for _ in range(2):
            clock.advance(10.0)
            await supervisor.supervise_once()
        assert len(supervisor.state("p").restart_times) == 2

        clock.advance(10.0)
        assert await supervisor.supervise_once() == {"p": False}
        assert PluginAuditEvent.GAVE_UP in _events(supervisor)

    @pytest.mark.asyncio
    async def test_a_plugin_that_crashes_occasionally_over_a_long_uptime_is_never_retired(self):
        # The defect a lifetime counter has: three crashes spread over three years
        # is a plugin that works, and a monotonic counter retires it forever.
        clock = _Clock()
        window = 600.0
        supervisor = _supervisor(max_restarts=2, restart_window_seconds=window, clock=clock)
        supervisor.add(_spec("p", ProcessModel.CHILD, [_FakeConnection("p", healthy=False)]))
        await supervisor.start("p")

        for _ in range(10):
            clock.advance(window * 5)
            assert await supervisor.supervise_once() == {"p": False}, (
                "the rebuilt connection is unhealthy too; what matters is that it was rebuilt"
            )
            assert len(supervisor.state("p").restart_times) == 1, (
                "restarts older than the window must fall out of the budget"
            )

        assert PluginAuditEvent.GAVE_UP not in _events(supervisor)
        assert len(_records_for(supervisor, PluginAuditEvent.RESTART)) == 10

    @pytest.mark.asyncio
    async def test_sustained_health_restores_the_full_budget(self):
        clock = _Clock()
        window = 600.0
        unhealthy = _FakeConnection("p", healthy=False)
        supervisor = _supervisor(max_restarts=2, restart_window_seconds=window, clock=clock)
        spec = _spec("p", ProcessModel.CHILD, [unhealthy])
        supervisor.add(spec)
        await supervisor.start("p")

        clock.advance(1.0)
        await supervisor.supervise_once()
        clock.advance(1.0)
        await supervisor.supervise_once()
        assert len(supervisor.state("p").restart_times) == 2

        # The last rebuild comes up healthy and stays healthy past the window.
        supervisor.connection("p").healthy = True
        for _ in range(3):
            clock.advance(window / 2)
            assert await supervisor.supervise_once() == {"p": True}
        assert supervisor.state("p").restart_times == [], "sustained health must clear the budget"

        # And the budget is genuinely spendable again, not merely emptied.
        supervisor.connection("p").healthy = False
        clock.advance(1.0)
        await supervisor.supervise_once()
        assert PluginAuditEvent.GAVE_UP not in _events(supervisor)
        assert len(supervisor.state("p").restart_times) == 1

    @pytest.mark.asyncio
    async def test_giving_up_releases_the_connection(self):
        connection = _FakeConnection("p", healthy=False)
        supervisor = _supervisor(max_restarts=0)
        supervisor.add(_spec("p", ProcessModel.CHILD, [connection]))
        await supervisor.start("p")

        assert await supervisor.supervise_once() == {"p": False}
        assert connection.shutdowns >= 1, "the give-up path must not leave the plugin running"
        assert connection.channel is None
        assert supervisor.available() == []

    @pytest.mark.asyncio
    async def test_a_retired_plugin_is_not_rebuilt_on_every_later_pass(self):
        supervisor = _supervisor(max_restarts=0)
        spec = _spec("p", ProcessModel.CHILD, [_FakeConnection("p", healthy=False)])
        supervisor.add(spec)
        await supervisor.start("p")

        for _ in range(4):
            await supervisor.supervise_once()
        assert len(spec.built) == 1, "a plugin the supervisor gave up on must stay given up on"


# --------------------------------------------------------------- registry re-pointing
class TestRegistryRepointing:
    """Dispatch must never reach a connection the supervisor has shut down."""

    @pytest.mark.asyncio
    async def test_a_successful_start_registers_the_plugin_with_its_declared_subsystems(self):
        reports = _registry("Report")
        decoders = _registry("Decoder")
        connection = _FakeConnection("asc3")
        supervisor = _supervisor(registries={"report": reports, "decoder": decoders})
        supervisor.add(_spec("asc3", ProcessModel.CHILD, [connection], subsystems=("decoder",)))

        assert await supervisor.start("asc3") is True
        assert decoders.get_connection("asc3") is connection
        assert decoders.origin("asc3") is Origin.GRPC
        assert reports.list_grpc() == {}, "a plugin serves only the subsystems its manifest names"

    @pytest.mark.asyncio
    async def test_a_start_that_fails_registers_nothing(self):
        decoders = _registry("Decoder")
        supervisor = _supervisor(registries={"decoder": decoders})
        supervisor.add(
            _spec("asc3", ProcessModel.CHILD, [_FakeConnection("asc3", healthy=False)], subsystems=("decoder",))
        )

        assert await supervisor.start("asc3") is False
        assert decoders.list_grpc() == {}
        assert decoders.origin("asc3") is None

    @pytest.mark.asyncio
    async def test_a_restart_repoints_every_registry_entry_to_the_new_connection(self):
        decoders = _registry("Decoder")
        old = _FakeConnection("asc3", healthy=False)
        new = _FakeConnection("asc3", healthy=True)
        supervisor = _supervisor(registries={"decoder": decoders})
        supervisor.add(_spec("asc3", ProcessModel.CHILD, [old, new], subsystems=("decoder",)))
        await supervisor.start("asc3")

        await supervisor.supervise_once()

        assert decoders.get_connection("asc3") is new
        assert decoders.list_grpc() == {"asc3": new}
        assert decoders.is_remote("asc3") is True
        assert decoders.origin("asc3") is Origin.GRPC
        assert decoders.list_names()["asc3"] is Origin.GRPC
        assert old.shutdowns == 1

    @pytest.mark.asyncio
    async def test_no_accessor_still_answers_with_the_shut_down_connection(self):
        decoders = _registry("Decoder")
        old = _FakeConnection("asc3", healthy=False)
        new = _FakeConnection("asc3", healthy=True)
        supervisor = _supervisor(registries={"decoder": decoders})
        supervisor.add(_spec("asc3", ProcessModel.CHILD, [old, new], subsystems=("decoder",)))
        await supervisor.start("asc3")
        await supervisor.supervise_once()

        assert old not in decoders.list_grpc().values()
        assert decoders.get_connection("asc3") is not old

    @pytest.mark.asyncio
    async def test_repointing_follows_the_connection_object_not_only_the_plugin_name(self):
        # A subsystem may serve a name that is not the plugin's own - one plugin
        # can register several decoders. Every entry holding the old connection is
        # stale after a restart, whatever it is keyed under.
        decoders = _registry("Decoder")
        notifiers = _registry("Notify")
        old = _FakeConnection("vendor", healthy=False)
        new = _FakeConnection("vendor", healthy=True)
        supervisor = _supervisor(registries={"decoder": decoders, "notify": notifiers})
        supervisor.add(_spec("vendor", ProcessModel.CHILD, [old, new], subsystems=("decoder",)))
        await supervisor.start("vendor")
        notifiers.register_grpc("vendor-pager", old)

        await supervisor.supervise_once()

        assert decoders.get_connection("vendor") is new
        assert notifiers.get_connection("vendor-pager") is new, (
            "every entry pointing at the old connection is stale, not just the one keyed by plugin name"
        )

    @pytest.mark.asyncio
    async def test_giving_up_unregisters_the_plugin_everywhere(self):
        decoders = _registry("Decoder")
        notifiers = _registry("Notify")
        connection = _FakeConnection("vendor", healthy=False)
        supervisor = _supervisor(max_restarts=0, registries={"decoder": decoders, "notify": notifiers})
        supervisor.add(_spec("vendor", ProcessModel.CHILD, [connection], subsystems=("decoder",)))
        await supervisor.start("vendor")
        notifiers.register_grpc("vendor-pager", connection)

        await supervisor.supervise_once()

        for registry, name in ((decoders, "vendor"), (notifiers, "vendor-pager")):
            assert registry.origin(name) is None
            assert registry.is_remote(name) is False
            assert registry.list_grpc() == {}
            assert name not in registry.list_names()
            with pytest.raises(ValueError):
                registry.get_connection(name)

    @pytest.mark.asyncio
    async def test_repointing_leaves_other_plugins_registrations_alone(self):
        decoders = _registry("Decoder")
        neighbour = _FakeConnection("siemens")
        old = _FakeConnection("asc3", healthy=False)
        new = _FakeConnection("asc3", healthy=True)
        supervisor = _supervisor(registries={"decoder": decoders})
        supervisor.add(_spec("asc3", ProcessModel.CHILD, [old, new], subsystems=("decoder",)))
        await supervisor.start("asc3")
        decoders.register_grpc("siemens", neighbour)

        await supervisor.supervise_once()
        assert decoders.get_connection("siemens") is neighbour

    @pytest.mark.asyncio
    async def test_an_in_process_name_is_left_to_the_in_process_path(self):
        # The coexistence mixin refuses a gRPC registration for a name already
        # served in-process; a supervisor that swallows that refusal would report a
        # start as successful while dispatch still goes to the in-process class.
        decoders = _registry("Decoder", in_process={"asc3"})
        supervisor = _supervisor(registries={"decoder": decoders})
        supervisor.add(_spec("asc3", ProcessModel.CHILD, [_FakeConnection("asc3")], subsystems=("decoder",)))

        assert await supervisor.start("asc3") is False, (
            "a name that resolves in-process cannot also resolve over gRPC"
        )
        assert decoders.origin("asc3") is Origin.IN_PROCESS
        assert supervisor.available() == []
        assert supervisor.state("asc3").last_error

    @pytest.mark.asyncio
    async def test_shutting_the_fleet_down_unregisters_every_plugin(self):
        decoders = _registry("Decoder")
        connection = _FakeConnection("asc3")
        supervisor = _supervisor(registries={"decoder": decoders})
        supervisor.add(_spec("asc3", ProcessModel.CHILD, [connection], subsystems=("decoder",)))
        await supervisor.start("asc3")

        await supervisor.shutdown_all()

        assert decoders.list_grpc() == {}
        assert connection.shutdowns == 1
        assert supervisor.available() == []


class TestDefaultRegistries:
    """The four registries that carry the coexistence mixin, and only those four."""

    def test_the_default_map_is_keyed_by_the_generated_subsystems(self):
        assert set(default_registries()) == set(constants.GENERATED_SUBSYSTEMS)

    def test_the_default_map_names_the_real_registries(self):
        assert default_registries() == {
            "decoder": DecoderRegistry,
            "method": IngestionMethodRegistry,
            "notify": NotificationRegistry,
            "report": ReportRegistry,
        }

    def test_every_default_registry_carries_the_coexistence_mixin(self):
        for name, registry in default_registries().items():
            assert issubclass(registry, GrpcCoexistenceMixin), f"{name} cannot hold a gRPC registration"

    def test_a_supervisor_with_no_registries_uses_the_defaults(self):
        assert PluginSupervisor().registries == default_registries()


# ----------------------------------------------------------------------- audit trail
class TestLifecycleIsAudited:
    """Every state change an operator would ask about leaves a row."""

    @pytest.mark.asyncio
    async def test_a_successful_start_records_a_launch(self):
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.CHILD, [_FakeConnection("p")]))
        await supervisor.start("p")

        assert _events(supervisor) == [PluginAuditEvent.LAUNCH]
        assert _records_for(supervisor, PluginAuditEvent.LAUNCH)[0].plugin_name == "p"

    @pytest.mark.asyncio
    async def test_a_malformed_handshake_is_recorded_and_logged_at_error(self, caplog):
        broken = _FakeConnection(
            "p", connect_error=protocol.HandshakeError("p: unsupported plugin wire-protocol version 7")
        )
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.CHILD, [broken]))

        with caplog.at_level(logging.ERROR):
            assert await supervisor.start("p") is False

        failures = _records_for(supervisor, PluginAuditEvent.HANDSHAKE_FAILED)
        assert len(failures) == 1, (
            "a handshake this host refused is an operator event, not a private field on a state object"
        )
        assert "version 7" in failures[0].detail, "the record must carry the diagnostic, not just the fact"
        assert failures[0].plugin_name == "p"

        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors, "a refused handshake must also reach the log an operator is already watching"
        assert any("p" in r.getMessage() for r in errors)

    @pytest.mark.asyncio
    async def test_a_refused_handshake_records_no_launch(self):
        supervisor = _supervisor()
        supervisor.add(
            _spec("p", ProcessModel.CHILD, [_FakeConnection("p", connect_error=protocol.HandshakeError("bad"))])
        )
        await supervisor.start("p")
        assert PluginAuditEvent.LAUNCH not in _events(supervisor)

    @pytest.mark.asyncio
    async def test_health_transitions_are_recorded_in_both_directions(self):
        connection = _FakeConnection("p", ProcessModel.EXTERNAL, healthy=True)
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.EXTERNAL, [connection]))
        await supervisor.start("p")

        connection.healthy = False
        await supervisor.supervise_once()
        connection.healthy = True
        await supervisor.supervise_once()

        assert _events(supervisor) == [
            PluginAuditEvent.LAUNCH,
            PluginAuditEvent.HEALTH_LOST,
            PluginAuditEvent.HEALTH_RESTORED,
        ]

    @pytest.mark.asyncio
    async def test_a_health_transition_is_recorded_once_not_once_per_poll(self):
        # Otherwise the audit table is a poll log and the transition is unfindable.
        connection = _FakeConnection("p", ProcessModel.EXTERNAL, healthy=True)
        supervisor = _supervisor()
        supervisor.add(_spec("p", ProcessModel.EXTERNAL, [connection]))
        await supervisor.start("p")

        connection.healthy = False
        for _ in range(5):
            await supervisor.supervise_once()

        assert len(_records_for(supervisor, PluginAuditEvent.HEALTH_LOST)) == 1

    @pytest.mark.asyncio
    async def test_a_restart_and_a_give_up_are_each_recorded(self):
        supervisor = _supervisor(max_restarts=1)
        supervisor.add(_spec("p", ProcessModel.CHILD, [_FakeConnection("p", healthy=False)]))
        await supervisor.start("p")

        await supervisor.supervise_once()
        await supervisor.supervise_once()

        assert len(_records_for(supervisor, PluginAuditEvent.RESTART)) == 1
        assert len(_records_for(supervisor, PluginAuditEvent.GAVE_UP)) == 1

    @pytest.mark.asyncio
    async def test_shutdown_is_recorded_for_every_plugin(self):
        supervisor = _supervisor()
        supervisor.add(_spec("a", ProcessModel.CHILD, [_FakeConnection("a")]))
        supervisor.add(_spec("b", ProcessModel.EXTERNAL, [_FakeConnection("b", ProcessModel.EXTERNAL)]))
        await supervisor.start_all()

        await supervisor.shutdown_all()

        shutdowns = _records_for(supervisor, PluginAuditEvent.SHUTDOWN)
        assert {r.plugin_name for r in shutdowns} == {"a", "b"}

    @pytest.mark.asyncio
    async def test_an_idle_cron_pass_records_nothing(self):
        supervisor = _supervisor()
        supervisor.add(
            _spec("p", ProcessModel.CRON, [_FakeConnection("p", ProcessModel.CRON, healthy=False, idle=True)])
        )
        await supervisor.start("p")
        before = len(supervisor.audit_sink.records)

        for _ in range(3):
            await supervisor.supervise_once()

        assert len(supervisor.audit_sink.records) == before, (
            "resting is not an event; recording it buries the events that are"
        )

    @pytest.mark.asyncio
    async def test_every_record_names_the_plugin_and_its_process_model(self):
        supervisor = _supervisor(max_restarts=1)
        supervisor.add(_spec("p", ProcessModel.CHILD, [_FakeConnection("p", healthy=False)]))
        await supervisor.start("p")
        await supervisor.supervise_once()
        await supervisor.supervise_once()
        await supervisor.shutdown_all()

        assert supervisor.audit_sink.records
        for record in supervisor.audit_sink.records:
            assert record.plugin_name == "p"
            assert isinstance(record.event_type, PluginAuditEvent)
            assert isinstance(record.process_model, ProcessModel)

    @pytest.mark.asyncio
    async def test_a_failing_audit_sink_does_not_break_supervision(self):
        # An audit trail that cannot be written is a problem; a plugin fleet that
        # stops being supervised because of it is an outage.
        class _BrokenSink:
            async def record(self, event):
                raise RuntimeError("audit database is down")

        supervisor = PluginSupervisor(audit_sink=_BrokenSink(), registries={})
        supervisor.add(_spec("p", ProcessModel.CHILD, [_FakeConnection("p")]))
        assert await supervisor.start("p") is True
        assert supervisor.available() == ["p"]
