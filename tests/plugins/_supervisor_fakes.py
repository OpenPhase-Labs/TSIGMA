"""Stand-ins the supervisor tests drive, in one place.

`tests/plugins/test_supervisor.py` scripts these against fakes and
`tests/plugins/test_supervisor_live.py` reuses the supervisor and registry
helpers against real subprocesses, so they live here rather than once per file.
"""

from dataclasses import dataclass, field

from tsigma.plugins import constants, protocol
from tsigma.plugins.audit import PluginAuditEvent, PluginAuditRecord, RecordingPluginAuditSink
from tsigma.plugins.coexistence import GrpcCoexistenceMixin
from tsigma.plugins.connection import ProcessModel
from tsigma.plugins.supervisor import PluginSpec, PluginSupervisor

CORE = constants.CORE_PROTOCOL_VERSION
APP = constants.PLUGIN_PROTOCOL_VERSION

MANIFEST_HANDSHAKE = protocol.HandshakeConfig(CORE, APP, "tcp", "127.0.0.1:5000", "grpc")

# A scripted fake answers its health check instantly and deterministically, so
# the startup retry budget buys the suite nothing but dead time. The live tests
# keep the real default - a subprocess genuinely needs the grace.
SCRIPTED_STARTUP_HEALTH_SECONDS = 0.0


class _FakeConnection:
    """A seam-conforming connection whose health, idleness and lifecycle a test drives.

    Everything the supervisor is allowed to do to a connection is counted here, so
    a test can assert what was NOT done - never launching an idle cron plugin,
    never stopping an externally orchestrated one - which is half of what
    mode-awareness means.
    """

    process_model = ProcessModel.CHILD

    def __init__(
        self,
        name: str,
        process_model: ProcessModel = ProcessModel.CHILD,
        *,
        healthy: bool = True,
        idle: bool = False,
        connect_error: Exception | None = None,
    ):
        self.name = name
        self.process_model = process_model
        self.handshake: protocol.HandshakeConfig | None = None
        self.channel = None
        self.healthy = healthy
        self.idle_flag = idle
        self.connect_error = connect_error
        self.connects = 0
        self.shutdowns = 0
        self.health_checks = 0

    @property
    def host_owns_lifecycle(self) -> bool:
        return self.process_model is not ProcessModel.EXTERNAL

    @property
    def idle(self) -> bool:
        return self.idle_flag

    async def connect(self) -> protocol.HandshakeConfig:
        self.connects += 1
        if self.connect_error is not None:
            raise self.connect_error
        self.handshake = MANIFEST_HANDSHAKE
        self.channel = f"channel-{self.name}-{self.connects}"
        return self.handshake

    async def is_healthy(self, timeout: float = 2.0) -> bool:
        self.health_checks += 1
        return self.healthy

    async def shutdown(self) -> None:
        self.shutdowns += 1
        self.channel = None

    def next_generation(self) -> "_FakeConnection":
        """The same plugin relaunched, still broken in the same way.

        A rebuild that came up healthy by default would end every crash loop
        after one restart, and the windowed budget - the thing this phase exists
        for - would never be spent.
        """
        return _FakeConnection(
            self.name,
            self.process_model,
            healthy=self.healthy,
            idle=self.idle_flag,
            connect_error=self.connect_error,
        )


@dataclass
class _ScriptedSpec(PluginSpec):
    """A spec whose ``build()`` hands out connections the test prepared.

    The supervisor rebuilds through ``PluginSpec.build()`` on every restart, so
    scripting that one method is all a test needs in order to supply the second
    and third generations of a connection without a subprocess anywhere. Past
    the end of the script the plugin keeps relaunching as it last was, rather
    than spontaneously recovering.
    """

    queue: list = field(default_factory=list)
    built: list = field(default_factory=list)

    def build(self):
        if self.queue:
            connection = self.queue.pop(0)
        elif self.built:
            connection = self.built[-1].next_generation()
        else:
            connection = _FakeConnection(self.name, self.process_model)
        self.built.append(connection)
        return connection


class _Clock:
    """Monotonic time the test moves. A long uptime cannot be tested in real time."""

    def __init__(self, start: float = 10_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _spec(name, model=ProcessModel.CHILD, connections=(), subsystems=()) -> _ScriptedSpec:
    kwargs = {"name": name, "process_model": model, "subsystems": tuple(subsystems)}
    if model is ProcessModel.EXTERNAL:
        kwargs["handshake"] = MANIFEST_HANDSHAKE
    else:
        kwargs["command"] = ["fake-plugin", name]
    return _ScriptedSpec(queue=list(connections), **kwargs)


def _registry(label: str, in_process: set[str] | None = None) -> type:
    """A throwaway registry carrying the real coexistence mixin.

    Built per test with ``type()`` so each one gets its own ``_grpc_plugins`` from
    ``__init_subclass__`` and no test can see another's registrations.
    """
    names = set(in_process or ())
    return type(
        f"_Fake{label}Registry",
        (GrpcCoexistenceMixin,),
        {"_names": names, "_in_process_names": classmethod(lambda cls: set(cls._names))},
    )


def _supervisor(**kwargs) -> PluginSupervisor:
    """A supervisor wired to fakes: recording sink, no waiting on a fake to boot."""
    kwargs.setdefault("audit_sink", RecordingPluginAuditSink())
    kwargs.setdefault("startup_health_seconds", SCRIPTED_STARTUP_HEALTH_SECONDS)
    return PluginSupervisor(**kwargs)


def _live_supervisor(**kwargs) -> PluginSupervisor:
    """A supervisor for real subprocesses: the real startup grace, recorded audit."""
    kwargs.setdefault("audit_sink", RecordingPluginAuditSink())
    return PluginSupervisor(**kwargs)


def _events(supervisor: PluginSupervisor) -> list[PluginAuditEvent]:
    return [record.event_type for record in supervisor.audit_sink.records]


def _records_for(supervisor: PluginSupervisor, event: PluginAuditEvent) -> list[PluginAuditRecord]:
    return [r for r in supervisor.audit_sink.records if r.event_type is event]
