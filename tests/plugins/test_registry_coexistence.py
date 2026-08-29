"""Phase 3 gate: in-process and gRPC registration coexist, per name.

TRANSITION SCAFFOLDING (ADR-0018) - these tests should be deleted along with the
mixin when in-process is demoted to a dev/test harness.
"""

import pytest

from tsigma.collection.decoders.base import DecoderRegistry
from tsigma.collection.registry import IngestionMethodRegistry
from tsigma.notifications.registry import NotificationRegistry
from tsigma.plugins.coexistence import GrpcCoexistenceMixin, Origin, RegistryConflictError
from tsigma.plugins.connection import DiscoveredConnection
from tsigma.plugins.protocol import HandshakeConfig
from tsigma.reports.registry import ReportRegistry

REGISTRIES = [ReportRegistry, NotificationRegistry, DecoderRegistry, IngestionMethodRegistry]


def _conn(name="remote"):
    return DiscoveredConnection(name, HandshakeConfig(1, 1, "tcp", "127.0.0.1:1", "grpc"))


@pytest.fixture(autouse=True)
def _clean():
    """Never leak a test registration into the process-wide registries."""
    yield
    for r in REGISTRIES:
        r._grpc_plugins.clear()


class TestWiring:
    @pytest.mark.parametrize("registry", REGISTRIES, ids=lambda r: r.__name__)
    def test_registry_has_the_seam(self, registry):
        assert issubclass(registry, GrpcCoexistenceMixin)

    @pytest.mark.parametrize("registry", REGISTRIES, ids=lambda r: r.__name__)
    def test_registry_reports_its_own_in_process_names(self, registry):
        # Overridden per registry; the mixin's base raises NotImplementedError,
        # so a registry that failed to get its hook fails here.
        assert isinstance(registry._in_process_names(), set)

    def test_each_registry_has_its_own_grpc_store(self):
        ReportRegistry.register_grpc("only-a-report", _conn())
        assert ReportRegistry.is_remote("only-a-report")
        for other in (NotificationRegistry, DecoderRegistry, IngestionMethodRegistry):
            assert not other.is_remote("only-a-report")


class TestPerNameDispatch:
    def test_a_name_resolves_to_one_origin(self):
        ReportRegistry.register_grpc("vendor-report", _conn())
        assert ReportRegistry.origin("vendor-report") is Origin.GRPC
        assert ReportRegistry.origin("nope") is None

    def test_existing_in_process_plugins_are_untouched(self):
        existing = next(iter(DecoderRegistry.list_all()))
        DecoderRegistry.register_grpc("vendor-decoder", _conn())
        assert DecoderRegistry.origin(existing) is Origin.IN_PROCESS
        assert DecoderRegistry.get(existing) is not None  # in-process path still works

    def test_both_kinds_coexist_in_one_view(self):
        before = len(DecoderRegistry.list_names())
        DecoderRegistry.register_grpc("vendor-decoder", _conn())
        names = DecoderRegistry.list_names()
        assert len(names) == before + 1
        assert names["vendor-decoder"] is Origin.GRPC
        assert Origin.IN_PROCESS in names.values()

    def test_a_name_cannot_be_both(self):
        existing = next(iter(ReportRegistry.list_all()))
        with pytest.raises(RegistryConflictError):
            ReportRegistry.register_grpc(existing, _conn())

    def test_get_connection_returns_the_registered_connection(self):
        conn = _conn()
        ReportRegistry.register_grpc("vendor-report", conn)
        assert ReportRegistry.get_connection("vendor-report") is conn

    def test_get_connection_rejects_in_process_names(self):
        existing = next(iter(ReportRegistry.list_all()))
        with pytest.raises(ValueError, match="not a gRPC plugin"):
            ReportRegistry.get_connection(existing)

    def test_unregister_removes_only_the_grpc_entry(self):
        ReportRegistry.register_grpc("vendor-report", _conn())
        ReportRegistry.unregister_grpc("vendor-report")
        assert ReportRegistry.origin("vendor-report") is None
        assert len(ReportRegistry.list_all()) > 0  # in-process untouched

    def test_unregister_is_idempotent(self):
        ReportRegistry.unregister_grpc("never-registered")


class TestCallersAboveAreUnaffected:
    """Registering a gRPC plugin must not disturb the existing dispatch path."""

    def test_in_process_get_still_works_after_grpc_registration(self):
        name = next(iter(DecoderRegistry.list_all()))
        before = DecoderRegistry.get(name)
        DecoderRegistry.register_grpc("vendor-decoder", _conn())
        assert DecoderRegistry.get(name) is before

    def test_list_all_is_unchanged_by_grpc_registration(self):
        before = set(ReportRegistry.list_all())
        ReportRegistry.register_grpc("vendor-report", _conn())
        assert set(ReportRegistry.list_all()) == before
