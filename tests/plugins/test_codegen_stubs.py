"""Phase 1 gate: the generated contract stubs import and expose the contract.

Guards three things that have silently broken before:
  - the generated tree does not shadow the real tsigma/ app package
  - every subsystem stub and vendored go-plugin stub imports
  - the pinned protocol constants match TSIGMA-Contract PROTOCOL.md
"""

import importlib

import pytest

from tsigma.plugins import constants

SUBSYSTEM_STUBS = [
    ("decoder", "Decoder"),
    ("method", "Method"),
    ("notify", "Notify"),
    ("report", "Report"),
]

GO_PLUGIN_STUBS = [
    ("grpc_broker_pb2_grpc", "GRPCBrokerStub"),
    ("grpc_controller_pb2_grpc", "GRPCControllerStub"),
    ("grpc_stdio_pb2_grpc", "GRPCStdioStub"),
]


class TestNoShadowing:
    """The generated tsigma/ tree must stay a namespace package."""

    def test_real_app_package_still_imports(self):
        # Regression: an __init__.py in gen/tsigma/ makes it a regular package
        # that shadows the real one, and these stop resolving.
        for name in ("tsigma.config", "tsigma.storage.base", "tsigma.auth.providers.local"):
            assert importlib.import_module(name) is not None

    def test_tsigma_is_a_namespace_package(self):
        import tsigma

        assert getattr(tsigma, "__file__", None) is None


class TestSubsystemStubs:
    """Each generated subsystem exposes its service stub and message module."""

    @pytest.mark.parametrize("subsystem,service", SUBSYSTEM_STUBS)
    def test_stub_imports(self, subsystem, service):
        mod = importlib.import_module(f"tsigma.{subsystem}.v1.{subsystem}_pb2_grpc")
        assert hasattr(mod, f"{service}Stub")
        assert hasattr(mod, f"{service}Servicer")

    @pytest.mark.parametrize("subsystem,_service", SUBSYSTEM_STUBS)
    def test_messages_import(self, subsystem, _service):
        mod = importlib.import_module(f"tsigma.{subsystem}.v1.{subsystem}_pb2")
        assert mod.DESCRIPTOR.package == f"tsigma.{subsystem}.v1"

    def test_auth_and_storage_are_not_generated(self):
        # Excluded by design - their proto packages collide with the real
        # tsigma/auth/ and tsigma/storage/ packages. See constants.py.
        assert "auth" not in constants.GENERATED_SUBSYSTEMS
        assert "storage" not in constants.GENERATED_SUBSYSTEMS


class TestContractDeltas:
    """The two deltas Phase 6/8 depend on are present in the generated stubs."""

    def test_decoder_carries_terminal_status(self):
        from tsigma.decoder.v1 import decoder_pb2

        arms = [f.name for f in decoder_pb2.DecodeResult.DESCRIPTOR.oneofs[0].fields]
        assert "status" in arms
        outcomes = [v.name for v in decoder_pb2.DecodeOutcome.DESCRIPTOR.values]
        assert outcomes == [
            "DECODE_OUTCOME_UNSPECIFIED",
            "DECODE_OUTCOME_SUCCESS",
            "DECODE_OUTCOME_PARTIAL",
            "DECODE_OUTCOME_FAILURE",
        ]

    def test_persist_response_carries_outcome_and_high_water_mark(self):
        from tsigma.method.v1 import method_pb2

        fields = {f.name: f.number for f in method_pb2.PersistResponse.DESCRIPTOR.fields}
        assert fields == {"events_inserted": 1, "outcome": 2, "max_event_time": 3}


class TestGoPluginStubs:
    """The vendored go-plugin envelope services."""

    @pytest.mark.parametrize("module,stub", GO_PLUGIN_STUBS)
    def test_stub_imports(self, module, stub):
        assert hasattr(importlib.import_module(module), stub)

    def test_health_comes_from_grpcio_health_checking(self):
        from grpc_health.v1 import health_pb2_grpc

        assert hasattr(health_pb2_grpc, "HealthStub")


class TestPinnedConstants:
    """Constants mirror PROTOCOL.md; drift here breaks the handshake."""

    def test_protocol_versions(self):
        assert constants.CORE_PROTOCOL_VERSION == 1
        assert constants.PLUGIN_PROTOCOL_VERSION == 1

    def test_magic_cookie(self):
        assert constants.MAGIC_COOKIE_KEY == "TSIGMA_PLUGIN_MAGIC"
        assert constants.MAGIC_COOKIE_VALUE == "tsigma-plugin-v1"
