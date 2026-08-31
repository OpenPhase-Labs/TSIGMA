"""Phase R1 gate: generated contract stubs, contract-read constants, staleness.

Three things guard the plugin host's baseline here:

  - the generated tree under ``tsigma/plugins/gen/`` covers exactly the
    subsystems the contract publishes for generation, stays a namespace package,
    and does not shadow the real ``tsigma`` app package;
  - the pinned handshake values in ``tsigma/plugins/constants.py`` are checked
    against TSIGMA-Contract's own published values by READING the contract.
    The APP wire-protocol version comes from ``VERSION`` (the one integer that
    file carries); the CORE version, the magic-cookie key and the cookie value
    come from ``PROTOCOL.md`` section 1, which is the only place they are
    published. Neither version is inferred from the other - they coincidentally
    both equal 1 today, so a test that restates local constants proves nothing;
  - the committed stub tree matches the contract as it stands right now. The
    staleness check runs here, in the normal test run, not only as a manually
    invoked script.

Every contract-dependent test SKIPS when the sibling contract repo is not
checked out, so the suite still runs on a machine without it.
"""

import importlib
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from tsigma.plugins import constants

from tests.plugins import _contract

REPO_ROOT = Path(__file__).resolve().parents[2]
GEN_DIR = REPO_ROOT / "tsigma" / "plugins" / "gen"
GEN_PROTO_SH = REPO_ROOT / "scripts" / "gen_proto.sh"

# Contract location and the PROTOCOL.md section-1 reader live in _contract, so
# this file and the handshake gate resolve a relocated contract identically.
CONTRACT_PROTO = _contract.CONTRACT_PROTO
VENDOR_DIR = CONTRACT_PROTO / "vendor" / "go-plugin"

SUBSYSTEM_STUBS = [
    ("decoder", "Decoder"),
    ("method", "Method"),
    ("notify", "Notify"),
    ("report", "Report"),
]

GO_PLUGIN_PROTOS = ["grpc_broker", "grpc_controller", "grpc_stdio"]

GO_PLUGIN_STUBS = [
    ("grpc_broker_pb2_grpc", "GRPCBrokerStub"),
    ("grpc_controller_pb2_grpc", "GRPCControllerStub"),
    ("grpc_stdio_pb2_grpc", "GRPCStdioStub"),
]

# Excluded from generation on purpose: their proto packages collide with this
# repo's real tsigma/auth/ and tsigma/storage/ packages.
UNGENERATED_SUBSYSTEMS = ["auth", "storage"]

# `CORE-PROTOCOL-VERSION` = `1` - anchored on the literal CORE bullet so the
# neighbouring APP-PROTOCOL-VERSION bullet can never satisfy it.
CORE_VERSION_RE = re.compile(r"`CORE-PROTOCOL-VERSION`\s*=\s*`(\d+)`")
# env `TSIGMA_PLUGIN_MAGIC` must equal the pinned cookie value `tsigma-plugin-v1`
COOKIE_RE = re.compile(r"env `([A-Za-z0-9_]+)`[^`]*cookie value[^`]*`([^`]+)`")
# VERSION: "Plugin Wire Protocol: 1"
APP_VERSION_RE = re.compile(r"^Plugin Wire Protocol:\s*(\d+)\s*$", re.MULTILINE)
# Any "Label: <bare integer>" line in VERSION.
VERSION_INT_LINE_RE = re.compile(r"^[^:\n]+:\s*(\d+)\s*$", re.MULTILINE)
# subsystems=(decoder method notify report) in scripts/gen_proto.sh
SCRIPT_SUBSYSTEMS_RE = re.compile(r"^subsystems=\(([^)]*)\)", re.MULTILINE)


class TestContractPublishedValues:
    """The pinned constants are read from the contract, never restated here."""

    def test_version_file_carries_exactly_one_integer_and_it_is_app(self):
        text = _contract.read_contract_file("VERSION")
        integers = VERSION_INT_LINE_RE.findall(text)
        assert len(integers) == 1, f"VERSION must carry exactly one integer, found {integers}"
        app = APP_VERSION_RE.search(text)
        assert app, "VERSION has no 'Plugin Wire Protocol: <n>' line"
        assert app.group(1) == integers[0]

    def test_app_protocol_version_matches_the_pinned_constant(self):
        text = _contract.read_contract_file("VERSION")
        app = APP_VERSION_RE.search(text)
        assert app, "VERSION has no 'Plugin Wire Protocol: <n>' line"
        assert constants.PLUGIN_PROTOCOL_VERSION == int(app.group(1))

    def test_core_protocol_version_matches_the_pinned_constant(self):
        section = _contract.protocol_section_one()
        found = CORE_VERSION_RE.findall(section)
        assert len(found) == 1, f"PROTOCOL.md section 1 must publish CORE-PROTOCOL-VERSION once, found {found}"
        assert constants.CORE_PROTOCOL_VERSION == int(found[0])

    def test_core_is_not_inferred_from_app(self):
        # Section 1 publishes both bullets, and they happen to carry the same
        # number today. CORE must still come only from its own bullet, and the
        # APP source file must be incapable of supplying CORE at all.
        section = _contract.protocol_section_one()
        assert "`APP-PROTOCOL-VERSION`" in section, "section 1 no longer publishes the APP bullet"
        assert "`CORE-PROTOCOL-VERSION`" in section, "section 1 no longer publishes the CORE bullet"
        assert "CORE-PROTOCOL-VERSION" not in _contract.read_contract_file("VERSION")

    def test_magic_cookie_key_and_value_match_the_pinned_constants(self):
        section = _contract.protocol_section_one()
        found = COOKIE_RE.findall(section)
        assert len(found) == 1, f"PROTOCOL.md section 1 must publish the magic cookie once, found {found}"
        key, value = found[0]
        assert constants.MAGIC_COOKIE_KEY == key
        assert constants.MAGIC_COOKIE_VALUE == value


class TestGeneratedTreeMatchesContract:
    """The tree covers what the contract publishes for generation, and no more."""

    @pytest.mark.parametrize("subsystem", constants.GENERATED_SUBSYSTEMS)
    def test_generated_subsystem_has_a_contract_proto(self, subsystem):
        _contract.require_contract()
        assert (CONTRACT_PROTO / "tsigma" / subsystem / "v1" / f"{subsystem}.proto").is_file()
        assert (GEN_DIR / "tsigma" / subsystem / "v1" / f"{subsystem}_pb2.py").is_file()
        assert (GEN_DIR / "tsigma" / subsystem / "v1" / f"{subsystem}_pb2_grpc.py").is_file()

    @pytest.mark.parametrize("subsystem", UNGENERATED_SUBSYSTEMS)
    def test_published_but_excluded_subsystems_are_not_generated(self, subsystem):
        _contract.require_contract()
        assert (CONTRACT_PROTO / "tsigma" / subsystem / "v1" / f"{subsystem}.proto").is_file(), (
            f"contract no longer publishes {subsystem}; the exclusion rationale needs revisiting"
        )
        assert subsystem not in constants.GENERATED_SUBSYSTEMS
        assert not (GEN_DIR / "tsigma" / subsystem).exists()

    def test_codegen_script_subsystem_list_agrees_with_constants(self):
        # The list lives in two places; they must not drift apart.
        match = SCRIPT_SUBSYSTEMS_RE.search(GEN_PROTO_SH.read_text(encoding="utf-8"))
        assert match, "scripts/gen_proto.sh no longer declares subsystems=(...)"
        assert tuple(match.group(1).split()) == tuple(constants.GENERATED_SUBSYSTEMS)

    @pytest.mark.parametrize("name", GO_PLUGIN_PROTOS)
    def test_vendored_go_plugin_protos_are_generated_flat(self, name):
        _contract.require_contract()
        assert (VENDOR_DIR / f"{name}.proto").is_file()
        # Flat, top-level module names: the go-plugin modules import each other
        # by bare name.
        assert (GEN_DIR / f"{name}_pb2.py").is_file()
        assert (GEN_DIR / f"{name}_pb2_grpc.py").is_file()

    def test_health_is_neither_vendored_nor_generated(self):
        pin = _contract.read_contract_file("proto/vendor/go-plugin/PIN.md")
        assert "grpc.health.v1 is NOT vendored" in pin
        assert not list(CONTRACT_PROTO.rglob("health*.proto"))
        assert not list(GEN_DIR.rglob("health_pb2*.py"))


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

    def test_only_the_root_and_v1_leaves_carry_an_init(self):
        assert (GEN_DIR / "__init__.py").is_file()
        assert not (GEN_DIR / "tsigma" / "__init__.py").exists()
        for subsystem in constants.GENERATED_SUBSYSTEMS:
            assert not (GEN_DIR / "tsigma" / subsystem / "__init__.py").exists()
            assert (GEN_DIR / "tsigma" / subsystem / "v1" / "__init__.py").is_file()


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

    def test_generated_subsystems_are_exactly_the_stub_tree(self):
        on_disk = sorted(p.name for p in (GEN_DIR / "tsigma").iterdir() if p.is_dir() and p.name != "__pycache__")
        assert on_disk == sorted(constants.GENERATED_SUBSYSTEMS)


class TestContractDeltas:
    """The deltas later phases depend on are present in the generated stubs."""

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


class TestStalenessGate:
    """A gen/ tree that no longer matches the contract fails the normal test run."""

    def test_codegen_script_is_present_and_executable(self):
        assert GEN_PROTO_SH.is_file()
        assert os.access(GEN_PROTO_SH, os.X_OK), "scripts/gen_proto.sh is not executable"

    def test_committed_stubs_are_current_with_the_contract(self):
        _contract.require_contract()
        bash = shutil.which("bash")
        if bash is None:
            pytest.skip("bash not available; scripts/gen_proto.sh --check cannot run here")
        try:
            result = subprocess.run(
                [bash, str(GEN_PROTO_SH), "--check"],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                timeout=300,
            )
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            pytest.fail("scripts/gen_proto.sh --check timed out after 300s")
        if "grpcio-tools not installed" in result.stderr:
            pytest.skip("grpcio-tools not installed; install the dev extra to run the staleness gate")
        assert result.returncode == 0, (
            "generated stubs are stale against the contract - rerun scripts/gen_proto.sh "
            f"and commit the result.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        assert "Contract stubs are up to date." in result.stdout
