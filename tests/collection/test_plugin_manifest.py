"""The front door: a plugin in its own repository gets in through a manifest.

Everything else in the plugin host assumes a `PluginSpec` already exists.
Nothing built one, so the whole host was unreachable code - a plugin that lives
outside the core repo (ADR-0082) has no import to fall back on.
"""

import logging

import pytest

from tsigma.plugins.connection import ProcessModel
from tsigma.plugins.manifest import (
    ManifestError,
    load_manifests,
    spec_from_manifest,
)

CHILD = {
    "name": "asc3",
    "process_model": "child",
    "command": ["/opt/plugins/asc3/asc3-decoder"],
    "subsystems": ["decoder"],
}
EXTERNAL = {
    "name": "fleet-reports",
    "process_model": "external",
    "subsystems": ["report"],
    "handshake": {
        "core_version": 1, "app_version": 1,
        "network": "tcp", "address": "10.0.0.9:7001",
    },
}


def _write(tmp_path, filename, body):
    (tmp_path / filename).write_text(body, encoding="utf-8")


class TestAManifestBecomesASpec:
    def test_a_launched_plugin(self):
        spec = spec_from_manifest(CHILD, source="asc3.toml")
        assert spec.name == "asc3"
        assert spec.process_model is ProcessModel.CHILD
        assert spec.command == ["/opt/plugins/asc3/asc3-decoder"]
        assert spec.subsystems == ("decoder",)

    def test_an_externally_orchestrated_plugin_declares_where_to_dial(self):
        spec = spec_from_manifest(EXTERNAL, source="fleet.toml")
        assert spec.process_model is ProcessModel.EXTERNAL
        assert spec.handshake.address == "10.0.0.9:7001"

    def test_the_spec_can_build_its_connection(self):
        # The whole point: a manifest produces something the supervisor drives.
        assert spec_from_manifest(CHILD, source="x").build() is not None


class TestAManifestIsValidatedNotTrusted:
    def test_a_missing_name_is_refused(self):
        with pytest.raises(ManifestError, match="name"):
            spec_from_manifest({"process_model": "child"}, source="x.toml")

    def test_an_unknown_process_model_is_refused(self):
        with pytest.raises(ManifestError, match="process_model"):
            spec_from_manifest({"name": "p", "process_model": "docker"}, source="x")

    def test_an_unknown_subsystem_is_refused(self):
        bad = dict(CHILD, subsystems=["decoder", "telepathy"])
        with pytest.raises(ManifestError, match="telepathy"):
            spec_from_manifest(bad, source="x")

    def test_a_child_without_a_command_is_refused(self):
        with pytest.raises(ManifestError, match="command"):
            spec_from_manifest({"name": "p", "process_model": "child"}, source="x")

    def test_an_external_manifest_handshake_goes_through_the_same_gate(self):
        # A manifest is just another place a handshake arrives from. Skipping
        # the gate here would let mode 2 bypass the protocol and network checks
        # a launched plugin's stdout line has to pass.
        bad = dict(EXTERNAL)
        bad["handshake"] = dict(EXTERNAL["handshake"], network="carrier-pigeon")
        with pytest.raises(Exception):
            spec_from_manifest(bad, source="x")

    def test_an_incomplete_handshake_is_refused(self):
        bad = dict(EXTERNAL)
        bad["handshake"] = {"core_version": 1, "app_version": 1}
        with pytest.raises(ManifestError, match="handshake"):
            spec_from_manifest(bad, source="x")


class TestLoadingADirectory:
    def test_manifests_load_in_name_order(self, tmp_path):
        _write(tmp_path, "b.toml", 'name="b"\nprocess_model="child"\ncommand=["/b"]\n')
        _write(tmp_path, "a.toml", 'name="a"\nprocess_model="child"\ncommand=["/a"]\n')
        assert [s.name for s in load_manifests(tmp_path)] == ["a", "b"]

    def test_a_missing_directory_is_not_an_error(self, tmp_path):
        # A deployment with no plugins is the normal case; the host still boots.
        assert load_manifests(tmp_path / "nope") == []

    def test_one_bad_manifest_does_not_cost_the_others(self, tmp_path, caplog):
        _write(tmp_path, "good.toml", 'name="good"\nprocess_model="child"\ncommand=["/g"]\n')
        _write(tmp_path, "broken.toml", "this is not toml {{{")
        _write(tmp_path, "invalid.toml", 'name="bad"\nprocess_model="docker"\n')

        with caplog.at_level(logging.ERROR):
            specs = load_manifests(tmp_path)

        assert [s.name for s in specs] == ["good"], (
            "one bad file must not cost an operator every plugin on the box"
        )
        assert "broken.toml" in caplog.text and "invalid.toml" in caplog.text

    def test_a_duplicate_name_keeps_the_first_and_says_so(self, tmp_path, caplog):
        _write(tmp_path, "a.toml", 'name="dup"\nprocess_model="child"\ncommand=["/first"]\n')
        _write(tmp_path, "b.toml", 'name="dup"\nprocess_model="child"\ncommand=["/second"]\n')

        with caplog.at_level(logging.ERROR):
            specs = load_manifests(tmp_path)

        assert len(specs) == 1
        assert specs[0].command == ["/first"]
        assert "declared twice" in caplog.text

    def test_non_toml_files_are_ignored(self, tmp_path):
        _write(tmp_path, "readme.md", "not a manifest")
        _write(tmp_path, "a.toml", 'name="a"\nprocess_model="child"\ncommand=["/a"]\n')
        assert [s.name for s in load_manifests(tmp_path)] == ["a"]
