"""Reading plugin manifests off disk - the host's front door.

Everything else in this package assumes a `PluginSpec` already exists. Nothing
built one, so no plugin could be loaded and the whole host was unreachable: a
plugin that lives in its own repository (ADR-0082) has no import for the core to
fall back on, and without this it has no way in at all.

A manifest is one TOML file per plugin in the plugins directory:

    name = "asc3"
    process_model = "child"          # child | cron | external
    command = ["/opt/plugins/asc3/asc3-decoder"]
    subsystems = ["decoder"]

An externally-orchestrated plugin (systemd, k8s) declares where to dial instead
of what to run, and its handshake fields are validated through the same gate a
launched plugin's stdout line goes through:

    name = "fleet-reports"
    process_model = "external"
    subsystems = ["report"]
    [handshake]
    core_version = 1
    app_version = 1
    network = "tcp"
    address = "10.0.0.9:7001"

A malformed manifest is refused by name and the others still load. One bad file
must not cost an operator every plugin on the box.
"""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path

from .connection import ProcessModel
from .constants import GENERATED_SUBSYSTEMS
from .protocol import HandshakeConfig, validate_handshake
from .supervisor import PluginSpec, PluginSpecError
from .transport import TLSConfig, TransportSecurityError

logger = logging.getLogger(__name__)

MANIFEST_SUFFIX = ".toml"


class ManifestError(ValueError):
    """A manifest file cannot be read as a plugin declaration."""


def _handshake_from(name: str, raw: dict) -> HandshakeConfig:
    missing = {"core_version", "app_version", "network", "address"} - set(raw)
    if missing:
        raise ManifestError(
            f"{name}: [handshake] is missing {', '.join(sorted(missing))}",
        )
    handshake = HandshakeConfig(
        core_version=raw["core_version"],
        app_version=raw["app_version"],
        network=raw["network"],
        address=raw["address"],
        protocol=raw.get("protocol", "grpc"),
    )
    # The same gate a launched plugin's stdout line passes. A manifest is just
    # another place a handshake can arrive from, and an unvalidated one would
    # let a mode-2 plugin skip the version and network checks entirely.
    validate_handshake(handshake)
    return handshake


def spec_from_manifest(data: dict, *, source: str) -> PluginSpec:
    """One parsed manifest -> a PluginSpec, or raise ManifestError."""
    name = data.get("name")
    if not name or not isinstance(name, str):
        raise ManifestError(f"{source}: a manifest needs a string 'name'")

    declared = data.get("process_model")
    try:
        process_model = ProcessModel(declared)
    except ValueError:
        raise ManifestError(
            f"{name}: process_model {declared!r} is not one of "
            f"{', '.join(m.value for m in ProcessModel)}",
        ) from None

    subsystems = tuple(data.get("subsystems", ()))
    unknown = set(subsystems) - set(GENERATED_SUBSYSTEMS)
    if unknown:
        raise ManifestError(
            f"{name}: unknown subsystem(s) {', '.join(sorted(unknown))}; "
            f"expected any of {', '.join(GENERATED_SUBSYSTEMS)}",
        )

    handshake = None
    if "handshake" in data:
        handshake = _handshake_from(name, data["handshake"])

    tls = None
    if "tls" in data:
        raw = data["tls"]
        if "ca" not in raw:
            raise ManifestError(f"{name}: [tls] needs at least a 'ca'")
        tls = TLSConfig(
            ca=raw["ca"], cert=raw.get("cert"), key=raw.get("key"),
            server_name=raw.get("server_name"),
        )

    command = data.get("command")
    if command is not None and not isinstance(command, list):
        raise ManifestError(f"{name}: 'command' must be a list of argv strings")

    try:
        return PluginSpec(
            name=name,
            process_model=process_model,
            command=command,
            handshake=handshake,
            subsystems=subsystems,
            tls=tls,
        )
    except (PluginSpecError, TransportSecurityError) as exc:
        # A networked plugin with no credentials is refused by name here, so one
        # misconfigured manifest is reported and skipped rather than dialled.
        raise ManifestError(str(exc)) from exc


def load_manifests(directory: Path | str) -> list[PluginSpec]:
    """Every readable manifest in `directory`, in name order.

    A missing directory is not an error - a deployment with no plugins is the
    normal case, and the host must start regardless.
    """
    path = Path(directory)
    if not path.is_dir():
        logger.debug("no plugin directory at %s; nothing to load", path)
        return []

    specs: list[PluginSpec] = []
    for file in sorted(path.glob(f"*{MANIFEST_SUFFIX}")):
        try:
            with file.open("rb") as handle:
                data = tomllib.load(handle)
            specs.append(spec_from_manifest(data, source=file.name))
        except (ManifestError, tomllib.TOMLDecodeError, OSError) as exc:
            # Named and skipped, never fatal: one bad manifest must not cost an
            # operator every other plugin on the box.
            logger.error("plugin manifest %s rejected: %s", file.name, exc)

    seen: dict[str, str] = {}
    unique: list[PluginSpec] = []
    for spec in specs:
        if spec.name in seen:
            logger.error(
                "plugin %s declared twice; keeping the first and ignoring the rest",
                spec.name,
            )
            continue
        seen[spec.name] = spec.name
        unique.append(spec)
    return unique
