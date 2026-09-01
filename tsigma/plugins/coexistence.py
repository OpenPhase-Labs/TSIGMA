"""TRANSITION SCAFFOLDING - in-process and gRPC registration side by side.

ADR-0018 makes gRPC the go-forward model and supersedes the in-process registry;
this mixin exists so subsystems migrate one plugin at a time instead of in a flag
day. It is meant to be DELETED once in-process is demoted to a dev/test harness -
so it is confined to this file plus one base class on each registry, and nothing
above the registries knows it is here.

Dispatch is per NAME: `asc3` can stay an in-process decoder while a vendor's
decoder runs out-of-process, in the same host, at the same time.

Scope note: this phase gives the registries somewhere to hold gRPC registrations
and a way to resolve them. The Remote* wrappers that make a connection callable
as a Report/Decoder/Method arrive with their own slices (P5, P6, P8).

Class-typed accessors stay in-process-only until those slices land. A registry's
`get` returns a CLASS; a remote name has no class, so `get` raises
`RemoteRegistrationError` naming the gRPC origin instead of claiming the name is
unregistered. `list_all` and the attribute filters (decoder extensions, method
execution modes) likewise carry in-process entries only - `list_names` is the one
complete view of both paths.
"""

import enum

from .connection import PluginConnection


class Origin(str, enum.Enum):
    """Where a registered name resolves to."""

    IN_PROCESS = "in_process"
    GRPC = "grpc"


class RegistryConflictError(ValueError):
    """A name was registered both in-process and over gRPC."""


class RemoteRegistrationError(ValueError):
    """A class-typed lookup was asked for a name that is served over gRPC.

    ValueError base is load-bearing: callers such as `decoders.auto` and the
    notification provider bootstrap already catch ValueError from `get`.
    """


class GrpcCoexistenceMixin:
    """Adds a gRPC registration path beside a registry's in-process decorator.

    Each registry gets its OWN store; without this the four registries would
    share one dict and a decoder named `x` would collide with a report named `x`.
    A subclass of a registry inherits that registry's store, matching how it
    inherits the in-process dict - both halves of a registry's state answer the
    same way about a name. A direct subclass of this mixin is a new registry and
    starts empty (`tests/plugins/_supervisor_fakes._registry` depends on that
    isolation).
    """

    _grpc_plugins: dict[str, PluginConnection]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not any("_grpc_plugins" in base.__dict__ for base in cls.__mro__[1:]):
            cls._grpc_plugins = {}

    # ---------------------------------------------------------------- register
    @classmethod
    def register_grpc(cls, name: str, connection: PluginConnection) -> PluginConnection:
        """Register a name as served by an out-of-process plugin."""
        if name in cls._in_process_names():
            raise RegistryConflictError(
                f"{name!r} is already registered in-process; a name resolves one way only"
            )
        cls._grpc_plugins[name] = connection
        return connection

    @classmethod
    def _guard_in_process(cls, name: str) -> None:
        """Refuse an in-process registration for a name served over gRPC.

        The other half of `register_grpc`'s guard: a name resolves one way only,
        whichever path claims it first.

        Raises:
            RegistryConflictError: If `name` is registered over gRPC.
        """
        if name in cls._grpc_plugins:
            raise RegistryConflictError(
                f"{name!r} is already registered over gRPC; a name resolves one way only"
            )

    @classmethod
    def _guard_remote_lookup(cls, name: str) -> None:
        """Refuse a class-typed lookup of a name served over gRPC.

        Raises:
            RemoteRegistrationError: If `name` is registered over gRPC.
        """
        if name in cls._grpc_plugins:
            raise RemoteRegistrationError(
                f"{name!r} is registered over gRPC (origin {Origin.GRPC.value}); "
                f"use get_connection({name!r}) - there is no in-process class to return"
            )

    @classmethod
    def unregister_grpc(cls, name: str) -> None:
        """Drop a gRPC registration (plugin removed, or supervisor gave up on it)."""
        cls._grpc_plugins.pop(name, None)

    # ----------------------------------------------------------------- resolve
    @classmethod
    def origin(cls, name: str) -> Origin | None:
        """Where `name` resolves, or None if it is not registered at all."""
        if name in cls._grpc_plugins:
            return Origin.GRPC
        if name in cls._in_process_names():
            return Origin.IN_PROCESS
        return None

    @classmethod
    def is_remote(cls, name: str) -> bool:
        return name in cls._grpc_plugins

    @classmethod
    def get_connection(cls, name: str) -> PluginConnection:
        """The connection serving `name`."""
        if name not in cls._grpc_plugins:
            raise ValueError(f"{name!r} is not a gRPC plugin")
        return cls._grpc_plugins[name]

    @classmethod
    def list_grpc(cls) -> dict[str, PluginConnection]:
        return dict(cls._grpc_plugins)

    @classmethod
    def list_names(cls) -> dict[str, Origin]:
        """Every registered name and where it resolves - both paths, one view."""
        names = {n: Origin.IN_PROCESS for n in cls._in_process_names()}
        names.update({n: Origin.GRPC for n in cls._grpc_plugins})
        return names

    # ------------------------------------------------------------------ hook
    @classmethod
    def _in_process_names(cls) -> set[str]:
        """The registry's own in-process store. Overridden per registry."""
        raise NotImplementedError
