"""Phase R3c gate: one name, one origin, across all four registries.

`GrpcCoexistenceMixin` lets a subsystem migrate a plugin at a time, so for every
name in it two answers have to stay reconcilable. What is gated here:

  - The conflict guard runs BOTH ways. `register_grpc` already refused a name the
    in-process store held; the in-process decorator refused nothing, so a gRPC
    name could be shadowed by a decorator and the registry would answer GRPC from
    `origin` while resolving in-process from `get`.
  - No accessor denies a registered gRPC name. `get` is class-typed and a remote
    name has no class, so it raises `RemoteRegistrationError` naming the gRPC
    origin - it does not report the name unregistered and it does not hand back
    the connection. The class-attribute filters (decoder extensions, method
    execution modes) are out of scope: they cannot classify a remote name from
    what a registry holds today, so nothing here asserts on them.
  - `unregister_grpc` clears the name from every one of those accessors, proved by
    the state that changed - an in-process registration of that name then succeeds.
  - A subclass of a registry inherits that registry's gRPC store, the way it
    already inherits the in-process dict, while direct subclasses of the mixin
    stay isolated (`_supervisor_fakes._registry` builds them that way).

The four registries are global, so the fixture snapshots and restores both halves
of each one's state rather than leaving registrations behind for the suite.
"""

import pytest

from tsigma.plugins.coexistence import Origin, RegistryConflictError, RemoteRegistrationError

from tests.plugins._supervisor_fakes import _FakeConnection, _registry
from tsigma.collection.decoders.base import DecoderRegistry
from tsigma.collection.registry import IngestionMethodRegistry
from tsigma.notifications.registry import NotificationRegistry
from tsigma.reports.registry import ReportRegistry

# (registry, its in-process store, whether it publishes list_available()).
# DecoderRegistry and ReportRegistry expose only the class-typed `list_all`, which
# cannot carry a remote name; `list_names()` is their complete view.
_REGISTRIES = [
    (DecoderRegistry, "_decoders", False),
    (IngestionMethodRegistry, "_methods", True),
    (NotificationRegistry, "_providers", True),
    (ReportRegistry, "_reports", False),
]

_CASES = [pytest.param(*spec, id=spec[0].__name__) for spec in _REGISTRIES]

every_registry = pytest.mark.parametrize("registry, store_attr, has_list_available", _CASES)


def _register_in_process(registry: type, name: str) -> type:
    """Register `name` in-process through the registry's own decorator form.

    `DecoderRegistry.register` is a bare class decorator keyed off `cls.name`; the
    other three are decorator FACTORIES, so the returned decorator has to be
    applied for anything to reach the store.
    """
    stub = type("_StubPlugin", (), {"name": name})
    if registry is DecoderRegistry:
        return registry.register(stub)
    return registry.register(name)(stub)


@pytest.fixture(autouse=True)
def _restore_registries():
    snapshots = [
        (registry, store_attr, registry._grpc_plugins.copy(), getattr(registry, store_attr).copy())
        for registry, store_attr, _ in _REGISTRIES
    ]
    yield
    for registry, store_attr, grpc_snapshot, in_process_snapshot in snapshots:
        registry._grpc_plugins.clear()
        registry._grpc_plugins.update(grpc_snapshot)
        getattr(registry, store_attr).clear()
        getattr(registry, store_attr).update(in_process_snapshot)


# --------------------------------------------------------------- conflict guard
class TestConflictGuardIsBidirectional:
    @every_registry
    def test_an_in_process_registration_is_refused_for_a_grpc_name(self, registry, store_attr, has_list_available):
        name = "coexist_grpc_first"
        registry.register_grpc(name, _FakeConnection(name))

        with pytest.raises(RegistryConflictError):
            _register_in_process(registry, name)

        assert name not in getattr(registry, store_attr)
        assert registry.origin(name) is Origin.GRPC
        assert registry.is_remote(name) is True
        assert name in registry.list_grpc()
        assert registry.list_names()[name] is Origin.GRPC

    @every_registry
    def test_a_grpc_registration_is_refused_for_an_in_process_name(self, registry, store_attr, has_list_available):
        name = "coexist_in_process_first"
        stub = _register_in_process(registry, name)

        with pytest.raises(RegistryConflictError):
            registry.register_grpc(name, _FakeConnection(name))

        assert getattr(registry, store_attr)[name] is stub
        assert registry.origin(name) is Origin.IN_PROCESS
        assert registry.is_remote(name) is False
        assert name not in registry.list_grpc()
        assert registry.list_names()[name] is Origin.IN_PROCESS


# ------------------------------------------------------------------- accessors
class TestNoAccessorDeniesARegisteredGrpcName:
    @every_registry
    def test_origin_remoteness_and_listings_agree(self, registry, store_attr, has_list_available):
        name = "coexist_listed"
        connection = registry.register_grpc(name, _FakeConnection(name))

        assert registry.origin(name) is Origin.GRPC
        assert registry.is_remote(name) is True
        assert registry.list_grpc()[name] is connection
        assert registry.list_names()[name] is Origin.GRPC
        assert registry.get_connection(name) is connection
        if has_list_available:
            assert name in registry.list_available()

    @every_registry
    def test_get_names_the_grpc_origin_instead_of_returning_a_remote(self, registry, store_attr, has_list_available):
        name = "coexist_looked_up"
        connection = registry.register_grpc(name, _FakeConnection(name))

        with pytest.raises(RemoteRegistrationError) as exc_info:
            registry.get(name)

        assert name in str(exc_info.value)
        assert Origin.GRPC.value in str(exc_info.value)
        assert "Unknown" not in str(exc_info.value)
        # `get` yields nothing at all; the connection has its own accessor.
        assert registry.get_connection(name) is connection


# ----------------------------------------------------------------- unregister
class TestUnregisterRemovesTheNameEverywhere:
    @every_registry
    def test_every_accessor_forgets_the_name(self, registry, store_attr, has_list_available):
        name = "coexist_dropped"
        registry.register_grpc(name, _FakeConnection(name))
        registry.unregister_grpc(name)

        assert registry.origin(name) is None
        assert registry.is_remote(name) is False
        assert name not in registry.list_grpc()
        assert name not in registry.list_names()
        assert name not in getattr(registry, store_attr)
        if has_list_available:
            assert name not in registry.list_available()

        with pytest.raises(ValueError) as exc_info:
            registry.get(name)
        assert not isinstance(exc_info.value, RemoteRegistrationError)

        with pytest.raises(ValueError):
            registry.get_connection(name)

    @every_registry
    def test_the_name_is_free_for_the_in_process_path_again(self, registry, store_attr, has_list_available):
        name = "coexist_reclaimed"
        registry.register_grpc(name, _FakeConnection(name))
        registry.unregister_grpc(name)

        stub = _register_in_process(registry, name)

        assert getattr(registry, store_attr)[name] is stub
        assert registry.origin(name) is Origin.IN_PROCESS
        assert registry.is_remote(name) is False
        assert registry.get(name) is stub


# ------------------------------------------------------------------ inheritance
class TestSubclassingARegistryKeepsOneStore:
    def test_a_subclass_sees_the_registrys_grpc_registrations(self):
        class SubDecoderRegistry(DecoderRegistry):
            pass

        name = "coexist_inherited"
        connection = DecoderRegistry.register_grpc(name, _FakeConnection(name))

        assert SubDecoderRegistry._grpc_plugins is DecoderRegistry._grpc_plugins
        assert SubDecoderRegistry.origin(name) is Origin.GRPC
        assert SubDecoderRegistry.is_remote(name) is True
        assert SubDecoderRegistry.list_grpc()[name] is connection
        assert SubDecoderRegistry.get_connection(name) is connection

    def test_direct_subclasses_of_the_mixin_stay_isolated(self):
        first = _registry("First")
        second = _registry("Second")

        name = "coexist_isolated"
        first.register_grpc(name, _FakeConnection(name))

        assert first.origin(name) is Origin.GRPC
        assert second.origin(name) is None
        assert name not in second.list_grpc()
        assert second.list_names() == {}
