from mpreg.core.service_registry import ServiceRegistry
from mpreg.datastructures.service_spec import ServiceSpec


def test_service_registry_register_and_find() -> None:
    registry = ServiceRegistry()
    spec = ServiceSpec(
        name="tradefeed",
        namespace="market",
        protocol="tcp",
        port=9000,
        targets=("127.0.0.1", "10.0.0.2"),
        tags=frozenset({"primary", "realtime"}),
        capabilities=frozenset({"quotes"}),
        metadata={"tier": "gold"},
        priority=10,
        weight=5,
        ttl_seconds=15.0,
    )
    registration = registry.register(spec)
    assert registration.spec.name == "tradefeed"
    assert registration.spec.namespace == "market"
    found = registry.find(namespace="market", name="tradefeed")
    assert len(found) == 1
    assert found[0].registration_id == registration.registration_id
    removed = registry.unregister(
        namespace="market",
        name="tradefeed",
        protocol="tcp",
        port=9000,
    )
    assert removed is not None
    assert registry.find(namespace="market", name="tradefeed") == ()
