import time

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
)
from mpreg.fabric.catalog import FunctionCatalog, FunctionEndpoint
from tests.test_helpers import TestPortManager


def _make_endpoint(
    *,
    function_id: str = "func-1",
    node_id: str,
    cluster_id: str = "test-cluster",
    ttl_seconds: float = 30.0,
    resources: frozenset[str] | None = None,
    advertised_at: float | None = None,
) -> FunctionEndpoint:
    identity = FunctionIdentity(
        name="update",
        function_id=function_id,
        version=SemanticVersion.parse("3.2.1"),
    )
    return FunctionEndpoint(
        identity=identity,
        resources=resources or frozenset({"db", "cache"}),
        node_id=node_id,
        cluster_id=cluster_id,
        advertised_at=advertised_at if advertised_at is not None else time.time(),
        ttl_seconds=ttl_seconds,
    )


def test_function_catalog_registers_and_filters() -> None:
    with TestPortManager() as port_manager:
        catalog = FunctionCatalog()
        node_a = port_manager.get_server_url()
        node_b = port_manager.get_server_url()
        entry_a = _make_endpoint(node_id=node_a, resources=frozenset({"db", "cache"}))
        entry_b = _make_endpoint(node_id=node_b, resources=frozenset({"gpu"}))
        assert catalog.register(entry_a)
        assert catalog.register(entry_b)

        selector = FunctionSelector(function_id="func-1")
        matches = catalog.find(selector, resources=frozenset({"gpu"}))
        assert matches == [entry_b]


def test_function_catalog_prunes_expired() -> None:
    with TestPortManager() as port_manager:
        catalog = FunctionCatalog()
        now = time.time()
        entry = _make_endpoint(
            node_id=port_manager.get_server_url(),
            advertised_at=now,
            ttl_seconds=0.2,
        )
        assert catalog.register(entry, now=now)
        removed = catalog.prune_expired(now=now + 0.3)
        assert removed == 1
        selector = FunctionSelector(function_id="func-1")
        assert catalog.find(selector) == []


def test_function_endpoint_roundtrip() -> None:
    with TestPortManager() as port_manager:
        entry = _make_endpoint(node_id=port_manager.get_server_url())
        payload = entry.to_dict()
        loaded = FunctionEndpoint.from_dict(payload)
        assert loaded.identity == entry.identity
        assert loaded.resources == entry.resources
