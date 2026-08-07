"""
Fabric cache federation tests.

These tests validate fabric-backed cache replication and routing selection.
"""

import time
from types import SimpleNamespace

import pytest

from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.serialization import JsonSerializer
from mpreg.datastructures.type_aliases import JsonDict
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport, ServerCacheTransport
from mpreg.fabric.catalog import CacheRole, CacheRoleEntry
from mpreg.fabric.index import RoutingIndex


class _StubServer:
    def __init__(self, cluster_id: str) -> None:
        self.settings = SimpleNamespace(cluster_id=cluster_id)

    def _get_all_peer_connections(self) -> JsonDict:
        return {}


def test_server_cache_transport_allowed_clusters() -> None:
    now = time.time()
    index = RoutingIndex()
    index.catalog.caches.register(
        CacheRoleEntry(
            role=CacheRole.SYNC,
            node_id="node-a",
            cluster_id="cluster-a",
            advertised_at=now,
            ttl_seconds=30.0,
        )
    )
    index.catalog.caches.register(
        CacheRoleEntry(
            role=CacheRole.SYNC,
            node_id="node-b",
            cluster_id="cluster-b",
            advertised_at=now,
            ttl_seconds=30.0,
        )
    )

    transport = ServerCacheTransport(
        server=_StubServer("cluster-a"),
        serializer=JsonSerializer(),
        routing_index=index,
        allowed_clusters=frozenset({"cluster-b"}),
    )

    assert transport.peer_ids() == ("node-b",)


@pytest.mark.asyncio
async def test_l4_cache_operations_use_fabric() -> None:
    transport = InProcessCacheTransport()
    cache_protocol = FabricCacheProtocol(
        "node-a", transport=transport, gossip_interval=60.0
    )
    cache_manager = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=True,
        ),
        cache_protocol=cache_protocol,
    )

    key = GlobalCacheKey(
        namespace="fabric.cache",
        identifier="test-key",
        version="v1.0.0",
    )
    metadata = CacheMetadata()

    put_result = await cache_manager.put(
        key,
        {"payload": "value"},
        metadata,
        CacheOptions(cache_levels=frozenset({CacheLevel.L4})),
    )
    assert put_result.success
    assert str(key) in cache_protocol.cache_entries

    get_result = await cache_manager.get(
        key, CacheOptions(cache_levels=frozenset({CacheLevel.L4}))
    )
    assert get_result.success
    assert get_result.cache_level == CacheLevel.L4

    await cache_manager.shutdown()
    await cache_protocol.shutdown()
