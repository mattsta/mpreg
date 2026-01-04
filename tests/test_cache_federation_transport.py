from __future__ import annotations

import time
from dataclasses import dataclass, field

import pytest

from mpreg.core.global_cache import CacheMetadata, GlobalCacheEntry, GlobalCacheKey
from mpreg.core.serialization import JsonSerializer
from mpreg.fabric.cache_federation import (
    CacheDigest,
    CacheDigestEntry,
    CacheOperationMessage,
    CacheOperationType,
)
from mpreg.fabric.cache_selection import CachePeerSelector
from mpreg.fabric.cache_transport import (
    CacheMessageKind,
    InProcessCacheTransport,
    ServerCacheTransport,
)
from mpreg.fabric.catalog import CacheNodeProfile, CacheRole, CacheRoleEntry
from mpreg.fabric.federation_graph import GeographicCoordinate
from mpreg.fabric.federation_planner import (
    FabricForwardingFailureReason,
    FabricForwardingPlan,
)
from mpreg.fabric.index import RoutingIndex
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    UnifiedMessage,
)


@dataclass(slots=True)
class StubReceiver:
    node_id: str
    digest: CacheDigest
    entries: dict[str, GlobalCacheEntry] = field(default_factory=dict)
    last_message: CacheOperationMessage | None = None

    async def handle_cache_message(self, message: CacheOperationMessage) -> bool:
        self.last_message = message
        return True

    def create_cache_digest(self) -> CacheDigest:
        return self.digest

    def get_cache_entry(self, key: GlobalCacheKey) -> GlobalCacheEntry | None:
        return self.entries.get(str(key))


@dataclass(slots=True)
class StubConnection:
    is_connected: bool = True


@dataclass(slots=True)
class StubCluster:
    local_url: str
    cluster_map: dict[str, str]

    def cluster_id_for_node_url(self, node_url: str) -> str | None:
        return self.cluster_map.get(node_url)


@dataclass(slots=True)
class StubSettings:
    cluster_id: str
    fabric_routing_max_hops: int = 3


@dataclass(slots=True)
class StubPlanner:
    next_peer_url: str | None

    def plan_next_hop(
        self,
        *,
        target_cluster: str | None,
        visited_clusters: tuple[str, ...] = (),
        remaining_hops: int | None = None,
    ) -> FabricForwardingPlan:
        remaining = 0 if remaining_hops is None else remaining_hops
        if not target_cluster or not self.next_peer_url:
            return FabricForwardingPlan(
                target_cluster=target_cluster or "",
                next_cluster=None,
                next_peer_url=None,
                planned_path=tuple(),
                federation_path=visited_clusters,
                remaining_hops=remaining,
                reason=FabricForwardingFailureReason.NO_PATH,
            )
        return FabricForwardingPlan(
            target_cluster=target_cluster,
            next_cluster="cluster-next",
            next_peer_url=self.next_peer_url,
            planned_path=(target_cluster,),
            federation_path=visited_clusters,
            remaining_hops=max(0, remaining - 1),
            reason=FabricForwardingFailureReason.OK,
        )


@dataclass(slots=True)
class StubServer:
    cluster: StubCluster
    settings: StubSettings
    _fabric_federation_planner: StubPlanner | None
    connections: dict[str, StubConnection]
    last_target_nodes: tuple[str, ...] | None = None

    def _get_all_peer_connections(self) -> dict[str, StubConnection]:
        return self.connections

    async def _send_fabric_message(
        self,
        message: UnifiedMessage,
        *,
        target_nodes: tuple[str, ...],
        source_peer_url: str | None = None,
        allow_routing_path_targets: bool = False,
    ) -> None:
        self.last_target_nodes = target_nodes

    def _next_fabric_headers(
        self,
        correlation_id: str,
        headers: MessageHeaders | None,
        *,
        max_hops: int | None,
    ) -> MessageHeaders | None:
        return headers

    def _fabric_next_hop_for_cluster(
        self, cluster_id: str, *, headers: MessageHeaders
    ) -> str | None:
        if not self._fabric_federation_planner:
            return None
        plan = self._fabric_federation_planner.plan_next_hop(
            target_cluster=cluster_id,
            visited_clusters=headers.federation_path,
            remaining_hops=headers.hop_budget,
        )
        if plan.can_forward and plan.next_peer_url:
            return plan.next_peer_url
        return None


def _build_digest(node_id: str, key: GlobalCacheKey) -> CacheDigest:
    entry = CacheDigestEntry(
        key=key,
        version=1,
        timestamp=time.time(),
        value_hash="hash",
        size_bytes=10,
        node_id=node_id,
    )
    return CacheDigest(
        node_id=node_id,
        timestamp=time.time(),
        entries={str(key): entry},
        total_entries=1,
        total_size_bytes=10,
    )


@pytest.mark.asyncio
async def test_transport_register_and_send_operation() -> None:
    transport = InProcessCacheTransport()
    key = GlobalCacheKey(namespace="cache", identifier="alpha")
    receiver = StubReceiver(node_id="node-a", digest=_build_digest("node-a", key))

    transport.register(receiver)
    assert transport.peer_ids() == ("node-a",)

    message = CacheOperationMessage(
        operation_type=CacheOperationType.PUT,
        operation_id="op-1",
        key=key,
        timestamp=time.time(),
        source_node="node-b",
        vector_clock={"node-b": 1},
        value={"value": 1},
        metadata=CacheMetadata(),
    )

    success = await transport.send_operation("node-a", message)

    assert success
    assert receiver.last_message == message


@pytest.mark.asyncio
async def test_transport_fetch_digest_and_entry() -> None:
    transport = InProcessCacheTransport()
    key = GlobalCacheKey(namespace="cache", identifier="beta")
    digest = _build_digest("node-b", key)
    entry = GlobalCacheEntry(
        key=key,
        value={"payload": "data"},
        metadata=CacheMetadata(),
    )
    receiver = StubReceiver(
        node_id="node-b",
        digest=digest,
        entries={str(key): entry},
    )

    transport.register(receiver)

    fetched_digest = await transport.fetch_digest("node-b")
    fetched_entry = await transport.fetch_entry("node-b", key)

    assert fetched_digest == digest
    assert fetched_entry == entry


def test_server_cache_transport_selects_peers_by_profile() -> None:
    now = time.time()
    routing_index = RoutingIndex()
    routing_index.catalog.caches.register(
        CacheRoleEntry(
            role=CacheRole.SYNC,
            node_id="node-west",
            cluster_id="cluster-a",
            advertised_at=now,
            ttl_seconds=30.0,
        ),
        now=now,
    )
    routing_index.catalog.caches.register(
        CacheRoleEntry(
            role=CacheRole.SYNC,
            node_id="node-eu",
            cluster_id="cluster-a",
            advertised_at=now,
            ttl_seconds=30.0,
        ),
        now=now,
    )
    routing_index.catalog.cache_profiles.register(
        CacheNodeProfile(
            node_id="node-west",
            cluster_id="cluster-a",
            region="us-west",
            coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
            capacity_mb=512,
            utilization_percent=20.0,
            avg_latency_ms=8.0,
            reliability_score=0.98,
            advertised_at=now,
            ttl_seconds=30.0,
        ),
        now=now,
    )
    routing_index.catalog.cache_profiles.register(
        CacheNodeProfile(
            node_id="node-eu",
            cluster_id="cluster-a",
            region="eu-west",
            coordinates=GeographicCoordinate(latitude=51.0, longitude=0.0),
            capacity_mb=512,
            utilization_percent=15.0,
            avg_latency_ms=20.0,
            reliability_score=0.98,
            advertised_at=now,
            ttl_seconds=30.0,
        ),
        now=now,
    )
    selector = CachePeerSelector(
        local_region="us-west",
        local_coordinates=GeographicCoordinate(latitude=37.0, longitude=-122.0),
    )
    transport = ServerCacheTransport(
        server=object(),
        serializer=JsonSerializer(),
        routing_index=routing_index,
        peer_selector=selector,
    )
    message = CacheOperationMessage(
        operation_type=CacheOperationType.PUT,
        operation_id="op-1",
        key=GlobalCacheKey(namespace="cache", identifier="alpha"),
        timestamp=now,
        source_node="node-local",
        vector_clock={"node-local": 1},
        value={"value": 1},
        metadata=CacheMetadata(geographic_hints=["eu-west"]),
    )

    peers = transport.select_peers(message, exclude="node-local", max_peers=1)

    assert peers == ("node-eu",)


@pytest.mark.asyncio
async def test_server_cache_transport_uses_path_vector_next_hop() -> None:
    target_node = "node-remote"
    next_hop = "node-hop"
    server = StubServer(
        cluster=StubCluster(
            local_url="node-local",
            cluster_map={target_node: "cluster-remote"},
        ),
        settings=StubSettings(cluster_id="cluster-local", fabric_routing_max_hops=3),
        _fabric_federation_planner=StubPlanner(next_peer_url=next_hop),
        connections={next_hop: StubConnection()},
    )
    transport = ServerCacheTransport(
        server=server,
        serializer=JsonSerializer(),
    )
    message = CacheOperationMessage(
        operation_type=CacheOperationType.PUT,
        operation_id="op-1",
        key=GlobalCacheKey(namespace="cache", identifier="alpha"),
        timestamp=time.time(),
        source_node="node-local",
        vector_clock={"node-local": 1},
        value={"value": 1},
        metadata=CacheMetadata(),
    )

    sent = await transport.send_operation(target_node, message)

    assert sent is True
    assert server.last_target_nodes == (next_hop,)


@pytest.mark.asyncio
async def test_server_cache_transport_prefers_direct_peer() -> None:
    target_node = "node-remote"
    server = StubServer(
        cluster=StubCluster(
            local_url="node-local",
            cluster_map={target_node: "cluster-remote"},
        ),
        settings=StubSettings(cluster_id="cluster-local", fabric_routing_max_hops=3),
        _fabric_federation_planner=StubPlanner(next_peer_url="node-hop"),
        connections={target_node: StubConnection()},
    )
    transport = ServerCacheTransport(
        server=server,
        serializer=JsonSerializer(),
    )
    message = CacheOperationMessage(
        operation_type=CacheOperationType.PUT,
        operation_id="op-2",
        key=GlobalCacheKey(namespace="cache", identifier="beta"),
        timestamp=time.time(),
        source_node="node-local",
        vector_clock={"node-local": 1},
        value={"value": 1},
        metadata=CacheMetadata(),
    )

    sent = await transport.send_operation(target_node, message)

    assert sent is True
    assert server.last_target_nodes == (target_node,)


@pytest.mark.asyncio
async def test_server_cache_transport_drops_without_route() -> None:
    target_node = "node-remote"
    server = StubServer(
        cluster=StubCluster(
            local_url="node-local",
            cluster_map={target_node: "cluster-remote"},
        ),
        settings=StubSettings(cluster_id="cluster-local", fabric_routing_max_hops=3),
        _fabric_federation_planner=StubPlanner(next_peer_url=None),
        connections={},
    )
    transport = ServerCacheTransport(
        server=server,
        serializer=JsonSerializer(),
    )
    message = CacheOperationMessage(
        operation_type=CacheOperationType.PUT,
        operation_id="op-3",
        key=GlobalCacheKey(namespace="cache", identifier="gamma"),
        timestamp=time.time(),
        source_node="node-local",
        vector_clock={"node-local": 1},
        value={"value": 1},
        metadata=CacheMetadata(),
    )

    sent = await transport.send_operation(target_node, message)

    assert sent is False
    assert server.last_target_nodes is None


@pytest.mark.asyncio
async def test_cache_transport_forwards_operation_to_target_node() -> None:
    target_node = "node-remote"
    next_hop = "node-hop"
    server = StubServer(
        cluster=StubCluster(
            local_url="node-local",
            cluster_map={target_node: "cluster-remote"},
        ),
        settings=StubSettings(cluster_id="cluster-local", fabric_routing_max_hops=3),
        _fabric_federation_planner=StubPlanner(next_peer_url=next_hop),
        connections={next_hop: StubConnection()},
    )
    transport = ServerCacheTransport(
        server=server,
        serializer=JsonSerializer(),
    )
    receiver = StubReceiver(
        node_id="node-local",
        digest=_build_digest("node-local", GlobalCacheKey("cache", "alpha")),
    )
    transport.register(receiver)
    op = CacheOperationMessage(
        operation_type=CacheOperationType.PUT,
        operation_id="op-forward",
        key=GlobalCacheKey(namespace="cache", identifier="alpha"),
        timestamp=time.time(),
        source_node="node-origin",
        vector_clock={"node-origin": 1},
        value={"value": 2},
        metadata=CacheMetadata(),
    )
    message = UnifiedMessage(
        message_id=op.operation_id,
        topic="mpreg.cache.sync.operation",
        message_type=MessageType.CACHE,
        delivery=DeliveryGuarantee.AT_LEAST_ONCE,
        payload={
            "kind": CacheMessageKind.OPERATION.value,
            "operation": op.to_dict(),
            "target_node": target_node,
        },
        headers=MessageHeaders(
            correlation_id=op.operation_id,
            routing_path=("node-origin",),
            federation_path=("cluster-origin",),
            hop_budget=3,
        ),
        timestamp=time.time(),
    )

    await transport.handle_message(message)

    assert receiver.last_message is None
    assert server.last_target_nodes == (next_hop,)
