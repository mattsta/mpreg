import time

import pytest

from mpreg.fabric.federation_graph import GraphBasedFederationRouter
from mpreg.fabric.link_state import (
    LinkStateAnnouncer,
    LinkStateAreaPair,
    LinkStateAreaPolicy,
    LinkStateNeighbor,
    LinkStateProcessor,
    LinkStateSummaryFilter,
    LinkStateTable,
    LinkStateUpdate,
)
from mpreg.fabric.peer_directory import PeerNeighbor


class _StubPublisher:
    def __init__(self) -> None:
        self.updates: list[LinkStateUpdate] = []

    async def publish(self, update: LinkStateUpdate) -> None:
        self.updates.append(update)


def test_link_state_table_rejects_stale_update() -> None:
    table = LinkStateTable(local_cluster="cluster-a")
    now = time.time()
    update = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-b", latency_ms=5.0),),
        advertised_at=now,
        ttl_seconds=10.0,
        sequence=1,
    )
    assert table.apply_update(update, now=now) is True

    stale = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-c", latency_ms=3.0),),
        advertised_at=now - 10.0,
        ttl_seconds=10.0,
        sequence=1,
    )
    assert table.apply_update(stale, now=now + 1.0) is False


@pytest.mark.asyncio
async def test_link_state_processor_updates_graph() -> None:
    router = GraphBasedFederationRouter()
    table = LinkStateTable(local_cluster="cluster-a")
    processor = LinkStateProcessor(
        local_cluster="cluster-a",
        table=table,
        router=router,
    )

    now = time.time()
    update = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-b", latency_ms=5.0),),
        advertised_at=now,
        ttl_seconds=30.0,
        sequence=1,
    )
    assert await processor.handle_update(update, sender_id="node-a") is True
    assert router.graph.get_edge("cluster-a", "cluster-b") is not None

    remove = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(),
        advertised_at=now + 5.0,
        ttl_seconds=30.0,
        sequence=2,
    )
    assert await processor.handle_update(remove, sender_id="node-a") is True
    assert router.graph.get_edge("cluster-a", "cluster-b") is None


def test_link_state_update_serializes_area() -> None:
    update = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-b", latency_ms=5.0),),
        area="area-1",
        advertised_at=123.0,
        ttl_seconds=30.0,
        sequence=1,
    )
    payload = update.to_dict()
    assert payload["area"] == "area-1"
    parsed = LinkStateUpdate.from_dict(payload)
    assert parsed.area == "area-1"


def test_link_state_table_neighbor_clusters() -> None:
    table = LinkStateTable(local_cluster="cluster-a")
    update = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(
            LinkStateNeighbor(cluster_id="cluster-b", latency_ms=5.0),
            LinkStateNeighbor(cluster_id="cluster-c", latency_ms=7.0),
        ),
        advertised_at=time.time(),
        ttl_seconds=30.0,
        sequence=1,
    )
    assert table.apply_update(update) is True
    assert table.neighbor_clusters("cluster-a") == frozenset({"cluster-b", "cluster-c"})


@pytest.mark.asyncio
async def test_link_state_expiry_removes_edges() -> None:
    router = GraphBasedFederationRouter()
    table = LinkStateTable(local_cluster="cluster-a")
    processor = LinkStateProcessor(
        local_cluster="cluster-a",
        table=table,
        router=router,
    )
    now = time.time()
    update = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-b", latency_ms=5.0),),
        advertised_at=now,
        ttl_seconds=5.0,
        sequence=1,
    )
    assert await processor.handle_update(update, sender_id="node-a") is True
    assert router.graph.get_edge("cluster-a", "cluster-b") is not None

    removed = processor.prune_expired(now=now + 6.0)
    assert removed == 1
    assert router.graph.get_edge("cluster-a", "cluster-b") is None


@pytest.mark.asyncio
async def test_link_state_processor_filters_area() -> None:
    router = GraphBasedFederationRouter()
    table = LinkStateTable(local_cluster="cluster-a")
    processor = LinkStateProcessor(
        local_cluster="cluster-a",
        table=table,
        router=router,
        allowed_areas=frozenset({"area-1"}),
    )
    now = time.time()
    update = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-b", latency_ms=5.0),),
        area="area-2",
        advertised_at=now,
        ttl_seconds=30.0,
        sequence=1,
    )
    assert await processor.handle_update(update, sender_id="node-a") is False
    assert router.graph.get_edge("cluster-a", "cluster-b") is None

    allowed = LinkStateUpdate(
        origin="cluster-a",
        neighbors=(LinkStateNeighbor(cluster_id="cluster-b", latency_ms=5.0),),
        area="area-1",
        advertised_at=now + 1.0,
        ttl_seconds=30.0,
        sequence=2,
    )
    assert await processor.handle_update(allowed, sender_id="node-a") is True
    assert router.graph.get_edge("cluster-a", "cluster-b") is not None


@pytest.mark.asyncio
async def test_link_state_announcer_multi_area_updates() -> None:
    publisher = _StubPublisher()
    area_policy = LinkStateAreaPolicy(
        local_areas=("area-a", "area-b"),
        neighbor_areas={
            "cluster-b": ("area-a",),
            "cluster-c": ("area-b",),
        },
        allow_unmapped_neighbors=False,
    )
    announcer = LinkStateAnnouncer(
        local_cluster="cluster-a",
        neighbor_locator=lambda: [
            PeerNeighbor(cluster_id="cluster-b", node_id="node-b"),
            PeerNeighbor(cluster_id="cluster-c", node_id="node-c"),
        ],
        publisher=publisher,
        area_policy=area_policy,
    )

    await announcer.announce_once(now=time.time())
    assert len(publisher.updates) == 2
    areas = {update.area for update in publisher.updates}
    assert areas == {"area-a", "area-b"}
    by_area = {update.area: update for update in publisher.updates}
    assert [n.cluster_id for n in by_area["area-a"].neighbors] == ["cluster-b"]
    assert [n.cluster_id for n in by_area["area-b"].neighbors] == ["cluster-c"]


@pytest.mark.asyncio
async def test_link_state_announcer_summary_exports() -> None:
    publisher = _StubPublisher()
    area_policy = LinkStateAreaPolicy(
        local_areas=("area-a", "area-b"),
        neighbor_areas={
            "cluster-b": ("area-a",),
            "cluster-c": ("area-b",),
        },
        area_hierarchy={"area-b": "area-a"},
        summary_filters={
            LinkStateAreaPair(
                source_area="area-b",
                target_area="area-a",
            ): LinkStateSummaryFilter(allowed_neighbors=frozenset({"cluster-c"})),
        },
        allow_unmapped_neighbors=False,
    )
    announcer = LinkStateAnnouncer(
        local_cluster="cluster-a",
        neighbor_locator=lambda: [
            PeerNeighbor(cluster_id="cluster-b", node_id="node-b"),
            PeerNeighbor(cluster_id="cluster-c", node_id="node-c"),
        ],
        publisher=publisher,
        area_policy=area_policy,
    )

    await announcer.announce_once(now=time.time())
    by_area = {update.area: update for update in publisher.updates}
    assert [n.cluster_id for n in by_area["area-b"].neighbors] == ["cluster-c"]
    assert [n.cluster_id for n in by_area["area-a"].neighbors] == [
        "cluster-b",
        "cluster-c",
    ]
