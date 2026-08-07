"""A3: Link-state TTL, sequence, area (INV-R4, R5, R6)."""

from __future__ import annotations

import time

import pytest

from mpreg.fabric.federation_graph import GraphBasedFederationRouter
from mpreg.fabric.link_state import (
    LinkStateAreaPair,
    LinkStateAreaPolicy,
    LinkStateNeighbor,
    LinkStateProcessor,
    LinkStateSummaryFilter,
    LinkStateTable,
    LinkStateUpdate,
)
from mpreg.fabric.peer_directory import PeerNeighbor


def test_expired_update_rejected() -> None:
    table = LinkStateTable(local_cluster="a")
    now = 1000.0
    upd = LinkStateUpdate(
        origin="b",
        neighbors=(LinkStateNeighbor(cluster_id="c"),),
        advertised_at=now - 100,
        ttl_seconds=10.0,
        sequence=1,
    )
    assert table.apply_update(upd, now=now) is False


def test_stale_sequence_rejected() -> None:
    table = LinkStateTable(local_cluster="a")
    now = time.time()
    n = (LinkStateNeighbor(cluster_id="c"),)
    assert table.apply_update(
        LinkStateUpdate(origin="b", neighbors=n, advertised_at=now, sequence=5),
        now=now,
    )
    assert (
        table.apply_update(
            LinkStateUpdate(origin="b", neighbors=n, advertised_at=now + 1, sequence=4),
            now=now + 1,
        )
        is False
    )


def test_purge_expired_removes_spf_inputs() -> None:
    table = LinkStateTable(local_cluster="a")
    now = 100.0
    table.apply_update(
        LinkStateUpdate(
            origin="b",
            neighbors=(LinkStateNeighbor(cluster_id="c"),),
            advertised_at=now,
            ttl_seconds=5.0,
            sequence=1,
        ),
        now=now,
    )
    assert table.has_origin("b")
    purged = table.purge_expired(now=now + 10)
    assert purged
    assert not table.has_origin("b")


def test_newer_sequence_accepted() -> None:
    table = LinkStateTable(local_cluster="a")
    now = time.time()
    n1 = (LinkStateNeighbor(cluster_id="c"),)
    n2 = (LinkStateNeighbor(cluster_id="d"),)
    assert table.apply_update(
        LinkStateUpdate(origin="b", neighbors=n1, advertised_at=now, sequence=1),
        now=now,
    )
    assert table.apply_update(
        LinkStateUpdate(origin="b", neighbors=n2, advertised_at=now + 1, sequence=2),
        now=now + 1,
    )
    assert table.neighbor_clusters("b") == frozenset({"d"})


def test_area_policy_local_and_allowed_areas() -> None:
    policy = LinkStateAreaPolicy(
        local_areas=("area-1", "backbone"),
        default_area="area-1",
    )
    assert policy.allowed_areas() == frozenset({"area-1", "backbone"})
    assert policy.is_local_area("area-1")
    assert policy.is_local_area("backbone")
    assert not policy.is_local_area("area-2")
    assert not policy.is_local_area(None)


def test_area_policy_group_neighbors_drops_foreign_areas() -> None:
    policy = LinkStateAreaPolicy(
        local_areas=("area-1",),
        neighbor_areas={
            "peer-local": ("area-1",),
            "peer-foreign": ("area-2",),
        },
        allow_unmapped_neighbors=False,
    )
    peers = [
        PeerNeighbor(cluster_id="peer-local", node_id="n-local"),
        PeerNeighbor(cluster_id="peer-foreign", node_id="n-foreign"),
        PeerNeighbor(cluster_id="peer-unknown", node_id="n-unknown"),
    ]
    grouped = policy.group_neighbors(peers)
    assert set(grouped.keys()) == {"area-1"}
    assert [p.cluster_id for p in grouped["area-1"]] == ["peer-local"]


def test_area_policy_summary_export_filter() -> None:
    policy = LinkStateAreaPolicy(
        local_areas=("area-1", "backbone"),
        area_hierarchy={"area-1": "backbone"},
        summary_filters={
            LinkStateAreaPair(source_area="area-1", target_area="backbone"): (
                LinkStateSummaryFilter(allowed_neighbors=frozenset({"n1", "n2"}))
            )
        },
    )
    neighbors = [
        PeerNeighbor(cluster_id="n1", node_id="node-1"),
        PeerNeighbor(cluster_id="n2", node_id="node-2"),
        PeerNeighbor(cluster_id="n3", node_id="node-3"),
    ]
    summary = policy.summarize_neighbors("area-1", neighbors)
    assert "backbone" in summary
    assert {p.cluster_id for p in summary["backbone"]} == {"n1", "n2"}


@pytest.mark.asyncio
async def test_processor_denies_cross_area_installation() -> None:
    """INV-R6: LinkStateProcessor.allowed_areas rejects foreign-area LS edges."""
    table = LinkStateTable(local_cluster="a")
    router = GraphBasedFederationRouter()
    proc = LinkStateProcessor(
        local_cluster="a",
        table=table,
        router=router,
        allowed_areas=frozenset({"area-1"}),
    )
    now = time.time()
    foreign = LinkStateUpdate(
        origin="b",
        neighbors=(LinkStateNeighbor(cluster_id="c", latency_ms=1.0),),
        advertised_at=now,
        ttl_seconds=30.0,
        sequence=1,
        area="area-2",
    )
    assert await proc.handle_update(foreign, sender_id="peer-b") is False
    assert table.stats.updates_filtered >= 1
    assert not table.has_origin("b")
    assert router.graph.get_node("b") is None

    local = LinkStateUpdate(
        origin="b",
        neighbors=(LinkStateNeighbor(cluster_id="c", latency_ms=1.0),),
        advertised_at=now,
        ttl_seconds=30.0,
        sequence=1,
        area="area-1",
    )
    assert await proc.handle_update(local, sender_id="peer-b") is True
    assert table.has_origin("b")
    assert router.graph.get_node("b") is not None
    assert router.graph.get_node("c") is not None


@pytest.mark.asyncio
async def test_processor_rejects_unscoped_when_areas_configured() -> None:
    table = LinkStateTable(local_cluster="a")
    router = GraphBasedFederationRouter()
    proc = LinkStateProcessor(
        local_cluster="a",
        table=table,
        router=router,
        allowed_areas=frozenset({"area-1"}),
    )
    now = time.time()
    unscoped = LinkStateUpdate(
        origin="b",
        neighbors=(LinkStateNeighbor(cluster_id="c"),),
        advertised_at=now,
        sequence=1,
        area=None,
    )
    assert await proc.handle_update(unscoped, sender_id="peer-b") is False
    assert table.stats.updates_filtered >= 1
