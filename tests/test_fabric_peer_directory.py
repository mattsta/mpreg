import time

from mpreg.fabric.catalog import NodeDescriptor, NodeKey
from mpreg.fabric.catalog_delta import RoutingCatalogDelta
from mpreg.fabric.peer_directory import PeerDirectory


def test_peer_directory_adds_nodes() -> None:
    directory = PeerDirectory(local_node_id="node-a", local_cluster_id="cluster-a")
    now = time.time()
    delta = RoutingCatalogDelta(
        update_id="u1",
        cluster_id="cluster-a",
        sent_at=now,
        nodes=(
            NodeDescriptor(
                node_id="node-a",
                cluster_id="cluster-a",
                resources=frozenset({"cpu"}),
                capabilities=frozenset(),
                advertised_at=now,
                ttl_seconds=30.0,
            ),
            NodeDescriptor(
                node_id="node-b",
                cluster_id="cluster-a",
                resources=frozenset({"gpu"}),
                capabilities=frozenset(),
                advertised_at=now,
                ttl_seconds=30.0,
            ),
        ),
    )

    result = directory.apply_delta(delta, now=now)
    assert result.nodes_added == 2
    assert directory.cluster_id_for_node("node-b") == "cluster-a"
    assert directory.peers_for_cluster("cluster-a") == ["node-b"]


def test_peer_directory_connected_filtering() -> None:
    directory = PeerDirectory(local_node_id="node-a", local_cluster_id="cluster-a")
    now = time.time()
    delta = RoutingCatalogDelta(
        update_id="u2",
        cluster_id="cluster-a",
        sent_at=now,
        nodes=(
            NodeDescriptor(
                node_id="node-a",
                cluster_id="cluster-a",
                advertised_at=now,
                ttl_seconds=30.0,
            ),
            NodeDescriptor(
                node_id="node-b",
                cluster_id="cluster-a",
                advertised_at=now,
                ttl_seconds=30.0,
            ),
            NodeDescriptor(
                node_id="node-c",
                cluster_id="cluster-a",
                advertised_at=now,
                ttl_seconds=30.0,
            ),
        ),
    )

    directory.apply_delta(delta, now=now)
    directory.mark_connected("node-b")
    assert directory.peers_for_cluster("cluster-a") == ["node-b", "node-c"]
    assert directory.peers_for_cluster("cluster-a", connected_only=True) == ["node-b"]


def test_peer_directory_removals_update_indexes() -> None:
    directory = PeerDirectory(local_node_id="node-a", local_cluster_id="cluster-a")
    now = time.time()
    node = NodeDescriptor(
        node_id="node-b",
        cluster_id="cluster-a",
        advertised_at=now,
        ttl_seconds=30.0,
    )
    delta = RoutingCatalogDelta(
        update_id="u3",
        cluster_id="cluster-a",
        sent_at=now,
        nodes=(node,),
    )
    directory.apply_delta(delta, now=now)

    removal = RoutingCatalogDelta(
        update_id="u4",
        cluster_id="cluster-a",
        sent_at=now,
        node_removals=(NodeKey(cluster_id="cluster-a", node_id="node-b"),),
    )
    result = directory.apply_delta(removal, now=now)
    assert result.nodes_removed == 1
    assert directory.cluster_id_for_node("node-b") is None
    assert directory.peers_for_cluster("cluster-a") == []


def test_peer_directory_ignores_expired_nodes() -> None:
    directory = PeerDirectory(local_node_id="node-a", local_cluster_id="cluster-a")
    now = time.time()
    expired = NodeDescriptor(
        node_id="node-b",
        cluster_id="cluster-a",
        advertised_at=now - 10.0,
        ttl_seconds=1.0,
    )
    delta = RoutingCatalogDelta(
        update_id="u5",
        cluster_id="cluster-a",
        sent_at=now,
        nodes=(expired,),
    )
    result = directory.apply_delta(delta, now=now)
    assert result.nodes_added == 0
    assert result.nodes_ignored == 1
    assert directory.peers_for_cluster("cluster-a") == []
