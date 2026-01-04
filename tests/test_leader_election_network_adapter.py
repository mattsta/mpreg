"""
Fabric-backed Raft leader election tests.

These tests validate the ProductionRaft-powered leader election wrapper and
its ability to elect a single leader using the fabric Raft transport.
"""

import asyncio

import pytest

from mpreg.datastructures.leader_election import RaftBasedLeaderElection
from mpreg.datastructures.production_raft_implementation import RaftConfiguration
from mpreg.fabric.raft_transport import (
    FabricRaftTransport,
    FabricRaftTransportConfig,
    FabricRaftTransportHooks,
)


class LoopbackRaftNetwork:
    def __init__(self) -> None:
        self.nodes: dict[str, FabricRaftTransport] = {}
        self.node_clusters: dict[str, str] = {}

    def register(
        self, node_id: str, cluster_id: str, transport: FabricRaftTransport
    ) -> None:
        self.nodes[node_id] = transport
        self.node_clusters[node_id] = cluster_id

    async def send_direct(self, node_id: str, message) -> bool:
        sender = (
            message.headers.routing_path[-1] if message.headers.routing_path else ""
        )
        target = self.nodes.get(node_id)
        if not target:
            return False
        await target.handle_message(message, source_peer_url=sender or None)
        return True

    async def send_to_cluster(
        self, cluster_id: str, message, source_peer_url: str | None
    ) -> bool:
        candidates = [
            node_id for node_id, cid in self.node_clusters.items() if cid == cluster_id
        ]
        if not candidates:
            return False
        target = self.nodes.get(sorted(candidates)[0])
        if not target:
            return False
        await target.handle_message(message, source_peer_url=source_peer_url)
        return True

    def resolve_cluster(self, node_id: str) -> str | None:
        return self.node_clusters.get(node_id)


@pytest.mark.asyncio
async def test_raft_leader_election_single_node() -> None:
    raft = RaftBasedLeaderElection(cluster_id="cluster-a")
    leader = await raft.elect_leader("namespace")
    assert leader == "cluster-a"
    assert await raft.is_leader("namespace") is True
    await raft.shutdown()


@pytest.mark.asyncio
async def test_raft_leader_election_multi_node_fabric_transport() -> None:
    network = LoopbackRaftNetwork()
    cluster_id = "raft-cluster"
    node_ids = ["node-a", "node-b", "node-c"]

    config = RaftConfiguration(
        election_timeout_min=0.15,
        election_timeout_max=0.3,
        heartbeat_interval=0.04,
        rpc_timeout=0.1,
    )

    elections: dict[str, RaftBasedLeaderElection] = {}
    for node_id in node_ids:
        transport = FabricRaftTransport(
            config=FabricRaftTransportConfig(
                node_id=node_id,
                cluster_id=cluster_id,
                request_timeout_seconds=0.25,
            ),
            hooks=FabricRaftTransportHooks(
                send_direct=network.send_direct,
                send_to_cluster=network.send_to_cluster,
                resolve_cluster=network.resolve_cluster,
            ),
        )
        network.register(node_id, cluster_id, transport)
        elections[node_id] = RaftBasedLeaderElection(
            cluster_id=cluster_id,
            node_id=node_id,
            cluster_members=set(node_ids),
            transport=transport,
            config=config,
        )

    await elections["node-a"].elect_leader("namespace")

    leader_id = None
    deadline = asyncio.get_running_loop().time() + 4.0
    while asyncio.get_running_loop().time() < deadline:
        leaders = []
        for node_id, node in elections.items():
            if await node.is_leader("namespace"):
                leaders.append(node_id)
        if len(leaders) == 1:
            leader_id = leaders[0]
            break
        await asyncio.sleep(0.1)

    assert leader_id is not None
    for node in elections.values():
        assert await node.get_current_leader("namespace") == leader_id

    await asyncio.gather(*(node.shutdown() for node in elections.values()))
