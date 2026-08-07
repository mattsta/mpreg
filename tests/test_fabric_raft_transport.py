import pytest

from mpreg.datastructures.production_raft import (
    AppendEntriesResponse,
    InstallSnapshotResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from mpreg.fabric.raft_transport import (
    FabricRaftTransport,
    FabricRaftTransportConfig,
    FabricRaftTransportHooks,
)


class StubRaftNode:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.vote_requests: list[RequestVoteRequest] = []

    async def handle_request_vote(
        self, request: RequestVoteRequest
    ) -> RequestVoteResponse:
        self.vote_requests.append(request)
        return RequestVoteResponse(
            term=request.term,
            vote_granted=True,
            voter_id=self.node_id,
        )

    async def handle_append_entries(self, request):
        return AppendEntriesResponse(
            term=request.term,
            success=True,
            follower_id=self.node_id,
            match_index=request.prev_log_index,
            conflict_index=-1,
            conflict_term=-1,
        )

    async def handle_install_snapshot(self, request):
        return InstallSnapshotResponse(
            term=request.term,
            success=True,
            follower_id=self.node_id,
        )


class LoopbackRaftNetwork:
    def __init__(self) -> None:
        self.nodes: dict[str, FabricRaftTransport] = {}
        self.node_clusters: dict[str, str] = {}
        self.direct_blocked: set[tuple[str, str]] = set()
        self.cluster_gateways: dict[str, str] = {}

    def register(
        self, node_id: str, cluster_id: str, transport: FabricRaftTransport
    ) -> None:
        self.nodes[node_id] = transport
        self.node_clusters[node_id] = cluster_id

    async def send_direct(self, node_id: str, message) -> bool:
        sender = (
            message.headers.routing_path[-1] if message.headers.routing_path else ""
        )
        if (sender, node_id) in self.direct_blocked:
            return False
        target = self.nodes.get(node_id)
        if not target:
            return False
        await target.handle_message(message, source_peer_url=sender or None)
        return True

    async def send_to_cluster(
        self, cluster_id: str, message, source_peer_url: str | None
    ) -> bool:
        gateway = self.cluster_gateways.get(cluster_id)
        if gateway is None:
            for node_id, cid in self.node_clusters.items():
                if cid == cluster_id:
                    gateway = node_id
                    break
        if gateway is None:
            return False
        target = self.nodes.get(gateway)
        if not target:
            return False
        await target.handle_message(message, source_peer_url=source_peer_url)
        return True

    def resolve_cluster(self, node_id: str) -> str | None:
        return self.node_clusters.get(node_id)


@pytest.mark.asyncio
async def test_fabric_raft_transport_direct_request_vote() -> None:
    network = LoopbackRaftNetwork()
    cluster_id = "cluster-a"

    hooks = FabricRaftTransportHooks(
        send_direct=network.send_direct,
        send_to_cluster=network.send_to_cluster,
        resolve_cluster=network.resolve_cluster,
    )
    transport_a = FabricRaftTransport(
        config=FabricRaftTransportConfig(
            node_id="node-a",
            cluster_id=cluster_id,
        ),
        hooks=hooks,
    )
    transport_b = FabricRaftTransport(
        config=FabricRaftTransportConfig(
            node_id="node-b",
            cluster_id=cluster_id,
        ),
        hooks=hooks,
    )
    network.register("node-a", cluster_id, transport_a)
    network.register("node-b", cluster_id, transport_b)

    raft_b = StubRaftNode("node-b")
    transport_b.register_node(raft_b)

    response = await transport_a.send_request_vote(
        "node-b",
        RequestVoteRequest(
            term=1,
            candidate_id="node-a",
            last_log_index=0,
            last_log_term=0,
        ),
    )

    assert response is not None
    assert response.vote_granted is True
    assert raft_b.vote_requests


@pytest.mark.asyncio
async def test_fabric_raft_transport_forwards_to_target() -> None:
    network = LoopbackRaftNetwork()
    cluster_id = "cluster-a"
    network.cluster_gateways[cluster_id] = "node-c"
    network.direct_blocked.add(("node-a", "node-b"))

    hooks = FabricRaftTransportHooks(
        send_direct=network.send_direct,
        send_to_cluster=network.send_to_cluster,
        resolve_cluster=network.resolve_cluster,
    )
    transport_a = FabricRaftTransport(
        config=FabricRaftTransportConfig(
            node_id="node-a",
            cluster_id=cluster_id,
        ),
        hooks=hooks,
    )
    transport_b = FabricRaftTransport(
        config=FabricRaftTransportConfig(
            node_id="node-b",
            cluster_id=cluster_id,
        ),
        hooks=hooks,
    )
    transport_c = FabricRaftTransport(
        config=FabricRaftTransportConfig(
            node_id="node-c",
            cluster_id=cluster_id,
        ),
        hooks=hooks,
    )

    network.register("node-a", cluster_id, transport_a)
    network.register("node-b", cluster_id, transport_b)
    network.register("node-c", cluster_id, transport_c)

    raft_b = StubRaftNode("node-b")
    transport_b.register_node(raft_b)

    response = await transport_a.send_request_vote(
        "node-b",
        RequestVoteRequest(
            term=2,
            candidate_id="node-a",
            last_log_index=1,
            last_log_term=1,
        ),
    )

    assert response is not None
    assert response.vote_granted is True
    assert raft_b.vote_requests
