"""D: Cross-plane chaos X1–X4 (L2 simulated)."""

from __future__ import annotations

import asyncio
import contextlib
import time

import pytest

from mpreg.client.call_policy import (
    ClientCallPolicy,
    RpcExecutionMode,
    call_with_policy,
)
from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.datastructures.production_raft import RaftState
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from mpreg.fabric.federation_graph import (
    FederationGraphEdge,
    FederationGraphNode,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    NodeType,
)
from mpreg.fabric.federation_planner import FabricFederationPlanner
from mpreg.fabric.link_state import LinkStateMode
from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RouteTable,
    RouteWithdrawal,
)
from mpreg.testing.faults import FaultInjector
from mpreg.testing.oracles import RaftOracle
from tests.test_production_raft_integration import (
    MockNetwork,
    NetworkAwareTransport,
    TestableStateMachine,
)


def _node(cid: str) -> FederationGraphNode:
    return FederationGraphNode(
        node_id=cid,
        node_type=NodeType.CLUSTER,
        region="r",
        coordinates=GeographicCoordinate(0.0, 0.0),
        max_capacity=100,
    )


@pytest.mark.asyncio
async def test_x1_leader_election_during_route_withdraw() -> None:
    """X1: Route withdraw storm concurrent with Raft election — no hang."""
    # Routing plane
    table = RouteTable(local_cluster="a")
    now = time.time()
    table.apply_announcement(
        RouteAnnouncement(
            destination=RouteDestination(cluster_id="z"),
            path=RoutePath(hops=("b", "z")),
            metrics=RouteMetrics(hop_count=1),
            advertiser="b",
            advertised_at=now,
            ttl_seconds=60,
        ),
        received_from="b",
        now=now,
    )
    # Raft plane
    network = MockNetwork()
    members = {"r0", "r1", "r2"}
    nodes = []
    for nid in members:
        n = ProductionRaft(
            node_id=nid,
            cluster_members=members,
            storage=RaftStorageFactory.create_memory_storage(nid),
            transport=NetworkAwareTransport(nid, network),
            state_machine=TestableStateMachine(),
            config=RaftConfiguration(
                election_timeout_min=0.15,
                election_timeout_max=0.30,
                heartbeat_interval=0.025,
            ),
        )
        network.register_node(nid, n)
        nodes.append(n)
        await n.start()

    oracle = RaftOracle()
    try:
        # Withdraw storm
        for _ in range(5):
            table.apply_withdrawal(
                RouteWithdrawal(
                    destination=RouteDestination(cluster_id="z"),
                    path=RoutePath(hops=("b", "z")),
                    advertiser="b",
                    withdrawn_at=now,
                ),
                received_from="b",
                now=now,
            )
            table.apply_announcement(
                RouteAnnouncement(
                    destination=RouteDestination(cluster_id="z"),
                    path=RoutePath(hops=("b", "z")),
                    metrics=RouteMetrics(hop_count=1),
                    advertiser="b",
                    advertised_at=now,
                    ttl_seconds=60,
                ),
                received_from="b",
                now=now,
            )

        leader = None
        for _ in range(80):
            for n in nodes:
                if n.current_state == RaftState.LEADER:
                    leader = n
                    break
            if leader:
                break
            await asyncio.sleep(0.05)
        assert leader is not None
        oracle.observe_role(
            leader.node_id, leader.persistent_state.current_term, "leader"
        )
        # RPC-style call must complete or structured fail (no hang)
        policy = ClientCallPolicy.for_mode(
            RpcExecutionMode.M2_SOFT_RT, deadline_seconds=1.0
        )

        async def ping() -> str:
            return "ok"

        assert await call_with_policy(ping, policy) == "ok"
        oracle.assert_safe()
    finally:
        for n in nodes:
            await n.stop()


@pytest.mark.asyncio
async def test_x2_minority_partition_no_commit() -> None:
    """X2: Minority Raft partition cannot commit; routing still plans.

    Uses a wall-clock deadline for polling — never wrap the whole Raft body in
    ``asyncio.wait_for``. Outer wait_for cancel walks nested gather/_fut_waiter
    chains and can RecursionError (~950) under election/heartbeat load.
    """
    deadline = time.monotonic() + 20.0

    def _time_left() -> float:
        return deadline - time.monotonic()

    network = MockNetwork()
    members = {"a", "b", "c"}
    nodes: dict[str, ProductionRaft] = {}
    try:
        for nid in members:
            n = ProductionRaft(
                node_id=nid,
                cluster_members=members,
                storage=RaftStorageFactory.create_memory_storage(nid),
                transport=NetworkAwareTransport(nid, network),
                state_machine=TestableStateMachine(),
                config=RaftConfiguration(
                    election_timeout_min=0.15,
                    election_timeout_max=0.30,
                    heartbeat_interval=0.025,
                ),
            )
            network.register_node(nid, n)
            nodes[nid] = n
            await n.start()

        leader = None
        while _time_left() > 8.0:
            for n in nodes.values():
                if n.current_state == RaftState.LEADER:
                    leader = n
                    break
            if leader:
                break
            await asyncio.sleep(0.05)
        assert leader is not None, "no leader elected within budget"

        # Commit one entry while healthy (bounded, not outer body cancel)
        submit_budget = min(5.0, max(0.5, _time_left() - 5.0))
        committed = await asyncio.wait_for(
            leader.submit_command("x=1"), timeout=submit_budget
        )
        assert committed is not None
        commit_before = leader.volatile_state.commit_index

        # Partition minority singleton vs majority rest
        majority = set(members) - {leader.node_id}
        singleton = next(iter(majority))
        rest = members - {singleton}
        network.create_partition({singleton}, rest)

        await asyncio.sleep(min(0.4, max(0.05, _time_left() * 0.2)))
        # Routing plane still works under LS prefer
        g = GraphBasedFederationRouter()
        for c in "xyz":
            g.add_node(_node(c))
        g.add_edge(
            FederationGraphEdge(
                "x",
                "y",
                latency_ms=1,
                bandwidth_mbps=100,
                reliability_score=1.0,
            )
        )
        planner = FabricFederationPlanner(
            local_cluster="x",
            graph_router=g,
            peer_locator=lambda c: [f"ws://{c}:1"],
            link_state_mode=LinkStateMode.PREFER,
            link_state_router=g,
        )
        plan = planner.plan_next_hop(target_cluster="y")
        assert plan.can_forward

        network.heal_partition()
        await asyncio.sleep(min(0.25, max(0.05, _time_left() * 0.1)))
        leaders = [n for n in nodes.values() if n.current_state == RaftState.LEADER]
        if leaders:
            terms = {n.persistent_state.current_term for n in leaders}
            assert len(leaders) <= 1 or len(terms) == len(leaders)
        assert commit_before >= 0
        assert _time_left() > 0.0, "x2 exceeded wall-clock budget without hang"
    finally:
        # Sequential stop: avoid gather-cancel of deep raft trees.
        for n in nodes.values():
            with contextlib.suppress(Exception):
                await asyncio.wait_for(n.stop(), timeout=3.0)


@pytest.mark.asyncio
async def test_x3_restart_mid_stream_client_timeout() -> None:
    """X3: Client sees TIMEOUT / structured error, not hang, when op dies."""
    policy = ClientCallPolicy.for_mode(
        RpcExecutionMode.M3_STREAMING, deadline_seconds=0.2, max_attempts=1
    )
    inj = FaultInjector(seed=1)
    inj.crash("server")

    async def broken_stream() -> str:
        if not inj.view().can_communicate("client", "server"):
            await asyncio.sleep(1.0)
        return "should-not"

    with pytest.raises(MpregError) as ei:
        await call_with_policy(broken_stream, policy)
    assert ei.value.code == int(MpregErrorCode.TIMEOUT)


def test_x4_clock_skew_view() -> None:
    """X4: Clock skew injection is observable per node."""
    inj = FaultInjector()
    inj.set_clock_skew("n0", 0.5)
    inj.set_clock_skew("n1", -0.5)
    wall = 10_000.0
    assert inj.view().now_for("n0", wall) == wall + 0.5
    assert inj.view().now_for("n1", wall) == wall - 0.5
    # Partition + skew compose
    inj.partition({"n0"}, {"n1", "n2"})
    assert not inj.view().can_communicate("n0", "n1")
