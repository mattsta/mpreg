"""B0/B1: Basic Raft role transitions and oracle observations (INV-C1, C2)."""

from __future__ import annotations

import asyncio

import pytest

from mpreg.datastructures.production_raft import RaftState
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from mpreg.testing.oracles import RaftOracle
from tests.test_production_raft_integration import (
    MockNetwork,
    NetworkAwareTransport,
    TestableStateMachine,
)

@pytest.mark.asyncio
async def test_single_node_becomes_leader() -> None:
    network = MockNetwork()
    storage = RaftStorageFactory.create_memory_storage("solo")
    transport = NetworkAwareTransport("solo", network)
    node = ProductionRaft(
        node_id="solo",
        cluster_members={"solo"},
        storage=storage,
        transport=transport,
        state_machine=TestableStateMachine(),
        config=RaftConfiguration(
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            heartbeat_interval=0.025,
        ),
    )
    network.register_node("solo", node)
    oracle = RaftOracle()
    await node.start()
    try:
        for _ in range(50):
            if node.current_state == RaftState.LEADER:
                break
            await asyncio.sleep(0.05)
        assert node.current_state == RaftState.LEADER
        term = node.persistent_state.current_term
        oracle.observe_role("solo", term, "leader")
        oracle.observe_commit("solo", node.volatile_state.commit_index)
        oracle.assert_safe()
    finally:
        await node.stop()

@pytest.mark.asyncio
async def test_three_node_election_safety() -> None:
    network = MockNetwork()
    nodes: dict[str, ProductionRaft] = {}
    members = {"n0", "n1", "n2"}
    for nid in members:
        storage = RaftStorageFactory.create_memory_storage(nid)
        transport = NetworkAwareTransport(nid, network)
        node = ProductionRaft(
            node_id=nid,
            cluster_members=members,
            storage=storage,
            transport=transport,
            state_machine=TestableStateMachine(),
            config=RaftConfiguration(
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            heartbeat_interval=0.025,
        ),
        )
        network.register_node(nid, node)
        nodes[nid] = node
        await node.start()

    oracle = RaftOracle()
    try:
        leader = None
        for _ in range(80):
            leaders = [
                n for n in nodes.values() if n.current_state == RaftState.LEADER
            ]
            if leaders:
                leader = leaders[0]
                break
            await asyncio.sleep(0.05)
        assert leader is not None
        # Observe all current leaders by term
        for n in nodes.values():
            if n.current_state == RaftState.LEADER:
                oracle.observe_role(
                    n.node_id, n.persistent_state.current_term, "leader"
                )
            oracle.observe_commit(n.node_id, n.volatile_state.commit_index)
        oracle.assert_safe()
    finally:
        for n in nodes.values():
            await n.stop()
