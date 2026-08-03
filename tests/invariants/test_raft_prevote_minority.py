"""B5: Minority partition should not thrash terms unboundedly with pre-vote."""

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
async def test_minority_term_growth_bounded_with_prevote() -> None:
    network = MockNetwork()
    members = {"a", "b", "c", "d", "e"}
    nodes: dict[str, ProductionRaft] = {}
    cfg = RaftConfiguration(
        election_timeout_min=0.15,
        election_timeout_max=0.30,
        heartbeat_interval=0.025,
        pre_vote_enabled=True,
    )
    for nid in members:
        n = ProductionRaft(
            node_id=nid,
            cluster_members=members,
            storage=RaftStorageFactory.create_memory_storage(nid),
            transport=NetworkAwareTransport(nid, network),
            state_machine=TestableStateMachine(),
            config=cfg,
        )
        network.register_node(nid, n)
        nodes[nid] = n
        await n.start()

    oracle = RaftOracle()
    try:
        # Elect under full connectivity
        for _ in range(80):
            if any(n.current_state == RaftState.LEADER for n in nodes.values()):
                break
            await asyncio.sleep(0.05)
        leaders = [n for n in nodes.values() if n.current_state == RaftState.LEADER]
        assert leaders
        term_before = max(n.persistent_state.current_term for n in nodes.values())

        # Isolate minority of 2
        network.create_partition({"a", "b"}, {"c", "d", "e"})
        await asyncio.sleep(1.0)

        terms_after = {nid: n.persistent_state.current_term for nid, n in nodes.items()}
        # Minority terms should not explode (allow modest growth)
        minority_growth = max(terms_after["a"], terms_after["b"]) - term_before
        assert minority_growth < 25, f"unbounded term thrash: {terms_after}"

        for n in nodes.values():
            if n.current_state == RaftState.LEADER:
                oracle.observe_role(
                    n.node_id, n.persistent_state.current_term, "leader"
                )
        oracle.assert_safe()
    finally:
        for n in nodes.values():
            await n.stop()
