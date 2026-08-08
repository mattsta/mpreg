"""B5: Minority partition must not thrash terms unboundedly with pre-vote.

Pre-vote probes peers *before* incrementing term. Isolated minority nodes
receive pre-vote rejections from the majority (which still has leader contact
or higher term visibility), so minority ``current_term`` growth stays bounded
even when election backoff is modest.
"""

from __future__ import annotations

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
        election_timeout_min=0.12,
        election_timeout_max=0.24,
        heartbeat_interval=0.02,
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
        leader = await ProductionRaft.wait_for_leader(nodes, timeout_seconds=8.0)
        term_before = max(n.persistent_state.current_term for n in nodes.values())
        assert leader.current_state == RaftState.LEADER

        # Isolate minority of 2 — majority keeps a leader and heartbeats.
        network.create_partition({"a", "b"}, {"c", "d", "e"})
        await ProductionRaft.wait_for_leader(
            {k: nodes[k] for k in ("c", "d", "e")},
            timeout_seconds=8.0,
        )

        # Let minority thrash pre-votes for a while without majority votes.
        import asyncio

        await asyncio.sleep(1.2)

        terms_after = {nid: n.persistent_state.current_term for nid, n in nodes.items()}
        minority_growth = max(terms_after["a"], terms_after["b"]) - term_before
        # With real pre-vote, minority should barely increment term (failed pre-votes
        # do not campaign). Allow a small slack for races at partition cut.
        assert minority_growth < 8, (
            f"unbounded term thrash under pre-vote: before={term_before} after={terms_after} "
            f"pre_votes={[ (n.node_id, n.metrics.pre_votes_started, n.metrics.pre_votes_failed, n.metrics.pre_votes_passed) for n in nodes.values() ]}"
        )

        # Majority terms should not explode either
        majority_growth = max(terms_after[k] for k in ("c", "d", "e")) - term_before
        assert majority_growth < 15, f"majority term explosion: {terms_after}"

        # At least one minority node should have attempted pre-vote
        minority_pre = sum(
            nodes[k].metrics.pre_votes_started for k in ("a", "b")
        )
        assert minority_pre >= 1, "expected minority pre-vote attempts"

        for n in nodes.values():
            if n.current_state == RaftState.LEADER:
                oracle.observe_role(
                    n.node_id, n.persistent_state.current_term, "leader"
                )
        oracle.assert_safe()
    finally:
        for n in nodes.values():
            await n.stop()


@pytest.mark.asyncio
async def test_prevote_disabled_still_elects() -> None:
    """Legacy path: pre_vote_enabled=False still elects a leader."""
    network = MockNetwork()
    members = {"a", "b", "c"}
    nodes: dict[str, ProductionRaft] = {}
    cfg = RaftConfiguration(
        election_timeout_min=0.12,
        election_timeout_max=0.24,
        heartbeat_interval=0.02,
        pre_vote_enabled=False,
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
    try:
        leader = await ProductionRaft.wait_for_leader(nodes, timeout_seconds=8.0)
        assert leader.current_state == RaftState.LEADER
        assert all(n.metrics.pre_votes_started == 0 for n in nodes.values())
    finally:
        for n in nodes.values():
            await n.stop()
