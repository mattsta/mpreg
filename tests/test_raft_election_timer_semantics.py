"""Regression: leader-contact time must not be fabricated by election cadence.

``last_heartbeat_time`` is *leader contact* (AppendEntries / InstallSnapshot /
granted vote / become_leader). Starting or losing an election must not stamp it,
or the next coordinator tick treats a failed election as fresh leader contact
and suppresses retries (split-vote livelock under load).
"""

from __future__ import annotations

import asyncio
import time

import pytest

from mpreg.datastructures.production_raft import RaftState
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
    RaftLeadershipError,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from tests.test_production_raft_integration import (
    MockNetwork,
    NetworkAwareTransport,
    TestableStateMachine,
)


def _make_cluster(
    size: int,
    *,
    election_min: float,
    election_max: float,
    heartbeat: float,
) -> tuple[dict[str, ProductionRaft], MockNetwork]:
    members = {f"n{i}" for i in range(size)}
    network = MockNetwork()
    config = RaftConfiguration(
        election_timeout_min=election_min,
        election_timeout_max=election_max,
        heartbeat_interval=heartbeat,
        rpc_timeout=min(0.1, election_min),
    )
    nodes: dict[str, ProductionRaft] = {}
    for nid in members:
        node = ProductionRaft(
            node_id=nid,
            cluster_members=members,
            storage=RaftStorageFactory.create_memory_storage(f"mem_{nid}"),
            transport=NetworkAwareTransport(nid, network),
            state_machine=TestableStateMachine(),
            config=config,
        )
        node.election_coordinator.timeout_bias_seconds = 0.0
        nodes[nid] = node
        network.register_node(nid, node)
    return nodes, network


@pytest.mark.asyncio
async def test_lost_election_does_not_fabricate_leader_contact():
    """A candidate that cannot reach peers must not stamp leader-contact time."""
    nodes, network = _make_cluster(
        3, election_min=0.10, election_max=0.16, heartbeat=0.025
    )
    # Fully isolate n0 so RequestVote cannot succeed and become_leader cannot run.
    network.can_communicate = lambda a, b: False  # type: ignore[method-assign]
    n0 = nodes["n0"]
    try:
        await n0.start()
        assert n0.last_heartbeat_time == 0.0
        await n0._start_election()
        assert n0.current_state != RaftState.LEADER
        assert n0.last_heartbeat_time == 0.0, (
            "lost election must not stamp leader-contact time "
            f"(now {n0.last_heartbeat_time})"
        )
        # With pre-vote, an isolated node fails the probe *before* term++ and
        # never enters CANDIDATE — still must not fabricate contact.
        attempted = (
            n0.metrics.elections_started
            + n0.metrics.pre_votes_started
            + n0.metrics.elections_lost
            + n0.metrics.pre_votes_failed
        )
        assert attempted >= 1
        assert n0.metrics.elections_won == 0
        assert n0.persistent_state.current_term == 0 or n0.metrics.elections_started >= 1
    finally:
        for n in nodes.values():
            await n.stop()


@pytest.mark.asyncio
async def test_become_leader_records_leader_contact():
    """Winning an election legitimately notes contact via become_leader."""
    nodes, _ = _make_cluster(
        3, election_min=0.10, election_max=0.18, heartbeat=0.025
    )
    try:
        for n in nodes.values():
            await n.start()
        leader = await ProductionRaft.wait_for_leader(nodes)
        assert leader.last_heartbeat_time > 0.0
        status = leader.get_status()
        assert status.state == "leader"
        assert status.time_since_leader_contact is not None
    finally:
        for n in nodes.values():
            await n.stop()


@pytest.mark.asyncio
async def test_failed_election_retries_after_partition_heals():
    """After a minority partition, healing must still yield a unique leader."""
    nodes, network = _make_cluster(
        3, election_min=0.10, election_max=0.16, heartbeat=0.025
    )
    try:
        network.create_partition({"n0"}, {"n1", "n2"})
        for n in nodes.values():
            await n.start()

        # Let the minority side burn attempts without majority.
        await asyncio.sleep(0.35)
        n0 = nodes["n0"]
        assert n0.current_state != RaftState.LEADER

        network.heal_partition()
        leader = await ProductionRaft.wait_for_leader(
            nodes,
            timeout_seconds=ProductionRaft.leadership_deadline_for_nodes(
                nodes, rounds=8.0
            ),
        )
        assert leader.current_state == RaftState.LEADER

        # Every live follower should observe the leader (contact or self).
        deadline = time.time() + leader.leadership_deadline_seconds(rounds=4.0)
        while time.time() < deadline:
            statuses = [n.get_status() for n in nodes.values()]
            ok = all(
                s.state == "leader"
                or s.current_leader == leader.node_id
                or (s.time_since_leader_contact is not None)
                for s in statuses
            )
            if ok:
                break
            await asyncio.sleep(0.05)
        else:
            detail = [s.to_dict() for s in (n.get_status() for n in nodes.values())]
            raise AssertionError(f"cluster did not converge on leader contact: {detail}")
    finally:
        for n in nodes.values():
            await n.stop()


@pytest.mark.asyncio
async def test_wait_for_leader_reports_status_on_timeout():
    """Readiness API must fail closed with actionable node statuses."""
    nodes, network = _make_cluster(
        3, election_min=0.15, election_max=0.25, heartbeat=0.04
    )
    try:
        network.can_communicate = lambda a, b: False  # type: ignore[method-assign]
        for n in nodes.values():
            await n.start()

        with pytest.raises(RaftLeadershipError) as ei:
            await ProductionRaft.wait_for_leader(nodes, timeout_seconds=0.4)
        err = ei.value
        assert err.statuses, "expected per-node statuses on leadership timeout"
        assert all(s.state != "leader" for s in err.statuses)
        assert "final=" in str(err)
    finally:
        for n in nodes.values():
            await n.stop()


@pytest.mark.asyncio
async def test_cluster_elects_leader_under_tight_timeouts():
    """Tight timeout bands still elect a unique leader via production wait API."""
    nodes, _ = _make_cluster(
        3, election_min=0.08, election_max=0.14, heartbeat=0.02
    )
    try:
        for n in nodes.values():
            await n.start()
        leader = await ProductionRaft.wait_for_leader(nodes)
        assert leader.current_state == RaftState.LEADER
        leaders = [n for n in nodes.values() if n.current_state == RaftState.LEADER]
        assert len(leaders) == 1
    finally:
        for n in nodes.values():
            await n.stop()
