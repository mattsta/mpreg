"""INV-C3 smoke: sequential register applies on leader SM (not Jepsen-class).

COR-12/A12: claim wording matches leader-focused smoke; poll helper reduces
sleep churn vs fixed long sleeps (not a real-time barrier).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from mpreg.datastructures.production_raft import RaftState
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from tests.test_production_raft_integration import (
    MockNetwork,
    NetworkAwareTransport,
    TestableStateMachine,
)

async def _poll_until(
    predicate: Callable[[], bool],
    *,
    timeout_s: float = 5.0,
    interval_s: float = 0.02,
) -> bool:
    """COR-16: tighter poll loop than fixed 50–100 × 50ms sleeps."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval_s)
    return predicate()

@pytest.mark.asyncio
async def test_sequential_writes_match_state_machine() -> None:
    network = MockNetwork()
    members = {"a", "b", "c"}
    nodes: list[ProductionRaft] = []
    sms: dict[str, TestableStateMachine] = {}
    for nid in members:
        sm = TestableStateMachine()
        sms[nid] = sm
        storage = RaftStorageFactory.create_memory_storage(nid)
        transport = NetworkAwareTransport(nid, network)
        node = ProductionRaft(
            node_id=nid,
            cluster_members=members,
            storage=storage,
            transport=transport,
            state_machine=sm,
            config=RaftConfiguration(
                election_timeout_min=0.15,
                election_timeout_max=0.30,
                heartbeat_interval=0.025,
            ),
        )
        network.register_node(nid, node)
        nodes.append(node)
        await node.start()

    try:
        leader_box: list[ProductionRaft | None] = [None]

        def _has_leader() -> bool:
            for n in nodes:
                if n.current_state == RaftState.LEADER:
                    leader_box[0] = n
                    return True
            return False

        assert await _poll_until(_has_leader, timeout_s=5.0)
        leader = leader_box[0]
        assert leader is not None

        expected: dict[str, int] = {}
        for i in range(5):
            cmd = f"k{i}={i * 10}"
            result = await leader.submit_command(cmd, client_id="client")
            assert result is not None
            expected[f"k{i}"] = i * 10

        def _all_applied() -> bool:
            return all(
                all(sm.state.get(k) == v for k, v in expected.items())
                for sm in sms.values()
            )

        await _poll_until(_all_applied, timeout_s=3.0)

        # Leader SM must match (INV-C3 smoke strength — not majority under fault).
        leader_sm = sms[leader.node_id]
        for k, v in expected.items():
            assert leader_sm.state.get(k) == v
    finally:
        for n in nodes:
            await n.stop()
