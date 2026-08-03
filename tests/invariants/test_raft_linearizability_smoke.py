"""B7: Sequential register linearizability smoke on single leader."""

from __future__ import annotations

import asyncio

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
        leader = None
        for _ in range(100):
            for n in nodes:
                if n.current_state == RaftState.LEADER:
                    leader = n
                    break
            if leader:
                break
            await asyncio.sleep(0.05)
        assert leader is not None

        expected: dict[str, int] = {}
        for i in range(5):
            cmd = f"k{i}={i * 10}"
            result = await leader.submit_command(cmd, client_id="client")
            assert result is not None
            expected[f"k{i}"] = i * 10

        # Wait for followers to apply
        for _ in range(50):
            if all(
                all(sm.state.get(k) == v for k, v in expected.items())
                for sm in sms.values()
            ):
                break
            await asyncio.sleep(0.05)

        # At least leader SM matches
        leader_sm = sms[leader.node_id]
        for k, v in expected.items():
            assert leader_sm.state.get(k) == v
    finally:
        for n in nodes:
            await n.stop()
