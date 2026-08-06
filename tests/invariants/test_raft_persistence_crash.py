"""B6: Crash mid-replicate recovers committed prefix from storage."""

from __future__ import annotations

import asyncio
import contextlib

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

def _cfg() -> RaftConfiguration:
    return RaftConfiguration(
        election_timeout_min=0.15,
        election_timeout_max=0.30,
        heartbeat_interval=0.025,
    )

@pytest.mark.asyncio
async def test_restart_recovers_committed_entries() -> None:
    network = MockNetwork()
    members = {"n0", "n1", "n2"}
    storages = {nid: RaftStorageFactory.create_memory_storage(nid) for nid in members}
    sms = {nid: TestableStateMachine() for nid in members}

    def build(nid: str) -> ProductionRaft:
        return ProductionRaft(
            node_id=nid,
            cluster_members=members,
            storage=storages[nid],
            transport=NetworkAwareTransport(nid, network),
            state_machine=sms[nid],
            config=_cfg(),
        )

    nodes = {nid: build(nid) for nid in members}
    for nid, n in nodes.items():
        network.register_node(nid, n)
        await n.start()

    try:
        leader = None
        for _ in range(100):
            for n in nodes.values():
                if n.current_state == RaftState.LEADER:
                    leader = n
                    break
            if leader:
                break
            await asyncio.sleep(0.05)
        assert leader is not None
        assert await leader.submit_command("persist=42") is not None
        commit = leader.volatile_state.commit_index
        assert commit >= 1

        # Crash all nodes (stop without clearing storage)
        for n in nodes.values():
            await n.stop()

        # Restart with same storages, fresh SM for followers
        network2 = MockNetwork()
        sms2 = {nid: TestableStateMachine() for nid in members}
        nodes2 = {}
        for nid in members:
            n = ProductionRaft(
                node_id=nid,
                cluster_members=members,
                storage=storages[nid],
                transport=NetworkAwareTransport(nid, network2),
                state_machine=sms2[nid],
                config=_cfg(),
            )
            network2.register_node(nid, n)
            nodes2[nid] = n
            await n.start()

        # Wait for leader and committed prefix visibility
        leader2 = None
        for _ in range(100):
            for n in nodes2.values():
                if n.current_state == RaftState.LEADER:
                    leader2 = n
                    break
            if leader2:
                break
            await asyncio.sleep(0.05)
        assert leader2 is not None
        # Persistent log should still contain committed index
        assert any(len(n.persistent_state.log_entries) >= 1 for n in nodes2.values())
        assert leader2.volatile_state.commit_index >= 1 or any(
            e.command == "persist=42"
            or (isinstance(e.command, str) and "persist" in e.command)
            for n in nodes2.values()
            for e in n.persistent_state.log_entries
        )
        # COR-T10-11 / INV-C8: majority of restarted nodes re-apply committed
        # command into the state machine (not only log presence).
        for _ in range(80):
            applied = sum(
                1
                for n in nodes2.values()
                if getattr(n.state_machine, "state", None)
                and (
                    n.state_machine.state.get("persist") == 42
                    or "persist=42" in str(n.state_machine.state)
                    or any(
                        "persist" in str(v)
                        for v in getattr(n.state_machine, "state", {}).values()
                    )
                )
            )
            if applied >= 2:  # majority of 3
                break
            # also accept last_applied catching up with commit on majority
            applied_idx = sum(
                1 for n in nodes2.values() if n.volatile_state.last_applied >= 1
            )
            if applied_idx >= 2:
                applied = applied_idx
                break
            await asyncio.sleep(0.05)
        assert applied >= 2, (
            f"INV-C8: expected majority SM/last_applied recovery, got {applied}"
        )
    finally:
        for n in list(nodes.values()):
            with contextlib.suppress(Exception):
                await n.stop()
        if "nodes2" in dir():
            for n in nodes2.values():
                with contextlib.suppress(Exception):
                    await n.stop()
