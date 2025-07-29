#!/usr/bin/env python3
"""
Debug script to trace commit_index updates in detail.
"""

import asyncio
import logging
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/Users/matt/repos/mpreg")

from mpreg.datastructures.production_raft_implementation import RaftState
from tests.test_production_raft_integration import (
    MockNetwork,
    TestProductionRaftIntegration,
)

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)


# Simple debugging without network interception


async def debug_commit_index_detailed():
    """Debug commit_index propagation with extreme detail."""

    print("=== DETAILED COMMIT INDEX DEBUG ===")

    test_instance = TestProductionRaftIntegration()

    with tempfile.TemporaryDirectory() as temp_dir:
        network = MockNetwork()
        nodes = test_instance.create_raft_cluster(
            3, Path(temp_dir), network, storage_type="memory"
        )

        def print_all_states(label: str):
            print(f"\n🏷️  {label}")
            for node_id, node in nodes.items():
                print(
                    f"   {node_id} ({node.current_state.value}): commit_index={node.volatile_state.commit_index}, last_applied={node.volatile_state.last_applied}, log_entries={len(node.persistent_state.log_entries)}"
                )
                if (
                    node.current_state == RaftState.LEADER
                    and node.leader_volatile_state
                ):
                    print(
                        f"     Leader match_index: {dict(node.leader_volatile_state.match_index)}"
                    )

        try:
            # Start all nodes
            for node in nodes.values():
                await node.start()

            # Wait for leader election
            leader = None
            followers = []
            for attempt in range(40):
                await asyncio.sleep(0.1)

                leaders = [
                    n for n in nodes.values() if n.current_state == RaftState.LEADER
                ]
                if leaders:
                    leader = leaders[0]
                    followers = [
                        n
                        for n in nodes.values()
                        if n.current_state == RaftState.FOLLOWER
                    ]
                    break

            if not leader:
                print("❌ No leader elected!")
                return

            print(f"✅ Leader: {leader.node_id}")
            print(f"✅ Followers: {[f.node_id for f in followers]}")

            print_all_states("AFTER LEADER ELECTION")

            # Submit command and trace what happens
            print(f"\n🚀 SUBMITTING COMMAND TO LEADER {leader.node_id}")
            result = await leader.submit_command("trace_key=888")
            print(f"Submit result: {result}")

            print_all_states("IMMEDIATELY AFTER SUBMIT")

            # Give a bit more time for RPCs
            await asyncio.sleep(0.2)

            print_all_states("AFTER REPLICATION DELAY")

            # Check if leader updated its commit_index
            if leader.volatile_state.commit_index == 2:
                print("✅ Leader commit_index updated to 2")
            else:
                print(
                    f"❌ Leader commit_index is {leader.volatile_state.commit_index}, expected 2"
                )

            # Check if followers got the updates
            for follower in followers:
                if follower.volatile_state.commit_index == 2:
                    print(f"✅ Follower {follower.node_id} commit_index updated to 2")
                else:
                    print(
                        f"❌ Follower {follower.node_id} commit_index is {follower.volatile_state.commit_index}, expected 2"
                    )

        finally:
            for node in nodes.values():
                await node.stop()


if __name__ == "__main__":
    asyncio.run(debug_commit_index_detailed())
