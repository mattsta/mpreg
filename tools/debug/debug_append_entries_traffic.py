#!/usr/bin/env python3
"""
Debug script to monitor AppendEntries RPC traffic and commit_index propagation.
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
logger = logging.getLogger(__name__)


# Simple debugging without network interception


async def debug_append_entries_traffic():
    """Debug AppendEntries traffic to identify commit_index propagation bug."""

    print("=== APPEND ENTRIES TRAFFIC DEBUG ===")

    test_instance = TestProductionRaftIntegration()

    with tempfile.TemporaryDirectory() as temp_dir:
        network = MockNetwork()
        nodes = test_instance.create_raft_cluster(
            3, Path(temp_dir), network, storage_type="memory"
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

            print("\n--- INITIAL STATE ---")
            for node_id, node in nodes.items():
                print(
                    f"{node_id}: commit_index={node.volatile_state.commit_index}, last_applied={node.volatile_state.last_applied}"
                )

            # Submit command
            print(f"\n--- SUBMITTING COMMAND TO LEADER {leader.node_id} ---")
            result = await leader.submit_command("test_key=777")
            print(f"Submit result: {result}")

            # Give time for replication
            await asyncio.sleep(0.2)

            print("\n--- FINAL STATE ANALYSIS ---")
            for node_id, node in nodes.items():
                print(f"\n{node_id} ({node.current_state.value}):")
                print(f"  commit_index: {node.volatile_state.commit_index}")
                print(f"  last_applied: {node.volatile_state.last_applied}")
                print(f"  log_entries: {len(node.persistent_state.log_entries)}")
                print(f"  commands_applied: {node.metrics.commands_applied}")

                # Show log details
                for i, entry in enumerate(node.persistent_state.log_entries):
                    print(
                        f"    Log[{i + 1}]: term={entry.term}, type={entry.entry_type.value}, command={entry.command}"
                    )

                if hasattr(node.state_machine, "state"):
                    print(f"  state_machine: {node.state_machine.state}")

            # Check for consistency
            leader_commit_index = leader.volatile_state.commit_index
            all_consistent = True
            for follower in followers:
                if follower.volatile_state.commit_index != leader_commit_index:
                    print(
                        f"\n❌ INCONSISTENCY: {follower.node_id} commit_index={follower.volatile_state.commit_index}, leader has {leader_commit_index}"
                    )
                    all_consistent = False

            if all_consistent:
                print(
                    f"\n✅ All nodes have consistent commit_index={leader_commit_index}"
                )

        finally:
            for node in nodes.values():
                await node.stop()


if __name__ == "__main__":
    asyncio.run(debug_append_entries_traffic())
