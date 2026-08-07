#!/usr/bin/env python3
"""
Debug script to trace the timing of commit_index propagation.
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


async def debug_commit_timing():
    """Debug the timing of commit_index propagation."""

    print("=== COMMIT TIMING DEBUG ===")

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
            for attempt in range(40):
                await asyncio.sleep(0.1)

                leaders = [
                    n for n in nodes.values() if n.current_state == RaftState.LEADER
                ]
                if leaders:
                    leader = leaders[0]
                    break

            if not leader:
                print("❌ No leader elected!")
                return

            print(f"✅ Leader: {leader.node_id}")

            print("\nStep 1: Leader's initial state")
            print(f"  commit_index: {leader.volatile_state.commit_index}")
            if leader.leader_volatile_state:
                print(
                    f"  match_index: {dict(leader.leader_volatile_state.match_index)}"
                )

            # Submit command
            print("\nStep 2: Submitting command")
            result = await leader.submit_command("timing_test=123")
            print(f"  Submit result: {result}")

            print("\nStep 3: Leader's state immediately after submit")
            print(f"  commit_index: {leader.volatile_state.commit_index}")
            if leader.leader_volatile_state:
                print(
                    f"  match_index: {dict(leader.leader_volatile_state.match_index)}"
                )

            # Wait for additional heartbeats
            print("\nStep 4: Waiting for heartbeats...")
            await asyncio.sleep(0.5)  # Wait for heartbeat interval

            # Check final state
            print("\nStep 5: Final states:")
            for node_id, node in nodes.items():
                print(
                    f"  {node_id}: commit_index={node.volatile_state.commit_index}, last_applied={node.volatile_state.last_applied}"
                )
                if hasattr(node.state_machine, "state"):
                    print(f"    state_machine: {node.state_machine.state}")

        finally:
            for node in nodes.values():
                await node.stop()


if __name__ == "__main__":
    asyncio.run(debug_commit_timing())
