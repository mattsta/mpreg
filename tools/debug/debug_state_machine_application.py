#!/usr/bin/env python3
"""
Debug script to investigate state machine application issues.
"""

import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/Users/matt/repos/mpreg")

from mpreg.datastructures.production_raft_implementation import RaftState
from tests.test_production_raft_integration import (
    MockNetwork,
    TestProductionRaftIntegration,
)


async def debug_state_machine_application():
    """Debug state machine application under concurrency."""

    print("=== STATE MACHINE APPLICATION DEBUG ===")

    # Create test instance
    test_instance = TestProductionRaftIntegration()

    with tempfile.TemporaryDirectory() as temp_dir:
        network = MockNetwork()
        nodes = test_instance.create_raft_cluster(
            3, Path(temp_dir), network, storage_type="memory"
        )

        try:
            # Start all nodes
            print("Starting nodes...")
            for node in nodes.values():
                await node.start()

            # Wait for leader election
            print("Waiting for leader election...")
            leader = None
            for attempt in range(50):  # 5 seconds
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

            print(f"✅ Leader elected: {leader.node_id}")

            # Print initial state
            print("\n--- INITIAL STATE ---")
            for node_id, node in nodes.items():
                print(
                    f"{node_id}: state={node.current_state.value}, term={node.persistent_state.current_term}"
                )
                print(
                    f"  commit_index={node.volatile_state.commit_index}, last_applied={node.volatile_state.last_applied}"
                )
                print(f"  log_entries={len(node.persistent_state.log_entries)}")
                print(
                    f"  state_machine.state={getattr(node.state_machine, 'state', None)}"
                )

            # Submit command
            print("\n--- SUBMITTING COMMAND ---")
            result = await leader.submit_command("debug_key=123")
            print(f"Submit result: {result}")

            # Wait a bit for replication
            await asyncio.sleep(0.3)

            # Print state after command
            print("\n--- AFTER COMMAND SUBMISSION ---")
            for node_id, node in nodes.items():
                print(
                    f"{node_id}: state={node.current_state.value}, term={node.persistent_state.current_term}"
                )
                print(
                    f"  commit_index={node.volatile_state.commit_index}, last_applied={node.volatile_state.last_applied}"
                )
                print(f"  log_entries={len(node.persistent_state.log_entries)}")
                if hasattr(node.state_machine, "state"):
                    print(f"  state_machine.state={node.state_machine.state}")
                    print(
                        f"  debug_key value: {node.state_machine.state.get('debug_key')}"
                    )
                print(f"  commands_applied_metric: {node.metrics.commands_applied}")

                # Debug log entries
                print("  Log entries:")
                for i, entry in enumerate(node.persistent_state.log_entries):
                    print(
                        f"    [{i + 1}] term={entry.term}, type={entry.entry_type.value}, command={entry.command}"
                    )

            # Manual trigger of state machine application
            print("\n--- MANUAL STATE MACHINE APPLICATION ---")
            for node_id, node in nodes.items():
                if node != leader:  # Focus on followers
                    print(f"Manually applying for {node_id}...")
                    old_applied = node.volatile_state.last_applied
                    await node._apply_committed_entries()
                    new_applied = node.volatile_state.last_applied
                    print(f"  Applied entries: {old_applied} -> {new_applied}")
                    if hasattr(node.state_machine, "state"):
                        print(
                            f"  After manual apply: debug_key = {node.state_machine.state.get('debug_key')}"
                        )

        finally:
            print("\n--- CLEANUP ---")
            for node in nodes.values():
                await node.stop()


if __name__ == "__main__":
    asyncio.run(debug_state_machine_application())
