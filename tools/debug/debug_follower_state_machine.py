#!/usr/bin/env python3
"""
Debug script to specifically investigate follower state machine application bugs.
"""

import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/Users/matt/repos/mpreg")

from mpreg.datastructures.production_raft_implementation import RaftState
from tests.test_production_raft_integration import (
    MockNetwork,
    TestProductionRaftIntegration,
)


async def debug_follower_state_machine():
    """Debug follower state machine application with detailed logging."""

    print("=== FOLLOWER STATE MACHINE DEBUG ===")

    # Simulate pytest environment
    os.environ["PYTEST_XDIST_WORKER"] = "gw0"

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

            # Submit command
            print(f"\n--- SUBMITTING COMMAND TO LEADER {leader.node_id} ---")
            command = "debug_key=999"
            result = await leader.submit_command(command)
            print(f"Submit result: {result}")

            # Let AppendEntries propagate
            await asyncio.sleep(0.1)

            print("\n--- DETAILED STATE ANALYSIS ---")

            # Analyze leader state
            print(f"\nLEADER {leader.node_id}:")
            print(f"  commit_index: {leader.volatile_state.commit_index}")
            print(f"  last_applied: {leader.volatile_state.last_applied}")
            print(f"  log_entries: {len(leader.persistent_state.log_entries)}")
            print(f"  commands_applied: {leader.metrics.commands_applied}")
            if hasattr(leader.state_machine, "state"):
                print(f"  state_machine: {leader.state_machine.state}")

            # Analyze each follower
            for follower in followers:
                print(f"\nFOLLOWER {follower.node_id}:")
                print(f"  commit_index: {follower.volatile_state.commit_index}")
                print(f"  last_applied: {follower.volatile_state.last_applied}")
                print(f"  log_entries: {len(follower.persistent_state.log_entries)}")
                print(f"  commands_applied: {follower.metrics.commands_applied}")
                if hasattr(follower.state_machine, "state"):
                    print(f"  state_machine: {follower.state_machine.state}")

                # Show detailed log entries
                print("  Log entries detail:")
                for i, entry in enumerate(follower.persistent_state.log_entries):
                    print(
                        f"    [{i + 1}] term={entry.term}, type={entry.entry_type.value}, command={entry.command}"
                    )

                # Manual trigger of _apply_committed_entries
                print("  Manual _apply_committed_entries trigger:")
                print(
                    f"    Before: last_applied={follower.volatile_state.last_applied}, commit_index={follower.volatile_state.commit_index}"
                )

                # Check the specific logic that should apply entries
                while (
                    follower.volatile_state.last_applied
                    < follower.volatile_state.commit_index
                ):
                    next_index = follower.volatile_state.last_applied + 1
                    print(f"    Processing entry at index {next_index}")

                    if next_index <= len(follower.persistent_state.log_entries):
                        entry = follower.persistent_state.log_entries[next_index - 1]
                        print(
                            f"      Entry: type={entry.entry_type.value}, command={entry.command}"
                        )

                        if (
                            entry.entry_type.value == "command"
                            and entry.command is not None
                        ):
                            print(f"      Applying command: {entry.command}")
                            try:
                                result = await follower.state_machine.apply_command(
                                    entry.command, entry.index
                                )
                                print(f"      Apply result: {result}")
                                follower.metrics.commands_applied += 1
                            except Exception as e:
                                print(f"      Apply ERROR: {e}")

                        follower.volatile_state.last_applied = next_index
                        print(f"      Updated last_applied to {next_index}")
                    else:
                        print(
                            f"      ERROR: next_index {next_index} > log_entries length {len(follower.persistent_state.log_entries)}"
                        )
                        break

                print(
                    f"    After manual apply: last_applied={follower.volatile_state.last_applied}"
                )
                if hasattr(follower.state_machine, "state"):
                    print(
                        f"    After manual apply state_machine: {follower.state_machine.state}"
                    )

        finally:
            for node in nodes.values():
                await node.stop()


if __name__ == "__main__":
    asyncio.run(debug_follower_state_machine())
