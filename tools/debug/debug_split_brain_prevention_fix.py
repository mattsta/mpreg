#!/usr/bin/env python3
"""
Debug script to demonstrate the split-brain prevention fix.

This script shows how the leader step-down timeout fix prevents split-brain scenarios
during network partitions by making leaders step down quickly when they lose majority contact.
"""

import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/Users/matt/repos/mpreg")

from mpreg.datastructures.production_raft_implementation import RaftState
from tests.test_raft_byzantine_edge_cases import TestRaftByzantineEdgeCases


async def demonstrate_split_brain_prevention():
    """Demonstrate the split-brain prevention fix in action."""

    print("=== SPLIT-BRAIN PREVENTION FIX DEMONSTRATION ===")
    print()

    test_instance = TestRaftByzantineEdgeCases()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a 5-node cluster
        nodes, network = await test_instance.create_byzantine_cluster(
            5, Path(temp_dir), None
        )

        try:
            # Start all nodes
            print("1. Starting 5-node cluster...")
            for node in nodes.values():
                await node.start()

            # Wait for leader election
            print("2. Waiting for leader election...")
            await asyncio.sleep(0.8)

            # Find initial leader
            leaders = [n for n in nodes.values() if n.current_state == RaftState.LEADER]
            if not leaders:
                print("❌ No leader elected")
                return

            leader = leaders[0]
            print(f"✅ Initial leader: {leader.node_id}")

            # Show the step-down timeout calculation
            step_down_timeout = min(
                leader.config.heartbeat_interval * 3.0,
                leader.config.election_timeout_max * 0.5,
            )
            print(f"📊 Leader step-down timeout: {step_down_timeout:.3f}s")
            print(f"   - Heartbeat interval: {leader.config.heartbeat_interval:.3f}s")
            print(
                f"   - Election timeout max: {leader.config.election_timeout_max:.3f}s"
            )
            print(
                f"   - Step-down = min({leader.config.heartbeat_interval:.3f}s * 3, {leader.config.election_timeout_max:.3f}s * 0.5)"
            )

            # Create network partition: 3 vs 2 (majority vs minority)
            group1 = {"node_0", "node_1", "node_2"}  # Majority
            group2 = {"node_3", "node_4"}  # Minority

            print("\n3. Creating network partition:")
            print(f"   - Majority partition: {group1}")
            print(f"   - Minority partition: {group2}")

            if leader.node_id in group2:
                print(
                    f"   - Leader {leader.node_id} is in MINORITY partition (should step down)"
                )
            else:
                print(
                    f"   - Leader {leader.node_id} is in MAJORITY partition (should remain)"
                )

            network.create_partition(group1, group2)

            # Monitor leader status over time
            print(
                f"\n4. Monitoring leader status (checking every 0.2s for {step_down_timeout + 1.0:.1f}s):"
            )

            monitor_time = step_down_timeout + 1.0
            checks = int(monitor_time / 0.2)

            for i in range(checks):
                await asyncio.sleep(0.2)

                # Check leader status in each partition
                group1_leaders = [
                    nodes[nid]
                    for nid in group1
                    if nodes[nid].current_state == RaftState.LEADER
                ]
                group2_leaders = [
                    nodes[nid]
                    for nid in group2
                    if nodes[nid].current_state == RaftState.LEADER
                ]

                elapsed = (i + 1) * 0.2
                print(
                    f"   t={elapsed:.1f}s: Majority leaders={len(group1_leaders)}, Minority leaders={len(group2_leaders)}"
                )

                # Show when step-down occurs
                if elapsed >= step_down_timeout and len(group2_leaders) == 0:
                    print(
                        f"   ✅ Minority partition leader stepped down after {elapsed:.1f}s (timeout: {step_down_timeout:.1f}s)"
                    )
                    break

            # Final verification
            print("\n5. Final verification:")
            group1_leaders = [
                nodes[nid]
                for nid in group1
                if nodes[nid].current_state == RaftState.LEADER
            ]
            group2_leaders = [
                nodes[nid]
                for nid in group2
                if nodes[nid].current_state == RaftState.LEADER
            ]

            print(
                f"   - Majority partition leaders: {[l.node_id for l in group1_leaders]}"
            )
            print(
                f"   - Minority partition leaders: {[l.node_id for l in group2_leaders]}"
            )

            if len(group1_leaders) <= 1 and len(group2_leaders) == 0:
                print("   ✅ SPLIT-BRAIN PREVENTION SUCCESSFUL!")
                print("   - Only majority partition has leader")
                print("   - Minority partition correctly has no leaders")
            else:
                print("   ❌ Split-brain prevention failed")

        finally:
            # Clean up
            cleanup_tasks = []
            for node in nodes.values():
                cleanup_tasks.append(asyncio.create_task(node.stop()))

            try:
                await asyncio.wait_for(
                    asyncio.gather(*cleanup_tasks, return_exceptions=True), timeout=5.0
                )
            except TimeoutError:
                print("Warning: Some nodes did not stop within timeout")


if __name__ == "__main__":
    asyncio.run(demonstrate_split_brain_prevention())
