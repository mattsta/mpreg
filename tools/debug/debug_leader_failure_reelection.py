#!/usr/bin/env python3
"""
Debug script to trace leader failure and reelection issues under high concurrency.
"""

import asyncio
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, "/Users/matt/repos/mpreg")

import contextlib

from mpreg.datastructures.production_raft_implementation import RaftState
from tests.test_production_raft_integration import (
    MockNetwork,
    TestProductionRaftIntegration,
)


async def debug_leader_failure_reelection():
    """Debug leader failure and reelection process."""

    print("=== LEADER FAILURE AND REELECTION DEBUG ===")
    print()

    test_instance = TestProductionRaftIntegration()

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)
        network = MockNetwork()

        # Create 3-node cluster like the failing test
        nodes = test_instance.create_raft_cluster(
            3, temp_dir, network, storage_type="memory"
        )

        try:
            print("1. Starting 3-node cluster with high concurrency config...")
            for node_id, node in nodes.items():
                await node.start()
                print(f"   - Started {node_id}")
                print(
                    f"     Election timeout: {node.config.election_timeout_min:.3f}s - {node.config.election_timeout_max:.3f}s"
                )
                print(f"     Heartbeat interval: {node.config.heartbeat_interval:.3f}s")

                # Calculate step-down timeout like the implementation does
                step_down_timeout = min(
                    node.config.heartbeat_interval * 3.0,
                    node.config.election_timeout_max * 0.5,
                )
                print(f"     Step-down timeout: {step_down_timeout:.3f}s")

            print("\n2. Waiting for initial leader election...")
            await asyncio.sleep(0.8)

            # Find initial leader
            leaders = [
                node
                for node in nodes.values()
                if node.current_state == RaftState.LEADER
            ]
            if not leaders:
                print("❌ No leader elected initially")
                return

            initial_leader = leaders[0]
            initial_leader_id = initial_leader.node_id
            print(f"✅ Initial leader: {initial_leader_id}")

            # Submit command to establish log
            print("\n3. Submitting command to establish log...")
            result = await initial_leader.submit_command("before_failure=1")
            print(f"Command result: {result}")
            await asyncio.sleep(0.1)

            # Show cluster state before failure
            print("\n4. Cluster state before failure:")
            for node_id, node in nodes.items():
                print(
                    f"   {node_id}: {node.current_state.value}, term={node.persistent_state.current_term}, log_entries={len(node.persistent_state.log_entries)}"
                )

            # Simulate leader failure
            print(f"\n5. Simulating failure of leader {initial_leader_id}...")
            await initial_leader.stop()

            remaining_nodes = [
                node for node in nodes.values() if node.node_id != initial_leader_id
            ]
            remaining_node_ids = [node.node_id for node in remaining_nodes]
            print(f"Remaining nodes: {remaining_node_ids}")

            # Monitor re-election process in detail
            print("\n6. Monitoring re-election process (up to 4 seconds)...")

            start_time = time.time()
            max_wait = 4.0  # Longer than the test timeout to see what happens

            step = 0
            while time.time() - start_time < max_wait:
                await asyncio.sleep(0.2)
                step += 1
                elapsed = time.time() - start_time

                # Check states of remaining nodes
                states = {}
                leaders = []

                for node in remaining_nodes:
                    state_str = node.current_state.value
                    term = node.persistent_state.current_term
                    states[node.node_id] = f"{state_str}(t={term})"

                    if node.current_state == RaftState.LEADER:
                        leaders.append(node)

                print(
                    f"   Step {step} (t={elapsed:.1f}s): {states}, Leaders: {len(leaders)}"
                )

                # Show leader contact info for debugging step-down logic
                for node in remaining_nodes:
                    if node.current_state == RaftState.LEADER:
                        # Check how many nodes this leader can contact
                        reachable_count = 1  # Self
                        for member_id in node.cluster_members:
                            if (
                                member_id != node.node_id
                                and member_id in remaining_node_ids
                            ):
                                reachable_count += 1

                        majority_needed = (len(node.cluster_members) // 2) + 1
                        time_since_majority = elapsed  # Approximate

                        # Calculate step-down timeout like the new implementation
                        time_since_became_leader = elapsed  # Approximate
                        grace_period = 2.0 * node.config.election_timeout_max

                        if time_since_became_leader < grace_period:
                            step_down_timeout = node.config.election_timeout_max * 0.8
                        else:
                            step_down_timeout = min(
                                node.config.heartbeat_interval * 3.0,
                                node.config.election_timeout_max * 0.5,
                            )

                        print(
                            f"     Leader {node.node_id}: reachable={reachable_count}/{len(node.cluster_members)} (need {majority_needed}), time_since_majority={time_since_majority:.1f}s, step_down_timeout={step_down_timeout:.1f}s"
                        )

                        if time_since_majority > step_down_timeout:
                            print(
                                f"     ⚠️  Leader {node.node_id} should step down soon!"
                            )

                if len(leaders) == 1:
                    new_leader = leaders[0]
                    print(
                        f"✅ New leader elected: {new_leader.node_id} after {elapsed:.1f}s"
                    )
                    break
                elif len(leaders) > 1:
                    leader_ids = [l.node_id for l in leaders]
                    print(f"❌ Multiple leaders detected: {leader_ids}")
                    break

            # Final status
            final_leaders = [
                node
                for node in remaining_nodes
                if node.current_state == RaftState.LEADER
            ]

            print(f"\n7. Final result after {max_wait}s:")
            print(f"   Leaders found: {len(final_leaders)}")
            if final_leaders:
                for leader in final_leaders:
                    print(f"   - Leader: {leader.node_id}")
            else:
                print("   ❌ No leaders found!")
                print("   Final states:")
                for node in remaining_nodes:
                    print(
                        f"     {node.node_id}: {node.current_state.value}, term={node.persistent_state.current_term}"
                    )

        finally:
            # Cleanup
            for node in nodes.values():
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(node.stop(), timeout=1.0)


if __name__ == "__main__":
    # Simulate high concurrency environment
    import os

    os.environ["PYTEST_XDIST_WORKER"] = "gw0"

    asyncio.run(debug_leader_failure_reelection())
