#!/usr/bin/env python3
"""
Debug script to understand why log convergence fails after partition healing
in the Byzantine split-brain prevention test.
"""

import asyncio
import tempfile
from pathlib import Path

from mpreg.datastructures.production_raft import RaftState
from tests.test_raft_byzantine_edge_cases import (
    TestRaftByzantineEdgeCases,
)


async def debug_convergence_issue():
    """Debug the convergence issue step by step."""
    print("=== DEBUGGING CONVERGENCE ISSUE ===")

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)
        test_instance = TestRaftByzantineEdgeCases()

        # Create 5-node cluster
        nodes, network = await test_instance.create_byzantine_cluster(5, temp_dir, None)

        try:
            # Start all nodes and elect leader
            print("1. Starting all nodes...")
            for node in nodes.values():
                await node.start()

            # Wait for initial leader
            print("2. Waiting for leader election...")
            initial_leader = None
            for _ in range(50):
                await asyncio.sleep(0.1)
                leaders = [
                    n for n in nodes.values() if n.current_state == RaftState.LEADER
                ]
                if leaders:
                    initial_leader = leaders[0]
                    break

            assert initial_leader is not None, "No initial leader elected"
            print(f"   Initial leader: {initial_leader.node_id}")

            # Submit some commands before partition
            print("3. Submitting commands before partition...")
            for i in range(3):
                result = await initial_leader.submit_command(f"before_partition_{i}")
                print(f"   Command {i}: {result is not None}")

            await asyncio.sleep(0.5)  # Allow replication

            # Show log lengths before partition
            print("4. Log lengths before partition:")
            for node_id, node in nodes.items():
                log_len = len(node.persistent_state.log_entries)
                print(f"   {node_id}: {log_len} entries")

            # Create network partition: 3 vs 2 split
            group1 = {"node_0", "node_1", "node_2"}  # Majority
            group2 = {"node_3", "node_4"}  # Minority

            print(f"5. Creating partition: {group1} vs {group2}")
            network.create_partition(group1, group2)

            # Wait for partition to take effect
            await asyncio.sleep(1.5)

            # Submit commands to majority partition
            group1_nodes = [nodes[nid] for nid in group1]
            majority_leader = None
            for node in group1_nodes:
                if node.current_state == RaftState.LEADER:
                    majority_leader = node
                    break

            if majority_leader:
                print(
                    f"6. Submitting commands to majority leader {majority_leader.node_id}..."
                )
                for i in range(5):
                    result = await majority_leader.submit_command(f"majority_cmd_{i}")
                    print(f"   Majority command {i}: {result is not None}")
                    await asyncio.sleep(0.1)

            # Show log lengths during partition
            print("7. Log lengths during partition:")
            for node_id, node in nodes.items():
                log_len = len(node.persistent_state.log_entries)
                state = node.current_state.value
                print(f"   {node_id}: {log_len} entries, state: {state}")

            # Heal partition
            print("8. Healing network partition...")
            network.heal_partition()

            # Show contact info states after healing
            print("9. Contact info after healing:")
            for node_id, node in nodes.items():
                if hasattr(node, "follower_contact_info"):
                    contact_dict = {}
                    for follower_id, info in node.follower_contact_info.items():
                        contact_dict[follower_id] = {
                            "status": info.contact_status.value,
                            "failures": info.consecutive_failures,
                            "last_attempt": info.last_attempt_time,
                        }
                    print(f"   {node_id}: {contact_dict}")

            # Wait a bit for reconnection
            print("10. Waiting for convergence...")
            for attempt in range(20):  # 4 seconds total
                await asyncio.sleep(0.2)

                if attempt % 5 == 0:  # Every 1 second
                    print(f"    Attempt {attempt}:")
                    leaders = [
                        n for n in nodes.values() if n.current_state == RaftState.LEADER
                    ]
                    if leaders:
                        leader = leaders[0]
                        leader_log_len = len(leader.persistent_state.log_entries)
                        print(
                            f"      Leader {leader.node_id}: {leader_log_len} entries"
                        )

                        for node_id, node in nodes.items():
                            if node != leader:
                                node_log_len = len(node.persistent_state.log_entries)
                                print(f"      {node_id}: {node_log_len} entries")

            # Final convergence check
            print("11. Final convergence analysis:")
            final_leaders = [
                n for n in nodes.values() if n.current_state == RaftState.LEADER
            ]
            if final_leaders:
                final_leader = final_leaders[0]
                leader_log_length = len(final_leader.persistent_state.log_entries)
                print(
                    f"    Final leader {final_leader.node_id}: {leader_log_length} entries"
                )

                converged_nodes = 0
                for node_id, node in nodes.items():
                    if node != final_leader:
                        node_log_length = len(node.persistent_state.log_entries)
                        converged = abs(node_log_length - leader_log_length) <= 1
                        if converged:
                            converged_nodes += 1

                        # Show follower contact info from leader's perspective
                        contact_info = final_leader.follower_contact_info.get(node_id)
                        if contact_info:
                            print(
                                f"    {node_id}: {node_log_length} entries, converged: {converged}"
                            )
                            print(
                                f"      Contact status: {contact_info.contact_status.value}"
                            )
                            print(
                                f"      Consecutive failures: {contact_info.consecutive_failures}"
                            )
                            print(
                                f"      Last successful: {contact_info.last_successful_contact}"
                            )
                        else:
                            print(
                                f"    {node_id}: {node_log_length} entries, converged: {converged} (no contact info)"
                            )

                convergence_rate = converged_nodes / (len(nodes) - 1)
                print(
                    f"    Convergence rate: {convergence_rate * 100:.1f}% ({converged_nodes}/{len(nodes) - 1})"
                )

                if convergence_rate < 0.5:
                    print("    ❌ CONVERGENCE FAILURE DETECTED")

                    # Analyze why nodes aren't converging
                    print("12. Analyzing convergence failure:")

                    # Check if leader is actually sending heartbeats
                    if hasattr(final_leader, "_heartbeat_task"):
                        print(
                            f"    Leader heartbeat task: {final_leader._heartbeat_task}"
                        )
                        if final_leader._heartbeat_task:
                            print(
                                f"    Task done: {final_leader._heartbeat_task.done()}"
                            )
                            if final_leader._heartbeat_task.done():
                                print(
                                    f"    Task exception: {final_leader._heartbeat_task.exception()}"
                                )

                    # Check network connectivity
                    print("    Network connectivity test:")
                    for node_id in nodes:
                        if node_id != final_leader.node_id:
                            can_comm = network.can_communicate(
                                final_leader.node_id, node_id
                            )
                            print(
                                f"      {final_leader.node_id} -> {node_id}: {can_comm}"
                            )
                else:
                    print("    ✅ Convergence successful")

        finally:
            # Cleanup
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
    asyncio.run(debug_convergence_issue())
