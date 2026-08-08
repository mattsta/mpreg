#!/usr/bin/env python3
"""
Benchmark to demonstrate the performance benefits of concurrent I/O replication.
"""

import asyncio
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mpreg.datastructures.production_raft_implementation import RaftState
from tests.test_production_raft_integration import (
    MockNetwork,
    TestProductionRaftIntegration,
)


async def benchmark_replication_performance():
    """Benchmark replication performance with concurrent I/O."""

    print("=== REPLICATION PERFORMANCE BENCHMARK ===")

    test_instance = TestProductionRaftIntegration()

    with tempfile.TemporaryDirectory() as temp_dir:
        network = MockNetwork()

        # Test with a larger cluster to see the benefits
        nodes = test_instance.create_raft_cluster(
            5, Path(temp_dir), network, storage_type="memory"
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
            print(f"✅ Cluster size: {len(nodes)} nodes")

            # Benchmark multiple command submissions
            num_commands = 10
            print(f"\n🚀 Submitting {num_commands} commands...")

            start_time = time.time()

            for i in range(num_commands):
                result = await leader.submit_command(f"benchmark_key_{i}={i * 10}")
                if result is None:
                    print(f"❌ Command {i} failed")
                    return

            end_time = time.time()
            total_time = end_time - start_time

            print(f"⏱️  Total time: {total_time:.3f}s")
            print(f"📊 Average per command: {total_time / num_commands:.3f}s")
            print(f"🔥 Commands per second: {num_commands / total_time:.1f}")

            # Verify all nodes have the commands
            print("\n✅ Verification:")
            all_correct = True
            for node_id, node in nodes.items():
                if hasattr(node.state_machine, "state"):
                    expected_keys = {
                        f"benchmark_key_{i}": i * 10 for i in range(num_commands)
                    }
                    actual_keys = {
                        k: v
                        for k, v in node.state_machine.state.items()
                        if k.startswith("benchmark_key_")
                    }

                    if actual_keys == expected_keys:
                        print(f"  {node_id}: ✅ All {num_commands} commands applied")
                    else:
                        print(
                            f"  {node_id}: ❌ {len(actual_keys)}/{num_commands} commands applied"
                        )
                        all_correct = False

            if all_correct:
                print(
                    f"\n🎉 SUCCESS: All commands replicated to all {len(nodes)} nodes!"
                )
                print("💡 Concurrent I/O optimization working correctly!")

        finally:
            for node in nodes.values():
                await node.stop()


if __name__ == "__main__":
    asyncio.run(benchmark_replication_performance())
