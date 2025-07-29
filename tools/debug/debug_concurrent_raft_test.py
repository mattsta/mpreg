#!/usr/bin/env python3
"""
Debug script to simulate concurrent RAFT tests and identify race conditions.
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


async def run_single_test(test_id: str, delay: float = 0.0):
    """Run a single RAFT test instance."""
    print(f"[{test_id}] Starting test (delay={delay:.2f}s)")

    # Simulate pytest-xdist worker environment
    os.environ["PYTEST_XDIST_WORKER"] = f"gw{test_id}"

    await asyncio.sleep(delay)

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
            for attempt in range(40):  # 4 seconds
                await asyncio.sleep(0.1)

                leaders = [
                    n for n in nodes.values() if n.current_state == RaftState.LEADER
                ]
                if leaders:
                    leader = leaders[0]
                    break

            if not leader:
                print(f"[{test_id}] ❌ No leader elected!")
                return {
                    "test_id": test_id,
                    "success": False,
                    "error": "No leader elected",
                }

            # Submit command and check replication
            result = await leader.submit_command(f"test_key_{test_id}=42")

            if result is None:
                print(f"[{test_id}] ❌ Command submission failed!")
                return {
                    "test_id": test_id,
                    "success": False,
                    "error": "Command submission failed",
                }

            # Wait for replication
            await asyncio.sleep(0.2)

            # Check all nodes have applied the command
            success = True
            applied_count = 0
            for node in nodes.values():
                if hasattr(node.state_machine, "state"):
                    value = node.state_machine.state.get(f"test_key_{test_id}")
                    if value == 42:
                        applied_count += 1
                    else:
                        print(
                            f"[{test_id}] ❌ Node {node.node_id}: expected 42, got {value}"
                        )
                        success = False

            if applied_count == 3:
                print(f"[{test_id}] ✅ Test passed - all nodes applied command")
                return {
                    "test_id": test_id,
                    "success": True,
                    "applied_count": applied_count,
                }
            else:
                print(f"[{test_id}] ❌ Only {applied_count}/3 nodes applied command")
                return {
                    "test_id": test_id,
                    "success": False,
                    "error": f"Only {applied_count}/3 applied",
                    "applied_count": applied_count,
                }

        except Exception as e:
            print(f"[{test_id}] ❌ Exception: {e}")
            return {"test_id": test_id, "success": False, "error": str(e)}
        finally:
            for node in nodes.values():
                await node.stop()


async def debug_concurrent_conditions():
    """Simulate concurrent test conditions to identify race conditions."""

    print("=== CONCURRENT RAFT TEST DEBUG ===")

    # Test 1: Sequential execution (should work)
    print("\n1. Sequential execution test...")
    sequential_results = []
    for i in range(3):
        result = await run_single_test(f"seq_{i}")
        sequential_results.append(result)

    sequential_success = sum(1 for r in sequential_results if r["success"])
    print(f"Sequential results: {sequential_success}/3 passed")

    # Test 2: Concurrent execution (might fail like pytest-xdist)
    print("\n2. Concurrent execution test...")
    concurrent_tasks = [
        run_single_test(f"conc_{i}", delay=i * 0.05)  # Slight stagger
        for i in range(5)
    ]

    concurrent_results = await asyncio.gather(*concurrent_tasks, return_exceptions=True)

    # Process results
    concurrent_success = 0
    for result in concurrent_results:
        if isinstance(result, dict) and result.get("success"):
            concurrent_success += 1
        elif isinstance(result, Exception):
            print(f"Concurrent test exception: {result}")

    print(f"Concurrent results: {concurrent_success}/5 passed")

    # Test 3: High concurrency simulation
    print("\n3. High concurrency simulation (pytest-xdist -n5 style)...")
    high_concurrency_tasks = [
        run_single_test(f"high_{i}", delay=0.0)  # No delay - all start simultaneously
        for i in range(5)
    ]

    high_concurrent_results = await asyncio.gather(
        *high_concurrency_tasks, return_exceptions=True
    )

    # Process results
    high_concurrent_success = 0
    failures = []
    for result in high_concurrent_results:
        if isinstance(result, dict):
            if result.get("success"):
                high_concurrent_success += 1
            else:
                failures.append(result)
        elif isinstance(result, Exception):
            print(f"High concurrency exception: {result}")
            failures.append({"error": str(result)})

    print(f"High concurrency results: {high_concurrent_success}/5 passed")

    if failures:
        print("Failure analysis:")
        for failure in failures:
            print(f"  - {failure}")

    # Summary
    print("\n=== SUMMARY ===")
    print(f"Sequential (expected baseline): {sequential_success}/3 passed")
    print(f"Concurrent (medium load): {concurrent_success}/5 passed")
    print(
        f"High concurrency (pytest-xdist simulation): {high_concurrent_success}/5 passed"
    )

    if high_concurrent_success < 5:
        print("❌ Race condition detected under high concurrency!")
    else:
        print("✅ No race conditions detected")


if __name__ == "__main__":
    asyncio.run(debug_concurrent_conditions())
