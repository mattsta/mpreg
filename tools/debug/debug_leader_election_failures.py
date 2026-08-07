#!/usr/bin/env python3
"""
Debug script for RAFT leader election failures under concurrency.
Specifically investigating: "Node did not become leader within 1.0s after restart"
"""

import asyncio
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

# Set up environment for high concurrency testing
os.environ["PYTEST_XDIST_WORKER"] = "gw5"  # Simulate the failing worker

import sys

sys.path.insert(0, "/Users/matt/repos/mpreg")

from tests.test_production_raft_integration import TestProductionRaftIntegration


@dataclass
class ElectionResult:
    run: int
    election_time: float
    success: bool
    error: str = ""
    final_state: str = ""
    final_term: int = 0


async def debug_leader_election():
    """Debug single node leader election failures."""

    print("=== LEADER ELECTION DEBUG ===")
    print(f"Worker environment: {os.environ.get('PYTEST_XDIST_WORKER', 'NONE')}")

    test_instance = TestProductionRaftIntegration()
    results: list[ElectionResult] = []

    for run in range(10):
        print(f"\n--- ELECTION TEST {run + 1}/10 ---")

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)

            start_time = time.time()
            try:
                # Run the single node test that's failing
                await test_instance.test_single_node_cluster_basic_operations(temp_dir)
                election_time = time.time() - start_time

                result = ElectionResult(
                    run=run + 1, election_time=election_time, success=True
                )
                results.append(result)
                print(f"✅ RUN {run + 1}: SUCCESS in {election_time:.2f}s")

            except Exception as e:
                election_time = time.time() - start_time

                result = ElectionResult(
                    run=run + 1,
                    election_time=election_time,
                    success=False,
                    error=str(e),
                )
                results.append(result)
                print(f"❌ RUN {run + 1}: FAILED in {election_time:.2f}s - {e}")

    # Analyze results
    print("\n=== ELECTION ANALYSIS ===")
    successes = [r for r in results if r.success]
    failures = [r for r in results if not r.success]

    print(
        f"Success rate: {len(successes)}/{len(results)} ({len(successes) / len(results) * 100:.1f}%)"
    )

    if successes:
        success_times = [r.election_time for r in successes]
        avg_success_time = sum(success_times) / len(success_times)
        max_success_time = max(success_times)
        min_success_time = min(success_times)
        print(
            f"Success times: avg={avg_success_time:.2f}s, min={min_success_time:.2f}s, max={max_success_time:.2f}s"
        )

    if failures:
        failure_times = [r.election_time for r in failures]
        avg_failure_time = sum(failure_times) / len(failure_times)
        print(f"Average failure time: {avg_failure_time:.2f}s")
        print("Failure errors:")
        for failure in failures:
            print(f"  Run {failure.run}: {failure.error}")

    # Identify the issue
    print("\n=== DIAGNOSIS ===")
    if len(failures) > 0:
        print("❌ ELECTION TIMING ISSUE DETECTED")
        print("Possible causes:")
        print("1. Election timeout too aggressive under concurrency")
        print("2. Resource contention delaying state transitions")
        print("3. Timing scaling factor insufficient")
        print("4. Port allocation delays affecting startup")
    else:
        print("✅ Elections working correctly in this test")


if __name__ == "__main__":
    asyncio.run(debug_leader_election())
