#!/usr/bin/env python3
"""
Deep dive debug script for RAFT split brain test failures.
This will run the test multiple times with detailed logging to identify failure patterns.
"""

import asyncio
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

# Set up environment for high concurrency testing
os.environ["PYTEST_XDIST_WORKER"] = "gw0"  # Simulate pytest-xdist worker

# Import the test infrastructure
import sys

sys.path.insert(0, "/Users/matt/repos/mpreg")

from tests.test_production_raft_integration import TestProductionRaftIntegration


@dataclass
class TestResult:
    run: int
    elapsed: float
    status: str


@dataclass
class FailureResult(TestResult):
    error: str
    error_type: str
    traceback: str


@dataclass
class SuccessResult(TestResult):
    pass


async def debug_split_brain_test():
    """Run split brain test with detailed debugging."""

    print("=== DEEP DIVE DEBUG: RAFT Split Brain Test ===")
    print(f"Worker environment: {os.environ.get('PYTEST_XDIST_WORKER', 'NONE')}")

    test_instance = TestProductionRaftIntegration()
    failures: list[FailureResult] = []
    successes: list[SuccessResult] = []

    for run in range(5):  # Reduce to 5 runs for cleaner output
        print(f"\n--- RUN {run + 1}/5 ---")

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)

            try:
                start_time = time.time()
                await test_instance._run_split_brain_test(temp_dir, run)
                elapsed = time.time() - start_time

                success_result = SuccessResult(
                    run=run + 1, elapsed=elapsed, status="SUCCESS"
                )
                successes.append(success_result)
                print(f"✅ RUN {run + 1}: SUCCESS in {elapsed:.2f}s")

            except Exception as e:
                elapsed = time.time() - start_time

                # Get full traceback for better debugging
                import traceback

                full_traceback = traceback.format_exc()

                failure_result = FailureResult(
                    run=run + 1,
                    elapsed=elapsed,
                    error=str(e),
                    error_type=type(e).__name__,
                    traceback=full_traceback,
                    status="FAILED",
                )
                failures.append(failure_result)
                print(
                    f"❌ RUN {run + 1}: FAILED in {elapsed:.2f}s - {type(e).__name__}: {e}"
                )
                print(f"Full traceback:\n{full_traceback}")

    # Analyze results
    print("\n=== ANALYSIS ===")
    print("Total runs: 5")
    print(f"Successes: {len(successes)}")
    print(f"Failures: {len(failures)}")
    print(f"Failure rate: {len(failures) / 5 * 100:.1f}%")

    if failures:
        print("\n=== FAILURE ANALYSIS ===")
        for failure in failures:
            print(f"Run {failure.run}: {failure.error_type} - {failure.error}")

        # Look for patterns
        error_types: dict[str, int] = {}
        for failure in failures:
            error_type = failure.error_type
            error_types[error_type] = error_types.get(error_type, 0) + 1

        print("\nError type patterns:")
        for error_type, count in error_types.items():
            print(f"  {error_type}: {count} occurrences")

    if successes:
        elapsed_times = [s.elapsed for s in successes]
        avg_success_time = sum(elapsed_times) / len(elapsed_times)
        print(f"\nAverage success time: {avg_success_time:.2f}s")

    if failures:
        elapsed_times = [f.elapsed for f in failures]
        avg_failure_time = sum(elapsed_times) / len(elapsed_times)
        print(f"Average failure time: {avg_failure_time:.2f}s")


if __name__ == "__main__":
    asyncio.run(debug_split_brain_test())
