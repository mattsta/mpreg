#!/usr/bin/env python3
"""
DEEP DIVE DEBUG: Find the exact deadlock in port allocator for 50 ports
"""

import time
from concurrent.futures import ThreadPoolExecutor

from tests.port_allocator import get_port_allocator

from mpreg.core.errors import OPERATIONAL_EXCEPTIONS


def simulate_xdist_worker(worker_id: str, port_count: int):
    """Simulate what a pytest-xdist worker does."""
    print(f"🔧 Worker {worker_id}: Starting port allocation for {port_count} ports")
    start_time = time.time()

    try:
        allocator = get_port_allocator()
        print(f"   Worker {worker_id}: Got allocator, worker_id={allocator.worker_id}")

        print(
            f"   Worker {worker_id}: Calling allocate_port_range({port_count}, 'servers')"
        )
        ports = allocator.allocate_port_range(port_count, "servers")

        duration = time.time() - start_time
        print(
            f"   ✅ Worker {worker_id}: SUCCESS - got {len(ports)} ports in {duration:.2f}s"
        )

        # Release ports
        for port in ports:
            allocator.release_port(port)

        print(f"   🧹 Worker {worker_id}: Released all ports")
        return True

    except OPERATIONAL_EXCEPTIONS as e:
        duration = time.time() - start_time
        print(f"   ❌ Worker {worker_id}: FAILED after {duration:.2f}s - {e}")
        return False


def test_sequential_allocation():
    """Test sequential allocation (should work)."""
    print("\n" + "=" * 60)
    print("TESTING SEQUENTIAL ALLOCATION (1 worker at a time)")
    print("=" * 60)

    for i in range(3):
        worker_id = f"seq-worker-{i}"
        success = simulate_xdist_worker(worker_id, 50)
        if not success:
            print(f"❌ Sequential allocation failed at worker {i}")
            return False

    print("✅ Sequential allocation completed successfully")
    return True


def test_concurrent_allocation():
    """Test concurrent allocation (likely to deadlock)."""
    print("\n" + "=" * 60)
    print("TESTING CONCURRENT ALLOCATION (multiple workers simultaneously)")
    print("=" * 60)

    workers = ["gw0", "gw1", "gw2"]

    with ThreadPoolExecutor(max_workers=len(workers)) as executor:
        print(f"🔧 Starting {len(workers)} concurrent workers...")

        # Submit all workers at once
        futures = []
        for worker_id in workers:
            future = executor.submit(simulate_xdist_worker, worker_id, 50)
            futures.append((worker_id, future))

        # Wait for results with timeout
        results = {}
        for worker_id, future in futures:
            try:
                success = future.result(timeout=30)  # 30 second timeout per worker
                results[worker_id] = success
            except OPERATIONAL_EXCEPTIONS as e:
                print(f"   ❌ Worker {worker_id}: TIMEOUT or ERROR: {e}")
                results[worker_id] = False

    success_count = sum(results.values())
    print(f"\n📊 CONCURRENT RESULTS: {success_count}/{len(workers)} workers succeeded")

    if success_count < len(workers):
        print("❌ DEADLOCK DETECTED in concurrent allocation")
        for worker_id, success in results.items():
            status = "✅" if success else "❌"
            print(f"   {status} {worker_id}")
        return False

    print("✅ Concurrent allocation completed successfully")
    return True


def test_different_port_counts():
    """Test different port counts to find the deadlock threshold."""
    print("\n" + "=" * 60)
    print("TESTING DIFFERENT PORT COUNTS")
    print("=" * 60)

    port_counts = [5, 10, 20, 30, 40, 50]

    for count in port_counts:
        print(f"\n🔧 Testing {count} ports with 2 concurrent workers...")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(simulate_xdist_worker, f"test-gw{i}", count)
                for i in range(2)
            ]

            results = []
            for i, future in enumerate(futures):
                try:
                    success = future.result(timeout=15)
                    results.append(success)
                except OPERATIONAL_EXCEPTIONS as e:
                    print(f"   ❌ Worker {i}: TIMEOUT: {e}")
                    results.append(False)

        success_count = sum(results)
        status = "✅" if success_count == 2 else "❌"
        print(f"   {status} {count} ports: {success_count}/2 workers succeeded")

        if success_count < 2:
            print(f"💡 DEADLOCK THRESHOLD FOUND: {count} ports causes issues")
            return count

    print("✅ All port counts work fine")
    return None


def main():
    """Run comprehensive port allocator deadlock analysis."""
    print("🔬 PORT ALLOCATOR DEADLOCK ANALYSIS")
    print("🎯 Goal: Find why large cluster tests hang in pytest")

    # Test 1: Sequential allocation
    sequential_ok = test_sequential_allocation()

    # Test 2: Concurrent allocation
    concurrent_ok = test_concurrent_allocation()

    # Test 3: Find deadlock threshold
    deadlock_threshold = test_different_port_counts()

    print("\n" + "=" * 60)
    print("🏁 ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"Sequential allocation: {'✅ WORKS' if sequential_ok else '❌ FAILS'}")
    print(f"Concurrent allocation: {'✅ WORKS' if concurrent_ok else '❌ FAILS'}")

    if deadlock_threshold:
        print(f"Deadlock threshold: {deadlock_threshold} ports")
        print(
            "💡 SOLUTION: Port allocator has concurrency issues with large port ranges"
        )
    elif not concurrent_ok:
        print("💡 SOLUTION: Port allocator has general concurrency deadlock")
    else:
        print("🤔 No deadlock found - issue may be elsewhere")


if __name__ == "__main__":
    main()
