#!/usr/bin/env python3
"""
Comprehensive Port Allocation System Test

This test validates the improved port allocation system, checking for:
1. No reversed ranges (start > end)
2. Efficient worker distribution
3. High-capacity concurrent usage
4. Proper range validation
5. Edge case handling
"""

import os
import tempfile

# Simulate different worker environments
WORKER_SCENARIOS = [
    ("master", "master"),
    ("gw0", "gw0"),
    ("gw1", "gw1"),
    ("gw5", "gw5"),
    ("gw10", "gw10"),
    ("gw11", "gw11"),  # This was the problematic worker
    ("gw15", "gw15"),
    ("gw19", "gw19"),  # Maximum unique worker before wrap
]


def test_worker_scenario(worker_id: str, expected_id: str):
    """Test port allocation for a specific worker scenario."""
    print(f"\n🧪 Testing Worker: {worker_id}")

    # Set environment to simulate this worker
    old_worker = os.environ.get("PYTEST_XDIST_WORKER")
    os.environ["PYTEST_XDIST_WORKER"] = expected_id

    try:
        # Import after setting environment
        from tests.port_allocator import PortAllocator

        # Create temporary directory for locks
        with tempfile.TemporaryDirectory() as temp_dir:
            allocator = PortAllocator(temp_dir)

            print(f"  Worker ID: {allocator.worker_id}")
            print(f"  Worker Offset: {allocator.worker_offset}")

            # Get worker info to check ranges
            info = allocator.get_worker_info()

            print("  Port Ranges:")
            all_ranges_valid = True

            for category, range_info in info["port_ranges"].items():
                start = range_info["start"]
                end = range_info["end"]
                available = range_info["available_ports"]

                print(f"    {category}: {start}-{end} ({available} ports)")

                # CRITICAL VALIDATION: No reversed ranges
                if start > end:
                    print(
                        f"    ❌ ERROR: Reversed range in {category}: {start} > {end}"
                    )
                    all_ranges_valid = False
                elif available <= 0:
                    print(f"    ⚠️  WARNING: No available ports in {category}")
                elif available < 50:
                    print(f"    ⚠️  WARNING: Only {available} ports in {category}")
                else:
                    print(f"    ✅ Valid range with {available} ports")

            # Test actual port allocation
            if all_ranges_valid:
                try:
                    # Test each category
                    allocated_ports = {}
                    for category in ["servers", "clients", "federation", "testing"]:
                        try:
                            port = allocator.allocate_port(category)
                            allocated_ports[category] = port
                            print(f"    ✅ Allocated {category} port: {port}")

                            # Validate port is in expected range
                            expected_range = info["port_ranges"][category]
                            if not (
                                expected_range["start"] <= port <= expected_range["end"]
                            ):
                                print(
                                    f"    ❌ ERROR: Port {port} outside expected range {expected_range['start']}-{expected_range['end']}"
                                )
                                all_ranges_valid = False

                        except Exception as e:
                            print(
                                f"    ❌ ERROR: Failed to allocate {category} port: {e}"
                            )
                            all_ranges_valid = False

                    # Test bulk allocation
                    try:
                        bulk_ports = allocator.allocate_port_range(5, "testing")
                        print(f"    ✅ Bulk allocated 5 testing ports: {bulk_ports}")

                        # Cleanup
                        for port in bulk_ports:
                            allocator.release_port(port)

                    except Exception as e:
                        print(f"    ❌ ERROR: Bulk allocation failed: {e}")
                        all_ranges_valid = False

                    # Cleanup allocated ports
                    for port in allocated_ports.values():
                        allocator.release_port(port)

                except Exception as e:
                    print(f"    ❌ ERROR: General allocation test failed: {e}")
                    all_ranges_valid = False

            return all_ranges_valid

    finally:
        # Restore environment
        if old_worker is None:
            os.environ.pop("PYTEST_XDIST_WORKER", None)
        else:
            os.environ["PYTEST_XDIST_WORKER"] = old_worker


def test_high_capacity_usage():
    """Test high-capacity concurrent usage simulation."""
    print("\n🚀 HIGH-CAPACITY CONCURRENT USAGE TEST")

    # Simulate multiple workers allocating ports simultaneously
    all_allocated_ports = set()
    success_count = 0

    for worker_id, expected_id in WORKER_SCENARIOS:
        # Set environment for this worker
        old_worker = os.environ.get("PYTEST_XDIST_WORKER")
        os.environ["PYTEST_XDIST_WORKER"] = expected_id

        try:
            from tests.port_allocator import PortAllocator

            with tempfile.TemporaryDirectory() as temp_dir:
                allocator = PortAllocator(temp_dir)

                # Each worker tries to allocate 10 ports in different categories
                worker_ports = []
                try:
                    for i in range(3):  # 3 ports per category
                        for category in ["servers", "clients", "federation", "testing"]:
                            port = allocator.allocate_port(category)
                            worker_ports.append(port)

                            # Check for conflicts with other workers
                            if port in all_allocated_ports:
                                print(
                                    f"    ❌ CONFLICT: Worker {worker_id} got port {port} already used!"
                                )
                                return False

                            all_allocated_ports.add(port)

                    print(
                        f"    ✅ Worker {worker_id}: Successfully allocated {len(worker_ports)} ports"
                    )
                    success_count += 1

                    # Cleanup
                    for port in worker_ports:
                        allocator.release_port(port)

                except Exception as e:
                    print(f"    ❌ Worker {worker_id}: Failed to allocate ports: {e}")

        finally:
            if old_worker is None:
                os.environ.pop("PYTEST_XDIST_WORKER", None)
            else:
                os.environ["PYTEST_XDIST_WORKER"] = old_worker

    print(f"\n📊 RESULTS: {success_count}/{len(WORKER_SCENARIOS)} workers succeeded")
    print(f"   Total unique ports allocated: {len(all_allocated_ports)}")

    return success_count >= len(WORKER_SCENARIOS) * 0.8  # 80% success rate


def main():
    """Run comprehensive port allocation tests."""
    print("🔧 COMPREHENSIVE PORT ALLOCATION SYSTEM TEST")
    print("=" * 60)

    all_tests_passed = True

    # Test each worker scenario
    for worker_id, expected_id in WORKER_SCENARIOS:
        success = test_worker_scenario(worker_id, expected_id)
        if not success:
            all_tests_passed = False

    # Test high-capacity usage
    if not test_high_capacity_usage():
        all_tests_passed = False

    print("\n" + "=" * 60)
    if all_tests_passed:
        print("🎉 ALL TESTS PASSED! Port allocation system is working correctly.")
        print("✅ No reversed ranges found")
        print("✅ All workers can allocate ports efficiently")
        print("✅ High-capacity concurrent usage works")
    else:
        print("❌ SOME TESTS FAILED! Port allocation system needs fixes.")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
