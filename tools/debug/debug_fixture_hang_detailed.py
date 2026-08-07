#!/usr/bin/env python3
"""
DEBUG: Find exactly where the large_cluster_ports fixture hangs
"""

import time

import pytest
from tests.port_allocator import get_port_allocator


@pytest.fixture
def debug_large_cluster_ports():
    """Debug version of large_cluster_ports with detailed logging."""
    print("🔧 STEP 1: Starting large cluster ports fixture")
    start_time = time.time()

    print("🔧 STEP 2: Getting port allocator...")
    allocator = get_port_allocator()
    print(f"   ✅ Got allocator in {time.time() - start_time:.2f}s: {allocator}")

    print("🔧 STEP 3: Allocating 50 ports...")
    step3_start = time.time()
    try:
        ports = allocator.allocate_port_range(50, "servers")
        print(f"   ✅ Allocated {len(ports)} ports in {time.time() - step3_start:.2f}s")
        print(f"   Ports: {ports[:5]}...{ports[-5:]}")
    except Exception as e:
        print(f"   ❌ FAILED to allocate ports: {e}")
        raise

    print(
        f"🔧 STEP 4: Yielding ports (total setup time: {time.time() - start_time:.2f}s)"
    )
    yield ports

    print("🔧 STEP 5: Starting cleanup...")
    cleanup_start = time.time()
    for i, port in enumerate(ports):
        try:
            allocator.release_port(port)
            if i % 10 == 0:
                print(f"   Released {i + 1}/{len(ports)} ports...")
        except Exception as e:
            print(f"   Warning: Failed to release port {port}: {e}")

    print(f"   ✅ Cleanup completed in {time.time() - cleanup_start:.2f}s")
    print(f"🔧 TOTAL FIXTURE TIME: {time.time() - start_time:.2f}s")


class TestFixtureDebug:
    """Test to isolate fixture hang."""

    def test_fixture_debug(self, debug_large_cluster_ports):
        """Simple test using the debug fixture."""
        print(f"🎯 Test running with {len(debug_large_cluster_ports)} ports")
        print("✅ Test completed successfully!")


if __name__ == "__main__":
    import subprocess
    import sys

    print("🔬 Testing large cluster ports fixture for hang...")

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                __file__ + "::TestFixtureDebug::test_fixture_debug",
                "-vs",
                "--tb=short",
                "-n0",
            ],
            timeout=60,
            capture_output=True,
            text=True,
        )

        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)

        if result.returncode == 0:
            print("✅ Debug fixture worked!")
        else:
            print(f"❌ Debug fixture failed: {result.returncode}")

    except subprocess.TimeoutExpired:
        print("❌ DEBUG FIXTURE TIMED OUT - REPRODUCED THE HANG!")
        print("This confirms the fixture setup is where the hang occurs")
