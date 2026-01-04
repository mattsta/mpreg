#!/usr/bin/env python3
"""
DEBUG: Test if the large_cluster_ports fixture hangs
"""

import pytest
from tests.port_allocator import get_port_allocator

@pytest.fixture
def debug_large_ports():
    """Debug version of large_cluster_ports with detailed logging."""
    print("🔧 Starting large port allocation...")
    allocator = get_port_allocator()
    print(f"   Got allocator: {allocator}")

    print("   Allocating 50 ports...")
    ports = allocator.allocate_port_range(50, "servers")
    print(f"   ✅ Allocated {len(ports)} ports: {ports[:5]}...")

    yield ports

    print("   🧹 Cleaning up ports...")
    for port in ports:
        allocator.release_port(port)
    print("   ✅ Cleanup complete")

class TestFixtureHang:
    """Test if large cluster fixtures hang."""

    def test_simple(self):
        """Simple test that should always work."""
        print("✅ Simple test works!")

    def test_with_large_ports(self, debug_large_ports):
        """Test with large port allocation to see if fixture hangs."""
        print(f"🎯 Got {len(debug_large_ports)} ports!")
        print("✅ Large ports test works!")

if __name__ == "__main__":
    print("🧪 Running pytest with debug fixture...")
    import subprocess
    import sys

    # Run pytest on this file
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            __file__ + "::TestFixtureHang::test_simple",
            "-vs",
            "--tb=short",
        ],
        timeout=30,
    )

    if result.returncode == 0:
        print("✅ Simple test passed")

        print("\n🔬 Now testing large ports fixture...")
        result2 = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                __file__ + "::TestFixtureHang::test_with_large_ports",
                "-vs",
                "--tb=short",
            ],
            timeout=30,
        )

        if result2.returncode == 0:
            print("✅ Large ports fixture works!")
        else:
            print("❌ Large ports fixture failed/hung!")
    else:
        print("❌ Simple test failed!")
