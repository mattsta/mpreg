#!/usr/bin/env python3
"""
DEBUG: Minimal version of the failing large cluster test
"""

import pytest

from tests.conftest import AsyncTestContext
from tests.port_allocator import get_port_allocator


@pytest.fixture
def my_large_cluster_ports():
    """Exactly the same fixture as the failing test."""
    print("🔧 My large cluster ports fixture starting...")
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(50, "servers")
    print(f"   ✅ Allocated {len(ports)} ports")
    yield ports
    for port in ports:
        allocator.release_port(port)
    print("   🧹 Released ports")


class TestMinimalLargeCluster:
    """Minimal reproduction of the failing large cluster test."""

    @pytest.mark.asyncio
    async def test_minimal_30_node(
        self,
        test_context: AsyncTestContext,
        my_large_cluster_ports: list[int],
    ):
        """Minimal test that just uses the same fixtures as the failing test."""
        print("🎯 Test started!")
        print(f"   Got {len(my_large_cluster_ports)} ports")
        print(f"   Context has {len(test_context.servers)} servers")
        print("✅ Test completed!")


if __name__ == "__main__":
    import subprocess
    import sys

    print("🧪 Running minimal large cluster test...")
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                __file__ + "::TestMinimalLargeCluster::test_minimal_30_node",
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
            print("✅ Minimal test passed!")
        else:
            print(f"❌ Minimal test failed with code {result.returncode}")

    except subprocess.TimeoutExpired:
        print("❌ Minimal test timed out!")
