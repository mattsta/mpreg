#!/usr/bin/env python3
"""
DEBUG: Exact replica of the failing test structure
"""

import pytest

from tests.conftest import AsyncTestContext
from tests.port_allocator import get_port_allocator


# EXACT copy of the fixtures from the failing test file
@pytest.fixture
def large_cluster_ports():
    """Pytest fixture for large cluster testing (50 ports)."""
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(50, "servers")
    yield ports
    for port in ports:
        allocator.release_port(port)


class TestAutoDiscoveryLargeClusters:
    """EXACT copy of the failing test class."""

    @pytest.mark.slow
    async def test_30_node_multi_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        large_cluster_ports: list[int],
    ):
        """EXACT copy of the failing test method signature."""
        print("🎯 Test started!")
        print(f"   Got {len(large_cluster_ports)} ports")
        print(f"   Context has {len(test_context.servers)} servers")
        print("✅ Test completed!")


if __name__ == "__main__":
    import subprocess
    import sys

    print("🧪 Running exact replica of failing test...")
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                f"{__file__}::TestAutoDiscoveryLargeClusters::test_30_node_multi_hub_auto_discovery",
                "-vs",
                "--tb=short",
                "-n0",
                "--setup-show",
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
            print("✅ Exact replica PASSED!")
        else:
            print(f"❌ Exact replica FAILED with code {result.returncode}")

    except subprocess.TimeoutExpired:
        print("❌ Exact replica TIMED OUT - reproduced the hang!")
