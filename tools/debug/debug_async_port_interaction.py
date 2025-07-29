#!/usr/bin/env python3
"""
DEBUG: Test if async fixture + large port allocation causes hang
"""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from tests.conftest import AsyncTestContext
from tests.port_allocator import get_port_allocator


@pytest_asyncio.fixture
async def my_test_context() -> AsyncGenerator[AsyncTestContext, None]:
    """Copy of the test_context fixture."""
    print("🔧 Setting up AsyncTestContext...")
    async with AsyncTestContext() as ctx:
        print("   ✅ AsyncTestContext ready")
        yield ctx
    print("   🧹 AsyncTestContext cleaned up")


@pytest.fixture
def my_large_ports():
    """Copy of large_cluster_ports fixture."""
    print("🔧 Setting up large ports...")
    allocator = get_port_allocator()
    print("   Allocating 50 ports...")
    ports = allocator.allocate_port_range(50, "servers")
    print(f"   ✅ Got {len(ports)} ports")
    yield ports
    print("   🧹 Releasing ports...")
    for port in ports:
        allocator.release_port(port)
    print("   ✅ Ports released")


class TestAsyncPortInteraction:
    """Test interaction between async context and port allocation."""

    def test_ports_only(self, my_large_ports):
        """Test ports fixture alone."""
        print(f"🎯 Got {len(my_large_ports)} ports")
        print("✅ Ports only test passed")

    @pytest.mark.asyncio
    async def test_async_context_only(self, my_test_context):
        """Test async context alone."""
        print(f"🎯 Got context with {len(my_test_context.servers)} servers")
        print("✅ Async context only test passed")

    @pytest.mark.asyncio
    async def test_both_fixtures(self, my_test_context, my_large_ports):
        """Test both fixtures together - this might hang!"""
        print(f"🎯 Got context with {len(my_test_context.servers)} servers")
        print(f"🎯 Got {len(my_large_ports)} ports")
        print("✅ Both fixtures test passed")


if __name__ == "__main__":
    import subprocess
    import sys

    tests_to_run = ["test_ports_only", "test_async_context_only", "test_both_fixtures"]

    for test_name in tests_to_run:
        print(f"\n{'=' * 50}")
        print(f"🧪 Running {test_name}...")
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    f"{__file__}::TestAsyncPortInteraction::{test_name}",
                    "-vs",
                    "--tb=short",
                    "-n0",
                ],
                timeout=30,
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED:")
                print(result.stdout)
                if result.stderr:
                    print("STDERR:", result.stderr)

        except subprocess.TimeoutExpired:
            print(f"❌ {test_name} TIMED OUT!")
            break  # Stop testing if we hit a hang
