#!/usr/bin/env python3
"""
Test 30-node auto-discovery with STATIC PORTS (like my debug script)
to verify if the port allocator is causing the pytest failures.
"""

import asyncio

import pytest

from tests.conftest import AsyncTestContext
from tests.test_comprehensive_auto_discovery import AutoDiscoveryTestHelpers


class Test30NodeStaticPorts(AutoDiscoveryTestHelpers):
    """Test 30-node auto-discovery with static ports."""

    @pytest.mark.asyncio
    async def test_30_node_static_ports(self, test_context: AsyncTestContext):
        """Test 30-node auto-discovery using static ports like the debug script."""
        # Use static ports like my debug script that WORKS
        static_ports = list(range(25000, 25030))  # 25000-25029

        print(
            f"\n🔧 Testing 30-node with STATIC PORTS: {static_ports[0]}-{static_ports[-1]}"
        )

        result = await self._test_cluster_auto_discovery(
            test_context, static_ports, "MULTI_HUB", expected_peers=29
        )

        assert result.success, f"30-node static ports failed: {result.failure_reason}"
        print("✅ SUCCESS with static ports!")


if __name__ == "__main__":
    # Also run directly like my debug script
    async def test_direct():
        """Test directly without pytest overhead."""
        from tests.conftest import AsyncTestContext

        test_context = AsyncTestContext()
        test_instance = Test30NodeStaticPorts()

        try:
            await test_instance.test_30_node_static_ports(test_context)
            print("🎉 DIRECT TEST PASSED!")
        finally:
            await test_context.cleanup()

    print("🧪 Running 30-node test with static ports...")
    asyncio.run(test_direct())
