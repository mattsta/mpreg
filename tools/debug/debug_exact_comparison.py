#!/usr/bin/env python3
"""
DEEP DIVE DEBUG: Find EXACT difference between working manual vs failing pytest

This will create the EXACT same 13-node cluster using both approaches to identify the differences.
"""

import asyncio
import contextlib

from tests.port_allocator import get_port_allocator

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


async def test_manual_approach():
    """Create 13-node cluster using manual approach (KNOWN TO WORK)."""
    print("🔧 MANUAL APPROACH: Creating 13-node cluster...")

    servers = []
    base_port = 25000
    num_hubs = min(3, 13)

    # Use EXACT same topology as pytest test
    for i in range(13):
        port = base_port + i

        # Multi-hub topology (same as pytest)
        peers = []
        if i < num_hubs:  # First 3 are hubs
            if i > 0:
                peers.append(f"ws://127.0.0.1:{base_port + (i - 1)}")
        else:
            # Non-hub nodes connect to one of the hubs (round-robin)
            hub_port = base_port + (i % num_hubs)
            peers.append(f"ws://127.0.0.1:{hub_port}")

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name=f"AutoDiscover-Node-{i}",
            cluster_id="test-cluster-13",
            resources={f"node-{i}", "test-resource"},
            peers=peers,
            gossip_interval=1.0,  # Same as pytest medium clusters
            log_level="ERROR",
        )

        server = MPREGServer(settings=settings)
        servers.append(server)

        # Start server
        asyncio.create_task(server.server())
        await asyncio.sleep(0.2)  # Same timing as pytest

        if (i + 1) % 5 == 0:
            print(f"  Started {i + 1}/13 servers...")

    print("✅ All 13 servers started manually")

    # Wait for discovery (same as pytest medium clusters)
    discovery_time = max(15.0, 13 * 1.0)  # Same formula as pytest
    print(f"⏳ Waiting {discovery_time}s for discovery...")
    await asyncio.sleep(discovery_time)

    # Check results
    print("📊 MANUAL RESULTS:")
    discovered_counts = []
    for i, server in enumerate(servers):
        peer_count = len(server.cluster.servers) - 1
        discovered_counts.append(peer_count)
        print(f"  Node {i}: {peer_count}/12 peers")

    success = all(count >= 12 for count in discovered_counts)
    print(f"MANUAL SUCCESS: {success}")

    # Cleanup
    for server in servers:
        with contextlib.suppress(Exception):
            await server.shutdown_async()

    return success, discovered_counts


async def test_pytest_approach():
    """Create 13-node cluster using pytest approach (FAILING)."""
    print("\n🔧 PYTEST APPROACH: Creating 13-node cluster...")

    # Use EXACT same fixtures as pytest
    test_context = AsyncTestContext()
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(20, "servers")  # medium_cluster_ports

    try:
        # Use EXACT same method as pytest test
        from tests.test_comprehensive_auto_discovery import AutoDiscoveryTestHelpers

        helper = AutoDiscoveryTestHelpers()

        print("📋 Creating servers using pytest helper...")
        servers = await helper._create_cluster_servers(
            test_context, ports[:13], "MULTI_HUB"
        )

        print("⏳ Waiting for discovery using pytest timing...")
        discovery_time = max(15.0, 13 * 1.0)  # Same as manual
        await asyncio.sleep(discovery_time)

        # Check results
        print("📊 PYTEST RESULTS:")
        discovered_counts = []
        for i, server in enumerate(servers):
            peer_count = len(server.cluster.servers) - 1
            discovered_counts.append(peer_count)
            print(f"  Node {i}: {peer_count}/12 peers")

        success = all(count >= 12 for count in discovered_counts)
        print(f"PYTEST SUCCESS: {success}")

        return success, discovered_counts

    finally:
        # Cleanup
        await test_context.cleanup()
        for port in ports:
            allocator.release_port(port)


async def main():
    """Compare both approaches to find the difference."""
    print("🔬 DEEP DIVE COMPARISON: Manual vs Pytest approaches")
    print("=" * 60)

    # Test manual approach first
    manual_success, manual_counts = await test_manual_approach()

    # Small delay between tests
    await asyncio.sleep(5)

    # Test pytest approach
    pytest_success, pytest_counts = await test_pytest_approach()

    # Analysis
    print("\n" + "=" * 60)
    print("🔍 ANALYSIS:")
    print(f"Manual approach: {manual_success} - counts: {manual_counts}")
    print(f"Pytest approach: {pytest_success} - counts: {pytest_counts}")

    if manual_success and not pytest_success:
        print("\n❌ PYTEST APPROACH FAILS - Need to identify the difference!")
        print("💡 Potential differences:")
        print("  1. Port allocation method (manual static vs allocator)")
        print("  2. Server creation timing/batching")
        print("  3. AsyncTestContext overhead")
        print("  4. Helper method differences")
    elif both_success := manual_success and pytest_success:
        print("\n✅ BOTH WORK - The issue may be intermittent or environment-specific")
    else:
        print("\n⚠️  BOTH FAIL - Issue may be fundamental")


if __name__ == "__main__":
    asyncio.run(main())
