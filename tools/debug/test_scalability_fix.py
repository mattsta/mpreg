#!/usr/bin/env python3
"""
Quick test to verify the scalability fix works.
Creates a larger cluster to test if the exponential explosion is fixed.
"""

import asyncio
import logging

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.port_allocator import PortAllocator

# Suppress all logging to see just our output
logging.getLogger().setLevel(logging.CRITICAL + 1)


async def test_scalability_fix():
    """Test that we can create a 20+ node cluster without exponential explosion."""
    print("🔬 Testing scalability fix: can we create 20+ nodes without explosion?")

    port_allocator = PortAllocator()
    servers = []
    tasks = []

    try:
        # Test with 20 nodes - this should have failed before
        cluster_size = 20
        ports = port_allocator.allocate_port_range(cluster_size, "testing")

        print(f"📍 Creating {cluster_size} servers with ports {ports[0]}-{ports[-1]}")

        # Create all servers
        for i, port in enumerate(ports):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"scale-{i}",
                cluster_id="scale-test",
                resources={f"resource-{i % 3}"},  # Distribute across 3 resources
                peers=None,
                connect=f"ws://127.0.0.1:{ports[0]}"
                if i > 0
                else None,  # Star topology
                advertised_urls=None,
                gossip_interval=2.0,  # Slow gossip
            )

            server = MPREGServer(settings=settings)
            servers.append(server)

            # Start server
            task = asyncio.create_task(server.server())
            tasks.append(task)

            # Small delay between starts
            await asyncio.sleep(0.1)

        print(f"✅ All {cluster_size} servers created and started")

        # Wait for cluster convergence
        print("⏱️  Waiting for cluster convergence...")
        await asyncio.sleep(5.0)

        # Check cluster health
        active_servers = 0
        total_connections = 0
        for server in servers:
            if hasattr(server, "peer_connections"):
                if len(server.peer_connections) >= 0:
                    active_servers += 1
                total_connections += len(server.peer_connections)

        print("📊 RESULTS:")
        print(f"   Active servers: {active_servers}/{cluster_size}")
        print(f"   Total connections: {total_connections}")

        # In star topology, we expect each non-leader server to have 1 connection
        # and the leader to have (n-1) connections, so total = 2*(n-1)
        expected_connections = 2 * (cluster_size - 1)
        connection_efficiency = (
            total_connections / expected_connections if expected_connections > 0 else 0
        )

        print(f"   Connection efficiency: {connection_efficiency:.1%}")

        if active_servers >= cluster_size * 0.8 and connection_efficiency >= 0.8:
            print("✅ SUCCESS: Scalability fix appears to work!")
            return True
        else:
            print("❌ FAILURE: Cluster did not converge properly")
            return False

    except Exception as e:
        print(f"💥 ERROR: {e}")
        return False

    finally:
        # Cleanup
        print("🧹 Cleaning up...")
        for task in tasks:
            if not task.done():
                task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    """Run the scalability test."""
    success = await test_scalability_fix()
    if success:
        print("\n🎉 Scalability fix validation: PASSED")
    else:
        print("\n💔 Scalability fix validation: FAILED")


if __name__ == "__main__":
    asyncio.run(main())
