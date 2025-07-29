#!/usr/bin/env python3
"""
ROOT CAUSE DEBUG: Find exactly what makes the system explode at scale.

This script tests ONLY the core components to isolate the problem:
1. Test just port allocation without servers
2. Test just server creation without connections
3. Test just connection establishment without federation
4. Test step-by-step to find the exact breaking point
"""

import asyncio
import time

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.port_allocator import PortAllocator


class RootCauseInvestigator:
    """Isolates the exact root cause of scaling failures."""

    def __init__(self):
        self.port_allocator = PortAllocator()
        print("🔬 ROOT CAUSE INVESTIGATION")
        print("=" * 50)

    async def test_1_port_allocation_only(self, sizes: list[int]):
        """Test 1: Can we allocate large numbers of ports?"""
        print("\n🧪 TEST 1: Port allocation capacity")

        for size in sizes:
            try:
                start = time.time()
                ports = self.port_allocator.allocate_port_range(size, "testing")
                alloc_time = (time.time() - start) * 1000

                print(
                    f"   ✅ {size:3d} ports: {alloc_time:5.1f}ms ({ports[0]}-{ports[-1]})"
                )

                # Release ports immediately
                for port in ports:
                    self.port_allocator.release_port(port)

            except Exception as e:
                print(f"   ❌ {size:3d} ports: FAILED - {e}")
                return size

        print("   ✅ Port allocation works fine")
        return None

    async def test_2_server_creation_only(self, sizes: list[int]):
        """Test 2: Can we create servers without starting them?"""
        print("\n🧪 TEST 2: Server object creation (no tasks)")

        for size in sizes:
            try:
                start = time.time()
                ports = self.port_allocator.allocate_port_range(size, "testing")
                servers = []

                for i, port in enumerate(ports):
                    settings = MPREGSettings(
                        host="127.0.0.1",
                        port=port,
                        name=f"test-{i}",
                        cluster_id="test",
                        resources={f"res-{i}"},
                        peers=None,
                        connect=None,
                        advertised_urls=None,
                        gossip_interval=60.0,  # Very slow
                    )

                    server = MPREGServer(settings=settings)
                    servers.append(server)

                create_time = (time.time() - start) * 1000
                print(f"   ✅ {size:3d} servers: {create_time:5.1f}ms")

                # Clean up
                del servers
                for port in ports:
                    self.port_allocator.release_port(port)

            except Exception as e:
                print(f"   ❌ {size:3d} servers: FAILED - {e}")
                return size

        print("   ✅ Server creation works fine")
        return None

    async def test_3_server_startup_only(self, sizes: list[int]):
        """Test 3: Can we start servers without connections?"""
        print("\n🧪 TEST 3: Server startup (isolated, no connections)")

        for size in sizes:
            try:
                start = time.time()
                ports = self.port_allocator.allocate_port_range(size, "testing")
                servers = []
                tasks = []

                for i, port in enumerate(ports):
                    settings = MPREGSettings(
                        host="127.0.0.1",
                        port=port,
                        name=f"test-{i}",
                        cluster_id="test",
                        resources={f"res-{i}"},
                        peers=None,
                        connect=None,  # NO connections
                        advertised_urls=None,
                        gossip_interval=60.0,  # Very slow
                    )

                    server = MPREGServer(settings=settings)
                    servers.append(server)

                    # Start server but don't connect
                    task = asyncio.create_task(server.server())
                    tasks.append(task)

                # Wait briefly for startup
                await asyncio.sleep(0.5)

                startup_time = (time.time() - start) * 1000
                print(f"   ✅ {size:3d} isolated servers: {startup_time:5.1f}ms")

                # Clean up
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

                for port in ports:
                    self.port_allocator.release_port(port)

            except Exception as e:
                print(f"   ❌ {size:3d} isolated servers: FAILED - {e}")
                return size

        print("   ✅ Isolated server startup works fine")
        return None

    async def test_4_single_connection(self, sizes: list[int]):
        """Test 4: Can we create star topology (1 connection per node)?"""
        print("\n🧪 TEST 4: Star topology (single connection per node)")

        for size in sizes:
            if size < 2:
                continue

            try:
                start = time.time()
                ports = self.port_allocator.allocate_port_range(size, "testing")
                servers = []
                tasks = []

                # Create servers
                for i, port in enumerate(ports):
                    settings = MPREGSettings(
                        host="127.0.0.1",
                        port=port,
                        name=f"test-{i}",
                        cluster_id="test",
                        resources={f"res-{i}"},
                        peers=None,
                        connect=None,  # NO auto-connect
                        advertised_urls=None,
                        gossip_interval=60.0,  # Very slow
                    )

                    server = MPREGServer(settings=settings)
                    servers.append(server)

                    task = asyncio.create_task(server.server())
                    tasks.append(task)

                # Wait for startup
                await asyncio.sleep(0.5)

                # Create star connections (all to node 0)
                connection_start = time.time()
                for i in range(1, min(size, 5)):  # Limit to first 4 connections
                    await servers[i]._establish_peer_connection(
                        f"ws://127.0.0.1:{ports[0]}"
                    )
                    await asyncio.sleep(0.1)  # Small delay between connections

                connection_time = (time.time() - connection_start) * 1000
                total_time = (time.time() - start) * 1000

                print(
                    f"   ✅ {size:3d} nodes (4 connections): {total_time:5.1f}ms total, {connection_time:5.1f}ms connections"
                )

                # Clean up quickly
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

                for port in ports:
                    self.port_allocator.release_port(port)

            except Exception as e:
                print(f"   ❌ {size:3d} star topology: FAILED - {e}")
                return size

        print("   ✅ Star topology connections work fine")
        return None

    async def run_investigation(self):
        """Run the complete root cause investigation."""
        test_sizes = [5, 10, 20, 30, 50, 100]

        # Test each component incrementally
        print("🎯 Testing each component to isolate the scaling problem...")

        # Test 1: Port allocation
        failure_size = await self.test_1_port_allocation_only(test_sizes)
        if failure_size:
            print(f"\n💥 ROOT CAUSE: Port allocation fails at {failure_size} ports")
            return

        # Test 2: Server creation
        failure_size = await self.test_2_server_creation_only(test_sizes)
        if failure_size:
            print(f"\n💥 ROOT CAUSE: Server creation fails at {failure_size} servers")
            return

        # Test 3: Server startup
        failure_size = await self.test_3_server_startup_only([5, 10, 15, 20, 25])
        if failure_size:
            print(f"\n💥 ROOT CAUSE: Server startup fails at {failure_size} servers")
            return

        # Test 4: Connections
        failure_size = await self.test_4_single_connection([5, 10, 15, 20, 25, 30])
        if failure_size:
            print(f"\n💥 ROOT CAUSE: Star connections fail at {failure_size} servers")
            return

        print("\n✅ ALL TESTS PASSED")
        print("🤔 The issue may be in:")
        print("   • Full mesh connections (O(n²) complexity)")
        print("   • Federation protocol overhead")
        print("   • Default function auto-registration")
        print("   • Logging system being overwhelmed")


async def main():
    """Run the root cause investigation."""
    investigator = RootCauseInvestigator()
    await investigator.run_investigation()


if __name__ == "__main__":
    # Suppress ALL output except our prints
    import logging

    logging.getLogger().setLevel(logging.CRITICAL + 1)

    asyncio.run(main())
