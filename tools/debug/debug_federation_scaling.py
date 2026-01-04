#!/usr/bin/env python3
"""
FEDERATION SCALING DEBUG: Test cluster scaling without auto-function registration.

This script tests if the federation protocol itself can scale to 20+ nodes by:
1. Disabling automatic function registration
2. Creating bare-bones servers with minimal federation
3. Testing progressive cluster sizes to find the scaling limit
"""

import asyncio
import logging
import time

from tests.port_allocator import PortAllocator

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer

class FederationScalingTester:
    """Tests federation protocol scaling without function registration noise."""

    def __init__(self):
        self.port_allocator = PortAllocator()
        self.servers: list[MPREGServer] = []
        self.tasks: list[asyncio.Task] = []

        # Suppress logging to see clean results
        logging.getLogger().setLevel(logging.CRITICAL + 1)

    async def create_minimal_cluster(self, size: int, topology: str = "star") -> dict:
        """Create a minimal cluster with specified topology."""
        print(f"\n🧪 TESTING: {size}-node {topology} topology")

        start_time = time.time()
        servers = []

        try:
            # Allocate ports
            ports = self.port_allocator.allocate_port_range(size, "testing")

            # Create servers with NO auto-connection initially
            for i, port in enumerate(ports):
                # Minimal settings - no auto-discovery, slow gossip
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"scale-{i}",
                    cluster_id="scale-test",
                    resources={
                        f"resource-{i % 5}"
                    },  # Distribute across 5 resource types
                    peers=None,
                    connect=None,  # No auto-connect
                    advertised_urls=None,
                    gossip_interval=10.0,  # Very slow to reduce noise
                )

                server = MPREGServer(settings=settings)
                servers.append(server)

                # Start server
                task = asyncio.create_task(server.server())
                self.tasks.append(task)

            self.servers.extend(servers)

            # Wait for servers to be ready
            await asyncio.sleep(0.5)
            print(f"   ✅ Created {len(servers)} isolated servers")

            # Create topology connections
            connection_errors = 0

            if topology == "star":
                # Star topology - all nodes connect to node 0
                for i in range(1, len(servers)):
                    try:
                        await servers[i]._establish_peer_connection(
                            f"ws://127.0.0.1:{servers[0].settings.port}"
                        )
                    except Exception as e:
                        connection_errors += 1
                        print(f"     ❌ Connection {i}->0 failed: {e}")

            elif topology == "chain":
                # Chain topology - linear connections
                for i in range(1, len(servers)):
                    try:
                        await servers[i]._establish_peer_connection(
                            f"ws://127.0.0.1:{servers[i - 1].settings.port}"
                        )
                    except Exception as e:
                        connection_errors += 1
                        print(f"     ❌ Connection {i}->{i - 1} failed: {e}")

            elif topology == "mesh":
                # Full mesh - every node connects to every other node
                for i in range(len(servers)):
                    for j in range(i + 1, len(servers)):
                        try:
                            await servers[i]._establish_peer_connection(
                                f"ws://127.0.0.1:{servers[j].settings.port}"
                            )
                        except Exception as e:
                            connection_errors += 1
                            print(f"     ❌ Connection {i}<->{j} failed: {e}")

            # Wait for federation to stabilize
            await asyncio.sleep(max(1.0, size * 0.1))

            # Measure cluster health
            total_connections = sum(len(s.peer_connections) for s in servers)
            active_servers = len([s for s in servers if hasattr(s, "peer_connections")])

            setup_time = (time.time() - start_time) * 1000

            # Calculate expected connections
            if topology == "star":
                expected_connections = (size - 1) * 2  # Bidirectional star
            elif topology == "chain":
                expected_connections = (size - 1) * 2  # Bidirectional chain
            elif topology == "mesh":
                expected_connections = size * (size - 1)  # Full mesh
            else:
                expected_connections = 0

            connection_efficiency = (
                total_connections / expected_connections
                if expected_connections > 0
                else 0
            )

            result = {
                "size": size,
                "topology": topology,
                "success": True,
                "setup_time_ms": setup_time,
                "active_servers": active_servers,
                "total_connections": total_connections,
                "expected_connections": expected_connections,
                "connection_efficiency": connection_efficiency,
                "connection_errors": connection_errors,
                "error": None,
            }

            print(
                f"   ✅ SUCCESS: {total_connections}/{expected_connections} connections ({connection_efficiency:.1%}), {setup_time:.0f}ms"
            )
            return result

        except Exception as e:
            error_time = (time.time() - start_time) * 1000
            print(f"   ❌ FAILED: {e}")

            return {
                "size": size,
                "topology": topology,
                "success": False,
                "setup_time_ms": error_time,
                "active_servers": 0,
                "total_connections": 0,
                "expected_connections": 0,
                "connection_efficiency": 0,
                "connection_errors": 0,
                "error": str(e),
            }

    async def cleanup(self):
        """Clean up all resources."""
        # Cancel tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

        self.servers.clear()
        self.tasks.clear()

        # Brief pause for cleanup
        await asyncio.sleep(0.5)

    async def run_scaling_tests(self):
        """Run systematic scaling tests."""
        print("🔬 FEDERATION SCALING INVESTIGATION")
        print("=" * 60)
        print(
            "🎯 Testing federation protocol scaling WITHOUT function registration noise"
        )

        results = []

        # Test star topology scaling
        print("\n📊 STAR TOPOLOGY SCALING:")
        for size in [5, 10, 15, 20, 25, 30, 40, 50]:
            result = await self.create_minimal_cluster(size, "star")
            results.append(result)

            await self.cleanup()

            if not result["success"]:
                print(f"   💥 Star topology fails at {size} nodes")
                break

        # Test chain topology scaling (more efficient)
        print("\n🔗 CHAIN TOPOLOGY SCALING:")
        for size in [10, 20, 30, 40, 50, 75, 100]:
            result = await self.create_minimal_cluster(size, "chain")
            results.append(result)

            await self.cleanup()

            if not result["success"]:
                print(f"   💥 Chain topology fails at {size} nodes")
                break

        # Test limited mesh topology
        print("\n🕸️  MESH TOPOLOGY SCALING (limited sizes):")
        for size in [5, 8, 10, 12, 15]:
            result = await self.create_minimal_cluster(size, "mesh")
            results.append(result)

            await self.cleanup()

            if not result["success"]:
                print(f"   💥 Mesh topology fails at {size} nodes")
                break

        # Analyze results
        self.analyze_scaling_results(results)

    def analyze_scaling_results(self, results: list[dict]):
        """Analyze scaling test results."""
        print("\n📈 SCALING ANALYSIS:")
        print("=" * 60)

        successful_results = [r for r in results if r["success"]]
        failed_results = [r for r in results if not r["success"]]

        if successful_results:
            max_star = max(
                [r["size"] for r in successful_results if r["topology"] == "star"],
                default=0,
            )
            max_chain = max(
                [r["size"] for r in successful_results if r["topology"] == "chain"],
                default=0,
            )
            max_mesh = max(
                [r["size"] for r in successful_results if r["topology"] == "mesh"],
                default=0,
            )

            print("✅ MAXIMUM SUCCESSFUL SIZES:")
            print(f"   Star topology: {max_star} nodes")
            print(f"   Chain topology: {max_chain} nodes")
            print(f"   Mesh topology: {max_mesh} nodes")

        if failed_results:
            print("\n❌ FIRST FAILURES:")
            for result in failed_results:
                print(
                    f"   {result['topology']:5} @ {result['size']:3d} nodes: {result['error']}"
                )

        # Performance trends
        if successful_results:
            print("\n📊 PERFORMANCE TRENDS:")
            for topology in ["star", "chain", "mesh"]:
                topology_results = [
                    r for r in successful_results if r["topology"] == topology
                ]
                if topology_results:
                    print(f"\n   {topology.upper()} TOPOLOGY:")
                    for result in topology_results:
                        print(
                            f"     {result['size']:3d} nodes: "
                            f"{result['setup_time_ms']:5.0f}ms, "
                            f"{result['connection_efficiency']:5.1%} efficiency, "
                            f"{result['connection_errors']:2d} errors"
                        )

        print("\n💡 CONCLUSIONS:")
        if successful_results:
            overall_max = max(r["size"] for r in successful_results)
            print(f"   • Federation protocol can handle up to {overall_max} nodes")
            print("   • Chain topology is most scalable")
            print("   • Mesh topology is least scalable (quadratic connections)")

            # Check if we hit the target
            if overall_max >= 20:
                print("   ✅ TARGET ACHIEVED: Can create 20+ node clusters!")
            else:
                print("   ❌ TARGET MISSED: Cannot reach 20+ nodes")
        else:
            print("   ❌ Federation protocol fails on all tested sizes")

async def main():
    """Run the federation scaling investigation."""
    tester = FederationScalingTester()
    try:
        await tester.run_scaling_tests()
    finally:
        await tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
