#!/usr/bin/env python3
"""
SCALABILITY INVESTIGATION: Find root cause why we can't create 20+ node clusters.

This script systematically creates clusters of increasing size to identify:
1. At what point does cluster creation fail?
2. What specific errors occur during scaling?
3. What resource limits are being hit?
4. Is it network ports, memory, connections, or something else?
"""

import asyncio
import resource
import time
from typing import Any, cast

import psutil

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.port_allocator import PortAllocator


class ScalabilityAnalyzer:
    """Analyzes cluster scalability limits and identifies bottlenecks."""

    def __init__(self):
        self.port_allocator = PortAllocator()
        self.servers: list[MPREGServer] = []
        self.tasks: list[asyncio.Task] = []
        self.results: list[dict[str, Any]] = []

    async def test_cluster_size(self, size: int) -> dict[str, Any]:
        """Test creating a cluster of a specific size and measure what happens."""
        print(f"\n🔍 TESTING CLUSTER SIZE: {size} nodes")

        start_time = time.time()
        memory_before = psutil.virtual_memory().used

        try:
            # Allocate ports
            ports = self.port_allocator.allocate_port_range(size, "scalability")
            print(f"   ✅ Allocated ports: {ports[0]}-{ports[-1]}")

            # Create servers
            servers = []
            for i, port in enumerate(ports):
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"scale-test-{i}",
                    cluster_id="scalability-test",
                    resources={f"resource-{i % 3}"},  # Distribute resources
                    peers=None,
                    connect=f"ws://127.0.0.1:{ports[0]}" if i > 0 else None,
                    advertised_urls=None,
                    gossip_interval=1.0,  # Standard interval
                )

                server = MPREGServer(settings=settings)
                servers.append(server)

                # Start server
                task = asyncio.create_task(server.server())
                self.tasks.append(task)

                # Small delay to avoid overwhelming the system
                await asyncio.sleep(0.05)

            self.servers.extend(servers)

            # Wait for cluster convergence
            print("   ⏱️  Waiting for cluster convergence...")
            await asyncio.sleep(max(2.0, size * 0.1))

            # Create connections between all nodes (full mesh test)
            connection_errors = 0
            for i in range(1, len(servers)):
                try:
                    await servers[i]._establish_peer_connection(
                        f"ws://127.0.0.1:{servers[0].settings.port}"
                    )
                except Exception as e:
                    connection_errors += 1
                    print(f"     ⚠️  Connection error {i}: {e}")

            # Wait for federation propagation
            await asyncio.sleep(max(1.0, size * 0.05))

            # Check cluster health
            active_servers = sum(1 for s in servers if len(s.peer_connections) >= 0)
            total_connections = sum(len(s.peer_connections) for s in servers)

            # Measure resource usage
            memory_after = psutil.virtual_memory().used
            memory_delta = memory_after - memory_before

            setup_time = (time.time() - start_time) * 1000

            result = {
                "size": size,
                "success": True,
                "setup_time_ms": setup_time,
                "active_servers": active_servers,
                "total_connections": total_connections,
                "connection_errors": connection_errors,
                "memory_used_mb": memory_delta / (1024 * 1024),
                "ports_used": len(ports),
                "error_message": None,
            }

            print(
                f"   ✅ SUCCESS: {active_servers}/{size} servers, {total_connections} connections"
            )
            print(
                f"   📊 Memory: {memory_delta / (1024 * 1024):.1f}MB, Time: {setup_time:.0f}ms"
            )

            return result

        except Exception as e:
            error_time = (time.time() - start_time) * 1000

            result = {
                "size": size,
                "success": False,
                "setup_time_ms": error_time,
                "active_servers": 0,
                "total_connections": 0,
                "connection_errors": 0,
                "memory_used_mb": 0,
                "ports_used": 0,
                "error_message": cast(Any, str(e)),
            }

            print(f"   ❌ FAILED: {e}")
            return result

    async def cleanup(self):
        """Clean up all servers and tasks."""
        print(
            f"\n🧹 Cleaning up {len(self.servers)} servers and {len(self.tasks)} tasks..."
        )

        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

        self.servers.clear()
        self.tasks.clear()
        # Release all ports individually
        for port in list(self.port_allocator._allocated_ports):
            self.port_allocator.release_port(port)

        # Give system time to clean up
        await asyncio.sleep(1.0)

    async def run_scalability_analysis(self):
        """Run systematic scalability analysis."""
        print("🔬 MPREG SCALABILITY INVESTIGATION")
        print("=" * 60)

        # Get system limits
        print("📊 SYSTEM RESOURCE LIMITS:")
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"   File descriptors: {soft_limit} (soft) / {hard_limit} (hard)")
        print(
            f"   Available memory: {psutil.virtual_memory().available / (1024**3):.1f} GB"
        )
        print(f"   CPU cores: {psutil.cpu_count()}")

        # Test increasingly large cluster sizes
        test_sizes = [5, 10, 15, 20, 25, 30, 40, 50, 75, 100]

        for size in test_sizes:
            try:
                result = await self.test_cluster_size(size)
                self.results.append(result)

                # If this size failed, don't try larger sizes
                if not result["success"]:
                    print(f"   💡 Stopping at size {size} due to failure")
                    break

                # Clean up between tests to avoid resource accumulation
                await self.cleanup()

            except KeyboardInterrupt:
                print("\n⛔ Test interrupted by user")
                break
            except Exception as e:
                print(f"   💥 Unexpected error at size {size}: {e}")
                break

        # Final cleanup
        await self.cleanup()

        # Analyze results
        self.analyze_results()

    def analyze_results(self):
        """Analyze the scalability test results."""
        print("\n📈 SCALABILITY ANALYSIS RESULTS:")
        print("=" * 60)

        if not self.results:
            print("❌ No results to analyze")
            return

        # Find maximum successful size
        successful_results = [r for r in self.results if r["success"]]
        failed_results = [r for r in self.results if not r["success"]]

        if successful_results:
            max_size = max(r["size"] for r in successful_results)
            print(f"✅ Maximum successful cluster size: {max_size} nodes")

            # Analyze trends in successful results
            print("\n📊 PERFORMANCE TRENDS:")
            for result in successful_results:
                print(
                    f"   Size {result['size']:3d}: "
                    f"{result['setup_time_ms']:5.0f}ms setup, "
                    f"{result['memory_used_mb']:5.1f}MB memory, "
                    f"{result['total_connections']:3d} connections"
                )

        if failed_results:
            print("\n❌ FAILURE ANALYSIS:")
            for result in failed_results:
                print(f"   Size {result['size']:3d}: {result['error']}")

        # Identify bottlenecks
        print("\n🔍 BOTTLENECK IDENTIFICATION:")

        if successful_results:
            # Memory usage trend
            memory_per_node = [
                r["memory_used_mb"] / r["size"] for r in successful_results
            ]
            avg_memory_per_node = sum(memory_per_node) / len(memory_per_node)
            print(f"   💾 Average memory per node: {avg_memory_per_node:.2f} MB")

            # Connection trend
            for result in successful_results:
                expected_connections = (
                    result["size"] - 1
                )  # Each node connects to leader
                actual_connections = result["total_connections"]
                efficiency = (
                    actual_connections / expected_connections
                    if expected_connections > 0
                    else 0
                )
                print(
                    f"   🔗 Size {result['size']:3d}: {efficiency:.1%} connection efficiency"
                )

        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        if failed_results:
            first_failure = failed_results[0]
            print(f"   • System fails at {first_failure['size']} nodes")
            print(f"   • Error type: {first_failure['error']}")

            if (
                "port" in first_failure["error"].lower()
                or "address" in first_failure["error"].lower()
            ):
                print("   🔧 LIKELY CAUSE: Network port exhaustion")
                print("   🔧 SOLUTION: Implement port reuse or connection pooling")
            elif "memory" in first_failure["error"].lower():
                print("   🔧 LIKELY CAUSE: Memory exhaustion")
                print("   🔧 SOLUTION: Optimize memory usage per node")
            elif "timeout" in first_failure["error"].lower():
                print("   🔧 LIKELY CAUSE: Connection timeout due to system overload")
                print("   🔧 SOLUTION: Optimize connection establishment timing")
            else:
                print("   🔧 UNKNOWN CAUSE: Requires deeper investigation")


async def main():
    """Run the scalability investigation."""
    analyzer = ScalabilityAnalyzer()
    try:
        await analyzer.run_scalability_analysis()
    finally:
        await analyzer.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
