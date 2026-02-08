"""
Clean Performance Research Suite - Focused Testing with Minimal Logging.

This module provides clean, focused performance research tests that demonstrate
advanced topological capabilities without excessive logging output.
"""

import asyncio
import time
from typing import Any

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import get_port_allocator
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext

pytestmark = pytest.mark.slow


class CleanPerformanceResearch:
    """Clean performance research with minimal logging."""

    def __init__(self, test_context: AsyncTestContext):
        self.test_context = test_context
        self.port_allocator = get_port_allocator()

    async def _cleanup_context_slice(
        self, *, tasks_start: int, servers_start: int
    ) -> None:
        """Shutdown and remove test-context resources created after slice markers."""
        new_servers = self.test_context.servers[servers_start:]
        if new_servers:
            await asyncio.gather(
                *(server.shutdown_async() for server in new_servers),
                return_exceptions=True,
            )

        new_tasks = self.test_context.tasks[tasks_start:]
        if new_tasks:
            await asyncio.wait(new_tasks, timeout=max(5.0, len(new_tasks) * 0.5))
        for task in new_tasks:
            if not task.done():
                task.cancel()
        if new_tasks:
            await asyncio.gather(*new_tasks, return_exceptions=True)

        del self.test_context.tasks[tasks_start:]
        del self.test_context.servers[servers_start:]
        await asyncio.sleep(0.05)

    async def test_gossip_federation_gossip_topology(
        self, cluster_sizes: list[int] = [12, 15, 9]
    ) -> dict[str, Any]:
        """Test ACTUAL Gossip↔Federation↔Gossip topology with proper federation bridges."""

        print("\n🏗️  Testing REAL Gossip↔Federation↔Gossip topology")
        print(f"   Cluster sizes: {cluster_sizes}")

        start_time = time.time()

        # 1. Create separate gossip clusters with SAME cluster_id for federation
        all_servers = []
        cluster_hubs = []  # Track the hub nodes for federation bridges
        total_nodes = 0

        for i, size in enumerate(cluster_sizes):
            ports = self.port_allocator.allocate_port_range(size, "research")

            # Create star hub topology for each cluster
            servers = []
            for j, port in enumerate(ports):
                connect_to = f"ws://127.0.0.1:{ports[0]}" if j > 0 else None

                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"cluster-{i}-node-{j}",
                    cluster_id="federation-research",  # SAME cluster_id for federation!
                    resources={f"cluster-{i}-resource-{j}"},
                    peers=None,  # Auto-discovery
                    connect=connect_to,
                    advertised_urls=None,
                    gossip_interval=0.5,
                )

                server = MPREGServer(settings=settings)
                servers.append(server)

                # Track hub nodes (first node of each cluster) for federation
                if j == 0:
                    cluster_hubs.append((port, server))

            all_servers.extend(servers)
            total_nodes += size

            # Start servers with minimal delay
            for server in servers:
                task = asyncio.create_task(server.server())
                self.test_context.tasks.append(task)
                await asyncio.sleep(0.1)

        # 2. Wait for intra-cluster convergence
        print("   Waiting for intra-cluster convergence...")
        await asyncio.sleep(max(3.0, total_nodes * 0.1))

        # 3. Create federation bridges between cluster hubs
        print(
            f"   Creating federation bridges between {len(cluster_hubs)} cluster hubs..."
        )
        federation_connections = []

        for i in range(len(cluster_hubs)):
            hub_port, hub_server = cluster_hubs[i]

            # Connect each hub to the next hub (ring topology for federation)
            next_i = (i + 1) % len(cluster_hubs)
            next_hub_port, next_hub_server = cluster_hubs[next_i]

            if i != next_i:  # Don't connect to self
                print(f"   Connecting hub {hub_port} to hub {next_hub_port}")

                # Create federation connection
                try:
                    # Add peer connection for federation
                    await hub_server._establish_peer_connection(
                        f"ws://127.0.0.1:{next_hub_port}"
                    )
                    federation_connections.append((hub_port, next_hub_port))
                except Exception as e:
                    print(f"   Warning: Federation connection failed: {e}")

        # 4. Wait for federation convergence
        print("   Waiting for federation convergence...")
        await asyncio.sleep(max(5.0, len(cluster_sizes) * 1.0))

        self.test_context.servers.extend(all_servers)
        setup_time = (time.time() - start_time) * 1000

        # 5. Test cross-cluster function propagation
        print("   Testing cross-cluster function propagation...")

        def research_test_function(data: str) -> str:
            return f"Federation research test: {data}"

        propagation_start = time.time()
        # Register function on first cluster's hub
        all_servers[0].register_command(
            "research_test", research_test_function, ["federation-resource"]
        )

        # Wait for propagation across federation
        await asyncio.sleep(max(3.0, len(cluster_sizes) * 0.5))
        propagation_time = (time.time() - propagation_start) * 1000

        # Count successful propagations
        nodes_with_function = sum(
            1 for server in all_servers if "research_test" in server.cluster.funtimes
        )

        # Calculate connection efficiency
        total_connections = sum(len(server.peer_connections) for server in all_servers)
        # Theoretical max: intra-cluster connections + federation bridges
        intra_cluster_max = sum(size * (size - 1) for size in cluster_sizes)
        federation_bridge_max = len(cluster_sizes) * 2  # Ring of hubs
        theoretical_max = intra_cluster_max + federation_bridge_max
        connection_efficiency = (
            total_connections / theoretical_max if theoretical_max > 0 else 0
        )

        success_rate = nodes_with_function / total_nodes

        print(
            f"   Results: {nodes_with_function}/{total_nodes} nodes have function ({success_rate:.2%})"
        )
        print(
            f"   Connections: {total_connections}/{theoretical_max} ({connection_efficiency:.2%})"
        )

        return {
            "topology": "Gossip↔Federation↔Gossip",
            "cluster_sizes": cluster_sizes,
            "total_nodes": total_nodes,
            "setup_time_ms": setup_time,
            "propagation_time_ms": propagation_time,
            "connection_efficiency": connection_efficiency,
            "function_success_rate": success_rate,
            "total_connections": total_connections,
            "federation_connections": len(federation_connections),
        }

    async def test_scalability_analysis(
        self, test_sizes: list[int] = [15, 20, 25, 30]
    ) -> list[dict[str, Any]]:
        """Clean scalability analysis across cluster sizes."""

        print("\n📊 Scalability Analysis")
        print(f"   Testing cluster sizes: {test_sizes}")

        results = []

        for size in test_sizes:
            print(f"   → Testing {size} nodes...", end=" ")

            start_time = time.time()

            # Allocate ports
            ports = self.port_allocator.allocate_port_range(size, "research")
            tasks_start = len(self.test_context.tasks)
            servers_start = len(self.test_context.servers)

            # Create star hub cluster
            servers = []
            try:
                for i, port in enumerate(ports):
                    connect_to = f"ws://127.0.0.1:{ports[0]}" if i > 0 else None

                    settings = MPREGSettings(
                        host="127.0.0.1",
                        port=port,
                        name=f"scale-node-{i}",
                        cluster_id=f"scale-test-{size}",
                        resources={f"scale-resource-{i}"},
                        peers=None,
                        connect=connect_to,
                        advertised_urls=None,
                        gossip_interval=0.5,
                    )

                    server = MPREGServer(settings=settings)
                    servers.append(server)

                self.test_context.servers.extend(servers)

                # Start servers
                for server in servers:
                    task = asyncio.create_task(server.server())
                    self.test_context.tasks.append(task)
                    await asyncio.sleep(0.05)  # Very minimal delay

                # Wait for convergence
                await asyncio.sleep(max(2.0, size * 0.15))
                setup_time = (time.time() - start_time) * 1000

                # Test function propagation
                def scale_test_function(data: str) -> str:
                    return f"Scale test: {data}"

                prop_start = time.time()
                servers[0].register_command(
                    "scale_test", scale_test_function, ["scale-resource"]
                )
                await asyncio.sleep(max(1.0, size * 0.05))

                # Count propagation success
                nodes_with_function = sum(
                    1 for server in servers if "scale_test" in server.cluster.funtimes
                )
                propagation_time = (time.time() - prop_start) * 1000

                # Calculate metrics
                total_connections = sum(
                    len(server.peer_connections) for server in servers
                )
                theoretical_max = size * (size - 1)
                efficiency = (
                    total_connections / theoretical_max if theoretical_max > 0 else 0
                )
                success_rate = nodes_with_function / size

                result = {
                    "cluster_size": size,
                    "setup_time_ms": setup_time,
                    "propagation_time_ms": propagation_time,
                    "connection_efficiency": efficiency,
                    "function_success_rate": success_rate,
                    "total_connections": total_connections,
                }

                results.append(result)

                print(
                    f"Setup: {setup_time:.0f}ms, Efficiency: {efficiency:.2%}, Success: {success_rate:.2%}"
                )
            finally:
                await self._cleanup_context_slice(
                    tasks_start=tasks_start,
                    servers_start=servers_start,
                )
                for port in ports:
                    self.port_allocator.release_port(port)

        return results


class TestCleanPerformanceResearch:
    """Clean performance research test suite."""

    async def test_multi_cluster_federation_topology(
        self,
        test_context: AsyncTestContext,
    ):
        """Test multi-cluster federation topology with clean output."""

        researcher = CleanPerformanceResearch(test_context)

        # Test 3-cluster federation
        result = await researcher.test_gossip_federation_gossip_topology([4, 5, 3])

        print("\n📊 MULTI-CLUSTER FEDERATION RESULTS:")
        print(f"   Topology: {result['topology']}")
        print(f"   Cluster sizes: {result['cluster_sizes']}")
        print(f"   Total nodes: {result['total_nodes']}")
        print(f"   Setup time: {result['setup_time_ms']:.0f}ms")
        print(f"   Function propagation: {result['propagation_time_ms']:.0f}ms")
        print(f"   Connection efficiency: {result['connection_efficiency']:.2%}")
        print(f"   Function success rate: {result['function_success_rate']:.2%}")
        print(f"   Total connections: {result['total_connections']}")

        # Validate results
        assert result["total_nodes"] == 12
        assert result["function_success_rate"] >= 0.8  # At least 80% success
        assert result["connection_efficiency"] > 0.5  # At least 50% efficiency

        print("✅ Multi-cluster federation topology successful")

    async def test_scalability_boundary_research(
        self,
        test_context: AsyncTestContext,
    ):
        """Research scalability boundaries with clean analysis."""

        researcher = CleanPerformanceResearch(test_context)

        # Test scalability across different sizes
        results = await researcher.test_scalability_analysis([5, 8, 12, 16])

        print("\n📈 SCALABILITY BOUNDARY RESEARCH:")
        print(f"   Test configurations: {len(results)}")

        # Analyze trends
        for i, result in enumerate(results):
            size = result["cluster_size"]
            setup = result["setup_time_ms"]
            propagation = result["propagation_time_ms"]
            efficiency = result["connection_efficiency"]
            success = result["function_success_rate"]

            print(
                f"   {size:2d} nodes: setup={setup:4.0f}ms, propagation={propagation:4.0f}ms, "
                f"efficiency={efficiency:.2%}, success={success:.2%}"
            )

        # Find performance trends
        setup_times = [r["setup_time_ms"] for r in results]
        efficiencies = [r["connection_efficiency"] for r in results]
        success_rates = [r["function_success_rate"] for r in results]

        # Simple trend analysis
        setup_trend = "increasing" if setup_times[-1] > setup_times[0] else "stable"
        efficiency_trend = (
            "decreasing" if efficiencies[-1] < efficiencies[0] else "stable"
        )

        print("\n📊 TREND ANALYSIS:")
        print(f"   Setup time trend: {setup_trend}")
        print(f"   Efficiency trend: {efficiency_trend}")
        print(f"   Average efficiency: {sum(efficiencies) / len(efficiencies):.2%}")
        print(f"   Average success rate: {sum(success_rates) / len(success_rates):.2%}")

        # Find optimal cluster size (best efficiency)
        best_efficiency_idx = max(
            range(len(results)), key=lambda i: results[i]["connection_efficiency"]
        )
        optimal_size = results[best_efficiency_idx]["cluster_size"]
        optimal_efficiency = results[best_efficiency_idx]["connection_efficiency"]

        print(
            f"   🏆 Optimal cluster size: {optimal_size} nodes ({optimal_efficiency:.2%} efficiency)"
        )

        # Validate results
        assert len(results) == 4
        assert all(
            r["function_success_rate"] >= 0.7 for r in results
        )  # At least 70% success
        assert optimal_efficiency > 0.6  # Best efficiency > 60%

        print("✅ Scalability boundary research completed")

    async def test_topology_comparison_research(
        self,
        test_context: AsyncTestContext,
    ):
        """Compare different topology patterns for research."""

        researcher = CleanPerformanceResearch(test_context)

        print("\n🔬 TOPOLOGY COMPARISON RESEARCH:")

        # Test different federation patterns
        topologies: list[dict[str, Any]] = [
            {"name": "Small Clusters", "sizes": [3, 3, 3]},
            {"name": "Mixed Sizes", "sizes": [2, 5, 4]},
            {"name": "Large Central", "sizes": [1, 7, 1]},
        ]

        comparison_results = []

        for topology in topologies:
            print(f"   Testing {topology['name']} pattern...")

            sizes: list[int] = topology["sizes"]  # Type annotation
            result = await researcher.test_gossip_federation_gossip_topology(sizes)

            # Add topology name to result
            result["topology_name"] = topology["name"]
            comparison_results.append(result)

            efficiency = result["connection_efficiency"]
            success = result["function_success_rate"]
            setup = result["setup_time_ms"]

            print(
                f"     → Efficiency: {efficiency:.2%}, Success: {success:.2%}, Setup: {setup:.0f}ms"
            )

        print("\n📋 TOPOLOGY COMPARISON ANALYSIS:")

        # Find best performing topologies
        best_efficiency = max(
            comparison_results, key=lambda r: r["connection_efficiency"]
        )
        best_speed = min(comparison_results, key=lambda r: r["setup_time_ms"])
        best_reliability = max(
            comparison_results, key=lambda r: r["function_success_rate"]
        )

        print(
            f"   🏆 Most efficient: {best_efficiency['topology_name']} ({best_efficiency['connection_efficiency']:.2%})"
        )
        print(
            f"   🏆 Fastest setup: {best_speed['topology_name']} ({best_speed['setup_time_ms']:.0f}ms)"
        )
        print(
            f"   🏆 Most reliable: {best_reliability['topology_name']} ({best_reliability['function_success_rate']:.2%})"
        )

        # Validate all topologies worked
        assert len(comparison_results) == 3
        assert all(r["function_success_rate"] >= 0.7 for r in comparison_results)

        print("✅ Topology comparison research completed")


# Custom fixtures for clean testing
@pytest.fixture
def research_ports():
    """Pytest fixture for research port allocation."""
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(50, "research")
    yield ports
    for port in ports:
        allocator.release_port(port)
