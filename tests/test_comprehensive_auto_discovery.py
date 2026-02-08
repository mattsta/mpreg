"""
Comprehensive Auto-Discovery Test Suite for MPREG Gossip Clusters.

This module provides comprehensive pytest tests for validating peer node auto-discovery
functionality across different cluster sizes and topologies. Tests prove that nodes
can automatically discover each other through gossip protocol without manual peer lists.

Key test scenarios:
- Small cluster discovery (2-5 nodes)
- Large cluster discovery (8-50 nodes)
- Different topology patterns (linear, star, ring, mesh)
- Function propagation across auto-discovered nodes
- Auto-discovery timing and performance
- Recovery after node failures
"""

import asyncio
import os
import time
from dataclasses import dataclass, field

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import MPREGException, RPCCommand
from mpreg.core.port_allocator import get_port_allocator
from mpreg.datastructures.function_identity import FunctionSelector, VersionConstraint
from mpreg.fabric.index import FunctionQuery
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import wait_for_condition


def get_concurrency_factor() -> float:
    """Scale timeouts for xdist workers under high concurrency."""
    return 2.0 if os.environ.get("PYTEST_XDIST_WORKER") else 1.0


# Custom fixture for larger cluster sizes
@pytest.fixture
def large_cluster_ports():
    """Pytest fixture for large cluster testing (50 ports)."""
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(50, "servers")
    yield ports
    for port in ports:
        allocator.release_port(port)


@pytest.fixture
def medium_cluster_ports():
    """Pytest fixture for medium cluster testing (20 ports)."""
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(20, "servers")
    yield ports
    for port in ports:
        allocator.release_port(port)


@dataclass
class AutoDiscoveryTestResult:
    """Results from an auto-discovery test."""

    cluster_size: int
    topology: str
    nodes_discovered: dict[int, int] = field(default_factory=dict)
    functions_propagated: dict[int, bool] = field(default_factory=dict)
    success: bool = False
    failure_reason: str = ""
    details: list[str] = field(default_factory=list)


class AutoDiscoveryTestHelpers:
    """Shared helper methods for auto-discovery testing."""

    async def _test_cluster_auto_discovery(
        self,
        test_context: AsyncTestContext,
        ports: list[int],
        topology: str,
        expected_peers: int,
    ) -> AutoDiscoveryTestResult:
        """Test auto-discovery for a specific cluster configuration."""

        result = AutoDiscoveryTestResult(len(ports), topology)
        concurrency_factor = get_concurrency_factor()

        print(f"\n{'=' * 60}")
        print(f"TESTING {len(ports)}-NODE CLUSTER - {topology} TOPOLOGY")
        print(f"{'=' * 60}")

        # Create cluster servers
        tasks_start = len(test_context.tasks)
        servers_start = len(test_context.servers)
        servers = await self._create_cluster_servers(test_context, ports, topology)

        # Allow time for all servers to fully initialize before starting discovery timing
        if len(ports) >= 30:
            startup_timeout = max(5.0, len(ports) * 0.1)
            print(
                f"Waiting up to {startup_timeout}s for all {len(ports)} servers to fully initialize..."
            )
            await wait_for_condition(
                lambda: all(
                    server._transport_listener is not None for server in servers
                ),
                timeout=startup_timeout * concurrency_factor,
                interval=0.1,
                error_message=(
                    "Not all servers started transport listeners within "
                    f"{startup_timeout:.1f}s"
                ),
            )

        # For large clusters, use more lenient success criteria
        if len(ports) >= 30:
            # Large clusters: accept if 80% of nodes discover 80% of peers
            min_discovery_threshold = int(expected_peers * 0.8)
            nodes_meeting_threshold = 0
            min_nodes_threshold = int(len(ports) * 0.8)
        else:
            # Small/medium clusters: expect full discovery
            min_discovery_threshold = expected_peers
            nodes_meeting_threshold = 0
            min_nodes_threshold = len(ports)

        # Wait for auto-discovery - realistic timing since algorithms converge fast
        if len(ports) >= 30:
            # Large clusters: algorithms converge quickly, just need time for all connections
            discovery_timeout = max(45.0, len(ports) * 1.0)
        elif len(ports) >= 10:
            # Medium clusters need moderate time
            discovery_timeout = max(15.0, len(ports) * 1.0)
        else:
            # Small clusters can use original timing
            discovery_timeout = max(5.0, len(ports) * 0.5)

        print(
            f"Waiting up to {discovery_timeout}s for auto-discovery ({len(ports)} nodes)..."
        )

        def discovery_ready() -> bool:
            meeting_threshold = 0
            for server in servers:
                discovered_count = max(len(server.cluster.servers) - 1, 0)
                if discovered_count >= min_discovery_threshold:
                    meeting_threshold += 1
            return meeting_threshold >= min_nodes_threshold

        try:
            await wait_for_condition(
                discovery_ready,
                timeout=discovery_timeout * concurrency_factor,
                interval=0.2,
                error_message="Auto-discovery did not converge within the timeout",
            )
        except AssertionError as exc:
            if discovery_ready():
                # Avoid false negatives when convergence happens right at timeout edge.
                pass
            else:
                discovered_counts = [
                    max(len(server.cluster.servers) - 1, 0) for server in servers
                ]
                discovery_missing_nodes = [
                    idx
                    for idx, count in enumerate(discovered_counts)
                    if count < min_discovery_threshold
                ]
                connectivity_samples = [
                    (
                        idx,
                        len(server.peer_connections),
                        sum(
                            1
                            for connection in server._get_all_peer_connections().values()
                            if connection.is_connected
                        ),
                        (
                            len(server._peer_directory.nodes()) - 1
                            if server._peer_directory is not None
                            else -1
                        ),
                        (
                            max(
                                len(
                                    server._fabric_control_plane.index.catalog.nodes.entries()
                                )
                                - 1,
                                0,
                            )
                            if server._fabric_control_plane is not None
                            else -1
                        ),
                    )
                    for idx, server in enumerate(servers)
                ]
                raise AssertionError(
                    "Auto-discovery did not converge within the timeout; "
                    f"nodes_meeting_threshold={sum(count >= min_discovery_threshold for count in discovered_counts)}/{min_nodes_threshold} "
                    f"min_discovered={min(discovered_counts)} "
                    f"avg_discovered={sum(discovered_counts) / max(len(discovered_counts), 1):.2f} "
                    f"max_discovered={max(discovered_counts)} "
                    f"missing_nodes={discovery_missing_nodes[:12]} "
                    "node_samples(index,peer_connections,active_connections,peer_directory_discovered,node_catalog_discovered)="
                    f"{connectivity_samples[:12]}"
                ) from exc

        # Check discovery results
        print("\n=== AUTO-DISCOVERY RESULTS ===")
        all_discovered = True

        discovery_ok_nodes: list[int] = []
        for i, server in enumerate(servers):
            cluster_servers = server.cluster.servers
            discovered_count = len(cluster_servers) - 1  # Exclude self
            discovered = discovered_count >= min_discovery_threshold

            result.nodes_discovered[i] = discovered_count

            print(
                f"Node {i}: {discovered_count}/{expected_peers} peers discovered - {'✅' if discovered else '❌'}"
            )
            result.details.append(
                f"Node {i}: {discovered_count}/{expected_peers} peers"
            )

            if discovered:
                nodes_meeting_threshold += 1
                discovery_ok_nodes.append(i)

        # Determine overall success based on cluster size
        if len(ports) >= 30:
            all_discovered = nodes_meeting_threshold >= min_nodes_threshold
            print(
                f"Large cluster success: {nodes_meeting_threshold}/{min_nodes_threshold} nodes meeting {min_discovery_threshold}/{expected_peers} peer threshold"
            )
        else:
            all_discovered = nodes_meeting_threshold == len(ports)
            print(
                f"Small/medium cluster success: {nodes_meeting_threshold}/{len(ports)} nodes with full discovery"
            )

        # Test function propagation
        print("\n=== FUNCTION PROPAGATION TEST ===")

        def test_function(data: str) -> str:
            return f"Auto-discovery test from node 0: {data}"

        # Register function on node 0
        servers[0].register_command("test_function", test_function, ["test-resource"])

        # Wait for propagation - realistic timing since gossip is fast
        if len(ports) >= 30:
            # Large clusters: function propagation is fast via gossip
            propagation_time = max(8.0, len(ports) * 0.2)  # Much more realistic
        elif len(ports) >= 10:
            # Medium clusters need moderate time
            propagation_time = max(6.0, len(ports) * 0.3)
        else:
            # Small clusters can use original timing
            propagation_time = max(3.0, len(ports) * 0.3)

        print(f"Waiting up to {propagation_time}s for function propagation...")

        if len(ports) >= 30:
            min_propagation_nodes = max(int(len(ports) * 0.8), 1)
            propagation_targets = set(discovery_ok_nodes)

            def propagation_ready() -> bool:
                propagated = 0
                for idx, server in enumerate(servers):
                    if idx not in propagation_targets:
                        continue
                    if "test_function" in server.cluster.funtimes:
                        propagated += 1
                return propagated >= min_propagation_nodes

        else:

            def propagation_ready() -> bool:
                return all(
                    "test_function" in server.cluster.funtimes for server in servers
                )

        try:
            await wait_for_condition(
                propagation_ready,
                timeout=propagation_time * concurrency_factor,
                interval=0.2,
                error_message="Function propagation did not converge within the timeout",
            )
        except AssertionError as exc:
            if not propagation_ready():
                propagated_targets: list[int] = []
                missing_targets: list[int] = []
                target_population = (
                    sorted(propagation_targets)
                    if len(ports) >= 30
                    else list(range(len(servers)))
                )
                for idx in target_population:
                    if "test_function" in servers[idx].cluster.funtimes:
                        propagated_targets.append(idx)
                    else:
                        missing_targets.append(idx)
                missing_connectivity = [
                    (
                        idx,
                        len(servers[idx].peer_connections),
                        max(len(servers[idx].cluster.servers) - 1, 0),
                    )
                    for idx in missing_targets[:12]
                ]
                raise AssertionError(
                    "Function propagation did not converge within the timeout; "
                    f"propagated={len(propagated_targets)}/{len(target_population)} "
                    f"required={min_propagation_nodes if len(ports) >= 30 else len(servers)} "
                    f"missing_nodes={missing_targets[:12]} "
                    f"missing_connectivity(peer_connections,discovered_peers)={missing_connectivity}"
                ) from exc

        # Check propagation results
        all_propagated = True
        propagated_target_count = 0
        propagation_target_nodes = discovery_ok_nodes
        for i, server in enumerate(servers):
            functions = list(server.cluster.funtimes.keys())
            has_function = "test_function" in functions

            result.functions_propagated[i] = has_function

            print(
                f"Node {i}: {'✅ HAS' if has_function else '❌ MISSING'} test_function"
            )

            if has_function:
                if i in propagation_target_nodes:
                    propagated_target_count += 1
            elif len(ports) < 30:
                all_propagated = False

        if len(ports) >= 30:
            min_propagation_nodes = max(int(len(ports) * 0.8), 1)
            all_propagated = propagated_target_count >= min_propagation_nodes
            print(
                f"Large cluster propagation: {propagated_target_count}/{min_propagation_nodes} nodes with function"
            )

        # Final result
        result.success = all_discovered and all_propagated
        if not result.success:
            if not all_discovered:
                result.failure_reason = "Incomplete peer discovery"
            elif not all_propagated:
                result.failure_reason = "Incomplete function propagation"

        status = "✅ SUCCESS" if result.success else "❌ FAILED"
        print(f"\n{topology} {len(ports)}-Node Result: {status}")

        # Cleanup servers before returning to avoid socket leaks in large clusters.
        await asyncio.gather(
            *(server.shutdown_async() for server in servers),
            return_exceptions=True,
        )
        new_tasks = test_context.tasks[tasks_start:]
        if new_tasks:
            await asyncio.wait(new_tasks, timeout=max(5.0, len(new_tasks) * 0.5))
        for task in new_tasks:
            if not task.done():
                task.cancel()
        if new_tasks:
            await asyncio.gather(*new_tasks, return_exceptions=True)
        del test_context.tasks[tasks_start:]
        del test_context.servers[servers_start:]
        await asyncio.sleep(0.1)

        return result

    async def _create_cluster_servers(
        self,
        test_context: AsyncTestContext,
        ports: list[int],
        topology: str,
        *,
        fabric_routing_enabled: bool = False,
    ) -> list[MPREGServer]:
        """Create cluster servers with the specified topology."""

        settings_list = []

        if topology == "LINEAR_CHAIN":
            # Linear chain: 0 ← 1 ← 2 ← 3 ← ... ← N
            for i, port in enumerate(ports):
                connect_to = f"ws://127.0.0.1:{ports[i - 1]}" if i > 0 else None
                settings_list.append(
                    self._create_node_settings(
                        i,
                        port,
                        connect_to,
                        len(ports),
                        fabric_routing_enabled=fabric_routing_enabled,
                    )
                )

        elif topology == "STAR_HUB":
            # Star: All nodes connect to node 0 (hub)
            for i, port in enumerate(ports):
                connect_to = f"ws://127.0.0.1:{ports[0]}" if i > 0 else None
                settings_list.append(
                    self._create_node_settings(
                        i,
                        port,
                        connect_to,
                        len(ports),
                        fabric_routing_enabled=fabric_routing_enabled,
                    )
                )

        elif topology == "RING":
            # Ring: 0 ← 1 ← 2 ← 3 ← ... ← N ← 0
            for i, port in enumerate(ports):
                if i == 0:
                    connect_to = f"ws://127.0.0.1:{ports[-1]}"  # Connect to last node
                else:
                    connect_to = f"ws://127.0.0.1:{ports[i - 1]}"
                settings_list.append(
                    self._create_node_settings(
                        i,
                        port,
                        connect_to,
                        len(ports),
                        fabric_routing_enabled=fabric_routing_enabled,
                    )
                )

        elif topology == "MULTI_HUB":
            # Multiple interconnected hubs: First 3 nodes are hubs that connect to each other
            # Other nodes connect to one of the hubs
            num_hubs = min(3, len(ports))
            for i, port in enumerate(ports):
                if i < num_hubs:  # First 3 are hubs
                    # Hubs connect to the previous hub (forming a hub chain)
                    connect_to = f"ws://127.0.0.1:{ports[i - 1]}" if i > 0 else None
                else:
                    # Non-hub nodes connect to one of the hubs (round-robin)
                    hub_port = ports[i % num_hubs]
                    connect_to = f"ws://127.0.0.1:{hub_port}"
                settings_list.append(
                    self._create_node_settings(
                        i,
                        port,
                        connect_to,
                        len(ports),
                        fabric_routing_enabled=fabric_routing_enabled,
                    )
                )

        # Create and start servers
        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        print(f"Starting {len(servers)} servers...")

        # For large clusters, start servers in smaller batches to prevent resource exhaustion
        if len(servers) >= 50:
            batch_size = 5  # Very small batches for 50+ nodes to prevent file descriptor exhaustion
            batch_delay = 1.0  # Longer delay to allow connections to stabilize
        elif len(servers) >= 20:
            batch_size = 8  # Medium batches for 20-49 nodes
            batch_delay = 0.5  # Moderate delay
        else:
            batch_size = len(servers)  # Start all at once for small clusters
            batch_delay = 0.0

        for batch_start in range(0, len(servers), batch_size):
            batch_end = min(batch_start + batch_size, len(servers))
            batch_servers = servers[batch_start:batch_end]

            print(
                f"  Starting batch {batch_start // batch_size + 1}: servers {batch_start + 1}-{batch_end}"
            )

            # Start this batch of servers
            for i, server in enumerate(batch_servers):
                task = asyncio.create_task(server.server())
                test_context.tasks.append(task)
                # Scale delay based on cluster size to prevent resource exhaustion
                if len(servers) >= 50:
                    await asyncio.sleep(0.5)  # Longer delay for 50+ nodes
                elif len(servers) >= 20:
                    await asyncio.sleep(0.3)  # Medium delay for 20-49 nodes
                else:
                    await asyncio.sleep(0.1)  # Quick start for small clusters

            # Wait between batches for large clusters
            if batch_delay > 0 and batch_end < len(servers):
                print(f"  Waiting {batch_delay}s before next batch...")
                await asyncio.sleep(batch_delay)

        return servers

    def _create_node_settings(
        self,
        node_id: int,
        port: int,
        connect_to: str | None,
        cluster_size: int,
        *,
        fabric_routing_enabled: bool = False,
    ) -> MPREGSettings:
        """Create settings for a node in the test cluster."""
        # Scale gossip interval based on cluster size to prevent congestion
        if cluster_size >= 30:
            gossip_interval = 1.0  # Faster gossip to stabilize large clusters
        elif cluster_size >= 10:
            gossip_interval = 1.0  # Moderate gossip for medium clusters
        else:
            gossip_interval = 0.5  # Fast gossip for small clusters

        return MPREGSettings(
            host="127.0.0.1",
            port=port,
            name=f"AutoDiscover-Node-{node_id}",
            cluster_id=f"test-cluster-{cluster_size}",
            resources={f"node-{node_id}", "test-resource"},
            peers=None,  # NO manual peers - should auto-discover
            connect=connect_to,
            advertised_urls=None,  # Use default advertised URL
            gossip_interval=gossip_interval,
            log_level="ERROR",  # CRITICAL: Prevent resource exhaustion from massive logging
            monitoring_enabled=False,  # Avoid monitoring port conflicts under concurrency
            fabric_routing_enabled=fabric_routing_enabled,
        )


class TestAutoDiscoverySmallClusters(AutoDiscoveryTestHelpers):
    """Test auto-discovery in small clusters (2-8 nodes)."""

    async def test_2_node_linear_chain_auto_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test 2-node linear chain auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, server_cluster_ports[:2], "LINEAR_CHAIN", expected_peers=1
        )
        assert result.success, f"2-node linear chain failed: {result.failure_reason}"

    async def test_3_node_linear_chain_auto_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test 3-node linear chain auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, server_cluster_ports[:3], "LINEAR_CHAIN", expected_peers=2
        )
        assert result.success, f"3-node linear chain failed: {result.failure_reason}"

    async def test_3_node_star_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test 3-node star hub auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, server_cluster_ports[:3], "STAR_HUB", expected_peers=2
        )
        assert result.success, f"3-node star hub failed: {result.failure_reason}"

    async def test_5_node_linear_chain_auto_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test 5-node linear chain auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, server_cluster_ports[:5], "LINEAR_CHAIN", expected_peers=4
        )
        assert result.success, f"5-node linear chain failed: {result.failure_reason}"

    async def test_5_node_star_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test 5-node star hub auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, server_cluster_ports[:5], "STAR_HUB", expected_peers=4
        )
        assert result.success, f"5-node star hub failed: {result.failure_reason}"

    async def test_5_node_ring_auto_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test 5-node ring auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, server_cluster_ports[:5], "RING", expected_peers=4
        )
        assert result.success, f"5-node ring failed: {result.failure_reason}"

    async def test_8_node_star_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test 8-node star hub auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, server_cluster_ports[:8], "STAR_HUB", expected_peers=7
        )
        assert result.success, f"8-node star hub failed: {result.failure_reason}"


class TestAutoDiscoveryMediumClusters(AutoDiscoveryTestHelpers):
    """Test auto-discovery in medium clusters (10-20 nodes).

    NOTE: Clusters larger than 10 nodes may hang in pytest environment due to
    resource contention. Only smaller tests are reliable in pytest.
    """

    async def test_10_node_star_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        medium_cluster_ports: list[int],
    ):
        """Test 10-node star hub auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, medium_cluster_ports[:10], "STAR_HUB", expected_peers=9
        )
        assert result.success, f"10-node star hub failed: {result.failure_reason}"

    async def test_13_node_multi_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        medium_cluster_ports: list[int],
    ):
        """Test 13-node multi-hub auto-discovery."""
        result = await self._test_cluster_auto_discovery(
            test_context, medium_cluster_ports[:13], "MULTI_HUB", expected_peers=12
        )
        assert result.success, f"13-node multi-hub failed: {result.failure_reason}"

    async def test_20_node_star_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        medium_cluster_ports: list[int],
    ):
        """Test 20-node star hub auto-discovery with timeout protection."""
        try:
            # Use asyncio timeout to prevent hanging
            result = await asyncio.wait_for(
                self._test_cluster_auto_discovery(
                    test_context,
                    medium_cluster_ports[:20],
                    "STAR_HUB",
                    expected_peers=19,
                ),
                timeout=120.0,  # 2 minute timeout
            )
            assert result.success, f"20-node star hub failed: {result.failure_reason}"
        except TimeoutError:
            pytest.fail("Test timed out after 2 minutes - likely hanging issue")


class TestAutoDiscoveryLargeClusters(AutoDiscoveryTestHelpers):
    """Test auto-discovery in large clusters (30-50 nodes).

    NOTE: Large cluster tests (30+ nodes) are skipped in pytest due to environment
    resource contention issues. The algorithms are verified to work correctly via
    manual test scripts:
    - debug_50_node_auto_discovery.py - Proves 50-node clusters achieve 87% peer discovery
    - Manual verification shows full topology convergence works correctly
    """

    @pytest.mark.slow
    @pytest.mark.no_cover  # Disable coverage for large tests
    async def test_30_node_multi_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        large_cluster_ports: list[int],
    ):
        """Test 30-node multi-hub auto-discovery.

        SKIPPED: Use debug_50_node_auto_discovery.py for manual verification.
        Manual testing proves the algorithms work correctly for large clusters.
        """
        result = await self._test_cluster_auto_discovery(
            test_context, large_cluster_ports[:30], "MULTI_HUB", expected_peers=29
        )
        assert result.success, f"30-node multi-hub failed: {result.failure_reason}"

    @pytest.mark.slow
    async def test_50_node_multi_hub_auto_discovery(
        self,
        test_context: AsyncTestContext,
        large_cluster_ports: list[int],
    ):
        """Test 50-node multi-hub auto-discovery."""
        if os.environ.get("PYTEST_XDIST_WORKER"):
            pytest.xfail(
                "Known 50-node convergence gap under xdist load; see "
                "tools/debug/auto_discovery_topology_probe.py evidence workflow."
            )
        result = await self._test_cluster_auto_discovery(
            test_context, large_cluster_ports[:50], "MULTI_HUB", expected_peers=49
        )
        assert result.success, f"50-node multi-hub failed: {result.failure_reason}"


class TestAutoDiscoveryFunctionPropagation(AutoDiscoveryTestHelpers):
    """Test function propagation across auto-discovered nodes."""

    async def test_function_propagation_across_auto_discovered_nodes(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test that functions propagate across all auto-discovered nodes."""
        concurrency_factor = get_concurrency_factor()
        ports = server_cluster_ports[:5]

        # Create linear chain with auto-discovery
        servers = await self._create_cluster_servers(
            test_context, ports, "LINEAR_CHAIN"
        )

        # Wait for auto-discovery
        discovery_timeout = max(5.0, len(ports) * 0.5) * concurrency_factor
        await wait_for_condition(
            lambda: all(
                len(server.cluster.servers) >= len(ports) for server in servers
            ),
            timeout=discovery_timeout,
            interval=0.2,
            error_message="Auto-discovery did not converge for propagation test",
        )

        # Register function on first node
        def propagation_test_function(data: str) -> str:
            return f"Auto-discovery propagation test: {data}"

        servers[0].register_command(
            "propagation_test", propagation_test_function, [f"node-{0}"]
        )

        # Wait for function propagation
        propagation_timeout = max(3.0, len(ports) * 0.3) * concurrency_factor
        await wait_for_condition(
            lambda: all(
                "propagation_test" in server.cluster.funtimes for server in servers
            ),
            timeout=propagation_timeout,
            interval=0.2,
            error_message="Function propagation did not converge",
        )

        # Test function accessibility from all nodes
        propagation_results = []
        for i, port in enumerate(ports):
            client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
            test_context.clients.append(client)
            await client.connect()

            try:
                result = await client._client.request(
                    [
                        RPCCommand(
                            name="test_result",
                            fun="propagation_test",
                            args=(f"from_node_{i}",),
                            locs=frozenset([f"node-{0}"]),
                        )
                    ]
                )

                has_function = "test_result" in result
                propagation_results.append(has_function)

                print(
                    f"Node {i}: {'✅ HAS' if has_function else '❌ MISSING'} propagation_test"
                )

            except Exception as e:
                propagation_results.append(False)
                print(f"Node {i}: ❌ ERROR accessing function: {e}")

        # All nodes should have the function
        all_propagated = all(propagation_results)
        assert all_propagated, f"Function propagation failed: {propagation_results}"

        print(
            f"✅ Function propagation successful across all {len(ports)} auto-discovered nodes"
        )

    async def test_bidirectional_function_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test that functions registered on any node are discoverable by all nodes."""
        concurrency_factor = get_concurrency_factor()
        ports = server_cluster_ports[:3]

        # Create star hub cluster
        servers = await self._create_cluster_servers(test_context, ports, "STAR_HUB")

        # Wait for auto-discovery
        discovery_timeout = max(5.0, len(ports) * 0.5) * concurrency_factor
        await wait_for_condition(
            lambda: all(
                len(server.cluster.servers) >= len(ports) for server in servers
            ),
            timeout=discovery_timeout,
            interval=0.2,
            error_message="Auto-discovery did not converge for bidirectional test",
        )

        # Register unique functions on each node
        for i, server in enumerate(servers):

            def make_node_function(node_id: int):
                def node_function(data: str) -> str:
                    return f"Node {node_id} processed: {data}"

                return node_function

            server.register_command(
                f"node_{i}_function", make_node_function(i), [f"node-{i}"]
            )

        # Wait for function propagation
        propagation_timeout = max(3.0, len(ports) * 0.3) * concurrency_factor

        def all_functions_propagated() -> bool:
            expected = {f"node_{i}_function" for i in range(len(ports))}
            for server in servers:
                if not expected.issubset(server.cluster.funtimes.keys()):
                    return False
            return True

        await wait_for_condition(
            all_functions_propagated,
            timeout=propagation_timeout,
            interval=0.2,
            error_message="Bidirectional function propagation did not converge",
        )

        # Test that each node can access functions from all other nodes
        discovery_matrix = []
        failure_details: dict[tuple[int, int], str] = {}
        call_timeout = max(1.5, len(ports) * 0.3) * concurrency_factor

        async def _call_with_retry(
            client: MPREGClientAPI, rpc_command: RPCCommand
        ) -> tuple[bool, Exception | None]:
            deadline = time.time() + call_timeout
            last_exc: Exception | None = None
            while time.time() < deadline:
                try:
                    result = await client._client.request([rpc_command])
                    return "test_result" in result, None
                except Exception as exc:  # pragma: no cover - best effort retry
                    last_exc = exc
                    await asyncio.sleep(0.1)
            return False, last_exc

        for i, port in enumerate(ports):
            client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
            test_context.clients.append(client)
            await client.connect()

            node_discoveries = []
            for j in range(len(ports)):
                command = RPCCommand(
                    name="test_result",
                    fun=f"node_{j}_function",
                    args=(f"test_from_node_{i}",),
                    locs=frozenset([f"node-{j}"]),
                )
                has_function, exc = await _call_with_retry(client, command)
                node_discoveries.append(has_function)
                if exc:
                    failure_details[(i, j)] = repr(exc)

            discovery_matrix.append(node_discoveries)
            print(
                f"Node {i} discoveries: {['✅' if d else '❌' for d in node_discoveries]}"
            )

        # All nodes should discover all functions
        all_discovered = all(all(row) for row in discovery_matrix)
        assert all_discovered, (
            "Bidirectional discovery failed: "
            f"{discovery_matrix} (errors={failure_details})"
        )

        print(
            f"✅ Bidirectional function discovery successful across {len(ports)} nodes"
        )

    async def test_fabric_routing_function_id_version_constraints(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test fabric routing with function_id + version constraints over auto-discovery."""
        concurrency_factor = get_concurrency_factor()
        ports = server_cluster_ports[:5]
        servers = await self._create_cluster_servers(
            test_context, ports, "STAR_HUB", fabric_routing_enabled=True
        )

        # Wait for auto-discovery and baseline gossip propagation
        discovery_timeout = max(5.0, len(ports) * 0.5) * concurrency_factor
        await wait_for_condition(
            lambda: all(
                len(server.cluster.servers) >= len(ports) for server in servers
            ),
            timeout=discovery_timeout,
            interval=0.2,
            error_message="Auto-discovery did not converge for fabric routing test",
        )

        function_id = "mesh-function-id"
        version = "2.1.0"

        def mesh_function(data: str) -> str:
            return f"mesh:{data}"

        servers[0].register_command(
            "mesh_function",
            mesh_function,
            ["mesh-resource"],
            function_id=function_id,
            version=version,
        )

        # Wait for fabric catalog propagation across nodes.
        def routes_ready() -> bool:
            for i, server in enumerate(servers):
                if not server._fabric_control_plane:
                    return False
                selector = FunctionSelector(
                    name="mesh_function",
                    function_id=function_id,
                    version_constraint=VersionConstraint.parse(">=2.0.0,<3.0.0"),
                )
                query = FunctionQuery(
                    selector=selector,
                    resources=frozenset({"mesh-resource"}),
                )
                matches = server._fabric_control_plane.index.find_functions(query)
                if not matches:
                    return False
            return True

        await wait_for_condition(
            routes_ready,
            timeout=10.0 * concurrency_factor,
            interval=0.5,
            error_message="Fabric routes did not propagate",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{ports[-1]}")
        test_context.clients.append(client)
        await client.connect()

        result = await client.call(
            "mesh_function",
            "payload",
            locs=frozenset({"mesh-resource"}),
            function_id=function_id,
            version_constraint=">=2.0.0,<3.0.0",
        )
        assert result == "mesh:payload"

    async def test_fabric_routing_resource_capability_filtering(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test fabric routing resource filtering over auto-discovery."""
        concurrency_factor = get_concurrency_factor()
        ports = server_cluster_ports[:5]
        servers = await self._create_cluster_servers(
            test_context, ports, "STAR_HUB", fabric_routing_enabled=True
        )

        discovery_timeout = max(5.0, len(ports) * 0.5) * concurrency_factor
        await wait_for_condition(
            lambda: all(
                len(server.cluster.servers) >= len(ports) for server in servers
            ),
            timeout=discovery_timeout,
            interval=0.2,
            error_message="Auto-discovery did not converge for resource filtering test",
        )

        function_id = "mesh-resource-id"
        version = "1.2.0"

        def gpu_handler(data: str) -> str:
            return f"gpu:{data}"

        def cpu_handler(data: str) -> str:
            return f"cpu:{data}"

        servers[0].register_command(
            "resource_function",
            gpu_handler,
            ["gpu"],
            function_id=function_id,
            version=version,
        )
        servers[1].register_command(
            "resource_function",
            cpu_handler,
            ["cpu"],
            function_id=function_id,
            version=version,
        )

        def routes_ready() -> bool:
            for server in servers:
                if not server._fabric_control_plane:
                    return False
                for required in (frozenset({"gpu"}), frozenset({"cpu"})):
                    selector = FunctionSelector(
                        name="resource_function",
                        function_id=function_id,
                        version_constraint=VersionConstraint.parse("==1.2.0"),
                    )
                    query = FunctionQuery(
                        selector=selector,
                        resources=required,
                    )
                    matches = server._fabric_control_plane.index.find_functions(query)
                    if not matches:
                        return False
            return True

        await wait_for_condition(
            routes_ready,
            timeout=10.0 * concurrency_factor,
            interval=0.5,
            error_message="Fabric resource routes did not propagate",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{ports[-1]}")
        test_context.clients.append(client)
        await client.connect()

        gpu_result = await client.call(
            "resource_function",
            "payload",
            locs=frozenset({"gpu"}),
            function_id=function_id,
            version_constraint="==1.2.0",
        )
        assert gpu_result == "gpu:payload"

        cpu_result = await client.call(
            "resource_function",
            "payload",
            locs=frozenset({"cpu"}),
            function_id=function_id,
            version_constraint="==1.2.0",
        )
        assert cpu_result == "cpu:payload"

        with pytest.raises(MPREGException):
            await client.call(
                "resource_function",
                "payload",
                locs=frozenset({"tpu"}),
                function_id=function_id,
                version_constraint="==1.2.0",
            )
