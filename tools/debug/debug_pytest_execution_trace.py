#!/usr/bin/env python3
"""
LOGGING DEBUG: Trace EXACTLY where pytest execution hangs
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

import pytest
from tests.port_allocator import get_port_allocator

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("pytest_execution_trace.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


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
    """Shared helper methods for auto-discovery testing with detailed logging."""

    async def _test_cluster_auto_discovery(
        self,
        test_context: AsyncTestContext,
        ports: list[int],
        topology: str,
        expected_peers: int,
    ) -> AutoDiscoveryTestResult:
        """Test auto-discovery for a specific cluster configuration."""
        logger.info(
            f"STEP 1: Starting _test_cluster_auto_discovery for {len(ports)}-node {topology}"
        )

        result = AutoDiscoveryTestResult(len(ports), topology)
        logger.info("STEP 2: Created result object")

        print(f"\n{'=' * 60}")
        print(f"TESTING {len(ports)}-NODE CLUSTER - {topology} TOPOLOGY")
        print(f"{'=' * 60}")

        # Create cluster servers
        logger.info("STEP 3: About to call _create_cluster_servers")
        servers = await self._create_cluster_servers(test_context, ports, topology)
        logger.info(f"STEP 4: _create_cluster_servers returned {len(servers)} servers")

        # Allow time for all servers to fully initialize before starting discovery timing
        if len(ports) >= 30:
            startup_wait = 15.0  # Large clusters need more startup time
            logger.info(
                f"STEP 5: Large cluster - waiting {startup_wait}s for initialization"
            )
            print(
                f"Waiting {startup_wait}s for all {len(ports)} servers to fully initialize..."
            )
            await asyncio.sleep(startup_wait)
            logger.info("STEP 6: Initialization wait completed")

        # Wait for auto-discovery - conservative timing for test environment
        if len(ports) >= 30:
            # Large clusters: generous timeout for test environment resource contention
            discovery_time = max(
                90.0, len(ports) * 2.0
            )  # Very generous for pytest environment
        elif len(ports) >= 10:
            # Medium clusters need moderate time
            discovery_time = max(15.0, len(ports) * 1.0)
        else:
            # Small clusters can use original timing
            discovery_time = max(5.0, len(ports) * 0.5)

        logger.info(f"STEP 7: Starting discovery wait for {discovery_time}s")
        print(f"Waiting {discovery_time}s for auto-discovery ({len(ports)} nodes)...")
        await asyncio.sleep(discovery_time)
        logger.info("STEP 8: Discovery wait completed")

        # Check discovery results
        print("\n=== AUTO-DISCOVERY RESULTS ===")
        logger.info("STEP 9: Checking discovery results")
        all_discovered = True

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

        logger.info(
            f"STEP 10: Iterating through {len(servers)} servers for discovery check"
        )
        for i, server in enumerate(servers):
            logger.debug(f"  Checking server {i}")
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

        logger.info(
            f"STEP 11: Discovery check completed, {nodes_meeting_threshold} nodes meeting threshold"
        )

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

        logger.info("STEP 12: Starting function propagation test")

        # Test function propagation
        print("\n=== FUNCTION PROPAGATION TEST ===")

        def test_function(data: str) -> str:
            return f"Auto-discovery test from node 0: {data}"

        # Register function on node 0
        servers[0].register_command("test_function", test_function, ["test-resource"])
        logger.info("STEP 13: Registered test function on node 0")

        # Wait for propagation - conservative timing for test environment
        if len(ports) >= 30:
            # Large clusters: conservative timeout for function propagation
            propagation_time = max(
                40.0, len(ports) * 0.8
            )  # Conservative for pytest environment
        elif len(ports) >= 10:
            # Medium clusters need moderate time
            propagation_time = max(10.0, len(ports) * 0.5)
        else:
            # Small clusters can use original timing
            propagation_time = max(3.0, len(ports) * 0.3)

        logger.info(f"STEP 14: Starting propagation wait for {propagation_time}s")
        print(f"Waiting {propagation_time}s for function propagation...")
        await asyncio.sleep(propagation_time)
        logger.info("STEP 15: Propagation wait completed")

        # Check propagation results
        logger.info("STEP 16: Checking propagation results")
        all_propagated = True
        for i, server in enumerate(servers):
            functions = list(server.cluster.funtimes.keys())
            has_function = "test_function" in functions

            result.functions_propagated[i] = has_function

            print(
                f"Node {i}: {'✅ HAS' if has_function else '❌ MISSING'} test_function"
            )

            if not has_function:
                all_propagated = False

        logger.info("STEP 17: Propagation check completed")

        # Final result
        result.success = all_discovered and all_propagated
        if not result.success:
            if not all_discovered:
                result.failure_reason = "Incomplete peer discovery"
            elif not all_propagated:
                result.failure_reason = "Incomplete function propagation"

        status = "✅ SUCCESS" if result.success else "❌ FAILED"
        print(f"\n{topology} {len(ports)}-Node Result: {status}")

        logger.info(
            f"STEP 18: _test_cluster_auto_discovery completed with success={result.success}"
        )
        return result

    async def _create_cluster_servers(
        self,
        test_context: AsyncTestContext,
        ports: list[int],
        topology: str,
    ) -> list[MPREGServer]:
        """Create cluster servers with the specified topology."""
        logger.info(
            f"STEP 3.1: _create_cluster_servers called with {len(ports)} ports, topology={topology}"
        )

        settings_list = []
        logger.info("STEP 3.2: Starting topology configuration")

        if topology == "MULTI_HUB":
            # Multiple interconnected hubs: First 3 nodes are hubs that connect to each other
            # Other nodes connect to one of the hubs
            num_hubs = min(3, len(ports))
            logger.info(f"STEP 3.3: MULTI_HUB topology with {num_hubs} hubs")
            for i, port in enumerate(ports):
                if i < num_hubs:  # First 3 are hubs
                    # Hubs connect to the previous hub (forming a hub chain)
                    connect_to = f"ws://127.0.0.1:{ports[i - 1]}" if i > 0 else None
                else:
                    # Non-hub nodes connect to one of the hubs (round-robin)
                    hub_port = ports[i % num_hubs]
                    connect_to = f"ws://127.0.0.1:{hub_port}"
                settings_list.append(
                    self._create_node_settings(i, port, connect_to, len(ports))
                )
        else:
            logger.error(f"Unknown topology: {topology}")
            raise ValueError(f"Unknown topology: {topology}")

        logger.info(f"STEP 3.4: Created {len(settings_list)} settings objects")

        # Create and start servers
        logger.info(f"STEP 3.5: Creating {len(settings_list)} MPREGServer objects")
        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)
        logger.info("STEP 3.6: Added servers to test context")

        print(f"Starting {len(servers)} servers...")

        # For large clusters, start servers in smaller batches to prevent resource exhaustion
        if len(servers) >= 20:
            batch_size = 5  # Start 5 servers at a time
            batch_delay = 2.0  # Wait between batches
            logger.info(
                f"STEP 3.7: Large cluster - using batched startup (batch_size={batch_size})"
            )
        else:
            batch_size = len(servers)  # Start all at once for small clusters
            batch_delay = 0.0
            logger.info("STEP 3.7: Small cluster - starting all servers at once")

        for batch_start in range(0, len(servers), batch_size):
            batch_end = min(batch_start + batch_size, len(servers))
            batch_servers = servers[batch_start:batch_end]

            logger.info(
                f"STEP 3.8.{batch_start // batch_size}: Starting batch {batch_start // batch_size + 1}: servers {batch_start + 1}-{batch_end}"
            )
            print(
                f"  Starting batch {batch_start // batch_size + 1}: servers {batch_start + 1}-{batch_end}"
            )

            # Start this batch of servers
            for i, server in enumerate(batch_servers):
                logger.debug(f"  Starting server {batch_start + i}")
                task = asyncio.create_task(server.server())
                test_context.tasks.append(task)
                await asyncio.sleep(0.2)  # Small delay within batch

            logger.info(
                f"STEP 3.9.{batch_start // batch_size}: Batch {batch_start // batch_size + 1} started"
            )

            # Wait between batches for large clusters
            if batch_delay > 0 and batch_end < len(servers):
                logger.info(
                    f"STEP 3.10.{batch_start // batch_size}: Waiting {batch_delay}s before next batch"
                )
                print(f"  Waiting {batch_delay}s before next batch...")
                await asyncio.sleep(batch_delay)

        logger.info(f"STEP 3.11: All servers started, returning {len(servers)} servers")
        return servers

    def _create_node_settings(
        self, node_id: int, port: int, connect_to: str | None, cluster_size: int
    ) -> MPREGSettings:
        """Create settings for a node in the test cluster."""
        logger.debug(f"Creating node settings for node {node_id} on port {port}")

        # Scale gossip interval based on cluster size to prevent congestion
        if cluster_size >= 30:
            gossip_interval = (
                2.0  # Slower gossip for large clusters to prevent congestion
            )
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
        )


@pytest.fixture
def debug_large_cluster_ports():
    """Debug version of large_cluster_ports with detailed logging."""
    logger.info("FIXTURE STEP 1: Starting debug_large_cluster_ports fixture")
    start_time = time.time()

    logger.info("FIXTURE STEP 2: Getting port allocator")
    allocator = get_port_allocator()
    logger.info(f"FIXTURE STEP 3: Got allocator: {allocator}")

    logger.info("FIXTURE STEP 4: Calling allocate_port_range(30, 'servers')")
    ports = allocator.allocate_port_range(
        30, "servers"
    )  # Use 30 instead of 50 for faster testing
    logger.info(
        f"FIXTURE STEP 5: Got {len(ports)} ports in {time.time() - start_time:.2f}s"
    )

    logger.info("FIXTURE STEP 6: Yielding ports")
    yield ports

    logger.info("FIXTURE STEP 7: Starting cleanup")
    cleanup_start = time.time()
    for port in ports:
        allocator.release_port(port)
    logger.info(
        f"FIXTURE STEP 8: Cleanup completed in {time.time() - cleanup_start:.2f}s"
    )


class TestDebugLargeCluster(AutoDiscoveryTestHelpers):
    """Debug version of large cluster test with detailed logging."""

    @pytest.mark.slow
    async def test_30_node_debug(
        self,
        test_context: AsyncTestContext,
        debug_large_cluster_ports: list[int],
    ):
        """Debug version of 30-node test with detailed logging."""
        logger.info("TEST STEP 1: test_30_node_debug started")
        logger.info(f"TEST STEP 2: Got {len(debug_large_cluster_ports)} ports")

        result = await self._test_cluster_auto_discovery(
            test_context, debug_large_cluster_ports[:30], "MULTI_HUB", expected_peers=29
        )

        logger.info(
            f"TEST STEP 3: _test_cluster_auto_discovery returned success={result.success}"
        )
        assert result.success, f"30-node multi-hub failed: {result.failure_reason}"
        logger.info("TEST STEP 4: test_30_node_debug completed successfully")


if __name__ == "__main__":
    import subprocess
    import sys

    print("🔬 Running debug version with detailed logging...")

    # Clear previous log
    with open("pytest_execution_trace.log", "w") as f:
        f.write("PYTEST EXECUTION TRACE LOG\n")
        f.write("=" * 50 + "\n")

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                __file__ + "::TestDebugLargeCluster::test_30_node_debug",
                "-vs",
                "--tb=short",
            ],
            timeout=120,
            capture_output=True,
            text=True,
        )

        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)

        if result.returncode == 0:
            print("✅ Debug test PASSED!")
        else:
            print(f"❌ Debug test FAILED: {result.returncode}")

    except subprocess.TimeoutExpired:
        print("❌ DEBUG TEST TIMED OUT!")

    print("\n📋 Check pytest_execution_trace.log for detailed execution trace")
