#!/usr/bin/env python3
"""
Comprehensive Auto-Discovery Test Suite for MPREG Gossip Clusters.

This tests auto-discovery functionality across different cluster sizes and topologies
to PROVE what works and what needs to be fixed.
"""

import asyncio
import contextlib

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


class AutoDiscoveryTestResult:
    """Results from an auto-discovery test."""

    def __init__(self, cluster_size: int, topology: str):
        self.cluster_size = cluster_size
        self.topology = topology
        self.nodes_discovered: dict[int, int] = {}  # node_id -> discovered_count
        self.functions_propagated: dict[int, bool] = {}  # node_id -> has_test_function
        self.success = False
        self.details: list[str] = []


async def test_autodiscovery_cluster(
    cluster_size: int, topology: str, base_port: int = 20000
) -> AutoDiscoveryTestResult:
    """Test auto-discovery for a specific cluster size and topology."""

    result = AutoDiscoveryTestResult(cluster_size, topology)
    ports = [base_port + i for i in range(cluster_size)]

    print(f"\n{'=' * 60}")
    print(f"TESTING {cluster_size}-NODE CLUSTER - {topology} TOPOLOGY")
    print(f"{'=' * 60}")

    # Create different topologies
    settings_list = []

    if topology == "LINEAR_CHAIN":
        # Linear chain: 0 ← 1 ← 2 ← 3 ← ... ← N
        for i, port in enumerate(ports):
            connect_to = f"ws://127.0.0.1:{ports[i - 1]}" if i > 0 else None
            settings_list.append(
                create_node_settings(i, port, connect_to, cluster_size)
            )

    elif topology == "STAR_HUB":
        # Star: All nodes connect to node 0 (hub)
        for i, port in enumerate(ports):
            connect_to = f"ws://127.0.0.1:{ports[0]}" if i > 0 else None
            settings_list.append(
                create_node_settings(i, port, connect_to, cluster_size)
            )

    elif topology == "RING":
        # Ring: 0 ← 1 ← 2 ← 3 ← ... ← N ← 0
        for i, port in enumerate(ports):
            if i == 0:
                connect_to = f"ws://127.0.0.1:{ports[-1]}"  # Connect to last node
            else:
                connect_to = f"ws://127.0.0.1:{ports[i - 1]}"
            settings_list.append(
                create_node_settings(i, port, connect_to, cluster_size)
            )

    elif topology == "MULTI_HUB":
        # Multiple hubs: Nodes 0,1,2 are hubs, others connect to random hub
        for i, port in enumerate(ports):
            if i < 3:  # First 3 are hubs
                connect_to = None
            else:
                hub_port = ports[i % 3]  # Connect to one of the hubs
                connect_to = f"ws://127.0.0.1:{hub_port}"
            settings_list.append(
                create_node_settings(i, port, connect_to, cluster_size)
            )

    # Start servers
    servers = [MPREGServer(settings=s) for s in settings_list]
    tasks = []

    print(f"Starting {cluster_size} servers...")
    for i, server in enumerate(servers):
        task = asyncio.create_task(server.server())
        tasks.append(task)
        print(f"  Node {i} started on port {ports[i]}")
        await asyncio.sleep(0.2)  # Staggered startup

    # Wait for auto-discovery
    discovery_time = max(5.0, cluster_size * 0.5)  # Scale wait time with cluster size
    print(f"\nWaiting {discovery_time}s for auto-discovery...")
    await asyncio.sleep(discovery_time)

    # Check discovery results
    print("\n=== AUTO-DISCOVERY RESULTS ===")
    expected_peers = cluster_size - 1  # N-1 peers (excluding self)
    all_discovered = True

    for i, server in enumerate(servers):
        cluster_servers = server.cluster.servers
        discovered_count = len(cluster_servers) - 1  # Exclude self
        discovered = discovered_count >= expected_peers

        result.nodes_discovered[i] = discovered_count

        print(
            f"Node {i}: {discovered_count}/{expected_peers} peers discovered - {'✅' if discovered else '❌'}"
        )
        result.details.append(f"Node {i}: {discovered_count}/{expected_peers} peers")

        if not discovered:
            all_discovered = False

    # Test function propagation
    print("\n=== FUNCTION PROPAGATION TEST ===")

    def test_function(data: str) -> str:
        return f"Auto-discovery test from node 0: {data}"

    # Register function on node 0
    servers[0].register_command("test_function", test_function, ["test-resource"])

    # Wait for propagation
    propagation_time = max(3.0, cluster_size * 0.3)
    print(f"Waiting {propagation_time}s for function propagation...")
    await asyncio.sleep(propagation_time)

    # Check propagation results
    all_propagated = True
    for i, server in enumerate(servers):
        functions = list(server.cluster.funtimes.keys())
        has_function = "test_function" in functions

        result.functions_propagated[i] = has_function

        print(f"Node {i}: {'✅ HAS' if has_function else '❌ MISSING'} test_function")

        if not has_function:
            all_propagated = False

    # Final result
    result.success = all_discovered and all_propagated
    status = "✅ SUCCESS" if result.success else "❌ FAILED"
    print(f"\n{topology} {cluster_size}-Node Result: {status}")

    # Cleanup
    for task in tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    return result


def create_node_settings(
    node_id: int, port: int, connect_to: str | None, cluster_size: int
) -> MPREGSettings:
    """Create settings for a node in the test cluster."""
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=f"AutoDiscover-Node-{node_id}",
        cluster_id=f"test-cluster-{cluster_size}",
        resources={f"node-{node_id}", "test-resource"},
        peers=None,  # NO manual peers - should auto-discover
        connect=connect_to,
        advertised_urls=None,  # Use default advertised URL
        gossip_interval=0.5,  # Fast gossip
    )


async def run_comprehensive_autodiscovery_tests():
    """Run comprehensive auto-discovery tests across different sizes and topologies."""

    print("🧪 COMPREHENSIVE MPREG AUTO-DISCOVERY TEST SUITE")
    print("=" * 70)

    test_cases = [
        # (cluster_size, topology)
        (2, "LINEAR_CHAIN"),
        (3, "LINEAR_CHAIN"),
        (3, "STAR_HUB"),
        (5, "LINEAR_CHAIN"),
        (5, "STAR_HUB"),
        (5, "RING"),
        (7, "LINEAR_CHAIN"),
        (7, "STAR_HUB"),
        (10, "STAR_HUB"),
        (10, "MULTI_HUB"),
    ]

    results = []
    passed = 0
    total = len(test_cases)

    for cluster_size, topology in test_cases:
        try:
            result = await test_autodiscovery_cluster(cluster_size, topology)
            results.append(result)
            if result.success:
                passed += 1
        except Exception as e:
            print(f"❌ ERROR in {cluster_size}-node {topology}: {e}")
            # Create a failed result instead of None
            failed_result = AutoDiscoveryTestResult(cluster_size, topology)
            failed_result.success = False
            failed_result.details.append(f"Exception: {e}")
            results.append(failed_result)

    # Final summary
    print(f"\n{'=' * 70}")
    print("COMPREHENSIVE AUTO-DISCOVERY TEST RESULTS")
    print(f"{'=' * 70}")
    print(f"PASSED: {passed}/{total} test cases")
    print(f"SUCCESS RATE: {(passed / total) * 100:.1f}%")

    print("\n=== DETAILED RESULTS ===")
    for i, (cluster_size, topology) in enumerate(test_cases):
        result = results[i]
        if result:
            status = "✅ PASS" if result.success else "❌ FAIL"
            print(f"{cluster_size:2d}-node {topology:12s}: {status}")
            if not result.success:
                print(
                    f"    Issues: {', '.join(result.details[:3])}"
                )  # Show first 3 issues
        else:
            print(f"{cluster_size:2d}-node {topology:12s}: ❌ ERROR")

    # Analysis
    print("\n=== ANALYSIS ===")
    if passed == total:
        print("🎉 AUTO-DISCOVERY IS WORKING PERFECTLY!")
        print("   All cluster sizes and topologies successfully auto-discover peers")
        print("   Function propagation works across all configurations")
    else:
        print("🚨 AUTO-DISCOVERY ISSUES DETECTED!")
        print("   Issues found in:")

        failing_patterns = []
        for i, (cluster_size, topology) in enumerate(test_cases):
            result = results[i]
            if result and not result.success:
                failing_patterns.append(f"{cluster_size}-node {topology}")

        for pattern in failing_patterns:
            print(f"   - {pattern}")

        print("\n🔧 RECOMMENDED FIXES:")
        print("   1. Implement proper advertised URL propagation")
        print("   2. Fix gossip protocol to forward peer information")
        print("   3. Add auto-connection to discovered peers")
        print("   4. Test function announcement propagation")


if __name__ == "__main__":
    asyncio.run(run_comprehensive_autodiscovery_tests())
