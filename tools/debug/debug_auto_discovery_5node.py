#!/usr/bin/env python3
"""
Test if auto-discovery works with the ORIGINAL linear chain setup from the performance test.
This tests whether nodes auto-discover each other through gossip WITHOUT manual peers lists.
"""

import asyncio

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_auto_discovery_linear_chain():
    """Test auto-discovery in the ORIGINAL linear chain setup (connect only, no peers)."""

    # Use the same ports as the performance test
    ports = [10000, 10001, 10002, 10003, 10004]

    # Create the ORIGINAL linear chain topology (connect only, no peers)
    settings_list = []
    for i, port in enumerate(ports):
        connect_to = f"ws://127.0.0.1:{ports[i - 1]}" if i > 0 else None

        settings_list.append(
            MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"AutoDiscover-Node-{i + 1}",
                cluster_id="auto-discovery-test",
                resources={f"node-{i + 1}"},
                peers=None,  # NO manual peers list - should auto-discover
                connect=connect_to,  # Only initial connection
                advertised_urls=None,  # Should use default
                gossip_interval=0.5,  # Fast gossip for testing
            )
        )

    servers = [MPREGServer(settings=s) for s in settings_list]

    print("=== Starting Linear Chain Auto-Discovery Test ===")
    print("Chain: 0(10000) ← 1(10001) ← 2(10002) ← 3(10003) ← 4(10004)")
    print("Expected: All nodes should auto-discover each other through gossip")

    # Start servers with staggered startup
    tasks = []
    for i, server in enumerate(servers):
        task = asyncio.create_task(server.server())
        tasks.append(task)
        print(f"Started Node {i}")
        await asyncio.sleep(0.3)

    # Wait for auto-discovery to complete
    print("\n=== Waiting for Auto-Discovery (10 seconds) ===")
    await asyncio.sleep(10.0)

    print("\n=== Auto-Discovery Results ===")
    all_discovered = True

    for i, server in enumerate(servers):
        peer_connections = list(server.peer_connections.keys())
        cluster_servers = server.cluster.servers
        functions = list(server.cluster.funtimes.keys())

        expected_peers = 4  # Should discover all 4 other nodes
        actual_peers = len(cluster_servers)
        discovered = actual_peers >= expected_peers

        print(f"\nNode {i} (port {ports[i]}):")
        print(f"  Direct connections: {len(peer_connections)} = {peer_connections}")
        print(f"  Discovered cluster: {actual_peers}/4 = {list(cluster_servers)}")
        print(f"  Functions: {functions}")
        print(
            f"  Auto-discovery: {'✅ SUCCESS' if discovered else f'❌ INCOMPLETE ({actual_peers}/4)'}"
        )

        if not discovered:
            all_discovered = False

    # Test function propagation across auto-discovered nodes
    print("\n=== Testing Function Propagation ===")

    def auto_test_function(data: str) -> str:
        return f"Auto-discovery test: {data}"

    print("Registering function on Node 0...")
    servers[0].register_command("auto_test_function", auto_test_function, ["node-1"])

    # Wait for function propagation
    await asyncio.sleep(3.0)

    print("\nFunction Propagation Results:")
    function_propagated = True
    for i, server in enumerate(servers):
        functions = list(server.cluster.funtimes.keys())
        has_function = "auto_test_function" in functions
        print(f"  Node {i}: {functions} - {'✅' if has_function else '❌ MISSING'}")
        if not has_function:
            function_propagated = False

    print("\n=== FINAL RESULTS ===")
    print(
        f"Auto-Discovery: {'✅ ALL NODES DISCOVERED' if all_discovered else '❌ INCOMPLETE DISCOVERY'}"
    )
    print(
        f"Function Propagation: {'✅ ALL NODES UPDATED' if function_propagated else '❌ INCOMPLETE PROPAGATION'}"
    )
    print(
        f"Overall Status: {'✅ AUTO-DISCOVERY WORKING' if (all_discovered and function_propagated) else '❌ AUTO-DISCOVERY BROKEN'}"
    )

    if not all_discovered:
        print("\n🔧 Auto-Discovery Issues:")
        print("- Nodes are not discovering all peers through gossip")
        print("- Linear chain may not propagate peer information")
        print("- Advertised URLs may not be working correctly")

    if not function_propagated:
        print("\n🔧 Function Propagation Issues:")
        print("- Functions not reaching all auto-discovered nodes")
        print("- Gossip protocol may not handle function announcements")

    # Cleanup
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(test_auto_discovery_linear_chain())
