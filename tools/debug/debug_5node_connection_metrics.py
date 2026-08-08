#!/usr/bin/env python3
"""
Debug script to check connection metrics in the 5-node cluster to verify N² gossip topology.
"""

import asyncio
import contextlib

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_5node_connection_metrics():
    """Test connection metrics in the exact same 5-node cluster as the performance test."""

    # Use the same ports as the performance test
    ports = [10000, 10001, 10002, 10003, 10004]

    # Create the ORIGINAL linear chain topology from the performance test
    settings_list = []
    for i, port in enumerate(ports):
        connect_to = f"ws://127.0.0.1:{ports[i - 1]}" if i > 0 else None

        settings_list.append(
            MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"Latency-Node-{i + 1}",
                cluster_id="latency-test-cluster",
                resources={f"latency-{i + 1}"},
                peers=None,
                connect=connect_to,
                advertised_urls=None,
                gossip_interval=0.1,  # Fast gossip for latency measurement
            )
        )

    servers = [MPREGServer(settings=s) for s in settings_list]

    # Start servers exactly like the performance test
    tasks = []
    for server in servers:
        task = asyncio.create_task(server.server())
        tasks.append(task)
        await asyncio.sleep(0.3)  # Same staggered startup

    await asyncio.sleep(2.0)  # Same initial wait

    print("=== 5-Node Cluster Connection Metrics ===")
    for i, server in enumerate(servers):
        peer_connections = list(server.peer_connections.keys())
        cluster_servers = server.cluster.servers  # Use the servers property instead
        functions = list(server.cluster.funtimes.keys())

        print(f"\nServer {i} (port {ports[i]}):")
        print(
            f"  Direct peer connections: {len(peer_connections)} = {peer_connections}"
        )
        print(
            f"  Known cluster servers: {len(cluster_servers)} = {list(cluster_servers)}"
        )
        print(f"  Known functions: {len(functions)} = {functions}")

    # Expected for N² gossip: Each server should see 4 others (5 total - itself)
    print("\n=== Connection Analysis ===")
    print("Expected for N² gossip cluster: Each server should see 4 other servers")

    all_properly_connected = True
    for i, server in enumerate(servers):
        expected_connections = 4  # N-1 for N=5
        actual_known_servers = len(server.cluster.servers)

        print(
            f"Server {i}: Expected {expected_connections}, Got {actual_known_servers} - {'✅' if actual_known_servers >= expected_connections else '❌'}"
        )

        if actual_known_servers < expected_connections:
            all_properly_connected = False

    print(
        f"\nOverall Result: {'✅ ALL SERVERS PROPERLY CONNECTED' if all_properly_connected else '❌ INCOMPLETE CONNECTIONS'}"
    )

    if not all_properly_connected:
        print("\n🔍 Connection Issues Detected:")
        print("- Linear chain topology may not form proper N² gossip cluster")
        print("- Some servers are not discovering all other servers")
        print("- This explains function propagation failures")

    # Now test function registration
    def test_function(data: str) -> str:
        return f"Latency test: {data}"

    print("\n=== Registering test_function on Server 0 ===")
    servers[0].register_command("test_function", test_function, ["latency-1"])

    # Wait for propagation
    await asyncio.sleep(5.0)

    print("\n=== Function Propagation Results ===")
    for i, server in enumerate(servers):
        functions = list(server.cluster.funtimes.keys())
        has_test_function = "test_function" in functions
        print(
            f"Server {i}: {functions} - {'✅' if has_test_function else '❌ MISSING test_function'}"
        )

    # Cleanup
    for task in tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


if __name__ == "__main__":
    asyncio.run(test_5node_connection_metrics())
