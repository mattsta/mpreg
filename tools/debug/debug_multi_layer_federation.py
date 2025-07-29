#!/usr/bin/env python3
"""
DEEP DIVE DEBUG: Multi-Layer Federation Communication Analysis

This script will provide PROOF of where nodes are failing to communicate
in the multi-layer federation cascade test.
"""

import asyncio
from typing import Any

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.port_allocator import get_port_allocator


async def debug_multi_layer_federation():
    """Debug multi-layer federation with detailed connection analysis."""

    print("🔍 DEEP DIVE DEBUG: Multi-Layer Federation Communication Analysis")
    print("=" * 80)

    port_allocator = get_port_allocator()

    async with AsyncTestContext() as test_context:
        # Step 1: Create simplified 3-layer federation
        layer_configs = [
            {"name": "Local", "cluster_sizes": [2, 2], "federation_type": "local"},
            {
                "name": "Regional",
                "cluster_sizes": [2, 2],
                "federation_type": "regional",
            },
            {"name": "Global", "cluster_sizes": [2, 2], "federation_type": "global"},
        ]

        all_servers = []
        layer_results: list[dict[str, Any]] = []

        for layer_idx, config in enumerate(layer_configs):
            print(f"\n📍 Creating {config['name']} Layer...")

            layer_servers = []
            layer_clusters = []

            for cluster_idx, size in enumerate(config["cluster_sizes"]):
                # Type check and convert size to int
                if isinstance(size, int):
                    cluster_size = size
                else:
                    # Cast to avoid mypy issue - we know this should be convertible
                    cluster_size = int(str(size))
                cluster_ports = port_allocator.allocate_port_range(
                    cluster_size, "research"
                )
                print(f"   Cluster {cluster_idx}: ports {cluster_ports}")

                # Create cluster servers
                cluster_servers = []
                for node_idx, port in enumerate(cluster_ports):
                    connect_to = (
                        f"ws://127.0.0.1:{cluster_ports[0]}" if node_idx > 0 else None
                    )

                    settings = MPREGSettings(
                        host="127.0.0.1",
                        port=port,
                        name=f"{config['name']}-C{cluster_idx}-N{node_idx}",
                        cluster_id="debug-federation",  # SAME cluster_id
                        resources={"debug-resource"},
                        peers=None,
                        connect=connect_to,
                        advertised_urls=None,
                        gossip_interval=0.5,
                    )

                    server = MPREGServer(settings=settings)
                    cluster_servers.append(server)
                    layer_servers.append(server)
                    all_servers.append(server)

                    print(
                        f"     Node {settings.name}: {port} -> connect_to={connect_to}"
                    )

                layer_clusters.append(cluster_servers)

                # Start cluster servers
                for server in cluster_servers:
                    task = asyncio.create_task(server.server())
                    test_context.tasks.append(task)
                    await asyncio.sleep(0.1)

            test_context.servers.extend(layer_servers)

            # Wait for intra-layer convergence
            print(f"   Waiting for {config['name']} layer convergence...")
            await asyncio.sleep(3.0)

            # DEBUG: Check intra-layer connections
            print(f"   🔍 DEBUG: {config['name']} Layer Connection Analysis:")
            for server in layer_servers:
                connections = list(server.peer_connections.keys())
                print(
                    f"     {server.settings.name}: {len(connections)} connections -> {connections}"
                )

            # Create inter-layer federation bridges
            if layer_idx > 0:
                print(
                    f"   🌉 Creating federation bridges to {layer_configs[layer_idx - 1]['name']} layer..."
                )

                # Connect first node of each cluster to corresponding node in previous layer
                prev_layer_clusters = layer_results[layer_idx - 1]["clusters"]
                assert isinstance(prev_layer_clusters, list), (
                    "clusters should be a list"
                )

                for cluster_idx, current_cluster in enumerate(layer_clusters):
                    if cluster_idx < len(prev_layer_clusters):
                        current_hub = current_cluster[
                            0
                        ]  # First node of current cluster
                        prev_hub = prev_layer_clusters[cluster_idx][
                            0
                        ]  # First node of prev cluster

                        try:
                            print(
                                f"     Connecting {current_hub.settings.name} -> {prev_hub.settings.name}"
                            )
                            await current_hub._establish_peer_connection(
                                f"ws://127.0.0.1:{prev_hub.settings.port}"
                            )
                            print(
                                f"     ✅ Bridge: {current_hub.settings.name} ↔ {prev_hub.settings.name}"
                            )
                        except Exception as e:
                            print(
                                f"     ❌ Bridge failed: {current_hub.settings.name} -> {prev_hub.settings.name}: {e}"
                            )

                # Wait for bridge convergence
                print("   Waiting for federation bridge convergence...")
                await asyncio.sleep(4.0)

                # DEBUG: Check cross-layer connections
                print("   🔍 DEBUG: Cross-Layer Bridge Analysis:")
                for server in layer_servers:
                    connections = list(server.peer_connections.keys())
                    print(
                        f"     {server.settings.name}: {len(connections)} connections -> {connections}"
                    )

            layer_results.append(
                {
                    "name": config["name"],
                    "servers": layer_servers,
                    "clusters": layer_clusters,
                }
            )

        print("\n🔍 FINAL CONNECTION MATRIX:")
        print("-" * 60)
        for server in all_servers:
            connections = list(server.peer_connections.keys())
            connection_status = []
            for conn_url in connections:
                # Extract port from URL
                port_str = conn_url.split(":")[-1]
                port = int(port_str)
                # Find server with that port
                target_server = None
                for s in all_servers:
                    if s.settings.port == port:
                        target_server = s
                        break
                if target_server:
                    connection_status.append(f"{target_server.settings.name}")
                else:
                    connection_status.append(f"UNKNOWN:{port}")

            print(
                f"{server.settings.name:20} -> {len(connections)} connections: {connection_status}"
            )

        print("\n🔍 FUNCTION PROPAGATION TEST:")
        print("-" * 40)

        # Register function on Global layer
        def debug_cascade_function(data: str) -> str:
            return f"Debug cascade: {data}"

        global_server_obj = layer_results[2]["servers"][
            0
        ]  # First server in Global layer
        assert hasattr(global_server_obj, "settings"), "Server should have settings"
        assert hasattr(global_server_obj, "register_command"), (
            "Server should have register_command method"
        )
        print(f"Registering function on: {global_server_obj.settings.name}")

        global_server_obj.register_command(
            "debug_cascade_test", debug_cascade_function, ["debug-resource"]
        )

        # Wait for propagation
        await asyncio.sleep(5.0)

        # Check which nodes received the function
        print("\n🔍 FUNCTION PROPAGATION RESULTS:")
        nodes_with_function = 0
        for server in all_servers:
            has_function = "debug_cascade_test" in server.cluster.funtimes
            status = "✅ HAS" if has_function else "❌ MISSING"
            print(f"  {server.settings.name:20}: {status} debug_cascade_test")
            if has_function:
                nodes_with_function += 1

        success_rate = nodes_with_function / len(all_servers)
        print("\n📊 PROPAGATION ANALYSIS:")
        print(f"  Total nodes: {len(all_servers)}")
        print(f"  Nodes with function: {nodes_with_function}")
        print(f"  Success rate: {success_rate:.2%}")

        print("\n🔍 ROOT CAUSE ANALYSIS:")

        # Analyze connectivity patterns
        isolated_nodes = []
        well_connected_nodes = []

        for server in all_servers:
            connection_count = len(server.peer_connections)
            if connection_count == 0:
                isolated_nodes.append(server.settings.name)
            elif connection_count >= 2:
                well_connected_nodes.append(server.settings.name)

        if isolated_nodes:
            print(f"  ❌ ISOLATED NODES (0 connections): {isolated_nodes}")
        if well_connected_nodes:
            print(f"  ✅ WELL-CONNECTED NODES (2+ connections): {well_connected_nodes}")

        # Check if function source is connected
        source_connections = len(global_server_obj.peer_connections)
        print(
            f"  📡 Function source ({global_server_obj.settings.name}): {source_connections} connections"
        )

        if source_connections == 0:
            print("  🚨 ROOT CAUSE: Function source is ISOLATED!")
        elif success_rate < 0.5:
            print(
                "  🚨 ROOT CAUSE: Federation bridges are not propagating functions effectively"
            )
            print(
                "     Check if all nodes share the same cluster_id and compatible resources"
            )
        else:
            print("  ✅ Federation appears to be working correctly")

        print("=" * 80)
        print("🔍 DEBUG ANALYSIS COMPLETE")


if __name__ == "__main__":
    asyncio.run(debug_multi_layer_federation())
