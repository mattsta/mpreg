#!/usr/bin/env python3
"""
Test script to validate the federated RPC system implementation.

This script tests the complete federated RPC functionality with real servers
to ensure announcements, propagation, and discovery work correctly.
"""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_federated_rpc_system():
    """Test the federated RPC system with 3 nodes."""

    print("🚀 Testing Federated RPC System")
    print("=" * 50)

    # Setup same configuration as the failing test
    ports = [10000, 10001, 10002]

    # Node 1: Hub
    node1_settings = MPREGSettings(
        host="127.0.0.1",
        port=ports[0],
        name="Fed-Test-Hub",
        cluster_id="federated-test-cluster",
        resources={"hub-resource"},
        peers=None,
        connect=None,
        advertised_urls=None,
        gossip_interval=1.0,
    )

    # Node 2: Spoke-1 (connects to hub)
    node2_settings = MPREGSettings(
        host="127.0.0.1",
        port=ports[1],
        name="Fed-Test-Spoke-1",
        cluster_id="federated-test-cluster",
        resources={"spoke1-resource"},
        peers=None,
        connect=f"ws://127.0.0.1:{ports[0]}",
        advertised_urls=None,
        gossip_interval=1.0,
    )

    # Node 3: Spoke-2 (connects to hub)
    node3_settings = MPREGSettings(
        host="127.0.0.1",
        port=ports[2],
        name="Fed-Test-Spoke-2",
        cluster_id="federated-test-cluster",
        resources={"spoke2-resource"},
        peers=None,
        connect=f"ws://127.0.0.1:{ports[0]}",
        advertised_urls=None,
        gossip_interval=1.0,
    )

    servers = [
        MPREGServer(settings=node1_settings),
        MPREGServer(settings=node2_settings),
        MPREGServer(settings=node3_settings),
    ]

    # Register test functions
    def hub_coordinate(task_name: str) -> dict:
        return {
            "coordinator": "Fed-Test-Hub",
            "task_name": task_name,
            "distributed_to": ["spoke1", "spoke2"],
        }

    def spoke1_process(coordination: dict) -> dict:
        return {
            "processor": "Fed-Test-Spoke-1",
            "task_name": coordination["task_name"],
            "result": f"processed_{coordination['task_name']}_by_spoke1",
        }

    def spoke2_analyze(spoke1_result: dict) -> str:
        return f"Analysis by Fed-Test-Spoke-2: {spoke1_result['result']} is complete"

    # Start servers first
    tasks = []
    for i, server in enumerate(servers):
        print(f"🚀 Starting server {i + 1} on port {ports[i]}")
        task = asyncio.create_task(server.server())
        tasks.append(task)
        await asyncio.sleep(0.5)

    # Wait for servers to be ready and connections to be established
    print(
        "⏱️  Waiting 5 seconds for servers to initialize and connections to establish..."
    )
    await asyncio.sleep(5.0)

    # Now register functions (when event loop is running and connections are established)
    print("📝 Registering functions with federated announcements...")
    servers[0].register_command("hub_coordinate", hub_coordinate, ["hub-resource"])
    servers[1].register_command("spoke1_process", spoke1_process, ["spoke1-resource"])
    servers[2].register_command("spoke2_analyze", spoke2_analyze, ["spoke2-resource"])

    print("⏱️  Waiting 5 seconds for federated RPC propagation...")
    await asyncio.sleep(5.0)

    # Check cluster status on each node using MPREG's native funtimes mapping
    print("\n🔍 Checking cluster function status:")
    for i, server in enumerate(servers):
        cluster = server.cluster
        total_functions = len(cluster.funtimes)
        all_servers = set()
        for fun_servers in cluster.funtimes.values():
            for locs_servers in fun_servers.values():
                all_servers.update(locs_servers)

        print(
            f"  Node {i + 1}: {total_functions} functions, {len(all_servers)} servers"
        )

        # Show what functions each node can see
        all_functions = list(cluster.funtimes.keys())
        print(f"    Functions: {all_functions}")

        for func_name in ["hub_coordinate", "spoke1_process", "spoke2_analyze"]:
            if func_name in cluster.funtimes:
                # Find all servers that have this function
                servers_with_func = set()
                for locs_servers in cluster.funtimes[func_name].values():
                    servers_with_func.update(locs_servers)
                print(f"    {func_name}: found on {servers_with_func}")
            else:
                print(f"    {func_name}: found on set()")

    # Test execution from Node 2 (spoke1) - this should now work with federated RPC
    print("\n🧪 Testing federated execution from Node 2...")
    client = MPREGClientAPI(f"ws://127.0.0.1:{ports[1]}")
    await client.connect()

    try:
        from mpreg.core.model import RPCCommand

        result = await client._client.request(
            [
                # Step 1: Hub coordinates (Node 1)
                RPCCommand(
                    name="coordination",
                    fun="hub_coordinate",
                    args=("federated_test_task",),
                    locs=frozenset(["hub-resource"]),
                ),
                # Step 2: Spoke 1 processes (Node 2)
                RPCCommand(
                    name="processing",
                    fun="spoke1_process",
                    args=("coordination",),
                    locs=frozenset(["spoke1-resource"]),
                ),
                # Step 3: Spoke 2 analyzes (Node 3) - This tests federated discovery
                RPCCommand(
                    name="analysis",
                    fun="spoke2_analyze",
                    args=("processing",),
                    locs=frozenset(["spoke2-resource"]),
                ),
            ]
        )

        # Check results
        if "analysis" in result:
            final_result = result["analysis"]
            print(f"  ✅ SUCCESS: {final_result}")
            print(
                "  🎉 Federated RPC system is working! Spoke nodes can discover each other's functions."
            )
        else:
            print(
                f"  ❌ FAILURE: Expected 'analysis' in result, got: {list(result.keys())}"
            )

    except Exception as e:
        print(f"  ❌ FAILURE: {type(e).__name__}: {e}")

    finally:
        await client.disconnect()

    # Cleanup
    print("\n🧹 Cleaning up servers...")
    for task in tasks:
        task.cancel()

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        # Task cancellation during cleanup is expected
        pass

    print("✅ Test complete!")


if __name__ == "__main__":
    asyncio.run(test_federated_rpc_system())
