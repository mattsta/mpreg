#!/usr/bin/env python3
"""
Debug script to test federated function propagation.
"""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_federated_function_propagation():
    """Test that function announcements propagate across federated servers."""

    # Create two servers in a federation
    server1_settings = MPREGSettings(
        host="127.0.0.1",
        port=9998,
        name="Server-1",
        cluster_id="debug-federation",
        resources={"resource-1"},
        connect=None,  # First server, no connection
        gossip_interval=0.1,
    )

    server2_settings = MPREGSettings(
        host="127.0.0.1",
        port=9999,
        name="Server-2",
        cluster_id="debug-federation",
        resources={"resource-2"},
        connect="ws://127.0.0.1:9998",  # Connect to first server
        gossip_interval=0.1,
    )

    server1 = MPREGServer(settings=server1_settings)
    server2 = MPREGServer(settings=server2_settings)

    # Start servers
    task1 = asyncio.create_task(server1.server())
    task2 = asyncio.create_task(server2.server())

    # Wait for servers to start and connect
    await asyncio.sleep(1.0)

    print("=== Initial Server States ===")
    print(f"Server 1 functions: {list(server1.registry._commands.keys())}")
    print(f"Server 1 cluster peers: {hasattr(server1.cluster, 'peers')}")
    print(f"Server 2 functions: {list(server2.registry._commands.keys())}")
    print(f"Server 2 cluster peers: {hasattr(server2.cluster, 'peers')}")

    # Register a function on server 1
    def test_function(data: str) -> str:
        return f"Server1 processed: {data}"

    print("\n=== Registering Function on Server 1 ===")
    server1.register_command("test_function", test_function, ["resource-1"])

    # Wait for propagation
    await asyncio.sleep(2.0)

    print("\n=== After Function Registration ===")
    print(f"Server 1 functions: {list(server1.registry._commands.keys())}")
    print(f"Server 1 cluster functions: {list(server1.cluster.funtimes.keys())}")
    print(f"Server 2 functions: {list(server2.registry._commands.keys())}")
    print(f"Server 2 cluster functions: {list(server2.cluster.funtimes.keys())}")

    # Try to call the function from server 2 via client
    print("\n=== Testing Function Call from Server 2 ===")
    client = MPREGClientAPI("ws://127.0.0.1:9999")  # Connect to server 2
    await client.connect()

    try:
        from mpreg.core.model import RPCCommand

        result = await client._client.request(
            [
                RPCCommand(
                    name="test_call",
                    fun="test_function",
                    args=("federation_test",),
                    locs=frozenset(["resource-1"]),
                )
            ]
        )

        print(f"✅ SUCCESS: Function call result: {result}")
    except Exception as e:
        print(f"❌ FAILED: Function call error: {e}")

    await client.disconnect()

    # Cleanup
    task1.cancel()
    task2.cancel()

    try:
        await task1
    except asyncio.CancelledError:
        pass

    try:
        await task2
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(test_federated_function_propagation())
