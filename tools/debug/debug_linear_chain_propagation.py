#!/usr/bin/env python3
"""
Debug script to test function propagation in a linear chain topology.
"""

import asyncio
import contextlib

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_linear_chain_propagation():
    """Test function propagation in a 3-node linear chain: A → B → C"""

    # Create 3 servers in linear chain
    server_a_settings = MPREGSettings(
        host="127.0.0.1",
        port=9000,
        name="Server-A",
        cluster_id="linear-test",
        resources={"resource-a"},
        connect=None,  # First server, no connection
        gossip_interval=0.1,
    )

    server_b_settings = MPREGSettings(
        host="127.0.0.1",
        port=9001,
        name="Server-B",
        cluster_id="linear-test",
        resources={"resource-b"},
        connect="ws://127.0.0.1:9000",  # Connect to A
        gossip_interval=0.1,
    )

    server_c_settings = MPREGSettings(
        host="127.0.0.1",
        port=9002,
        name="Server-C",
        cluster_id="linear-test",
        resources={"resource-c"},
        connect="ws://127.0.0.1:9001",  # Connect to B
        gossip_interval=0.1,
    )

    server_a = MPREGServer(settings=server_a_settings)
    server_b = MPREGServer(settings=server_b_settings)
    server_c = MPREGServer(settings=server_c_settings)

    # Start servers
    task_a = asyncio.create_task(server_a.server())
    task_b = asyncio.create_task(server_b.server())
    task_c = asyncio.create_task(server_c.server())

    # Wait for chain formation
    await asyncio.sleep(2.0)

    print("=== Initial Chain State ===")
    print(f"Server A functions: {list(server_a.cluster.funtimes.keys())}")
    print(f"Server B functions: {list(server_b.cluster.funtimes.keys())}")
    print(f"Server C functions: {list(server_c.cluster.funtimes.keys())}")

    # Register function on Server A
    def chain_test_function(data: str) -> str:
        return f"ServerA processed: {data}"

    print("\n=== Registering Function on Server A ===")
    server_a.register_command(
        "chain_test_function", chain_test_function, ["resource-a"]
    )

    # Wait for propagation through the chain A → B → C
    await asyncio.sleep(5.0)

    print("\n=== After Function Registration and Propagation ===")
    print(f"Server A functions: {list(server_a.cluster.funtimes.keys())}")
    print(f"Server B functions: {list(server_b.cluster.funtimes.keys())}")
    print(f"Server C functions: {list(server_c.cluster.funtimes.keys())}")

    # Test function call from Server C (should route to Server A)
    print("\n=== Testing Function Call from Server C ===")
    client = MPREGClientAPI("ws://127.0.0.1:9002")  # Connect to server C
    await client.connect()

    try:
        from mpreg.core.model import RPCCommand

        result = await client._client.request(
            [
                RPCCommand(
                    name="chain_test",
                    fun="chain_test_function",
                    args=("linear_chain_test",),
                    locs=frozenset(["resource-a"]),
                )
            ]
        )

        print(f"✅ SUCCESS: Function call result: {result}")
        success = True
    except Exception as e:
        print(f"❌ FAILED: Function call error: {e}")
        success = False

    await client.disconnect()

    # Show final analysis
    print("\n=== Linear Chain Propagation Analysis ===")
    print("Chain topology: A(9000) → B(9001) → C(9002)")
    print("Function registered on: Server A")
    print("Function call from: Server C")
    print("Expected: Function should be available on Server C through propagation")
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")

    if not success:
        print("\n🔍 Possible issues:")
        print("- Multi-hop propagation not working in linear chain")
        print("- Function announcement doesn't traverse the full chain")
        print("- Timing issues with chain formation")

    # Cleanup
    task_a.cancel()
    task_b.cancel()
    task_c.cancel()

    for task in [task_a, task_b, task_c]:
        with contextlib.suppress(asyncio.CancelledError):
            await task


if __name__ == "__main__":
    asyncio.run(test_linear_chain_propagation())
