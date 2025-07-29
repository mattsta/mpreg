#!/usr/bin/env python3
"""
Debug script to test function propagation in the exact same 5-node linear chain as the performance test.
"""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_5node_chain_propagation():
    """Test function propagation in the exact same 5-node linear chain as the performance test."""

    # Use the same ports as the performance test
    ports = [10000, 10001, 10002, 10003, 10004]

    # Create linear chain topology exactly like the performance test
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

    print("=== 5-Node Chain State After Formation ===")
    for i, server in enumerate(servers):
        print(f"Server {i} functions: {list(server.cluster.funtimes.keys())}")

    # Exact same function registration as the performance test
    def test_function(data: str) -> str:
        return f"Latency test: {data}"

    print("\n=== Registering test_function on Server 0 ===")
    servers[0].register_command("test_function", test_function, ["latency-1"])

    # Wait for propagation and test from the last node - same as performance test
    await asyncio.sleep(10.0)

    print("\n=== After Function Registration and Propagation ===")
    for i, server in enumerate(servers):
        print(f"Server {i} functions: {list(server.cluster.funtimes.keys())}")

    # Test function call from last server exactly like the performance test
    print(f"\n=== Testing Function Call from Server {len(ports) - 1} (Last Node) ===")
    client = MPREGClientAPI(f"ws://127.0.0.1:{ports[-1]}")  # Connect to last node
    await client.connect()

    try:
        from mpreg.core.model import RPCCommand

        result = await client._client.request(
            [
                RPCCommand(
                    name="latency_test",
                    fun="test_function",
                    args=("propagation_test",),
                    locs=frozenset(["latency-1"]),
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
    print("\n=== 5-Node Linear Chain Analysis ===")
    print("Chain topology: 0(10000) → 1(10001) → 2(10002) → 3(10003) → 4(10004)")
    print("Function registered on: Server 0")
    print("Function call from: Server 4")
    print(
        "Expected: Function should be available on Server 4 through multi-hop propagation"
    )
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")

    if not success:
        print("\n🔍 Debugging multi-hop propagation failure:")
        print("- Check if all servers can see the function in their funtimes")
        print("- Verify HELLO message exchanges in 5-node chain")
        print("- Look for timing issues in longer chains")

    # Cleanup
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(test_5node_chain_propagation())
