#!/usr/bin/env python3
"""Simple test to debug RPC execution issue."""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer


async def debug_simple_rpc():
    """Test simple RPC execution."""

    print("🔍 Debug Simple RPC Execution")
    print("=" * 40)

    # Single server test
    settings = MPREGSettings(
        host="127.0.0.1",
        port=20000,
        name="Debug-Server",
        cluster_id="debug-cluster",
        resources={"debug-resource"},
    )

    server = MPREGServer(settings=settings)

    # Register simple function
    def simple_function(message: str) -> str:
        return f"Echo: {message}"

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(1.0)  # Let server start

    # Register function after server is running
    server.register_command("simple_function", simple_function, ["debug-resource"])
    print("📝 Registered function: simple_function")

    await asyncio.sleep(1.0)  # Let registration propagate

    # Check cluster state
    print(f"🔍 Cluster functions: {list(server.cluster.funtimes.keys())}")
    print(f"🔍 Cluster servers: {len(server.cluster.servers)}")

    # Test with client
    client = MPREGClientAPI("ws://127.0.0.1:20000")
    await client.connect()

    try:
        result = await client._client.request(
            [
                RPCCommand(
                    name="test",
                    fun="simple_function",
                    args=("Hello World",),
                    locs=frozenset(["debug-resource"]),
                )
            ]
        )

        print(f"✅ SUCCESS: {result}")

    except Exception as e:
        print(f"❌ FAILURE: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()

    finally:
        await client.disconnect()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass  # Server cancellation during cleanup is expected


if __name__ == "__main__":
    asyncio.run(debug_simple_rpc())
