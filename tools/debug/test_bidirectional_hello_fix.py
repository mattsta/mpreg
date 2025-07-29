#!/usr/bin/env python3
"""
Test bidirectional HELLO exchange for PRE-CONNECTION function sharing.
"""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_bidirectional_hello_pre_connection():
    """Test that PRE-CONNECTION functions are shared via bidirectional HELLO exchange."""
    print("🔥 TESTING BIDIRECTIONAL HELLO EXCHANGE")

    # Primary server settings
    settings1 = MPREGSettings(
        host="127.0.0.1",
        port=9001,
        name="Primary Server",
        cluster_id="test-cluster",
        resources={"model-a", "dataset-1"},
        peers=None,
        connect=None,
        advertised_urls=None,
        gossip_interval=5.0,
    )

    # Secondary server that connects to primary
    settings2 = MPREGSettings(
        host="127.0.0.1",
        port=9002,
        name="Secondary Server",
        cluster_id="test-cluster",
        resources={"model-b", "dataset-2"},
        peers=None,
        connect="ws://127.0.0.1:9001",
        advertised_urls=None,
        gossip_interval=5.0,
    )

    server1 = MPREGServer(settings=settings1)
    server2 = MPREGServer(settings=settings2)

    # 🔧 CRITICAL: Register function BEFORE starting servers (PRE-CONNECTION)
    def pre_bidirectional_test(data: str) -> str:
        return f"bidirectional_hello_success: {data}"

    print("📝 Registering PRE-CONNECTION function on server1...")
    server1.register_command(
        "pre_bidirectional_test", pre_bidirectional_test, ["bidirectional-resource"]
    )

    print("🚀 Starting server1...")
    task1 = asyncio.create_task(server1.server())
    await asyncio.sleep(0.2)

    print("🚀 Starting server2 (will connect to server1)...")
    task2 = asyncio.create_task(server2.server())

    # 🔍 Wait for bidirectional HELLO exchange and cluster formation
    print("⏰ Waiting for bidirectional HELLO exchange...")
    await asyncio.sleep(1.0)

    # 🎯 TEST: server2 should now know about server1's PRE-CONNECTION function
    print("🔍 DEBUG: Checking cluster states...")
    print(
        f"🟦 Server1 cluster has {len(server1.cluster.peers_info)} peers: {list(server1.cluster.peers_info.keys())}"
    )
    print(f"🟦 Server1 functions: {list(server1.registry._commands.keys())}")
    print(
        f"🟨 Server2 cluster has {len(server2.cluster.peers_info)} peers: {list(server2.cluster.peers_info.keys())}"
    )
    print(f"🟨 Server2 functions: {list(server2.registry._commands.keys())}")

    print("🔍 Testing cross-server call from server2 to server1...")
    client = MPREGClientAPI(f"ws://127.0.0.1:{server2.settings.port}")
    await client.connect()

    try:
        result = await client.call(
            "pre_bidirectional_test",
            "hello_world",
            locs=frozenset(["bidirectional-resource"]),
        )
        print(f"✅ BIDIRECTIONAL HELLO SUCCESS: {result}")
        success = result == "bidirectional_hello_success: hello_world"
    except Exception as e:
        print(f"❌ BIDIRECTIONAL HELLO FAILED: {type(e).__name__}: {e}")
        # Let's check what functions server2 actually knows about
        try:
            functions_result = await client.call("echo", "/functions", locs=frozenset())
            print(f"📋 Server2 available functions: {functions_result}")
        except Exception as e2:
            print(f"❌ Could not get function list: {e2}")
        success = False

    # Cleanup
    await client.disconnect()
    server1.shutdown()
    server2.shutdown()
    task1.cancel()
    task2.cancel()

    try:
        await asyncio.gather(task1, task2, return_exceptions=True)
    except asyncio.CancelledError:
        # Task cancellation during cleanup is expected
        pass

    return success


async def main():
    """Run the bidirectional HELLO test."""
    print("🎯 BIDIRECTIONAL HELLO EXCHANGE TEST")
    print("=" * 50)

    success = await test_bidirectional_hello_pre_connection()

    print("\n" + "=" * 50)
    if success:
        print("🎉 BIDIRECTIONAL HELLO EXCHANGE WORKING!")
        print("🔥 PRE-CONNECTION FUNCTIONS NOW SUPPORTED!")
        return 0
    else:
        print("💥 BIDIRECTIONAL HELLO EXCHANGE STILL BROKEN!")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
