#!/usr/bin/env python3
import contextlib

from mpreg.core.errors import OPERATIONAL_EXCEPTIONS

"""
Test fabric catalog propagation for PRE-CONNECTION function sharing.
"""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager


async def test_bidirectional_catalog_pre_connection():
    """Test that PRE-CONNECTION functions are shared via catalog propagation."""
    print("🔥 TESTING BIDIRECTIONAL CATALOG PROPAGATION")

    with TestPortManager() as port_manager:
        server_urls = port_manager.get_server_cluster(2)
        server1_url = server_urls[0]
        server2_url = server_urls[1]
        server1_port = int(server1_url.rsplit(":", 1)[1])
        server2_port = int(server2_url.rsplit(":", 1)[1])

        # Primary server settings
        settings1 = MPREGSettings(
            host="127.0.0.1",
            port=server1_port,
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
            port=server2_port,
            name="Secondary Server",
            cluster_id="test-cluster",
            resources={"model-b", "dataset-2"},
            peers=None,
            connect=server1_url,
            advertised_urls=None,
            gossip_interval=5.0,
        )

        server1 = MPREGServer(settings=settings1)
        server2 = MPREGServer(settings=settings2)

        # 🔧 CRITICAL: Register function BEFORE starting servers (PRE-CONNECTION)
        def pre_bidirectional_test(data: str) -> str:
            return f"bidirectional_catalog_success: {data}"

        print("📝 Registering PRE-CONNECTION function on server1...")
        server1.register_command(
            "pre_bidirectional_test", pre_bidirectional_test, ["bidirectional-resource"]
        )

        print("🚀 Starting server1...")
        task1 = asyncio.create_task(server1.server())
        await asyncio.sleep(0.2)

        print("🚀 Starting server2 (will connect to server1)...")
        task2 = asyncio.create_task(server2.server())

        # 🔍 Wait for catalog propagation and cluster formation
        print("⏰ Waiting for catalog propagation...")
        await asyncio.sleep(7.0)

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
        client = MPREGClientAPI(server2_url)
        await client.connect()

        try:
            result = await client.call(
                "pre_bidirectional_test",
                "hello_world",
                locs=frozenset(["bidirectional-resource"]),
            )
            print(f"✅ BIDIRECTIONAL CATALOG SUCCESS: {result}")
            success = result == "bidirectional_catalog_success: hello_world"
        except OPERATIONAL_EXCEPTIONS as e:
            print(f"❌ BIDIRECTIONAL CATALOG FAILED: {type(e).__name__}: {e}")
            # Let's check what functions server2 actually knows about
            try:
                functions_result = await client.call(
                    "echo", "/functions", locs=frozenset()
                )
                print(f"📋 Server2 available functions: {functions_result}")
            except OPERATIONAL_EXCEPTIONS as e2:
                print(f"❌ Could not get function list: {e2}")
            success = False

        # Cleanup
        await client.disconnect()
        server1.shutdown()
        server2.shutdown()
        task1.cancel()
        task2.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(task1, task2, return_exceptions=True)

        return success


async def main():
    """Run the bidirectional catalog test."""
    print("🎯 BIDIRECTIONAL CATALOG PROPAGATION TEST")
    print("=" * 50)

    success = await test_bidirectional_catalog_pre_connection()

    print("\n" + "=" * 50)
    if success:
        print("🎉 BIDIRECTIONAL CATALOG PROPAGATION WORKING!")
        print("🔥 PRE-CONNECTION FUNCTIONS NOW SUPPORTED!")
        return 0
    else:
        print("💥 BIDIRECTIONAL CATALOG PROPAGATION STILL BROKEN!")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
