#!/usr/bin/env python3
"""
Debug script to investigate the peer discovery mechanism in detail.

This will trace:
1. What advertised_urls are being sent in HELLO messages
2. What gets stored in cluster.peers_info
3. What the _manage_peer_connections task is trying to connect to
4. Why auto-discovery isn't working for N² gossip clusters
"""

import asyncio

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def debug_peer_discovery_mechanism():
    """Debug the peer discovery mechanism step by step."""

    print("🔍 DEBUGGING PEER DISCOVERY MECHANISM")
    print("=" * 50)

    # Create 3-node star hub cluster to debug auto-discovery
    ports = [15000, 15001, 15002]

    # Node 0: Hub (no connect)
    hub_settings = MPREGSettings(
        host="127.0.0.1",
        port=ports[0],
        name="Debug-Hub",
        cluster_id="debug-cluster",
        resources={"hub-resource"},
        peers=None,
        connect=None,
        advertised_urls=None,  # Should generate default URL
        gossip_interval=2.0,  # Slower for debugging
    )

    # Node 1: Spoke (connects to hub)
    spoke1_settings = MPREGSettings(
        host="127.0.0.1",
        port=ports[1],
        name="Debug-Spoke-1",
        cluster_id="debug-cluster",
        resources={"spoke1-resource"},
        peers=None,
        connect=f"ws://127.0.0.1:{ports[0]}",
        advertised_urls=None,
        gossip_interval=2.0,
    )

    # Node 2: Spoke (connects to hub)
    spoke2_settings = MPREGSettings(
        host="127.0.0.1",
        port=ports[2],
        name="Debug-Spoke-2",
        cluster_id="debug-cluster",
        resources={"spoke2-resource"},
        peers=None,
        connect=f"ws://127.0.0.1:{ports[0]}",
        advertised_urls=None,
        gossip_interval=2.0,
    )

    servers = [
        MPREGServer(settings=hub_settings),
        MPREGServer(settings=spoke1_settings),
        MPREGServer(settings=spoke2_settings),
    ]

    print("🚀 Starting servers...")
    tasks = []
    for i, server in enumerate(servers):
        task = asyncio.create_task(server.server())
        tasks.append(task)
        print(f"  Started {server.settings.name} on port {ports[i]}")
        await asyncio.sleep(0.5)

    print("\n⏳ Waiting 3 seconds for initial connections...")
    await asyncio.sleep(3.0)

    print("\n📊 ANALYZING CLUSTER STATE AFTER INITIAL CONNECTIONS")
    print("=" * 60)

    for i, server in enumerate(servers):
        print(f"\n🖥️  {server.settings.name} (port {ports[i]}):")

        # Check advertised URLs
        advertised_urls = getattr(
            server, "advertised_urls", server.settings.advertised_urls
        )
        print(f"  📡 Advertised URLs: {advertised_urls}")

        # Check cluster peers_info
        peers_info = server.cluster.peers_info
        print(f"  🔗 Known peers in cluster.peers_info ({len(peers_info)}):")
        for url, peer_info in peers_info.items():
            print(f"    - {url}")
            print(f"      └─ advertised_urls: {peer_info.advertised_urls}")
            print(f"      └─ cluster_id: {peer_info.cluster_id}")
            print(f"      └─ functions: {peer_info.funs}")

        # Check active peer connections
        peer_connections = server.peer_connections
        print(f"  🔐 Active peer connections ({len(peer_connections)}):")
        for url, connection in peer_connections.items():
            print(
                f"    - {url} -> {'✅ Connected' if connection.is_connected else '❌ Disconnected'}"
            )

        # Check cluster servers (discovered through functions)
        cluster_servers = server.cluster.servers
        print(f"  🌐 Discovered cluster servers ({len(cluster_servers)}):")
        for server_url in cluster_servers:
            print(f"    - {server_url}")

    print("\n⏳ Waiting another 5 seconds for auto-discovery...")
    await asyncio.sleep(5.0)

    print("\n📊 ANALYZING CLUSTER STATE AFTER AUTO-DISCOVERY PERIOD")
    print("=" * 60)

    # Expected behavior: Each node should have discovered all other nodes
    # Hub should know about both spokes
    # Each spoke should know about hub + other spoke (via hub's gossip)

    expected_peers = {
        0: 2,  # Hub should see both spokes
        1: 2,  # Spoke1 should see hub + spoke2
        2: 2,  # Spoke2 should see hub + spoke1
    }

    print("\n🎯 AUTO-DISCOVERY ANALYSIS:")
    discovery_working = True

    for i, server in enumerate(servers):
        actual_peers = len(server.cluster.peers_info)
        expected = expected_peers[i]

        status = "✅ SUCCESS" if actual_peers >= expected else "❌ FAILED"
        print(
            f"  {server.settings.name}: {actual_peers}/{expected} peers discovered - {status}"
        )

        if actual_peers < expected:
            discovery_working = False

            print(f"    🔍 Missing peers for {server.settings.name}:")
            all_expected_peers = {
                f"ws://127.0.0.1:{p}" for j, p in enumerate(ports) if j != i
            }
            known_peers = set(server.cluster.peers_info.keys())
            missing_peers = all_expected_peers - known_peers

            for missing_peer in missing_peers:
                print(f"      - Missing: {missing_peer}")

    overall_status = (
        "✅ AUTO-DISCOVERY WORKING" if discovery_working else "❌ AUTO-DISCOVERY BROKEN"
    )
    print(f"\n🏁 Overall Status: {overall_status}")

    if not discovery_working:
        print("\n🔧 DIAGNOSIS:")
        print(
            "  The peer discovery mechanism is not propagating peer metadata properly."
        )
        print(
            "  Nodes are only discovering their direct connections, not indirect peers."
        )
        print(
            "  The gossip protocol needs to share peer information, not just function information."
        )

        print("\n💡 ROOT CAUSE HYPOTHESIS:")
        print(
            "  1. HELLO messages contain advertised_urls but only share function metadata"
        )
        print(
            "  2. Peers are added to cluster.peers_info but no active connections are made to advertised_urls"
        )
        print("  3. _manage_peer_connections should connect to all peers_info entries")
        print(
            "  4. But it's either not running or not finding the right URLs to connect to"
        )

    # Cleanup
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(debug_peer_discovery_mechanism())
