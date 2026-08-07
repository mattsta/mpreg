#!/usr/bin/env python3
"""Debug gossip blocking behavior after GOODBYE."""

import asyncio
import contextlib

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager


async def debug_gossip_blocking():
    """Debug gossip blocking with detailed logging."""
    port_manager = TestPortManager()

    try:
        # Create 3 servers: Server1 <-> Server2 <-> Server3 (linear chain)
        server1_port = port_manager.get_server_port()
        server2_port = port_manager.get_server_port()
        server3_port = port_manager.get_server_port()

        print(f"Server1 port: {server1_port}")
        print(f"Server2 port: {server2_port}")
        print(f"Server3 port: {server3_port}")

        server1 = MPREGServer(
            MPREGSettings(
                port=server1_port,
                name="Server1",
                cluster_id="gossip-debug",
                log_level="DEBUG",
                gossip_interval=0.5,
            )
        )

        server2 = MPREGServer(
            MPREGSettings(
                port=server2_port,
                name="Server2",
                cluster_id="gossip-debug",
                peers=[f"ws://127.0.0.1:{server1_port}"],
                log_level="DEBUG",
                gossip_interval=0.5,
            )
        )

        server3 = MPREGServer(
            MPREGSettings(
                port=server3_port,
                name="Server3",
                cluster_id="gossip-debug",
                peers=[f"ws://127.0.0.1:{server2_port}"],
                log_level="DEBUG",
                gossip_interval=0.5,
            )
        )

        # Start servers
        server1_task = asyncio.create_task(server1.server())
        server2_task = asyncio.create_task(server2.server())
        server3_task = asyncio.create_task(server3.server())

        # Wait for cluster formation
        print("Waiting for cluster formation...")
        await asyncio.sleep(3.0)

        server2_url = f"ws://127.0.0.1:{server2_port}"

        print("\n=== INITIAL STATE ===")
        print(f"Server1 peers_info: {list(server1.cluster.peers_info.keys())}")
        print(f"Server3 peers_info: {list(server3.cluster.peers_info.keys())}")
        print(f"Server1 departed_peers: {set(server1.cluster._departed_peers.keys())}")
        print(f"Server3 departed_peers: {set(server3.cluster._departed_peers.keys())}")

        # Server2 sends GOODBYE
        print("\n=== SENDING GOODBYE ===")
        await server2.send_goodbye(GoodbyeReason.CLUSTER_REBALANCE)
        await asyncio.sleep(1.0)

        print(f"Server1 peers_info: {list(server1.cluster.peers_info.keys())}")
        print(f"Server3 peers_info: {list(server3.cluster.peers_info.keys())}")
        print(f"Server1 departed_peers: {set(server1.cluster._departed_peers.keys())}")
        print(f"Server3 departed_peers: {set(server3.cluster._departed_peers.keys())}")

        # Shutdown Server2
        print("\n=== SHUTTING DOWN SERVER2 ===")
        await server2.shutdown_async()
        server2_task.cancel()
        await asyncio.sleep(0.3)

        # Restart Server2 (connects only to Server1)
        print("\n=== RESTARTING SERVER2 ===")
        server2_new = MPREGServer(
            MPREGSettings(
                port=server2_port,
                name="Server2_Restarted",
                cluster_id="gossip-debug",
                peers=[f"ws://127.0.0.1:{server1_port}"],  # Only connects to Server1
                log_level="DEBUG",
                gossip_interval=0.5,
            )
        )

        server2_new_task = asyncio.create_task(server2_new.server())
        await asyncio.sleep(1.5)

        print(f"Server1 peers_info: {list(server1.cluster.peers_info.keys())}")
        print(f"Server3 peers_info: {list(server3.cluster.peers_info.keys())}")
        print(f"Server1 departed_peers: {set(server1.cluster._departed_peers.keys())}")
        print(f"Server3 departed_peers: {set(server3.cluster._departed_peers.keys())}")

        # Wait for gossip propagation
        print("\n=== AFTER GOSSIP PROPAGATION ===")
        await asyncio.sleep(3.0)

        print(f"Server1 peers_info: {list(server1.cluster.peers_info.keys())}")
        print(f"Server3 peers_info: {list(server3.cluster.peers_info.keys())}")
        print(f"Server1 departed_peers: {set(server1.cluster._departed_peers.keys())}")
        print(f"Server3 departed_peers: {set(server3.cluster._departed_peers.keys())}")

        # THE KEY TEST: With re-entry broadcast, Server3 should clear Server2 from departed peers
        if server2_url not in server3.cluster._departed_peers:
            print(
                "✅ CORRECT: Server3 cleared Server2 from departed peers (re-entry broadcast)"
            )
        else:
            print("❌ BUG: Server3 incorrectly kept Server2 in departed peers")

        # Server3 should have Server2 in peers_info due to re-entry broadcast
        if server2_url in server3.cluster.peers_info:
            print(
                "✅ CORRECT: Server3 has Server2 in peers_info (re-entry broadcast propagated)"
            )
        else:
            print(
                "❌ BUG: Server3 incorrectly blocked Server2 despite re-entry broadcast"
            )

    finally:
        # Cleanup
        try:
            await server1.shutdown_async()
            await server2_new.shutdown_async()
            await server3.shutdown_async()
        except Exception as e:
            print(f"Error during cleanup: {e}")

        for task in [server1_task, server2_new_task, server3_task]:
            task.cancel()

        with contextlib.suppress(Exception):
            await asyncio.gather(
                server1_task, server2_new_task, server3_task, return_exceptions=True
            )

        port_manager.cleanup()


if __name__ == "__main__":
    asyncio.run(debug_gossip_blocking())
