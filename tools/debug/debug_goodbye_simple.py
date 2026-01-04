#!/usr/bin/env python3
"""Simple debug test for GOODBYE protocol."""

import asyncio
import contextlib

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager


async def debug_goodbye_simple():
    """Debug GOODBYE protocol with detailed logging."""
    port_manager = TestPortManager()

    try:
        # Create two servers
        server1_port = port_manager.get_server_port()
        server2_port = port_manager.get_server_port()

        print(f"Server1 port: {server1_port}")
        print(f"Server2 port: {server2_port}")

        server1 = MPREGServer(
            MPREGSettings(
                port=server1_port,
                name="Server1",
                cluster_id="debug-goodbye",
                log_level="DEBUG",
            )
        )

        server2 = MPREGServer(
            MPREGSettings(
                port=server2_port,
                name="Server2",
                cluster_id="debug-goodbye",
                peers=[f"ws://127.0.0.1:{server1_port}"],
                log_level="DEBUG",
            )
        )

        # Start servers
        server1_task = asyncio.create_task(server1.server())
        server2_task = asyncio.create_task(server2.server())

        # Wait for cluster formation
        print("Waiting for cluster formation...")
        await asyncio.sleep(3.0)

        # Check initial state
        print("\nInitial state:")
        print(f"Server1 peers_info: {list(server1.cluster.peers_info.keys())}")
        print(f"Server2 peers_info: {list(server2.cluster.peers_info.keys())}")

        server2_url = f"ws://127.0.0.1:{server2_port}"
        print(f"\nChecking if {server2_url} is in server1.cluster.peers_info:")
        print(f"Result: {server2_url in server1.cluster.peers_info}")

        # Send GOODBYE from server2
        print("\nSending GOODBYE from server2...")
        await server2.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

        # Wait for processing
        print("Waiting for GOODBYE processing...")
        await asyncio.sleep(2.0)

        # Check final state
        print("\nFinal state:")
        print(f"Server1 peers_info: {list(server1.cluster.peers_info.keys())}")
        print(f"Server2 peers_info: {list(server2.cluster.peers_info.keys())}")

        print(f"\nChecking if {server2_url} is in server1.cluster.peers_info:")
        print(f"Result: {server2_url in server1.cluster.peers_info}")

        # Check if peer was removed
        if server2_url not in server1.cluster.peers_info:
            print("✅ GOODBYE protocol worked correctly!")
        else:
            print("❌ GOODBYE protocol failed - peer still in cluster")

    finally:
        # Cleanup
        try:
            await server1.shutdown_async()
            await server2.shutdown_async()
        except Exception as e:
            print(f"Error during cleanup: {e}")

        server1_task.cancel()
        server2_task.cancel()

        with contextlib.suppress(Exception):
            await asyncio.gather(server1_task, server2_task, return_exceptions=True)

        port_manager.cleanup()


if __name__ == "__main__":
    asyncio.run(debug_goodbye_simple())
