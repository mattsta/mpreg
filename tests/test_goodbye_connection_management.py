"""Test GOODBYE protocol integration with connection management.

This test verifies that:
1. Connection management respects the GOODBYE protocol
2. Departed peers are not automatically reconnected to
3. Connection retry storms are prevented
4. The departed peers blocklist works correctly
"""

import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager


class TestGoodbyeConnectionManagement:
    """Test suite for GOODBYE protocol connection management integration."""

    @pytest.mark.asyncio
    async def test_connection_manager_respects_goodbye(self):
        """Test that connection manager doesn't reconnect to departed peers."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create two servers
            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()

            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="Server1",
                    cluster_id="goodbye-connection-test",
                    log_level="INFO",
                    gossip_interval=1.0,  # Faster gossip for testing
                )
            )

            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2",
                    cluster_id="goodbye-connection-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                    gossip_interval=1.0,  # Faster gossip for testing
                )
            )

            ctx.servers.extend([server1, server2])

            # Start servers
            server1_task = asyncio.create_task(server1.server())
            server2_task = asyncio.create_task(server2.server())
            ctx.tasks.extend([server1_task, server2_task])

            # Wait for cluster formation and connection establishment
            await asyncio.sleep(3.0)

            # Verify initial connections are established
            server2_url = f"ws://127.0.0.1:{server2_port}"
            assert server2_url in server1.cluster.peers_info
            assert server2_url in server1.peer_connections
            assert server1.peer_connections[server2_url].is_connected

            # Server2 sends GOODBYE
            await server2.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

            # Wait for GOODBYE processing
            await asyncio.sleep(1.0)

            # Verify server1 removed server2 and added to blocklist
            assert server2_url not in server1.cluster.peers_info
            assert server2_url in server1.cluster._departed_peers

            # Wait for multiple connection management cycles (gossip_interval = 1.0)
            # This would normally trigger reconnection attempts, but shouldn't due to GOODBYE
            await asyncio.sleep(4.0)

            # Verify server2 is still blocked and no reconnection occurred
            assert server2_url in server1.cluster._departed_peers
            assert server2_url not in server1.cluster.peers_info
            # Connection might still exist but should not be re-established if dropped

            # Verify logging would show skipped connections (can't easily test logs directly)
            # The key is that no new peer_info entry was created
            assert (
                len(
                    [
                        url
                        for url in server1.cluster.peers_info.keys()
                        if url == server2_url
                    ]
                )
                == 0
            )

    @pytest.mark.asyncio
    async def test_manual_departed_peer_blocking(self):
        """Test that manually marked departed peers are blocked from connection."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create a server
            server_port = port_manager.get_server_port()
            server = MPREGServer(
                MPREGSettings(
                    port=server_port,
                    name="TestServer",
                    cluster_id="goodbye-manual-test",
                    log_level="INFO",
                    gossip_interval=1.0,  # Faster testing
                )
            )

            ctx.servers.append(server)
            server_task = asyncio.create_task(server.server())
            ctx.tasks.append(server_task)
            await asyncio.sleep(1.0)

            # Manually add a peer to departed list (simulating GOODBYE received)
            departed_peer_url = "ws://127.0.0.1:99999"
            server.cluster._departed_peers[departed_peer_url] = time.time()

            # Also add the peer to peers_info to simulate it was there before
            from mpreg.core.model import PeerInfo

            fake_peer = PeerInfo(
                url=departed_peer_url,
                funs=("test_func",),
                locs=frozenset({"test-resource"}),
                last_seen=time.time(),
                cluster_id="goodbye-manual-test",
            )
            server.cluster.peers_info[departed_peer_url] = fake_peer

            # Wait for connection management cycle
            await asyncio.sleep(2.0)

            # Verify departed peer is NOT in connections (blocked by GOODBYE protocol)
            assert departed_peer_url not in server.peer_connections
            assert departed_peer_url in server.cluster._departed_peers

            # Verify the connection manager skipped this peer
            # (We can't easily verify logs, but the lack of connection is proof)

    @pytest.mark.asyncio
    async def test_departed_peers_cleanup_after_timeout(self):
        """Test that departed peers are eventually cleaned up from blocklist."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            server_port = port_manager.get_server_port()
            server = MPREGServer(
                MPREGSettings(
                    port=server_port,
                    name="TestServer",
                    cluster_id="goodbye-cleanup-test",
                    log_level="INFO",
                )
            )

            ctx.servers.append(server)
            server_task = asyncio.create_task(server.server())
            ctx.tasks.append(server_task)
            await asyncio.sleep(1.0)

            # Manually add a departed peer with old timestamp
            old_timestamp = (
                time.time() - 400.0
            )  # 400 seconds ago (past 5-minute timeout)
            server.cluster._departed_peers["ws://127.0.0.1:99999"] = old_timestamp

            # Also add a recent one that shouldn't be cleaned up
            recent_timestamp = (
                time.time() - 100.0
            )  # 100 seconds ago (within 5-minute timeout)
            server.cluster._departed_peers["ws://127.0.0.1:88888"] = recent_timestamp

            # Verify both are initially present
            assert "ws://127.0.0.1:99999" in server.cluster._departed_peers
            assert "ws://127.0.0.1:88888" in server.cluster._departed_peers

            # Trigger cleanup by calling _prune_dead_peers directly
            server.cluster._prune_dead_peers()

            # Verify old one was cleaned up but recent one remains
            assert "ws://127.0.0.1:99999" not in server.cluster._departed_peers
            assert "ws://127.0.0.1:88888" in server.cluster._departed_peers
