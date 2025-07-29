"""Comprehensive tests for the GOODBYE protocol.

Tests the complete GOODBYE message lifecycle including:
- GOODBYE message creation and sending
- Peer removal from cluster
- Connection cleanup
- Integration with shutdown process
- Live cluster scenarios with multiple nodes

All tests use live MPREG servers with no mocking.
"""

import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason, RPCServerGoodbye
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager


class TestGoodbyeProtocol:
    """Test suite for GOODBYE protocol functionality."""

    @pytest.mark.asyncio
    async def test_goodbye_message_creation(self):
        """Test creating GOODBYE messages with proper fields."""
        from mpreg.core.model import GoodbyeReason, RPCServerGoodbye

        goodbye = RPCServerGoodbye(
            departing_node_url="ws://127.0.0.1:8000",
            cluster_id="test-cluster",
            reason=GoodbyeReason.GRACEFUL_SHUTDOWN,
        )

        assert goodbye.what == "GOODBYE"
        assert goodbye.departing_node_url == "ws://127.0.0.1:8000"
        assert goodbye.cluster_id == "test-cluster"
        assert goodbye.reason == GoodbyeReason.GRACEFUL_SHUTDOWN
        assert isinstance(goodbye.timestamp, float)
        assert goodbye.timestamp <= time.time()

    @pytest.mark.asyncio
    async def test_goodbye_reasons_enum(self):
        """Test all GOODBYE reason enum values."""
        from mpreg.core.model import GoodbyeReason

        # Test all enum values exist
        assert GoodbyeReason.GRACEFUL_SHUTDOWN.value == "graceful_shutdown"
        assert GoodbyeReason.MAINTENANCE.value == "maintenance"
        assert GoodbyeReason.CLUSTER_REBALANCE.value == "cluster_rebalance"
        assert GoodbyeReason.MANUAL_REMOVAL.value == "manual_removal"

    @pytest.mark.asyncio
    async def test_two_node_goodbye_basic(self):
        """Test basic GOODBYE between two nodes."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create two servers
            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()

            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="Server1",
                    cluster_id="goodbye-test",
                    log_level="INFO",
                )
            )

            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2",
                    cluster_id="goodbye-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                )
            )

            ctx.servers.extend([server1, server2])

            # Start servers
            server1_task = asyncio.create_task(server1.server())
            server2_task = asyncio.create_task(server2.server())
            ctx.tasks.extend([server1_task, server2_task])

            # Wait for cluster formation
            await asyncio.sleep(2.0)

            # Verify cluster is formed (server1 should know about server2)
            assert f"ws://127.0.0.1:{server2_port}" in server1.cluster.peers_info
            assert len(server1.cluster.peers_info) == 2  # server1 + server2

            # Send GOODBYE from server2
            await server2.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

            # Wait for GOODBYE to propagate
            await asyncio.sleep(2.0)

            # Verify server1 removed server2 from cluster
            assert f"ws://127.0.0.1:{server2_port}" not in server1.cluster.peers_info
            assert (
                len(server1.cluster.peers_info) == 1
            )  # server1 should still have itself

            # Verify server1 closed connection to server2
            assert f"ws://127.0.0.1:{server2_port}" not in server1.peer_connections

    @pytest.mark.asyncio
    async def test_three_node_goodbye_propagation(self):
        """Test GOODBYE propagation in a 3-node cluster."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create three servers in star topology
            hub_port = port_manager.get_server_port()
            node1_port = port_manager.get_server_port()
            node2_port = port_manager.get_server_port()

            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="Hub",
                    cluster_id="goodbye-test-3node",
                    log_level="INFO",
                )
            )

            node1 = MPREGServer(
                MPREGSettings(
                    port=node1_port,
                    name="Node1",
                    cluster_id="goodbye-test-3node",
                    peers=[f"ws://127.0.0.1:{hub_port}"],
                    log_level="INFO",
                )
            )

            node2 = MPREGServer(
                MPREGSettings(
                    port=node2_port,
                    name="Node2",
                    cluster_id="goodbye-test-3node",
                    peers=[f"ws://127.0.0.1:{hub_port}"],
                    log_level="INFO",
                )
            )

            ctx.servers.extend([hub, node1, node2])

            # Start servers
            hub_task = asyncio.create_task(hub.server())
            node1_task = asyncio.create_task(node1.server())
            node2_task = asyncio.create_task(node2.server())
            ctx.tasks.extend([hub_task, node1_task, node2_task])

            # Wait for cluster formation
            await asyncio.sleep(3.0)

            # Verify all nodes know each other (including themselves)
            assert len(hub.cluster.peers_info) == 3  # hub knows itself, node1 and node2
            assert (
                len(node1.cluster.peers_info) >= 2
            )  # node1 knows at least itself and hub
            assert (
                len(node2.cluster.peers_info) >= 2
            )  # node2 knows at least itself and hub

            # Node1 sends GOODBYE
            await node1.send_goodbye(GoodbyeReason.MAINTENANCE)

            # Wait for propagation
            await asyncio.sleep(1.5)

            # Verify hub removed node1
            assert f"ws://127.0.0.1:{node1_port}" not in hub.cluster.peers_info
            assert (
                f"ws://127.0.0.1:{node2_port}" in hub.cluster.peers_info
            )  # node2 still there

            # Verify hub closed connection to node1
            assert f"ws://127.0.0.1:{node1_port}" not in hub.peer_connections

    @pytest.mark.asyncio
    async def test_goodbye_during_shutdown(self):
        """Test GOODBYE is automatically sent during shutdown_async."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create two servers
            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()

            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="Server1",
                    cluster_id="goodbye-shutdown-test",
                    log_level="INFO",
                )
            )

            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2",
                    cluster_id="goodbye-shutdown-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                )
            )

            # Only add server1 to context for automatic cleanup
            # We'll manually shutdown server2 to test GOODBYE
            ctx.servers.append(server1)

            # Start servers
            server1_task = asyncio.create_task(server1.server())
            server2_task = asyncio.create_task(server2.server())
            ctx.tasks.extend([server1_task, server2_task])

            # Wait for cluster formation
            await asyncio.sleep(2.0)

            # Verify cluster is formed
            assert f"ws://127.0.0.1:{server2_port}" in server1.cluster.peers_info

            # Shutdown server2 manually (this should send GOODBYE)
            await server2.shutdown_async()

            # Wait for GOODBYE to propagate
            await asyncio.sleep(1.0)

            # Verify server1 removed server2 from cluster
            assert f"ws://127.0.0.1:{server2_port}" not in server1.cluster.peers_info
            assert (
                len(server1.cluster.peers_info) == 1
            )  # server1 should still have itself

    @pytest.mark.asyncio
    async def test_goodbye_unknown_peer(self):
        """Test handling GOODBYE from unknown peer."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            server_port = port_manager.get_server_port()
            server = MPREGServer(
                MPREGSettings(
                    port=server_port,
                    name="TestServer",
                    cluster_id="goodbye-unknown-test",
                    log_level="INFO",
                )
            )

            ctx.servers.append(server)

            # Start server
            server_task = asyncio.create_task(server.server())
            ctx.tasks.append(server_task)
            await asyncio.sleep(1.0)

            # Create GOODBYE from unknown peer
            fake_goodbye = RPCServerGoodbye(
                departing_node_url="ws://127.0.0.1:99999",
                cluster_id="goodbye-unknown-test",
                reason=GoodbyeReason.MANUAL_REMOVAL,
            )

            # Handle the GOODBYE (should not crash)
            await server._handle_goodbye_message(fake_goodbye, "ws://127.0.0.1:99999")

            # Verify cluster state unchanged (only server itself)
            assert len(server.cluster.peers_info) == 1

    @pytest.mark.asyncio
    async def test_cluster_remove_peer_functionality(self):
        """Test the cluster remove_peer method directly."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            server_port = port_manager.get_server_port()
            server = MPREGServer(
                MPREGSettings(
                    port=server_port,
                    name="TestServer",
                    cluster_id="remove-peer-test",
                    log_level="INFO",
                )
            )

            ctx.servers.append(server)

            # Start server
            server_task = asyncio.create_task(server.server())
            ctx.tasks.append(server_task)
            await asyncio.sleep(1.0)

            # Manually add a peer to cluster
            from mpreg.core.model import PeerInfo

            fake_peer = PeerInfo(
                url="ws://127.0.0.1:99999",
                funs=("test_func",),
                locs=frozenset({"test-resource"}),
                last_seen=time.time(),
                cluster_id="remove-peer-test",
            )
            server.cluster.peers_info["ws://127.0.0.1:99999"] = fake_peer

            # Verify peer is in cluster (plus server itself)
            assert "ws://127.0.0.1:99999" in server.cluster.peers_info
            assert len(server.cluster.peers_info) == 2

            # Remove peer
            removed = await server.cluster.remove_peer("ws://127.0.0.1:99999")

            # Verify removal (server itself should still be there)
            assert removed is True
            assert "ws://127.0.0.1:99999" not in server.cluster.peers_info
            assert len(server.cluster.peers_info) == 1

            # Try to remove non-existent peer
            removed_again = await server.cluster.remove_peer("ws://127.0.0.1:88888")
            assert removed_again is False

    @pytest.mark.asyncio
    async def test_goodbye_all_reasons(self):
        """Test GOODBYE with all different reason types."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            reasons_to_test = [
                GoodbyeReason.GRACEFUL_SHUTDOWN,
                GoodbyeReason.MAINTENANCE,
                GoodbyeReason.CLUSTER_REBALANCE,
                GoodbyeReason.MANUAL_REMOVAL,
            ]

            for reason in reasons_to_test:
                # Create fresh servers for each test
                server1_port = port_manager.get_server_port()
                server2_port = port_manager.get_server_port()

                server1 = MPREGServer(
                    MPREGSettings(
                        port=server1_port,
                        name=f"Server1_{reason.value}",
                        cluster_id=f"goodbye-reason-{reason.value}",
                        log_level="INFO",
                    )
                )

                server2 = MPREGServer(
                    MPREGSettings(
                        port=server2_port,
                        name=f"Server2_{reason.value}",
                        cluster_id=f"goodbye-reason-{reason.value}",
                        peers=[f"ws://127.0.0.1:{server1_port}"],
                        log_level="INFO",
                    )
                )

                # Start servers
                server1_task = asyncio.create_task(server1.server())
                server2_task = asyncio.create_task(server2.server())

                # Wait for cluster formation
                await asyncio.sleep(1.5)

                # Verify cluster formation
                assert f"ws://127.0.0.1:{server2_port}" in server1.cluster.peers_info

                # Send GOODBYE with specific reason
                await server2.send_goodbye(reason)
                await asyncio.sleep(0.5)

                # Verify removal worked
                assert (
                    f"ws://127.0.0.1:{server2_port}" not in server1.cluster.peers_info
                )

                # Clean shutdown
                await server1.shutdown_async()
                await server2.shutdown_async()
                server1_task.cancel()
                server2_task.cancel()

                try:
                    await asyncio.wait_for(
                        asyncio.gather(
                            server1_task, server2_task, return_exceptions=True
                        ),
                        timeout=2.0,
                    )
                except TimeoutError:
                    pass

    @pytest.mark.asyncio
    async def test_goodbye_prevents_reconnection_attempts(self):
        """Test that GOODBYE prevents further reconnection attempts."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create two servers
            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()

            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="Server1",
                    cluster_id="goodbye-reconnect-test",
                    log_level="INFO",
                )
            )

            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2",
                    cluster_id="goodbye-reconnect-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                )
            )

            ctx.servers.extend([server1, server2])

            # Start servers
            server1_task = asyncio.create_task(server1.server())
            server2_task = asyncio.create_task(server2.server())
            ctx.tasks.extend([server1_task, server2_task])

            # Wait for cluster formation
            await asyncio.sleep(2.0)

            # Verify cluster is formed
            initial_peers = len(server1.cluster.peers_info)
            assert initial_peers > 0

            # Send GOODBYE from server2
            await server2.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

            # Wait for GOODBYE processing
            await asyncio.sleep(1.0)

            # Verify server2 was removed from server1's peer list (server1 itself should remain)
            assert len(server1.cluster.peers_info) == 1

            # Wait additional time to see if reconnection attempts happen
            await asyncio.sleep(3.0)

            # Verify server2 is still not in peer list (no reconnection, only server1 itself)
            assert len(server1.cluster.peers_info) == 1
            assert f"ws://127.0.0.1:{server2_port}" not in server1.cluster.peers_info
