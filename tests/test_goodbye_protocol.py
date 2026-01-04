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
import contextlib
import time
import uuid

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason, RPCServerGoodbye
from mpreg.fabric.catalog import NodeDescriptor
from mpreg.fabric.catalog_delta import RoutingCatalogDelta
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager


def _peer_node_ids(server: MPREGServer) -> set[str]:
    directory = server._peer_directory
    if not directory:
        return set()
    return {node.node_id for node in directory.nodes()}


def _peer_count(server: MPREGServer) -> int:
    return len(_peer_node_ids(server))


class TestGoodbyeProtocol:
    """Test suite for GOODBYE protocol functionality."""

    @pytest.mark.asyncio
    async def test_goodbye_message_creation(self):
        """Test creating GOODBYE messages with proper fields."""
        from mpreg.core.model import GoodbyeReason, RPCServerGoodbye

        with TestPortManager() as port_manager:
            departing_url = port_manager.get_server_url()
            goodbye = RPCServerGoodbye(
                departing_node_url=departing_url,
                cluster_id="test-cluster",
                reason=GoodbyeReason.GRACEFUL_SHUTDOWN,
            )

            assert goodbye.what == "GOODBYE"
            assert goodbye.departing_node_url == departing_url
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
            assert f"ws://127.0.0.1:{server2_port}" in _peer_node_ids(server1)
            assert _peer_count(server1) == 2  # server1 + server2

            # Send GOODBYE from server2
            await server2.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

            # Wait for GOODBYE to propagate
            await asyncio.sleep(2.0)

            # Verify server1 removed server2 from cluster
            assert f"ws://127.0.0.1:{server2_port}" not in _peer_node_ids(server1)
            assert _peer_count(server1) == 1  # server1 should still have itself

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
            assert _peer_count(hub) == 3  # hub knows itself, node1 and node2
            assert _peer_count(node1) >= 2  # node1 knows at least itself and hub
            assert _peer_count(node2) >= 2  # node2 knows at least itself and hub

            # Node1 sends GOODBYE
            await node1.send_goodbye(GoodbyeReason.MAINTENANCE)

            # Wait for propagation
            await asyncio.sleep(1.5)

            # Verify hub removed node1
            assert f"ws://127.0.0.1:{node1_port}" not in _peer_node_ids(hub)
            assert f"ws://127.0.0.1:{node2_port}" in _peer_node_ids(
                hub
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
            assert f"ws://127.0.0.1:{server2_port}" in _peer_node_ids(server1)

            # Shutdown server2 manually (this should send GOODBYE)
            await server2.shutdown_async()

            # Wait for GOODBYE to propagate
            await asyncio.sleep(1.0)

            # Verify server1 removed server2 from cluster
            assert f"ws://127.0.0.1:{server2_port}" not in _peer_node_ids(server1)
            assert _peer_count(server1) == 1  # server1 should still have itself

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
            fake_url = port_manager.get_server_url()
            fake_goodbye = RPCServerGoodbye(
                departing_node_url=fake_url,
                cluster_id="goodbye-unknown-test",
                reason=GoodbyeReason.MANUAL_REMOVAL,
            )

            # Handle the GOODBYE (should not crash)
            await server._handle_goodbye_message(fake_goodbye, fake_url)

            # Verify cluster state unchanged (only server itself)
            assert _peer_count(server) == 1

    @pytest.mark.asyncio
    async def test_catalog_departure_removes_peer(self):
        """Test removing a peer via catalog departure updates."""
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

            fake_url = port_manager.get_server_url()
            missing_url = port_manager.get_server_url()
            now = time.time()
            delta = RoutingCatalogDelta(
                update_id=str(uuid.uuid4()),
                cluster_id="remove-peer-test",
                sent_at=now,
                nodes=(
                    NodeDescriptor(
                        node_id=fake_url,
                        cluster_id="remove-peer-test",
                        resources=frozenset({"test-resource"}),
                        capabilities=frozenset(),
                        advertised_at=now,
                        ttl_seconds=30.0,
                    ),
                ),
            )
            server._fabric_control_plane.applier.apply(delta, now=now)

            # Verify peer is in catalog (plus server itself)
            assert fake_url in _peer_node_ids(server)
            assert _peer_count(server) == 2

            # Remove peer
            server._apply_remote_departure(
                server_url=fake_url, cluster_id="remove-peer-test"
            )

            # Verify removal (server itself should still be there)
            assert fake_url not in _peer_node_ids(server)
            assert _peer_count(server) == 1

            # Try to remove non-existent peer (no-op)
            server._apply_remote_departure(
                server_url=missing_url, cluster_id="remove-peer-test"
            )
            assert _peer_count(server) == 1

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
                assert f"ws://127.0.0.1:{server2_port}" in _peer_node_ids(server1)

                # Send GOODBYE with specific reason
                await server2.send_goodbye(reason)
                await asyncio.sleep(0.5)

                # Verify removal worked
                assert f"ws://127.0.0.1:{server2_port}" not in _peer_node_ids(server1)

                # Clean shutdown
                await server1.shutdown_async()
                await server2.shutdown_async()
                server1_task.cancel()
                server2_task.cancel()

                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        asyncio.gather(
                            server1_task, server2_task, return_exceptions=True
                        ),
                        timeout=2.0,
                    )

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
            initial_peers = _peer_count(server1)
            assert initial_peers > 0

            # Send GOODBYE from server2
            await server2.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

            # Wait for GOODBYE processing
            await asyncio.sleep(1.0)

            # Verify server2 was removed from server1's peer list (server1 itself should remain)
            assert _peer_count(server1) == 1

            # Wait additional time to see if reconnection attempts happen
            await asyncio.sleep(3.0)

            # Verify server2 is still not in peer list (no reconnection, only server1 itself)
            assert _peer_count(server1) == 1
            assert f"ws://127.0.0.1:{server2_port}" not in _peer_node_ids(server1)
