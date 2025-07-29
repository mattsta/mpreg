"""Comprehensive integration tests for the GOODBYE protocol.

These tests verify the complete lifecycle of the GOODBYE protocol:
1. Gossip protocol adds nodes dynamically
2. GOODBYE removes nodes and prevents gossip re-addition
3. Blocklist management prevents connection retry storms
4. New HELLO messages can re-add previously departed nodes
5. Long-running stability with multiple GOODBYE/HELLO cycles
6. Stress testing with rapid departures and re-entries
"""

import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager


class TestGoodbyeComprehensiveIntegration:
    """Comprehensive integration test suite for GOODBYE protocol."""

    @pytest.mark.asyncio
    async def test_complete_gossip_goodbye_lifecycle(self):
        """Test complete lifecycle: gossip addition -> GOODBYE -> blocked re-addition -> new HELLO."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create 3 servers in linear chain for gossip propagation
            # Server1 <- Server2 <- Server3
            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()
            server3_port = port_manager.get_server_port()

            # Server1: Hub
            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="Server1_Hub",
                    cluster_id="gossip-goodbye-lifecycle",
                    log_level="INFO",
                    gossip_interval=1.0,  # Fast gossip for testing
                )
            )

            # Server2: Connected to Server1
            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2_Relay",
                    cluster_id="gossip-goodbye-lifecycle",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                    gossip_interval=1.0,
                )
            )

            ctx.servers.extend([server1, server2])

            # Start first two servers
            server1_task = asyncio.create_task(server1.server())
            server2_task = asyncio.create_task(server2.server())
            ctx.tasks.extend([server1_task, server2_task])

            # Wait for initial cluster formation
            await asyncio.sleep(3.0)

            # Verify Server1 and Server2 know each other
            server1_url = f"ws://127.0.0.1:{server1_port}"
            server2_url = f"ws://127.0.0.1:{server2_port}"

            assert server2_url in server1.cluster.peers_info
            assert server1_url in server2.cluster.peers_info

            # PHASE 1: Add Server3 via gossip propagation
            # Server3 connects only to Server2, should learn about Server1 via gossip
            server3 = MPREGServer(
                MPREGSettings(
                    port=server3_port,
                    name="Server3_Leaf",
                    cluster_id="gossip-goodbye-lifecycle",
                    peers=[f"ws://127.0.0.1:{server2_port}"],
                    log_level="INFO",
                    gossip_interval=1.0,
                )
            )

            ctx.servers.append(server3)
            server3_task = asyncio.create_task(server3.server())
            ctx.tasks.append(server3_task)

            # Wait for gossip propagation
            await asyncio.sleep(4.0)

            server3_url = f"ws://127.0.0.1:{server3_port}"

            # Verify all servers know each other via gossip
            assert server3_url in server1.cluster.peers_info
            assert server3_url in server2.cluster.peers_info
            assert server1_url in server3.cluster.peers_info
            assert server2_url in server3.cluster.peers_info

            print(
                "✅ Phase 1: Gossip propagation complete - all 3 servers know each other"
            )

            # PHASE 2: Server3 sends GOODBYE
            await server3.send_goodbye(GoodbyeReason.MAINTENANCE)
            await asyncio.sleep(2.0)

            # Verify GOODBYE propagated and Server3 removed from all clusters
            assert server3_url not in server1.cluster.peers_info
            assert server3_url not in server2.cluster.peers_info

            # Verify Server3 added to departed peers blocklist
            assert server3_url in server1.cluster._departed_peers
            assert server3_url in server2.cluster._departed_peers

            print("✅ Phase 2: GOODBYE processed - Server3 removed and blocklisted")

            # PHASE 3: Wait for multiple gossip cycles to ensure Server3 doesn't re-appear
            await asyncio.sleep(5.0)

            # Verify Server3 is still blocked and not re-added by gossip
            assert server3_url not in server1.cluster.peers_info
            assert server3_url not in server2.cluster.peers_info
            assert server3_url in server1.cluster._departed_peers
            assert server3_url in server2.cluster._departed_peers

            print(
                "✅ Phase 3: Stability verified - Server3 remains blocked after multiple gossip cycles"
            )

            # PHASE 4: Server3 reconnects with new HELLO (simulating restart)
            # Shutdown current Server3
            await server3.shutdown_async()
            server3_task.cancel()
            await asyncio.sleep(1.0)

            # Clear departed peers to simulate clean restart or timeout
            server1.cluster._departed_peers.clear()
            server2.cluster._departed_peers.clear()

            # Create new Server3 instance (simulating restart)
            server3_new = MPREGServer(
                MPREGSettings(
                    port=server3_port,
                    name="Server3_Restarted",
                    cluster_id="gossip-goodbye-lifecycle",
                    peers=[f"ws://127.0.0.1:{server2_port}"],
                    log_level="INFO",
                    gossip_interval=1.0,
                )
            )

            ctx.servers.append(server3_new)
            server3_new_task = asyncio.create_task(server3_new.server())
            ctx.tasks.append(server3_new_task)

            # Wait for re-connection and gossip propagation
            await asyncio.sleep(4.0)

            # Verify Server3 successfully re-joined the cluster
            assert server3_url in server1.cluster.peers_info
            assert server3_url in server2.cluster.peers_info
            assert server1_url in server3_new.cluster.peers_info
            assert server2_url in server3_new.cluster.peers_info

            # Verify departed peers list is clear
            assert server3_url not in server1.cluster._departed_peers
            assert server3_url not in server2.cluster._departed_peers

            print(
                "✅ Phase 4: Re-connection successful - Server3 re-joined after departed peers timeout"
            )

    @pytest.mark.asyncio
    async def test_multiple_goodbye_hello_cycles(self):
        """Test stability with multiple GOODBYE/HELLO cycles from the same node."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create a 2-server cluster
            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()

            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="Server1_Stable",
                    cluster_id="multi-cycle-test",
                    log_level="INFO",
                    gossip_interval=0.5,  # Very fast gossip
                )
            )

            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2_Cycling",
                    cluster_id="multi-cycle-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )

            ctx.servers.extend([server1, server2])

            server1_task = asyncio.create_task(server1.server())
            server2_task = asyncio.create_task(server2.server())
            ctx.tasks.extend([server1_task, server2_task])

            await asyncio.sleep(2.0)

            server2_url = f"ws://127.0.0.1:{server2_port}"

            # Verify initial connection
            assert server2_url in server1.cluster.peers_info

            # Perform 3 GOODBYE/HELLO cycles
            for cycle in range(3):
                print(f"🔄 Starting GOODBYE/HELLO cycle {cycle + 1}")

                # GOODBYE phase
                await server2.send_goodbye(GoodbyeReason.CLUSTER_REBALANCE)
                await asyncio.sleep(1.0)

                # Verify removal and blocklisting
                assert server2_url not in server1.cluster.peers_info
                assert server2_url in server1.cluster._departed_peers

                # Simulate timeout by clearing departed peers
                server1.cluster._departed_peers.clear()
                await asyncio.sleep(0.5)

                # Re-establish connection (simulating HELLO)
                # Server2 should reconnect via its initial peer configuration
                await asyncio.sleep(1.5)  # Let connection manager retry

                # Verify re-connection
                assert server2_url in server1.cluster.peers_info
                assert server2_url not in server1.cluster._departed_peers

                print(f"✅ Cycle {cycle + 1} completed successfully")

            # Final stability check
            await asyncio.sleep(2.0)
            assert server2_url in server1.cluster.peers_info
            assert len(server1.cluster._departed_peers) == 0

    @pytest.mark.asyncio
    async def test_stress_rapid_departures_and_reconnections(self):
        """Stress test with rapid departures and reconnections from multiple nodes."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create 5-server cluster with hub topology
            hub_port = port_manager.get_server_port()
            node_ports = [port_manager.get_server_port() for _ in range(4)]

            # Hub server
            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="Hub",
                    cluster_id="stress-test-cluster",
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )

            # 4 node servers, all connected to hub
            nodes = []
            for i, port in enumerate(node_ports):
                node = MPREGServer(
                    MPREGSettings(
                        port=port,
                        name=f"Node{i + 1}",
                        cluster_id="stress-test-cluster",
                        peers=[f"ws://127.0.0.1:{hub_port}"],
                        log_level="INFO",
                        gossip_interval=0.5,
                    )
                )
                nodes.append(node)

            ctx.servers.extend([hub] + nodes)

            # Start all servers
            hub_task = asyncio.create_task(hub.server())
            node_tasks = [asyncio.create_task(node.server()) for node in nodes]
            ctx.tasks.extend([hub_task] + node_tasks)

            # Wait for cluster formation
            await asyncio.sleep(3.0)

            # Verify all nodes connected to hub
            for port in node_ports:
                node_url = f"ws://127.0.0.1:{port}"
                assert node_url in hub.cluster.peers_info

            print(f"✅ Initial cluster formed: Hub + {len(nodes)} nodes")

            # STRESS PHASE: Rapid departures
            departure_reasons = [
                GoodbyeReason.GRACEFUL_SHUTDOWN,
                GoodbyeReason.MAINTENANCE,
                GoodbyeReason.CLUSTER_REBALANCE,
                GoodbyeReason.MANUAL_REMOVAL,
            ]

            # Send GOODBYE from all nodes rapidly
            for i, node in enumerate(nodes):
                reason = departure_reasons[i % len(departure_reasons)]
                await node.send_goodbye(reason)
                await asyncio.sleep(0.2)  # Very short delay between departures

            # Wait for processing
            await asyncio.sleep(2.0)

            # Verify all nodes departed and blocklisted
            for port in node_ports:
                node_url = f"ws://127.0.0.1:{port}"
                assert node_url not in hub.cluster.peers_info
                assert node_url in hub.cluster._departed_peers

            print(f"✅ Rapid departures processed: All {len(nodes)} nodes departed")

            # Verify hub stability
            assert len(hub.cluster.peers_info) == 1  # Only hub itself
            assert len(hub.cluster._departed_peers) == len(nodes)

            # Clear departed peers (simulating timeout)
            hub.cluster._departed_peers.clear()

            # Let connection management attempt reconnections
            await asyncio.sleep(3.0)

            # Verify nodes can reconnect
            reconnected_count = len(
                [
                    url
                    for url in hub.cluster.peers_info.keys()
                    if url != f"ws://127.0.0.1:{hub_port}"
                ]
            )

            # At least some nodes should have reconnected
            assert reconnected_count > 0

            print(
                f"✅ Stress test completed: {reconnected_count}/{len(nodes)} nodes reconnected"
            )

    @pytest.mark.asyncio
    async def test_gossip_stability_with_departed_peers(self):
        """Test that gossip remains stable and doesn't propagate departed peer information."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create 4-server mesh for complex gossip interactions
            ports = [port_manager.get_server_port() for _ in range(4)]

            # Server1: No initial peers (will be discovered via gossip)
            server1 = MPREGServer(
                MPREGSettings(
                    port=ports[0],
                    name="Server1",
                    cluster_id="gossip-stability-test",
                    log_level="INFO",
                    gossip_interval=0.8,
                )
            )

            # Server2: Connects to Server1
            server2 = MPREGServer(
                MPREGSettings(
                    port=ports[1],
                    name="Server2",
                    cluster_id="gossip-stability-test",
                    peers=[f"ws://127.0.0.1:{ports[0]}"],
                    log_level="INFO",
                    gossip_interval=0.8,
                )
            )

            # Server3: Connects to Server2
            server3 = MPREGServer(
                MPREGSettings(
                    port=ports[2],
                    name="Server3",
                    cluster_id="gossip-stability-test",
                    peers=[f"ws://127.0.0.1:{ports[1]}"],
                    log_level="INFO",
                    gossip_interval=0.8,
                )
            )

            # Server4: Connects to Server3
            server4 = MPREGServer(
                MPREGSettings(
                    port=ports[3],
                    name="Server4",
                    cluster_id="gossip-stability-test",
                    peers=[f"ws://127.0.0.1:{ports[2]}"],
                    log_level="INFO",
                    gossip_interval=0.8,
                )
            )

            servers = [server1, server2, server3, server4]
            ctx.servers.extend(servers)

            # Start all servers
            tasks = [asyncio.create_task(server.server()) for server in servers]
            ctx.tasks.extend(tasks)

            # Wait for full mesh formation via gossip
            await asyncio.sleep(5.0)

            # Verify full mesh formed
            urls = [f"ws://127.0.0.1:{port}" for port in ports]
            for i, server in enumerate(servers):
                other_urls = [url for j, url in enumerate(urls) if j != i]
                for other_url in other_urls:
                    assert other_url in server.cluster.peers_info

            print("✅ Full mesh formed via gossip")

            # Server2 and Server4 send GOODBYE simultaneously
            await asyncio.gather(
                server2.send_goodbye(GoodbyeReason.MAINTENANCE),
                server4.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN),
            )

            await asyncio.sleep(2.0)

            # Verify departed servers removed from remaining servers
            departed_urls = [urls[1], urls[3]]  # Server2 and Server4
            remaining_servers = [server1, server3]

            for server in remaining_servers:
                for departed_url in departed_urls:
                    assert departed_url not in server.cluster.peers_info
                    assert departed_url in server.cluster._departed_peers

            # Run multiple gossip cycles to ensure stability
            await asyncio.sleep(6.0)  # ~7-8 gossip cycles

            # Verify departed peers stay departed (not re-added by gossip)
            for server in remaining_servers:
                for departed_url in departed_urls:
                    assert departed_url not in server.cluster.peers_info
                    assert departed_url in server.cluster._departed_peers

            # Verify remaining servers still know each other
            assert urls[0] in server3.cluster.peers_info  # Server1 in Server3
            assert urls[2] in server1.cluster.peers_info  # Server3 in Server1

            print(
                "✅ Gossip stability verified: Departed peers stay departed, remaining peers connected"
            )

    @pytest.mark.asyncio
    async def test_departed_peer_cleanup_integration(self):
        """Test that departed peer cleanup integrates properly with the full system."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            server_port = port_manager.get_server_port()
            server = MPREGServer(
                MPREGSettings(
                    port=server_port,
                    name="CleanupTestServer",
                    cluster_id="cleanup-integration-test",
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )

            ctx.servers.append(server)
            server_task = asyncio.create_task(server.server())
            ctx.tasks.append(server_task)

            await asyncio.sleep(1.0)

            # Manually add some departed peers with different timestamps
            old_peer = "ws://127.0.0.1:88888"
            recent_peer = "ws://127.0.0.1:99999"

            # Old peer (should be cleaned up)
            server.cluster._departed_peers[old_peer] = time.time() - 400.0

            # Recent peer (should remain)
            server.cluster._departed_peers[recent_peer] = time.time() - 100.0

            # Current peer that will send GOODBYE
            current_peer = "ws://127.0.0.1:77777"
            server.cluster._departed_peers[current_peer] = time.time()

            # Verify initial state
            assert len(server.cluster._departed_peers) == 3

            # Trigger cleanup via the dead peer pruning mechanism
            # This happens automatically during normal operation
            server.cluster._prune_dead_peers()

            # Verify cleanup occurred correctly
            assert old_peer not in server.cluster._departed_peers  # Cleaned up (old)
            assert recent_peer in server.cluster._departed_peers  # Kept (recent)
            assert current_peer in server.cluster._departed_peers  # Kept (current)

            assert len(server.cluster._departed_peers) == 2

            print("✅ Departed peer cleanup working correctly")

            # Test that cleanup doesn't interfere with new GOODBYE processing
            # Simulate another peer sending GOODBYE
            new_goodbye_peer = "ws://127.0.0.1:66666"

            # Add peer to cluster first
            from mpreg.core.model import PeerInfo

            peer_info = PeerInfo(
                url=new_goodbye_peer,
                funs=("test_func",),
                locs=frozenset({"test-resource"}),
                last_seen=time.time(),
                cluster_id="cleanup-integration-test",
            )
            server.cluster.peers_info[new_goodbye_peer] = peer_info

            # Remove via GOODBYE protocol
            removed = await server.cluster.remove_peer(new_goodbye_peer)
            assert removed is True

            # Verify new departed peer added correctly
            assert new_goodbye_peer in server.cluster._departed_peers
            assert new_goodbye_peer not in server.cluster.peers_info
            assert len(server.cluster._departed_peers) == 3

            print("✅ New GOODBYE processing works alongside cleanup")
