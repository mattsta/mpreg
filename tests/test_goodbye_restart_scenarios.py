"""Test specific restart scenarios to verify GOODBYE vs HELLO behavior.

This test validates the critical distinction:
- GOODBYE blocklist prevents OTHER NODES from gossiping departed peer back
- GOODBYE blocklist does NOT prevent the DEPARTED PEER itself from reconnecting with new HELLO
- A restarted node can immediately rejoin even if it's in "GOODBYE timeout jail"
"""

import asyncio

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager


class TestGoodbyeRestartScenarios:
    """Test restart scenarios with GOODBYE protocol."""

    @pytest.mark.asyncio
    async def test_restart_node_immediately_after_goodbye(self):
        """Test that a node can restart immediately after sending GOODBYE."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()

            # Server1: Stable node
            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="StableNode",
                    cluster_id="restart-test",
                    log_level="INFO",
                )
            )

            # Server2: Will restart
            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="RestartingNode",
                    cluster_id="restart-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                )
            )

            ctx.servers.extend([server1, server2])

            server1_task = asyncio.create_task(server1.server())
            server2_task = asyncio.create_task(server2.server())
            ctx.tasks.extend([server1_task, server2_task])

            # Wait for initial cluster formation
            await asyncio.sleep(2.0)

            server2_url = f"ws://127.0.0.1:{server2_port}"

            # Verify initial cluster
            assert server2_url in server1.cluster.peers_info
            assert server2_url not in server1.cluster._departed_peers

            print("✅ Initial cluster formed")

            # Server2 sends GOODBYE and immediately shuts down
            await server2.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)
            await asyncio.sleep(0.5)  # Brief pause for GOODBYE processing

            # Verify GOODBYE processed
            assert server2_url not in server1.cluster.peers_info
            assert server2_url in server1.cluster._departed_peers

            print("✅ GOODBYE processed - node departed and blocklisted")

            # Shutdown server2
            await server2.shutdown_async()
            server2_task.cancel()

            # CRITICAL TEST: Restart server2 IMMEDIATELY (while still in blocklist)
            # This simulates a fast restart scenario
            await asyncio.sleep(0.1)  # Very short delay

            # Verify server2 is still in departed list on server1
            assert server2_url in server1.cluster._departed_peers

            print("🚀 Restarting node immediately while still in blocklist...")

            # Create new server2 instance
            server2_restarted = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="RestartedNode",
                    cluster_id="restart-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                )
            )

            ctx.servers.append(server2_restarted)
            server2_restart_task = asyncio.create_task(server2_restarted.server())
            ctx.tasks.append(server2_restart_task)

            # Wait for reconnection
            await asyncio.sleep(2.0)

            # VERIFY: Despite being in blocklist, new HELLO should allow immediate re-entry
            assert server2_url in server1.cluster.peers_info  # Back in cluster
            assert (
                server2_url not in server1.cluster._departed_peers
            )  # Removed from blocklist

            print(
                "✅ Node successfully restarted and rejoined despite previous GOODBYE"
            )
            print(
                f"   - Node back in cluster: {server2_url in server1.cluster.peers_info}"
            )
            print(
                f"   - Blocklist cleared: {server2_url not in server1.cluster._departed_peers}"
            )

    @pytest.mark.asyncio
    async def test_gossip_vs_direct_hello_distinction(self):
        """Test that gossip is blocked but direct HELLO is allowed."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create 3-node linear chain: Node1 <-> Node2 <-> Node3
            # This tests gossip propagation vs direct connections
            node1_port = port_manager.get_server_port()
            node2_port = port_manager.get_server_port()
            node3_port = port_manager.get_server_port()

            node1 = MPREGServer(
                MPREGSettings(
                    port=node1_port,
                    name="Node1",
                    cluster_id="gossip-hello-test",
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )

            node2 = MPREGServer(
                MPREGSettings(
                    port=node2_port,
                    name="Node2",
                    cluster_id="gossip-hello-test",
                    peers=[f"ws://127.0.0.1:{node1_port}"],
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )

            node3 = MPREGServer(
                MPREGSettings(
                    port=node3_port,
                    name="Node3",
                    cluster_id="gossip-hello-test",
                    peers=[f"ws://127.0.0.1:{node2_port}"],
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )

            ctx.servers.extend([node1, node2, node3])

            tasks = [
                asyncio.create_task(node.server()) for node in [node1, node2, node3]
            ]
            ctx.tasks.extend(tasks)

            # Wait for full mesh formation via gossip
            await asyncio.sleep(3.0)

            node2_url = f"ws://127.0.0.1:{node2_port}"

            # Verify all nodes know each other
            assert node2_url in node1.cluster.peers_info
            assert node2_url in node3.cluster.peers_info

            print("✅ Full mesh formed via gossip")

            # Node2 sends GOODBYE
            await node2.send_goodbye(GoodbyeReason.MAINTENANCE)
            await asyncio.sleep(1.0)

            # Verify Node2 removed from all peers
            assert node2_url not in node1.cluster.peers_info
            assert node2_url not in node3.cluster.peers_info
            assert node2_url in node1.cluster._departed_peers
            assert node2_url in node3.cluster._departed_peers

            print("✅ Node2 departed and blocklisted on all peers")

            # Shutdown Node2
            await node2.shutdown_async()
            tasks[1].cancel()
            await asyncio.sleep(0.5)

            # Start Node2 again - it should be able to connect directly to Node1
            # even though Node1 has Node2 in its departed peers list
            node2_restarted = MPREGServer(
                MPREGSettings(
                    port=node2_port,
                    name="Node2_Restarted",
                    cluster_id="gossip-hello-test",
                    peers=[
                        f"ws://127.0.0.1:{node1_port}"
                    ],  # Direct connection to Node1
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )

            ctx.servers.append(node2_restarted)
            node2_restart_task = asyncio.create_task(node2_restarted.server())
            ctx.tasks.append(node2_restart_task)

            # Wait for direct connection
            await asyncio.sleep(1.5)

            # VERIFY: Node2 should be able to connect directly to Node1 via HELLO
            assert node2_url in node1.cluster.peers_info  # Direct HELLO worked
            assert node2_url not in node1.cluster._departed_peers  # Cleared by HELLO

            # BUT: Node3 should still have Node2 in departed list until gossip timeout
            # or direct connection (depending on gossip timing)
            initial_node3_departed = node2_url in node3.cluster._departed_peers

            print("✅ Direct HELLO bypass worked:")
            print(
                f"   - Node1 accepted Node2 via direct HELLO: {node2_url in node1.cluster.peers_info}"
            )
            print(
                f"   - Node1 cleared blocklist: {node2_url not in node1.cluster._departed_peers}"
            )
            print(f"   - Node3 still has Node2 blocklisted: {initial_node3_departed}")

            # Wait for gossip propagation
            await asyncio.sleep(2.0)

            # Now Node3 should also learn about Node2 (either via gossip or direct connection)
            print(
                f"   - After gossip: Node3 knows Node2: {node2_url in node3.cluster.peers_info}"
            )

    @pytest.mark.asyncio
    async def test_multiple_rapid_restarts(self):
        """Test multiple rapid restart cycles to ensure robustness."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            stable_port = port_manager.get_server_port()
            cycling_port = port_manager.get_server_port()

            # Stable node
            stable_node = MPREGServer(
                MPREGSettings(
                    port=stable_port,
                    name="StableNode",
                    cluster_id="rapid-restart-test",
                    log_level="INFO",
                )
            )

            ctx.servers.append(stable_node)
            stable_task = asyncio.create_task(stable_node.server())
            ctx.tasks.append(stable_task)

            await asyncio.sleep(1.0)

            cycling_url = f"ws://127.0.0.1:{cycling_port}"

            # Perform 3 rapid restart cycles
            for cycle in range(3):
                print(f"🔄 Rapid restart cycle {cycle + 1}/3")

                # Create cycling node
                cycling_node = MPREGServer(
                    MPREGSettings(
                        port=cycling_port,
                        name=f"CyclingNode_{cycle}",
                        cluster_id="rapid-restart-test",
                        peers=[f"ws://127.0.0.1:{stable_port}"],
                        log_level="INFO",
                    )
                )

                cycling_task = asyncio.create_task(cycling_node.server())

                # Wait for join
                await asyncio.sleep(1.0)

                # Verify joined
                assert cycling_url in stable_node.cluster.peers_info
                assert cycling_url not in stable_node.cluster._departed_peers

                # Send GOODBYE and shutdown immediately
                await cycling_node.send_goodbye(GoodbyeReason.CLUSTER_REBALANCE)
                await asyncio.sleep(0.3)  # Brief processing time

                # Verify GOODBYE processed
                assert cycling_url not in stable_node.cluster.peers_info
                assert cycling_url in stable_node.cluster._departed_peers

                # Shutdown
                await cycling_node.shutdown_async()
                cycling_task.cancel()

                # Very brief pause before next cycle
                await asyncio.sleep(0.2)

                print(f"   ✅ Cycle {cycle + 1} GOODBYE completed")

            # Final verification: Try one more restart to ensure system is still functional
            final_node = MPREGServer(
                MPREGSettings(
                    port=cycling_port,
                    name="FinalNode",
                    cluster_id="rapid-restart-test",
                    peers=[f"ws://127.0.0.1:{stable_port}"],
                    log_level="INFO",
                )
            )

            ctx.servers.append(final_node)
            final_task = asyncio.create_task(final_node.server())
            ctx.tasks.append(final_task)

            await asyncio.sleep(1.5)

            # Should still work after all the cycling
            assert cycling_url in stable_node.cluster.peers_info
            assert cycling_url not in stable_node.cluster._departed_peers

            print("✅ System remains functional after multiple rapid restart cycles")

    @pytest.mark.asyncio
    async def test_restart_during_gossip_propagation(self):
        """Test restart timing during gossip propagation periods."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create 4 nodes in star formation for complex gossip timing
            hub_port = port_manager.get_server_port()
            spoke_ports = [port_manager.get_server_port() for _ in range(3)]

            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="Hub",
                    cluster_id="gossip-timing-test",
                    log_level="INFO",
                    gossip_interval=0.3,  # Fast gossip
                )
            )

            spokes = []
            for i, port in enumerate(spoke_ports):
                spoke = MPREGServer(
                    MPREGSettings(
                        port=port,
                        name=f"Spoke{i + 1}",
                        cluster_id="gossip-timing-test",
                        peers=[f"ws://127.0.0.1:{hub_port}"],
                        log_level="INFO",
                        gossip_interval=0.3,
                    )
                )
                spokes.append(spoke)

            ctx.servers.extend([hub] + spokes)

            tasks = [asyncio.create_task(node.server()) for node in [hub] + spokes]
            ctx.tasks.extend(tasks)

            # Wait for full cluster formation
            await asyncio.sleep(2.0)

            spoke1_url = f"ws://127.0.0.1:{spoke_ports[0]}"

            # Verify cluster formed
            assert spoke1_url in hub.cluster.peers_info
            for spoke in spokes[1:]:  # Other spokes should know about spoke1 via gossip
                assert spoke1_url in spoke.cluster.peers_info

            print("✅ Star cluster formed with gossip propagation")

            # Spoke1 sends GOODBYE
            await spokes[0].send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

            # Shutdown spoke1 DURING gossip propagation (before all nodes process GOODBYE)
            await asyncio.sleep(0.1)  # Very short time - may not reach all nodes
            await spokes[0].shutdown_async()
            tasks[1].cancel()

            # Wait a bit for partial GOODBYE propagation
            await asyncio.sleep(0.2)

            print("🚀 Restarting spoke1 during gossip propagation...")

            # Restart spoke1 immediately
            spoke1_restarted = MPREGServer(
                MPREGSettings(
                    port=spoke_ports[0],
                    name="Spoke1_Restarted",
                    cluster_id="gossip-timing-test",
                    peers=[f"ws://127.0.0.1:{hub_port}"],
                    log_level="INFO",
                    gossip_interval=0.3,
                )
            )

            ctx.servers.append(spoke1_restarted)
            spoke1_restart_task = asyncio.create_task(spoke1_restarted.server())
            ctx.tasks.append(spoke1_restart_task)

            # Wait for reconnection
            await asyncio.sleep(1.5)

            # Verify spoke1 successfully rejoined despite timing complexity
            assert spoke1_url in hub.cluster.peers_info
            assert spoke1_url not in hub.cluster._departed_peers

            print("✅ Node successfully restarted despite gossip timing complexity")
            print(f"   - Hub knows spoke1: {spoke1_url in hub.cluster.peers_info}")
            print(
                f"   - Hub blocklist cleared: {spoke1_url not in hub.cluster._departed_peers}"
            )
