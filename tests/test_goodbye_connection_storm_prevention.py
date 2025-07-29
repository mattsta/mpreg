"""Test that GOODBYE protocol prevents connection retry storms.

This test specifically validates the original problem that the GOODBYE protocol
was designed to solve: preventing endless connection retry attempts when nodes
leave a cluster, which caused performance issues in large-scale deployments.
"""

import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager


class TestGoodbyeConnectionStormPrevention:
    """Test that GOODBYE protocol prevents connection retry storms."""

    @pytest.mark.asyncio
    async def test_no_connection_retry_storm_after_goodbye(self):
        """Test that GOODBYE prevents connection retry storms.

        Before GOODBYE protocol: Node departures would result in endless connection
        retry attempts, causing performance degradation and resource waste.

        After GOODBYE protocol: Departed nodes are blocklisted, preventing retry storms.
        """
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create a 5-node cluster to simulate scale scenario
            hub_port = port_manager.get_server_port()
            node_ports = [port_manager.get_server_port() for _ in range(4)]

            # Hub server that other nodes connect to
            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="Hub",
                    cluster_id="storm-prevention-test",
                    log_level="INFO",
                    gossip_interval=0.5,  # Aggressive gossip to test storm potential
                )
            )

            # Create 4 nodes that connect to hub
            nodes = []
            for i, port in enumerate(node_ports):
                node = MPREGServer(
                    MPREGSettings(
                        port=port,
                        name=f"Node{i + 1}",
                        cluster_id="storm-prevention-test",
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

            # Verify full cluster formed
            node_urls = [f"ws://127.0.0.1:{port}" for port in node_ports]
            for node_url in node_urls:
                assert node_url in hub.cluster.peers_info

            print(f"✅ Initial cluster formed: Hub + {len(nodes)} nodes")

            # Track connection state before GOODBYE
            initial_connections = len(hub.peer_connections)
            initial_peer_count = len(hub.cluster.peers_info) - 1  # Exclude hub itself

            # All nodes send GOODBYE rapidly (simulating mass departure)
            print(
                "🚨 Simulating mass departure without proper cleanup (pre-GOODBYE scenario)"
            )

            departure_start_time = time.time()

            for node in nodes:
                await node.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)
                await asyncio.sleep(0.1)  # Rapid departures

            departure_end_time = time.time()
            departure_duration = departure_end_time - departure_start_time

            # Wait for GOODBYE processing
            await asyncio.sleep(2.0)

            # Verify all nodes properly departed via GOODBYE
            for node_url in node_urls:
                assert node_url not in hub.cluster.peers_info
                assert node_url in hub.cluster._departed_peers

            print(f"✅ GOODBYE processing completed in {departure_duration:.2f}s")

            # KEY TEST: Monitor for connection retry storms
            # Run for extended period to see if hub tries to reconnect to departed nodes
            storm_monitoring_duration = 10.0  # 10 seconds of monitoring
            monitoring_start_time = time.time()

            print(
                f"🔍 Monitoring for {storm_monitoring_duration}s to detect connection retry storms..."
            )

            # Track connection attempts during monitoring period
            connection_attempts = 0
            initial_departed_count = len(hub.cluster._departed_peers)

            while time.time() - monitoring_start_time < storm_monitoring_duration:
                await asyncio.sleep(0.5)

                # Check if hub is trying to reconnect to departed peers
                current_connections = len(hub.peer_connections)
                current_departed_count = len(hub.cluster._departed_peers)

                # In a retry storm scenario, we'd see:
                # 1. Connection attempts to departed peers (would fail)
                # 2. Fluctuating connection counts
                # 3. High CPU usage from retry attempts

                # With GOODBYE protocol, we should see:
                # 1. Stable connection count (no retry attempts)
                # 2. Stable departed peers count
                # 3. No connection attempts to blocklisted peers

                # Verify stability (no storm indicators)
                assert current_connections <= 1  # Hub should have 0-1 connections max
                assert (
                    current_departed_count == initial_departed_count
                )  # Stable blocklist

                # Check that departed peers remain blocklisted
                for node_url in node_urls:
                    assert node_url in hub.cluster._departed_peers
                    assert node_url not in hub.cluster.peers_info

            monitoring_end_time = time.time()
            actual_monitoring_duration = monitoring_end_time - monitoring_start_time

            print(
                f"✅ Storm prevention verified: No retry attempts detected during {actual_monitoring_duration:.2f}s monitoring"
            )

            # Final verification: System is stable and efficient
            final_connections = len(hub.peer_connections)
            final_peer_count = len(hub.cluster.peers_info) - 1  # Exclude hub itself
            final_departed_count = len(hub.cluster._departed_peers)

            # Verify proper state
            assert final_connections == 0  # No connections to departed peers
            assert final_peer_count == 0  # No peers (all departed)
            assert final_departed_count == len(nodes)  # All nodes properly blocklisted

            print("✅ Final state verification:")
            print(f"   - Connections: {final_connections} (should be 0)")
            print(f"   - Active peers: {final_peer_count} (should be 0)")
            print(
                f"   - Blocklisted peers: {final_departed_count} (should be {len(nodes)})"
            )
            print("   - No connection retry storms detected ✅")

    @pytest.mark.asyncio
    async def test_graceful_degradation_under_load(self):
        """Test that GOODBYE protocol maintains performance under load."""
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()

            # Create larger cluster to stress test
            server_count = 8
            ports = [port_manager.get_server_port() for _ in range(server_count)]

            # Create servers in star topology (all connect to first server)
            hub_port = ports[0]

            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="LoadTestHub",
                    cluster_id="load-test-cluster",
                    log_level="INFO",
                    gossip_interval=0.3,  # Aggressive gossip
                )
            )

            nodes = []
            for i, port in enumerate(ports[1:], 1):
                node = MPREGServer(
                    MPREGSettings(
                        port=port,
                        name=f"LoadNode{i}",
                        cluster_id="load-test-cluster",
                        peers=[f"ws://127.0.0.1:{hub_port}"],
                        log_level="INFO",
                        gossip_interval=0.3,
                    )
                )
                nodes.append(node)

            ctx.servers.extend([hub] + nodes)

            # Start all servers
            all_tasks = []
            hub_task = asyncio.create_task(hub.server())
            all_tasks.append(hub_task)

            for node in nodes:
                task = asyncio.create_task(node.server())
                all_tasks.append(task)

            ctx.tasks.extend(all_tasks)

            # Wait for cluster formation
            await asyncio.sleep(4.0)

            # Verify cluster formation
            node_urls = [f"ws://127.0.0.1:{port}" for port in ports[1:]]
            for node_url in node_urls:
                assert node_url in hub.cluster.peers_info

            print(f"✅ Load test cluster formed: Hub + {len(nodes)} nodes")

            # Measure performance before mass departure
            start_time = time.time()

            # Staggered departures to simulate real-world load
            departure_tasks = []
            for i, node in enumerate(nodes):
                # Stagger departures every 100ms
                async def delayed_goodbye(n, delay):
                    await asyncio.sleep(delay)
                    await n.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

                task = asyncio.create_task(delayed_goodbye(node, i * 0.1))
                departure_tasks.append(task)

            # Wait for all departures to complete
            await asyncio.gather(*departure_tasks)
            await asyncio.sleep(2.0)  # Processing time

            departure_completion_time = time.time()
            total_departure_time = departure_completion_time - start_time

            # Verify all departures processed correctly
            departed_count = 0
            for node_url in node_urls:
                if node_url in hub.cluster._departed_peers:
                    departed_count += 1
                assert node_url not in hub.cluster.peers_info

            # Performance verification
            avg_departure_time = total_departure_time / len(nodes)

            print("✅ Load test results:")
            print(f"   - Total departure time: {total_departure_time:.2f}s")
            print(f"   - Average time per departure: {avg_departure_time:.3f}s")
            print(f"   - Nodes properly departed: {departed_count}/{len(nodes)}")
            print(f"   - Hub stability maintained: {len(hub.cluster.peers_info) == 1}")

            # Verify performance is reasonable (should be very fast)
            assert avg_departure_time < 1.0  # Each departure should take < 1 second
            assert departed_count == len(nodes)  # All departures should succeed
            assert len(hub.cluster.peers_info) == 1  # Only hub should remain
