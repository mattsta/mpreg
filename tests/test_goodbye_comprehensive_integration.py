"""Integration tests for catalog-driven GOODBYE propagation."""

import asyncio

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager, wait_for_condition


def _peer_node_ids(server: MPREGServer) -> set[str]:
    directory = server._peer_directory
    if not directory:
        return set()
    return {node.node_id for node in directory.nodes()}


class TestGoodbyeComprehensiveIntegration:
    @pytest.mark.asyncio
    async def test_goodbye_removes_node_from_all_peers(self):
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()
            hub_port = port_manager.get_server_port()
            node1_port = port_manager.get_server_port()
            node2_port = port_manager.get_server_port()

            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="Hub",
                    cluster_id="goodbye-mesh",
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            node1 = MPREGServer(
                MPREGSettings(
                    port=node1_port,
                    name="Node1",
                    cluster_id="goodbye-mesh",
                    peers=[f"ws://127.0.0.1:{hub_port}"],
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            node2 = MPREGServer(
                MPREGSettings(
                    port=node2_port,
                    name="Node2",
                    cluster_id="goodbye-mesh",
                    peers=[f"ws://127.0.0.1:{hub_port}"],
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            ctx.servers.extend([hub, node1, node2])

            hub_task = asyncio.create_task(hub.server())
            node1_task = asyncio.create_task(node1.server())
            node2_task = asyncio.create_task(node2.server())
            ctx.tasks.extend([hub_task, node1_task, node2_task])

            await asyncio.sleep(3.0)

            node2_url = f"ws://127.0.0.1:{node2_port}"
            assert node2_url in _peer_node_ids(hub)
            assert node2_url in _peer_node_ids(node1)

            await node2.send_goodbye(GoodbyeReason.MAINTENANCE)

            await wait_for_condition(
                lambda: node2_url not in _peer_node_ids(hub),
                timeout=4.0,
                error_message="Hub did not remove Node2 after GOODBYE",
            )
            await wait_for_condition(
                lambda: node2_url not in _peer_node_ids(node1),
                timeout=4.0,
                error_message="Node1 did not remove Node2 after GOODBYE",
            )

    @pytest.mark.asyncio
    async def test_goodbye_then_rejoin(self):
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()
            hub_port = port_manager.get_server_port()
            node_port = port_manager.get_server_port()

            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="Hub",
                    cluster_id="goodbye-rejoin",
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            node = MPREGServer(
                MPREGSettings(
                    port=node_port,
                    name="Node",
                    cluster_id="goodbye-rejoin",
                    peers=[f"ws://127.0.0.1:{hub_port}"],
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            ctx.servers.extend([hub, node])

            hub_task = asyncio.create_task(hub.server())
            node_task = asyncio.create_task(node.server())
            ctx.tasks.extend([hub_task, node_task])

            await asyncio.sleep(2.0)

            node_url = f"ws://127.0.0.1:{node_port}"
            assert node_url in _peer_node_ids(hub)

            await node.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

            await wait_for_condition(
                lambda: node_url not in _peer_node_ids(hub),
                timeout=3.0,
                error_message="Node not removed after GOODBYE",
            )

            await node.shutdown_async()
            try:
                await asyncio.wait_for(node_task, timeout=2.0)
            except TimeoutError:
                node_task.cancel()
                await asyncio.gather(node_task, return_exceptions=True)

            node_restart = MPREGServer(
                MPREGSettings(
                    port=node_port,
                    name="Node-Restarted",
                    cluster_id="goodbye-rejoin",
                    peers=[f"ws://127.0.0.1:{hub_port}"],
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            ctx.servers.append(node_restart)
            node_restart_task = asyncio.create_task(node_restart.server())
            ctx.tasks.append(node_restart_task)

            await wait_for_condition(
                lambda: node_url in _peer_node_ids(hub),
                timeout=4.0,
                error_message="Node did not rejoin after restart",
            )
