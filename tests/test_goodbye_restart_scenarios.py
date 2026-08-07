"""Restart scenarios for catalog-driven peer discovery."""

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


class TestGoodbyeRestartScenarios:
    @pytest.mark.asyncio
    async def test_restart_in_hub_topology(self):
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()
            hub_port = port_manager.get_server_port()
            node_port = port_manager.get_server_port()

            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="Hub",
                    cluster_id="restart-hub",
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            node = MPREGServer(
                MPREGSettings(
                    port=node_port,
                    name="Node",
                    cluster_id="restart-hub",
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
                error_message="Hub did not remove node after GOODBYE",
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
                    cluster_id="restart-hub",
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
                error_message="Hub did not see node after restart",
            )
