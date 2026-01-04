"""Test connection stability after multiple GOODBYE removals."""

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


class TestGoodbyeConnectionStormPrevention:
    @pytest.mark.asyncio
    async def test_bulk_departures_do_not_reconnect(self):
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()
            hub_port = port_manager.get_server_port()
            node_ports = [port_manager.get_server_port() for _ in range(3)]

            hub = MPREGServer(
                MPREGSettings(
                    port=hub_port,
                    name="Hub",
                    cluster_id="goodbye-storm-test",
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            nodes = [
                MPREGServer(
                    MPREGSettings(
                        port=node_port,
                        name=f"Node-{idx}",
                        cluster_id="goodbye-storm-test",
                        peers=[f"ws://127.0.0.1:{hub_port}"],
                        gossip_interval=0.5,
                        monitoring_enabled=False,
                    )
                )
                for idx, node_port in enumerate(node_ports)
            ]
            ctx.servers.append(hub)
            ctx.servers.extend(nodes)

            hub_task = asyncio.create_task(hub.server())
            ctx.tasks.append(hub_task)
            for node in nodes:
                ctx.tasks.append(asyncio.create_task(node.server()))

            await asyncio.sleep(3.0)

            for node_port in node_ports:
                node_url = f"ws://127.0.0.1:{node_port}"
                assert node_url in _peer_node_ids(hub)

            await asyncio.gather(
                *[node.send_goodbye(GoodbyeReason.MANUAL_REMOVAL) for node in nodes]
            )

            await wait_for_condition(
                lambda: len(_peer_node_ids(hub)) == 1,
                timeout=5.0,
                error_message="Hub still sees departed peers after GOODBYE",
            )

            await asyncio.sleep(2.0)
            assert len(hub.peer_connections) == 0
