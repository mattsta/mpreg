"""Test GOODBYE removal and immediate re-entry with catalog-based discovery."""

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


class TestGoodbyeImmediateReentry:
    @pytest.mark.asyncio
    async def test_immediate_reentry_after_goodbye(self):
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()
            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()

            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="Server1_Stable",
                    cluster_id="immediate-reentry-test",
                    log_level="INFO",
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2_Rejoining",
                    cluster_id="immediate-reentry-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            ctx.servers.extend([server1, server2])

            server1_task = asyncio.create_task(server1.server())
            server2_task = asyncio.create_task(server2.server())
            ctx.tasks.extend([server1_task, server2_task])

            await asyncio.sleep(2.0)

            server2_url = f"ws://127.0.0.1:{server2_port}"
            assert server2_url in _peer_node_ids(server1)

            await server2.send_goodbye(GoodbyeReason.MAINTENANCE)

            await wait_for_condition(
                lambda: server2_url not in _peer_node_ids(server1),
                timeout=3.0,
                error_message="Server2 was not removed after GOODBYE",
            )

            await server2.shutdown_async()
            try:
                await asyncio.wait_for(server2_task, timeout=2.0)
            except TimeoutError:
                server2_task.cancel()
                await asyncio.gather(server2_task, return_exceptions=True)

            server2_new = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2_Restarted",
                    cluster_id="immediate-reentry-test",
                    peers=[f"ws://127.0.0.1:{server1_port}"],
                    log_level="INFO",
                    gossip_interval=0.5,
                    monitoring_enabled=False,
                )
            )
            ctx.servers.append(server2_new)
            server2_new_task = asyncio.create_task(server2_new.server())
            ctx.tasks.append(server2_new_task)

            await wait_for_condition(
                lambda: server2_url in _peer_node_ids(server1),
                timeout=4.0,
                error_message="Server2 did not rejoin after restart",
            )
