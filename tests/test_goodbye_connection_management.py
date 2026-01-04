"""Test catalog-driven connection management and GOODBYE removal."""

import asyncio
import time
import uuid

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.fabric.catalog import NodeDescriptor
from mpreg.fabric.catalog_delta import RoutingCatalogDelta
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager, wait_for_condition


def _peer_node_ids(server: MPREGServer) -> set[str]:
    directory = server._peer_directory
    if not directory:
        return set()
    return {node.node_id for node in directory.nodes()}


class TestGoodbyeConnectionManagement:
    @pytest.mark.asyncio
    async def test_connection_manager_respects_catalog_departure(self):
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()
            server1_port = port_manager.get_server_port()
            server2_port = port_manager.get_server_port()

            server1 = MPREGServer(
                MPREGSettings(
                    port=server1_port,
                    name="Server1",
                    cluster_id="goodbye-connection-test",
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )
            server2 = MPREGServer(
                MPREGSettings(
                    port=server2_port,
                    name="Server2",
                    cluster_id="goodbye-connection-test",
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
            assert server2_url in _peer_node_ids(server1)
            assert server2_url in server1.peer_connections

            await server2.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

            await wait_for_condition(
                lambda: server2_url not in _peer_node_ids(server1),
                timeout=3.0,
                error_message="Server2 not removed from catalog after GOODBYE",
            )

            await asyncio.sleep(1.0)
            assert server2_url not in server1.peer_connections

    @pytest.mark.asyncio
    async def test_catalog_departure_removes_connection_candidate(self):
        async with AsyncTestContext() as ctx:
            port_manager = TestPortManager()
            server_port = port_manager.get_server_port()
            server = MPREGServer(
                MPREGSettings(
                    port=server_port,
                    name="Server1",
                    cluster_id="goodbye-manual-test",
                    log_level="INFO",
                    gossip_interval=0.5,
                )
            )
            ctx.servers.append(server)
            server_task = asyncio.create_task(server.server())
            ctx.tasks.append(server_task)
            await asyncio.sleep(1.0)

            fake_url = port_manager.get_server_url()
            now = time.time()
            delta = RoutingCatalogDelta(
                update_id=str(uuid.uuid4()),
                cluster_id="goodbye-manual-test",
                sent_at=now,
                nodes=(
                    NodeDescriptor(
                        node_id=fake_url,
                        cluster_id="goodbye-manual-test",
                        resources=frozenset(),
                        capabilities=frozenset(),
                        advertised_at=now,
                        ttl_seconds=30.0,
                    ),
                ),
            )
            server._fabric_control_plane.applier.apply(delta, now=now)

            assert fake_url in _peer_node_ids(server)

            server._apply_remote_departure(
                server_url=fake_url, cluster_id="goodbye-manual-test"
            )
            assert fake_url not in _peer_node_ids(server)
