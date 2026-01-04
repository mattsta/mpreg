"""
Tests for catalog-driven auto-discovery across clusters.

These tests validate that nodes discover each other via fabric catalog updates
and can route RPCs without explicit mesh configuration.
"""

import asyncio
import os

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import wait_for_condition


def _peer_node_ids(server: MPREGServer) -> set[str]:
    directory = server._peer_directory
    if not directory:
        return set()
    return {node.node_id for node in directory.nodes()}


def _concurrency_factor() -> float:
    return 2.0 if os.environ.get("PYTEST_XDIST_WORKER") else 1.0


class TestCatalogAutoDiscovery:
    async def test_two_node_catalog_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        port1, port2 = server_cluster_ports[:2]

        node1_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Catalog-Node-1",
            cluster_id="catalog-test-cluster",
            resources={"node1-resource"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=0.5,
            monitoring_enabled=False,
        )

        node2_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Catalog-Node-2",
            cluster_id="catalog-test-cluster",
            resources={"node2-resource"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=0.5,
            monitoring_enabled=False,
        )

        servers = [
            MPREGServer(settings=node1_settings),
            MPREGServer(settings=node2_settings),
        ]
        test_context.servers.extend(servers)

        def node1_function(data: str) -> dict:
            return {
                "processed_by": "Catalog-Node-1",
                "input_data": data,
                "discovery_test": True,
            }

        def node2_function(processed_data: dict) -> str:
            return f"Final result from {processed_data['processed_by']}: {processed_data['input_data']}"

        servers[0].register_command(
            "node1_function", node1_function, ["node1-resource"]
        )
        servers[1].register_command(
            "node2_function", node2_function, ["node2-resource"]
        )

        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port2}" in _peer_node_ids(servers[0]),
            timeout=5.0 * _concurrency_factor(),
            error_message="Catalog discovery did not converge for node2",
        )

        client1 = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        test_context.clients.append(client1)
        await client1.connect()

        result = await client1._client.request(
            [
                RPCCommand(
                    name="step1_result",
                    fun="node1_function",
                    args=("catalog_test_data",),
                    locs=frozenset(["node1-resource"]),
                ),
                RPCCommand(
                    name="step2_result",
                    fun="node2_function",
                    args=("step1_result",),
                    locs=frozenset(["node2-resource"]),
                ),
            ]
        )

        assert (
            result["step2_result"]
            == "Final result from Catalog-Node-1: catalog_test_data"
        )

    async def test_three_node_catalog_mesh_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        port1, port2, port3 = server_cluster_ports[:3]

        settings = [
            MPREGSettings(
                host="127.0.0.1",
                port=port1,
                name="Catalog-Hub",
                cluster_id="mesh-catalog-cluster",
                resources={"hub-resource"},
                connect=None,
                gossip_interval=0.5,
                monitoring_enabled=False,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port2,
                name="Catalog-Spoke-1",
                cluster_id="mesh-catalog-cluster",
                resources={"spoke1-resource"},
                connect=f"ws://127.0.0.1:{port1}",
                gossip_interval=0.5,
                monitoring_enabled=False,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port3,
                name="Catalog-Spoke-2",
                cluster_id="mesh-catalog-cluster",
                resources={"spoke2-resource"},
                connect=f"ws://127.0.0.1:{port1}",
                gossip_interval=0.5,
                monitoring_enabled=False,
            ),
        ]
        servers = [MPREGServer(settings=s) for s in settings]
        test_context.servers.extend(servers)

        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await wait_for_condition(
            lambda: len(_peer_node_ids(servers[0])) >= 3,
            timeout=6.0 * _concurrency_factor(),
            error_message="Catalog mesh did not converge to 3 nodes",
        )

        for server in servers:
            assert len(_peer_node_ids(server)) >= 3

    async def test_catalog_refresh_keeps_nodes_alive(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        port1, port2, port3 = server_cluster_ports[:3]
        ttl_seconds = 4.0

        settings = [
            MPREGSettings(
                host="127.0.0.1",
                port=port1,
                name="Catalog-Refresh-Hub",
                cluster_id="catalog-refresh-cluster",
                resources={"hub-resource"},
                connect=None,
                gossip_interval=0.5,
                fabric_catalog_ttl_seconds=ttl_seconds,
                monitoring_enabled=False,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port2,
                name="Catalog-Refresh-Spoke-1",
                cluster_id="catalog-refresh-cluster",
                resources={"spoke1-resource"},
                connect=f"ws://127.0.0.1:{port1}",
                gossip_interval=0.5,
                fabric_catalog_ttl_seconds=ttl_seconds,
                monitoring_enabled=False,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port3,
                name="Catalog-Refresh-Spoke-2",
                cluster_id="catalog-refresh-cluster",
                resources={"spoke2-resource"},
                connect=f"ws://127.0.0.1:{port1}",
                gossip_interval=0.5,
                fabric_catalog_ttl_seconds=ttl_seconds,
                monitoring_enabled=False,
            ),
        ]
        servers = [MPREGServer(settings=s) for s in settings]
        test_context.servers.extend(servers)

        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await wait_for_condition(
            lambda: len(_peer_node_ids(servers[0])) >= 3,
            timeout=6.0 * _concurrency_factor(),
            error_message="Catalog refresh cluster did not converge to 3 nodes",
        )

        await asyncio.sleep(ttl_seconds * 1.5 * _concurrency_factor())

        for server in servers:
            assert len(_peer_node_ids(server)) >= 3, (
                "Catalog refresh failed to keep peer entries alive"
            )
