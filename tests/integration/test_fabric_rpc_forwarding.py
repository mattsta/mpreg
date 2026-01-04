import asyncio
import time

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import MPREGException
from mpreg.datastructures.function_identity import FunctionSelector
from mpreg.fabric.index import FunctionQuery
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager, wait_for_condition


def _function_visible(
    server: MPREGServer,
    *,
    name: str,
    function_id: str,
    cluster_id: str,
    node_id: str | None = None,
    resources: frozenset[str] | None = None,
) -> bool:
    if not server._fabric_control_plane:
        return False
    query = FunctionQuery(
        selector=FunctionSelector(name=name, function_id=function_id),
        cluster_id=cluster_id,
    )
    matches = server._fabric_control_plane.index.find_functions(query, now=time.time())
    if resources:
        matches = [entry for entry in matches if resources.issubset(entry.resources)]
    if node_id:
        return any(entry.node_id == node_id for entry in matches)
    return bool(matches)


@pytest.mark.asyncio
async def test_fabric_rpc_forwarding_via_intermediate() -> None:
    async with AsyncTestContext() as ctx:
        port_manager = TestPortManager()
        port_a = port_manager.get_server_port()
        port_b = port_manager.get_server_port()
        port_c = port_manager.get_server_port()

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Fabric-A",
            cluster_id="fabric-cluster",
            connect=None,
            peers=None,
            gossip_interval=0.5,
            fabric_routing_enabled=True,
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Fabric-B",
            cluster_id="fabric-cluster",
            connect=f"ws://127.0.0.1:{port_a}",
            peers=None,
            gossip_interval=0.5,
            fabric_routing_enabled=True,
        )
        settings_c = MPREGSettings(
            host="127.0.0.1",
            port=port_c,
            name="Fabric-C",
            cluster_id="fabric-cluster",
            connect=f"ws://127.0.0.1:{port_b}",
            peers=None,
            gossip_interval=0.5,
            fabric_routing_enabled=True,
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        server_c = MPREGServer(settings=settings_c)
        ctx.servers.extend([server_a, server_b, server_c])

        ctx.tasks.extend(
            [
                asyncio.create_task(server_a.server()),
                asyncio.create_task(server_b.server()),
                asyncio.create_task(server_c.server()),
            ]
        )

        await asyncio.sleep(2.0)

        def mesh_function(data: str) -> str:
            return f"mesh:{data}"

        server_c.register_command(
            "mesh_function",
            mesh_function,
            ["mesh-resource"],
            function_id="func-mesh",
            version="1.0.0",
        )

        await wait_for_condition(
            lambda: _function_visible(
                server_a,
                name="mesh_function",
                function_id="func-mesh",
                cluster_id="fabric-cluster",
                node_id=server_c.cluster.local_url,
            ),
            timeout=10.0,
            interval=0.2,
            error_message="fabric function did not propagate to server A",
        )

        async with MPREGClientAPI(f"ws://127.0.0.1:{port_a}") as client:
            result = await client.call(
                "mesh_function",
                "payload",
                locs=frozenset({"mesh-resource"}),
                function_id="func-mesh",
                version_constraint="==1.0.0",
            )

        assert result == "mesh:payload"


@pytest.mark.asyncio
async def test_fabric_rpc_resource_filtering() -> None:
    async with AsyncTestContext() as ctx:
        port_manager = TestPortManager()
        port_a = port_manager.get_server_port()
        port_b = port_manager.get_server_port()
        port_c = port_manager.get_server_port()

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Fabric-Res-A",
            cluster_id="fabric-resources",
            connect=None,
            peers=None,
            gossip_interval=0.5,
            fabric_routing_enabled=True,
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Fabric-Res-B",
            cluster_id="fabric-resources",
            connect=f"ws://127.0.0.1:{port_a}",
            peers=None,
            gossip_interval=0.5,
            fabric_routing_enabled=True,
        )
        settings_c = MPREGSettings(
            host="127.0.0.1",
            port=port_c,
            name="Fabric-Res-C",
            cluster_id="fabric-resources",
            connect=f"ws://127.0.0.1:{port_b}",
            peers=None,
            gossip_interval=0.5,
            fabric_routing_enabled=True,
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        server_c = MPREGServer(settings=settings_c)
        ctx.servers.extend([server_a, server_b, server_c])

        ctx.tasks.extend(
            [
                asyncio.create_task(server_a.server()),
                asyncio.create_task(server_b.server()),
                asyncio.create_task(server_c.server()),
            ]
        )

        await asyncio.sleep(2.0)

        def cpu_function(data: str) -> str:
            return f"cpu:{data}"

        def gpu_function(data: str) -> str:
            return f"gpu:{data}"

        server_b.register_command(
            "tensor_function",
            cpu_function,
            ["cpu"],
            function_id="func-tensor",
            version="1.0.0",
        )
        server_c.register_command(
            "tensor_function",
            gpu_function,
            ["gpu"],
            function_id="func-tensor",
            version="1.0.0",
        )

        await wait_for_condition(
            lambda: _function_visible(
                server_a,
                name="tensor_function",
                function_id="func-tensor",
                cluster_id="fabric-resources",
                resources=frozenset({"gpu"}),
            ),
            timeout=10.0,
            interval=0.2,
            error_message="GPU function did not propagate to server A",
        )
        await wait_for_condition(
            lambda: _function_visible(
                server_a,
                name="tensor_function",
                function_id="func-tensor",
                cluster_id="fabric-resources",
                resources=frozenset({"cpu"}),
            ),
            timeout=10.0,
            interval=0.2,
            error_message="CPU function did not propagate to server A",
        )

        async with MPREGClientAPI(f"ws://127.0.0.1:{port_a}") as client:
            gpu_result = await client.call(
                "tensor_function",
                "payload",
                locs=frozenset({"gpu"}),
                function_id="func-tensor",
                version_constraint="==1.0.0",
            )
            assert gpu_result == "gpu:payload"

            cpu_result = await client.call(
                "tensor_function",
                "payload",
                locs=frozenset({"cpu"}),
                function_id="func-tensor",
                version_constraint="==1.0.0",
            )
            assert cpu_result == "cpu:payload"

            with pytest.raises(MPREGException):
                await client.call(
                    "tensor_function",
                    "payload",
                    locs=frozenset({"tpu"}),
                    function_id="func-tensor",
                    version_constraint="==1.0.0",
                )


def test_fabric_hop_budget_blocks_forward_headers() -> None:
    from mpreg.fabric.message import MessageHeaders

    port_manager = TestPortManager()
    port_a = port_manager.get_server_port()
    server = MPREGServer(
        settings=MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Fabric-Hop-Headers",
            cluster_id="fabric-hop",
            fabric_routing_enabled=True,
            fabric_routing_max_hops=1,
        )
    )

    headers = MessageHeaders(
        correlation_id="req-1",
        source_cluster="fabric-hop",
        routing_path=("ws://node-a", "ws://node-b"),
        hop_budget=1,
    )
    next_headers = server._next_fabric_headers(
        "req-1",
        headers,
        max_hops=server.settings.fabric_routing_max_hops,
    )
    assert next_headers is None
