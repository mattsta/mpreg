import asyncio

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import allocate_port_range
from mpreg.datastructures.function_identity import FunctionSelector
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


@pytest.mark.asyncio
async def test_fabric_catalog_propagates_over_server_gossip(
    test_context: AsyncTestContext,
) -> None:
    ports = allocate_port_range(2, "servers")
    settings_a = MPREGSettings(
        host="127.0.0.1",
        port=ports[0],
        name="Fabric A",
        cluster_id="fabric-cluster",
        gossip_interval=0.2,
        monitoring_enabled=False,
    )
    settings_b = MPREGSettings(
        host="127.0.0.1",
        port=ports[1],
        name="Fabric B",
        cluster_id="fabric-cluster",
        connect=f"ws://127.0.0.1:{ports[0]}",
        gossip_interval=0.2,
        monitoring_enabled=False,
    )

    server_a = MPREGServer(settings=settings_a)
    server_b = MPREGServer(settings=settings_b)
    test_context.servers.extend([server_a, server_b])

    task_a = asyncio.create_task(server_a.server())
    task_b = asyncio.create_task(server_b.server())
    test_context.tasks.extend([task_a, task_b])

    await asyncio.sleep(1.0)

    def fabric_test(payload: str) -> str:
        return payload

    server_a.register_command("fabric_test", fabric_test, ["cpu"])

    await asyncio.sleep(1.5)

    catalog = server_b._fabric_control_plane.catalog
    matches = catalog.functions.find(FunctionSelector(name="fabric_test"))

    assert matches
