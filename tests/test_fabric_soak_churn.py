import asyncio
import time

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext

pytestmark = pytest.mark.slow

FUNCTION_NAME = "soak_echo"
FUNCTION_ID = "soak.echo"
VERSION_CONSTRAINT = ">=1.0.0,<2.0.0"


def _make_echo(node_idx: int):
    def echo(payload: str) -> str:
        return f"node-{node_idx}:{payload}"

    return echo


async def _start_cluster(
    test_context: AsyncTestContext,
    ports: list[int],
    *,
    cluster_id: str,
    connect_url: str | None = None,
    gossip_interval: float = 0.3,
    node_offset: int = 0,
) -> list[MPREGServer]:
    servers: list[MPREGServer] = []
    hub_url = connect_url or f"ws://127.0.0.1:{ports[0]}"

    for idx, port in enumerate(ports):
        connect = None
        if connect_url:
            connect = connect_url
        elif idx > 0:
            connect = hub_url

        node_id = idx + node_offset
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name=f"{cluster_id}-node-{node_id}",
            cluster_id=cluster_id,
            resources={"compute"},
            connect=connect,
            gossip_interval=gossip_interval,
            fabric_catalog_ttl_seconds=15.0,
            fabric_route_ttl_seconds=10.0,
        )
        server = MPREGServer(settings=settings)
        server.register_command(
            FUNCTION_NAME,
            _make_echo(node_id),
            ["compute"],
            function_id=FUNCTION_ID,
            version="1.0.0",
        )
        test_context.servers.append(server)
        task = asyncio.create_task(server.server(), name=f"{cluster_id}-node-{node_id}")
        test_context.tasks.append(task)
        servers.append(server)
        await asyncio.sleep(0.05)

    await asyncio.sleep(1.0)
    return servers


async def _await_peer_state(
    client: MPREGClientAPI,
    expected_count: int,
    *,
    departed_urls: set[str] | None = None,
    timeout: float = 8.0,
) -> set[str]:
    deadline = time.time() + timeout
    last_count = None
    departed_urls = departed_urls or set()

    while time.time() < deadline:
        peers = await client.list_peers()
        urls = {peer["url"] for peer in peers}
        last_count = len(urls)
        if last_count == expected_count and not (urls & departed_urls):
            return urls
        await asyncio.sleep(0.1)

    raise AssertionError(
        f"Expected {expected_count} peers without departed URLs; got {last_count}"
    )


@pytest.mark.asyncio
async def test_fabric_soak_routing_stability(test_context: AsyncTestContext) -> None:
    cluster_size = 15
    with port_range_context(cluster_size, "servers") as ports:
        await _start_cluster(test_context, ports, cluster_id="soak-cluster")

        async with MPREGClientAPI(f"ws://127.0.0.1:{ports[0]}") as client:
            await _await_peer_state(client, cluster_size - 1)

            for iteration in range(5):
                payloads = [f"payload-{iteration}-{idx}" for idx in range(8)]
                results = await asyncio.gather(
                    *[
                        client.call(
                            FUNCTION_NAME,
                            payload,
                            function_id=FUNCTION_ID,
                            version_constraint=VERSION_CONSTRAINT,
                        )
                        for payload in payloads
                    ]
                )

                for payload, result in zip(payloads, results, strict=False):
                    assert isinstance(result, str)
                    assert result.endswith(payload)


@pytest.mark.asyncio
async def test_fabric_churn_recovery(test_context: AsyncTestContext) -> None:
    initial_size = 12
    replacements = 2
    total_ports = initial_size + replacements

    with port_range_context(total_ports, "servers") as ports:
        initial_ports = ports[:initial_size]
        replacement_ports = ports[initial_size:]

        servers = await _start_cluster(
            test_context, initial_ports, cluster_id="churn-cluster"
        )
        hub_url = f"ws://127.0.0.1:{initial_ports[0]}"

        async with MPREGClientAPI(hub_url) as client:
            await _await_peer_state(client, initial_size - 1)

            departing = servers[-replacements:]
            departed_urls = {
                f"ws://127.0.0.1:{server.settings.port}" for server in departing
            }
            for server in departing:
                await server.shutdown_async()

            await asyncio.sleep(0.5)

            await _start_cluster(
                test_context,
                replacement_ports,
                cluster_id="churn-cluster",
                connect_url=hub_url,
                node_offset=initial_size,
            )

            await _await_peer_state(
                client, initial_size - 1, departed_urls=departed_urls
            )

            payloads = [f"churn-{idx}" for idx in range(6)]
            results = await asyncio.gather(
                *[
                    client.call(
                        FUNCTION_NAME,
                        payload,
                        function_id=FUNCTION_ID,
                        version_constraint=VERSION_CONSTRAINT,
                    )
                    for payload in payloads
                ]
            )

            for payload, result in zip(payloads, results, strict=False):
                assert isinstance(result, str)
                assert result.endswith(payload)
