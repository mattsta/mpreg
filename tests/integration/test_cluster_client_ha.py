import asyncio
import time

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.cluster_client import MPREGClusterClient
from mpreg.core.cluster_map import ClusterMapSnapshot
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext

FUNCTION_ID = "market.quote"
INDICATOR_ID = "market.indicator"


def _make_named_handler(node_name: str, prefix: str):
    def handler(payload: str) -> str:
        return f"{prefix}:{node_name}:{payload}"

    return handler


async def _start_server(
    ctx: AsyncTestContext,
    *,
    port: int,
    name: str,
    cluster_id: str,
    connect: str | None,
    cache_region: str = "local",
    discovery_summary_export_enabled: bool = False,
    discovery_summary_export_interval_seconds: float = 30.0,
    discovery_summary_export_namespaces: tuple[str, ...] = (),
    discovery_summary_export_scope: str | None = None,
    discovery_summary_resolver_mode: bool = False,
    discovery_summary_resolver_prune_interval_seconds: float = 30.0,
    federation_permissive: bool = False,
    register: tuple[str, str] | None = None,
) -> MPREGServer:
    federation_config = (
        create_permissive_bridging_config(cluster_id) if federation_permissive else None
    )
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id=cluster_id,
        connect=connect,
        cache_region=cache_region,
        federation_config=federation_config,
        resources={"market"},
        gossip_interval=0.3,
        fabric_catalog_ttl_seconds=15.0,
        fabric_route_ttl_seconds=10.0,
        discovery_summary_export_enabled=discovery_summary_export_enabled,
        discovery_summary_export_interval_seconds=discovery_summary_export_interval_seconds,
        discovery_summary_export_namespaces=discovery_summary_export_namespaces,
        discovery_summary_export_scope=discovery_summary_export_scope,
        discovery_summary_resolver_mode=discovery_summary_resolver_mode,
        discovery_summary_resolver_prune_interval_seconds=discovery_summary_resolver_prune_interval_seconds,
    )
    server = MPREGServer(settings=settings)
    if register:
        fun_name, fun_id = register
        server.register_command(
            fun_name,
            _make_named_handler(name, fun_name),
            ["market"],
            function_id=fun_id,
            version="1.0.0",
        )
    ctx.servers.append(server)
    ctx.tasks.append(asyncio.create_task(server.server(), name=name))
    await asyncio.sleep(0.05)
    return server


async def _wait_for_cluster_nodes(
    client: MPREGClientAPI, expected_count: int, *, timeout: float = 6.0
) -> ClusterMapSnapshot:
    deadline = time.time() + timeout
    last_count = 0
    while time.time() < deadline:
        snapshot = await client.cluster_map()
        last_count = len(snapshot.nodes)
        if last_count >= expected_count:
            return snapshot
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"Expected at least {expected_count} nodes in cluster map, got {last_count}"
    )


def _load_score_for(snapshot: ClusterMapSnapshot, node_id: str) -> float | None:
    for node in snapshot.nodes:
        if node.node_id == node_id:
            load = node.load
            if load is not None:
                return float(load.load_score)
    return None


async def _wait_for_load_preference(
    client: MPREGClientAPI,
    *,
    provider_a_url: str,
    provider_b_url: str,
    timeout: float = 6.0,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        snapshot = await client.cluster_map()
        load_a = _load_score_for(snapshot, provider_a_url)
        load_b = _load_score_for(snapshot, provider_b_url)
        if load_a is not None and load_b is not None and load_a > load_b:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("Load metrics did not converge for providers")


@pytest.mark.asyncio
async def test_cluster_map_and_client_pool_failover(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub1_url = f"ws://127.0.0.1:{ports[0]}"
        hub2_url = f"ws://127.0.0.1:{ports[1]}"
        provider_url = f"ws://127.0.0.1:{ports[2]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="hub-1",
            cluster_id="market",
            connect=None,
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="hub-2",
            cluster_id="market",
            connect=hub1_url,
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="provider-1",
            cluster_id="market",
            connect=hub1_url,
            register=("quote", FUNCTION_ID),
        )

        async with MPREGClientAPI(hub1_url) as client:
            snapshot = await _wait_for_cluster_nodes(client, expected_count=3)
            nodes = snapshot.nodes
            assert any(node.advertised_urls for node in nodes)

        cluster_client = MPREGClusterClient(seed_urls=(hub1_url,))
        await cluster_client.connect()
        await cluster_client.refresh_cluster_map()
        result = await cluster_client.call(
            "quote", "EURUSD", function_id=FUNCTION_ID, version_constraint=">=1.0.0"
        )
        assert "provider-1" in str(result)

        for server in test_context.servers:
            if server.settings.port == ports[0]:
                await server.shutdown_async()
                break

        await asyncio.sleep(0.5)

        result_after = await cluster_client.call(
            "quote", "EURUSD", function_id=FUNCTION_ID, version_constraint=">=1.0.0"
        )
        assert "provider-1" in str(result_after)
        await cluster_client.disconnect()


@pytest.mark.asyncio
async def test_load_aware_routing_prefers_less_busy_provider(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        provider_a_url = f"ws://127.0.0.1:{ports[1]}"
        provider_b_url = f"ws://127.0.0.1:{ports[2]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id="market-load",
            connect=None,
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="provider-a",
            cluster_id="market-load",
            connect=hub_url,
            register=("indicator", INDICATOR_ID),
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="provider-b",
            cluster_id="market-load",
            connect=hub_url,
            register=("indicator", INDICATOR_ID),
        )

        busy_clients = []
        for _ in range(3):
            busy_client = MPREGClientAPI(provider_a_url)
            await busy_client.connect()
            busy_clients.append(busy_client)
            test_context.clients.append(busy_client)

        async with MPREGClientAPI(hub_url) as client:
            await asyncio.sleep(0.6)
            await _wait_for_load_preference(
                client,
                provider_a_url=provider_a_url,
                provider_b_url=provider_b_url,
            )

            results = [
                await client.call(
                    "indicator",
                    "SPY",
                    function_id=INDICATOR_ID,
                    version_constraint=">=1.0.0",
                )
                for _ in range(4)
            ]

            assert all("provider-b" in str(result) for result in results)


@pytest.mark.asyncio
async def test_cluster_client_prefers_region(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        provider_a_url = f"ws://127.0.0.1:{ports[1]}"
        provider_b_url = f"ws://127.0.0.1:{ports[2]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id="market-region",
            connect=None,
            cache_region="us-east",
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="provider-a",
            cluster_id="market-region",
            connect=hub_url,
            cache_region="us-east",
            register=("quote", FUNCTION_ID),
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="provider-b",
            cluster_id="market-region",
            connect=hub_url,
            cache_region="eu-west",
            register=("quote", FUNCTION_ID),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_cluster_nodes(client, expected_count=3)

        cluster_client = MPREGClusterClient(
            seed_urls=(hub_url,),
            preferred_region="us-east",
        )
        await cluster_client.connect()
        await cluster_client.refresh_cluster_map()
        candidates = cluster_client._candidate_urls()
        assert provider_b_url not in candidates
        await cluster_client.disconnect()


@pytest.mark.asyncio
async def test_cluster_client_uses_summary_ingress_hints(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        global_url = f"ws://127.0.0.1:{ports[0]}"
        region_url = f"ws://127.0.0.1:{ports[1]}"
        provider_url = f"ws://127.0.0.1:{ports[2]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="global-hub",
            cluster_id="global",
            connect=None,
            discovery_summary_resolver_mode=True,
            discovery_summary_resolver_prune_interval_seconds=0.2,
            federation_permissive=True,
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="region-hub",
            cluster_id="region-east",
            connect=global_url,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.1,
            discovery_summary_export_namespaces=("svc.market",),
            discovery_summary_export_scope="global",
            federation_permissive=True,
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="region-provider",
            cluster_id="region-east",
            connect=region_url,
            register=("svc.market.quote", FUNCTION_ID),
        )

        async with MPREGClientAPI(global_url) as client:
            deadline = time.time() + 6.0
            while time.time() < deadline:
                snapshot = await client.cluster_map()
                region_nodes = [
                    node for node in snapshot.nodes if node.cluster_id == "region-east"
                ]
                if len(region_nodes) >= 1:
                    break
                await asyncio.sleep(0.1)

        cluster_client = MPREGClusterClient(seed_urls=(global_url,))
        await cluster_client.connect()

        deadline = time.time() + 6.0
        summary_response = None
        while time.time() < deadline:
            summary_response = await cluster_client.summary_query(
                namespace="svc.market",
                scope="global",
                include_ingress=True,
                ingress_limit=1,
            )
            summary = next(
                (
                    item
                    for item in summary_response.items
                    if item.service_id == "svc.market.quote"
                ),
                None,
            )
            if summary is not None:
                break
            await asyncio.sleep(0.1)

        assert summary_response is not None
        summary = next(
            (
                item
                for item in summary_response.items
                if item.service_id == "svc.market.quote"
            ),
            None,
        )
        assert summary is not None
        assert summary.source_cluster is not None

        result = await cluster_client.call_with_summary(
            summary,
            "svc.market.quote",
            "EURUSD",
            function_id=FUNCTION_ID,
            version_constraint=">=1.0.0",
            ingress=summary_response.ingress,
        )
        assert "region-provider" in str(result)

        direct_result = await cluster_client.call(
            "svc.market.quote",
            "EURUSD",
            function_id=FUNCTION_ID,
            version_constraint=">=1.0.0",
            target_cluster=summary.source_cluster,
            use_ingress_hints=True,
        )
        assert "region-provider" in str(direct_result)
        await cluster_client.disconnect()


@pytest.mark.asyncio
async def test_cluster_client_auto_summary_redirect(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        global_url = f"ws://127.0.0.1:{ports[0]}"
        region_url = f"ws://127.0.0.1:{ports[1]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="global-hub",
            cluster_id="global",
            connect=None,
            discovery_summary_resolver_mode=True,
            discovery_summary_resolver_prune_interval_seconds=0.2,
            federation_permissive=True,
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="region-hub",
            cluster_id="region-east",
            connect=global_url,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.1,
            discovery_summary_export_namespaces=("svc.market",),
            discovery_summary_export_scope="global",
            federation_permissive=True,
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="region-provider",
            cluster_id="region-east",
            connect=region_url,
            register=("svc.market.quote", FUNCTION_ID),
        )

        async with MPREGClientAPI(global_url) as client:
            await _wait_for_cluster_nodes(client, expected_count=3)

        cluster_client = MPREGClusterClient(
            seed_urls=(global_url,),
            auto_summary_redirect=True,
            summary_redirect_scope="global",
            summary_redirect_ingress_limit=1,
        )
        await cluster_client.connect()

        deadline = time.time() + 6.0
        while time.time() < deadline:
            summary_response = await cluster_client.summary_query(
                namespace="svc.market",
                scope="global",
                include_ingress=False,
            )
            if any(
                item.service_id == "svc.market.quote" for item in summary_response.items
            ):
                break
            await asyncio.sleep(0.1)

        result = await cluster_client.call(
            "svc.market.quote",
            "EURUSD",
            function_id=FUNCTION_ID,
            version_constraint=">=1.0.0",
            preferred_urls=(global_url,),
            enable_summary_redirect=True,
        )
        assert "region-provider" in str(result)
        await cluster_client.disconnect()
