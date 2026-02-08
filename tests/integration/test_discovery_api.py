import asyncio
import time

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.cluster_client import MPREGClusterClient
from mpreg.client.pubsub_client import MPREGPubSubClient
from mpreg.core.cluster_map import (
    CatalogQueryResponse,
    ClusterMapResponse,
    PeerSnapshot,
)
from mpreg.core.config import MPREGSettings
from mpreg.core.discovery_summary import ServiceSummary
from mpreg.core.discovery_tenant import DiscoveryTenantCredential
from mpreg.core.model import MPREGException
from mpreg.core.namespace_policy import CutoverWindow, NamespacePolicyRule
from mpreg.core.port_allocator import port_range_context
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


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
    enable_default_queue: bool = False,
    resources: set[str] | None = None,
    discovery_resolver_mode: bool = False,
    discovery_resolver_seed_on_start: bool = True,
    discovery_resolver_namespaces: tuple[str, ...] = (),
    discovery_resolver_prune_interval_seconds: float = 30.0,
    discovery_resolver_query_cache_enabled: bool = True,
    discovery_resolver_query_ttl_seconds: float = 10.0,
    discovery_resolver_query_stale_seconds: float = 20.0,
    discovery_resolver_query_negative_ttl_seconds: float = 5.0,
    discovery_resolver_query_cache_max_entries: int = 1000,
    discovery_summary_resolver_mode: bool = False,
    discovery_summary_resolver_namespaces: tuple[str, ...] = (),
    discovery_summary_resolver_prune_interval_seconds: float = 30.0,
    discovery_summary_resolver_scopes: tuple[str, ...] = (),
    discovery_policy_enabled: bool = False,
    discovery_policy_default_allow: bool = True,
    discovery_policy_rules: tuple[NamespacePolicyRule, ...] = (),
    discovery_tenant_mode: bool = False,
    discovery_tenant_allow_request_override: bool = True,
    discovery_tenant_default_id: str | None = None,
    discovery_tenant_header: str | None = None,
    discovery_tenant_credentials: tuple[DiscoveryTenantCredential, ...] = (),
    discovery_summary_export_enabled: bool = False,
    discovery_summary_export_interval_seconds: float = 30.0,
    discovery_summary_export_namespaces: tuple[str, ...] = (),
    discovery_summary_export_scope: str | None = None,
    discovery_summary_export_include_unscoped: bool = True,
    discovery_summary_export_hold_down_seconds: float = 0.0,
    discovery_summary_export_store_forward_seconds: float = 0.0,
    discovery_summary_export_store_forward_max_messages: int = 0,
    discovery_rate_limit_requests_per_minute: int = 0,
    discovery_rate_limit_window_seconds: float = 60.0,
    discovery_rate_limit_max_keys: int = 2000,
    cache_region: str = "local",
    fabric_catalog_ttl_seconds: float = 15.0,
    federation_permissive: bool = False,
    rpc_spec_gossip_mode: str = "summary",
    rpc_spec_gossip_namespaces: tuple[str, ...] = (),
    rpc_spec_gossip_max_bytes: int | None = None,
    registers: tuple[tuple[str, str, str], ...] = (),
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
        resources=resources or {"market"},
        gossip_interval=0.3,
        fabric_catalog_ttl_seconds=fabric_catalog_ttl_seconds,
        fabric_route_ttl_seconds=10.0,
        enable_default_queue=enable_default_queue,
        discovery_resolver_mode=discovery_resolver_mode,
        discovery_resolver_seed_on_start=discovery_resolver_seed_on_start,
        discovery_resolver_namespaces=discovery_resolver_namespaces,
        discovery_resolver_prune_interval_seconds=discovery_resolver_prune_interval_seconds,
        discovery_resolver_query_cache_enabled=discovery_resolver_query_cache_enabled,
        discovery_resolver_query_ttl_seconds=discovery_resolver_query_ttl_seconds,
        discovery_resolver_query_stale_seconds=discovery_resolver_query_stale_seconds,
        discovery_resolver_query_negative_ttl_seconds=discovery_resolver_query_negative_ttl_seconds,
        discovery_resolver_query_cache_max_entries=discovery_resolver_query_cache_max_entries,
        discovery_summary_resolver_mode=discovery_summary_resolver_mode,
        discovery_summary_resolver_namespaces=discovery_summary_resolver_namespaces,
        discovery_summary_resolver_prune_interval_seconds=discovery_summary_resolver_prune_interval_seconds,
        discovery_summary_resolver_scopes=discovery_summary_resolver_scopes,
        discovery_policy_enabled=discovery_policy_enabled,
        discovery_policy_default_allow=discovery_policy_default_allow,
        discovery_policy_rules=discovery_policy_rules,
        discovery_tenant_mode=discovery_tenant_mode,
        discovery_tenant_allow_request_override=discovery_tenant_allow_request_override,
        discovery_tenant_default_id=discovery_tenant_default_id,
        discovery_tenant_header=discovery_tenant_header,
        discovery_tenant_credentials=discovery_tenant_credentials,
        discovery_summary_export_enabled=discovery_summary_export_enabled,
        discovery_summary_export_interval_seconds=discovery_summary_export_interval_seconds,
        discovery_summary_export_namespaces=discovery_summary_export_namespaces,
        discovery_summary_export_scope=discovery_summary_export_scope,
        discovery_summary_export_include_unscoped=discovery_summary_export_include_unscoped,
        discovery_summary_export_hold_down_seconds=discovery_summary_export_hold_down_seconds,
        discovery_summary_export_store_forward_seconds=discovery_summary_export_store_forward_seconds,
        discovery_summary_export_store_forward_max_messages=discovery_summary_export_store_forward_max_messages,
        discovery_rate_limit_requests_per_minute=discovery_rate_limit_requests_per_minute,
        discovery_rate_limit_window_seconds=discovery_rate_limit_window_seconds,
        discovery_rate_limit_max_keys=discovery_rate_limit_max_keys,
        rpc_spec_gossip_mode=rpc_spec_gossip_mode,
        rpc_spec_gossip_namespaces=rpc_spec_gossip_namespaces,
        rpc_spec_gossip_max_bytes=rpc_spec_gossip_max_bytes,
    )
    server = MPREGServer(settings=settings)
    for fun_name, fun_id, fun_version in registers:
        server.register_command(
            fun_name,
            _make_named_handler(name, fun_name),
            ["market"],
            function_id=fun_id,
            version=fun_version,
        )
    ctx.servers.append(server)
    ctx.tasks.append(asyncio.create_task(server.server(), name=name))
    await asyncio.sleep(0.05)
    return server


async def _wait_for_cluster_map_v2(
    client: MPREGClientAPI, expected_count: int, *, timeout: float = 6.0
) -> ClusterMapResponse:
    deadline = time.time() + timeout
    last_count = 0
    while time.time() < deadline:
        snapshot = await client.cluster_map_v2()
        nodes = snapshot.nodes
        last_count = len(nodes)
        if last_count >= expected_count:
            return snapshot
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"Expected at least {expected_count} nodes in cluster map v2, got {last_count}"
    )


async def _wait_for_summary_exports(
    server: MPREGServer, namespace: str, *, timeout: float = 6.0
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        exported, _ = server._summary_export_state.summary_for_namespace(namespace)
        if exported:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"Expected summary exports for namespace {namespace}")


async def _wait_for_catalog_functions(
    client: MPREGClientAPI,
    *,
    namespace: str,
    expected_count: int,
    timeout: float = 6.0,
    locs: frozenset[str] | None = None,
) -> CatalogQueryResponse:
    deadline = time.time() + timeout
    last_count = 0
    while time.time() < deadline:
        if locs:
            raw_response = await client.call(
                "catalog_query",
                {"entry_type": "functions", "namespace": namespace},
                locs=locs,
            )
            if not isinstance(raw_response, dict):
                raise AssertionError("Expected catalog_query dict response")
            response = CatalogQueryResponse.from_dict(raw_response)
        else:
            response = await client.catalog_query(
                entry_type="functions", namespace=namespace
            )
        last_count = len(response.items)
        if last_count >= expected_count:
            return response
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"Expected at least {expected_count} functions, got {last_count}"
    )


async def _wait_for_peers(
    client: MPREGClientAPI, expected_count: int, *, timeout: float = 6.0
) -> tuple[PeerSnapshot, ...]:
    deadline = time.time() + timeout
    last_count = 0
    while time.time() < deadline:
        peers = await client.list_peers()
        last_count = len(peers)
        if last_count >= expected_count:
            return peers
        await asyncio.sleep(0.1)
    raise AssertionError(f"Expected at least {expected_count} peers, got {last_count}")


async def _wait_for_targeted_peers(
    client: MPREGClientAPI,
    expected_count: int,
    *,
    target_cluster: str,
    timeout: float = 6.0,
) -> tuple[PeerSnapshot, ...]:
    deadline = time.time() + timeout
    last_count = 0
    while time.time() < deadline:
        result = await client.list_peers(target_cluster=target_cluster)
        last_count = len(result)
        if last_count >= expected_count:
            return result
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"Expected at least {expected_count} peers for {target_cluster}, got {last_count}"
    )


@pytest.mark.asyncio
async def test_cluster_map_v2_paginates_and_scopes(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"

        hub = await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id="market",
            connect=None,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="node-2",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.market.indicator", "market.indicator", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="node-3",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.other.quote", "other.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            snapshot = await _wait_for_cluster_map_v2(client, expected_count=3)
            known_nodes = snapshot.nodes
            known_ids = {node.node_id for node in known_nodes}
            for node in known_nodes:
                assert node.region == "local"

            page1 = await client.cluster_map_v2(scope="zone", limit=2)
            nodes1 = page1.nodes
            assert len(nodes1) == 2
            token = page1.next_page_token
            assert token

            page2 = await client.cluster_map_v2(scope="zone", page_token=token)
            nodes2 = page2.nodes
            all_nodes = (*nodes1, *nodes2)
            node_ids = [node.node_id for node in all_nodes]
            assert len(node_ids) >= 3
            assert len(set(node_ids)) == len(node_ids)

            local = await client.cluster_map_v2(scope="local")
            local_nodes = local.nodes
            assert len(local_nodes) == 1
            assert local_nodes[0].node_id in known_ids


@pytest.mark.asyncio
async def test_cluster_map_v2_resource_filter(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="queue-node",
            cluster_id="market",
            connect=None,
            enable_default_queue=True,
            resources={"market", "queue-node"},
            registers=(("svc.market.queue", "market.queue", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="rpc-node",
            cluster_id="market",
            connect=hub_url,
            resources={"market", "rpc-node"},
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            snapshot = await _wait_for_cluster_map_v2(client, expected_count=2)
            total_nodes = snapshot.nodes

            response = await client.cluster_map_v2(resources=["queue-node"])
            nodes = response.nodes
            assert nodes
            assert len(nodes) < len(total_nodes)


@pytest.mark.asyncio
async def test_list_peers_scope_filters(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id="market",
            connect=None,
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="node-2",
            cluster_id="market",
            connect=hub_url,
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="node-3",
            cluster_id="market",
            connect=hub_url,
        )

        async with MPREGClientAPI(hub_url) as client:
            peers = await _wait_for_peers(client, expected_count=2)
            scopes = {peer.scope for peer in peers}
            assert scopes == {"zone"}

            zone_peers = await client.list_peers(scope="zone")
            assert zone_peers
            assert all(peer.scope == "zone" for peer in zone_peers)
            assert all(peer.cluster_id == "market" for peer in zone_peers)

            local_peers = await client.list_peers(scope="local")
            assert not local_peers


@pytest.mark.asyncio
async def test_list_peers_classifies_region_and_global(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(4, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        hub_cluster = "cluster-a1"

        await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id=hub_cluster,
            connect=None,
            cache_region="us-east",
            federation_permissive=True,
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="node-a2",
            cluster_id="cluster-a2",
            connect=hub_url,
            cache_region="us-east",
            federation_permissive=True,
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="node-b1",
            cluster_id="cluster-b1",
            connect=hub_url,
            cache_region="eu-west",
            federation_permissive=True,
        )
        await _start_server(
            test_context,
            port=ports[3],
            name="node-b2",
            cluster_id="cluster-b2",
            connect=hub_url,
            cache_region="eu-west",
            federation_permissive=True,
        )

        async with MPREGClientAPI(hub_url) as client:
            peers = await _wait_for_targeted_peers(
                client,
                expected_count=3,
                target_cluster=hub_cluster,
            )
            scopes = {peer.scope for peer in peers}
            assert "region" in scopes
            assert "global" in scopes
            regions = {peer.region for peer in peers}
            assert "us-east" in regions
            assert "eu-west" in regions


@pytest.mark.asyncio
async def test_catalog_query_functions_and_nodes(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"

        hub = await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id="market",
            connect=None,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="node-2",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.market.indicator", "market.indicator", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="node-3",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.other.quote", "other.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            snapshot = await _wait_for_cluster_map_v2(client, expected_count=3)
            known_nodes = snapshot.nodes
            known_node_ids = {node.node_id for node in known_nodes}
            await _wait_for_catalog_functions(
                client, namespace="svc.market", expected_count=2
            )

            page1 = await client.catalog_query(
                entry_type="functions", namespace="svc.market", limit=1
            )
            items1 = page1.items
            assert len(items1) == 1
            token = page1.next_page_token
            assert token

            page2 = await client.catalog_query(
                entry_type="functions", namespace="svc.market", page_token=token
            )
            items2 = page2.items
            all_items = (*items1, *items2)
            names = [
                item.get("identity", {}).get("name")
                for item in all_items
                if isinstance(item, dict)
            ]
            assert "svc.market.quote" in names
            assert "svc.market.indicator" in names
            assert "svc.other.quote" not in names

            node_page = await client.catalog_query(entry_type="nodes", scope="local")
            node_items = node_page.items
            assert len(node_items) == 1
            assert node_items[0].get("node_id") in known_node_ids


@pytest.mark.asyncio
async def test_catalog_watch_emits_deltas(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"

        server = await _start_server(
            test_context,
            port=ports[0],
            name="delta-hub",
            cluster_id="market",
            connect=None,
        )

        async with MPREGClientAPI(server_url) as client:
            pubsub = MPREGPubSubClient(base_client=client)
            await pubsub.start()
            queue: asyncio.Queue = asyncio.Queue()

            def on_delta(message):
                queue.put_nowait(message)

            watch_info = await client.catalog_watch()
            await pubsub.subscribe(
                patterns=[watch_info.topic],
                callback=on_delta,
                get_backlog=False,
            )

            server.register_command(
                "svc.market.quote_watch",
                _make_named_handler("delta-hub", "quote_watch"),
                ["market"],
                function_id="market.quote_watch",
                version="1.0.0",
            )

            deadline = time.time() + 5.0
            found = False
            while time.time() < deadline:
                try:
                    message = await asyncio.wait_for(
                        queue.get(), timeout=max(0.1, deadline - time.time())
                    )
                except TimeoutError:
                    break
                payload = message.payload
                if not isinstance(payload, dict):
                    continue
                delta = payload.get("delta", {})
                if not isinstance(delta, dict):
                    continue
                functions = delta.get("functions", [])
                if not isinstance(functions, list):
                    continue
                names = [
                    entry.get("identity", {}).get("name")
                    for entry in functions
                    if isinstance(entry, dict)
                ]
                if "svc.market.quote_watch" in names:
                    found = True
                    break

            assert found
            await pubsub.stop()


@pytest.mark.asyncio
async def test_resolver_mode_serves_discovery_from_cache(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id="market",
            connect=None,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="node-2",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.market.indicator", "market.indicator", "1.0.0"),),
        )
        resolver = await _start_server(
            test_context,
            port=ports[2],
            name="resolver",
            cluster_id="market",
            connect=hub_url,
            discovery_resolver_mode=True,
            discovery_resolver_seed_on_start=False,
            registers=(),
        )

        async with MPREGClientAPI(resolver.cluster.local_url) as client:
            response = await _wait_for_catalog_functions(
                client, namespace="svc.market", expected_count=2
            )
            assert len(response.items) >= 2

        assert resolver._discovery_resolver is not None
        assert resolver._discovery_resolver.stats.deltas_applied > 0


@pytest.mark.asyncio
async def test_resolver_cache_stats_and_resync(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id="market",
            connect=None,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="node-2",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.market.indicator", "market.indicator", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[2],
            name="resolver",
            cluster_id="market",
            connect=hub_url,
            discovery_resolver_mode=True,
            discovery_resolver_seed_on_start=False,
            registers=(),
        )

        async with MPREGClientAPI(f"ws://127.0.0.1:{ports[2]}") as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.market", expected_count=2
            )
            stats = await client.resolver_cache_stats()
            assert stats.enabled is True
            assert stats.entry_counts.functions >= 2
            assert stats.stats is not None
            assert stats.stats.deltas_applied >= 1

            resync = await client.resolver_resync()
            assert resync.enabled is True
            assert resync.resynced is True
            assert resync.entry_counts is not None
            assert resync.entry_counts.functions >= 2


@pytest.mark.asyncio
async def test_resolver_query_cache_stale_and_negative(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"

        await _start_server(
            test_context,
            port=ports[0],
            name="hub",
            cluster_id="market",
            connect=None,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="node-2",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.market.indicator", "market.indicator", "1.0.0"),),
        )
        resolver = await _start_server(
            test_context,
            port=ports[2],
            name="resolver",
            cluster_id="market",
            connect=hub_url,
            discovery_resolver_mode=True,
            discovery_resolver_seed_on_start=False,
            discovery_resolver_query_ttl_seconds=0.2,
            discovery_resolver_query_stale_seconds=0.6,
            discovery_resolver_query_negative_ttl_seconds=0.3,
            registers=(),
        )

        async with MPREGClientAPI(resolver.cluster.local_url) as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.market", expected_count=2
            )
            response = await client.catalog_query(
                entry_type="functions", namespace="svc.market"
            )
            assert len(response.items) >= 2

            missing = await client.catalog_query(
                entry_type="functions", namespace="svc.missing"
            )
            assert len(missing.items) == 0

            await client.catalog_query(entry_type="functions", namespace="svc.missing")

            await asyncio.sleep(0.25)
            stale_response = await client.catalog_query(
                entry_type="functions", namespace="svc.market"
            )
            assert len(stale_response.items) >= 2

        resolver_cache = resolver._discovery_resolver
        assert resolver_cache is not None
        with resolver_cache.locked():
            query_stats = resolver_cache.query_cache.snapshot()
        stats_dict = query_stats.to_dict()
        assert stats_dict.get("catalog_negative_hits", 0) >= 1
        assert stats_dict.get("catalog_stale_serves", 0) >= 1


@pytest.mark.asyncio
async def test_namespace_policy_filters_catalog_query(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        secure_url = f"ws://127.0.0.1:{ports[1]}"
        policy_rules = (
            NamespacePolicyRule(
                namespace="svc.secret",
                visibility=("secure-cluster",),
            ),
        )
        await _start_server(
            test_context,
            port=ports[0],
            name="policy-node",
            cluster_id="market",
            connect=None,
            federation_permissive=True,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=True,
            discovery_policy_rules=policy_rules,
            registers=(
                ("svc.market.quote", "market.quote", "1.0.0"),
                ("svc.secret.audit", "secret.audit", "1.0.0"),
            ),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="secure-node",
            cluster_id="secure-cluster",
            connect=server_url,
            federation_permissive=True,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=True,
            discovery_policy_rules=policy_rules,
            registers=(),
        )

        async with (
            MPREGClientAPI(server_url) as client,
            MPREGClientAPI(secure_url) as secure_client,
        ):
            deadline = time.time() + 5.0
            names: list[str] = []
            while time.time() < deadline:
                response = await client.catalog_query(
                    entry_type="functions",
                    namespace="svc",
                )
                names = [
                    item.get("identity", {}).get("name")
                    for item in response.items
                    if isinstance(item, dict)
                ]
                if "svc.market.quote" in names:
                    break
                await asyncio.sleep(0.1)

            assert "svc.market.quote" in names
            assert "svc.secret.audit" not in names

            deadline = time.time() + 5.0
            secure_names: list[str] = []
            while time.time() < deadline:
                secure_response = await secure_client.catalog_query(
                    entry_type="functions",
                    namespace="svc",
                )
                secure_names = [
                    item.get("identity", {}).get("name")
                    for item in secure_response.items
                    if isinstance(item, dict)
                ]
                if "svc.market.quote" in secure_names:
                    break
                await asyncio.sleep(0.1)

            assert "svc.market.quote" in secure_names
            assert "svc.secret.audit" in secure_names

            status = await client.namespace_status(namespace="svc.secret")
            assert status.allowed is False
            secure_status = await secure_client.namespace_status(namespace="svc.secret")
            assert secure_status.allowed is True


@pytest.mark.asyncio
async def test_catalog_query_scope_and_tags_filters(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        server = await _start_server(
            test_context,
            port=ports[0],
            name="tag-node",
            cluster_id="market",
            connect=None,
            registers=(),
        )
        server.register_command(
            "svc.market.zone",
            _make_named_handler("tag-node", "zone"),
            ["market"],
            function_id="market.zone",
            version="1.0.0",
            scope="zone",
            tags=("alpha",),
        )
        server.register_command(
            "svc.market.region",
            _make_named_handler("tag-node", "region"),
            ["market"],
            function_id="market.region",
            version="1.0.0",
            scope="region",
            tags=("beta",),
        )

        async with MPREGClientAPI(server_url) as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.market", expected_count=2
            )
            region_only = await client.catalog_query(
                entry_type="functions", namespace="svc.market", scope="region"
            )
            region_items = region_only.items
            region_names = {
                item.get("identity", {}).get("name")
                for item in region_items
                if isinstance(item, dict)
            }
            assert "svc.market.region" in region_names
            assert "svc.market.zone" not in region_names

            tagged = await client.catalog_query(
                entry_type="functions", namespace="svc.market", tags=["beta"]
            )
            tagged_items = tagged.items
            tagged_names = {
                item.get("identity", {}).get("name")
                for item in tagged_items
                if isinstance(item, dict)
            }
            assert tagged_names == {"svc.market.region"}


@pytest.mark.asyncio
async def test_namespace_policy_apply_and_audit(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="policy-admin",
            cluster_id="market",
            connect=None,
            registers=(),
        )

        async with MPREGClientAPI(server_url) as client:
            validate_response = await client.namespace_policy_validate(
                rules=[
                    {
                        "namespace": "svc.secret",
                        "visibility": ["secure-cluster"],
                        "policy_version": "v1",
                    }
                ],
                actor="tester",
            )
            assert validate_response.valid is True

            apply_response = await client.namespace_policy_apply(
                rules=[
                    {
                        "namespace": "svc.secret",
                        "visibility": ["secure-cluster"],
                        "policy_version": "v1",
                    }
                ],
                enabled=True,
                actor="tester",
            )
            assert apply_response.applied is True

            status = await client.namespace_status(namespace="svc.secret")
            assert status.allowed is False

            audit = await client.namespace_policy_audit(limit=5)
            entries = audit.entries
            assert entries


@pytest.mark.asyncio
async def test_namespace_policy_tenant_visibility_and_audit(
    test_context: AsyncTestContext,
) -> None:
    rules = (
        NamespacePolicyRule(
            namespace="svc.secret",
            visibility_tenants=("tenant-a",),
            policy_version="v1",
        ),
    )
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="tenant-policy",
            cluster_id="market",
            connect=None,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=False,
            discovery_policy_rules=rules,
            discovery_tenant_mode=True,
            registers=(("svc.secret.fn", "secret.fn", "1.0.0"),),
        )

        async with MPREGClientAPI(server_url) as client:
            allowed = await client.catalog_query(
                entry_type="functions",
                namespace="svc.secret",
                viewer_tenant_id="tenant-a",
            )
            allowed_items = allowed.items
            assert allowed_items

            denied = await client.catalog_query(
                entry_type="functions",
                namespace="svc.secret",
                viewer_tenant_id="tenant-b",
            )
            denied_items = denied.items
            assert not denied_items

            audit = await client.discovery_access_audit(limit=10)
            entries = audit.entries
            assert any(
                entry.event == "catalog_query" and entry.reason == "viewer_denied"
                for entry in entries
            )


@pytest.mark.asyncio
async def test_namespace_policy_tenant_auth_binding(
    test_context: AsyncTestContext,
) -> None:
    rules = (
        NamespacePolicyRule(
            namespace="svc.secret",
            visibility_tenants=("tenant-a",),
            policy_version="v1",
        ),
    )
    credentials = (
        DiscoveryTenantCredential(
            tenant_id="tenant-a",
            token="token-a",
            scheme="bearer",
        ),
    )
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="tenant-auth",
            cluster_id="market",
            connect=None,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=False,
            discovery_policy_rules=rules,
            discovery_tenant_mode=True,
            discovery_tenant_allow_request_override=False,
            discovery_tenant_credentials=credentials,
            registers=(("svc.secret.fn", "secret.fn", "1.0.0"),),
        )

        async with MPREGClientAPI(
            server_url,
            auth_token="token-a",
        ) as authed_client:
            allowed = await authed_client.catalog_query(
                entry_type="functions",
                namespace="svc.secret",
                viewer_tenant_id="tenant-b",
            )
            allowed_items = allowed.items
            assert allowed_items

        async with MPREGClientAPI(server_url) as plain_client:
            denied = await plain_client.catalog_query(
                entry_type="functions",
                namespace="svc.secret",
                viewer_tenant_id="tenant-a",
            )
            denied_items = denied.items
            assert not denied_items


@pytest.mark.asyncio
async def test_namespace_policy_export_returns_current_rules(
    test_context: AsyncTestContext,
) -> None:
    rules = (
        NamespacePolicyRule(
            namespace="svc.secret",
            visibility=("secure-cluster",),
            policy_version="v2",
        ),
    )
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="policy-export",
            cluster_id="market",
            connect=None,
            discovery_policy_enabled=True,
            discovery_policy_rules=rules,
            registers=(),
        )

        async with MPREGClientAPI(server_url) as client:
            export = await client.namespace_policy_export()
            assert export.enabled is True
            assert export.rule_count == 1
            assert export.rules[0].namespace == "svc.secret"
            assert export.rules[0].policy_version == "v2"


@pytest.mark.asyncio
async def test_discovery_rate_limit_blocks_excess_queries(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="rate-limit",
            cluster_id="market",
            connect=None,
            discovery_rate_limit_requests_per_minute=2,
            discovery_rate_limit_window_seconds=60.0,
            discovery_rate_limit_max_keys=10,
            registers=(),
        )

        async with MPREGClientAPI(server_url) as client:
            await client.catalog_query(entry_type="functions")
            await client.catalog_query(entry_type="functions")
            with pytest.raises(MPREGException) as exc:
                await client.catalog_query(entry_type="functions")
            assert exc.value.rpc_error.code == 429


@pytest.mark.asyncio
async def test_summary_query_returns_service_summaries(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="summary-node",
            cluster_id="market",
            connect=None,
            registers=(
                ("svc.market.quote", "market.quote", "1.0.0"),
                ("svc.market.indicator", "market.indicator", "1.0.0"),
            ),
        )

        async with MPREGClientAPI(server_url) as client:
            deadline = time.time() + 5.0
            service_ids: list[str] = []
            while time.time() < deadline:
                response = await client.summary_query(namespace="svc.market")
                service_ids = [item.service_id for item in response.items]
                if len(service_ids) >= 2:
                    break
                await asyncio.sleep(0.1)

            assert "svc.market.quote" in service_ids
            assert "svc.market.indicator" in service_ids


@pytest.mark.asyncio
async def test_summary_watch_scope_topics(test_context: AsyncTestContext) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="summary-watch",
            cluster_id="market",
            connect=None,
            registers=(),
        )

        async with MPREGClientAPI(server_url) as client:
            scoped = await client.summary_watch(scope="global", namespace="svc.market")
            assert scoped.topic == "mpreg.discovery.summary.global.svc.market"
            scoped_root = await client.summary_watch(scope="global")
            assert scoped_root.topic == "mpreg.discovery.summary.global"
            unscoped = await client.summary_watch(namespace="svc.market")
            assert unscoped.topic == "mpreg.discovery.summary.svc.market"


@pytest.mark.asyncio
async def test_summary_scope_cache_filters(test_context: AsyncTestContext) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="summary-scope",
            cluster_id="market",
            connect=None,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.2,
            discovery_summary_export_scope="global",
            discovery_summary_export_include_unscoped=False,
            discovery_summary_resolver_mode=True,
            discovery_summary_resolver_scopes=("global",),
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(server_url) as client:
            deadline = time.time() + 6.0
            items: tuple[ServiceSummary, ...] = ()
            while time.time() < deadline:
                response = await client.summary_query(
                    scope="global", namespace="svc.market"
                )
                items = response.items
                if items:
                    break
                await asyncio.sleep(0.1)
            assert items
            assert all(item.scope == "global" for item in items)

            regional = await client.summary_query(
                scope="region", namespace="svc.market"
            )
            assert not regional.items


@pytest.mark.asyncio
async def test_summary_query_global_uses_summary_cache(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        server = await _start_server(
            test_context,
            port=ports[0],
            name="summary-cache",
            cluster_id="market",
            connect=None,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.1,
            discovery_summary_resolver_mode=True,
            discovery_summary_resolver_prune_interval_seconds=0.2,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(server_url) as client:
            deadline = time.time() + 5.0
            found = False
            while time.time() < deadline and not found:
                response = await client.summary_query(
                    namespace="svc.market", scope="global"
                )
                service_ids = [item.service_id for item in response.items]
                if "svc.market.quote" in service_ids:
                    for item in response.items:
                        if item.service_id == "svc.market.quote":
                            assert item.source_cluster == "market"
                    found = True
                    break
                await asyncio.sleep(0.1)

            assert found


@pytest.mark.asyncio
async def test_summary_query_ingress_hints(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="summary-ingress",
            cluster_id="market",
            connect=None,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(server_url) as client:
            deadline = time.time() + 5.0
            ingress: dict[str, list[str]] = {}
            found = False
            while time.time() < deadline and not found:
                response = await client.summary_query(
                    namespace="svc.market",
                    scope="global",
                    include_ingress=True,
                    ingress_limit=1,
                    ingress_scope="zone",
                    ingress_capabilities=["rpc"],
                )
                service_ids = [item.service_id for item in response.items]
                if "svc.market.quote" in service_ids:
                    ingress = {
                        str(key): list(value or [])
                        for key, value in (response.ingress or {}).items()
                        if value is not None
                    }
                    found = True
                    break
                await asyncio.sleep(0.1)

            assert found
            assert ingress.get("market")
            assert server_url in ingress.get("market", [])


@pytest.mark.asyncio
async def test_summary_query_ingress_filters(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="summary-ingress-filter",
            cluster_id="market",
            connect=None,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(server_url) as client:
            deadline = time.time() + 5.0
            found = False
            while time.time() < deadline and not found:
                response = await client.summary_query(namespace="svc.market")
                service_ids = [item.service_id for item in response.items]
                if "svc.market.quote" in service_ids:
                    found = True
                    break
                await asyncio.sleep(0.1)

            assert found

            response = await client.summary_query(
                namespace="svc.market",
                scope="global",
                include_ingress=True,
                ingress_scope="global",
            )
            assert not response.ingress

            response = await client.summary_query(
                namespace="svc.market",
                scope="global",
                include_ingress=True,
                ingress_capabilities=["queue"],
            )
            assert not response.ingress

            response = await client.summary_query(
                namespace="svc.market",
                scope="global",
                include_ingress=True,
                ingress_tags=["edge"],
            )
            assert not response.ingress


@pytest.mark.asyncio
async def test_summary_query_multi_region_delegation(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        global_url = f"ws://127.0.0.1:{ports[0]}"
        us_url = f"ws://127.0.0.1:{ports[1]}"
        eu_url = f"ws://127.0.0.1:{ports[2]}"

        policy_rules = (
            NamespacePolicyRule(
                namespace="svc.prod.us-east",
                owners=("us-east",),
                allow_summaries=True,
                export_scopes=("global",),
            ),
            NamespacePolicyRule(
                namespace="svc.prod.eu-west",
                owners=("eu-west",),
                allow_summaries=True,
                export_scopes=("global",),
            ),
        )

        await _start_server(
            test_context,
            port=ports[0],
            name="global-resolver",
            cluster_id="global",
            connect=None,
            discovery_summary_resolver_mode=True,
            discovery_summary_resolver_prune_interval_seconds=0.2,
            federation_permissive=True,
        )
        us_server = await _start_server(
            test_context,
            port=ports[1],
            name="region-us",
            cluster_id="us-east",
            connect=global_url,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=False,
            discovery_policy_rules=policy_rules,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.1,
            discovery_summary_export_namespaces=("svc.prod",),
            discovery_summary_export_scope="global",
            federation_permissive=True,
            registers=(("svc.prod.us-east.quote", "prod.quote.us", "1.0.0"),),
        )
        eu_server = await _start_server(
            test_context,
            port=ports[2],
            name="region-eu",
            cluster_id="eu-west",
            connect=global_url,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=False,
            discovery_policy_rules=policy_rules,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.1,
            discovery_summary_export_namespaces=("svc.prod",),
            discovery_summary_export_scope="global",
            federation_permissive=True,
            registers=(("svc.prod.eu-west.quote", "prod.quote.eu", "1.0.0"),),
        )

        async with MPREGClientAPI(global_url) as client:
            await _wait_for_cluster_map_v2(client, expected_count=3)

        await _wait_for_summary_exports(us_server, "svc.prod.us-east")
        await _wait_for_summary_exports(eu_server, "svc.prod.eu-west")

        summary_response = None
        summaries: dict[str, ServiceSummary] = {}
        async with MPREGClientAPI(global_url) as client:
            deadline = time.time() + 6.0
            while time.time() < deadline:
                response = await client.summary_query(
                    namespace="svc.prod",
                    scope="global",
                    include_ingress=True,
                    ingress_limit=1,
                    ingress_scope="zone",
                )
                summary_response = response
                summaries = {item.service_id: item for item in response.items}
                if (
                    "svc.prod.us-east.quote" in summaries
                    and "svc.prod.eu-west.quote" in summaries
                ):
                    break
                await asyncio.sleep(0.1)

        assert summary_response is not None
        ingress = summary_response.ingress or {}
        assert ingress.get("us-east")
        assert ingress.get("eu-west")

        us_summary = summaries.get("svc.prod.us-east.quote")
        eu_summary = summaries.get("svc.prod.eu-west.quote")
        assert us_summary is not None
        assert eu_summary is not None

        cluster_client = MPREGClusterClient(seed_urls=(global_url,))
        await cluster_client.connect()

        result_us = await cluster_client.call_with_summary(
            us_summary,
            "svc.prod.us-east.quote",
            "EURUSD",
            function_id="prod.quote.us",
            version_constraint=">=1.0.0",
            ingress=summary_response.ingress,
        )
        assert "region-us" in str(result_us)

        result_eu = await cluster_client.call_with_summary(
            eu_summary,
            "svc.prod.eu-west.quote",
            "EURUSD",
            function_id="prod.quote.eu",
            version_constraint=">=1.0.0",
            ingress=summary_response.ingress,
        )
        assert "region-eu" in str(result_eu)
        await cluster_client.disconnect()


@pytest.mark.asyncio
async def test_summary_cutover_window_propagation(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        global_url = f"ws://127.0.0.1:{ports[0]}"
        region_url = f"ws://127.0.0.1:{ports[1]}"

        window_start = time.time() - 0.5
        window_end = time.time() + 3.0
        policy_rules = (
            NamespacePolicyRule(
                namespace="svc.cutover",
                owners=("region-east",),
                allow_summaries=True,
                export_scopes=("global",),
                cutover_windows=(
                    CutoverWindow(
                        starts_at=window_start,
                        ends_at=window_end,
                    ),
                ),
            ),
        )

        await _start_server(
            test_context,
            port=ports[0],
            name="cutover-global",
            cluster_id="global",
            connect=None,
            discovery_summary_resolver_mode=True,
            discovery_summary_resolver_prune_interval_seconds=0.1,
            federation_permissive=True,
        )
        region_server = await _start_server(
            test_context,
            port=ports[1],
            name="cutover-region",
            cluster_id="region-east",
            connect=global_url,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=False,
            discovery_policy_rules=policy_rules,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.1,
            discovery_summary_export_namespaces=("svc.cutover",),
            discovery_summary_export_scope="global",
            fabric_catalog_ttl_seconds=1.0,
            federation_permissive=True,
            registers=(("svc.cutover.quote", "cutover.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(global_url) as client:
            await _wait_for_summary_exports(region_server, "svc.cutover")
            deadline = time.time() + 6.0
            found = False
            while time.time() < deadline and not found:
                response = await client.summary_query(
                    namespace="svc.cutover",
                    scope="global",
                )
                service_ids = [item.service_id for item in response.items]
                if "svc.cutover.quote" in service_ids:
                    found = True
                    break
                await asyncio.sleep(0.1)

            assert found

            await asyncio.sleep(max(0.0, window_end + 1.2 - time.time()))
            deadline = time.time() + 4.0
            gone = False
            while time.time() < deadline and not gone:
                response = await client.summary_query(
                    namespace="svc.cutover",
                    scope="global",
                )
                service_ids = [item.service_id for item in response.items]
                if "svc.cutover.quote" not in service_ids:
                    gone = True
                    break
                await asyncio.sleep(0.2)

            assert gone


@pytest.mark.asyncio
async def test_summary_watch_emits_summaries(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        server = await _start_server(
            test_context,
            port=ports[0],
            name="summary-watch",
            cluster_id="market",
            connect=None,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.1,
            discovery_summary_export_namespaces=("svc.market",),
            registers=(),
        )

        async with MPREGClientAPI(server_url) as client:
            pubsub = MPREGPubSubClient(base_client=client)
            await pubsub.start()
            queue: asyncio.Queue = asyncio.Queue()

            def on_summary(message):
                queue.put_nowait(message)

            watch_info = await client.summary_watch()
            await pubsub.subscribe(
                patterns=[watch_info.topic],
                callback=on_summary,
                get_backlog=False,
            )

            server.register_command(
                "svc.market.quote_watch",
                _make_named_handler("summary-watch", "quote_watch"),
                ["market"],
                function_id="market.quote_watch",
                version="1.0.0",
            )

            deadline = time.time() + 5.0
            found = False
            while time.time() < deadline:
                try:
                    message = await asyncio.wait_for(
                        queue.get(), timeout=max(0.1, deadline - time.time())
                    )
                except TimeoutError:
                    break
                payload = message.payload
                if not isinstance(payload, dict):
                    continue
                summaries = payload.get("summaries", [])
                if not isinstance(summaries, list):
                    continue
                service_ids = [
                    entry.get("service_id")
                    for entry in summaries
                    if isinstance(entry, dict)
                ]
                if "svc.market.quote_watch" in service_ids:
                    found = True
                    break

            assert found
            await pubsub.stop()


@pytest.mark.asyncio
async def test_summary_watch_respects_policy_owners(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        policy_rules = (
            NamespacePolicyRule(
                namespace="svc.market",
                owners=("market",),
                allow_summaries=True,
            ),
            NamespacePolicyRule(
                namespace="svc.secret",
                owners=("other-cluster",),
                allow_summaries=True,
            ),
        )
        server = await _start_server(
            test_context,
            port=ports[0],
            name="summary-owners",
            cluster_id="market",
            connect=None,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=True,
            discovery_policy_rules=policy_rules,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.1,
            registers=(),
        )

        async with MPREGClientAPI(server_url) as client:
            pubsub = MPREGPubSubClient(base_client=client)
            await pubsub.start()
            queue: asyncio.Queue = asyncio.Queue()

            def on_summary(message):
                queue.put_nowait(message)

            watch_info = await client.summary_watch()
            await pubsub.subscribe(
                patterns=[watch_info.topic],
                callback=on_summary,
                get_backlog=False,
            )

            server.register_command(
                "svc.market.quote",
                _make_named_handler("summary-owners", "quote"),
                ["market"],
                function_id="market.quote",
                version="1.0.0",
            )
            server.register_command(
                "svc.secret.audit",
                _make_named_handler("summary-owners", "audit"),
                ["market"],
                function_id="secret.audit",
                version="1.0.0",
            )

            deadline = time.time() + 5.0
            found_allowed = False
            found_denied = False
            while time.time() < deadline and not found_allowed:
                try:
                    message = await asyncio.wait_for(
                        queue.get(), timeout=max(0.1, deadline - time.time())
                    )
                except TimeoutError:
                    break
                payload = message.payload
                if not isinstance(payload, dict):
                    continue
                summaries = payload.get("summaries", [])
                if not isinstance(summaries, list):
                    continue
                service_ids = [
                    entry.get("service_id")
                    for entry in summaries
                    if isinstance(entry, dict)
                ]
                if "svc.market.quote" in service_ids:
                    found_allowed = True
                if "svc.secret.audit" in service_ids:
                    found_denied = True

            assert found_allowed
            assert not found_denied
            await pubsub.stop()


@pytest.mark.asyncio
async def test_summary_export_cutover_windows(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        future_start = time.time() + 5.0
        policy_rules = (
            NamespacePolicyRule(
                namespace="svc.secret",
                owners=("market",),
                allow_summaries=True,
                cutover_windows=(
                    CutoverWindow(
                        starts_at=future_start,
                        ends_at=future_start + 30.0,
                    ),
                ),
            ),
        )
        server = await _start_server(
            test_context,
            port=ports[0],
            name="summary-cutover",
            cluster_id="market",
            connect=None,
            discovery_policy_enabled=True,
            discovery_policy_default_allow=False,
            discovery_policy_rules=policy_rules,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.2,
            discovery_summary_export_namespaces=("svc.secret",),
            registers=(("svc.secret.audit", "secret.audit", "1.0.0"),),
        )

        deadline = time.time() + 2.0
        exported = None
        while time.time() < deadline:
            exported, _ = server._summary_export_state.summary_for_namespace(
                "svc.secret"
            )
            if exported:
                break
            await asyncio.sleep(0.1)

        assert exported is None

        active_start = time.time() - 1.0
        active_rules = [
            NamespacePolicyRule(
                namespace="svc.secret",
                owners=("market",),
                allow_summaries=True,
                cutover_windows=(
                    CutoverWindow(
                        starts_at=active_start,
                        ends_at=active_start + 30.0,
                    ),
                ),
            ).to_dict()
        ]

        async with MPREGClientAPI(server_url) as client:
            response = await client.namespace_policy_apply(
                rules=active_rules,
                enabled=True,
                default_allow=False,
            )
            assert response.applied is True

        deadline = time.time() + 3.0
        exported = None
        while time.time() < deadline:
            exported, _ = server._summary_export_state.summary_for_namespace(
                "svc.secret"
            )
            if exported:
                break
            await asyncio.sleep(0.1)

        assert exported is not None


@pytest.mark.asyncio
async def test_summary_watch_hold_down_delays_changes(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        server = await _start_server(
            test_context,
            port=ports[0],
            name="summary-hold-down",
            cluster_id="market",
            connect=None,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.2,
            discovery_summary_export_namespaces=("svc.market",),
            discovery_summary_export_hold_down_seconds=1.0,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )

        async with MPREGClientAPI(server_url) as client:
            pubsub = MPREGPubSubClient(base_client=client)
            await pubsub.start()
            queue: asyncio.Queue = asyncio.Queue()

            def on_summary(message):
                queue.put_nowait(message)

            watch_info = await client.summary_watch()
            await pubsub.subscribe(
                patterns=[watch_info.topic],
                callback=on_summary,
                get_backlog=False,
            )

            deadline = time.time() + 5.0
            found_initial = False
            while time.time() < deadline and not found_initial:
                try:
                    message = await asyncio.wait_for(
                        queue.get(), timeout=max(0.1, deadline - time.time())
                    )
                except TimeoutError:
                    break
                payload = message.payload
                if not isinstance(payload, dict):
                    continue
                summaries = payload.get("summaries", [])
                if not isinstance(summaries, list):
                    continue
                service_ids = [
                    entry.get("service_id")
                    for entry in summaries
                    if isinstance(entry, dict)
                    and entry.get("namespace") == "svc.market"
                ]
                if "svc.market.quote" in service_ids and len(service_ids) == 1:
                    found_initial = True

            assert found_initial

            server.register_command(
                "svc.market.indicator",
                _make_named_handler("summary-hold-down", "indicator"),
                ["market"],
                function_id="market.indicator",
                version="1.0.0",
            )

            deadline = time.time() + 0.8
            found_indicator = False
            while time.time() < deadline and not found_indicator:
                try:
                    message = await asyncio.wait_for(
                        queue.get(), timeout=max(0.1, deadline - time.time())
                    )
                except TimeoutError:
                    break
                payload = message.payload
                if not isinstance(payload, dict):
                    continue
                summaries = payload.get("summaries", [])
                if not isinstance(summaries, list):
                    continue
                service_ids = [
                    entry.get("service_id")
                    for entry in summaries
                    if isinstance(entry, dict)
                    and entry.get("namespace") == "svc.market"
                ]
                if "svc.market.indicator" in service_ids:
                    found_indicator = True

            assert not found_indicator

            deadline = time.time() + 3.0
            found_indicator = False
            while time.time() < deadline and not found_indicator:
                try:
                    message = await asyncio.wait_for(
                        queue.get(), timeout=max(0.1, deadline - time.time())
                    )
                except TimeoutError:
                    break
                payload = message.payload
                if not isinstance(payload, dict):
                    continue
                summaries = payload.get("summaries", [])
                if not isinstance(summaries, list):
                    continue
                service_ids = [
                    entry.get("service_id")
                    for entry in summaries
                    if isinstance(entry, dict)
                    and entry.get("namespace") == "svc.market"
                ]
                if "svc.market.indicator" in service_ids:
                    found_indicator = True

            assert found_indicator
            await pubsub.stop()


@pytest.mark.asyncio
async def test_summary_watch_store_forward_backlog(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        server = await _start_server(
            test_context,
            port=ports[0],
            name="summary-store-forward",
            cluster_id="market",
            connect=None,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=5.0,
            discovery_summary_export_store_forward_seconds=30.0,
            discovery_summary_export_store_forward_max_messages=10,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )

        deadline = time.time() + 2.0
        while time.time() < deadline:
            if server._summary_export_state.store_forward_size() > 0:
                break
            await asyncio.sleep(0.1)

        assert server._summary_export_state.store_forward_size() > 0
        backlog = server._summary_export_state.store_forward_backlog(
            now=time.time(), max_age_seconds=20.0
        )
        assert backlog
        topics = {message.topic for message in backlog}
        assert "mpreg.discovery.summary" in topics, f"backlog topics: {topics}"

        async with MPREGClientAPI(server_url) as client:
            pubsub = MPREGPubSubClient(base_client=client)
            await pubsub.start()
            queue: asyncio.Queue = asyncio.Queue()

            def on_summary(message):
                queue.put_nowait(message)

            watch_info = await client.summary_watch()
            await pubsub.subscribe(
                patterns=[watch_info.topic],
                callback=on_summary,
                get_backlog=True,
                backlog_seconds=20,
            )

            message = await asyncio.wait_for(queue.get(), timeout=2.0)
            payload = message.payload
            assert isinstance(payload, dict)
            summaries = payload.get("summaries", [])
            assert isinstance(summaries, list)
            service_ids = [
                entry.get("service_id")
                for entry in summaries
                if isinstance(entry, dict)
            ]
            assert "svc.market.quote" in service_ids
            await pubsub.stop()


@pytest.mark.asyncio
async def test_summary_watch_namespace_topic_filters(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        server_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="summary-ns",
            cluster_id="market",
            connect=None,
            discovery_summary_export_enabled=True,
            discovery_summary_export_interval_seconds=0.2,
            registers=(
                ("svc.market.quote", "market.quote", "1.0.0"),
                ("svc.other.quote", "other.quote", "1.0.0"),
            ),
        )

        async with MPREGClientAPI(server_url) as client:
            pubsub = MPREGPubSubClient(base_client=client)
            await pubsub.start()
            queue: asyncio.Queue = asyncio.Queue()

            def on_summary(message):
                queue.put_nowait(message)

            watch_info = await client.summary_watch(namespace="svc.market")
            await pubsub.subscribe(
                patterns=[watch_info.topic],
                callback=on_summary,
                get_backlog=False,
            )

            message = await asyncio.wait_for(queue.get(), timeout=5.0)
            payload = message.payload
            assert isinstance(payload, dict)
            summaries = payload.get("summaries", [])
            assert isinstance(summaries, list)
            namespaces = {
                entry.get("namespace") for entry in summaries if isinstance(entry, dict)
            }
            assert namespaces == {"svc.market"}
            await pubsub.stop()


@pytest.mark.asyncio
async def test_rpc_discovery_endpoints(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="rpc-hub",
            cluster_id="market",
            connect=None,
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="rpc-node",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.market.indicator", "market.indicator", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.market", expected_count=2
            )

            listing = await client.rpc_list(
                namespace="svc.market",
                scope="zone",
                limit=10,
            )
            items = listing.items
            assert len(items) >= 2
            assert all(item.summary for item in items)
            assert all(item.spec_digest for item in items)

            details = await client.rpc_describe(
                mode="scatter",
                namespace="svc.market",
                scope="zone",
            )
            detail_items = details.items
            assert len(detail_items) >= 1
            assert all(item.spec for item in detail_items)
            errors = details.errors
            if errors:
                assert all(entry.node_id for entry in errors)

            report = await client.rpc_report(namespace="svc.market", scope="zone")
            assert report.total_functions >= 2


@pytest.mark.asyncio
async def test_rpc_describe_auto_uses_gossiped_specs(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="rpc-hub",
            cluster_id="market",
            connect=None,
            rpc_spec_gossip_mode="full",
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="rpc-node",
            cluster_id="market",
            connect=hub_url,
            rpc_spec_gossip_mode="full",
            registers=(("svc.market.indicator", "market.indicator", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.market", expected_count=2
            )

            details = await client.rpc_describe(
                mode="auto",
                namespace="svc.market",
                scope="zone",
                detail_level="full",
            )
            assert len(details.items) >= 2
            assert not details.errors
            assert all(item.spec for item in details.items)

            summary_only = await client.rpc_describe(
                mode="auto",
                namespace="svc.market",
                scope="zone",
                detail_level="summary",
            )
            assert len(summary_only.items) >= 2
            assert all(item.spec is None for item in summary_only.items)


@pytest.mark.asyncio
async def test_rpc_describe_auto_scatter_fills_missing_specs(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="rpc-hub",
            cluster_id="market",
            connect=None,
            rpc_spec_gossip_mode="summary",
            registers=(("svc.market.quote", "market.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="rpc-node",
            cluster_id="market",
            connect=hub_url,
            rpc_spec_gossip_mode="summary",
            registers=(("svc.market.indicator", "market.indicator", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.market", expected_count=2
            )

            details = await client.rpc_describe(
                mode="auto",
                namespace="svc.market",
                scope="zone",
                detail_level="full",
            )
            assert len(details.items) >= 2
            assert not details.errors
            assert all(item.spec for item in details.items)


@pytest.mark.asyncio
async def test_rpc_describe_catalog_filters_by_identity_and_digest(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="rpc-filter-hub",
            cluster_id="market",
            connect=None,
            registers=(
                ("svc.filter.alpha", "filter.alpha", "1.0.0"),
                ("svc.filter.beta", "filter.beta", "1.0.0"),
            ),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.filter", expected_count=2
            )

            listing = await client.rpc_list(namespace="svc.filter")
            digest_map = {
                item.identity.name: item.spec_digest for item in listing.items
            }
            alpha_digest = digest_map.get("svc.filter.alpha")
            assert alpha_digest

            by_name = await client.rpc_describe(
                mode="catalog",
                function_names=("svc.filter.alpha",),
            )
            assert len(by_name.items) == 1
            assert by_name.items[0].identity.name == "svc.filter.alpha"

            by_id = await client.rpc_describe(
                mode="catalog",
                function_ids=("filter.beta",),
            )
            assert len(by_id.items) == 1
            assert by_id.items[0].identity.function_id == "filter.beta"

            by_digest = await client.rpc_describe(
                mode="catalog",
                spec_digests=(alpha_digest,),
            )
            assert len(by_digest.items) == 1
            assert by_digest.items[0].spec_digest == alpha_digest


@pytest.mark.asyncio
async def test_namespace_filter_boundary_in_discovery(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="rpc-namespace-boundary",
            cluster_id="market",
            connect=None,
            registers=(
                ("svc.alpha.one", "alpha.one", "1.0.0"),
                ("svc2.alpha.two", "alpha.two", "1.0.0"),
            ),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_catalog_functions(client, namespace="svc", expected_count=1)

            listing = await client.rpc_list(namespace="svc")
            names = {item.identity.name for item in listing.items}
            assert "svc.alpha.one" in names
            assert not any(name.startswith("svc2.") for name in names)

            summary = await client.summary_query(namespace="svc")
            namespaces = {item.namespace for item in summary.items}
            assert "svc.alpha" in namespaces
            assert not any(value.startswith("svc2") for value in namespaces)


@pytest.mark.asyncio
async def test_rpc_discovery_federation_auto_scatter(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        await _start_server(
            test_context,
            port=ports[0],
            name="rpc-fed-alpha",
            cluster_id="alpha",
            connect=None,
            federation_permissive=True,
            rpc_spec_gossip_mode="summary",
            registers=(("svc.fed.quote", "fed.quote", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="rpc-fed-beta",
            cluster_id="beta",
            connect=hub_url,
            federation_permissive=True,
            rpc_spec_gossip_mode="summary",
            registers=(("svc.fed.indicator", "fed.indicator", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.fed", expected_count=2
            )

            listing = await client.rpc_list(namespace="svc.fed")
            cluster_ids = {item.cluster_id for item in listing.items}
            assert "alpha" in cluster_ids
            assert "beta" in cluster_ids

            details = await client.rpc_describe(
                mode="auto",
                namespace="svc.fed",
                detail_level="full",
            )
            assert len(details.items) >= 2
            assert not details.errors
            assert all(item.spec for item in details.items)
            assert any(item.cluster_id == "beta" for item in details.items)


@pytest.mark.asyncio
async def test_local_only_control_plane_commands_execute_locally(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        hub = await _start_server(
            test_context,
            port=ports[0],
            name="rpc-hub",
            cluster_id="market",
            connect=None,
            registers=(("svc.local.alpha", "local.alpha", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="rpc-node",
            cluster_id="market",
            connect=hub_url,
            registers=(("svc.remote.beta", "remote.beta", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_catalog_functions(client, namespace="svc", expected_count=2)
            baseline = hub.cluster._remote_command_stats.total

            await client.cluster_map_v2()
            assert hub.cluster._remote_command_stats.total == baseline
            await client.cluster_map()
            assert hub.cluster._remote_command_stats.total == baseline

            await client.catalog_query(entry_type="functions", namespace="svc")
            assert hub.cluster._remote_command_stats.total == baseline
            await client.catalog_watch()
            assert hub.cluster._remote_command_stats.total == baseline

            await client.list_peers()
            assert hub.cluster._remote_command_stats.total == baseline

            await client.discovery_access_audit(limit=5)
            assert hub.cluster._remote_command_stats.total == baseline

            await client.rpc_list(namespace="svc")
            assert hub.cluster._remote_command_stats.total == baseline
            await client.rpc_describe(namespace="svc", mode="local")
            assert hub.cluster._remote_command_stats.total == baseline
            await client.rpc_report(namespace="svc")
            assert hub.cluster._remote_command_stats.total == baseline

            await client.call("rpc_describe_local", {"namespace": "svc"})
            assert hub.cluster._remote_command_stats.total == baseline

            await client.summary_query(namespace="svc")
            assert hub.cluster._remote_command_stats.total == baseline
            await client.summary_watch(namespace="svc")
            assert hub.cluster._remote_command_stats.total == baseline

            await client.resolver_cache_stats()
            assert hub.cluster._remote_command_stats.total == baseline
            await client.resolver_resync()
            assert hub.cluster._remote_command_stats.total == baseline

            rules = [
                {
                    "namespace": "svc.secret",
                    "visibility": ["secure-cluster"],
                    "policy_version": "v1",
                }
            ]
            await client.namespace_policy_validate(rules=rules, actor="tester")
            assert hub.cluster._remote_command_stats.total == baseline
            await client.namespace_policy_apply(
                rules=rules,
                enabled=False,
                default_allow=True,
                actor="tester",
            )
            assert hub.cluster._remote_command_stats.total == baseline
            await client.namespace_policy_export()
            assert hub.cluster._remote_command_stats.total == baseline
            await client.namespace_policy_audit(limit=5)
            assert hub.cluster._remote_command_stats.total == baseline
            await client.namespace_status(namespace="svc.secret")
            assert hub.cluster._remote_command_stats.total == baseline

            await client.call("svc.remote.beta", "ping")
            assert hub.cluster._remote_command_stats.total > baseline


@pytest.mark.asyncio
async def test_target_cluster_routes_remote_command(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(2, "servers") as ports:
        hub_url = f"ws://127.0.0.1:{ports[0]}"
        hub = await _start_server(
            test_context,
            port=ports[0],
            name="hub-alpha",
            cluster_id="alpha",
            connect=None,
            federation_permissive=True,
            registers=(("svc.local.alpha", "local.alpha", "1.0.0"),),
        )
        await _start_server(
            test_context,
            port=ports[1],
            name="node-beta",
            cluster_id="beta",
            connect=hub_url,
            federation_permissive=True,
            registers=(("svc.remote.echo", "remote.echo", "1.0.0"),),
        )

        async with MPREGClientAPI(hub_url) as client:
            await _wait_for_catalog_functions(
                client, namespace="svc.remote", expected_count=1
            )
            baseline = hub.cluster._remote_command_stats.total
            result = await client.call("svc.remote.echo", "ping", target_cluster="beta")
            assert isinstance(result, str)
            assert result.endswith(":ping")
            assert hub.cluster._remote_command_stats.total > baseline
