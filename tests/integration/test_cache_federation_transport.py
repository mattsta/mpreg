import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import CacheMetadata, GlobalCacheEntry, GlobalCacheKey
from mpreg.core.port_allocator import allocate_port_range
from mpreg.fabric.cache_federation import CacheOperationType
from mpreg.fabric.federation_config import create_explicit_bridging_config
from mpreg.fabric.route_control import RouteDestination
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


@pytest.mark.asyncio
async def test_cache_federation_transport_roundtrip(
    test_context: AsyncTestContext,
) -> None:
    ports = allocate_port_range(2, "servers")
    settings_a = MPREGSettings(
        host="127.0.0.1",
        port=ports[0],
        name="Cache A",
        cluster_id="cache-cluster",
        gossip_interval=0.2,
        monitoring_enabled=False,
        enable_default_cache=True,
    )
    settings_b = MPREGSettings(
        host="127.0.0.1",
        port=ports[1],
        name="Cache B",
        cluster_id="cache-cluster",
        connect=f"ws://127.0.0.1:{ports[0]}",
        gossip_interval=0.2,
        monitoring_enabled=False,
        enable_default_cache=True,
    )

    server_a = MPREGServer(settings=settings_a)
    server_b = MPREGServer(settings=settings_b)
    test_context.servers.extend([server_a, server_b])

    task_a = asyncio.create_task(server_a.server())
    task_b = asyncio.create_task(server_b.server())
    test_context.tasks.extend([task_a, task_b])

    await asyncio.sleep(1.5)

    transport_a = server_a._cache_fabric_transport
    transport_b = server_b._cache_fabric_transport
    assert transport_a is not None
    assert transport_b is not None
    assert server_a._cache_fabric_protocol is not None
    assert server_b._cache_fabric_protocol is not None

    deadline = time.time() + 5.0
    while time.time() < deadline:
        if server_b.cluster.local_url in transport_a.peer_ids():
            break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("cache federation peer did not register in catalog")

    key = GlobalCacheKey(namespace="cache", identifier="digest-key")
    entry = GlobalCacheEntry(
        key=key,
        value={"payload": "digest"},
        metadata=CacheMetadata(created_by="node-b"),
    )
    server_b._cache_fabric_protocol.cache_entries[str(key)] = entry

    digest = await transport_a.fetch_digest(server_b.cluster.local_url)
    assert digest is not None
    assert str(key) in digest.entries

    fetched_entry = await transport_a.fetch_entry(server_b.cluster.local_url, key)
    assert fetched_entry is not None
    assert fetched_entry.key == entry.key

    put_key = GlobalCacheKey(namespace="cache", identifier="put-key")
    metadata = CacheMetadata(created_by="node-a")
    operation_id = await server_a._cache_fabric_protocol.propagate_cache_operation(
        CacheOperationType.PUT,
        put_key,
        {"payload": "value"},
        metadata,
    )
    operation = server_a._cache_fabric_protocol.cache_operations[operation_id]
    await transport_a.send_operation(server_b.cluster.local_url, operation)

    await asyncio.sleep(0.5)

    assert str(put_key) in server_b._cache_fabric_protocol.cache_entries


@pytest.mark.asyncio
async def test_cache_federation_multi_hop_fetch(
    test_context: AsyncTestContext,
) -> None:
    ports = allocate_port_range(3, "servers")
    settings_a = MPREGSettings(
        host="127.0.0.1",
        port=ports[0],
        name="Cache Chain A",
        cluster_id="cache-chain-a",
        gossip_interval=0.3,
        monitoring_enabled=False,
        enable_default_cache=True,
        enable_cache_federation=True,
        federation_config=create_explicit_bridging_config(
            "cache-chain-a", {"cache-chain-b"}
        ),
    )
    settings_b = MPREGSettings(
        host="127.0.0.1",
        port=ports[1],
        name="Cache Chain B",
        cluster_id="cache-chain-b",
        connect=f"ws://127.0.0.1:{ports[0]}",
        gossip_interval=0.3,
        monitoring_enabled=False,
        enable_default_cache=True,
        enable_cache_federation=True,
        federation_config=create_explicit_bridging_config(
            "cache-chain-b", {"cache-chain-a", "cache-chain-c"}
        ),
    )
    settings_c = MPREGSettings(
        host="127.0.0.1",
        port=ports[2],
        name="Cache Chain C",
        cluster_id="cache-chain-c",
        connect=f"ws://127.0.0.1:{ports[1]}",
        gossip_interval=0.3,
        monitoring_enabled=False,
        enable_default_cache=True,
        enable_cache_federation=True,
        federation_config=create_explicit_bridging_config(
            "cache-chain-c", {"cache-chain-b"}
        ),
    )

    server_a = MPREGServer(settings=settings_a)
    server_b = MPREGServer(settings=settings_b)
    server_c = MPREGServer(settings=settings_c)
    test_context.servers.extend([server_a, server_b, server_c])

    task_a = asyncio.create_task(server_a.server())
    task_b = asyncio.create_task(server_b.server())
    task_c = asyncio.create_task(server_c.server())
    test_context.tasks.extend([task_a, task_b, task_c])

    await asyncio.sleep(2.0)

    transport_c = server_c._cache_fabric_transport
    protocol_a = server_a._cache_fabric_protocol
    assert transport_c is not None
    assert protocol_a is not None

    deadline = time.time() + 8.0
    while time.time() < deadline:
        control_plane = server_c._fabric_control_plane
        if control_plane and control_plane.route_table.select_route(
            RouteDestination(cluster_id="cache-chain-a")
        ):
            break
        await asyncio.sleep(0.2)
    else:
        raise AssertionError("route table did not learn cache-chain-a path")

    deadline = time.time() + 8.0
    while time.time() < deadline:
        if (
            server_c.cluster.cluster_id_for_node_url(server_a.cluster.local_url)
            == "cache-chain-a"
        ):
            break
        await asyncio.sleep(0.2)
    else:
        raise AssertionError("catalog did not learn cache-chain-a node mapping")

    deadline = time.time() + 8.0
    while time.time() < deadline:
        if (
            server_a.cluster.cluster_id_for_node_url(server_c.cluster.local_url)
            == "cache-chain-c"
        ):
            break
        await asyncio.sleep(0.2)
    else:
        raise AssertionError("catalog did not learn cache-chain-c node mapping")

    deadline = time.time() + 8.0
    while time.time() < deadline:
        control_plane = server_a._fabric_control_plane
        if control_plane and control_plane.route_table.select_route(
            RouteDestination(cluster_id="cache-chain-c")
        ):
            break
        await asyncio.sleep(0.2)
    else:
        raise AssertionError("route table did not learn cache-chain-c path")

    key = GlobalCacheKey(namespace="cache", identifier="chain-key")
    entry = GlobalCacheEntry(
        key=key,
        value={"payload": "chain"},
        metadata=CacheMetadata(created_by="node-a"),
    )
    protocol_a.cache_entries[str(key)] = entry

    digest = await transport_c.fetch_digest(server_a.cluster.local_url)
    assert digest is not None
    assert str(key) in digest.entries

    fetched_entry = await transport_c.fetch_entry(server_a.cluster.local_url, key)
    assert fetched_entry is not None
    assert fetched_entry.key == entry.key
