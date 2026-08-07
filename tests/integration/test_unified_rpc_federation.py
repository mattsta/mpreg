"""
End-to-end tests for unified RPC federation routing.

These tests validate target_cluster routing with unified routing enabled
using live MPREGServer instances and the port allocator.
"""

import asyncio
import time

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import MPREGException
from mpreg.datastructures.function_identity import FunctionSelector
from mpreg.fabric.federation_config import (
    create_explicit_bridging_config,
    create_permissive_bridging_config,
)
from mpreg.fabric.index import FunctionQuery
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import wait_for_condition


def _function_visible(server: MPREGServer, name: str, cluster_id: str) -> bool:
    if not server._fabric_control_plane:
        return False
    query = FunctionQuery(selector=FunctionSelector(name=name), cluster_id=cluster_id)
    return bool(
        server._fabric_control_plane.index.find_functions(query, now=time.time())
    )


def _peer_node_ids(server: MPREGServer) -> set[str]:
    directory = server._peer_directory
    if not directory:
        return set()
    return {node.node_id for node in directory.nodes()}


class TestUnifiedRPCFederation:
    async def test_response_reroute_when_reply_path_breaks(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        port_a, port_b, port_c, port_d = server_cluster_ports[:4]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Federated-Reply-A",
            cluster_id="cluster-a",
            resources={"cpu"},
            peers=[
                f"ws://127.0.0.1:{port_b}",
                f"ws://127.0.0.1:{port_d}",
            ],
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-a", {"cluster-b", "cluster-d"}
            ),
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Federated-Reply-B",
            cluster_id="cluster-b",
            resources={"cpu"},
            peers=[f"ws://127.0.0.1:{port_c}"],
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-b", {"cluster-a", "cluster-c"}
            ),
        )
        settings_c = MPREGSettings(
            host="127.0.0.1",
            port=port_c,
            name="Federated-Reply-C",
            cluster_id="cluster-c",
            resources={"cpu"},
            peers=None,
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-c", {"cluster-b", "cluster-d"}
            ),
        )
        settings_d = MPREGSettings(
            host="127.0.0.1",
            port=port_d,
            name="Federated-Reply-D",
            cluster_id="cluster-d",
            resources={"cpu"},
            peers=[f"ws://127.0.0.1:{port_c}"],
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-d", {"cluster-a", "cluster-c"}
            ),
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        server_c = MPREGServer(settings=settings_c)
        server_d = MPREGServer(settings=settings_d)
        test_context.servers.extend([server_a, server_b, server_c, server_d])

        task_c = asyncio.create_task(server_c.server())
        test_context.tasks.append(task_c)
        await asyncio.sleep(0.4)

        task_d = asyncio.create_task(server_d.server())
        test_context.tasks.append(task_d)
        await asyncio.sleep(0.4)

        task_b = asyncio.create_task(server_b.server())
        test_context.tasks.append(task_b)
        await asyncio.sleep(0.6)

        task_a = asyncio.create_task(server_a.server())
        test_context.tasks.append(task_a)
        await asyncio.sleep(1.0)

        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_b}" in _peer_node_ids(server_a),
            timeout=5.0,
            error_message="cluster-a did not register cluster-b",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_d}" in _peer_node_ids(server_a),
            timeout=5.0,
            error_message="cluster-a did not register cluster-d",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_c}" in _peer_node_ids(server_b),
            timeout=5.0,
            error_message="cluster-b did not register cluster-c",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_c}" in _peer_node_ids(server_d),
            timeout=5.0,
            error_message="cluster-d did not register cluster-c",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_a}" in _peer_node_ids(server_d),
            timeout=5.0,
            error_message="cluster-d did not register cluster-a",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_d}" in _peer_node_ids(server_c),
            timeout=5.0,
            error_message="cluster-c did not register cluster-d",
        )

        request_started = asyncio.Event()
        release_response = asyncio.Event()

        async def cluster_echo(payload: str) -> str:
            request_started.set()
            await release_response.wait()
            return f"cluster-c:{payload}"

        server_c.register_command("cluster_echo", cluster_echo, ["cpu"])
        await wait_for_condition(
            lambda: _function_visible(server_a, "cluster_echo", "cluster-c"),
            timeout=6.0,
            error_message="cluster-a did not see cluster-c function",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
        test_context.clients.append(client)
        await client.connect()

        call_task = asyncio.create_task(
            client.call("cluster_echo", "payload", target_cluster="cluster-c")
        )

        await asyncio.wait_for(request_started.wait(), timeout=5.0)
        await server_b.shutdown_async()
        test_context.servers.remove(server_b)

        release_response.set()
        result = await asyncio.wait_for(call_task, timeout=8.0)
        assert result == "cluster-c:payload"

    async def test_target_cluster_routing_selects_remote(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        port_a, port_b = server_cluster_ports[:2]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Unified-RPC-Cluster-A",
            cluster_id="cluster-a",
            resources={"cpu"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
            federation_config=create_permissive_bridging_config("cluster-a"),
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Unified-RPC-Cluster-B",
            cluster_id="cluster-b",
            resources={"cpu"},
            peers=None,
            connect=f"ws://127.0.0.1:{port_a}",
            advertised_urls=None,
            gossip_interval=1.0,
            federation_config=create_permissive_bridging_config("cluster-b"),
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        test_context.servers.extend([server_a, server_b])

        task_a = asyncio.create_task(server_a.server())
        test_context.tasks.append(task_a)
        await asyncio.sleep(0.4)

        task_b = asyncio.create_task(server_b.server())
        test_context.tasks.append(task_b)
        await asyncio.sleep(1.0)

        def cluster_echo_a(payload: str) -> str:
            return f"cluster-a:{payload}"

        def cluster_echo_b(payload: str) -> str:
            return f"cluster-b:{payload}"

        server_a.register_command("cluster_echo", cluster_echo_a, ["cpu"])
        server_b.register_command("cluster_echo", cluster_echo_b, ["cpu"])
        await wait_for_condition(
            lambda: _function_visible(server_a, "cluster_echo", "cluster-b"),
            timeout=5.0,
            error_message="cluster-a did not see cluster-b function",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
        test_context.clients.append(client)
        await client.connect()

        remote_result = await client.call(
            "cluster_echo", "payload", target_cluster="cluster-b"
        )
        assert remote_result == "cluster-b:payload"

        local_result = await client.call(
            "cluster_echo", "payload", target_cluster="cluster-a"
        )
        assert local_result == "cluster-a:payload"

    async def test_target_cluster_missing_fails_cleanly(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        port_a = server_cluster_ports[0]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Unified-RPC-Cluster-A",
            cluster_id="cluster-a",
            resources={"cpu"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
            federation_config=create_permissive_bridging_config("cluster-a"),
        )

        server_a = MPREGServer(settings=settings_a)
        test_context.servers.append(server_a)

        task_a = asyncio.create_task(server_a.server())
        test_context.tasks.append(task_a)
        await asyncio.sleep(0.6)

        def cluster_echo(payload: str) -> str:
            return f"cluster-a:{payload}"

        server_a.register_command("cluster_echo", cluster_echo, ["cpu"])
        await asyncio.sleep(0.6)

        client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
        test_context.clients.append(client)
        await client.connect()

        with pytest.raises(MPREGException):
            await client.call(
                "cluster_echo", "payload", target_cluster="missing-cluster"
            )

    async def test_multi_hop_federated_rpc_forwarding(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        port_a, port_b, port_c = server_cluster_ports[:3]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Federated-RPC-A",
            cluster_id="cluster-a",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_b}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-a", {"cluster-b"}
            ),
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Federated-RPC-B",
            cluster_id="cluster-b",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_c}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-b", {"cluster-a", "cluster-c"}
            ),
        )
        settings_c = MPREGSettings(
            host="127.0.0.1",
            port=port_c,
            name="Federated-RPC-C",
            cluster_id="cluster-c",
            resources={"cpu"},
            connect=None,
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-c", {"cluster-b"}
            ),
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        server_c = MPREGServer(settings=settings_c)
        test_context.servers.extend([server_a, server_b, server_c])

        task_c = asyncio.create_task(server_c.server())
        test_context.tasks.append(task_c)
        await asyncio.sleep(0.4)

        task_b = asyncio.create_task(server_b.server())
        test_context.tasks.append(task_b)
        await asyncio.sleep(0.6)

        task_a = asyncio.create_task(server_a.server())
        test_context.tasks.append(task_a)
        await asyncio.sleep(1.0)

        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_b}" in _peer_node_ids(server_a),
            timeout=5.0,
            error_message="cluster-a did not register cluster-b",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_c}" in _peer_node_ids(server_b),
            timeout=5.0,
            error_message="cluster-b did not register cluster-c",
        )

        def cluster_echo(payload: str) -> str:
            return f"cluster-c:{payload}"

        server_c.register_command("cluster_echo", cluster_echo, ["cpu"])
        await wait_for_condition(
            lambda: _function_visible(server_a, "cluster_echo", "cluster-c"),
            timeout=5.0,
            error_message="cluster-a did not see cluster-c function",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
        test_context.clients.append(client)
        await client.connect()

        result = await client.call(
            "cluster_echo", "payload", target_cluster="cluster-c"
        )
        assert result == "cluster-c:payload"

    async def test_target_cluster_direct_without_unified_or_mesh(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        port_a, port_b = server_cluster_ports[:2]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Direct-RPC-A",
            cluster_id="cluster-a",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_b}",
            gossip_interval=1.0,
            fabric_routing_enabled=False,
            federation_config=create_permissive_bridging_config("cluster-a"),
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Direct-RPC-B",
            cluster_id="cluster-b",
            resources={"cpu"},
            connect=None,
            gossip_interval=1.0,
            fabric_routing_enabled=False,
            federation_config=create_permissive_bridging_config("cluster-b"),
        )

        server_b = MPREGServer(settings=settings_b)
        server_a = MPREGServer(settings=settings_a)
        test_context.servers.extend([server_a, server_b])

        task_b = asyncio.create_task(server_b.server())
        test_context.tasks.append(task_b)
        await asyncio.sleep(0.4)

        task_a = asyncio.create_task(server_a.server())
        test_context.tasks.append(task_a)
        await asyncio.sleep(1.0)

        def cluster_echo(payload: str) -> str:
            return f"cluster-b:{payload}"

        server_b.register_command("cluster_echo", cluster_echo, ["cpu"])
        await wait_for_condition(
            lambda: _function_visible(server_a, "cluster_echo", "cluster-b"),
            timeout=5.0,
            error_message="cluster-a did not see cluster-b function",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
        test_context.clients.append(client)
        await client.connect()

        result = await client.call(
            "cluster_echo", "payload", target_cluster="cluster-b"
        )
        assert result == "cluster-b:payload"

    async def test_federated_rpc_hop_budget_exhausted(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        port_a, port_b, port_c = server_cluster_ports[:3]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Federated-RPC-A",
            cluster_id="cluster-a",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_b}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            fabric_routing_max_hops=1,
            federation_config=create_explicit_bridging_config(
                "cluster-a", {"cluster-b"}
            ),
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Federated-RPC-B",
            cluster_id="cluster-b",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_c}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-b", {"cluster-a", "cluster-c"}
            ),
        )
        settings_c = MPREGSettings(
            host="127.0.0.1",
            port=port_c,
            name="Federated-RPC-C",
            cluster_id="cluster-c",
            resources={"cpu"},
            connect=None,
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-c", {"cluster-b"}
            ),
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        server_c = MPREGServer(settings=settings_c)
        test_context.servers.extend([server_a, server_b, server_c])

        task_c = asyncio.create_task(server_c.server())
        test_context.tasks.append(task_c)
        await asyncio.sleep(0.4)

        task_b = asyncio.create_task(server_b.server())
        test_context.tasks.append(task_b)
        await asyncio.sleep(0.6)

        task_a = asyncio.create_task(server_a.server())
        test_context.tasks.append(task_a)
        await asyncio.sleep(1.0)

        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_b}" in _peer_node_ids(server_a),
            timeout=5.0,
            error_message="cluster-a did not register cluster-b",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_c}" in _peer_node_ids(server_b),
            timeout=5.0,
            error_message="cluster-b did not register cluster-c",
        )

        def cluster_echo(payload: str) -> str:
            return f"cluster-c:{payload}"

        server_c.register_command("cluster_echo", cluster_echo, ["cpu"])
        await wait_for_condition(
            lambda: _function_visible(server_a, "cluster_echo", "cluster-c"),
            timeout=5.0,
            error_message="cluster-a did not see cluster-c function",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
        test_context.clients.append(client)
        await client.connect()

        result = await client.call(
            "cluster_echo", "payload", target_cluster="cluster-c"
        )
        assert result["error"] == "fabric_forward_failed"
        assert result["reason"] == "hop_budget_exhausted"
        assert result["target_cluster"] == "cluster-c"

    async def test_federated_rpc_multi_hop_with_mesh_enabled(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        port_a, port_b, port_c = server_cluster_ports[:3]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Federated-Mesh-A",
            cluster_id="cluster-a",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_b}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-a", {"cluster-b"}
            ),
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Federated-Mesh-B",
            cluster_id="cluster-b",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_c}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-b", {"cluster-a", "cluster-c"}
            ),
        )
        settings_c = MPREGSettings(
            host="127.0.0.1",
            port=port_c,
            name="Federated-Mesh-C",
            cluster_id="cluster-c",
            resources={"cpu"},
            connect=None,
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "cluster-c", {"cluster-b"}
            ),
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        server_c = MPREGServer(settings=settings_c)
        test_context.servers.extend([server_a, server_b, server_c])

        task_c = asyncio.create_task(server_c.server())
        test_context.tasks.append(task_c)
        await asyncio.sleep(0.4)

        task_b = asyncio.create_task(server_b.server())
        test_context.tasks.append(task_b)
        await asyncio.sleep(0.6)

        task_a = asyncio.create_task(server_a.server())
        test_context.tasks.append(task_a)
        await asyncio.sleep(1.0)

        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_b}" in _peer_node_ids(server_a),
            timeout=5.0,
            error_message="cluster-a did not register cluster-b",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_c}" in _peer_node_ids(server_b),
            timeout=5.0,
            error_message="cluster-b did not register cluster-c",
        )

        def cluster_echo(payload: str) -> str:
            return f"cluster-c:{payload}"

        server_c.register_command("cluster_echo", cluster_echo, ["cpu"])
        await wait_for_condition(
            lambda: _function_visible(server_a, "cluster_echo", "cluster-c"),
            timeout=5.0,
            error_message="cluster-a did not see cluster-c function",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
        test_context.clients.append(client)
        await client.connect()

        result = await client.call(
            "cluster_echo", "payload", target_cluster="cluster-c"
        )
        assert result == "cluster-c:payload"

    async def test_federated_rpc_four_cluster_chain(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        port_a, port_b, port_c, port_d = server_cluster_ports[:4]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Federated-Chain-A",
            cluster_id="chain-a",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_b}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config("chain-a", {"chain-b"}),
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Federated-Chain-B",
            cluster_id="chain-b",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_c}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "chain-b", {"chain-a", "chain-c"}
            ),
        )
        settings_c = MPREGSettings(
            host="127.0.0.1",
            port=port_c,
            name="Federated-Chain-C",
            cluster_id="chain-c",
            resources={"cpu"},
            connect=f"ws://127.0.0.1:{port_d}",
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config(
                "chain-c", {"chain-b", "chain-d"}
            ),
        )
        settings_d = MPREGSettings(
            host="127.0.0.1",
            port=port_d,
            name="Federated-Chain-D",
            cluster_id="chain-d",
            resources={"cpu"},
            connect=None,
            gossip_interval=1.0,
            fabric_routing_enabled=True,
            federation_config=create_explicit_bridging_config("chain-d", {"chain-c"}),
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        server_c = MPREGServer(settings=settings_c)
        server_d = MPREGServer(settings=settings_d)
        test_context.servers.extend([server_a, server_b, server_c, server_d])

        task_d = asyncio.create_task(server_d.server())
        test_context.tasks.append(task_d)
        await asyncio.sleep(0.4)

        task_c = asyncio.create_task(server_c.server())
        test_context.tasks.append(task_c)
        await asyncio.sleep(0.4)

        task_b = asyncio.create_task(server_b.server())
        test_context.tasks.append(task_b)
        await asyncio.sleep(0.6)

        task_a = asyncio.create_task(server_a.server())
        test_context.tasks.append(task_a)
        await asyncio.sleep(1.0)

        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_b}" in _peer_node_ids(server_a),
            timeout=5.0,
            error_message="chain-a did not register chain-b",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_c}" in _peer_node_ids(server_b),
            timeout=5.0,
            error_message="chain-b did not register chain-c",
        )
        await wait_for_condition(
            lambda: f"ws://127.0.0.1:{port_d}" in _peer_node_ids(server_c),
            timeout=5.0,
            error_message="chain-c did not register chain-d",
        )

        def cluster_echo(payload: str) -> str:
            return f"chain-d:{payload}"

        server_d.register_command("cluster_echo", cluster_echo, ["cpu"])
        await wait_for_condition(
            lambda: _function_visible(server_a, "cluster_echo", "chain-d"),
            timeout=10.0,
            error_message="chain-a did not see chain-d function",
        )

        client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
        test_context.clients.append(client)
        await client.connect()

        async def send_payload(payload: str) -> str:
            return await client.call("cluster_echo", payload, target_cluster="chain-d")

        results = await asyncio.gather(
            send_payload("payload-0"),
            send_payload("payload-1"),
            send_payload("payload-2"),
        )
        assert results == [
            "chain-d:payload-0",
            "chain-d:payload-1",
            "chain-d:payload-2",
        ]
