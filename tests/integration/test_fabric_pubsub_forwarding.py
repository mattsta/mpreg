import asyncio

import pytest

from mpreg.client.pubsub_client import MPREGPubSubExtendedClient
from mpreg.core.config import MPREGSettings
from mpreg.fabric.federation_config import create_explicit_bridging_config
from mpreg.fabric.index import TopicQuery
from mpreg.fabric.route_control import RouteDestination
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager, wait_for_condition


@pytest.mark.asyncio
async def test_fabric_pubsub_forwarding_across_servers() -> None:
    async with AsyncTestContext() as ctx:
        with TestPortManager() as port_manager:
            port_a = port_manager.get_server_port()
            port_b = port_manager.get_server_port()

            settings_a = MPREGSettings(
                host="127.0.0.1",
                port=port_a,
                name="PubSub-A",
                cluster_id="fabric-pubsub-cluster",
                connect=None,
                peers=None,
                gossip_interval=0.5,
                fabric_routing_enabled=True,
            )
            settings_b = MPREGSettings(
                host="127.0.0.1",
                port=port_b,
                name="PubSub-B",
                cluster_id="fabric-pubsub-cluster",
                connect=f"ws://127.0.0.1:{port_a}",
                peers=None,
                gossip_interval=0.5,
                fabric_routing_enabled=True,
            )

            server_a = MPREGServer(settings=settings_a)
            server_b = MPREGServer(settings=settings_b)
            ctx.servers.extend([server_a, server_b])
            ctx.tasks.extend(
                [
                    asyncio.create_task(server_a.server()),
                    asyncio.create_task(server_b.server()),
                ]
            )

            await asyncio.sleep(2.0)

            topic = "alerts.critical"
            received = asyncio.Event()
            payload_holder: dict[str, str] = {}

            def on_message(message):
                payload_holder["payload"] = str(message.payload)
                received.set()

            async with MPREGPubSubExtendedClient(
                f"ws://127.0.0.1:{port_b}"
            ) as client_b:
                await client_b.pubsub.subscribe(
                    patterns=[topic],
                    callback=on_message,
                    get_backlog=False,
                )

                def subscription_gossiped() -> bool:
                    if not server_a._fabric_control_plane:
                        return False
                    matches = server_a._fabric_control_plane.index.match_topics(
                        TopicQuery(topic=topic)
                    )
                    return any(
                        sub.node_id != server_a.cluster.local_url for sub in matches
                    )

                await wait_for_condition(
                    subscription_gossiped,
                    timeout=8.0,
                    interval=0.2,
                    error_message="Remote subscription did not reach server A",
                )

                async with MPREGPubSubExtendedClient(
                    f"ws://127.0.0.1:{port_a}"
                ) as client_a:
                    success = await client_a.pubsub.publish(
                        topic=topic,
                        payload="payload",
                    )
                    assert success is True

                await asyncio.wait_for(received.wait(), timeout=5.0)
                assert payload_holder["payload"] == "payload"


@pytest.mark.asyncio
async def test_fabric_pubsub_forwarding_multi_hop() -> None:
    async with AsyncTestContext() as ctx:
        with TestPortManager() as port_manager:
            port_a = port_manager.get_server_port()
            port_b = port_manager.get_server_port()
            port_c = port_manager.get_server_port()

            settings_a = MPREGSettings(
                host="127.0.0.1",
                port=port_a,
                name="PubSub-A",
                cluster_id="cluster-a",
                connect=None,
                peers=None,
                gossip_interval=0.5,
                fabric_routing_enabled=True,
                federation_config=create_explicit_bridging_config(
                    "cluster-a", {"cluster-b"}
                ),
            )
            settings_b = MPREGSettings(
                host="127.0.0.1",
                port=port_b,
                name="PubSub-B",
                cluster_id="cluster-b",
                connect=f"ws://127.0.0.1:{port_a}",
                peers=None,
                gossip_interval=0.5,
                fabric_routing_enabled=True,
                federation_config=create_explicit_bridging_config(
                    "cluster-b", {"cluster-a", "cluster-c"}
                ),
            )
            settings_c = MPREGSettings(
                host="127.0.0.1",
                port=port_c,
                name="PubSub-C",
                cluster_id="cluster-c",
                connect=f"ws://127.0.0.1:{port_b}",
                peers=None,
                gossip_interval=0.5,
                fabric_routing_enabled=True,
                federation_config=create_explicit_bridging_config(
                    "cluster-c", {"cluster-b"}
                ),
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

            assert (
                f"ws://127.0.0.1:{port_c}" not in server_a._get_all_peer_connections()
            )

            topic = "alerts.multi_hop"
            received = asyncio.Event()
            payload_holder: dict[str, str] = {}

            def on_message(message):
                payload_holder["payload"] = str(message.payload)
                received.set()

            async with MPREGPubSubExtendedClient(
                f"ws://127.0.0.1:{port_c}"
            ) as client_c:
                await client_c.pubsub.subscribe(
                    patterns=[topic],
                    callback=on_message,
                    get_backlog=False,
                )

                def subscription_gossiped() -> bool:
                    if not server_a._fabric_control_plane:
                        return False
                    matches = server_a._fabric_control_plane.index.match_topics(
                        TopicQuery(topic=topic)
                    )
                    return any(sub.cluster_id == "cluster-c" for sub in matches)

                def route_ready() -> bool:
                    control_plane = server_a._fabric_control_plane
                    if control_plane is None:
                        return False
                    return bool(
                        control_plane.route_table.routes_for(
                            RouteDestination(cluster_id="cluster-c")
                        )
                    )

                await wait_for_condition(
                    subscription_gossiped,
                    timeout=8.0,
                    interval=0.2,
                    error_message="Remote subscription did not reach server A",
                )
                await wait_for_condition(
                    route_ready,
                    timeout=8.0,
                    interval=0.2,
                    error_message="Route to cluster-c did not propagate to server A",
                )

                async with MPREGPubSubExtendedClient(
                    f"ws://127.0.0.1:{port_a}"
                ) as client_a:
                    success = await client_a.pubsub.publish(
                        topic=topic,
                        payload="payload",
                    )
                    assert success is True

                await asyncio.wait_for(received.wait(), timeout=5.0)
                assert payload_holder["payload"] == "payload"
