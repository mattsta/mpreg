"""
Test PubSub notification delivery across federated clusters using fabric routing.
"""

import asyncio

import pytest

from mpreg.client.pubsub_client import MPREGPubSubExtendedClient
from mpreg.core.config import MPREGSettings
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.fabric.index import TopicQuery
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager, wait_for_condition


class TestFederationPubSubIntegration:
    """Test PubSub notifications across federated clusters."""

    @pytest.mark.asyncio
    async def test_pubsub_notifications_across_federation(self) -> None:
        async with AsyncTestContext() as ctx:
            with TestPortManager() as port_manager:
                port_a = port_manager.get_server_port()
                port_b = port_manager.get_server_port()

                server1_url = f"ws://127.0.0.1:{port_a}"
                server2_url = f"ws://127.0.0.1:{port_b}"

                settings_a = MPREGSettings(
                    host="127.0.0.1",
                    port=port_a,
                    name="FedServer1",
                    cluster_id="fed-cluster-1",
                    connect=None,
                    peers=None,
                    gossip_interval=0.5,
                    fabric_routing_enabled=True,
                    federation_config=create_permissive_bridging_config(
                        "fed-cluster-1"
                    ),
                )
                settings_b = MPREGSettings(
                    host="127.0.0.1",
                    port=port_b,
                    name="FedServer2",
                    cluster_id="fed-cluster-2",
                    connect=server1_url,
                    peers=None,
                    gossip_interval=0.5,
                    fabric_routing_enabled=True,
                    federation_config=create_permissive_bridging_config(
                        "fed-cluster-2"
                    ),
                )

                server1 = MPREGServer(settings=settings_a)
                server2 = MPREGServer(settings=settings_b)
                ctx.servers.extend([server1, server2])
                ctx.tasks.extend(
                    [
                        asyncio.create_task(server1.server()),
                        asyncio.create_task(server2.server()),
                    ]
                )

                await asyncio.sleep(2.0)

                received = asyncio.Event()
                received_topics: list[str] = []

                def on_message(message) -> None:
                    received_topics.append(message.topic)
                    received.set()

                async with MPREGPubSubExtendedClient(server2_url) as client2:
                    await client2.subscribe(
                        patterns=["federation.test.*"],
                        callback=on_message,
                        get_backlog=False,
                    )

                    def subscription_gossiped() -> bool:
                        if not server1._fabric_control_plane:
                            return False
                        matches = server1._fabric_control_plane.index.match_topics(
                            TopicQuery(topic="federation.test.message1")
                        )
                        return any(
                            sub.node_id != server1.cluster.local_url for sub in matches
                        )

                    await wait_for_condition(
                        subscription_gossiped,
                        timeout=10.0,
                        interval=0.2,
                        error_message="Federated subscription did not reach server 1",
                    )

                    async with MPREGPubSubExtendedClient(server1_url) as client1:
                        success = await client1.publish(
                            "federation.test.message1",
                            {"source": "cluster-1"},
                        )
                        assert success is True

                    await asyncio.wait_for(received.wait(), timeout=5.0)
                    assert "federation.test.message1" in received_topics

    @pytest.mark.asyncio
    async def test_federation_subscription_isolation(self) -> None:
        async with AsyncTestContext() as ctx:
            with TestPortManager() as port_manager:
                port_a = port_manager.get_server_port()
                port_b = port_manager.get_server_port()

                server1_url = f"ws://127.0.0.1:{port_a}"
                server2_url = f"ws://127.0.0.1:{port_b}"

                settings_a = MPREGSettings(
                    host="127.0.0.1",
                    port=port_a,
                    name="IsolationServer1",
                    cluster_id="iso-cluster-1",
                    connect=None,
                    peers=None,
                    gossip_interval=0.5,
                    fabric_routing_enabled=True,
                    federation_config=create_permissive_bridging_config(
                        "iso-cluster-1"
                    ),
                )
                settings_b = MPREGSettings(
                    host="127.0.0.1",
                    port=port_b,
                    name="IsolationServer2",
                    cluster_id="iso-cluster-2",
                    connect=server1_url,
                    peers=None,
                    gossip_interval=0.5,
                    fabric_routing_enabled=True,
                    federation_config=create_permissive_bridging_config(
                        "iso-cluster-2"
                    ),
                )

                server1 = MPREGServer(settings=settings_a)
                server2 = MPREGServer(settings=settings_b)
                ctx.servers.extend([server1, server2])
                ctx.tasks.extend(
                    [
                        asyncio.create_task(server1.server()),
                        asyncio.create_task(server2.server()),
                    ]
                )

                await asyncio.sleep(2.0)

                local_messages: list[str] = []
                remote_messages: list[str] = []
                remote_ready = asyncio.Event()

                def local_callback(message) -> None:
                    local_messages.append(message.topic)

                def remote_callback(message) -> None:
                    remote_messages.append(message.topic)
                    remote_ready.set()

                async with MPREGPubSubExtendedClient(server1_url) as client1:
                    async with MPREGPubSubExtendedClient(server2_url) as client2:
                        await client1.subscribe(
                            patterns=["local.*"],
                            callback=local_callback,
                            get_backlog=False,
                        )
                        await client2.subscribe(
                            patterns=["federation.*"],
                            callback=remote_callback,
                            get_backlog=False,
                        )

                        def remote_subscription_gossiped() -> bool:
                            if not server1._fabric_control_plane:
                                return False
                            matches = server1._fabric_control_plane.index.match_topics(
                                TopicQuery(topic="federation.message")
                            )
                            return any(
                                sub.node_id != server1.cluster.local_url
                                for sub in matches
                            )

                        await wait_for_condition(
                            remote_subscription_gossiped,
                            timeout=10.0,
                            interval=0.2,
                            error_message="Remote federation subscription not gossiped",
                        )

                        await client1.publish(
                            "local.message", {"type": "local", "cluster": 1}
                        )
                        await client1.publish(
                            "federation.message",
                            {"type": "federation", "cluster": 1},
                        )

                        await asyncio.wait_for(remote_ready.wait(), timeout=5.0)

                assert local_messages == ["local.message"]
                assert remote_messages == ["federation.message"]
