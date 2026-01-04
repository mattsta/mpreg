import asyncio
import time

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubExtendedClient
from mpreg.core.config import MPREGSettings
from mpreg.datastructures.function_identity import FunctionSelector
from mpreg.fabric.index import FunctionQuery, TopicQuery
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager, wait_for_condition


def _function_visible(server: MPREGServer, name: str, cluster_id: str) -> bool:
    if not server._fabric_control_plane:
        return False
    query = FunctionQuery(selector=FunctionSelector(name=name), cluster_id=cluster_id)
    return bool(
        server._fabric_control_plane.index.find_functions(query, now=time.time())
    )


@pytest.mark.asyncio
async def test_fabric_live_rpc_and_pubsub_workflow() -> None:
    async with AsyncTestContext() as ctx:
        with TestPortManager() as port_manager:
            port_a = port_manager.get_server_port()
            port_b = port_manager.get_server_port()

            settings_a = MPREGSettings(
                host="127.0.0.1",
                port=port_a,
                name="Fabric-Unified-A",
                cluster_id="fabric-unified",
                resources={"cpu"},
                connect=None,
                peers=None,
                gossip_interval=0.5,
                fabric_routing_enabled=True,
            )
            settings_b = MPREGSettings(
                host="127.0.0.1",
                port=port_b,
                name="Fabric-Unified-B",
                cluster_id="fabric-unified",
                resources={"cpu"},
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

            await asyncio.sleep(1.5)

            def remote_only(payload: str) -> str:
                return f"remote:{payload}"

            server_b.register_command("remote_only", remote_only, ["cpu"])

            await wait_for_condition(
                lambda: _function_visible(server_a, "remote_only", "fabric-unified"),
                timeout=6.0,
                interval=0.2,
                error_message="Server A did not see remote_only in fabric catalog",
            )

            client = MPREGClientAPI(f"ws://127.0.0.1:{port_a}")
            ctx.clients.append(client)
            await client.connect()

            result = await client.call("remote_only", "payload")
            assert result == "remote:payload"

            topic = "alerts.critical"
            received = asyncio.Event()
            payload_holder: dict[str, str] = {}

            def on_message(message) -> None:
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
