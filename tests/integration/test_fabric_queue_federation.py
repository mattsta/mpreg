import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.fabric.federation_config import (
    create_explicit_bridging_config,
    create_permissive_bridging_config,
)
from mpreg.fabric.index import QueueQuery
from mpreg.fabric.route_control import RouteDestination
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager, wait_for_condition


def _queue_visible(server: MPREGServer, queue_name: str, cluster_id: str) -> bool:
    if not server._fabric_control_plane:
        return False
    matches = server._fabric_control_plane.index.find_queues(
        QueueQuery(queue_name=queue_name),
        now=time.time(),
    )
    return any(entry.cluster_id == cluster_id for entry in matches)


async def _start_queue_servers(
    ctx: AsyncTestContext, port_manager: TestPortManager
) -> tuple[MPREGServer, MPREGServer]:
    port_a = port_manager.get_server_port()
    port_b = port_manager.get_server_port()

    settings_a = MPREGSettings(
        host="127.0.0.1",
        port=port_a,
        name="Queue-A",
        cluster_id="queue-cluster-a",
        resources={"queue"},
        connect=None,
        peers=None,
        gossip_interval=0.5,
        monitoring_enabled=False,
        enable_default_queue=True,
        federation_config=create_permissive_bridging_config("queue-cluster-a"),
    )
    settings_b = MPREGSettings(
        host="127.0.0.1",
        port=port_b,
        name="Queue-B",
        cluster_id="queue-cluster-b",
        resources={"queue"},
        connect=f"ws://127.0.0.1:{port_a}",
        peers=None,
        gossip_interval=0.5,
        monitoring_enabled=False,
        enable_default_queue=True,
        federation_config=create_permissive_bridging_config("queue-cluster-b"),
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
    return server_a, server_b


async def _start_queue_chain(
    ctx: AsyncTestContext, port_manager: TestPortManager
) -> tuple[MPREGServer, MPREGServer, MPREGServer]:
    port_a = port_manager.get_server_port()
    port_b = port_manager.get_server_port()
    port_c = port_manager.get_server_port()

    settings_a = MPREGSettings(
        host="127.0.0.1",
        port=port_a,
        name="Queue-Chain-A",
        cluster_id="queue-chain-a",
        resources={"queue"},
        connect=None,
        peers=None,
        gossip_interval=0.4,
        monitoring_enabled=False,
        enable_default_queue=True,
        federation_config=create_explicit_bridging_config(
            "queue-chain-a", {"queue-chain-b"}
        ),
    )
    settings_b = MPREGSettings(
        host="127.0.0.1",
        port=port_b,
        name="Queue-Chain-B",
        cluster_id="queue-chain-b",
        resources={"queue"},
        connect=f"ws://127.0.0.1:{port_a}",
        peers=None,
        gossip_interval=0.4,
        monitoring_enabled=False,
        enable_default_queue=True,
        federation_config=create_explicit_bridging_config(
            "queue-chain-b", {"queue-chain-a", "queue-chain-c"}
        ),
    )
    settings_c = MPREGSettings(
        host="127.0.0.1",
        port=port_c,
        name="Queue-Chain-C",
        cluster_id="queue-chain-c",
        resources={"queue"},
        connect=f"ws://127.0.0.1:{port_b}",
        peers=None,
        gossip_interval=0.4,
        monitoring_enabled=False,
        enable_default_queue=True,
        federation_config=create_explicit_bridging_config(
            "queue-chain-c", {"queue-chain-b"}
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
    return server_a, server_b, server_c


@pytest.mark.asyncio
async def test_fabric_queue_delivery_and_ack() -> None:
    async with AsyncTestContext() as ctx:
        with TestPortManager() as port_manager:
            server_a, server_b = await _start_queue_servers(ctx, port_manager)

            assert server_b._fabric_queue_federation is not None
            await server_b._fabric_queue_federation.create_queue("jobs")

            received = asyncio.Event()
            payload_holder: dict[str, str] = {}

            def on_message(message) -> None:
                payload_holder["payload"] = str(message.payload)
                received.set()

            server_b._queue_manager.subscribe_to_queue(
                "jobs",
                subscriber_id="worker-1",
                topic_pattern="jobs.*",
                callback=on_message,
                auto_acknowledge=True,
            )

            await wait_for_condition(
                lambda: _queue_visible(server_a, "jobs", "queue-cluster-b"),
                timeout=8.0,
                interval=0.2,
                error_message="Queue advertisement did not reach server A",
            )

            assert server_a._fabric_queue_federation is not None
            result = await server_a._fabric_queue_federation.send_message_globally(
                queue_name="jobs",
                topic="jobs.created",
                payload={"job": "run"},
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                target_cluster="queue-cluster-b",
            )

            assert result.success is True

            await asyncio.wait_for(received.wait(), timeout=5.0)
            assert payload_holder["payload"] == "{'job': 'run'}"

            await wait_for_condition(
                lambda: not server_a._fabric_queue_federation.in_flight,
                timeout=5.0,
                interval=0.2,
                error_message="Queue federation ack did not clear in-flight state",
            )


@pytest.mark.asyncio
async def test_fabric_queue_global_subscription_forwarding() -> None:
    async with AsyncTestContext() as ctx:
        with TestPortManager() as port_manager:
            server_a, server_b = await _start_queue_servers(ctx, port_manager)

            assert server_b._fabric_queue_federation is not None
            await server_b._fabric_queue_federation.create_queue("events")

            await wait_for_condition(
                lambda: _queue_visible(server_a, "events", "queue-cluster-b"),
                timeout=8.0,
                interval=0.2,
                error_message="Queue advertisement did not reach server A",
            )

            assert server_a._fabric_queue_federation is not None
            received = asyncio.Event()

            def on_message(message) -> None:
                if message.payload.get("event") == "ok":
                    received.set()

            subscription_id = (
                await server_a._fabric_queue_federation.subscribe_globally(
                    subscriber_id="sub-1",
                    queue_pattern="events",
                    topic_pattern="events.*",
                    callback=on_message,
                )
            )

            await wait_for_condition(
                lambda: bool(
                    server_b._fabric_queue_federation
                    and server_b._fabric_queue_federation._local_subscription_handles.get(
                        subscription_id
                    )
                ),
                timeout=5.0,
                interval=0.2,
                error_message="Remote subscription not established",
            )

            await server_b._queue_manager.send_message(
                "events",
                "events.created",
                {"event": "ok"},
                DeliveryGuarantee.AT_LEAST_ONCE,
            )

            await asyncio.wait_for(received.wait(), timeout=5.0)


@pytest.mark.asyncio
async def test_fabric_queue_global_quorum_delivery() -> None:
    async with AsyncTestContext() as ctx:
        with TestPortManager() as port_manager:
            server_a, server_b = await _start_queue_servers(ctx, port_manager)

            assert server_a._fabric_queue_federation is not None
            assert server_b._fabric_queue_federation is not None
            await server_a._fabric_queue_federation.create_queue("quorum")
            await server_b._fabric_queue_federation.create_queue("quorum")

            await wait_for_condition(
                lambda: _queue_visible(server_a, "quorum", "queue-cluster-b"),
                timeout=8.0,
                interval=0.2,
                error_message="Queue advertisement did not reach server A",
            )

            received_a = asyncio.Event()
            received_b = asyncio.Event()

            server_a._queue_manager.subscribe_to_queue(
                "quorum",
                subscriber_id="local-a",
                topic_pattern="quorum.*",
                callback=lambda msg: received_a.set(),
                auto_acknowledge=True,
            )
            server_b._queue_manager.subscribe_to_queue(
                "quorum",
                subscriber_id="local-b",
                topic_pattern="quorum.*",
                callback=lambda msg: received_b.set(),
                auto_acknowledge=True,
            )

            assert server_a._fabric_queue_delivery is not None
            result = await server_a._fabric_queue_delivery.deliver_with_global_quorum(
                queue_name="quorum",
                topic="quorum.test",
                payload={"value": "ok"},
                target_clusters={"queue-cluster-a", "queue-cluster-b"},
            )

            assert result.success is True


@pytest.mark.asyncio
async def test_fabric_queue_multi_hop_delivery() -> None:
    async with AsyncTestContext() as ctx:
        with TestPortManager() as port_manager:
            server_a, _server_b, server_c = await _start_queue_chain(ctx, port_manager)

            assert server_a._fabric_queue_federation is not None
            await server_a._fabric_queue_federation.create_queue("jobs")

            received = asyncio.Event()

            def on_message(message) -> None:
                if message.payload.get("job") == "chain":
                    received.set()

            server_a._queue_manager.subscribe_to_queue(
                "jobs",
                subscriber_id="worker-chain",
                topic_pattern="jobs.*",
                callback=on_message,
                auto_acknowledge=True,
            )

            await wait_for_condition(
                lambda: _queue_visible(server_c, "jobs", "queue-chain-a"),
                timeout=8.0,
                interval=0.2,
                error_message="Queue advertisement did not reach server C",
            )

            await wait_for_condition(
                lambda: (
                    server_c._fabric_control_plane is not None
                    and server_c._fabric_control_plane.route_table.select_route(
                        RouteDestination(cluster_id="queue-chain-a")
                    )
                    is not None
                ),
                timeout=8.0,
                interval=0.2,
                error_message="Route table did not learn a path to queue-chain-a",
            )

            assert server_c._fabric_queue_federation is not None
            result = await server_c._fabric_queue_federation.send_message_globally(
                queue_name="jobs",
                topic="jobs.chain",
                payload={"job": "chain"},
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                target_cluster="queue-chain-a",
            )

            assert result.success is True
            await asyncio.wait_for(received.wait(), timeout=6.0)
