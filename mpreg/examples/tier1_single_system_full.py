from __future__ import annotations

#!/usr/bin/env python3
"""Tier 1: Single-system full capability demonstrations.

Run:
  uv run python mpreg/examples/tier1_single_system_full.py --system rpc
  uv run python mpreg/examples/tier1_single_system_full.py --system cache
"""


import argparse
import asyncio
import time
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.core.model import PubSubMessage, PubSubSubscription, RPCCommand, TopicPattern
from mpreg.core.port_allocator import port_range_context
from mpreg.core.topic_exchange import TopicExchange
from mpreg.examples.showcase_utils import run_with_servers
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer


def _ensure(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


async def demo_rpc() -> None:
    """Full RPC system demo: dependency graph + resource routing + concurrency."""
    with port_range_context(2, "servers") as ports:
        settings = [
            MPREGSettings(port=ports[0], name="RPC-CPU", resources={"cpu", "math"}),
            MPREGSettings(
                port=ports[1],
                name="RPC-GPU",
                resources={"gpu", "ml"},
                peers=[f"ws://127.0.0.1:{ports[0]}"],
            ),
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            cpu, gpu = servers

            def add(a: int, b: int) -> int:
                return a + b

            def multiply(x: int, factor: int) -> int:
                return x * factor

            def model_score(x: int) -> dict[str, Any]:
                return {"score": x / 100.0, "model": "demo"}

            cpu.register_command("add", add, ["cpu", "math"])
            cpu.register_command("multiply", multiply, ["cpu", "math"])
            gpu.register_command("model_score", model_score, ["gpu", "ml"])

            async with MPREGClientAPI(f"ws://127.0.0.1:{ports[0]}") as client:
                result = await client._client.request(
                    [
                        RPCCommand(
                            name="sum",
                            fun="add",
                            args=(20, 22),
                            locs=frozenset(["cpu", "math"]),
                        ),
                        RPCCommand(
                            name="scaled",
                            fun="multiply",
                            args=("sum", 3),
                            locs=frozenset(["cpu", "math"]),
                        ),
                        RPCCommand(
                            name="scored",
                            fun="model_score",
                            args=("scaled",),
                            locs=frozenset(["gpu", "ml"]),
                        ),
                    ]
                )

                print("RPC result:", result)
                scored = result.get("scored")
                _ensure(isinstance(scored, dict), "RPC demo: missing scored result")
                _ensure(
                    abs(scored.get("score", 0.0) - 1.26) < 1e-6,
                    "RPC demo: unexpected score",
                )

        await run_with_servers(settings, _run)


async def demo_pubsub() -> None:
    """Full Topic Exchange demo: routing, wildcards, and fan-out."""
    exchange = TopicExchange("ws://local", "demo_cluster")
    exchange.add_subscription(
        PubSubSubscription(
            subscription_id="auth",
            patterns=(
                TopicPattern(pattern="user.*.login"),
                TopicPattern(pattern="user.*.logout"),
            ),
            subscriber="auth-svc",
            created_at=time.time(),
        )
    )
    exchange.add_subscription(
        PubSubSubscription(
            subscription_id="orders",
            patterns=(
                TopicPattern(pattern="order.#"),
                TopicPattern(pattern="payment.*.completed"),
            ),
            subscriber="orders-svc",
            created_at=time.time(),
        )
    )

    notifications = []

    notifications.extend(
        exchange.publish_message(
            PubSubMessage(
                message_id="m1",
                topic="user.123.login",
                payload={"user": "alice"},
                publisher="demo",
                headers={},
                timestamp=time.time(),
            )
        )
    )
    notifications.extend(
        exchange.publish_message(
            PubSubMessage(
                message_id="m2",
                topic="order.us.456.created",
                payload={"order": 456},
                publisher="demo",
                headers={},
                timestamp=time.time(),
            )
        )
    )
    notifications.extend(
        exchange.publish_message(
            PubSubMessage(
                message_id="m3",
                topic="payment.card.completed",
                payload={"order": 456},
                publisher="demo",
                headers={},
                timestamp=time.time(),
            )
        )
    )

    print("PubSub notifications:", len(notifications))
    _ensure(len(notifications) == 3, "PubSub demo: unexpected notification count")


async def demo_queue() -> None:
    """Full queue demo: at-least-once and quorum delivery."""
    manager = create_reliable_queue_manager()
    await manager.create_queue("jobs")

    received: list[str] = []

    def worker(message: Any) -> None:
        received.append(str(message.payload))

    manager.subscribe_to_queue("jobs", "worker-1", "jobs.*", callback=worker)
    manager.subscribe_to_queue("jobs", "worker-2", "jobs.*", callback=worker)

    await manager.send_message(
        "jobs", "jobs.batch", {"task": "build-index"}, DeliveryGuarantee.AT_LEAST_ONCE
    )
    await manager.send_message(
        "jobs",
        "jobs.batch",
        {"task": "refresh-cache"},
        DeliveryGuarantee.QUORUM,
        required_acknowledgments=2,
    )
    await asyncio.sleep(0.5)
    print("Queue received:", received)
    _ensure(
        len(received) == 4
        and "build-index" in str(received)
        and "refresh-cache" in str(received),
        "Queue demo: missing expected deliveries",
    )
    await manager.shutdown()


async def demo_cache() -> None:
    """Full cache demo: L1-L4 with fabric sync and federation scope."""
    cache_config = GlobalCacheConfiguration(
        enable_l2_persistent=False,
        enable_l3_distributed=True,
        enable_l4_federation=True,
        local_cluster_id="cluster-a",
        local_region="us-west",
    )

    cache_transport = InProcessCacheTransport()
    cache_protocol_a = FabricCacheProtocol(
        "node-a", transport=cache_transport, gossip_interval=60.0
    )
    cache_protocol_b = FabricCacheProtocol(
        "node-b", transport=cache_transport, gossip_interval=60.0
    )

    cache_a = GlobalCacheManager(cache_config, cache_protocol=cache_protocol_a)
    cache_b = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=True,
            enable_l4_federation=True,
            local_cluster_id="cluster-b",
            local_region="eu-west",
        ),
        cache_protocol=cache_protocol_b,
    )

    key = GlobalCacheKey.from_data("demo.cache", {"customer": "alice"})
    metadata = CacheMetadata(
        computation_cost_ms=250.0,
        ttl_seconds=60.0,
        geographic_hints=["eu-west"],
    )

    await cache_a.put(key, {"payload": "cached-value"}, metadata)
    await cache_protocol_a.sync_cache_state("node-b")
    await asyncio.sleep(0.1)

    # L3 sync fetch
    l3_result = await cache_b.get(
        key, CacheOptions(cache_levels=frozenset([CacheLevel.L3]))
    )
    print("L3 fetch:", l3_result.success)
    _ensure(l3_result.success, "Cache demo: L3 fetch failed")

    # L4 federation-scope fetch
    l4_result = await cache_b.get(
        key, CacheOptions(cache_levels=frozenset([CacheLevel.L4]))
    )
    print("L4 fetch:", l4_result.success)
    _ensure(l4_result.success, "Cache demo: L4 fetch failed")

    await cache_a.shutdown()
    await cache_b.shutdown()
    await cache_protocol_a.shutdown()
    await cache_protocol_b.shutdown()


async def demo_fabric() -> None:
    """Full fabric federation demo: multi-cluster routing + status sharing."""
    config_a = create_permissive_bridging_config("cluster-a")
    config_b = create_permissive_bridging_config("cluster-b")

    with port_range_context(2, "servers") as ports:
        settings = [
            MPREGSettings(
                port=ports[0],
                name="Cluster-A",
                resources={"alpha"},
                cluster_id="cluster-a",
                federation_config=config_a,
            ),
            MPREGSettings(
                port=ports[1],
                name="Cluster-B",
                resources={"beta"},
                cluster_id="cluster-b",
                peers=[f"ws://127.0.0.1:{ports[0]}"],
                federation_config=config_b,
            ),
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            server_a, server_b = servers

            def alpha_task(x: int) -> str:
                return f"alpha-{x}"

            def beta_task(x: int) -> str:
                return f"beta-{x}"

            server_a.register_command("alpha_task", alpha_task, ["alpha"])
            server_b.register_command("beta_task", beta_task, ["beta"])

            async with MPREGClientAPI(f"ws://127.0.0.1:{ports[0]}") as client:
                result = await client._client.request(
                    [
                        RPCCommand(
                            name="a",
                            fun="alpha_task",
                            args=(1,),
                            locs=frozenset(["alpha"]),
                        ),
                        RPCCommand(
                            name="b",
                            fun="beta_task",
                            args=(2,),
                            locs=frozenset(["beta"]),
                        ),
                    ]
                )
                print("Fabric RPC:", result)
                _ensure("a" in result and "b" in result, "Fabric demo: missing results")

        await run_with_servers(settings, _run)


async def demo_monitoring() -> None:
    """Full monitoring demo: cross-system tracking timeline."""
    from mpreg.core.monitoring.unified_monitoring import (
        EventType,
        SystemType,
        create_unified_system_monitor,
    )

    monitor = create_unified_system_monitor()
    await monitor.start()

    tracking_id = await monitor.record_cross_system_event(
        correlation_id="demo-flow",
        event_type=EventType.REQUEST_START,
        source_system=SystemType.RPC,
        metadata={"operation": "demo"},
    )
    await monitor.record_cross_system_event(
        correlation_id="demo-flow",
        event_type=EventType.CROSS_SYSTEM_CORRELATION,
        source_system=SystemType.RPC,
        target_system=SystemType.CACHE,
        tracking_id=tracking_id,
        latency_ms=12.0,
    )
    await monitor.record_cross_system_event(
        correlation_id="demo-flow",
        event_type=EventType.REQUEST_COMPLETE,
        source_system=SystemType.RPC,
        tracking_id=tracking_id,
        latency_ms=40.0,
    )

    timeline = monitor.get_tracking_timeline(tracking_id)
    print("Monitoring timeline length:", len(timeline))
    _ensure(len(timeline) >= 3, "Monitoring demo: incomplete timeline")
    await monitor.stop()


SYSTEMS = {
    "rpc": demo_rpc,
    "pubsub": demo_pubsub,
    "queue": demo_queue,
    "cache": demo_cache,
    "fabric": demo_fabric,
    "monitoring": demo_monitoring,
}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", choices=sorted(SYSTEMS.keys()), required=True)
    args = parser.parse_args()
    await SYSTEMS[args.system]()


if __name__ == "__main__":
    asyncio.run(main())
