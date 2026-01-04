from __future__ import annotations

#!/usr/bin/env python3
"""Tier 3: Full system expansion demo across all systems.

Scenario: Multi-stage intake pipeline with caching, pub/sub notifications,
queue-based backpressure, federation replication, and monitoring.
"""


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
from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    SystemType,
    create_unified_system_monitor,
)
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


async def _await_fabric_ready(
    server: MPREGServer,
    *,
    target_cluster: str,
    function_name: str,
    timeout: float = 8.0,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        peers = server.cluster.peer_urls_for_cluster(target_cluster)
        if peers and function_name in server.cluster.funtimes:
            return
        await asyncio.sleep(0.1)
    raise RuntimeError(
        f"Tier 3 demo: fabric route to {target_cluster} not ready for {function_name}"
    )


async def main() -> None:
    config_a = create_permissive_bridging_config("cluster-a")
    config_b = create_permissive_bridging_config("cluster-b")

    with port_range_context(3, "servers") as ports:
        settings = [
            MPREGSettings(
                port=ports[0],
                name="Intake",
                resources={"ingestion", "raw"},
                cluster_id="cluster-a",
                federation_config=config_a,
                cache_region="us-west",
                cache_latitude=37.7749,
                cache_longitude=-122.4194,
            ),
            MPREGSettings(
                port=ports[1],
                name="Process",
                resources={"processing", "compute"},
                peers=[f"ws://127.0.0.1:{ports[0]}"],
                cluster_id="cluster-a",
                federation_config=config_a,
                cache_region="us-west",
                cache_latitude=37.7749,
                cache_longitude=-122.4194,
            ),
            MPREGSettings(
                port=ports[2],
                name="Analytics",
                resources={"analytics", "ml"},
                peers=[f"ws://127.0.0.1:{ports[0]}"],
                cluster_id="cluster-b",
                federation_config=config_b,
                cache_region="eu-west",
                cache_latitude=51.5074,
                cache_longitude=-0.1278,
            ),
        ]

    async def _run(servers: list[MPREGServer]) -> None:
        intake, processing, analytics = servers

        def ingest(sensor_id: str, readings: list[float]) -> dict[str, Any]:
            return {
                "sensor_id": sensor_id,
                "readings": readings,
                "ingested_at": time.time(),
            }

        def normalize(payload: dict[str, Any]) -> dict[str, Any]:
            readings = payload["readings"]
            normalized = [(r - 50) / 50 for r in readings]
            return {**payload, "normalized": normalized}

        def detect(payload: dict[str, Any]) -> dict[str, Any]:
            anomaly = any(abs(r) > 0.9 for r in payload["normalized"])
            return {**payload, "anomaly": anomaly, "analyzed_at": time.time()}

        intake.register_command("ingest", ingest, ["ingestion", "raw"])
        processing.register_command("normalize", normalize, ["processing", "compute"])
        analytics.register_command("detect", detect, ["analytics", "ml"])

        await _await_fabric_ready(
            intake, target_cluster="cluster-b", function_name="detect"
        )

        exchange = TopicExchange("ws://local", "system_demo")
        queue_manager = create_reliable_queue_manager()
        await queue_manager.create_queue("alerts")
        exchange.add_subscription(
            PubSubSubscription(
                subscription_id="alerts",
                patterns=(TopicPattern(pattern="sensor.*.alert"),),
                subscriber="queue-bridge",
                created_at=time.time(),
            )
        )

        alert_messages: list[str] = []

        def alert_worker(message: Any) -> None:
            alert_messages.append(str(message.payload))

        queue_manager.subscribe_to_queue(
            "alerts", "alert-worker", "alerts.*", alert_worker
        )

        monitor = create_unified_system_monitor()
        await monitor.start()

        cache_transport = InProcessCacheTransport()
        cache_protocol = FabricCacheProtocol(
            "node-a", transport=cache_transport, gossip_interval=60.0
        )

        cache = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=True,
                enable_l4_federation=True,
                local_cluster_id="cluster-a",
                local_region="us-west",
            ),
            cache_protocol=cache_protocol,
        )

        async with MPREGClientAPI(f"ws://127.0.0.1:{ports[0]}") as client:
            tracking_id = await monitor.record_cross_system_event(
                correlation_id="full-system",
                event_type=EventType.REQUEST_START,
                source_system=SystemType.RPC,
                metadata={"workflow": "sensor-pipeline"},
            )

            workflow = await client._client.request(
                [
                    RPCCommand(
                        name="ingested",
                        fun="ingest",
                        args=("sensor-1", [49.0, 50.1, 100.0]),
                        locs=frozenset(["ingestion", "raw"]),
                    ),
                    RPCCommand(
                        name="normalized",
                        fun="normalize",
                        args=("ingested",),
                        locs=frozenset(["processing", "compute"]),
                    ),
                    RPCCommand(
                        name="analyzed",
                        fun="detect",
                        args=("normalized",),
                        locs=frozenset(["analytics", "ml"]),
                    ),
                ]
            )

            await monitor.record_cross_system_event(
                correlation_id="full-system",
                event_type=EventType.CROSS_SYSTEM_CORRELATION,
                source_system=SystemType.RPC,
                target_system=SystemType.CACHE,
                tracking_id=tracking_id,
                latency_ms=12.0,
            )

            cache_key = GlobalCacheKey.from_data(
                "pipeline.results", workflow["analyzed"]
            )
            cache_options = CacheOptions(
                cache_levels=frozenset([CacheLevel.L1, CacheLevel.L4])
            )
            await cache.put(
                cache_key,
                workflow["analyzed"],
                CacheMetadata(computation_cost_ms=15.0, ttl_seconds=120.0),
                options=cache_options,
            )

            if workflow["analyzed"]["anomaly"]:
                notifications = exchange.publish_message(
                    PubSubMessage(
                        message_id="alert-1",
                        topic="sensor.1.alert",
                        payload={
                            "sensor": "sensor-1",
                            "severity": "high",
                            "ts": time.time(),
                        },
                        publisher="pipeline",
                        headers={},
                        timestamp=time.time(),
                    )
                )
                for notification in notifications:
                    await queue_manager.send_message(
                        "alerts",
                        "alerts.sensor",
                        notification.message.payload,
                        DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                await asyncio.sleep(0.2)

            await monitor.record_cross_system_event(
                correlation_id="full-system",
                event_type=EventType.CROSS_SYSTEM_CORRELATION,
                source_system=SystemType.CACHE,
                target_system=SystemType.PUBSUB,
                tracking_id=tracking_id,
                latency_ms=8.0,
            )

            l4_result = await cache.get(cache_key, cache_options)

            await monitor.record_cross_system_event(
                correlation_id="full-system",
                event_type=EventType.REQUEST_COMPLETE,
                source_system=SystemType.RPC,
                tracking_id=tracking_id,
                latency_ms=60.0,
            )

            print("Workflow result:", workflow["analyzed"])
            print("Alerts queued:", alert_messages)
            print("Federated cache hit:", l4_result.success)
            print("Tracking events:", len(monitor.get_tracking_timeline(tracking_id)))
            _ensure(
                workflow["analyzed"]["anomaly"] is True,
                "Tier 3 demo: anomaly flag missing",
            )
            _ensure(l4_result.success, "Tier 3 demo: federation cache miss")
            _ensure(
                len(monitor.get_tracking_timeline(tracking_id)) >= 3,
                "Tier 3 demo: incomplete monitoring timeline",
            )
            if workflow["analyzed"]["anomaly"]:
                _ensure(alert_messages, "Tier 3 demo: missing alert delivery")

        await cache.shutdown()
        await cache_protocol.shutdown()
        await queue_manager.shutdown()
        await monitor.stop()

        await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
