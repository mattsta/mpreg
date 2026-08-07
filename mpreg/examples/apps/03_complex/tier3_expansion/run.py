"""L3 tier3_expansion — multi-plane expansion with scenario depth + ensures."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    CacheMetadata,
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
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer


async def _await_fabric_ready(
    server: MPREGServer,
    *,
    target_cluster: str,
    function_name: str,
    timeout: float = 8.0,
) -> None:
    from mpreg.core.rpc_naming import qualify_rpc_name

    # Catalog keys are FQNs (bare names auto-qualify under default ns).
    fqn = qualify_rpc_name(function_name)
    deadline = time.time() + timeout
    while time.time() < deadline:
        peers = server.cluster.peer_urls_for_cluster(target_cluster)
        funs = server.cluster.funtimes
        if peers and (fqn in funs or function_name in funs):
            return
        await asyncio.sleep(0.1)
    raise RuntimeError(
        f"fabric route to {target_cluster} not ready for {function_name} ({fqn})"
    )


async def main() -> None:
    with app_run(
        "tier3_expansion",
        "Tier3 Expansion — multi-plane system tour",
        level="L3",
    ):
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
                    log_level="WARNING",
                    gossip_interval=0.5,
                ),
                MPREGSettings(
                    port=ports[1],
                    name="Process",
                    resources={"processing", "enrich"},
                    cluster_id="cluster-a",
                    peers=[f"ws://127.0.0.1:{ports[0]}"],
                    federation_config=config_a,
                    log_level="WARNING",
                    gossip_interval=0.5,
                ),
                MPREGSettings(
                    port=ports[2],
                    name="Edge-B",
                    resources={"edge"},
                    cluster_id="cluster-b",
                    peers=[f"ws://127.0.0.1:{ports[0]}"],
                    federation_config=config_b,
                    log_level="WARNING",
                    gossip_interval=0.5,
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                intake, process, edge = servers

                def ingest(payload: dict[str, Any]) -> dict[str, Any]:
                    return {"id": payload.get("id"), "raw": payload, "stage": "ingest"}

                def enrich(doc: dict[str, Any]) -> dict[str, Any]:
                    return {**doc, "stage": "enrich", "score": 0.9}

                def edge_tag(doc: dict[str, Any]) -> dict[str, Any]:
                    return {**doc, "edge": "cluster-b", "stage": "edge"}

                intake.register_command("ingest", ingest, ["ingestion", "raw"])
                process.register_command("enrich", enrich, ["processing", "enrich"])
                edge.register_command("edge_tag", edge_tag, ["edge"])

                hub = f"ws://127.0.0.1:{ports[0]}"
                monitor = create_unified_system_monitor()
                await monitor.start()
                exchange = TopicExchange(server_url=hub, cluster_id="cluster-a")
                notices: list[PubSubMessage] = []

                transport = InProcessCacheTransport()
                protocol = FabricCacheProtocol(
                    "tier3-cache", transport=transport, gossip_interval=60.0
                )
                cache = GlobalCacheManager(
                    GlobalCacheConfiguration(
                        enable_l2_persistent=False,
                        enable_l3_distributed=False,
                        enable_l4_federation=False,
                    ),
                    cache_protocol=protocol,
                )
                qmgr = create_reliable_queue_manager()

                try:
                    with scenario(
                        "multi-stage RPC DAG across resources",
                        "prod.tier3",
                        "rpc.dag",
                        "rpc.locs",
                        "rpc.register",
                    ):
                        async with MPREGClientAPI(hub) as client:
                            result = await client.request(
                                [
                                    RPCCommand(
                                        name="ingested",
                                        fun="ingest",
                                        args=({"id": "o-1", "sku": "x"},),
                                        locs=frozenset(["ingestion", "raw"]),
                                    ),
                                    RPCCommand(
                                        name="enriched",
                                        fun="enrich",
                                        args=("ingested",),
                                        locs=frozenset(["processing", "enrich"]),
                                    ),
                                ]
                            )
                        ensure(
                            isinstance(result, dict) and "enriched" in result,
                            f"DAG missing enriched: {result}",
                        )
                        ensure(
                            result["enriched"].get("stage") == "enrich",
                            f"bad enrich stage {result['enriched']}",
                        )
                        ensure(
                            result["enriched"].get("score") == 0.9,
                            f"bad score {result['enriched']}",
                        )
                        ok(f"DAG enriched={result['enriched']}")

                    with scenario(
                        "cross-cluster fabric edge tag",
                        "fabric.cross_rpc",
                        "rpc.locs",
                    ):
                        await _await_fabric_ready(
                            intake, target_cluster="cluster-b", function_name="edge_tag"
                        )
                        async with MPREGClientAPI(hub) as client:
                            tagged = await client.call(
                                "edge_tag",
                                {"id": "o-1", "stage": "enrich"},
                                locs=frozenset(["edge"]),
                            )
                        ensure(
                            isinstance(tagged, dict)
                            and tagged.get("edge") == "cluster-b",
                            f"edge tag failed {tagged}",
                        )
                        ok(f"edge_tag={tagged}")

                    with scenario(
                        "cache put after enrich + hit",
                        "cache.l4",
                        "cache.put_get",
                    ):
                        key = GlobalCacheKey.from_data("tier3.order", {"id": "o-1"})
                        await cache.put(
                            key,
                            {"id": "o-1", "stage": "enrich", "score": 0.9},
                            CacheMetadata(computation_cost_ms=5.0, ttl_seconds=60.0),
                        )
                        hit = await cache.get(key)
                        ensure(
                            hit.success and hit.entry is not None,
                            f"cache miss {hit}",
                        )
                        ensure(
                            hit.entry.value.get("score") == 0.9,
                            f"bad cached {hit.entry.value}",
                        )
                        ok("order cached after enrich")

                    with scenario(
                        "pubsub notify + queue fulfill",
                        "pubsub.exchange",
                        "queue.alo",
                    ):
                        sub = PubSubSubscription(
                            subscription_id="tier3-sub",
                            patterns=(TopicPattern(pattern="order.#"),),
                            subscriber="tier3",
                            created_at=time.time(),
                            get_backlog=False,
                        )
                        ensure(exchange.add_subscription(sub) is True, "sub failed")
                        msg = PubSubMessage(
                            topic="order.created",
                            payload={"id": "o-1"},
                            timestamp=time.time(),
                            message_id="m-tier3-1",
                            publisher="intake",
                        )
                        notes = exchange.publish_message(msg)
                        ensure(len(notes) >= 1, f"no pubsub fanout {notes}")
                        notices.extend(
                            n.message if hasattr(n, "message") else msg for n in notes
                        )

                        await qmgr.create_queue("fulfill")
                        delivered: list[str] = []

                        def _worker(message: Any) -> None:
                            payload = getattr(message, "payload", message)
                            if isinstance(payload, dict):
                                delivered.append(str(payload.get("id", payload)))
                            else:
                                delivered.append(str(payload))

                        qmgr.subscribe_to_queue(
                            "fulfill", "worker-1", "fulfill.*", callback=_worker
                        )
                        send = await qmgr.send_message(
                            "fulfill",
                            "fulfill.ship",
                            {"id": "o-1", "action": "ship"},
                            DeliveryGuarantee.AT_LEAST_ONCE,
                        )
                        ensure(
                            getattr(send, "success", True) is not False,
                            f"queue send failed {send}",
                        )
                        for _ in range(40):
                            if delivered:
                                break
                            await asyncio.sleep(0.05)
                        ensure(len(delivered) >= 1, "queue worker never fired")
                        ensure(
                            any("o-1" in d for d in delivered),
                            f"bad queue payload {delivered}",
                        )
                        ok(f"pubsub notes≥1 queue delivered={delivered!r}")

                    with scenario(
                        "monitoring timeline for expansion flow",
                        "mon.timeline",
                        "mon.events",
                    ):
                        tid = await monitor.record_cross_system_event(
                            correlation_id="tier3-flow",
                            event_type=EventType.REQUEST_START,
                            source_system=SystemType.RPC,
                            metadata={"app": "tier3_expansion"},
                        )
                        await monitor.record_cross_system_event(
                            correlation_id="tier3-flow",
                            event_type=EventType.CROSS_SYSTEM_CORRELATION,
                            source_system=SystemType.RPC,
                            target_system=SystemType.CACHE,
                            tracking_id=tid,
                            latency_ms=5.0,
                        )
                        await monitor.record_cross_system_event(
                            correlation_id="tier3-flow",
                            event_type=EventType.REQUEST_COMPLETE,
                            source_system=SystemType.RPC,
                            tracking_id=tid,
                            latency_ms=12.0,
                        )
                        timeline = monitor.get_tracking_timeline(tid)
                        ensure(
                            len(timeline) >= 3,
                            f"timeline short {len(timeline)}",
                        )
                        ok(f"timeline events={len(timeline)}")
                finally:
                    await cache.shutdown()
                    await protocol.shutdown()
                    await qmgr.shutdown()
                    await monitor.stop()

                step(
                    "non-claim: expansion sketch — see FEATURE_CATALOG for remaining gaps"
                )

            await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
