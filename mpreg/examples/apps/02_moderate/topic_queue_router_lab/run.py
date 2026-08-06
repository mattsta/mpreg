"""L2 topic_queue_router_lab — TopicQueueRouter pattern match + strategies."""

from __future__ import annotations

import asyncio

from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import (
    MessageQueueManager,
    QueueManagerConfiguration,
)
from mpreg.core.topic_queue_routing import (
    RoutingStrategy,
    TopicRoutedQueue,
    create_high_performance_topic_router,
    create_topic_queue_router,
)
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    get_probe,
    ok,
    scenario,
    step,
)

async def main() -> None:
    with app_run(
        "topic_queue_router_lab",
        "Topic Queue Router Lab — pattern fanout + strategies",
        level="L2",
        probe=True,
    ):
        probe = get_probe()
        assert probe is not None
        mqm = MessageQueueManager(QueueManagerConfiguration())
        try:
            with scenario(
                "register patterns + fanout match",
                "queue.topic_route",
                "queue.factories",
            ):
                router = create_topic_queue_router(
                    message_queue_manager=mqm,
                    routing_strategy=RoutingStrategy.FANOUT_ALL,
                )
                await router.register_queue_pattern(
                    TopicRoutedQueue(
                        queue_name="orders",
                        topic_patterns=["order.*"],
                        routing_priority=1,
                        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                )
                await router.register_queue_pattern(
                    TopicRoutedQueue(
                        queue_name="all",
                        topic_patterns=["#"],
                        routing_priority=9,
                        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                )
                await router.register_queue_pattern(
                    TopicRoutedQueue(
                        queue_name="users",
                        topic_patterns=["user.#"],
                        routing_priority=2,
                        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                )
                matches = await probe.measure_await(
                    "router.route",
                    router.route_message_to_queues("order.created", {"id": 1}),
                )
                ensure("orders" in matches, f"orders miss {matches}")
                ensure("all" in matches, f"all miss {matches}")
                ensure("users" not in matches, f"users false positive {matches}")
                ok(f"order.created → {sorted(matches)}")

            with scenario("unregister removes pattern", "queue.topic_route"):
                router = create_topic_queue_router(message_queue_manager=mqm)
                await router.register_queue_pattern(
                    TopicRoutedQueue(
                        queue_name="tmp",
                        topic_patterns=["tmp.*"],
                        routing_priority=1,
                        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                )
                ensure(
                    "tmp"
                    in await probe.measure_await(
                        "router.route",
                        router.route_message_to_queues("tmp.x", {}),
                    ),
                    "pre-unreg",
                )
                removed = await router.unregister_queue_pattern("tmp")
                ensure(removed is True, "unregister false")
                ensure(
                    "tmp"
                    not in await probe.measure_await(
                        "router.route",
                        router.route_message_to_queues("tmp.x", {}),
                    ),
                    "still matched",
                )
                gone = await router.unregister_queue_pattern("nope")
                ensure(gone is False, "missing should be False")
                ok("unregister ok")

            with scenario(
                "send_via_topic strategies", "queue.topic_route", "queue.send"
            ):
                router = create_topic_queue_router(message_queue_manager=mqm)
                await mqm.create_queue("strat-q")
                await router.register_queue_pattern(
                    TopicRoutedQueue(
                        queue_name="strat-q",
                        topic_patterns=["strat.*"],
                        routing_priority=1,
                        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                )
                for strategy in (
                    RoutingStrategy.FANOUT_ALL,
                    RoutingStrategy.ROUND_ROBIN,
                    RoutingStrategy.LOAD_BALANCED,
                ):
                    msg = await probe.measure_await(
                        "router.send",
                        router.send_via_topic(
                            topic="strat.ping",
                            message={"s": strategy.value},
                            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                            routing_strategy=strategy,
                        ),
                    )
                    ensure(msg.topic == "strat.ping", msg.topic)
                    ensure(
                        msg.routing_metadata.routing_strategy == strategy,
                        f"meta {msg.routing_metadata.routing_strategy}",
                    )
                    ensure("strat-q" in msg.routed_queues, msg.routed_queues)
                ok("FANOUT/RR/LB send_via_topic")

            with scenario("routing statistics counters", "queue.topic_route"):
                router = create_topic_queue_router(message_queue_manager=mqm)
                await router.register_queue_pattern(
                    TopicRoutedQueue(
                        queue_name="stats-q",
                        topic_patterns=["stats.*"],
                        routing_priority=1,
                        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                )
                await probe.measure_await(
                    "router.route", router.route_message_to_queues("stats.a", {})
                )
                await probe.measure_await(
                    "router.route", router.route_message_to_queues("stats.b", {})
                )
                stats = await router.get_routing_statistics()
                ensure(stats.total_routes >= 2, f"total {stats.total_routes}")
                # Phase G F21 fix: pure route matches bump successful_routes
                ensure(
                    stats.successful_routes >= 2,
                    f"successful_routes {stats.successful_routes}",
                )
                ensure(stats.cache_misses >= 1, f"misses {stats.cache_misses}")
                ensure(stats.active_queue_patterns >= 1, "no patterns")
                step(
                    "F21 fixed: route_message_to_queues increments successful_routes "
                    "on non-empty match"
                )
                ok(
                    f"routes={stats.total_routes} ok={stats.successful_routes} "
                    f"misses={stats.cache_misses} patterns={stats.active_queue_patterns}"
                )

            with scenario("high-performance factory defaults", "queue.factories"):
                hp = create_high_performance_topic_router(mqm)
                ensure(hp.config.max_queue_fanout == 100, hp.config.max_queue_fanout)
                ensure(
                    hp.config.default_routing_strategy == RoutingStrategy.LOAD_BALANCED,
                    str(hp.config.default_routing_strategy),
                )
                ensure(hp.config.enable_load_balancing is True, "lb off")
                step("create_high_performance_topic_router → LOAD_BALANCED")
                ok("hp factory")

            with scenario("no match returns empty", "queue.topic_route"):
                router = create_topic_queue_router(message_queue_manager=mqm)
                await router.register_queue_pattern(
                    TopicRoutedQueue(
                        queue_name="only-a",
                        topic_patterns=["alpha.*"],
                        routing_priority=1,
                        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                    )
                )
                empty = await probe.measure_await(
                    "router.route", router.route_message_to_queues("beta.x", {})
                )
                ensure(empty == [] or len(empty) == 0, f"unexpected {empty}")
                ok("no-match empty")

            with scenario("router latency/throughput probe", "mon.slo"):
                route_op = probe.op("router.route")
                send_op = probe.op("router.send")
                ensure(route_op.count >= 5, f"routes {route_op.count}")
                ensure(send_op.count >= 3, f"sends {send_op.count}")
                ensure(route_op.p95_ms < 5000.0, f"route p95 {route_op.p95_ms}")
                ensure(probe.throughput_ops_s > 0.0, "throughput")
                ok(
                    f"probe ops={probe.total_ops} route_p95={route_op.p95_ms:.2f} "
                    f"throughput_ops_s={probe.throughput_ops_s:.1f}"
                )
        finally:
            # MessageQueueManager may not require close; best-effort
            close = getattr(mqm, "shutdown", None) or getattr(mqm, "close", None)
            if close is not None:
                result = close()
                if asyncio.iscoroutine(result):
                    await result

if __name__ == "__main__":
    asyncio.run(main())
