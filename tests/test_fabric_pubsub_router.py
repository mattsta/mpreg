from mpreg.fabric.catalog import RoutingCatalog, TopicSubscription
from mpreg.fabric.index import RoutingIndex
from mpreg.fabric.pubsub_router import (
    PubSubRouteReason,
    PubSubRoutingPlanner,
)


def _add_subscription(
    catalog: RoutingCatalog,
    *,
    subscription_id: str,
    node_id: str,
    cluster_id: str,
    patterns: tuple[str, ...],
) -> None:
    catalog.topics.register(
        TopicSubscription(
            subscription_id=subscription_id,
            node_id=node_id,
            cluster_id=cluster_id,
            patterns=patterns,
            advertised_at=100.0,
            ttl_seconds=30.0,
        ),
        now=100.0,
    )


def test_pubsub_router_no_subscribers() -> None:
    catalog = RoutingCatalog()
    router = PubSubRoutingPlanner(
        routing_index=RoutingIndex(catalog=catalog),
        local_node_id="node-a",
    )

    plan = router.plan("alerts.critical", now=100.0)

    assert plan.reason == PubSubRouteReason.NO_SUBSCRIBERS
    assert plan.target_nodes == ()


def test_pubsub_router_local_and_remote() -> None:
    catalog = RoutingCatalog()
    _add_subscription(
        catalog,
        subscription_id="sub-local",
        node_id="node-a",
        cluster_id="cluster-a",
        patterns=("alerts.*",),
    )
    _add_subscription(
        catalog,
        subscription_id="sub-remote",
        node_id="node-b",
        cluster_id="cluster-a",
        patterns=("alerts.#",),
    )
    router = PubSubRoutingPlanner(
        routing_index=RoutingIndex(catalog=catalog),
        local_node_id="node-a",
    )

    plan = router.plan("alerts.critical", now=100.0)

    assert plan.reason == PubSubRouteReason.LOCAL_AND_REMOTE
    assert plan.target_nodes == ("node-b",)


def test_pubsub_router_filters_clusters() -> None:
    catalog = RoutingCatalog()
    _add_subscription(
        catalog,
        subscription_id="sub-remote",
        node_id="node-b",
        cluster_id="cluster-b",
        patterns=("metrics.*",),
    )
    router = PubSubRoutingPlanner(
        routing_index=RoutingIndex(catalog=catalog),
        local_node_id="node-a",
        allowed_clusters=frozenset({"cluster-a"}),
    )

    plan = router.plan("metrics.cpu", now=100.0)

    assert plan.reason == PubSubRouteReason.NO_SUBSCRIBERS
    assert plan.target_nodes == ()
