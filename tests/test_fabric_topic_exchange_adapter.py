from mpreg.core.model import PubSubSubscription, TopicPattern
from mpreg.fabric.adapters.topic_exchange import TopicExchangeCatalogAdapter


def test_topic_exchange_adapter_builds_delta() -> None:
    subscription = PubSubSubscription(
        subscription_id="sub-1",
        patterns=(TopicPattern(pattern="sensor.*"),),
        subscriber="client-1",
        created_at=10.0,
        get_backlog=False,
        backlog_seconds=0,
    )
    adapter = TopicExchangeCatalogAdapter(
        node_id="node-a",
        cluster_id="cluster-a",
        ttl_seconds=45.0,
    )

    delta = adapter.build_delta(subscription, now=100.0, update_id="update-1")

    assert delta.cluster_id == "cluster-a"
    assert delta.update_id == "update-1"
    assert len(delta.topics) == 1
    entry = delta.topics[0]
    assert entry.subscription_id == "sub-1"
    assert entry.node_id == "node-a"
    assert entry.patterns == ("sensor.*",)
    assert entry.ttl_seconds == 45.0


def test_topic_exchange_adapter_builds_removal() -> None:
    adapter = TopicExchangeCatalogAdapter(
        node_id="node-a",
        cluster_id="cluster-a",
    )

    delta = adapter.build_removal("sub-2", now=100.0, update_id="update-2")

    assert delta.cluster_id == "cluster-a"
    assert delta.update_id == "update-2"
    assert delta.topic_removals == ("sub-2",)
