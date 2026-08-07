"""Adapter to emit topic subscription announcements into routing catalog deltas."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from mpreg.core.model import PubSubSubscription
from mpreg.core.topic_exchange import TopicExchange
from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    NodeId,
    SubscriptionId,
    Timestamp,
)

from ..catalog import TopicSubscription
from ..catalog_delta import RoutingCatalogDelta


@dataclass(slots=True)
class TopicExchangeCatalogAdapter:
    node_id: NodeId
    cluster_id: ClusterId
    ttl_seconds: DurationSeconds = 30.0

    @classmethod
    def for_exchange(
        cls,
        exchange: TopicExchange,
        *,
        ttl_seconds: DurationSeconds = 30.0,
    ) -> TopicExchangeCatalogAdapter:
        return cls(
            node_id=exchange.server_url,
            cluster_id=exchange.cluster_id,
            ttl_seconds=ttl_seconds,
        )

    def build_delta(
        self,
        subscription: PubSubSubscription,
        *,
        now: Timestamp | None = None,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta:
        timestamp = now if now is not None else time.time()
        entry = TopicSubscription(
            subscription_id=subscription.subscription_id,
            node_id=self.node_id,
            cluster_id=self.cluster_id,
            patterns=tuple(pattern.pattern for pattern in subscription.patterns),
            advertised_at=timestamp,
            ttl_seconds=self.ttl_seconds,
        )
        return RoutingCatalogDelta(
            update_id=update_id or str(uuid.uuid4()),
            cluster_id=self.cluster_id,
            sent_at=timestamp,
            topics=(entry,),
        )

    def build_removal(
        self,
        subscription_id: SubscriptionId,
        *,
        now: Timestamp | None = None,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta:
        timestamp = now if now is not None else time.time()
        return RoutingCatalogDelta(
            update_id=update_id or str(uuid.uuid4()),
            cluster_id=self.cluster_id,
            sent_at=timestamp,
            topic_removals=(subscription_id,),
        )

    def build_full_delta(
        self,
        exchange: TopicExchange,
        *,
        now: Timestamp | None = None,
        update_id: str | None = None,
    ) -> RoutingCatalogDelta:
        timestamp = now if now is not None else time.time()
        topics = tuple(
            TopicSubscription(
                subscription_id=sub.subscription_id,
                node_id=self.node_id,
                cluster_id=self.cluster_id,
                patterns=tuple(pattern.pattern for pattern in sub.patterns),
                advertised_at=timestamp,
                ttl_seconds=self.ttl_seconds,
            )
            for sub in exchange.subscriptions.values()
        )
        return RoutingCatalogDelta(
            update_id=update_id or str(uuid.uuid4()),
            cluster_id=self.cluster_id,
            sent_at=timestamp,
            topics=topics,
        )
