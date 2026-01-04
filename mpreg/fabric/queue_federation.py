"""Fabric-native queue federation manager and routing helpers."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from mpreg.core.message_queue import (
    DeliveryGuarantee as QueueDeliveryGuarantee,
)
from mpreg.core.message_queue import (
    DeliveryResult,
    QueueConfiguration,
    QueuedMessage,
)
from mpreg.core.message_queue_manager import MessageQueueManager
from mpreg.core.task_manager import ManagedObject
from mpreg.core.topic_taxonomy import TopicValidator
from mpreg.datastructures.message_structures import MessageId
from mpreg.datastructures.type_aliases import (
    ClusterId,
    QueueName,
    SubscriberId,
    Timestamp,
)
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.fabric.announcers import FabricQueueAnnouncer
from mpreg.fabric.catalog import QueueEndpoint, QueueHealth
from mpreg.fabric.cluster_messenger import ClusterMessenger
from mpreg.fabric.index import QueueQuery, RoutingIndex
from mpreg.fabric.message import DeliveryGuarantee as FabricDeliveryGuarantee
from mpreg.fabric.message import MessageType, UnifiedMessage
from mpreg.fabric.queue_messages import (
    QueueFederationAck,
    QueueFederationKind,
    QueueFederationRequest,
    QueueFederationSubscription,
    QueueMessageOptions,
    queue_message_from_dict,
)


class FabricQueueDeliveryStatus(Enum):
    """Delivery status for fabric queue federation."""

    PENDING = "pending"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"
    ACKNOWLEDGED = "acknowledged"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class FabricQueueSubscription:
    """Subscription spanning local and remote queues."""

    subscription_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    subscriber_id: SubscriberId = ""
    queue_pattern: str = ""
    topic_pattern: str = ""
    source_cluster: ClusterId = ""
    target_clusters: set[ClusterId] = field(default_factory=set)
    delivery_guarantee: QueueDeliveryGuarantee = QueueDeliveryGuarantee.AT_LEAST_ONCE
    auto_acknowledge: bool = True
    created_at: Timestamp = field(default_factory=time.time)
    callback: Callable[[QueuedMessage], None] | None = None
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class FabricQueueInFlight:
    """Message in-flight across the fabric with acknowledgment tracking."""

    request: QueueFederationRequest
    federation_path: list[ClusterId] = field(default_factory=list)
    vector_clock: VectorClock = field(default_factory=VectorClock.empty)
    status: FabricQueueDeliveryStatus = FabricQueueDeliveryStatus.PENDING
    sent_at: Timestamp = field(default_factory=time.time)
    acknowledged_by: set[ClusterId] = field(default_factory=set)
    required_cluster_acks: int = 1
    timeout_seconds: float = 60.0

    def is_timeout_exceeded(self) -> bool:
        return (time.time() - self.sent_at) > self.timeout_seconds

    def is_acknowledged(self) -> bool:
        return len(self.acknowledged_by) >= self.required_cluster_acks


@dataclass(slots=True)
class FabricQueueStatistics:
    """Statistics for fabric queue federation."""

    total_federated_messages: int = 0
    successful_deliveries: int = 0
    failed_deliveries: int = 0
    acknowledgments_received: int = 0
    active_subscriptions: int = 0
    known_remote_queues: int = 0
    last_activity: Timestamp = field(default_factory=time.time)

    def success_rate(self) -> float:
        total = self.successful_deliveries + self.failed_deliveries
        return self.successful_deliveries / total if total else 0.0


@dataclass(slots=True)
class FabricQueueFederationManager(ManagedObject):
    """Fabric-native queue federation manager."""

    cluster_id: ClusterId
    node_id: str
    routing_index: RoutingIndex
    queue_manager: MessageQueueManager
    queue_announcer: FabricQueueAnnouncer
    messenger: ClusterMessenger
    allowed_clusters: frozenset[ClusterId] | None = None

    subscriptions: dict[str, FabricQueueSubscription] = field(default_factory=dict)
    in_flight: dict[str, FabricQueueInFlight] = field(default_factory=dict)
    stats: FabricQueueStatistics = field(default_factory=FabricQueueStatistics)
    _local_subscription_handles: dict[str, list[tuple[QueueName, str]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _remote_subscription_handles: dict[str, list[tuple[QueueName, ClusterId]]] = field(
        default_factory=lambda: defaultdict(list)
    )

    async def create_queue(
        self,
        queue_name: QueueName,
        config: QueueConfiguration | None = None,
        *,
        advertise: bool = True,
    ) -> bool:
        created = await self.queue_manager.create_queue(queue_name, config)
        if created and advertise:
            await self._advertise_queue(queue_name, config)
        return created

    async def advertise_existing_queues(self) -> None:
        for queue_name, config in self.queue_manager.queue_configs.items():
            await self._advertise_queue(queue_name, config)

    async def subscribe_globally(
        self,
        subscriber_id: SubscriberId,
        queue_pattern: str,
        topic_pattern: str,
        *,
        delivery_guarantee: QueueDeliveryGuarantee = QueueDeliveryGuarantee.AT_LEAST_ONCE,
        callback: Callable[[QueuedMessage], None] | None = None,
        auto_acknowledge: bool = True,
        target_clusters: set[ClusterId] | None = None,
        **metadata: str,
    ) -> str:
        subscription = FabricQueueSubscription(
            subscriber_id=subscriber_id,
            queue_pattern=queue_pattern,
            topic_pattern=topic_pattern,
            source_cluster=self.cluster_id,
            target_clusters=target_clusters or set(),
            delivery_guarantee=delivery_guarantee,
            auto_acknowledge=auto_acknowledge,
            callback=callback,
            metadata=metadata,
        )
        self.subscriptions[subscription.subscription_id] = subscription
        await self._setup_local_subscriptions(subscription)
        await self._setup_remote_subscriptions(subscription)
        self.stats.active_subscriptions += 1
        return subscription.subscription_id

    async def discover_queues(
        self, queue_pattern: str | None = None
    ) -> dict[ClusterId, list[QueueEndpoint]]:
        matching: dict[ClusterId, list[QueueEndpoint]] = defaultdict(list)
        for entry in self.routing_index.catalog.queues.entries():
            if queue_pattern and not self._queue_matches_pattern(
                entry.queue_name, queue_pattern
            ):
                continue
            matching[entry.cluster_id].append(entry)
        return dict(matching)

    def get_federation_statistics(self) -> FabricQueueStatistics:
        remote_entries = [
            entry
            for entry in self.routing_index.catalog.queues.entries()
            if entry.cluster_id != self.cluster_id
        ]
        self.stats.known_remote_queues = len(remote_entries)
        self.stats.active_subscriptions = len(self.subscriptions)
        return self.stats

    async def send_message_globally(
        self,
        queue_name: QueueName,
        topic: str,
        payload: Any,
        *,
        delivery_guarantee: QueueDeliveryGuarantee = QueueDeliveryGuarantee.AT_LEAST_ONCE,
        target_cluster: ClusterId | None = None,
        required_cluster_acks: int = 1,
        options: QueueMessageOptions | None = None,
    ) -> DeliveryResult:
        target = target_cluster or await self._discover_queue_cluster(queue_name)
        if target is None:
            return DeliveryResult(
                success=False,
                message_id=None,  # type: ignore[arg-type]
                error_message=f"Queue {queue_name} not found in fabric catalog",
            )

        if self.allowed_clusters is not None and target not in self.allowed_clusters:
            return DeliveryResult(
                success=False,
                message_id=None,  # type: ignore[arg-type]
                error_message=f"Target cluster {target} not permitted",
            )

        if target == self.cluster_id:
            return await self.queue_manager.send_message(
                queue_name,
                topic,
                payload,
                delivery_guarantee,
                **(options.to_dict() if options else {}),
            )

        return await self._send_remote_message(
            queue_name=queue_name,
            topic=topic,
            payload=payload,
            delivery_guarantee=delivery_guarantee,
            target_cluster=target,
            required_cluster_acks=required_cluster_acks,
            options=options or QueueMessageOptions(),
        )

    async def handle_fabric_message(
        self,
        message: UnifiedMessage,
        *,
        source_peer_url: str | None = None,
    ) -> None:
        try:
            payload = queue_message_from_dict(message.payload)
        except Exception as exc:
            logger.warning(
                "[{}] Invalid queue federation payload: {}",
                self.cluster_id,
                exc,
            )
            return

        if (
            self.allowed_clusters is not None
            and message.headers.source_cluster
            and message.headers.source_cluster not in self.allowed_clusters
        ):
            logger.warning(
                "[{}] Rejecting queue message from disallowed cluster {}",
                self.cluster_id,
                message.headers.source_cluster,
            )
            return

        if isinstance(payload, QueueFederationRequest):
            await self._handle_queue_request(payload, message, source_peer_url)
            return

        if isinstance(payload, QueueFederationAck):
            await self._handle_queue_ack(payload, message, source_peer_url)
            return

        if isinstance(payload, QueueFederationSubscription):
            await self._handle_queue_subscription(payload, message)
            return

    async def _handle_queue_request(
        self,
        request: QueueFederationRequest,
        message: UnifiedMessage,
        source_peer_url: str | None,
    ) -> None:
        if request.target_cluster != self.cluster_id:
            await self._forward_queue_message(
                message, request.target_cluster, source_peer_url
            )
            return

        if request.subscription_id and request.subscription_id in self.subscriptions:
            subscription = self.subscriptions[request.subscription_id]
            if subscription.callback is not None:
                queued = QueuedMessage(
                    id=MessageId.from_string(request.request_id),
                    topic=request.topic,
                    payload=request.payload,
                    delivery_guarantee=subscription.delivery_guarantee,
                )
                subscription.callback(queued)
            if request.ack_token:
                await self._send_ack(
                    request=request,
                    success=True,
                    subscriber_id=subscription.subscriber_id,
                )
            return

        result = await self.queue_manager.send_message(
            request.queue_name,
            request.topic,
            request.payload,
            request.delivery_guarantee,
            **request.options.to_dict(),
        )
        if request.ack_token:
            await self._send_ack(
                request=request,
                success=result.success,
                error_message=result.error_message,
            )

    async def _handle_queue_ack(
        self,
        ack: QueueFederationAck,
        message: UnifiedMessage,
        source_peer_url: str | None,
    ) -> None:
        if (
            message.headers.target_cluster
            and message.headers.target_cluster != self.cluster_id
        ):
            await self._forward_queue_message(
                message, message.headers.target_cluster, source_peer_url
            )
            return

        in_flight = self.in_flight.get(ack.ack_token)
        if not in_flight:
            return
        in_flight.acknowledged_by.add(ack.acknowledging_cluster)
        self.stats.acknowledgments_received += 1
        if in_flight.is_acknowledged():
            self.stats.successful_deliveries += 1
            self.in_flight.pop(ack.ack_token, None)

    async def _handle_queue_subscription(
        self,
        subscription: QueueFederationSubscription,
        message: UnifiedMessage,
    ) -> None:
        target_cluster = subscription.target_cluster or message.headers.target_cluster
        if target_cluster and target_cluster != self.cluster_id:
            await self._forward_queue_message(message, target_cluster, None)
            return

        local_queues = self.queue_manager.list_queues()
        for queue_name in local_queues:
            if not self._queue_matches_pattern(queue_name, subscription.queue_pattern):
                continue

            def _forward(message: QueuedMessage, qname: QueueName = queue_name) -> None:
                options = QueueMessageOptions(
                    priority=message.priority,
                    delay_seconds=message.delay_seconds,
                    visibility_timeout_seconds=message.visibility_timeout_seconds,
                    max_retries=message.max_retries,
                    acknowledgment_timeout_seconds=message.acknowledgment_timeout_seconds,
                    required_acknowledgments=message.required_acknowledgments,
                    headers=dict(message.headers),
                )
                request = QueueFederationRequest(
                    request_id=str(message.id),
                    queue_name=qname,
                    topic=message.topic,
                    payload=message.payload,
                    delivery_guarantee=subscription.delivery_guarantee,
                    source_cluster=self.cluster_id,
                    target_cluster=subscription.source_cluster,
                    subscription_id=subscription.subscription_id,
                    subscriber_id=subscription.subscriber_id,
                    ack_token=None,
                    required_cluster_acks=0,
                    options=options,
                )
                try:
                    asyncio.get_running_loop().create_task(self._send_request(request))
                except RuntimeError:
                    asyncio.run(self._send_request(request))

            subscription_id = self.queue_manager.subscribe_to_queue(
                queue_name,
                subscription.subscriber_id,
                subscription.topic_pattern,
                callback=_forward,
                auto_acknowledge=subscription.auto_acknowledge,
            )
            if subscription_id:
                self._local_subscription_handles[subscription.subscription_id].append(
                    (queue_name, subscription_id)
                )

    async def _setup_local_subscriptions(
        self, subscription: FabricQueueSubscription
    ) -> None:
        for queue_name in self.queue_manager.list_queues():
            if not self._queue_matches_pattern(queue_name, subscription.queue_pattern):
                continue
            subscription_id = self.queue_manager.subscribe_to_queue(
                queue_name,
                subscription.subscriber_id,
                subscription.topic_pattern,
                callback=subscription.callback,
                auto_acknowledge=subscription.auto_acknowledge,
                **subscription.metadata,
            )
            if subscription_id:
                self._local_subscription_handles[subscription.subscription_id].append(
                    (queue_name, subscription_id)
                )

    async def _setup_remote_subscriptions(
        self, subscription: FabricQueueSubscription
    ) -> None:
        entries = self.routing_index.catalog.queues.entries()
        for entry in entries:
            if entry.cluster_id == self.cluster_id:
                continue
            if (
                subscription.target_clusters
                and entry.cluster_id not in subscription.target_clusters
            ):
                continue
            if not self._queue_matches_pattern(
                entry.queue_name, subscription.queue_pattern
            ):
                continue

            request = QueueFederationSubscription(
                subscription_id=subscription.subscription_id,
                subscriber_id=subscription.subscriber_id,
                queue_pattern=subscription.queue_pattern,
                topic_pattern=subscription.topic_pattern,
                delivery_guarantee=subscription.delivery_guarantee,
                auto_acknowledge=subscription.auto_acknowledge,
                source_cluster=self.cluster_id,
                target_cluster=entry.cluster_id,
            )
            await self._send_subscription(request)
            self._remote_subscription_handles[subscription.subscription_id].append(
                (entry.queue_name, entry.cluster_id)
            )

    async def _discover_queue_cluster(self, queue_name: QueueName) -> ClusterId | None:
        if queue_name in self.queue_manager.list_queues():
            return self.cluster_id
        matches = self.routing_index.find_queues(QueueQuery(queue_name))
        if not matches:
            return None
        local_match = next(
            (entry for entry in matches if entry.cluster_id == self.cluster_id),
            None,
        )
        if local_match:
            return self.cluster_id
        matches.sort(
            key=lambda entry: (entry.health != QueueHealth.HEALTHY, entry.cluster_id)
        )
        return matches[0].cluster_id

    async def _send_remote_message(
        self,
        *,
        queue_name: QueueName,
        topic: str,
        payload: Any,
        delivery_guarantee: QueueDeliveryGuarantee,
        target_cluster: ClusterId,
        required_cluster_acks: int,
        options: QueueMessageOptions,
    ) -> DeliveryResult:
        request_id = str(uuid.uuid4())
        ack_token = str(uuid.uuid4()) if required_cluster_acks > 0 else None
        request = QueueFederationRequest(
            request_id=request_id,
            queue_name=queue_name,
            topic=topic,
            payload=payload,
            delivery_guarantee=delivery_guarantee,
            source_cluster=self.cluster_id,
            target_cluster=target_cluster,
            ack_token=ack_token,
            required_cluster_acks=required_cluster_acks,
            options=options,
        )
        if ack_token:
            self.in_flight[ack_token] = FabricQueueInFlight(
                request=request,
                federation_path=[],
                required_cluster_acks=required_cluster_acks,
            )
        return await self._send_request(request)

    async def _send_request(self, request: QueueFederationRequest) -> DeliveryResult:
        headers = self.messenger.next_headers(
            request.request_id,
            None,
            target_cluster=request.target_cluster,
        )
        if headers is None:
            return DeliveryResult(
                success=False,
                message_id=None,  # type: ignore[arg-type]
                error_message="fabric_queue_hop_budget_exhausted",
            )
        message = UnifiedMessage(
            message_id=request.request_id,
            topic=f"mpreg.queue.{request.queue_name}",
            message_type=MessageType.QUEUE,
            delivery=FabricDeliveryGuarantee.AT_LEAST_ONCE,
            payload=request.to_dict(),
            headers=headers,
            timestamp=time.time(),
        )
        success = await self.messenger.send_to_cluster(
            message, request.target_cluster, None
        )
        if success:
            self.stats.total_federated_messages += 1
            return DeliveryResult(
                success=True,
                message_id=MessageId.from_string(request.request_id),
                delivery_timestamp=time.time(),
                delivered_to={request.target_cluster},
            )
        self.stats.failed_deliveries += 1
        return DeliveryResult(
            success=False,
            message_id=None,  # type: ignore[arg-type]
            error_message="fabric_queue_send_failed",
        )

    async def _send_ack(
        self,
        *,
        request: QueueFederationRequest,
        success: bool,
        subscriber_id: SubscriberId | None = None,
        error_message: str | None = None,
    ) -> None:
        if not request.ack_token:
            return
        ack = QueueFederationAck(
            ack_token=request.ack_token,
            message_id=request.request_id,
            acknowledging_cluster=self.cluster_id,
            acknowledging_subscriber=subscriber_id or "",
            success=success,
            error_message=error_message,
        )
        headers = self.messenger.next_headers(
            request.ack_token,
            None,
            target_cluster=request.source_cluster,
        )
        if headers is None:
            return
        message = UnifiedMessage(
            message_id=request.ack_token,
            topic=f"mpreg.queue.ack.{request.queue_name}",
            message_type=MessageType.QUEUE,
            delivery=FabricDeliveryGuarantee.AT_LEAST_ONCE,
            payload=ack.to_dict(),
            headers=headers,
            timestamp=time.time(),
        )
        await self.messenger.send_to_cluster(message, request.source_cluster, None)

    async def _send_subscription(
        self, subscription: QueueFederationSubscription
    ) -> None:
        headers = self.messenger.next_headers(
            subscription.subscription_id,
            None,
            target_cluster=subscription.target_cluster,
        )
        if headers is None:
            return
        message = UnifiedMessage(
            message_id=subscription.subscription_id,
            topic="mpreg.queue.subscription",
            message_type=MessageType.QUEUE,
            delivery=FabricDeliveryGuarantee.AT_LEAST_ONCE,
            payload=subscription.to_dict(kind=QueueFederationKind.SUBSCRIBE),
            headers=headers,
            timestamp=time.time(),
        )
        await self.messenger.send_to_cluster(message, subscription.target_cluster, None)

    async def _forward_queue_message(
        self,
        message: UnifiedMessage,
        target_cluster: ClusterId,
        source_peer_url: str | None,
    ) -> None:
        headers = self.messenger.next_headers(
            message.headers.correlation_id,
            message.headers,
            target_cluster=target_cluster,
        )
        if headers is None:
            return
        forward_message = UnifiedMessage(
            message_id=message.message_id,
            topic=message.topic,
            message_type=MessageType.QUEUE,
            delivery=message.delivery,
            payload=message.payload,
            headers=headers,
            timestamp=message.timestamp,
        )
        await self.messenger.send_to_cluster(
            forward_message, target_cluster, source_peer_url
        )

    async def _advertise_queue(
        self, queue_name: QueueName, config: QueueConfiguration | None
    ) -> None:
        supported = frozenset(
            FabricDeliveryGuarantee(guarantee.value)
            for guarantee in QueueDeliveryGuarantee
        )
        config = config or QueueConfiguration(name=queue_name)
        endpoint = QueueEndpoint(
            queue_name=queue_name,
            cluster_id=self.cluster_id,
            node_id=self.node_id,
            delivery_guarantees=supported,
            health=QueueHealth.HEALTHY,
            current_subscribers=0,
            metadata={
                "queue_type": config.queue_type.value,
                "max_size": str(config.max_size),
            },
            advertised_at=time.time(),
            ttl_seconds=config.default_visibility_timeout_seconds,
        )
        await self.queue_announcer.advertise(endpoint)

    def _queue_matches_pattern(self, queue_name: str, pattern: str) -> bool:
        return TopicValidator.matches_pattern(queue_name, pattern)
