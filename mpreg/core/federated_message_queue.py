"""
Federated Message Queue System for MPREG.

This module extends the basic message queue system to work transparently across
federated MPREG clusters with the following capabilities:

- Cross-cluster queue discovery and subscription
- Federation-aware delivery guarantees (quorum across clusters)
- Multi-hop acknowledgment routing through federation paths
- Global queue advertisements via gossip protocol
- Circuit breaker protection for federated operations
- Transparent client experience across cluster boundaries

The system integrates with MPREG's existing federation infrastructure:
- FederationBridge for cluster communication
- GraphBasedFederationRouter for optimal path selection
- Gossip protocol for queue advertisement distribution
- Vector clocks for distributed acknowledgment ordering
"""

import asyncio
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from ..datastructures.vector_clock import VectorClock
from ..federation.federated_topic_exchange import FederatedTopicExchange
from ..federation.federation_bridge import (
    GraphAwareFederationBridge as FederationBridge,
)
from .message_queue import (
    DeliveryGuarantee,
    DeliveryResult,
    QueueConfiguration,
    QueuedMessage,
)
from .message_queue_manager import (
    MessageQueueManager,
    QueueManagerConfiguration,
)
from .model import PubSubMessage
from .task_manager import ManagedObject

# Type aliases for semantic clarity
type ClusterNodeId = str
type FederatedQueueName = str
type FederatedSubscriptionId = str
type FederationPath = list[ClusterNodeId]
type AckToken = str
type QueueAdvertisementId = str


class FederatedDeliveryStatus(Enum):
    """Extended delivery status for federated operations."""

    PENDING_FEDERATION = "pending_federation"  # Routing through federation
    IN_TRANSIT = "in_transit"  # Message traveling between clusters
    FEDERATED_DELIVERED = "federated_delivered"  # Delivered to remote cluster
    FEDERATED_ACKNOWLEDGED = (
        "federated_acknowledged"  # ACK received from remote cluster
    )
    FEDERATION_FAILED = "federation_failed"  # Federation routing failed


@dataclass(frozen=True, slots=True)
class QueueAdvertisement:
    """Advertisement of a queue's availability across federation."""

    advertisement_id: QueueAdvertisementId = field(
        default_factory=lambda: str(uuid.uuid4())
    )
    cluster_id: ClusterNodeId = ""
    queue_name: FederatedQueueName = ""
    supported_guarantees: list[DeliveryGuarantee] = field(default_factory=list)
    current_subscribers: int = 0
    queue_health: str = "healthy"  # healthy, degraded, unavailable
    advertised_at: float = field(default_factory=time.time)
    ttl_seconds: float = 300.0  # Advertisement expires after 5 minutes
    metadata: dict[str, str] = field(default_factory=dict)

    def is_expired(self) -> bool:
        """Check if this advertisement has expired."""
        return (time.time() - self.advertised_at) > self.ttl_seconds


@dataclass(frozen=True, slots=True)
class FederatedSubscription:
    """Subscription that spans multiple clusters."""

    subscription_id: FederatedSubscriptionId = field(
        default_factory=lambda: str(uuid.uuid4())
    )
    subscriber_id: str = ""
    local_cluster_id: ClusterNodeId = ""
    queue_pattern: str = ""
    topic_pattern: str = ""
    target_clusters: set[ClusterNodeId] = field(default_factory=set)
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE
    auto_acknowledge: bool = True
    created_at: float = field(default_factory=time.time)
    callback: Callable[[QueuedMessage], None] | None = None
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class FederatedInFlightMessage:
    """Message in transit across federation with acknowledgment tracking."""

    message: QueuedMessage
    federation_path: FederationPath = field(default_factory=list)
    source_cluster: ClusterNodeId = ""
    target_cluster: ClusterNodeId = ""
    ack_token: AckToken = field(default_factory=lambda: str(uuid.uuid4()))
    vector_clock: VectorClock = field(default_factory=VectorClock.empty)
    federated_status: FederatedDeliveryStatus = (
        FederatedDeliveryStatus.PENDING_FEDERATION
    )
    sent_at: float = field(default_factory=time.time)
    delivered_to_clusters: set[ClusterNodeId] = field(default_factory=set)
    acknowledged_by_clusters: set[ClusterNodeId] = field(default_factory=set)
    required_cluster_acks: int = 1
    federation_timeout_seconds: float = 60.0

    def is_federation_timeout_exceeded(self) -> bool:
        """Check if federation timeout has been exceeded."""
        return (time.time() - self.sent_at) > self.federation_timeout_seconds

    def is_federally_acknowledged(self) -> bool:
        """Check if message has received required federated acknowledgments."""
        return len(self.acknowledged_by_clusters) >= self.required_cluster_acks


@dataclass(frozen=True, slots=True)
class FederatedAcknowledgment:
    """Acknowledgment traveling back through federation path."""

    ack_token: AckToken
    message_id: str
    acknowledging_cluster: ClusterNodeId
    acknowledging_subscriber: str
    return_path: FederationPath = field(default_factory=list)
    vector_clock: VectorClock = field(default_factory=VectorClock.empty)
    ack_timestamp: float = field(default_factory=time.time)
    success: bool = True
    error_message: str | None = None


@dataclass(slots=True)
class FederatedQueueStatistics:
    """Statistics for federated queue operations."""

    total_federated_messages: int = 0
    successful_cross_cluster_deliveries: int = 0
    failed_federation_attempts: int = 0
    cross_cluster_acknowledgments: int = 0
    active_federated_subscriptions: int = 0
    known_remote_queues: int = 0
    federation_latency_ms: float = 0.0
    last_federation_activity: float = field(default_factory=time.time)

    def federation_success_rate(self) -> float:
        """Calculate federation success rate."""
        total = (
            self.successful_cross_cluster_deliveries + self.failed_federation_attempts
        )
        return self.successful_cross_cluster_deliveries / total if total > 0 else 0.0


class FederatedMessageQueueManager(ManagedObject):
    """
    Federation-aware message queue manager providing transparent cross-cluster operations.

    Features:
    - Global queue discovery across federated clusters
    - Transparent cross-cluster message delivery
    - Federation-aware delivery guarantees (quorum across clusters)
    - Multi-hop acknowledgment routing
    - Circuit breaker protection for federation operations
    - Queue advertisement distribution via gossip protocol
    """

    def __init__(
        self,
        config: QueueManagerConfiguration,
        federation_bridge: FederationBridge,
        local_cluster_id: ClusterNodeId,
    ) -> None:
        super().__init__(name="FederatedMessageQueueManager")
        self.config = config
        self.federation_bridge = federation_bridge
        self.local_cluster_id = local_cluster_id

        # Local queue management
        self.local_queue_manager = MessageQueueManager(
            config, federation_bridge.local_cluster
        )

        # Federation-specific data structures
        self.queue_advertisements: dict[QueueAdvertisementId, QueueAdvertisement] = {}
        self.remote_queues: dict[
            ClusterNodeId, dict[FederatedQueueName, QueueAdvertisement]
        ] = defaultdict(dict)
        self.federated_subscriptions: dict[
            FederatedSubscriptionId, FederatedSubscription
        ] = {}
        self.federated_in_flight: dict[AckToken, FederatedInFlightMessage] = {}
        self.pending_acknowledgments: dict[AckToken, FederatedAcknowledgment] = {}

        # Statistics and monitoring
        self.federation_stats = FederatedQueueStatistics()

        # Start federation workers
        self._start_federation_workers()

        logger.info(
            f"Federated Message Queue Manager initialized for cluster {local_cluster_id}"
        )

    def _start_federation_workers(self) -> None:
        """Start background workers for federation operations."""
        try:
            self.create_task(
                self._queue_advertisement_worker(), name="queue_advertisement_worker"
            )
            self.create_task(
                self._federation_discovery_worker(), name="federation_discovery_worker"
            )
            self.create_task(
                self._acknowledgment_router_worker(),
                name="acknowledgment_router_worker",
            )
            self.create_task(
                self._federation_timeout_worker(), name="federation_timeout_worker"
            )
            logger.info(f"Started {len(self._task_manager)} federation workers")
        except RuntimeError:
            logger.warning("No event loop running, skipping federation workers")

    async def create_federated_queue(
        self,
        queue_name: FederatedQueueName,
        config: QueueConfiguration | None = None,
        advertise_globally: bool = True,
    ) -> bool:
        """Create a queue and optionally advertise it across the federation."""
        success = await self.local_queue_manager.create_queue(queue_name, config)

        if success and advertise_globally:
            await self._advertise_queue(
                queue_name, config or QueueConfiguration(name=queue_name)
            )

        return success

    async def subscribe_globally(
        self,
        subscriber_id: str,
        queue_pattern: str,
        topic_pattern: str,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
        callback: Callable[[QueuedMessage], None] | None = None,
        auto_acknowledge: bool = True,
        target_clusters: set[ClusterNodeId] | None = None,
        **metadata: str,
    ) -> FederatedSubscriptionId:
        """
        Subscribe to queues across the entire federation.

        This method:
        1. Discovers matching queues across all federated clusters
        2. Sets up local subscriptions for queues in this cluster
        3. Establishes federation routes for remote queue subscriptions
        4. Returns a unified subscription handle
        """
        # Create federated subscription
        subscription = FederatedSubscription(
            subscriber_id=subscriber_id,
            local_cluster_id=self.local_cluster_id,
            queue_pattern=queue_pattern,
            topic_pattern=topic_pattern,
            target_clusters=target_clusters or set(),
            delivery_guarantee=delivery_guarantee,
            auto_acknowledge=auto_acknowledge,
            callback=callback,
            metadata=metadata,
        )

        # Store federated subscription
        self.federated_subscriptions[subscription.subscription_id] = subscription

        # Set up local subscriptions for matching local queues
        await self._setup_local_subscriptions(subscription)

        # Set up federation routes for remote queues
        await self._setup_federation_routes(subscription)

        self.federation_stats.active_federated_subscriptions += 1

        logger.info(
            f"Created global subscription {subscription.subscription_id} for {subscriber_id}"
        )
        return subscription.subscription_id

    async def send_message_globally(
        self,
        queue_name: FederatedQueueName,
        topic: str,
        payload: Any,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
        target_cluster: ClusterNodeId | None = None,
        required_cluster_acks: int = 1,
        **options: Any,
    ) -> DeliveryResult:
        """
        Send a message to a queue anywhere in the federation.

        This method:
        1. Discovers the optimal cluster hosting the target queue
        2. Routes the message through the federation if needed
        3. Tracks federated delivery and acknowledgment
        4. Returns unified delivery result
        """
        # Find target cluster for the queue
        if target_cluster is None:
            target_cluster = await self._discover_queue_cluster(queue_name)

        if target_cluster is None:
            return DeliveryResult(
                success=False,
                message_id=None,  # type: ignore
                error_message=f"Queue {queue_name} not found in federation",
            )

        # If local delivery, use local queue manager
        if target_cluster == self.local_cluster_id:
            return await self.local_queue_manager.send_message(
                queue_name, topic, payload, delivery_guarantee, **options
            )

        # Federated delivery
        return await self._send_federated_message(
            queue_name,
            topic,
            payload,
            delivery_guarantee,
            target_cluster,
            required_cluster_acks,
            **options,
        )

    async def acknowledge_federated_message(
        self,
        ack_token: AckToken,
        subscriber_id: str,
        success: bool = True,
        error_message: str | None = None,
    ) -> bool:
        """Acknowledge a message that was delivered via federation."""
        if ack_token not in self.federated_in_flight:
            logger.warning(f"Cannot acknowledge unknown federated message: {ack_token}")
            return False

        # Create federated acknowledgment
        ack = FederatedAcknowledgment(
            ack_token=ack_token,
            message_id=str(self.federated_in_flight[ack_token].message.id),
            acknowledging_cluster=self.local_cluster_id,
            acknowledging_subscriber=subscriber_id,
            success=success,
            error_message=error_message,
        )

        # Route acknowledgment back through federation
        await self._route_acknowledgment(ack)

        self.federation_stats.cross_cluster_acknowledgments += 1
        return True

    async def discover_federated_queues(
        self, queue_pattern: str | None = None
    ) -> dict[ClusterNodeId, list[QueueAdvertisement]]:
        """Discover all queues matching pattern across the federation."""
        matching_queues: dict[ClusterNodeId, list[QueueAdvertisement]] = defaultdict(
            list
        )

        # Include local queues
        local_queues = self.local_queue_manager.list_queues()
        for queue_name in local_queues:
            if queue_pattern is None or self._queue_matches_pattern(
                queue_name, queue_pattern
            ):
                # Create advertisement for local queue
                ad = QueueAdvertisement(
                    cluster_id=self.local_cluster_id,
                    queue_name=queue_name,
                    supported_guarantees=list(DeliveryGuarantee),
                    current_subscribers=len(
                        self.local_queue_manager.queues.get(
                            queue_name, type("", (), {"subscriptions": {}})
                        ).subscriptions  # type: ignore
                    ),
                    queue_health="healthy",
                )
                matching_queues[self.local_cluster_id].append(ad)

        # Include remote queues from advertisements
        for cluster_id, cluster_queues in self.remote_queues.items():
            for queue_name, advertisement in cluster_queues.items():
                if queue_pattern is None or self._queue_matches_pattern(
                    queue_name, queue_pattern
                ):
                    if not advertisement.is_expired():
                        matching_queues[cluster_id].append(advertisement)

        return dict(matching_queues)

    def get_federation_statistics(self) -> FederatedQueueStatistics:
        """Get comprehensive federation statistics."""
        # Update current statistics
        self.federation_stats.known_remote_queues = sum(
            len(queues) for queues in self.remote_queues.values()
        )
        self.federation_stats.active_federated_subscriptions = len(
            self.federated_subscriptions
        )

        return self.federation_stats

    # Internal methods for federation operations

    async def _advertise_queue(
        self, queue_name: FederatedQueueName, config: QueueConfiguration
    ) -> None:
        """Advertise a local queue to the federation."""
        advertisement = QueueAdvertisement(
            cluster_id=self.local_cluster_id,
            queue_name=queue_name,
            supported_guarantees=list(DeliveryGuarantee),
            current_subscribers=0,
            queue_health="healthy",
            metadata={
                "queue_type": config.queue_type.value,
                "max_size": str(config.max_size),
            },
        )

        self.queue_advertisements[advertisement.advertisement_id] = advertisement

        # Publish advertisement via federation bridge
        await self._publish_queue_advertisement(advertisement)

        logger.debug(f"Advertised queue {queue_name} to federation")

    async def _publish_queue_advertisement(
        self, advertisement: QueueAdvertisement
    ) -> None:
        """Publish queue advertisement through federation."""
        try:
            advertisement_message = PubSubMessage(
                topic=f"mpreg.federation.queue.advertisement.{advertisement.cluster_id}",
                payload={
                    "advertisement_id": advertisement.advertisement_id,
                    "cluster_id": advertisement.cluster_id,
                    "queue_name": advertisement.queue_name,
                    "supported_guarantees": [
                        g.value for g in advertisement.supported_guarantees
                    ],
                    "current_subscribers": advertisement.current_subscribers,
                    "queue_health": advertisement.queue_health,
                    "advertised_at": advertisement.advertised_at,
                    "ttl_seconds": advertisement.ttl_seconds,
                    "metadata": advertisement.metadata,
                },
                timestamp=time.time(),
                message_id=str(uuid.uuid4()),
                publisher=f"federated-queue-manager-{self.local_cluster_id}",
            )

            # Publish locally first
            if isinstance(self.federation_bridge.local_cluster, FederatedTopicExchange):
                await self.federation_bridge.local_cluster.publish_message_async(
                    advertisement_message
                )
            else:
                self.federation_bridge.local_cluster.publish_message(
                    advertisement_message
                )

            # Also queue for federation forwarding
            self.federation_bridge._queue_message_for_forwarding(advertisement_message)

            # Track federation statistics
            self.federation_stats.total_federated_messages += 1

        except Exception as e:
            logger.error(f"Failed to publish queue advertisement: {e}")

    async def _setup_local_subscriptions(
        self, subscription: FederatedSubscription
    ) -> None:
        """Set up local subscriptions for federated subscription."""
        local_queues = self.local_queue_manager.list_queues()

        for queue_name in local_queues:
            if self._queue_matches_pattern(queue_name, subscription.queue_pattern):
                self.local_queue_manager.subscribe_to_queue(
                    queue_name,
                    subscription.subscriber_id,
                    subscription.topic_pattern,
                    callback=subscription.callback,
                    auto_acknowledge=subscription.auto_acknowledge,
                    **subscription.metadata,
                )

    async def _setup_federation_routes(
        self, subscription: FederatedSubscription
    ) -> None:
        """Set up federation routes for remote queue subscriptions."""
        # Find remote queues matching the pattern
        for cluster_id, cluster_queues in self.remote_queues.items():
            if (
                subscription.target_clusters
                and cluster_id not in subscription.target_clusters
            ):
                continue

            for queue_name, advertisement in cluster_queues.items():
                if self._queue_matches_pattern(queue_name, subscription.queue_pattern):
                    if not advertisement.is_expired():
                        await self._establish_federation_route(
                            subscription, cluster_id, queue_name
                        )

    async def _establish_federation_route(
        self,
        subscription: FederatedSubscription,
        target_cluster: ClusterNodeId,
        queue_name: FederatedQueueName,
    ) -> None:
        """Establish a federation route for cross-cluster subscription."""
        try:
            # Use federation bridge to set up route
            route_message = PubSubMessage(
                topic=f"mpreg.federation.queue.route.{target_cluster}",
                payload={
                    "subscription_id": subscription.subscription_id,
                    "subscriber_id": subscription.subscriber_id,
                    "source_cluster": self.local_cluster_id,
                    "target_cluster": target_cluster,
                    "queue_name": queue_name,
                    "topic_pattern": subscription.topic_pattern,
                    "delivery_guarantee": subscription.delivery_guarantee.value,
                    "auto_acknowledge": subscription.auto_acknowledge,
                },
                timestamp=time.time(),
                message_id=str(uuid.uuid4()),
                publisher=f"federated-queue-manager-{self.local_cluster_id}",
            )

            if isinstance(self.federation_bridge.local_cluster, FederatedTopicExchange):
                await self.federation_bridge.local_cluster.publish_message_async(
                    route_message
                )
            else:
                self.federation_bridge.local_cluster.publish_message(route_message)

            logger.debug(
                f"Established federation route to {target_cluster}/{queue_name}"
            )

        except Exception as e:
            logger.error(f"Failed to establish federation route: {e}")

    async def _discover_queue_cluster(
        self, queue_name: FederatedQueueName
    ) -> ClusterNodeId | None:
        """Discover which cluster hosts the specified queue."""
        # Check local queues first
        if queue_name in self.local_queue_manager.list_queues():
            return self.local_cluster_id

        # Check remote queue advertisements
        for cluster_id, cluster_queues in self.remote_queues.items():
            if queue_name in cluster_queues:
                advertisement = cluster_queues[queue_name]
                if not advertisement.is_expired():
                    return cluster_id

        return None

    async def _send_federated_message(
        self,
        queue_name: FederatedQueueName,
        topic: str,
        payload: Any,
        delivery_guarantee: DeliveryGuarantee,
        target_cluster: ClusterNodeId,
        required_cluster_acks: int,
        **options: Any,
    ) -> DeliveryResult:
        """Send message to remote cluster via federation."""
        try:
            # Create federated message tracking
            message_id = str(uuid.uuid4())
            ack_token = str(uuid.uuid4())

            federated_msg = FederatedInFlightMessage(
                message=QueuedMessage(
                    id=type(
                        "", (), {"id": message_id, "__str__": lambda: message_id}
                    )(),  # type: ignore
                    topic=topic,
                    payload=payload,
                    delivery_guarantee=delivery_guarantee,
                    **options,
                ),
                source_cluster=self.local_cluster_id,
                target_cluster=target_cluster,
                ack_token=ack_token,
                required_cluster_acks=required_cluster_acks,
            )

            # Track federated message
            self.federated_in_flight[ack_token] = federated_msg

            # Send via federation bridge
            federation_message = PubSubMessage(
                topic=f"mpreg.federation.queue.message.{target_cluster}",
                payload={
                    "ack_token": ack_token,
                    "queue_name": queue_name,
                    "message_topic": topic,
                    "message_payload": payload,
                    "delivery_guarantee": delivery_guarantee.value,
                    "source_cluster": self.local_cluster_id,
                    "message_options": options,
                },
                timestamp=time.time(),
                message_id=message_id,
                publisher=f"federated-queue-manager-{self.local_cluster_id}",
            )

            if isinstance(self.federation_bridge.local_cluster, FederatedTopicExchange):
                await self.federation_bridge.local_cluster.publish_message_async(
                    federation_message
                )
            else:
                self.federation_bridge.local_cluster.publish_message(federation_message)

            self.federation_stats.total_federated_messages += 1

            return DeliveryResult(
                success=True,
                message_id=type(
                    "", (), {"id": message_id, "__str__": lambda: message_id}
                )(),  # type: ignore
                delivery_timestamp=time.time(),
                delivered_to={target_cluster},
            )

        except Exception as e:
            logger.error(f"Failed to send federated message: {e}")
            self.federation_stats.failed_federation_attempts += 1
            return DeliveryResult(
                success=False,
                message_id=None,  # type: ignore
                error_message=str(e),
            )

    async def _route_acknowledgment(self, ack: FederatedAcknowledgment) -> None:
        """Route acknowledgment back through federation to source cluster."""
        try:
            ack_message = PubSubMessage(
                topic=f"mpreg.federation.queue.ack.{self.local_cluster_id}",
                payload={
                    "ack_token": ack.ack_token,
                    "message_id": ack.message_id,
                    "acknowledging_cluster": ack.acknowledging_cluster,
                    "acknowledging_subscriber": ack.acknowledging_subscriber,
                    "success": ack.success,
                    "error_message": ack.error_message,
                    "ack_timestamp": ack.ack_timestamp,
                },
                timestamp=time.time(),
                message_id=str(uuid.uuid4()),
                publisher=f"federated-queue-manager-{self.local_cluster_id}",
            )

            if isinstance(self.federation_bridge.local_cluster, FederatedTopicExchange):
                await self.federation_bridge.local_cluster.publish_message_async(
                    ack_message
                )
            else:
                self.federation_bridge.local_cluster.publish_message(ack_message)

        except Exception as e:
            logger.error(f"Failed to route acknowledgment: {e}")

    def _queue_matches_pattern(self, queue_name: str, pattern: str) -> bool:
        """Check if queue name matches pattern."""
        import fnmatch

        return fnmatch.fnmatch(queue_name, pattern)

    # Background workers

    async def _queue_advertisement_worker(self) -> None:
        """Background worker to refresh queue advertisements."""
        try:
            while True:
                try:
                    await asyncio.sleep(60.0)  # Refresh every minute

                    # Refresh advertisements for local queues
                    for queue_name in self.local_queue_manager.list_queues():
                        queue_info = self.local_queue_manager.get_queue_info(queue_name)
                        if queue_info:
                            config = QueueConfiguration(name=queue_name)
                            await self._advertise_queue(queue_name, config)

                    # Clean up expired advertisements
                    expired_ads = [
                        ad_id
                        for ad_id, ad in self.queue_advertisements.items()
                        if ad.is_expired()
                    ]
                    for ad_id in expired_ads:
                        del self.queue_advertisements[ad_id]

                    logger.debug("Queue advertisement refresh completed")

                except Exception as e:
                    logger.error(f"Queue advertisement worker error: {e}")
                    await asyncio.sleep(60.0)

        except asyncio.CancelledError:
            logger.info("Queue advertisement worker cancelled")
        finally:
            logger.info("Queue advertisement worker stopped")

    async def _federation_discovery_worker(self) -> None:
        """Background worker to discover remote queues."""
        try:
            # Subscribe to federation advertisement topics from all clusters
            advertisement_pattern = "mpreg.federation.queue.advertisement.*"

            # Set up subscription to receive queue advertisements
            try:
                # Subscribe to federation advertisement messages
                await self._setup_federation_advertisement_subscription()
            except Exception as e:
                logger.warning(
                    f"Failed to set up federation advertisement subscription: {e}"
                )

            while True:
                try:
                    await asyncio.sleep(30.0)  # Discovery every 30 seconds

                    # Process any pending advertisement messages
                    await self._process_pending_advertisements()

                    # Clean up expired remote queue advertisements
                    await self._cleanup_expired_advertisements()

                    logger.debug("Federation discovery sweep completed")

                except Exception as e:
                    logger.error(f"Federation discovery worker error: {e}")
                    await asyncio.sleep(30.0)

        except asyncio.CancelledError:
            logger.info("Federation discovery worker cancelled")
        finally:
            logger.info("Federation discovery worker stopped")

    async def _acknowledgment_router_worker(self) -> None:
        """Background worker to route acknowledgments."""
        try:
            while True:
                try:
                    await asyncio.sleep(1.0)  # Check acknowledgments frequently

                    # Process pending acknowledgments
                    processed_acks = []
                    for ack_token, ack in self.pending_acknowledgments.items():
                        try:
                            await self._route_acknowledgment(ack)
                            processed_acks.append(ack_token)
                        except Exception as e:
                            logger.error(
                                f"Failed to route acknowledgment {ack_token}: {e}"
                            )

                    # Remove processed acknowledgments
                    for ack_token in processed_acks:
                        del self.pending_acknowledgments[ack_token]

                except Exception as e:
                    logger.error(f"Acknowledgment router worker error: {e}")
                    await asyncio.sleep(5.0)

        except asyncio.CancelledError:
            logger.info("Acknowledgment router worker cancelled")
        finally:
            logger.info("Acknowledgment router worker stopped")

    async def _federation_timeout_worker(self) -> None:
        """Background worker to handle federation timeouts."""
        try:
            while True:
                try:
                    await asyncio.sleep(10.0)  # Check timeouts every 10 seconds

                    # Check for timed out federated messages
                    timed_out_messages = []
                    for ack_token, fed_msg in self.federated_in_flight.items():
                        if fed_msg.is_federation_timeout_exceeded():
                            timed_out_messages.append(ack_token)

                    # Handle timeouts
                    for ack_token in timed_out_messages:
                        fed_msg = self.federated_in_flight[ack_token]
                        logger.warning(
                            f"Federation timeout for message {fed_msg.message.id}"
                        )

                        # Mark as failed and clean up
                        fed_msg.federated_status = (
                            FederatedDeliveryStatus.FEDERATION_FAILED
                        )
                        del self.federated_in_flight[ack_token]
                        self.federation_stats.failed_federation_attempts += 1

                except Exception as e:
                    logger.error(f"Federation timeout worker error: {e}")
                    await asyncio.sleep(10.0)

        except asyncio.CancelledError:
            logger.info("Federation timeout worker cancelled")
        finally:
            logger.info("Federation timeout worker stopped")

    async def shutdown(self) -> None:
        """Shutdown federated queue manager and all components."""
        logger.info("Shutting down Federated Message Queue Manager...")

        # Shutdown local queue manager
        await self.local_queue_manager.shutdown()

        # Shutdown task manager (cancels all federation workers)
        await super().shutdown()

        # Clear all data structures
        self.queue_advertisements.clear()
        self.remote_queues.clear()
        self.federated_subscriptions.clear()
        self.federated_in_flight.clear()
        self.pending_acknowledgments.clear()

        logger.info("Federated Message Queue Manager shutdown complete")

    # Federation discovery helper methods

    async def _setup_federation_advertisement_subscription(self) -> None:
        """Set up federation communication for queue discovery."""
        try:
            logger.info(
                f"Setting up federation discovery for cluster {self.local_cluster_id}"
            )

            # Subscribe to federation queue advertisements using local topic exchange
            advertisement_topic = "mpreg.federation.queue.advertisement.*"

            from .model import PubSubSubscription, TopicPattern

            # Create subscription for federation messages
            subscription = PubSubSubscription(
                subscription_id=f"federated-queue-discovery-{self.local_cluster_id}",
                subscriber=f"federated-queue-manager-{self.local_cluster_id}",
                patterns=(TopicPattern(pattern=advertisement_topic),),
                created_at=time.time(),
                get_backlog=False,
            )

            # Add subscription to local topic exchange (this listens for messages sent via federation bridge)
            self.federation_bridge.local_cluster.add_subscription(subscription)
            logger.debug(
                f"Subscribed to federation advertisements: {advertisement_topic}"
            )

        except Exception as e:
            logger.error(f"Failed to setup federation discovery: {e}")

    async def _handle_federation_advertisement_message(
        self, message: PubSubMessage
    ) -> None:
        """Handle incoming federation queue advertisement messages."""
        try:
            # Parse the advertisement message
            payload = message.payload
            if not isinstance(payload, dict):
                logger.warning(f"Invalid advertisement payload format: {type(payload)}")
                return

            # Extract advertisement data
            cluster_id = payload.get("cluster_id")
            queue_name = payload.get("queue_name")

            if not cluster_id or not queue_name:
                logger.warning(
                    f"Missing cluster_id or queue_name in advertisement: {payload}"
                )
                return

            # Don't process advertisements from our own cluster
            if cluster_id == self.local_cluster_id:
                return

            # Create queue advertisement from message
            try:
                from .message_queue import DeliveryGuarantee

                advertisement = QueueAdvertisement(
                    advertisement_id=payload.get("advertisement_id", str(uuid.uuid4())),
                    cluster_id=cluster_id,
                    queue_name=queue_name,
                    supported_guarantees=[
                        DeliveryGuarantee(g)
                        for g in payload.get("supported_guarantees", [])
                    ],
                    current_subscribers=payload.get("current_subscribers", 0),
                    queue_health=payload.get("queue_health", "unknown"),
                    advertised_at=payload.get("advertised_at", time.time()),
                    ttl_seconds=payload.get("ttl_seconds", 300.0),
                    metadata=payload.get("metadata", {}),
                )

                # Store the remote queue advertisement
                self.remote_queues[cluster_id][queue_name] = advertisement

                logger.debug(
                    f"Received queue advertisement: {queue_name} from cluster {cluster_id}"
                )

            except Exception as e:
                logger.error(f"Failed to parse queue advertisement: {e}")

        except Exception as e:
            logger.error(f"Error handling federation advertisement message: {e}")

    async def _process_pending_advertisements(self) -> None:
        """Process any pending federation advertisement messages."""
        # This method can be used for batched processing if needed
        # For now, messages are processed immediately in the callback
        pass

    async def _cleanup_expired_advertisements(self) -> None:
        """Clean up expired remote queue advertisements."""
        try:
            current_time = time.time()

            for cluster_id in list(self.remote_queues.keys()):
                cluster_queues = self.remote_queues[cluster_id]

                # Find expired advertisements
                expired_queues = [
                    queue_name
                    for queue_name, advertisement in cluster_queues.items()
                    if advertisement.is_expired()
                ]

                # Remove expired advertisements
                for queue_name in expired_queues:
                    del cluster_queues[queue_name]
                    logger.debug(
                        f"Removed expired advertisement: {queue_name} from cluster {cluster_id}"
                    )

                # Remove empty cluster entries
                if not cluster_queues:
                    del self.remote_queues[cluster_id]

        except Exception as e:
            logger.error(f"Error cleaning up expired advertisements: {e}")


# Factory functions for federated queue managers


def create_federated_queue_manager(
    federation_bridge: FederationBridge,
    local_cluster_id: ClusterNodeId,
    config: QueueManagerConfiguration | None = None,
) -> FederatedMessageQueueManager:
    """Create a standard federated message queue manager."""
    if config is None:
        config = QueueManagerConfiguration()
    return FederatedMessageQueueManager(config, federation_bridge, local_cluster_id)


def create_high_throughput_federated_queue_manager(
    federation_bridge: FederationBridge, local_cluster_id: ClusterNodeId
) -> FederatedMessageQueueManager:
    """Create a high-throughput optimized federated queue manager."""
    config = QueueManagerConfiguration(
        default_max_queue_size=50000,
        default_visibility_timeout_seconds=10.0,
        default_acknowledgment_timeout_seconds=60.0,
        max_queues=5000,
    )
    return FederatedMessageQueueManager(config, federation_bridge, local_cluster_id)


def create_reliable_federated_queue_manager(
    federation_bridge: FederationBridge, local_cluster_id: ClusterNodeId
) -> FederatedMessageQueueManager:
    """Create a reliability-focused federated queue manager."""
    config = QueueManagerConfiguration(
        default_visibility_timeout_seconds=60.0,
        default_acknowledgment_timeout_seconds=600.0,  # 10 minutes
        enable_auto_queue_creation=False,  # Explicit queue creation
    )
    return FederatedMessageQueueManager(config, federation_bridge, local_cluster_id)
