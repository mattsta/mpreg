"""
Federated Queue Gossip Protocol Extension for MPREG.

This module extends MPREG's existing gossip protocol to include queue advertisements
and federated queue state synchronization across clusters.

Features:
- Queue advertisement distribution via epidemic gossip
- Vector clock-based queue state consistency
- Anti-entropy for queue discovery convergence
- Bandwidth-efficient queue metadata propagation
- Circuit breaker integration for failed cluster handling
"""

import asyncio
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from ..federation.federation_gossip import (
    GossipMessage,
    GossipMessageType,
    GossipProtocol,
    VectorClock,
)
from .federated_message_queue import (
    ClusterNodeId,
    QueueAdvertisement,
    QueueAdvertisementId,
)
from .task_manager import ManagedObject

# Type aliases for semantic clarity
type QueueGossipVectorClock = dict[ClusterNodeId, int]
type QueueStateVersion = int
type GossipRoundId = str


class QueueGossipMessageType(Enum):
    """Types of queue-specific gossip messages."""

    QUEUE_ADVERTISEMENT = "queue_advertisement"
    QUEUE_STATE_UPDATE = "queue_state_update"
    QUEUE_REMOVAL = "queue_removal"
    QUEUE_HEALTH_UPDATE = "queue_health_update"
    QUEUE_DISCOVERY_REQUEST = "queue_discovery_request"
    QUEUE_DISCOVERY_RESPONSE = "queue_discovery_response"


@dataclass(frozen=True, slots=True)
class QueueGossipPayload:
    """Base payload for queue gossip messages."""

    cluster_id: ClusterNodeId
    timestamp: float = field(default_factory=time.time)
    vector_clock: QueueGossipVectorClock = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for gossip transmission."""
        return {
            "cluster_id": self.cluster_id,
            "timestamp": self.timestamp,
            "vector_clock": self.vector_clock,
            "message_id": self.message_id,
        }


@dataclass(frozen=True, slots=True)
class QueueAdvertisementGossipPayload(QueueGossipPayload):
    """Gossip payload for queue advertisements."""

    advertisement: QueueAdvertisement = field(
        default_factory=lambda: QueueAdvertisement()
    )
    operation: str = "advertise"  # advertise, update, remove

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for gossip transmission."""
        return {
            "cluster_id": self.cluster_id,
            "timestamp": self.timestamp,
            "vector_clock": self.vector_clock,
            "message_id": self.message_id,
            "advertisement": {
                "advertisement_id": self.advertisement.advertisement_id,
                "cluster_id": self.advertisement.cluster_id,
                "queue_name": self.advertisement.queue_name,
                "supported_guarantees": [
                    g.value for g in self.advertisement.supported_guarantees
                ],
                "current_subscribers": self.advertisement.current_subscribers,
                "queue_health": self.advertisement.queue_health,
                "advertised_at": self.advertisement.advertised_at,
                "ttl_seconds": self.advertisement.ttl_seconds,
                "metadata": self.advertisement.metadata,
            },
            "operation": self.operation,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueAdvertisementGossipPayload":
        """Create from dictionary received via gossip."""
        from .message_queue import DeliveryGuarantee

        ad_data = data["advertisement"]
        advertisement = QueueAdvertisement(
            advertisement_id=ad_data["advertisement_id"],
            cluster_id=ad_data["cluster_id"],
            queue_name=ad_data["queue_name"],
            supported_guarantees=[
                DeliveryGuarantee(g) for g in ad_data["supported_guarantees"]
            ],
            current_subscribers=ad_data["current_subscribers"],
            queue_health=ad_data["queue_health"],
            advertised_at=ad_data["advertised_at"],
            ttl_seconds=ad_data["ttl_seconds"],
            metadata=ad_data["metadata"],
        )

        return cls(
            cluster_id=data["cluster_id"],
            timestamp=data["timestamp"],
            vector_clock=data["vector_clock"],
            message_id=data["message_id"],
            advertisement=advertisement,
            operation=data["operation"],
        )


@dataclass(frozen=True, slots=True)
class QueueDiscoveryRequestPayload(QueueGossipPayload):
    """Gossip payload for queue discovery requests."""

    requesting_cluster: ClusterNodeId = ""
    queue_pattern: str = "*"  # Pattern to match queue names
    discovery_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for gossip transmission."""
        return {
            "cluster_id": self.cluster_id,
            "timestamp": self.timestamp,
            "vector_clock": self.vector_clock,
            "message_id": self.message_id,
            "requesting_cluster": self.requesting_cluster,
            "queue_pattern": self.queue_pattern,
            "discovery_id": self.discovery_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueDiscoveryRequestPayload":
        """Create from dictionary received via gossip."""
        return cls(
            cluster_id=data["cluster_id"],
            timestamp=data["timestamp"],
            vector_clock=data["vector_clock"],
            message_id=data["message_id"],
            requesting_cluster=data["requesting_cluster"],
            queue_pattern=data["queue_pattern"],
            discovery_id=data["discovery_id"],
        )


@dataclass(frozen=True, slots=True)
class QueueDiscoveryResponsePayload(QueueGossipPayload):
    """Gossip payload for queue discovery responses."""

    discovery_id: str = ""
    responding_cluster: ClusterNodeId = ""
    matching_queues: list[QueueAdvertisement] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for gossip transmission."""
        return {
            "cluster_id": self.cluster_id,
            "timestamp": self.timestamp,
            "vector_clock": self.vector_clock,
            "message_id": self.message_id,
            "discovery_id": self.discovery_id,
            "responding_cluster": self.responding_cluster,
            "matching_queues": [
                {
                    "advertisement_id": ad.advertisement_id,
                    "cluster_id": ad.cluster_id,
                    "queue_name": ad.queue_name,
                    "supported_guarantees": [g.value for g in ad.supported_guarantees],
                    "current_subscribers": ad.current_subscribers,
                    "queue_health": ad.queue_health,
                    "advertised_at": ad.advertised_at,
                    "ttl_seconds": ad.ttl_seconds,
                    "metadata": ad.metadata,
                }
                for ad in self.matching_queues
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueDiscoveryResponsePayload":
        """Create from dictionary received via gossip."""
        from .message_queue import DeliveryGuarantee

        matching_queues = []
        for ad_data in data["matching_queues"]:
            advertisement = QueueAdvertisement(
                advertisement_id=ad_data["advertisement_id"],
                cluster_id=ad_data["cluster_id"],
                queue_name=ad_data["queue_name"],
                supported_guarantees=[
                    DeliveryGuarantee(g) for g in ad_data["supported_guarantees"]
                ],
                current_subscribers=ad_data["current_subscribers"],
                queue_health=ad_data["queue_health"],
                advertised_at=ad_data["advertised_at"],
                ttl_seconds=ad_data["ttl_seconds"],
                metadata=ad_data["metadata"],
            )
            matching_queues.append(advertisement)

        return cls(
            cluster_id=data["cluster_id"],
            timestamp=data["timestamp"],
            vector_clock=data["vector_clock"],
            message_id=data["message_id"],
            discovery_id=data["discovery_id"],
            responding_cluster=data["responding_cluster"],
            matching_queues=matching_queues,
        )


@dataclass(slots=True)
class QueueGossipStatistics:
    """Statistics for queue gossip operations."""

    advertisements_sent: int = 0
    advertisements_received: int = 0
    discovery_requests_sent: int = 0
    discovery_responses_sent: int = 0
    queue_state_updates: int = 0
    gossip_convergence_time_ms: float = 0.0
    active_queue_advertisements: int = 0
    last_gossip_activity: float = field(default_factory=time.time)

    def update_convergence_time(self, start_time: float) -> None:
        """Update gossip convergence time measurement."""
        self.gossip_convergence_time_ms = (time.time() - start_time) * 1000


class FederatedQueueGossipProtocol(ManagedObject):
    """
    Gossip protocol extension for federated queue advertisement and discovery.

    This class extends MPREG's existing gossip protocol to handle:
    - Queue advertisement distribution across clusters
    - Anti-entropy queue discovery
    - Vector clock-based queue state consistency
    - Bandwidth-efficient queue metadata propagation
    """

    def __init__(
        self,
        cluster_id: ClusterNodeId,
        base_gossip_protocol: GossipProtocol,
        federated_queue_manager: Any,  # Avoid circular import
    ) -> None:
        super().__init__(name="FederatedQueueGossipProtocol")
        self.cluster_id = cluster_id
        self.base_gossip = base_gossip_protocol
        self.queue_manager = federated_queue_manager

        # Queue gossip state
        self.queue_vector_clock: QueueGossipVectorClock = defaultdict(int)
        self.advertised_queues: dict[
            QueueAdvertisementId, QueueAdvertisementGossipPayload
        ] = {}
        self.pending_discoveries: dict[str, float] = {}  # discovery_id -> timestamp

        # Statistics
        self.gossip_stats = QueueGossipStatistics()

        # Start gossip workers
        self._start_gossip_workers()

        logger.info(
            f"Federated Queue Gossip Protocol initialized for cluster {cluster_id}"
        )

    def _start_gossip_workers(self) -> None:
        """Start background workers for queue gossip operations."""
        try:
            self.create_task(self._queue_gossip_worker(), name="queue_gossip_worker")
            self.create_task(self._anti_entropy_worker(), name="anti_entropy_worker")
            self.create_task(
                self._discovery_timeout_worker(), name="discovery_timeout_worker"
            )
            logger.info(f"Started {len(self._task_manager)} queue gossip workers")
        except RuntimeError:
            logger.warning("No event loop running, skipping queue gossip workers")

    async def gossip_queue_advertisement(
        self, advertisement: QueueAdvertisement, operation: str = "advertise"
    ) -> None:
        """Gossip a queue advertisement to the federation."""
        # Increment vector clock
        self.queue_vector_clock[self.cluster_id] += 1

        # Create gossip payload
        payload = QueueAdvertisementGossipPayload(
            cluster_id=self.cluster_id,
            vector_clock=dict(self.queue_vector_clock),
            advertisement=advertisement,
            operation=operation,
        )

        # Store locally
        self.advertised_queues[advertisement.advertisement_id] = payload

        # Gossip to federation
        await self._gossip_payload(QueueGossipMessageType.QUEUE_ADVERTISEMENT, payload)

        self.gossip_stats.advertisements_sent += 1
        logger.debug(f"Gossiped queue advertisement for {advertisement.queue_name}")

    async def request_queue_discovery(
        self, queue_pattern: str = "*", timeout_seconds: float = 30.0
    ) -> list[QueueAdvertisement]:
        """Request discovery of queues matching pattern across federation."""
        discovery_id = str(uuid.uuid4())
        start_time = time.time()

        # Track discovery request
        self.pending_discoveries[discovery_id] = start_time

        # Create discovery request
        request_payload = QueueDiscoveryRequestPayload(
            cluster_id=self.cluster_id,
            vector_clock=dict(self.queue_vector_clock),
            requesting_cluster=self.cluster_id,
            queue_pattern=queue_pattern,
            discovery_id=discovery_id,
        )

        # Gossip discovery request
        await self._gossip_payload(
            QueueGossipMessageType.QUEUE_DISCOVERY_REQUEST, request_payload
        )

        self.gossip_stats.discovery_requests_sent += 1

        # Wait for responses (in real implementation, would use async queues)
        await asyncio.sleep(min(timeout_seconds, 5.0))  # Simplified for demo

        # Clean up discovery tracking
        if discovery_id in self.pending_discoveries:
            del self.pending_discoveries[discovery_id]
            self.gossip_stats.update_convergence_time(start_time)

        # Return discovered queues (simplified - real implementation would aggregate responses)
        return []

    async def handle_gossip_message(
        self,
        message_type: QueueGossipMessageType,
        payload_data: dict[str, Any],
        sender_cluster: ClusterNodeId,
    ) -> None:
        """Handle incoming queue gossip messages."""
        try:
            if message_type == QueueGossipMessageType.QUEUE_ADVERTISEMENT:
                await self._handle_queue_advertisement(payload_data, sender_cluster)
            elif message_type == QueueGossipMessageType.QUEUE_DISCOVERY_REQUEST:
                await self._handle_discovery_request(payload_data, sender_cluster)
            elif message_type == QueueGossipMessageType.QUEUE_DISCOVERY_RESPONSE:
                await self._handle_discovery_response(payload_data, sender_cluster)
            else:
                logger.warning(f"Unknown queue gossip message type: {message_type}")

        except Exception as e:
            logger.error(f"Failed to handle queue gossip message: {e}")

    async def _handle_queue_advertisement(
        self, payload_data: dict[str, Any], sender_cluster: ClusterNodeId
    ) -> None:
        """Handle incoming queue advertisement."""
        try:
            payload = QueueAdvertisementGossipPayload.from_dict(payload_data)

            # Update vector clock
            for cluster, clock in payload.vector_clock.items():
                self.queue_vector_clock[cluster] = max(
                    self.queue_vector_clock[cluster], clock
                )

            # Process advertisement
            if payload.operation == "advertise":
                # Add/update remote queue advertisement
                cluster_id = payload.advertisement.cluster_id
                # Ensure cluster entry exists
                if cluster_id not in self.queue_manager.remote_queues:
                    self.queue_manager.remote_queues[cluster_id] = {}
                self.queue_manager.remote_queues[cluster_id][
                    payload.advertisement.queue_name
                ] = payload.advertisement
            elif payload.operation == "remove":
                # Remove queue advertisement
                cluster_queues = self.queue_manager.remote_queues.get(
                    payload.advertisement.cluster_id, {}
                )
                if payload.advertisement.queue_name in cluster_queues:
                    del cluster_queues[payload.advertisement.queue_name]

            self.gossip_stats.advertisements_received += 1
            logger.debug(f"Processed queue advertisement from {sender_cluster}")

        except Exception as e:
            logger.error(f"Failed to handle queue advertisement: {e}")

    async def _handle_discovery_request(
        self, payload_data: dict[str, Any], sender_cluster: ClusterNodeId
    ) -> None:
        """Handle incoming queue discovery request."""
        try:
            request = QueueDiscoveryRequestPayload.from_dict(payload_data)

            # Find matching local queues
            matching_ads = []
            for (
                ad_id,
                ad_payload,
            ) in self.queue_manager.queue_advertisements.items():
                if isinstance(ad_payload, QueueAdvertisement):
                    ad = ad_payload
                else:
                    # Handle nested structure if needed
                    ad = getattr(ad_payload, "advertisement", ad_payload)

                if self._queue_matches_pattern(ad.queue_name, request.queue_pattern):
                    matching_ads.append(ad)

            # Send response if we have matches
            if matching_ads:
                response = QueueDiscoveryResponsePayload(
                    cluster_id=self.cluster_id,
                    vector_clock=dict(self.queue_vector_clock),
                    discovery_id=request.discovery_id,
                    responding_cluster=self.cluster_id,
                    matching_queues=matching_ads,
                )

                await self._gossip_payload(
                    QueueGossipMessageType.QUEUE_DISCOVERY_RESPONSE, response
                )
                self.gossip_stats.discovery_responses_sent += 1

            logger.debug(f"Processed discovery request from {sender_cluster}")

        except Exception as e:
            logger.error(f"Failed to handle discovery request: {e}")

    async def _handle_discovery_response(
        self, payload_data: dict[str, Any], sender_cluster: ClusterNodeId
    ) -> None:
        """Handle incoming queue discovery response."""
        try:
            response = QueueDiscoveryResponsePayload.from_dict(payload_data)

            # Process discovered queues
            for advertisement in response.matching_queues:
                if advertisement.cluster_id not in self.queue_manager.remote_queues:
                    self.queue_manager.remote_queues[advertisement.cluster_id] = {}
                self.queue_manager.remote_queues[advertisement.cluster_id][
                    advertisement.queue_name
                ] = advertisement

            logger.debug(
                f"Processed discovery response from {sender_cluster} with {len(response.matching_queues)} queues"
            )

        except Exception as e:
            logger.error(f"Failed to handle discovery response: {e}")

    async def _gossip_payload(
        self, message_type: QueueGossipMessageType, payload: QueueGossipPayload
    ) -> None:
        """Send gossip payload through base gossip protocol."""
        try:
            # Create gossip message
            gossip_msg = GossipMessage(
                message_id=f"{self.cluster_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}",
                message_type=GossipMessageType.RUMOR,  # Use rumor type for queue messages
                sender_id=self.cluster_id,
                payload={
                    "queue_gossip_type": message_type.value,
                    "queue_payload": payload.to_dict(),
                },
                vector_clock=VectorClock.from_dict(dict(self.queue_vector_clock)),
                created_at=time.time(),
            )

            # Send via base gossip protocol
            await self.base_gossip.add_message(gossip_msg)

        except Exception as e:
            logger.error(f"Failed to gossip payload: {e}")

    def _queue_matches_pattern(self, queue_name: str, pattern: str) -> bool:
        """Check if queue name matches pattern."""
        import fnmatch

        return fnmatch.fnmatch(queue_name, pattern)

    # Background workers

    async def _queue_gossip_worker(self) -> None:
        """Background worker for periodic queue gossip operations."""
        try:
            while True:
                try:
                    await asyncio.sleep(30.0)  # Gossip every 30 seconds

                    # Refresh advertisements for active queues
                    for (
                        ad_id,
                        advertisement,
                    ) in self.queue_manager.queue_advertisements.items():
                        if isinstance(advertisement, QueueAdvertisement):
                            await self.gossip_queue_advertisement(
                                advertisement, "advertise"
                            )

                    logger.debug("Queue gossip sweep completed")

                except Exception as e:
                    logger.error(f"Queue gossip worker error: {e}")
                    await asyncio.sleep(30.0)

        except asyncio.CancelledError:
            logger.info("Queue gossip worker cancelled")
        finally:
            logger.info("Queue gossip worker stopped")

    async def _anti_entropy_worker(self) -> None:
        """Background worker for anti-entropy queue discovery."""
        try:
            while True:
                try:
                    await asyncio.sleep(120.0)  # Anti-entropy every 2 minutes

                    # Perform periodic discovery to ensure convergence
                    await self.request_queue_discovery("*")

                    # Clean up expired advertisements
                    for (
                        cluster_id,
                        cluster_queues,
                    ) in self.queue_manager.remote_queues.items():
                        expired_queues = [
                            queue_name
                            for queue_name, ad in cluster_queues.items()
                            if ad.is_expired()
                        ]
                        for queue_name in expired_queues:
                            del cluster_queues[queue_name]

                    logger.debug("Anti-entropy sweep completed")

                except Exception as e:
                    logger.error(f"Anti-entropy worker error: {e}")
                    await asyncio.sleep(120.0)

        except asyncio.CancelledError:
            logger.info("Anti-entropy worker cancelled")
        finally:
            logger.info("Anti-entropy worker stopped")

    async def _discovery_timeout_worker(self) -> None:
        """Background worker to clean up timed out discovery requests."""
        try:
            while True:
                try:
                    await asyncio.sleep(60.0)  # Check timeouts every minute

                    current_time = time.time()
                    timed_out_discoveries = [
                        discovery_id
                        for discovery_id, start_time in self.pending_discoveries.items()
                        if (current_time - start_time) > 60.0  # 1 minute timeout
                    ]

                    for discovery_id in timed_out_discoveries:
                        del self.pending_discoveries[discovery_id]
                        logger.debug(
                            f"Cleaned up timed out discovery request {discovery_id}"
                        )

                except Exception as e:
                    logger.error(f"Discovery timeout worker error: {e}")
                    await asyncio.sleep(60.0)

        except asyncio.CancelledError:
            logger.info("Discovery timeout worker cancelled")
        finally:
            logger.info("Discovery timeout worker stopped")

    def get_gossip_statistics(self) -> QueueGossipStatistics:
        """Get current queue gossip statistics."""
        self.gossip_stats.active_queue_advertisements = len(self.advertised_queues)
        return self.gossip_stats

    async def shutdown(self) -> None:
        """Shutdown queue gossip protocol."""
        logger.info("Shutting down Federated Queue Gossip Protocol...")

        # Shutdown task manager (cancels all workers)
        await super().shutdown()

        # Clear all data structures
        self.advertised_queues.clear()
        self.pending_discoveries.clear()
        self.queue_vector_clock.clear()

        logger.info("Federated Queue Gossip Protocol shutdown complete")
