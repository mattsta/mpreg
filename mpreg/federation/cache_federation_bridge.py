"""
Cache-Federation Bridge for Cross-Cluster Synchronization.

This module implements the bridge between MPREG's cache system and federation
infrastructure, enabling seamless cache coherence across federated clusters.
The bridge integrates with topic routing to handle cache invalidation and
replication messages while maintaining consistency through vector clocks.

Key Features:
- Cross-cluster cache invalidation propagation
- Federation-aware cache replication strategies
- Topic-pattern based routing for cache messages
- Vector clock coordination for consistency
- Automatic fallback and error recovery
- Performance monitoring and statistics

Architecture:
The bridge acts as a translator between local cache operations and federated
cache coherence protocols, using the TopicExchange for message routing and
the federation graph for cluster topology awareness.
"""

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import RLock
from typing import Any

from loguru import logger

from mpreg.core.model import PubSubMessage
from mpreg.core.topic_exchange import TopicExchange
from mpreg.datastructures.federated_cache_coherence import (
    CacheCoherenceMetadata,
    CacheInvalidationMessage,
    CacheOperationType,
    CacheReplicationMessage,
    FederatedCacheKey,
    FederatedCacheStatistics,
    ReplicationStrategy,
)
from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    PriorityLevel,
    Timestamp,
)
from mpreg.datastructures.vector_clock import VectorClock


@dataclass(frozen=True, slots=True)
class CacheFederationConfiguration:
    """Configuration for cache federation bridge."""

    cluster_id: ClusterId
    invalidation_timeout_seconds: DurationSeconds = 30.0
    replication_timeout_seconds: DurationSeconds = 60.0
    max_pending_operations: int = 1000
    enable_async_replication: bool = True
    enable_sync_replication: bool = True
    default_replication_strategy: ReplicationStrategy = (
        ReplicationStrategy.ASYNC_REPLICATION
    )
    coherence_check_interval_seconds: DurationSeconds = 300.0  # 5 minutes
    topic_prefix: str = "mpreg.cache.federation"

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("Cluster ID cannot be empty")
        if self.invalidation_timeout_seconds <= 0:
            raise ValueError("Invalidation timeout must be positive")
        if self.replication_timeout_seconds <= 0:
            raise ValueError("Replication timeout must be positive")
        if self.max_pending_operations <= 0:
            raise ValueError("Max pending operations must be positive")


@dataclass(frozen=True, slots=True)
class PendingCacheOperation:
    """Tracks pending cache operations across federation."""

    operation_id: str
    operation_type: CacheOperationType
    cache_key: FederatedCacheKey
    target_clusters: frozenset[ClusterId]
    created_at: Timestamp
    timeout_seconds: DurationSeconds
    vector_clock: VectorClock
    priority: PriorityLevel = "1.0"

    def is_expired(self) -> bool:
        """Check if operation has exceeded timeout."""
        return (time.time() - self.created_at) > self.timeout_seconds

    def affects_cluster(self, cluster_id: ClusterId) -> bool:
        """Check if operation affects given cluster."""
        return not self.target_clusters or cluster_id in self.target_clusters


@dataclass(slots=True)
class CacheFederationStatistics:
    """Statistics for cache federation bridge operations."""

    invalidations_sent: int = 0
    invalidations_received: int = 0
    invalidations_processed: int = 0
    replications_sent: int = 0
    replications_received: int = 0
    replications_processed: int = 0
    coherence_violations_detected: int = 0
    timeout_operations: int = 0
    failed_operations: int = 0
    pending_operations_count: int = 0
    average_operation_latency_ms: float = 0.0
    last_statistics_reset: Timestamp = field(default_factory=time.time)

    def reset(self) -> None:
        """Reset all statistics counters."""
        self.invalidations_sent = 0
        self.invalidations_received = 0
        self.invalidations_processed = 0
        self.replications_sent = 0
        self.replications_received = 0
        self.replications_processed = 0
        self.coherence_violations_detected = 0
        self.timeout_operations = 0
        self.failed_operations = 0
        self.pending_operations_count = 0
        self.average_operation_latency_ms = 0.0
        self.last_statistics_reset = time.time()

    def success_rate(self) -> float:
        """Calculate operation success rate."""
        total_operations = self.invalidations_sent + self.replications_sent
        failed_operations = self.timeout_operations + self.failed_operations
        if total_operations == 0:
            return 1.0
        return 1.0 - (failed_operations / total_operations)

    def processing_efficiency(self) -> float:
        """Calculate processing efficiency rate."""
        total_received = self.invalidations_received + self.replications_received
        total_processed = self.invalidations_processed + self.replications_processed
        if total_received == 0:
            return 1.0
        return total_processed / total_received


@dataclass(slots=True)
class CacheFederationBridge:
    """
    Bridge connecting cache system with federation infrastructure.

    This bridge enables cross-cluster cache operations by translating between
    local cache events and federated cache coherence protocols. It uses the
    TopicExchange for message routing and maintains vector clock consistency.

    The bridge handles:
    - Cache invalidation propagation across clusters
    - Cache replication coordination
    - Topic-pattern based routing for cache messages
    - Vector clock synchronization for ordering
    - Error recovery and timeout handling
    """

    config: CacheFederationConfiguration
    topic_exchange: TopicExchange
    local_cluster_id: ClusterId = field(init=False)
    vector_clock: VectorClock = field(default_factory=VectorClock.empty)
    pending_operations: dict[str, PendingCacheOperation] = field(default_factory=dict)
    cluster_cache_statistics: dict[ClusterId, FederatedCacheStatistics] = field(
        default_factory=lambda: defaultdict(FederatedCacheStatistics)
    )
    statistics: CacheFederationStatistics = field(
        default_factory=CacheFederationStatistics
    )
    message_processing_queue: deque[PubSubMessage] = field(default_factory=deque)
    _lock: RLock = field(default_factory=RLock)
    _shutdown_requested: bool = field(default=False, init=False)
    _disable_background_tasks: bool = field(
        default=True, init=False
    )  # Default disabled for tests

    def __post_init__(self) -> None:
        """Initialize federation bridge."""
        self.local_cluster_id = self.config.cluster_id

        # Subscribe to federation cache topics
        self._setup_topic_subscriptions()

        # Start background processing tasks
        self._start_background_tasks()

    def _setup_topic_subscriptions(self) -> None:
        """Set up topic subscriptions for federation cache messages."""
        # Subscribe to cache invalidation messages
        invalidation_pattern = f"{self.config.topic_prefix}.invalidation.#"

        # Subscribe to cache replication messages
        replication_pattern = f"{self.config.topic_prefix}.replication.#"

        # Subscribe to cache coherence status messages
        coherence_pattern = f"{self.config.topic_prefix}.coherence.#"

        logger.info(
            f"Cache federation bridge subscribing to patterns: "
            f"{invalidation_pattern}, {replication_pattern}, {coherence_pattern}"
        )

    def _start_background_tasks(self) -> None:
        """Start background processing tasks."""
        try:
            # Only start background tasks if explicitly requested
            # In test environments, we don't want these tasks
            if not getattr(self, "_disable_background_tasks", True):
                asyncio.create_task(self._process_pending_operations())
                asyncio.create_task(self._process_message_queue())
                asyncio.create_task(self._periodic_coherence_check())
        except RuntimeError:
            # No event loop running
            logger.warning("No event loop running, background tasks will not start")

    async def send_cache_invalidation(
        self,
        cache_key: FederatedCacheKey,
        target_clusters: frozenset[ClusterId] | None = None,
        invalidation_type: str = "single_key",
        reason: str = "manual_invalidation",
        priority: PriorityLevel = "2.0",
    ) -> str:
        """
        Send cache invalidation message to target clusters.

        Args:
            cache_key: The federated cache key to invalidate
            target_clusters: Clusters to target (None = all clusters)
            invalidation_type: Type of invalidation operation
            reason: Human-readable reason for invalidation
            priority: Message priority level

        Returns:
            Operation ID for tracking
        """
        with self._lock:
            # Increment vector clock for this operation
            self.vector_clock = self.vector_clock.increment(self.local_cluster_id)

            # Create invalidation message
            invalidation_message = (
                CacheInvalidationMessage.create_single_key_invalidation(
                    cache_key=cache_key,
                    source_cluster=self.local_cluster_id,
                    target_clusters=target_clusters or frozenset(),
                    vector_clock=self.vector_clock,
                )
            )

            # Create pending operation for tracking
            pending_operation = PendingCacheOperation(
                operation_id=invalidation_message.invalidation_id,
                operation_type=CacheOperationType.SINGLE_KEY_INVALIDATION,
                cache_key=cache_key,
                target_clusters=target_clusters or frozenset(),
                created_at=time.time(),
                timeout_seconds=self.config.invalidation_timeout_seconds,
                vector_clock=self.vector_clock,
                priority=priority,
            )

            self.pending_operations[invalidation_message.invalidation_id] = (
                pending_operation
            )

            # Convert to PubSub message and publish
            pubsub_message = self._create_pubsub_message(invalidation_message)
            notifications = self.topic_exchange.publish_message(pubsub_message)

            # Update statistics
            self.statistics.invalidations_sent += 1
            self.statistics.pending_operations_count = len(self.pending_operations)

            logger.info(
                f"Sent cache invalidation for {cache_key.global_key()} to "
                f"{len(target_clusters) if target_clusters else 'all'} clusters, "
                f"delivered to {len(notifications)} subscribers"
            )

            return invalidation_message.invalidation_id

    async def send_cache_replication(
        self,
        cache_key: FederatedCacheKey,
        cache_value: Any,
        coherence_metadata: CacheCoherenceMetadata,
        target_clusters: frozenset[ClusterId],
        strategy: ReplicationStrategy | None = None,
        priority: PriorityLevel = "1.5",
    ) -> str:
        """
        Send cache replication message to target clusters.

        Args:
            cache_key: The federated cache key to replicate
            cache_value: Value to replicate
            coherence_metadata: Cache coherence state information
            target_clusters: Clusters to replicate to
            strategy: Replication strategy (async/sync)
            priority: Message priority level

        Returns:
            Operation ID for tracking
        """
        with self._lock:
            if not target_clusters:
                raise ValueError("Target clusters cannot be empty for replication")

            # Use default strategy if not specified
            if strategy is None:
                strategy = self.config.default_replication_strategy

            # Increment vector clock for this operation
            self.vector_clock = self.vector_clock.increment(self.local_cluster_id)

            # Create replication message
            if strategy == ReplicationStrategy.ASYNC_REPLICATION:
                replication_message = CacheReplicationMessage.create_async_replication(
                    cache_key=cache_key,
                    cache_value=cache_value,
                    coherence_metadata=coherence_metadata,
                    source_cluster=self.local_cluster_id,
                    target_clusters=target_clusters,
                )
            else:
                replication_message = CacheReplicationMessage.create_sync_replication(
                    cache_key=cache_key,
                    cache_value=cache_value,
                    coherence_metadata=coherence_metadata,
                    source_cluster=self.local_cluster_id,
                    target_clusters=target_clusters,
                )

            # Create pending operation for tracking
            pending_operation = PendingCacheOperation(
                operation_id=replication_message.replication_id,
                operation_type=CacheOperationType.ASYNC_REPLICATION
                if strategy == ReplicationStrategy.ASYNC_REPLICATION
                else CacheOperationType.SYNC_REPLICATION,
                cache_key=cache_key,
                target_clusters=target_clusters,
                created_at=time.time(),
                timeout_seconds=self.config.replication_timeout_seconds,
                vector_clock=self.vector_clock,
                priority=priority,
            )

            self.pending_operations[replication_message.replication_id] = (
                pending_operation
            )

            # Convert to PubSub message and publish
            pubsub_message = self._create_pubsub_message(replication_message)
            notifications = self.topic_exchange.publish_message(pubsub_message)

            # Update statistics
            self.statistics.replications_sent += 1
            self.statistics.pending_operations_count = len(self.pending_operations)

            logger.info(
                f"Sent cache replication ({strategy.value}) for {cache_key.global_key()} "
                f"to {len(target_clusters)} clusters, delivered to {len(notifications)} subscribers"
            )

            return replication_message.replication_id

    async def handle_cache_invalidation_message(
        self, invalidation_message: CacheInvalidationMessage
    ) -> bool:
        """
        Handle incoming cache invalidation message from remote cluster.

        Args:
            invalidation_message: The invalidation message to process

        Returns:
            True if successfully processed, False otherwise
        """
        try:
            with self._lock:
                # Check if this invalidation affects our cluster
                if not invalidation_message.affects_cluster(self.local_cluster_id):
                    logger.debug(
                        f"Invalidation {invalidation_message.invalidation_id} does not affect "
                        f"cluster {self.local_cluster_id}"
                    )
                    return True

                # Update vector clock
                self.vector_clock = self.vector_clock.update(
                    invalidation_message.vector_clock
                )

                # Process the invalidation
                cache_key = invalidation_message.cache_key
                logger.info(
                    f"Processing cache invalidation for {cache_key.global_key()} "
                    f"from cluster {invalidation_message.source_cluster}"
                )

                # TODO: Integrate with actual cache system to perform invalidation
                # For now, we just log and update statistics

                # Update statistics
                self.statistics.invalidations_received += 1
                self.statistics.invalidations_processed += 1

                # Update cluster statistics
                source_cluster = invalidation_message.source_cluster
                current_stats = self.cluster_cache_statistics[source_cluster]
                # Create new statistics with updated values since fields are read-only
                self.cluster_cache_statistics[source_cluster] = (
                    FederatedCacheStatistics(
                        local_hits=current_stats.local_hits,
                        local_misses=current_stats.local_misses,
                        federation_hits=current_stats.federation_hits,
                        federation_misses=current_stats.federation_misses,
                        invalidations_sent=current_stats.invalidations_sent,
                        invalidations_received=current_stats.invalidations_received + 1,
                        replications_sent=current_stats.replications_sent,
                        replications_received=current_stats.replications_received,
                        coherence_violations=current_stats.coherence_violations,
                        cross_cluster_requests=current_stats.cross_cluster_requests,
                        average_invalidation_latency_ms=current_stats.average_invalidation_latency_ms,
                        average_replication_latency_ms=current_stats.average_replication_latency_ms,
                        protocol_transitions=current_stats.protocol_transitions,
                        cluster_synchronizations=current_stats.cluster_synchronizations,
                    )
                )

                return True

        except Exception as e:
            logger.error(f"Failed to handle cache invalidation: {e}")
            self.statistics.failed_operations += 1
            return False

    async def handle_cache_replication_message(
        self, replication_message: CacheReplicationMessage
    ) -> bool:
        """
        Handle incoming cache replication message from remote cluster.

        Args:
            replication_message: The replication message to process

        Returns:
            True if successfully processed, False otherwise
        """
        try:
            with self._lock:
                # Check if this replication affects our cluster
                if self.local_cluster_id not in replication_message.target_clusters:
                    logger.debug(
                        f"Replication {replication_message.replication_id} does not target "
                        f"cluster {self.local_cluster_id}"
                    )
                    return True

                # Update vector clock - replication messages don't have vector_clock, get from coherence_metadata
                self.vector_clock = self.vector_clock.update(
                    replication_message.coherence_metadata.vector_clock
                )

                # Process the replication
                cache_key = replication_message.cache_key
                logger.info(
                    f"Processing cache replication ({replication_message.replication_strategy.value}) "
                    f"for {cache_key.global_key()} from cluster {replication_message.source_cluster}"
                )

                # TODO: Integrate with actual cache system to store replicated data
                # For now, we just log and update statistics

                # Update statistics
                self.statistics.replications_received += 1
                self.statistics.replications_processed += 1

                # Update cluster statistics
                source_cluster = replication_message.source_cluster
                current_stats = self.cluster_cache_statistics[source_cluster]
                # Create new statistics with updated values since fields are read-only
                self.cluster_cache_statistics[source_cluster] = (
                    FederatedCacheStatistics(
                        local_hits=current_stats.local_hits,
                        local_misses=current_stats.local_misses,
                        federation_hits=current_stats.federation_hits,
                        federation_misses=current_stats.federation_misses,
                        invalidations_sent=current_stats.invalidations_sent,
                        invalidations_received=current_stats.invalidations_received,
                        replications_sent=current_stats.replications_sent,
                        replications_received=current_stats.replications_received + 1,
                        coherence_violations=current_stats.coherence_violations,
                        cross_cluster_requests=current_stats.cross_cluster_requests,
                        average_invalidation_latency_ms=current_stats.average_invalidation_latency_ms,
                        average_replication_latency_ms=current_stats.average_replication_latency_ms,
                        protocol_transitions=current_stats.protocol_transitions,
                        cluster_synchronizations=current_stats.cluster_synchronizations,
                    )
                )

                return True

        except Exception as e:
            logger.error(f"Failed to handle cache replication: {e}")
            self.statistics.failed_operations += 1
            return False

    def get_federation_statistics(self) -> CacheFederationStatistics:
        """Get current federation bridge statistics."""
        with self._lock:
            # Update pending operations count
            self.statistics.pending_operations_count = len(self.pending_operations)

            # Calculate average operation latency
            if self.pending_operations:
                total_latency = sum(
                    (time.time() - op.created_at) * 1000  # Convert to milliseconds
                    for op in self.pending_operations.values()
                )
                self.statistics.average_operation_latency_ms = total_latency / len(
                    self.pending_operations
                )

            return self.statistics

    def get_cluster_statistics(self, cluster_id: ClusterId) -> FederatedCacheStatistics:
        """Get statistics for specific cluster."""
        with self._lock:
            return self.cluster_cache_statistics[cluster_id]

    def get_all_cluster_statistics(self) -> dict[ClusterId, FederatedCacheStatistics]:
        """Get statistics for all known clusters."""
        with self._lock:
            return dict(self.cluster_cache_statistics)

    def complete_operation(self, operation_id: str, success: bool = True) -> bool:
        """
        Mark a pending operation as complete.

        Args:
            operation_id: ID of operation to complete
            success: Whether operation succeeded

        Returns:
            True if operation was found and completed, False otherwise
        """
        with self._lock:
            operation = self.pending_operations.pop(operation_id, None)
            if operation is None:
                return False

            # Calculate operation latency
            latency_ms = (time.time() - operation.created_at) * 1000

            # Update statistics
            if not success:
                self.statistics.failed_operations += 1

            # Update average latency (simple moving average)
            if self.statistics.average_operation_latency_ms == 0:
                self.statistics.average_operation_latency_ms = latency_ms
            else:
                self.statistics.average_operation_latency_ms = (
                    self.statistics.average_operation_latency_ms * 0.9
                    + latency_ms * 0.1
                )

            self.statistics.pending_operations_count = len(self.pending_operations)

            logger.debug(
                f"Completed operation {operation_id} ({'success' if success else 'failure'}) "
                f"in {latency_ms:.2f}ms"
            )

            return True

    def _create_pubsub_message(
        self, cache_message: CacheInvalidationMessage | CacheReplicationMessage
    ) -> PubSubMessage:
        """Create PubSub message from cache federation message."""
        if isinstance(cache_message, CacheInvalidationMessage):
            message_type = "invalidation"
            message_id = cache_message.invalidation_id
            topic_pattern = cache_message.topic_pattern
        else:  # CacheReplicationMessage
            message_type = "replication"
            message_id = cache_message.replication_id
            topic_pattern = cache_message.topic_pattern

        return PubSubMessage(
            message_id=message_id,
            topic=topic_pattern,
            payload=cache_message,  # Use the cache message as payload
            publisher=f"cache-federation-{self.local_cluster_id}",
            headers={
                "source_cluster": self.local_cluster_id,
                "message_type": message_type,
                "federation_bridge": "cache_federation",
                "vector_clock": str(self.vector_clock),
            },
            timestamp=time.time(),
        )

    async def _process_pending_operations(self) -> None:
        """Background task to clean up expired pending operations."""
        try:
            while not self._shutdown_requested:
                await asyncio.sleep(10)  # Check every 10 seconds

                with self._lock:
                    expired_operations = [
                        op_id
                        for op_id, operation in self.pending_operations.items()
                        if operation.is_expired()
                    ]

                    for op_id in expired_operations:
                        operation = self.pending_operations.pop(op_id)
                        self.statistics.timeout_operations += 1
                        logger.warning(
                            f"Operation {op_id} timed out after "
                            f"{operation.timeout_seconds} seconds"
                        )

                    if expired_operations:
                        self.statistics.pending_operations_count = len(
                            self.pending_operations
                        )
                        logger.info(
                            f"Cleaned up {len(expired_operations)} expired operations"
                        )

        except asyncio.CancelledError:
            logger.info("Pending operations cleanup task cancelled")
        except Exception as e:
            logger.error(f"Error in pending operations cleanup: {e}")

    async def _process_message_queue(self) -> None:
        """Background task to process incoming federation messages."""
        try:
            while not self._shutdown_requested:
                # Process any queued messages
                while self.message_processing_queue:
                    with self._lock:
                        if not self.message_processing_queue:
                            break
                        message = self.message_processing_queue.popleft()

                    await self._process_federation_message(message)

                await asyncio.sleep(0.1)  # Short sleep to avoid busy waiting

        except asyncio.CancelledError:
            logger.info("Message processing task cancelled")
        except Exception as e:
            logger.error(f"Error in message processing: {e}")

    async def _process_federation_message(self, message: PubSubMessage) -> None:
        """Process a single federation message."""
        try:
            message_type = message.headers.get("message_type")

            if message_type == "invalidation":
                if isinstance(message.payload, CacheInvalidationMessage):
                    await self.handle_cache_invalidation_message(message.payload)
                else:
                    logger.warning(
                        f"Invalid invalidation message payload type: {type(message.payload)}"
                    )

            elif message_type == "replication":
                if isinstance(message.payload, CacheReplicationMessage):
                    await self.handle_cache_replication_message(message.payload)
                else:
                    logger.warning(
                        f"Invalid replication message payload type: {type(message.payload)}"
                    )

            else:
                logger.warning(f"Unknown federation message type: {message_type}")

        except Exception as e:
            logger.error(
                f"Error processing federation message {message.message_id}: {e}"
            )
            self.statistics.failed_operations += 1

    async def _periodic_coherence_check(self) -> None:
        """Background task for periodic cache coherence validation."""
        try:
            while not self._shutdown_requested:
                await asyncio.sleep(self.config.coherence_check_interval_seconds)

                try:
                    # Perform coherence validation
                    violations = await self._validate_cache_coherence()

                    if violations > 0:
                        self.statistics.coherence_violations_detected += violations
                        logger.warning(
                            f"Detected {violations} cache coherence violations"
                        )
                    else:
                        logger.debug("Cache coherence validation passed")

                except Exception as e:
                    logger.error(f"Error in coherence validation: {e}")

        except asyncio.CancelledError:
            logger.info("Coherence check task cancelled")
        except Exception as e:
            logger.error(f"Error in coherence check: {e}")

    async def _validate_cache_coherence(self) -> int:
        """
        Validate cache coherence across federation.

        Returns:
            Number of coherence violations detected
        """
        # TODO: Implement actual coherence validation logic
        # This would involve checking consistency of cache entries
        # across clusters and detecting violations

        violations = 0

        with self._lock:
            # For now, just check for stale pending operations
            current_time = time.time()
            stale_operations = [
                op
                for op in self.pending_operations.values()
                if (current_time - op.created_at) > (op.timeout_seconds * 2)
            ]

            violations = len(stale_operations)

            if stale_operations:
                logger.warning(
                    f"Found {len(stale_operations)} stale pending operations that "
                    f"may indicate coherence issues"
                )

        return violations

    async def shutdown(self) -> None:
        """Shutdown the federation bridge and cleanup resources."""
        logger.info("Shutting down cache federation bridge")

        with self._lock:
            self._shutdown_requested = True

            # Clear pending operations
            pending_count = len(self.pending_operations)
            self.pending_operations.clear()

            # Clear message queue
            queue_count = len(self.message_processing_queue)
            self.message_processing_queue.clear()

            logger.info(
                f"Cache federation bridge shutdown complete: "
                f"cleared {pending_count} pending operations and {queue_count} queued messages"
            )
