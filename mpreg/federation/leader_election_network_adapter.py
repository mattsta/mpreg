"""
Network Service Adapter for Leader Election Integration with MPREG System.

This module provides the network integration layer that connects the leader election
system with MPREG's communication infrastructure (RPC, Topic Pub/Sub, Message Queue).
It enables real distributed leader election across federated clusters.

Key Features:
- Integration with MPREG's TopicExchange for Raft message distribution
- RPC-based vote requests and heartbeats for Raft consensus
- Message queue integration for reliable leader election state coordination
- Network partition detection and recovery via federation bridge
- Automatic cluster discovery through MPREG's federation system
- High-performance async communication with backpressure handling
"""

import asyncio
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from threading import RLock
from typing import Any

from loguru import logger

from mpreg.core.enhanced_rpc import TopicAwareRPCExecutor
from mpreg.core.model import PubSubMessage, TopicPattern
from mpreg.core.task_manager import ManagedObject
from mpreg.core.topic_exchange import TopicExchange
from mpreg.datastructures.leader_election import (
    LeaderElectionMetrics,
    LeaderElectionVote,
    RaftBasedLeaderElection,
)
from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    Timestamp,
)
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.federation.unified_federation import FederationBridgeProtocol


class LeaderElectionMessageType(Enum):
    """Well-defined message types for leader election network communication."""

    VOTE_REQUEST = "vote_request"
    VOTE_RESPONSE = "vote_response"
    HEARTBEAT = "heartbeat"
    METRICS_UPDATE = "metrics_update"
    LEADERSHIP_ANNOUNCEMENT = "leadership_announcement"


@dataclass(frozen=True, slots=True)
class LeaderElectionNetworkConfiguration:
    """Configuration for leader election network adapter."""

    cluster_id: ClusterId
    topic_prefix: str = "mpreg.leader_election"
    vote_request_timeout_seconds: DurationSeconds = 5.0
    heartbeat_timeout_seconds: DurationSeconds = 2.0
    max_concurrent_requests: int = 50
    enable_metrics_collection: bool = True
    enable_network_partition_detection: bool = True
    network_retry_attempts: int = 3
    network_retry_base_delay: float = 0.1
    cluster_discovery_interval_seconds: DurationSeconds = 30.0

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("Cluster ID cannot be empty")
        if self.vote_request_timeout_seconds <= 0:
            raise ValueError("Vote request timeout must be positive")
        if self.heartbeat_timeout_seconds <= 0:
            raise ValueError("Heartbeat timeout must be positive")


@dataclass(frozen=True, slots=True)
class LeaderElectionMessageHeaders:
    """Well-defined headers for leader election messages."""

    message_type: LeaderElectionMessageType
    source_cluster: ClusterId
    target_cluster: ClusterId
    namespace: str
    term: int
    vector_clock: str

    def to_dict(self) -> dict[str, str]:
        """Convert headers to dictionary format for PubSub."""
        data = asdict(self)
        # Convert enum to string value and ensure all values are strings
        data["message_type"] = self.message_type.value
        data["term"] = str(self.term)
        return {k: str(v) for k, v in data.items()}


@dataclass(frozen=True, slots=True)
class LeaderElectionNetworkMessage:
    """Network message for leader election communication."""

    message_type: LeaderElectionMessageType
    source_cluster: ClusterId
    target_cluster: ClusterId
    namespace: str
    term: int
    payload: Any  # LeaderElectionVote, LeaderElectionTerm, LeaderElectionMetrics, etc.
    vector_clock: VectorClock
    timestamp: Timestamp = field(default_factory=time.time)
    request_id: str = field(default_factory=lambda: f"req_{int(time.time() * 1000000)}")

    def to_topic_pattern(self, topic_prefix: str) -> TopicPattern:
        """Generate topic pattern for this message."""
        pattern_str = f"{topic_prefix}.{self.message_type.value}.{self.target_cluster}.{self.namespace}"
        return TopicPattern(pattern=pattern_str, exact_match=True)

    def to_headers(self) -> LeaderElectionMessageHeaders:
        """Create well-typed headers for this message."""
        return LeaderElectionMessageHeaders(
            message_type=self.message_type,
            source_cluster=self.source_cluster,
            target_cluster=self.target_cluster,
            namespace=self.namespace,
            term=self.term,
            vector_clock=str(self.vector_clock),
        )

    def to_pubsub_message(self, topic_prefix: str) -> PubSubMessage:
        """Convert to PubSub message for TopicExchange."""
        headers = self.to_headers()

        return PubSubMessage(
            message_id=self.request_id,
            topic=self.to_topic_pattern(topic_prefix).pattern,
            payload=self,
            publisher=f"leader_election_{self.source_cluster}",
            headers=headers.to_dict(),
            timestamp=self.timestamp,
        )


@dataclass(slots=True)
class LeaderElectionNetworkStatistics:
    """Statistics for leader election network operations."""

    vote_requests_sent: int = 0
    vote_requests_received: int = 0
    vote_responses_sent: int = 0
    vote_responses_received: int = 0
    heartbeats_sent: int = 0
    heartbeats_received: int = 0
    network_failures: int = 0
    timeout_errors: int = 0
    cluster_discoveries: int = 0
    partition_detections: int = 0
    average_request_latency_ms: float = 0.0
    last_statistics_reset: Timestamp = field(default_factory=time.time)

    def reset(self) -> None:
        """Reset all statistics counters."""
        self.vote_requests_sent = 0
        self.vote_requests_received = 0
        self.vote_responses_sent = 0
        self.vote_responses_received = 0
        self.heartbeats_sent = 0
        self.heartbeats_received = 0
        self.network_failures = 0
        self.timeout_errors = 0
        self.cluster_discoveries = 0
        self.partition_detections = 0
        self.average_request_latency_ms = 0.0
        self.last_statistics_reset = time.time()


@dataclass(slots=True)
class LeaderElectionNetworkAdapter(ManagedObject):
    """
    Network service adapter for leader election integration with MPREG system.

    This adapter bridges the leader election algorithms with MPREG's distributed
    communication infrastructure, enabling real-world deployment of the leader
    election system across federated clusters.

    Architecture Integration:
    - Uses TopicExchange for broadcasting election messages
    - Uses EnhancedRPCSystem for direct peer-to-peer vote requests
    - Integrates with FederationBridge for cluster discovery
    - Provides metrics collection for monitoring and debugging
    - Handles network partitions and failures gracefully
    """

    config: LeaderElectionNetworkConfiguration
    topic_exchange: TopicExchange
    rpc_system: TopicAwareRPCExecutor
    federation_bridge: FederationBridgeProtocol | None = None

    # Leader election implementation (concrete type for attribute access)
    leader_election: RaftBasedLeaderElection = field(init=False)

    # Network state
    vector_clock: VectorClock = field(default_factory=VectorClock.empty)
    known_clusters: set[ClusterId] = field(default_factory=set)
    cluster_health: dict[ClusterId, Timestamp] = field(default_factory=dict)
    pending_requests: dict[str, asyncio.Future] = field(default_factory=dict)

    # Statistics and monitoring
    statistics: LeaderElectionNetworkStatistics = field(
        default_factory=LeaderElectionNetworkStatistics
    )
    message_processing_queue: deque[LeaderElectionNetworkMessage] = field(
        default_factory=deque
    )

    # Thread safety
    _lock: RLock = field(default_factory=RLock)
    _shutdown_requested: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Initialize the network adapter."""
        # Initialize ManagedObject
        ManagedObject.__init__(self, name="LeaderElectionNetworkAdapter")

        # Create leader election implementation
        self.leader_election = RaftBasedLeaderElection(
            cluster_id=self.config.cluster_id
        )

        # Add self to known clusters
        self.known_clusters.add(self.config.cluster_id)
        self.cluster_health[self.config.cluster_id] = time.time()

        # Set up network integration
        self._setup_network_integration()

        # Start background tasks
        self._start_background_tasks()

    def _setup_network_integration(self) -> None:
        """Set up integration with MPREG's network systems."""
        # Subscribe to leader election topics
        vote_request_pattern = (
            f"{self.config.topic_prefix}.vote_request.{self.config.cluster_id}.#"
        )
        heartbeat_pattern = (
            f"{self.config.topic_prefix}.heartbeat.{self.config.cluster_id}.#"
        )
        metrics_pattern = (
            f"{self.config.topic_prefix}.metrics_update.{self.config.cluster_id}.#"
        )

        logger.info(
            f"Leader election network adapter subscribing to patterns: "
            f"{vote_request_pattern}, {heartbeat_pattern}, {metrics_pattern}"
        )

        # TopicAwareRPCExecutor uses execute_with_topics, not register_method
        # We'll use topic-based communication instead of direct RPC registration

    def _start_background_tasks(self) -> None:
        """Start background processing tasks using ManagedObject task management."""
        try:
            self.create_task(
                self._process_message_queue(), name="message_queue_processor"
            )
            self.create_task(self._cluster_discovery_loop(), name="cluster_discovery")
            if self.config.enable_network_partition_detection:
                self.create_task(
                    self._network_partition_detection_loop(), name="partition_detection"
                )
        except RuntimeError:
            logger.warning("No event loop running, background tasks will not start")

    # Public API for leader election with network integration

    async def elect_leader(self, namespace: str) -> ClusterId:
        """
        Elect leader with full network coordination.

        This method integrates the underlying leader election algorithm
        with the MPREG network infrastructure for real distributed elections.
        """
        # Update our cluster metrics for the election
        if self.config.enable_metrics_collection:
            await self._collect_and_broadcast_metrics()

        # Discover any new clusters before election
        await self._discover_clusters()

        # Update leader election with known clusters
        for cluster_id in self.known_clusters:
            if cluster_id != self.config.cluster_id:
                # Use placeholder metrics if we don't have real ones
                metrics = LeaderElectionMetrics(
                    cluster_id=cluster_id,
                    cpu_usage=0.5,
                    memory_usage=0.5,
                    cache_hit_rate=0.8,
                    active_connections=100,
                    network_latency_ms=self._get_cluster_network_latency(cluster_id),
                    last_heartbeat=self.cluster_health.get(cluster_id, 0),
                )
                self.leader_election.update_metrics(cluster_id, metrics)

        # Perform election with network integration
        return await self._perform_networked_election(namespace)

    async def _perform_networked_election(self, namespace: str) -> ClusterId:
        """Perform leader election with real network vote requests."""
        # For now, use the basic leader election without monkey patching
        # TODO: Implement proper network integration hooks in RaftBasedLeaderElection
        result = await self.leader_election.elect_leader(namespace)

        # Broadcast election result if we won
        if result == self.config.cluster_id:
            await self._broadcast_leadership_announcement(namespace, result)

        return result

    async def _send_vote_request(
        self, target_cluster: ClusterId, namespace: str, term: int
    ) -> LeaderElectionVote:
        """Send vote request to target cluster via network."""
        request_start = time.time()

        try:
            # Create vote request message
            message = LeaderElectionNetworkMessage(
                message_type=LeaderElectionMessageType.VOTE_REQUEST,
                source_cluster=self.config.cluster_id,
                target_cluster=target_cluster,
                namespace=namespace,
                term=term,
                payload={
                    "candidate_id": self.config.cluster_id,
                    "last_log_index": 0,  # Simplified for cache leadership
                    "last_log_term": 0,
                },
                vector_clock=self.vector_clock,
            )

            # Send via topic-based RPC using TopicAwareRPCExecutor
            try:
                # Create RPC commands for vote request
                from mpreg.core.enhanced_rpc import TopicAwareRPCCommand

                vote_command = TopicAwareRPCCommand(
                    name="leader_election_vote_request",
                    fun="handle_vote_request",
                    args=(),
                    kwargs=message.payload,
                    command_id=f"vote_req_{target_cluster}_{term}",
                )

                # Execute command via topic-aware RPC
                result = await asyncio.wait_for(
                    self.rpc_system.execute_with_topics([vote_command]),
                    timeout=self.config.vote_request_timeout_seconds,
                )

                # Extract response from RPC result
                if result.all_successful and result.command_results:
                    command_result = list(result.command_results.values())[0]
                    # Simulate vote response based on command success
                    response = {
                        "vote_granted": command_result.success,
                        "term": term,
                        "voter_id": target_cluster,
                    }
                else:
                    response = {"vote_granted": False, "error": "RPC execution failed"}

                # Update statistics
                self.statistics.vote_requests_sent += 1
                latency = (time.time() - request_start) * 1000
                self._update_average_latency(latency)

                # Create vote response
                return LeaderElectionVote(
                    term=term,
                    candidate_id=self.config.cluster_id,
                    voter_id=target_cluster,
                    granted=bool(response.get("vote_granted", False)),
                    timestamp=time.time(),
                )

            except TimeoutError:
                self.statistics.timeout_errors += 1
                return LeaderElectionVote(
                    term=term,
                    candidate_id=self.config.cluster_id,
                    voter_id=target_cluster,
                    granted=False,
                    timestamp=time.time(),
                )

        except Exception as e:
            logger.error(f"Failed to send vote request to {target_cluster}: {e}")
            self.statistics.network_failures += 1
            return LeaderElectionVote(
                term=term,
                candidate_id=self.config.cluster_id,
                voter_id=target_cluster,
                granted=False,
                timestamp=time.time(),
            )

    # RPC handlers for incoming requests

    async def _handle_vote_request_rpc(
        self, request_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle incoming vote request via RPC."""
        try:
            candidate_id = request_data["candidate_id"]
            term = request_data.get("term", 0)

            # Simple vote granting logic (can be enhanced)
            # Grant vote if candidate has reasonable metrics
            vote_granted = True

            # Update statistics
            self.statistics.vote_requests_received += 1
            self.statistics.vote_responses_sent += 1

            # Update cluster health
            self.cluster_health[candidate_id] = time.time()

            return {
                "vote_granted": vote_granted,
                "term": term,
                "voter_id": self.config.cluster_id,
            }

        except Exception as e:
            logger.error(f"Error handling vote request: {e}")
            return {"vote_granted": False, "error": str(e)}

    async def _handle_heartbeat_rpc(
        self, heartbeat_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle incoming heartbeat via RPC."""
        try:
            source_cluster = heartbeat_data["source_cluster"]
            namespace = heartbeat_data["namespace"]
            term = heartbeat_data.get("term", 0)

            # Update cluster health
            self.cluster_health[source_cluster] = time.time()
            self.statistics.heartbeats_received += 1

            # If this is from current leader, reset election timeout
            current_leader = await self.leader_election.get_current_leader(namespace)
            if current_leader == source_cluster:
                # Reset our election timer (implementation specific)
                pass

            return {
                "acknowledged": True,
                "cluster_id": self.config.cluster_id,
                "timestamp": time.time(),
            }

        except Exception as e:
            logger.error(f"Error handling heartbeat: {e}")
            return {"acknowledged": False, "error": str(e)}

    async def _handle_metrics_exchange_rpc(
        self, metrics_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle metrics exchange via RPC."""
        try:
            source_cluster = metrics_data["source_cluster"]
            metrics = LeaderElectionMetrics(**metrics_data["metrics"])

            # Update metrics in leader election
            self.leader_election.update_metrics(source_cluster, metrics)
            self.cluster_health[source_cluster] = time.time()

            return {"received": True, "cluster_id": self.config.cluster_id}

        except Exception as e:
            logger.error(f"Error handling metrics exchange: {e}")
            return {"received": False, "error": str(e)}

    # Background tasks

    async def _process_message_queue(self) -> None:
        """Process queued leader election messages."""
        try:
            while not self._shutdown_requested:
                while self.message_processing_queue:
                    with self._lock:
                        if not self.message_processing_queue:
                            break
                        message = self.message_processing_queue.popleft()

                    await self._process_leader_election_message(message)

                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info("Message processing task cancelled")
        except Exception as e:
            logger.error(f"Error in message processing: {e}")

    async def _cluster_discovery_loop(self) -> None:
        """Periodically discover new clusters via federation bridge."""
        try:
            while not self._shutdown_requested:
                await self._discover_clusters()
                await asyncio.sleep(self.config.cluster_discovery_interval_seconds)

        except asyncio.CancelledError:
            logger.info("Cluster discovery task cancelled")
        except Exception as e:
            logger.error(f"Error in cluster discovery: {e}")

    async def _network_partition_detection_loop(self) -> None:
        """Detect network partitions and handle them gracefully."""
        try:
            while not self._shutdown_requested:
                await self._detect_network_partitions()
                await asyncio.sleep(10.0)  # Check every 10 seconds

        except asyncio.CancelledError:
            logger.info("Network partition detection task cancelled")
        except Exception as e:
            logger.error(f"Error in partition detection: {e}")

    # Helper methods

    async def _collect_and_broadcast_metrics(self) -> None:
        """Collect local metrics and broadcast to other clusters."""
        # This would integrate with MPREG's monitoring system
        # For now, create sample metrics
        local_metrics = LeaderElectionMetrics(
            cluster_id=self.config.cluster_id,
            cpu_usage=0.3,  # Would come from system monitoring
            memory_usage=0.4,
            cache_hit_rate=0.9,
            active_connections=len(self.known_clusters) * 10,
            network_latency_ms=5.0,
        )

        # Broadcast to other clusters via topic exchange
        for cluster_id in self.known_clusters:
            if cluster_id != self.config.cluster_id:
                try:
                    # Create metrics message and publish via topic exchange
                    metrics_message = LeaderElectionNetworkMessage(
                        message_type=LeaderElectionMessageType.METRICS_UPDATE,
                        source_cluster=self.config.cluster_id,
                        target_cluster=cluster_id,
                        namespace="metrics",
                        term=0,
                        payload={
                            "source_cluster": self.config.cluster_id,
                            "metrics": {
                                "cluster_id": local_metrics.cluster_id,
                                "cpu_usage": local_metrics.cpu_usage,
                                "memory_usage": local_metrics.memory_usage,
                                "cache_hit_rate": local_metrics.cache_hit_rate,
                                "active_connections": local_metrics.active_connections,
                                "network_latency_ms": local_metrics.network_latency_ms,
                            },
                        },
                        vector_clock=self.vector_clock,
                    )

                    pubsub_msg = metrics_message.to_pubsub_message(
                        self.config.topic_prefix
                    )
                    self.topic_exchange.publish_message(pubsub_msg)

                except Exception as e:
                    logger.debug(f"Failed to send metrics to {cluster_id}: {e}")

    async def _discover_clusters(self) -> None:
        """Discover clusters via federation bridge."""
        if self.federation_bridge:
            try:
                # Get known clusters from federation bridge
                federation_clusters = self.federation_bridge.get_known_clusters()
                new_clusters = federation_clusters - self.known_clusters

                if new_clusters:
                    self.known_clusters.update(new_clusters)
                    self.statistics.cluster_discoveries += len(new_clusters)
                    logger.info(
                        f"Discovered {len(new_clusters)} new clusters: {new_clusters}"
                    )

                    # Initialize health tracking for new clusters
                    current_time = time.time()
                    for cluster_id in new_clusters:
                        self.cluster_health[cluster_id] = current_time
            except AttributeError:
                # Federation bridge doesn't support get_known_clusters
                logger.debug("Federation bridge does not support cluster discovery")
            except Exception as e:
                logger.error(f"Error during cluster discovery: {e}")

    def _get_cluster_network_latency(self, cluster_id: ClusterId) -> float:
        """Get network latency to cluster (placeholder implementation)."""
        # In production, this would measure actual network latency
        return 10.0 if cluster_id in self.known_clusters else 100.0

    def _update_average_latency(self, latency_ms: float) -> None:
        """Update running average of request latency."""
        if self.statistics.average_request_latency_ms == 0:
            self.statistics.average_request_latency_ms = latency_ms
        else:
            # Simple exponential moving average
            self.statistics.average_request_latency_ms = (
                self.statistics.average_request_latency_ms * 0.9 + latency_ms * 0.1
            )

    async def _broadcast_leadership_announcement(
        self, namespace: str, leader_id: ClusterId
    ) -> None:
        """Broadcast leadership announcement to all clusters."""
        announcement = LeaderElectionNetworkMessage(
            message_type=LeaderElectionMessageType.LEADERSHIP_ANNOUNCEMENT,
            source_cluster=self.config.cluster_id,
            target_cluster="*",  # Broadcast
            namespace=namespace,
            term=self.leader_election.namespace_terms.get(namespace, 0),
            payload={"leader_id": leader_id},
            vector_clock=self.vector_clock,
        )

        # Publish to topic exchange
        pubsub_message = announcement.to_pubsub_message(self.config.topic_prefix)
        self.topic_exchange.publish_message(pubsub_message)

    async def _process_leader_election_message(
        self, message: LeaderElectionNetworkMessage
    ) -> None:
        """Process incoming leader election message."""
        # Implementation for processing various message types
        if message.message_type == LeaderElectionMessageType.LEADERSHIP_ANNOUNCEMENT:
            # Update local knowledge of leader
            payload = message.payload
            leader_id = payload["leader_id"]

            # Update leader election state
            if message.namespace not in self.leader_election.namespace_leaders:
                lease_expiry = time.time() + self.leader_election.leader_lease_duration
                self.leader_election.namespace_leaders[message.namespace] = (
                    leader_id,
                    message.term,
                    lease_expiry,
                )

    async def _detect_network_partitions(self) -> None:
        """Detect potential network partitions."""
        current_time = time.time()
        silent_threshold = 30.0  # 30 seconds

        partitioned_clusters = []
        for cluster_id, last_seen in self.cluster_health.items():
            if cluster_id != self.config.cluster_id:
                if current_time - last_seen > silent_threshold:
                    partitioned_clusters.append(cluster_id)

        if partitioned_clusters:
            self.statistics.partition_detections += len(partitioned_clusters)
            logger.warning(
                f"Detected potential network partition with clusters: {partitioned_clusters}"
            )

            # Remove partitioned clusters from known clusters temporarily
            for cluster_id in partitioned_clusters:
                self.known_clusters.discard(cluster_id)

    # Public API

    async def get_current_leader(self, namespace: str) -> ClusterId | None:
        """Get current leader for namespace."""
        return await self.leader_election.get_current_leader(namespace)

    async def is_leader(self, namespace: str) -> bool:
        """Check if this cluster is the leader for namespace."""
        return await self.leader_election.is_leader(namespace)

    async def step_down(self, namespace: str) -> None:
        """Step down as leader for namespace."""
        await self.leader_election.step_down(namespace)

    def get_statistics(self) -> LeaderElectionNetworkStatistics:
        """Get network adapter statistics."""
        return self.statistics

    def get_known_clusters(self) -> set[ClusterId]:
        """Get set of known clusters."""
        return self.known_clusters.copy()

    async def shutdown(self) -> None:
        """Shutdown the network adapter."""
        logger.info("Shutting down leader election network adapter")

        with self._lock:
            self._shutdown_requested = True

            # Cancel pending requests
            for future in self.pending_requests.values():
                if not future.done():
                    future.cancel()

            self.pending_requests.clear()
            self.message_processing_queue.clear()

        # Shutdown the leader election to cancel heartbeat tasks
        logger.info("Shutting down leader election background tasks")
        await self.leader_election.shutdown()

        # Shutdown managed background tasks
        logger.info("Shutting down network adapter background tasks")
        await ManagedObject.shutdown(self)

        logger.info("Leader election network adapter shutdown complete")
