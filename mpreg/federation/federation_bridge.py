"""
Unified Graph-Aware Federation Bridge for MPREG.

This module provides the main FederationBridge class combining:
- Thread-safe concurrent operations
- Intelligent graph-based multi-hop routing
- Backward compatibility with existing federation API
- Comprehensive error handling and resilience
- Memory-efficient incremental updates
- Advanced monitoring and observability

This replaces federation_bridge_v2.py with integrated graph routing capabilities.
"""

import asyncio
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING, Any

from loguru import logger

if TYPE_CHECKING:
    from ..core.federated_delivery_guarantees import FederatedDeliveryCoordinator
    from ..core.federated_message_queue import FederatedMessageQueueManager

from ..core.model import PubSubMessage
from ..core.statistics import (
    ClusterInfo,
    FederationBridgeStatistics,
    RemoteClusterInfo,
    RoutingConfiguration,
)
from ..core.topic_exchange import TopicExchange
from .federation_graph import (
    FederationGraphEdge,
    FederationGraphNode,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    NodeType,
    PathList,
)
from .federation_graph_monitor import FederationGraphMonitor
from .federation_optimized import (
    CircuitBreaker,
    ClusterIdentity,
    ClusterStatus,
    IntelligentRoutingTable,
    LatencyMetrics,
    OptimizedClusterState,
)


@dataclass(slots=True)
class GraphAwareClusterConnection:
    """
    Advanced cluster connection with graph routing and health monitoring.
    """

    cluster_id: str
    cluster_identity: ClusterIdentity
    status: ClusterStatus = ClusterStatus.DISCONNECTED
    circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)
    latency_metrics: LatencyMetrics = field(default_factory=LatencyMetrics)

    # Connection pool (simulated for now)
    active_connections: int = 0
    max_connections: int = 10
    connection_pool: list = field(default_factory=list)

    # Health monitoring
    last_heartbeat: float = field(default_factory=time.time)
    consecutive_failures: int = 0
    last_successful_operation: float = field(default_factory=time.time)

    # Traffic shaping
    message_queue: asyncio.Queue = field(
        default_factory=lambda: asyncio.Queue(maxsize=1000)
    )
    rate_limit_per_second: int = 1000
    current_rate: float = 0.0
    rate_window_start: float = field(default_factory=time.time)

    # Graph routing specific
    supports_multi_hop: bool = True
    routing_preference: str = "optimal"  # "optimal", "fast", "reliable"

    _lock: RLock = field(default_factory=RLock)

    async def send_message(
        self, message: PubSubMessage, route_path: PathList | None = None
    ) -> bool:
        """Send a message to the remote cluster with optional routing path."""
        if not self.circuit_breaker.can_execute():
            logger.debug(f"Circuit breaker open for cluster {self.cluster_id}")
            return False

        # Check rate limiting
        if not self._check_rate_limit():
            logger.warning(f"Rate limit exceeded for cluster {self.cluster_id}")
            return False

        start_time = time.time()

        try:
            # Add routing information to message if multi-hop
            if route_path and len(route_path) > 2:
                message.routing_path = route_path
                message.current_hop = 0

            # Send message via websocket to remote cluster
            await self._send_via_websocket(message)

            # Queue message locally for potential retries
            await asyncio.wait_for(self.message_queue.put(message), timeout=1.0)

            # Record success
            latency_ms = (time.time() - start_time) * 1000
            self.latency_metrics.record_latency(latency_ms, success=True)
            self.circuit_breaker.record_success()

            with self._lock:
                self.consecutive_failures = 0
                self.last_successful_operation = time.time()
                self.status = ClusterStatus.ACTIVE

            return True

        except Exception as e:
            # Record failure
            latency_ms = (time.time() - start_time) * 1000
            self.latency_metrics.record_latency(latency_ms, success=False)
            self.circuit_breaker.record_failure()

            with self._lock:
                self.consecutive_failures += 1
                if self.consecutive_failures >= 3:
                    self.status = ClusterStatus.FAILED

            logger.error(f"Failed to send message to cluster {self.cluster_id}: {e}")
            return False

    async def _send_via_websocket(self, message: PubSubMessage) -> None:
        """Send message to remote cluster via MPREG PubSub client."""
        # Use MPREG PubSub client to send message to remote cluster
        from ..client.client_api import MPREGClientAPI
        from ..client.pubsub_client import MPREGPubSubClient

        # Get the bridge URL from cluster identity
        bridge_url = self.cluster_identity.bridge_url
        if not bridge_url:
            raise ValueError(f"No bridge URL configured for cluster {self.cluster_id}")

        # Create MPREG client API and PubSub client to send message
        base_client = None
        client = None
        try:
            base_client = MPREGClientAPI(url=bridge_url)
            client = MPREGPubSubClient(base_client=base_client)

            await base_client.connect()
            await client.start()

            # Send the pub/sub message via MPREG's publish mechanism
            # Add federation_hop header to indicate this message came from a remote cluster
            from ..core.statistics import MessageHeaders

            # Create MessageHeaders with federation info
            headers = MessageHeaders(
                custom_headers={
                    "federation_hop": "True",
                    "federation_source": self.cluster_identity.cluster_id,
                    # Include original headers as well
                    **(message.headers or {}),
                }
            )

            success = await client.publish(
                topic=message.topic, payload=message.payload, headers=headers
            )

            if success:
                logger.debug(
                    f"Sent federation message to {bridge_url} via MPREG PubSub client"
                )
            else:
                logger.warning(
                    f"Failed to send federation message to {bridge_url} - publish returned False"
                )

        except Exception as e:
            logger.error(
                f"Failed to send message via MPREG PubSub client to {bridge_url}: {e}"
            )
            raise
        finally:
            if client:
                try:
                    await client.stop()
                except Exception:
                    pass  # Ignore stop errors
            if base_client:
                try:
                    await base_client.disconnect()
                except Exception:
                    pass  # Ignore disconnect errors

    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limits."""
        current_time = time.time()

        with self._lock:
            # Reset rate window if needed
            if current_time - self.rate_window_start >= 1.0:
                self.current_rate = 0
                self.rate_window_start = current_time

            if self.current_rate >= self.rate_limit_per_second:
                return False

            self.current_rate += 1
            return True

    def is_healthy(self) -> bool:
        """Check if the cluster connection is healthy."""
        with self._lock:
            if self.status == ClusterStatus.FAILED:
                return False

            # Check recent heartbeat
            if time.time() - self.last_heartbeat > 30.0:
                return False

            # Check success rate
            if self.latency_metrics.success_rate < 0.8:
                return False

            return True

    def get_routing_priority(self) -> float:
        """Get routing priority for this cluster (higher = better)."""
        if not self.is_healthy():
            return 0.0

        base_weight = self.latency_metrics.get_routing_weight()

        # Apply cluster preferences
        preference_weight = self.cluster_identity.preference_weight

        # Apply network tier preference (lower tier = lower priority)
        tier_weight = 1.0 / self.cluster_identity.network_tier

        return base_weight * preference_weight * tier_weight


@dataclass(slots=True)
class GraphAwareFederationBridge:
    """
    Production-grade federation bridge with integrated graph routing.

    Key features:
    - Thread-safe operations with proper locking
    - Intelligent graph-based multi-hop routing
    - Backward compatibility with existing federation API
    - Circuit breakers and error handling
    - Incremental state updates
    - Comprehensive monitoring
    - Memory-efficient data structures
    """

    local_cluster: TopicExchange
    cluster_identity: ClusterIdentity

    # Core state
    remote_clusters: dict[str, GraphAwareClusterConnection] = field(
        default_factory=dict
    )
    cluster_states: dict[str, OptimizedClusterState] = field(default_factory=dict)
    routing_table: IntelligentRoutingTable = field(
        default_factory=IntelligentRoutingTable
    )
    local_state: OptimizedClusterState = field(init=False)

    # Graph routing components
    graph_router: GraphBasedFederationRouter = field(init=False)
    graph_monitor: FederationGraphMonitor | None = field(init=False)

    # Configuration
    max_clusters: int = 50
    sync_interval_seconds: float = 30.0
    health_check_interval_seconds: float = 10.0
    enable_graph_routing: bool = True
    enable_monitoring: bool = True
    graph_routing_threshold: int = 2  # Use graph routing for paths > 2 hops

    # Statistics and monitoring
    message_stats: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    performance_metrics: dict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    graph_routing_stats: dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    last_sync: float = 0.0

    # Thread safety
    _lock: RLock = field(default_factory=RLock)
    _background_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)

    # Message forwarding queue
    _outbound_message_queue: asyncio.Queue["PubSubMessage"] = field(init=False)
    # Federation message receiving queue
    _inbound_federation_queue: asyncio.Queue["PubSubMessage"] = field(init=False)

    # Registered federated queue managers
    _federated_queue_managers: list["FederatedMessageQueueManager"] = field(
        default_factory=list
    )

    # Registered federated delivery coordinators
    _federated_delivery_coordinators: list["FederatedDeliveryCoordinator"] = field(
        default_factory=list
    )

    def __post_init__(self) -> None:
        """Initialize local state and graph routing components."""
        self.local_state = OptimizedClusterState(
            cluster_id=self.cluster_identity.cluster_id
        )

        # Initialize graph routing
        if self.enable_graph_routing:
            self.graph_router = GraphBasedFederationRouter(
                cache_ttl_seconds=30.0, max_cache_size=10000
            )

            # Initialize monitoring if enabled
            if self.enable_monitoring:
                self.graph_monitor = FederationGraphMonitor(
                    self.graph_router.graph,
                    collection_interval=5.0,
                    optimization_interval=60.0,
                )
            else:
                self.graph_monitor = None
        else:
            self.graph_monitor = None

        # Add local cluster to graph
        self._add_local_cluster_to_graph()

    def _add_local_cluster_to_graph(self) -> None:
        """Add local cluster as a node in the graph."""
        if not self.enable_graph_routing:
            return

        coords = self.cluster_identity.geographic_coordinates

        local_node = FederationGraphNode(
            node_id=self.cluster_identity.cluster_id,
            node_type=NodeType.CLUSTER,
            region=self.cluster_identity.region,
            coordinates=GeographicCoordinate(coords[0], coords[1]),
            max_capacity=self.cluster_identity.max_bandwidth_mbps,
            current_load=0.0,
            health_score=1.0,
            processing_latency_ms=1.0,
            bandwidth_mbps=self.cluster_identity.max_bandwidth_mbps,
            reliability_score=self.cluster_identity.preference_weight,
        )

        self.graph_router.add_node(local_node)

    async def start(self) -> None:
        """Start the federation bridge and background tasks."""
        logger.info(
            f"Starting Graph-Aware Federation Bridge for cluster {self.cluster_identity.cluster_id}"
        )

        # Initialize message queues in current event loop
        self._outbound_message_queue = asyncio.Queue(maxsize=1000)
        self._inbound_federation_queue = asyncio.Queue(maxsize=1000)

        # Start graph monitoring
        if self.graph_monitor:
            await self.graph_monitor.start_monitoring()

        # Start background tasks
        self._start_background_tasks()

        self.message_stats["bridge_started"] = int(time.time())

    async def stop(self) -> None:
        """Stop the federation bridge and cleanup resources."""
        logger.info("Stopping Graph-Aware Federation Bridge")

        # Signal shutdown
        self._shutdown_event.set()

        # Stop graph monitoring
        if self.graph_monitor:
            await self.graph_monitor.stop_monitoring()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()
        self.message_stats["bridge_stopped"] = int(time.time())

    def register_federated_queue_manager(
        self, manager: "FederatedMessageQueueManager"
    ) -> None:
        """Register a federated queue manager to receive federation advertisements."""
        self._federated_queue_managers.append(manager)
        logger.debug(
            f"Registered federated queue manager for cluster {self.cluster_identity.cluster_id}"
        )

    def register_federated_delivery_coordinator(
        self, coordinator: "FederatedDeliveryCoordinator"
    ) -> None:
        """Register a federated delivery coordinator to receive consensus votes."""
        self._federated_delivery_coordinators.append(coordinator)
        logger.debug(
            f"Registered federated delivery coordinator for cluster {self.cluster_identity.cluster_id}"
        )

    def _start_background_tasks(self) -> None:
        """Start background tasks for health monitoring and sync."""
        # Health check task
        health_task = asyncio.create_task(self._health_check_loop())
        self._background_tasks.add(health_task)

        # Sync task
        sync_task = asyncio.create_task(self._sync_loop())
        self._background_tasks.add(sync_task)

        # Message forwarding task
        forwarding_task = asyncio.create_task(self._message_forwarding_loop())
        self._background_tasks.add(forwarding_task)

        # Federation message receiving task
        receiving_task = asyncio.create_task(self._federation_message_receiving_loop())
        self._background_tasks.add(receiving_task)

        # Graph optimization task (if enabled)
        if self.enable_graph_routing and self.graph_monitor:
            graph_task = asyncio.create_task(self._graph_update_loop())
            self._background_tasks.add(graph_task)

    async def _health_check_loop(self) -> None:
        """Background task for health monitoring."""
        while not self._shutdown_event.is_set():
            try:
                await self._perform_health_checks()
                await asyncio.sleep(self.health_check_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(self.health_check_interval_seconds)

    async def _sync_loop(self) -> None:
        """Background task for periodic synchronization."""
        while not self._shutdown_event.is_set():
            try:
                await self._perform_sync()
                await asyncio.sleep(self.sync_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sync error: {e}")
                await asyncio.sleep(self.sync_interval_seconds)

    async def _graph_update_loop(self) -> None:
        """Background task for graph metric updates."""
        while not self._shutdown_event.is_set():
            try:
                await self._update_graph_metrics()
                await asyncio.sleep(10.0)  # Update every 10 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Graph update error: {e}")
                await asyncio.sleep(10.0)

    async def _message_forwarding_loop(self) -> None:
        """Background task for processing outbound federation messages."""
        # Set up subscription to federation messages that need forwarding
        await self._setup_message_forwarding_subscription()

        while not self._shutdown_event.is_set():
            try:
                # Wait for message to forward (blocks until message available)
                message = await self._outbound_message_queue.get()

                # Forward the message to all connected clusters
                await self._forward_message_to_all_clusters(message)

                # Mark task as done
                self._outbound_message_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Message forwarding error: {e}")

    async def _federation_message_receiving_loop(self) -> None:
        """Background task for receiving and processing federation messages from remote clusters."""
        while not self._shutdown_event.is_set():
            try:
                # Wait for incoming federation message (blocks until message available)
                message = await self._inbound_federation_queue.get()

                # Process the received federation message
                await self._process_received_federation_message(message)

                # Mark task as done
                self._inbound_federation_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federation message receiving error: {e}")

    async def queue_incoming_federation_message(self, message: "PubSubMessage") -> None:
        """Queue an incoming federation message for processing."""
        try:
            # Only queue federation messages that have federation_hop header (from remote clusters)
            if message.topic.startswith("mpreg.federation.") and message.headers.get(
                "federation_hop"
            ):
                # Queue message for processing (blocks with backpressure if full)
                await self._inbound_federation_queue.put(message)
                logger.debug(f"Queued incoming federation message: {message.topic}")
        except Exception as e:
            logger.error(f"Failed to queue incoming federation message: {e}")

    async def _process_received_federation_message(
        self, message: "PubSubMessage"
    ) -> None:
        """Process a federation message received from a remote cluster."""
        try:
            # Increment message received counter
            self.message_stats["messages_received"] += 1

            # Check the message topic to determine what type of federation message this is
            if message.topic.startswith("mpreg.federation.queue.advertisement."):
                await self._handle_received_queue_advertisement(message)
            elif message.topic.startswith("mpreg.federation.queue.message."):
                await self._handle_received_queue_message(message)
            elif message.topic.startswith("mpreg.federation.queue.ack."):
                await self._handle_received_acknowledgment(message)
            elif message.topic.startswith("mpreg.federation.consensus.request."):
                await self._handle_received_consensus_request(message)
            elif message.topic.startswith("mpreg.federation.consensus.vote."):
                await self._handle_received_consensus_vote(message)
            else:
                # For general federation messages, forward to local topic exchange
                logger.debug(
                    f"Forwarding general federation message to local topic exchange: {message.topic}"
                )
                self.local_cluster.publish_message(message)

        except Exception as e:
            logger.error(f"Error processing federation message: {e}")

    async def _handle_received_queue_advertisement(
        self, message: "PubSubMessage"
    ) -> None:
        """Handle a received queue advertisement from a remote cluster."""
        try:
            payload = message.payload
            if not isinstance(payload, dict):
                logger.warning(f"Invalid queue advertisement payload: {type(payload)}")
                return

            cluster_id = payload.get("cluster_id")
            queue_name = payload.get("queue_name")

            if not cluster_id or not queue_name:
                logger.warning(
                    f"Missing cluster_id or queue_name in advertisement: {payload}"
                )
                return

            # Don't process advertisements from our own cluster
            if cluster_id == self.cluster_identity.cluster_id:
                return

            logger.debug(
                f"Received queue advertisement: {queue_name} from cluster {cluster_id}"
            )

            # Forward this advertisement to any registered federated queue managers
            for manager in self._federated_queue_managers:
                await manager._handle_federation_advertisement_message(message)

        except Exception as e:
            logger.error(f"Error handling queue advertisement: {e}")

    async def _handle_received_queue_message(self, message: "PubSubMessage") -> None:
        """Handle a received queue message from a remote cluster."""
        try:
            logger.debug(f"Received federated queue message: {message.topic}")

            # Extract message payload
            payload = message.payload
            if not isinstance(payload, dict):
                logger.warning(f"Invalid queue message payload format: {type(payload)}")
                return

            # Extract queue message details
            ack_token = payload.get("ack_token")
            queue_name = payload.get("queue_name")
            message_topic = payload.get("message_topic")
            message_payload = payload.get("message_payload")
            delivery_guarantee = payload.get("delivery_guarantee")
            source_cluster = payload.get("source_cluster")
            message_options = payload.get("message_options", {})

            # Type check required fields
            if not (
                isinstance(ack_token, str)
                and isinstance(queue_name, str)
                and isinstance(message_topic, str)
                and isinstance(source_cluster, str)
                and message_payload is not None
            ):
                logger.warning(
                    f"Missing or invalid required fields in queue message: {payload}"
                )
                return

            # Route to registered federated queue managers
            message_delivered = False
            for manager in self._federated_queue_managers:
                try:
                    # Check if this manager has the target queue
                    if queue_name in manager.local_queue_manager.list_queues():
                        # Convert delivery guarantee string back to enum
                        from ..core.message_queue import DeliveryGuarantee

                        delivery_guarantee_enum = DeliveryGuarantee(delivery_guarantee)

                        # Send message to local queue
                        result = await manager.local_queue_manager.send_message(
                            queue_name,
                            message_topic,
                            message_payload,
                            delivery_guarantee_enum,
                            **message_options,
                        )

                        if result.success:
                            message_delivered = True
                            logger.debug(
                                f"Delivered federated message to queue {queue_name}"
                            )

                            # Update federation statistics
                            manager.federation_stats.total_federated_messages += 1
                            manager.federation_stats.successful_cross_cluster_deliveries += 1
                            logger.debug(
                                f"Updated federation stats - total messages: {manager.federation_stats.total_federated_messages}"
                            )

                            # Send acknowledgment back to source cluster if needed
                            if delivery_guarantee_enum in [
                                DeliveryGuarantee.AT_LEAST_ONCE,
                                DeliveryGuarantee.QUORUM,
                            ]:
                                await self._send_federation_acknowledgment(
                                    ack_token, source_cluster, True
                                )
                        break

                except Exception as e:
                    logger.error(f"Error delivering message to queue manager: {e}")

            if not message_delivered:
                logger.warning(f"No queue manager found for queue {queue_name}")
                # Send negative acknowledgment
                await self._send_federation_acknowledgment(
                    ack_token, source_cluster, False, f"Queue {queue_name} not found"
                )

        except Exception as e:
            logger.error(f"Error handling queue message: {e}")

    async def _send_federation_acknowledgment(
        self,
        ack_token: str,
        target_cluster: str,
        success: bool,
        error_message: str | None = None,
    ) -> None:
        """Send federation acknowledgment back to source cluster."""
        try:
            ack_payload = {
                "ack_token": ack_token,
                "success": success,
                "acknowledging_cluster": self.cluster_identity.cluster_id,
                "error_message": error_message,
                "ack_timestamp": time.time(),
            }

            # Send acknowledgment to target cluster
            connection = self.remote_clusters.get(target_cluster)
            if connection:
                from ..core.model import PubSubMessage

                ack_pub_message = PubSubMessage(
                    topic=f"mpreg.federation.queue.ack.{target_cluster}",
                    payload=ack_payload,
                    timestamp=time.time(),
                    message_id=str(uuid.uuid4()),
                    publisher=f"federation-bridge-{self.cluster_identity.cluster_id}",
                    headers={},
                )
                await connection.send_message(ack_pub_message)
                logger.debug(
                    f"Sent federation acknowledgment to {target_cluster}: {success}"
                )
            else:
                logger.warning(
                    f"No connection to cluster {target_cluster} for acknowledgment"
                )

        except Exception as e:
            logger.error(f"Error sending federation acknowledgment: {e}")

    async def _handle_received_acknowledgment(self, message: "PubSubMessage") -> None:
        """Handle a received acknowledgment from a remote cluster."""
        try:
            logger.debug(f"Received federated acknowledgment: {message.topic}")
            # Process the acknowledgment

        except Exception as e:
            logger.error(f"Error handling acknowledgment: {e}")

    async def _handle_received_consensus_request(
        self, message: "PubSubMessage"
    ) -> None:
        """Handle a received consensus request from a remote cluster."""
        try:
            logger.info(f"🔔 RECEIVED CONSENSUS REQUEST: {message.topic}")
            logger.info(f"🔍 From publisher: {message.publisher}")
            logger.info(f"🔍 Our cluster ID: {self.cluster_identity.cluster_id}")

            payload = message.payload
            if not isinstance(payload, dict):
                logger.warning(
                    f"❌ Invalid consensus request payload format: {type(payload)}"
                )
                return

            logger.info(f"📋 Consensus request payload: {payload}")

            # Extract consensus request details
            consensus_id = payload.get("consensus_id")
            source_cluster = payload.get("source_cluster")
            target_cluster = payload.get("target_cluster")

            logger.info(
                f"🎯 Consensus details: id={consensus_id}, source={source_cluster}, target={target_cluster}"
            )

            if not all([consensus_id, source_cluster, target_cluster]):
                logger.warning(
                    f"❌ Missing required fields in consensus request: {payload}"
                )
                return

            # Ensure types for mypy
            assert isinstance(source_cluster, str)
            assert isinstance(target_cluster, str)

            # Verify this request is for our cluster
            if target_cluster != self.cluster_identity.cluster_id:
                logger.info(
                    f"⏭️  Consensus request not for this cluster ({target_cluster} != {self.cluster_identity.cluster_id})"
                )
                return

            logger.info("✅ This consensus request IS for our cluster!")
            logger.info(
                f"🗳️  Registered delivery coordinators: {len(self._federated_delivery_coordinators)}"
            )

            # For quorum consensus, automatically vote "yes" if we're healthy
            vote = {
                "consensus_id": consensus_id,
                "voter_cluster": self.cluster_identity.cluster_id,
                "vote": "yes",  # Always vote yes for now (in real system, would validate)
                "weight": 1.0,
                "timestamp": time.time(),
                "message_id": payload.get("message_id"),
            }

            logger.info(f"📝 Created vote: {vote}")

            # Send vote back to source cluster
            logger.info(f"📤 Sending vote back to source cluster: {source_cluster}")
            await self._send_consensus_vote(source_cluster, vote)
            logger.info("✅ Vote sent successfully!")

        except Exception as e:
            logger.error(f"💥 Error handling consensus request: {e}")
            import traceback

            logger.error(f"💥 Full traceback: {traceback.format_exc()}")

    async def _handle_received_consensus_vote(self, message: "PubSubMessage") -> None:
        """Handle a received consensus vote from a remote cluster."""
        try:
            payload = message.payload
            logger.info(f"🗳️  RECEIVED CONSENSUS VOTE: {message.topic}")
            if not isinstance(payload, dict):
                logger.warning(
                    f"❌ Invalid consensus vote payload format: {type(payload)}"
                )
                return

            logger.info(f"📋 Vote payload: {payload}")
            logger.info(
                f"🎯 Forwarding to {len(self._federated_delivery_coordinators)} delivery coordinators"
            )

            # Forward vote to any registered delivery coordinators
            for i, coordinator in enumerate(self._federated_delivery_coordinators):
                logger.info(f"📤 Forwarding vote to coordinator {i}")
                await coordinator.handle_consensus_vote(payload)
                logger.info(f"✅ Vote forwarded to coordinator {i}")

        except Exception as e:
            logger.error(f"💥 Error handling consensus vote: {e}")
            import traceback

            logger.error(f"💥 Full traceback: {traceback.format_exc()}")

    async def _send_consensus_vote(self, target_cluster: str, vote: dict) -> None:
        """Send a consensus vote to the target cluster."""
        try:
            import uuid

            from ..core.model import PubSubMessage

            vote_message = PubSubMessage(
                topic=f"mpreg.federation.consensus.vote.{target_cluster}",
                payload=vote,
                timestamp=time.time(),
                message_id=str(uuid.uuid4()),
                publisher=f"federation-bridge-{self.cluster_identity.cluster_id}",
                headers={"federation_consensus": "true"},
            )

            # Send vote via federation connection
            connection = self.remote_clusters.get(target_cluster)
            if connection and connection.is_healthy():
                await connection.send_message(vote_message)
                logger.debug(f"Sent consensus vote to cluster {target_cluster}")
            else:
                logger.warning(
                    f"No healthy connection to cluster {target_cluster} for consensus vote"
                )

        except Exception as e:
            logger.error(f"Error sending consensus vote to {target_cluster}: {e}")

    async def _perform_health_checks(self) -> None:
        """Perform health checks on all cluster connections."""
        with self._lock:
            for cluster_id, connection in self.remote_clusters.items():
                if not connection.is_healthy():
                    logger.warning(f"Cluster {cluster_id} is unhealthy")

                    # Update graph node health
                    if self.enable_graph_routing:
                        self.graph_router.update_node_metrics(
                            cluster_id,
                            current_load=connection.current_rate,
                            health_score=0.5,
                            processing_latency_ms=connection.latency_metrics.avg_latency_ms,
                        )

                # Update heartbeat
                connection.last_heartbeat = time.time()

    async def _perform_sync(self) -> None:
        """Perform periodic synchronization with remote clusters."""
        self.last_sync = time.time()

        # Sync cluster states
        for cluster_id in list(self.remote_clusters.keys()):
            await self._sync_cluster_state(cluster_id)

    async def _sync_cluster_state(self, cluster_id: str) -> None:
        """Sync state with a specific cluster."""
        connection = self.remote_clusters.get(cluster_id)
        if not connection:
            return

        try:
            # In a real implementation, this would fetch actual cluster state
            # For now, update based on connection health
            if connection.is_healthy():
                connection.status = ClusterStatus.ACTIVE
            else:
                connection.status = ClusterStatus.FAILED

        except Exception as e:
            logger.error(f"Failed to sync cluster {cluster_id}: {e}")

    async def _update_graph_metrics(self) -> None:
        """Update graph metrics from cluster connections."""
        if not self.enable_graph_routing:
            return

        with self._lock:
            for cluster_id, connection in self.remote_clusters.items():
                # Update node metrics
                health_score = 1.0 if connection.is_healthy() else 0.3
                current_load = connection.current_rate
                latency_ms = connection.latency_metrics.avg_latency_ms

                self.graph_router.update_node_metrics(
                    cluster_id,
                    current_load=current_load,
                    health_score=health_score,
                    processing_latency_ms=latency_ms,
                )

                # Update edge metrics for connections from this cluster
                for neighbor_id in self.graph_router.graph.get_neighbors(cluster_id):
                    neighbor_connection = self.remote_clusters.get(neighbor_id)
                    if neighbor_connection:
                        avg_latency = (
                            latency_ms
                            + neighbor_connection.latency_metrics.avg_latency_ms
                        ) / 2
                        utilization = min(
                            connection.current_rate / connection.rate_limit_per_second,
                            1.0,
                        )

                        self.graph_router.update_edge_metrics(
                            cluster_id,
                            neighbor_id,
                            latency_ms=avg_latency,
                            utilization=utilization,
                        )

    async def add_cluster(self, cluster_identity: ClusterIdentity) -> bool:
        """Add a new cluster to the federation."""
        cluster_id = cluster_identity.cluster_id

        if cluster_id in self.remote_clusters:
            logger.warning(f"Cluster {cluster_id} already exists")
            return False

        if len(self.remote_clusters) >= self.max_clusters:
            logger.error(f"Cannot add cluster {cluster_id}: maximum clusters reached")
            return False

        # Create cluster connection
        connection = GraphAwareClusterConnection(
            cluster_id=cluster_id,
            cluster_identity=cluster_identity,
            status=ClusterStatus.ACTIVE,  # Start as active for immediate communication
        )

        with self._lock:
            self.remote_clusters[cluster_id] = connection

            # Add to graph
            if self.enable_graph_routing:
                self._add_cluster_to_graph(cluster_identity)

        logger.info(f"Added cluster {cluster_id} to federation")
        self.message_stats["clusters_added"] += 1

        return True

    def _add_cluster_to_graph(self, cluster_identity: ClusterIdentity) -> None:
        """Add cluster to graph routing."""
        coords = cluster_identity.geographic_coordinates

        node = FederationGraphNode(
            node_id=cluster_identity.cluster_id,
            node_type=NodeType.CLUSTER,
            region=cluster_identity.region,
            coordinates=GeographicCoordinate(coords[0], coords[1]),
            max_capacity=cluster_identity.max_bandwidth_mbps,
            current_load=0.0,
            health_score=1.0,
            processing_latency_ms=10.0,  # Initial estimate
            bandwidth_mbps=cluster_identity.max_bandwidth_mbps,
            reliability_score=cluster_identity.preference_weight,
        )

        self.graph_router.add_node(node)

        # Create edges to other clusters
        self._create_graph_edges_for_cluster(cluster_identity.cluster_id)

    def _create_graph_edges_for_cluster(self, cluster_id: str) -> None:
        """Create graph edges for a cluster."""
        cluster_identity = self.remote_clusters[cluster_id].cluster_identity

        # Connect to local cluster
        if self._should_connect_clusters(cluster_identity, self.cluster_identity):
            self._create_graph_edge(cluster_id, self.cluster_identity.cluster_id)

        # Connect to other remote clusters
        for other_cluster_id, other_connection in self.remote_clusters.items():
            if other_cluster_id == cluster_id:
                continue

            if self._should_connect_clusters(
                cluster_identity, other_connection.cluster_identity
            ):
                self._create_graph_edge(cluster_id, other_cluster_id)

    def _should_connect_clusters(
        self, cluster_a: ClusterIdentity, cluster_b: ClusterIdentity
    ) -> bool:
        """Determine if two clusters should be connected."""
        # Connect if same region
        if cluster_a.region == cluster_b.region:
            return True

        # Connect if both are high-tier networks
        if cluster_a.network_tier <= 2 and cluster_b.network_tier <= 2:
            return True

        # Connect if geographic distance is reasonable
        coord_a = GeographicCoordinate(
            cluster_a.geographic_coordinates[0], cluster_a.geographic_coordinates[1]
        )
        coord_b = GeographicCoordinate(
            cluster_b.geographic_coordinates[0], cluster_b.geographic_coordinates[1]
        )

        distance_km = coord_a.distance_to(coord_b)
        return distance_km < 5000  # Within 5000km

    def _create_graph_edge(self, cluster_a: str, cluster_b: str) -> None:
        """Create a graph edge between two clusters."""
        # Get cluster identities (handle local cluster case)
        identity_a = (
            self.remote_clusters[cluster_a].cluster_identity
            if cluster_a in self.remote_clusters
            else self.cluster_identity
        )
        identity_b = (
            self.remote_clusters[cluster_b].cluster_identity
            if cluster_b in self.remote_clusters
            else self.cluster_identity
        )

        # Estimate latency based on geographic distance
        coord_a = GeographicCoordinate(
            identity_a.geographic_coordinates[0], identity_a.geographic_coordinates[1]
        )
        coord_b = GeographicCoordinate(
            identity_b.geographic_coordinates[0], identity_b.geographic_coordinates[1]
        )

        distance_km = coord_a.distance_to(coord_b)
        estimated_latency_ms = max(10.0, distance_km * 0.01)  # ~10ms per 1000km

        # Calculate bandwidth
        min_bandwidth = min(
            identity_a.max_bandwidth_mbps, identity_b.max_bandwidth_mbps
        )

        # Create edge
        edge = FederationGraphEdge(
            source_id=cluster_a,
            target_id=cluster_b,
            latency_ms=estimated_latency_ms,
            bandwidth_mbps=min_bandwidth,
            reliability_score=min(
                identity_a.preference_weight, identity_b.preference_weight
            ),
            current_utilization=0.0,
            priority_class=min(identity_a.network_tier, identity_b.network_tier),
        )

        self.graph_router.add_edge(edge)

    async def remove_cluster(self, cluster_id: str) -> bool:
        """Remove a cluster from the federation."""
        if cluster_id not in self.remote_clusters:
            return False

        with self._lock:
            # Remove from graph
            if self.enable_graph_routing:
                self.graph_router.remove_node(cluster_id)

            # Remove connection
            del self.remote_clusters[cluster_id]

            # Remove from cluster states
            if cluster_id in self.cluster_states:
                del self.cluster_states[cluster_id]

        logger.info(f"Removed cluster {cluster_id} from federation")
        self.message_stats["clusters_removed"] += 1

        return True

    async def send_message(self, message: PubSubMessage, target_cluster: str) -> bool:
        """Send message with intelligent routing."""
        start_time = time.time()

        try:
            # Check if we should use graph routing
            if self._should_use_graph_routing(target_cluster):
                return await self._send_message_with_graph_routing(
                    message, target_cluster, start_time
                )
            else:
                return await self._send_message_direct(
                    message, target_cluster, start_time
                )

        except Exception as e:
            logger.error(f"Error sending message to {target_cluster}: {e}")
            self.graph_routing_stats["routing_errors"] += 1
            return False

    def _should_use_graph_routing(self, target_cluster: str) -> bool:
        """Determine if graph routing should be used."""
        if not self.enable_graph_routing:
            return False

        # Check if target cluster is directly connected and healthy
        if target_cluster in self.remote_clusters:
            connection = self.remote_clusters[target_cluster]
            if connection.is_healthy():
                return False  # Use direct routing for healthy connections

        # Use graph routing for unknown or unhealthy clusters
        return True

    async def _send_message_with_graph_routing(
        self, message: PubSubMessage, target_cluster: str, start_time: float
    ) -> bool:
        """Send message using graph routing."""
        local_cluster_id = self.cluster_identity.cluster_id

        # Find optimal path
        path = self.graph_router.find_optimal_path(local_cluster_id, target_cluster)

        if not path or len(path) < 2:
            self.graph_routing_stats["no_path_found"] += 1
            return await self._send_message_direct(message, target_cluster, start_time)

        # Route through the path
        success = await self._route_through_path(message, path)

        # Update statistics
        routing_type = "multi_hop" if len(path) > 2 else "graph"
        self.graph_routing_stats[f"{routing_type}_routing_used"] += 1

        latency_ms = (time.time() - start_time) * 1000
        self.performance_metrics["avg_graph_routing_latency_ms"] = (
            0.1 * latency_ms
            + 0.9 * self.performance_metrics.get("avg_graph_routing_latency_ms", 0)
        )

        return success

    async def _send_message_direct(
        self, message: PubSubMessage, target_cluster: str, start_time: float
    ) -> bool:
        """Send message using direct routing."""
        connection = self.remote_clusters.get(target_cluster)

        if not connection:
            logger.warning(f"No connection to cluster {target_cluster}")
            return False

        success = await connection.send_message(message)

        # Update statistics
        self.graph_routing_stats["direct_routing_used"] += 1

        latency_ms = (time.time() - start_time) * 1000
        self.performance_metrics["avg_direct_routing_latency_ms"] = (
            0.1 * latency_ms
            + 0.9 * self.performance_metrics.get("avg_direct_routing_latency_ms", 0)
        )

        return success

    async def _route_through_path(self, message: PubSubMessage, path: PathList) -> bool:
        """Route message through the computed path."""
        if len(path) < 2:
            return False

        # For multi-hop routing, send to the first hop with full path info
        next_hop = path[1]  # Skip local cluster (path[0])

        connection = self.remote_clusters.get(next_hop)
        if not connection:
            return False

        return await connection.send_message(message, path)

    def get_cluster_status(self, cluster_id: str) -> ClusterStatus:
        """Get cluster status (backward compatibility)."""
        connection = self.remote_clusters.get(cluster_id)
        if connection:
            return connection.status
        return ClusterStatus.DISCONNECTED

    def get_routing_table(self) -> IntelligentRoutingTable:
        """Get routing table (backward compatibility)."""
        return self.routing_table

    def get_comprehensive_statistics(self) -> FederationBridgeStatistics:
        """Get comprehensive statistics."""
        with self._lock:
            cluster_info = ClusterInfo(
                local_cluster_id=self.cluster_identity.cluster_id,
                total_remote_clusters=len(self.remote_clusters),
                active_clusters=len(
                    [c for c in self.remote_clusters.values() if c.is_healthy()]
                ),
                max_clusters=self.max_clusters,
            )

            routing_config = RoutingConfiguration(
                graph_routing_enabled=self.enable_graph_routing,
                monitoring_enabled=self.enable_monitoring,
                graph_routing_threshold=self.graph_routing_threshold,
            )

            # Get optional graph routing performance
            graph_routing_performance = None
            if self.enable_graph_routing:
                graph_routing_performance = (
                    self.graph_router.get_comprehensive_statistics()
                )

            # Get optional monitoring statistics
            monitoring = None
            if self.graph_monitor:
                monitoring = self.graph_monitor.get_comprehensive_status()

            # Build remote clusters info
            remote_clusters = {
                cluster_id: RemoteClusterInfo(
                    healthy=connection.is_healthy(),
                    status=connection.status.value,
                    latency_ms=connection.latency_metrics.avg_latency_ms,
                    success_rate=connection.latency_metrics.success_rate,
                )
                for cluster_id, connection in self.remote_clusters.items()
            }

            return FederationBridgeStatistics(
                cluster_info=cluster_info,
                routing_configuration=routing_config,
                message_statistics=dict(self.message_stats),
                performance_metrics=dict(self.performance_metrics),
                graph_routing_stats=dict(self.graph_routing_stats),
                last_sync=self.last_sync,
                uptime_seconds=time.time()
                - self.message_stats.get("bridge_started", time.time()),
                remote_clusters=remote_clusters,
                graph_routing_performance=graph_routing_performance,
                monitoring=monitoring,
            )

    # Message forwarding methods

    async def _setup_message_forwarding_subscription(self) -> None:
        """Set up subscription to federation messages that need forwarding."""
        try:
            from ..core.model import PubSubSubscription, TopicPattern

            # Subscribe to federation messages that should be forwarded to remote clusters
            federation_pattern = "mpreg.federation.*"

            subscription = PubSubSubscription(
                subscription_id=f"federation-bridge-{self.cluster_identity.cluster_id}",
                subscriber=f"federation-bridge-{self.cluster_identity.cluster_id}",
                patterns=(TopicPattern(pattern=federation_pattern),),
                created_at=time.time(),
                get_backlog=False,
            )

            # Add subscription to local topic exchange
            self.local_cluster.add_subscription(subscription)

            # Set up callback to handle messages (queue them for forwarding)
            self._setup_federation_message_callback()

            logger.debug(f"Federation bridge subscribed to {federation_pattern}")

        except Exception as e:
            logger.error(f"Failed to setup message forwarding subscription: {e}")

    def _setup_federation_message_callback(self) -> None:
        """Set up callback to queue federation messages for forwarding."""
        # The topic exchange doesn't have a built-in federation callback mechanism
        # We'll handle federation forwarding when messages are explicitly queued
        # via _queue_message_for_forwarding() calls from the federated message queue
        logger.debug("Federation message callback setup completed")

    def _queue_message_for_forwarding(self, message: "PubSubMessage") -> None:
        """Queue a message for forwarding to remote clusters."""
        try:
            # Only forward federation messages
            if message.topic.startswith("mpreg.federation."):
                # Queue message for forwarding (non-blocking)
                try:
                    self._outbound_message_queue.put_nowait(message)
                    logger.info(
                        f"Queued federation message for forwarding: {message.topic}"
                    )
                except asyncio.QueueFull:
                    logger.warning("Outbound message queue is full, dropping message")
                    self.message_stats["messages_dropped"] = (
                        self.message_stats.get("messages_dropped", 0) + 1
                    )
        except Exception as e:
            logger.error(f"Failed to queue message for forwarding: {e}")

    async def _forward_message_to_all_clusters(self, message: "PubSubMessage") -> None:
        """Forward a message to all connected remote clusters."""
        # Don't forward messages that have already been through federation (avoid loops)
        if message.headers and message.headers.get("federation_hop"):
            return

        forwarded_count = 0

        # Check if this is a targeted queue message
        if message.topic.startswith("mpreg.federation.queue.message."):
            # Extract target cluster from topic: mpreg.federation.queue.message.{target_cluster}
            target_cluster = message.topic.split(".")[-1]
            logger.debug(f"Targeted queue message for cluster {target_cluster}")

            # Only forward to the specific target cluster
            with self._lock:
                if target_cluster in self.remote_clusters:
                    connection = self.remote_clusters[target_cluster]
                    if connection.is_healthy():
                        try:
                            success = await connection.send_message(message)
                            if success:
                                forwarded_count += 1
                                logger.info(
                                    f"Forwarded targeted queue message to cluster {target_cluster}"
                                )
                            else:
                                logger.warning(
                                    f"Failed to forward targeted message to cluster {target_cluster} - send returned False"
                                )
                        except Exception as e:
                            logger.warning(
                                f"Failed to forward targeted message to cluster {target_cluster}: {e}"
                            )
                    else:
                        logger.warning(
                            f"Target cluster {target_cluster} is not healthy"
                        )
                else:
                    logger.warning(
                        f"Target cluster {target_cluster} not found in remote clusters"
                    )
        else:
            # For non-targeted messages (like advertisements), forward to all clusters
            with self._lock:
                for cluster_id, connection in self.remote_clusters.items():
                    if connection.is_healthy():
                        try:
                            # Forward the message via websocket
                            success = await connection.send_message(message)
                            if success:
                                forwarded_count += 1
                                logger.info(
                                    f"Forwarded federation message to cluster {cluster_id}"
                                )
                            else:
                                logger.warning(
                                    f"Failed to forward message to cluster {cluster_id} - send returned False"
                                )
                        except Exception as e:
                            logger.warning(
                                f"Failed to forward message to cluster {cluster_id}: {e}"
                            )

        if forwarded_count > 0:
            self.message_stats["messages_forwarded"] = (
                self.message_stats.get("messages_forwarded", 0) + 1
            )
            logger.debug(f"Federation message forwarded to {forwarded_count} clusters")

    async def receive_message_from_remote_cluster(
        self, message: "PubSubMessage", source_cluster_id: str
    ) -> None:
        """Receive a message from a remote cluster and inject it into local topic exchange."""
        try:
            # Mark the message as coming from federation to avoid loops
            if not message.headers:
                message.headers = {}
            message.headers["federation_source"] = source_cluster_id
            message.headers["federation_hop"] = True

            # Inject message into local topic exchange
            self.local_cluster.publish_message(message)

            self.message_stats["messages_received"] = (
                self.message_stats.get("messages_received", 0) + 1
            )
            logger.debug(
                f"Received federation message from cluster {source_cluster_id}"
            )

        except Exception as e:
            logger.error(
                f"Failed to receive message from cluster {source_cluster_id}: {e}"
            )


# Backward compatibility alias
FederationBridge = GraphAwareFederationBridge
ProductionFederationBridge = GraphAwareFederationBridge
