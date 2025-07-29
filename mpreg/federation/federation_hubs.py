"""
Hierarchical Hub Architecture for MPREG Federation.

This module implements a hierarchical federation system with specialized hub nodes
that provide scalable message aggregation and routing for planet-scale deployments.
The hub architecture reduces O(N²) complexity to O(log N) through strategic
aggregation and hierarchical routing.

Key Features:
- Three-tier hub hierarchy: Global → Regional → Local
- Intelligent message aggregation to reduce bandwidth
- Automatic hub selection and failover
- Subscription state compression through bloom filter aggregation
- Hub health monitoring and load balancing

This is Phase 2.1 of the Planet-Scale Federation Roadmap.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Any, Protocol

from loguru import logger

from ..core.model import PubSubMessage
from ..core.statistics import (
    HubAggregationInfo,
    HubCapacity,
    HubComprehensiveStatistics,
    HubCurrentLoad,
    HubInfo,
    HubUtilization,
    TopologyStatistics,
)
from .federation_graph import (
    GeographicCoordinate,
)
from .federation_optimized import (
    ClusterIdentity,
    LatencyMetrics,
    OptimizedBloomFilter,
    OptimizedClusterState,
)


class HubTier(Enum):
    """Tiers in the hub hierarchy."""

    LOCAL = "local"  # Aggregates clusters in a local area
    REGIONAL = "regional"  # Aggregates local hubs in a region
    GLOBAL = "global"  # Aggregates regional hubs globally


@dataclass(slots=True)
class HubCapabilities:
    """Capabilities and characteristics of a hub node."""

    # Basic capacity
    max_clusters: int = 1000
    max_child_hubs: int = 100
    max_subscriptions: int = 1000000

    # Performance characteristics
    aggregation_latency_ms: float = 5.0
    bloom_filter_size: int = 1000000
    message_buffer_size: int = 10000

    # Geographic coverage
    coverage_radius_km: float = 500.0
    preferred_regions: set[str] = field(default_factory=set)

    # Quality of service
    reliability_class: int = 1  # 1=high, 2=medium, 3=low
    bandwidth_mbps: int = 10000
    cpu_capacity: float = 100.0  # CPU units
    memory_capacity_gb: float = 32.0


@dataclass(slots=True)
class HubLoadMetrics:
    """Load and performance metrics for a hub."""

    # Current load
    active_clusters: int = 0
    active_child_hubs: int = 0
    active_subscriptions: int = 0

    # Performance metrics
    current_cpu_usage: float = 0.0
    current_memory_usage_gb: float = 0.0
    messages_per_second: float = 0.0
    aggregation_ratio: float = 0.0  # Compression achieved

    # Network metrics
    inbound_bandwidth_mbps: float = 0.0
    outbound_bandwidth_mbps: float = 0.0

    # Health indicators
    last_health_check: float = field(default_factory=time.time)
    consecutive_failures: int = 0

    def get_utilization_score(self) -> float:
        """Calculate overall utilization score (0.0 to 1.0)."""
        cpu_util = self.current_cpu_usage / 100.0
        memory_util = self.current_memory_usage_gb / 32.0  # Assume 32GB baseline
        network_util = (
            max(self.inbound_bandwidth_mbps, self.outbound_bandwidth_mbps) / 10000.0
        )

        return min(1.0, max(cpu_util, memory_util, network_util))

    def is_healthy(self) -> bool:
        """Check if hub is healthy based on metrics."""
        return (
            self.consecutive_failures < 3
            and self.get_utilization_score() < 0.9
            and time.time() - self.last_health_check < 60.0
        )


@dataclass(slots=True)
class AggregatedSubscriptionState:
    """Aggregated subscription state for efficient distribution."""

    # Aggregated bloom filter
    aggregated_filter: OptimizedBloomFilter = field(
        default_factory=OptimizedBloomFilter
    )

    # Subscription counts by topic pattern
    subscription_counts: dict[str, int] = field(default_factory=dict)

    # Popular topics (for optimization)
    popular_topics: list[str] = field(default_factory=list)

    # Metadata
    total_subscriptions: int = 0
    compression_ratio: float = 0.0
    last_aggregated: float = field(default_factory=time.time)

    def aggregate_from_clusters(
        self, cluster_states: dict[str, OptimizedClusterState]
    ) -> None:
        """Aggregate subscription state from multiple clusters."""
        # Reset aggregated state
        self.aggregated_filter = OptimizedBloomFilter()
        self.subscription_counts.clear()
        self.total_subscriptions = 0

        # Count patterns across all clusters
        pattern_counts: dict[str, int] = defaultdict(int)

        for cluster_state in cluster_states.values():
            # Add patterns to aggregated filter
            for pattern in cluster_state.pattern_set:
                self.aggregated_filter.add_pattern(pattern)
                pattern_counts[pattern] += 1
                self.total_subscriptions += 1

        # Store pattern counts
        self.subscription_counts = dict(pattern_counts)

        # Identify popular topics (appearing in multiple clusters)
        self.popular_topics = [
            pattern for pattern, count in pattern_counts.items() if count > 1
        ]
        self.popular_topics.sort(key=lambda p: pattern_counts[p], reverse=True)

        # Calculate compression ratio
        original_size = sum(len(cs.pattern_set) for cs in cluster_states.values())
        compressed_size = len(self.subscription_counts)
        self.compression_ratio = compressed_size / max(1, original_size)

        self.last_aggregated = time.time()


class HubProtocol(Protocol):
    """Protocol for hub node implementations."""

    def get_hub_id(self) -> str:
        """Get unique identifier for this hub."""
        ...

    def get_hub_tier(self) -> HubTier:
        """Get tier level of this hub."""
        ...

    async def register_cluster(
        self, cluster_id: str, cluster_identity: ClusterIdentity
    ) -> bool:
        """Register a cluster with this hub."""
        ...

    async def unregister_cluster(self, cluster_id: str) -> bool:
        """Unregister a cluster from this hub."""
        ...

    async def route_message(
        self, message: PubSubMessage, routing_hint: str | None = None
    ) -> bool:
        """Route a message through the hub hierarchy."""
        ...

    def get_aggregated_state(self) -> AggregatedSubscriptionState:
        """Get aggregated subscription state."""
        ...


@dataclass
class FederationHub(ABC):
    """
    Base class for federation hub nodes.

    Provides common functionality for all hub types including:
    - Cluster registration and management
    - Message routing and aggregation
    - Health monitoring and load balancing
    - Subscription state aggregation
    """

    hub_id: str
    hub_tier: HubTier
    capabilities: HubCapabilities
    coordinates: GeographicCoordinate
    region: str = "global"

    # Fields assigned in __post_init__
    registered_clusters: dict[str, ClusterIdentity] = field(init=False)
    cluster_states: dict[str, OptimizedClusterState] = field(init=False)
    cluster_connections: dict[str, LatencyMetrics] = field(init=False)
    child_hubs: dict[str, "FederationHub"] = field(init=False)
    parent_hub: "FederationHub | None" = field(init=False)
    aggregated_state: AggregatedSubscriptionState = field(init=False)
    load_metrics: HubLoadMetrics = field(init=False)
    message_buffer: deque[Any] = field(init=False)
    routing_stats: dict[str, int] = field(init=False)
    _lock: RLock = field(init=False)
    _background_tasks: set[asyncio.Task[Any]] = field(init=False)
    _shutdown_event: asyncio.Event = field(init=False)

    def __post_init__(self) -> None:
        """
        Initialize federation hub.
        """
        # Cluster management
        self.registered_clusters = {}
        self.cluster_states = {}
        self.cluster_connections = {}

        # Child hub management (for regional/global hubs)
        self.child_hubs = {}
        self.parent_hub = None

        # Aggregated state
        self.aggregated_state = AggregatedSubscriptionState()

        # Performance monitoring
        self.load_metrics = HubLoadMetrics()

        # Message routing
        self.message_buffer = deque(maxlen=self.capabilities.message_buffer_size)
        self.routing_stats = defaultdict(int)

        # Thread safety
        self._lock = RLock()

        # Background tasks
        self._background_tasks = set()
        self._shutdown_event = asyncio.Event()

        logger.info(
            f"Initialized {self.hub_tier.value} hub {self.hub_id} in region {self.region}"
        )

    def get_hub_id(self) -> str:
        """Get unique identifier for this hub."""
        return self.hub_id

    def get_hub_tier(self) -> HubTier:
        """Get tier level of this hub."""
        return self.hub_tier

    async def start(self) -> None:
        """Start the hub and background tasks."""
        logger.info(f"Starting {self.hub_tier.value} hub {self.hub_id}")

        # Start background tasks
        self._start_background_tasks()

        self.routing_stats["hub_started"] = int(time.time())

    async def stop(self) -> None:
        """Stop the hub and cleanup resources."""
        logger.info(f"Stopping {self.hub_tier.value} hub {self.hub_id}")

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()
        self.routing_stats["hub_stopped"] = int(time.time())

    def _start_background_tasks(self) -> None:
        """Start background tasks for hub operations."""
        # Health monitoring task
        health_task = asyncio.create_task(self._health_monitoring_loop())
        self._background_tasks.add(health_task)

        # Aggregation task
        aggregation_task = asyncio.create_task(self._aggregation_loop())
        self._background_tasks.add(aggregation_task)

        # Load balancing task
        load_balancing_task = asyncio.create_task(self._load_balancing_loop())
        self._background_tasks.add(load_balancing_task)

    async def _health_monitoring_loop(self) -> None:
        """Background task for health monitoring."""
        while not self._shutdown_event.is_set():
            try:
                await self._update_health_metrics()
                await asyncio.sleep(30.0)  # Check every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitoring error in hub {self.hub_id}: {e}")
                await asyncio.sleep(30.0)

    async def _aggregation_loop(self) -> None:
        """Background task for subscription aggregation."""
        while not self._shutdown_event.is_set():
            try:
                await self._update_aggregated_state()
                await asyncio.sleep(60.0)  # Aggregate every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Aggregation error in hub {self.hub_id}: {e}")
                await asyncio.sleep(60.0)

    async def _load_balancing_loop(self) -> None:
        """Background task for load balancing."""
        while not self._shutdown_event.is_set():
            try:
                await self._balance_load()
                await asyncio.sleep(120.0)  # Balance every 2 minutes
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Load balancing error in hub {self.hub_id}: {e}")
                await asyncio.sleep(120.0)

    async def _update_health_metrics(self) -> None:
        """Update health and load metrics."""
        with self._lock:
            # Update cluster counts
            self.load_metrics.active_clusters = len(self.registered_clusters)
            self.load_metrics.active_child_hubs = len(self.child_hubs)
            self.load_metrics.active_subscriptions = (
                self.aggregated_state.total_subscriptions
            )

            # Simulate performance metrics (in real implementation, would collect actual metrics)
            self.load_metrics.current_cpu_usage = min(
                80.0, self.load_metrics.active_clusters * 0.5
            )
            self.load_metrics.current_memory_usage_gb = min(
                28.0, self.load_metrics.active_clusters * 0.01
            )

            # Update health timestamp
            self.load_metrics.last_health_check = time.time()

    async def _update_aggregated_state(self) -> None:
        """Update aggregated subscription state."""
        with self._lock:
            if self.cluster_states:
                self.aggregated_state.aggregate_from_clusters(self.cluster_states)

                # Update compression ratio in load metrics
                self.load_metrics.aggregation_ratio = (
                    self.aggregated_state.compression_ratio
                )

    async def _balance_load(self) -> None:
        """Perform load balancing operations."""
        # Check if we're overloaded
        if self.load_metrics.get_utilization_score() > 0.8:
            logger.warning(
                f"Hub {self.hub_id} is overloaded (utilization: {self.load_metrics.get_utilization_score():.2f})"
            )

            # In a real implementation, would trigger load redistribution
            # For now, just log the condition

    async def register_cluster(
        self, cluster_id: str, cluster_identity: ClusterIdentity
    ) -> bool:
        """Register a cluster with this hub."""
        with self._lock:
            # Check if already registered
            if cluster_id in self.registered_clusters:
                return False

            # Check capacity
            if len(self.registered_clusters) >= self.capabilities.max_clusters:
                logger.warning(
                    f"Hub {self.hub_id} at capacity, cannot register cluster {cluster_id}"
                )
                return False

            # Register cluster
            self.registered_clusters[cluster_id] = cluster_identity
            self.cluster_states[cluster_id] = OptimizedClusterState(
                cluster_id=cluster_id
            )
            self.cluster_connections[cluster_id] = LatencyMetrics()

            # Update load metrics immediately
            self.load_metrics.active_clusters = len(self.registered_clusters)

            # Update CPU usage based on cluster count
            self.load_metrics.current_cpu_usage = min(
                80.0, self.load_metrics.active_clusters * 0.5
            )
            self.load_metrics.current_memory_usage_gb = min(
                28.0, self.load_metrics.active_clusters * 0.01
            )

            logger.info(f"Registered cluster {cluster_id} with hub {self.hub_id}")
            self.routing_stats["clusters_registered"] += 1

            return True

    async def unregister_cluster(self, cluster_id: str) -> bool:
        """Unregister a cluster from this hub."""
        with self._lock:
            if cluster_id not in self.registered_clusters:
                return False

            # Remove cluster
            del self.registered_clusters[cluster_id]
            del self.cluster_states[cluster_id]
            del self.cluster_connections[cluster_id]

            # Update load metrics immediately
            self.load_metrics.active_clusters = len(self.registered_clusters)

            # Update CPU usage based on cluster count
            self.load_metrics.current_cpu_usage = min(
                80.0, self.load_metrics.active_clusters * 0.5
            )
            self.load_metrics.current_memory_usage_gb = min(
                28.0, self.load_metrics.active_clusters * 0.01
            )

            logger.info(f"Unregistered cluster {cluster_id} from hub {self.hub_id}")
            self.routing_stats["clusters_unregistered"] += 1

            return True

    async def route_message(
        self, message: PubSubMessage, routing_hint: str | None = None
    ) -> bool:
        """Route a message through the hub hierarchy."""
        try:
            # Add to message buffer
            self.message_buffer.append(message)

            # Update routing statistics
            self.routing_stats["messages_routed"] += 1
            self.load_metrics.messages_per_second = len(self.message_buffer) / max(
                1, time.time() - self.routing_stats.get("hub_started", time.time())
            )

            # Route based on hub tier
            return await self._route_message_by_tier(message, routing_hint)

        except Exception as e:
            logger.error(f"Message routing error in hub {self.hub_id}: {e}")
            self.routing_stats["routing_errors"] += 1
            return False

    @abstractmethod
    async def _route_message_by_tier(
        self, message: PubSubMessage, routing_hint: str | None
    ) -> bool:
        """Route message based on hub tier (implemented by subclasses)."""
        pass

    def get_aggregated_state(self) -> AggregatedSubscriptionState:
        """Get aggregated subscription state."""
        return self.aggregated_state

    def register_child_hub(self, child_hub: "FederationHub") -> bool:
        """Register a child hub."""
        with self._lock:
            if child_hub.hub_id in self.child_hubs:
                return False

            if len(self.child_hubs) >= self.capabilities.max_child_hubs:
                return False

            self.child_hubs[child_hub.hub_id] = child_hub
            child_hub.parent_hub = self

            logger.info(
                f"Registered child hub {child_hub.hub_id} with parent {self.hub_id}"
            )
            return True

    def unregister_child_hub(self, child_hub_id: str) -> bool:
        """Unregister a child hub."""
        with self._lock:
            if child_hub_id not in self.child_hubs:
                return False

            child_hub = self.child_hubs[child_hub_id]
            child_hub.parent_hub = None
            del self.child_hubs[child_hub_id]

            logger.info(
                f"Unregistered child hub {child_hub_id} from parent {self.hub_id}"
            )
            return True

    def get_comprehensive_statistics(self) -> HubComprehensiveStatistics:
        """Get comprehensive hub statistics."""
        with self._lock:
            hub_info = HubInfo(
                hub_id=self.hub_id,
                hub_tier=self.hub_tier.value,
                region=self.region,
                coordinates=(
                    self.coordinates.latitude,
                    self.coordinates.longitude,
                ),
            )

            capacity = HubCapacity(
                max_clusters=self.capabilities.max_clusters,
                max_child_hubs=self.capabilities.max_child_hubs,
                max_subscriptions=self.capabilities.max_subscriptions,
            )

            current_load = HubCurrentLoad(
                active_clusters=self.load_metrics.active_clusters,
                active_child_hubs=self.load_metrics.active_child_hubs,
                active_subscriptions=self.load_metrics.active_subscriptions,
                utilization_score=self.load_metrics.get_utilization_score(),
                is_healthy=self.load_metrics.is_healthy(),
            )

            aggregation_info = HubAggregationInfo(
                total_subscriptions=self.aggregated_state.total_subscriptions,
                compression_ratio=self.aggregated_state.compression_ratio,
                popular_topics_count=len(self.aggregated_state.popular_topics),
                last_aggregated=self.aggregated_state.last_aggregated,
            )

            return HubComprehensiveStatistics(
                hub_info=hub_info,
                capacity=capacity,
                current_load=current_load,
                routing_statistics=dict(self.routing_stats),
                aggregation_info=aggregation_info,
                child_hubs=list(self.child_hubs.keys()),
                parent_hub=self.parent_hub.hub_id if self.parent_hub else None,
            )


@dataclass(slots=True)
class LocalHub(FederationHub):
    """
    Local hub that aggregates clusters in a local area.

    Local hubs are the first tier in the hierarchy and directly manage
    clusters within their geographic coverage area.
    """

    def __post_init__(self) -> None:
        """Initialize local hub."""
        FederationHub.__post_init__(self)

    async def _route_message_by_tier(
        self, message: PubSubMessage, routing_hint: str | None
    ) -> bool:
        """Route message at local hub level."""
        # Local hubs route directly to clusters or up to parent
        if routing_hint and routing_hint in self.registered_clusters:
            # Route to specific cluster
            logger.debug(
                f"Local hub {self.hub_id} routing message to cluster {routing_hint}"
            )
            return True

        # Check if any local clusters are interested
        interested_clusters = []
        for cluster_id, cluster_state in self.cluster_states.items():
            if message.topic in cluster_state.pattern_set:
                interested_clusters.append(cluster_id)

        if interested_clusters:
            # Route to interested local clusters
            logger.debug(
                f"Local hub {self.hub_id} routing message to {len(interested_clusters)} interested clusters"
            )
            return True

        # Route up to parent hub if no local interest
        if self.parent_hub:
            logger.debug(
                f"Local hub {self.hub_id} routing message up to parent {self.parent_hub.hub_id}"
            )
            return await self.parent_hub.route_message(message, routing_hint)

        return False


@dataclass(slots=True)
class RegionalHub(FederationHub):
    """
    Regional hub that aggregates local hubs in a region.

    Regional hubs form the middle tier of the hierarchy and manage
    local hubs within their geographic region.
    """

    def __post_init__(self) -> None:
        """Initialize regional hub."""
        FederationHub.__post_init__(self)

    async def _route_message_by_tier(
        self, message: PubSubMessage, routing_hint: str | None
    ) -> bool:
        """Route message at regional hub level."""
        # Regional hubs route to child local hubs or up to global

        # Check child hubs for interest
        interested_hubs = []
        for hub_id, child_hub in self.child_hubs.items():
            if message.topic in child_hub.get_aggregated_state().subscription_counts:
                interested_hubs.append(hub_id)

        if interested_hubs:
            # Route to interested child hubs
            logger.debug(
                f"Regional hub {self.hub_id} routing message to {len(interested_hubs)} interested child hubs"
            )

            # Route to each interested child hub
            success_count = 0
            for hub_id in interested_hubs:
                child_hub = self.child_hubs[hub_id]
                if await child_hub.route_message(message, routing_hint):
                    success_count += 1

            return success_count > 0

        # Route up to global hub if no regional interest
        if self.parent_hub:
            logger.debug(
                f"Regional hub {self.hub_id} routing message up to global {self.parent_hub.hub_id}"
            )
            return await self.parent_hub.route_message(message, routing_hint)

        return False


@dataclass(slots=True)
class GlobalHub(FederationHub):
    """
    Global hub that aggregates regional hubs globally.

    Global hubs form the top tier of the hierarchy and coordinate
    message routing across all regions worldwide.
    """

    def __post_init__(self) -> None:
        """Initialize global hub."""
        FederationHub.__post_init__(self)

    async def _route_message_by_tier(
        self, message: PubSubMessage, routing_hint: str | None
    ) -> bool:
        """Route message at global hub level."""
        # Global hubs route to regional hubs based on subscription interest

        # Check all regional hubs for interest
        interested_hubs = []
        for hub_id, child_hub in self.child_hubs.items():
            if message.topic in child_hub.get_aggregated_state().subscription_counts:
                interested_hubs.append(hub_id)

        if interested_hubs:
            # Route to interested regional hubs
            logger.debug(
                f"Global hub {self.hub_id} routing message to {len(interested_hubs)} interested regional hubs"
            )

            # Route to each interested regional hub
            success_count = 0
            for hub_id in interested_hubs:
                child_hub = self.child_hubs[hub_id]
                if await child_hub.route_message(message, routing_hint):
                    success_count += 1

            return success_count > 0

        # No interest found globally
        logger.debug(
            f"Global hub {self.hub_id} found no interest for message topic {message.topic}"
        )
        return False


@dataclass(slots=True)
class HubTopology:
    """
    Manages the overall hub topology and hierarchy.

    Provides utilities for creating, managing, and navigating the
    three-tier hub hierarchy.
    """

    global_hubs: dict[str, "GlobalHub"] = field(default_factory=dict)
    regional_hubs: dict[str, "RegionalHub"] = field(default_factory=dict)
    local_hubs: dict[str, "LocalHub"] = field(default_factory=dict)
    hub_hierarchy: dict[str, list[str]] = field(
        default_factory=dict
    )  # parent -> children
    hub_registry: dict[str, FederationHub] = field(
        default_factory=dict
    )  # hub_id -> hub
    topology_stats: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _lock: RLock = field(default_factory=RLock)

    def add_global_hub(self, hub: GlobalHub) -> bool:
        """Add a global hub to the topology."""
        with self._lock:
            if hub.hub_id in self.hub_registry:
                return False

            self.global_hubs[hub.hub_id] = hub
            self.hub_registry[hub.hub_id] = hub
            self.hub_hierarchy[hub.hub_id] = []

            self.topology_stats["global_hubs"] += 1
            logger.info(f"Added global hub {hub.hub_id} to topology")
            return True

    def add_regional_hub(self, hub: RegionalHub, parent_global_hub_id: str) -> bool:
        """Add a regional hub to the topology under a global hub."""
        with self._lock:
            if (
                hub.hub_id in self.hub_registry
                or parent_global_hub_id not in self.global_hubs
            ):
                return False

            # Add to topology
            self.regional_hubs[hub.hub_id] = hub
            self.hub_registry[hub.hub_id] = hub
            self.hub_hierarchy[hub.hub_id] = []

            # Establish parent-child relationship
            parent_hub = self.global_hubs[parent_global_hub_id]
            parent_hub.register_child_hub(hub)
            self.hub_hierarchy[parent_global_hub_id].append(hub.hub_id)

            self.topology_stats["regional_hubs"] += 1
            logger.info(
                f"Added regional hub {hub.hub_id} under global hub {parent_global_hub_id}"
            )
            return True

    def add_local_hub(self, hub: LocalHub, parent_regional_hub_id: str) -> bool:
        """Add a local hub to the topology under a regional hub."""
        with self._lock:
            if (
                hub.hub_id in self.hub_registry
                or parent_regional_hub_id not in self.regional_hubs
            ):
                return False

            # Add to topology
            self.local_hubs[hub.hub_id] = hub
            self.hub_registry[hub.hub_id] = hub
            self.hub_hierarchy[hub.hub_id] = []

            # Establish parent-child relationship
            parent_hub = self.regional_hubs[parent_regional_hub_id]
            parent_hub.register_child_hub(hub)
            self.hub_hierarchy[parent_regional_hub_id].append(hub.hub_id)

            self.topology_stats["local_hubs"] += 1
            logger.info(
                f"Added local hub {hub.hub_id} under regional hub {parent_regional_hub_id}"
            )
            return True

    def get_hub(self, hub_id: str) -> FederationHub | None:
        """Get a hub by ID."""
        return self.hub_registry.get(hub_id)

    def find_best_local_hub(
        self, cluster_coordinates: GeographicCoordinate, region: str
    ) -> LocalHub | None:
        """Find the best local hub for a cluster based on location."""
        best_hub = None
        best_distance = float("inf")

        with self._lock:
            for hub in self.local_hubs.values():
                if hub.region != region:
                    continue

                distance = hub.coordinates.distance_to(cluster_coordinates)
                if (
                    distance <= hub.capabilities.coverage_radius_km
                    and distance < best_distance
                    and len(hub.registered_clusters) < hub.capabilities.max_clusters
                ):
                    best_distance = distance
                    best_hub = hub

        return best_hub

    def get_topology_statistics(self) -> TopologyStatistics:
        """Get comprehensive topology statistics."""
        with self._lock:
            total_clusters = sum(
                len(hub.registered_clusters) for hub in self.hub_registry.values()
            )
            total_subscriptions = sum(
                hub.get_aggregated_state().total_subscriptions
                for hub in self.hub_registry.values()
            )

            hub_utilization = HubUtilization(
                global_hubs={
                    hub_id: hub.load_metrics.get_utilization_score()
                    for hub_id, hub in self.global_hubs.items()
                },
                regional_hubs={
                    hub_id: hub.load_metrics.get_utilization_score()
                    for hub_id, hub in self.regional_hubs.items()
                },
                local_hubs={
                    hub_id: hub.load_metrics.get_utilization_score()
                    for hub_id, hub in self.local_hubs.items()
                },
            )

            aggregation_ratios = {
                hub_id: hub.get_aggregated_state().compression_ratio
                for hub_id, hub in self.hub_registry.items()
            }

            return TopologyStatistics(
                hub_counts=dict(self.topology_stats),
                total_hubs=len(self.hub_registry),
                total_clusters=total_clusters,
                total_subscriptions=total_subscriptions,
                hierarchy_depth=3,  # Global -> Regional -> Local
                hub_utilization=hub_utilization,
                aggregation_ratios=aggregation_ratios,
            )

    async def start_all_hubs(self) -> None:
        """Start all hubs in the topology."""
        with self._lock:
            for hub in self.hub_registry.values():
                await hub.start()

    async def stop_all_hubs(self) -> None:
        """Stop all hubs in the topology."""
        with self._lock:
            for hub in self.hub_registry.values():
                await hub.stop()


@dataclass(slots=True)
class TopicBloomFilter:
    """
    Bloom filter for efficient topic subscription tracking across federation.

    Provides probabilistic set membership testing for topic patterns with
    low memory footprint for large-scale federation scenarios.
    """

    size: int = 10000
    hash_count: int = 7
    _bits: int = 0

    def add(self, topic: str) -> None:
        """Add a topic pattern to the bloom filter."""
        for i in range(self.hash_count):
            hash_val = hash((topic, i)) % self.size
            self._bits |= 1 << hash_val

    def might_contain(self, topic: str) -> bool:
        """Check if topic might be in the filter (probabilistic)."""
        for i in range(self.hash_count):
            hash_val = hash((topic, i)) % self.size
            if not (self._bits & (1 << hash_val)):
                return False
        return True

    def clear(self) -> None:
        """Clear the bloom filter."""
        self._bits = 0

    def merge(self, other: "TopicBloomFilter") -> "TopicBloomFilter":
        """Merge another bloom filter with this one."""
        if other.size != self.size or other.hash_count != self.hash_count:
            raise ValueError("Cannot merge bloom filters with different parameters")

        merged = TopicBloomFilter(size=self.size, hash_count=self.hash_count)
        merged._bits = self._bits | other._bits
        return merged

    def add_pattern(self, pattern: str) -> None:
        """Add a pattern to the bloom filter (alias for add)."""
        self.add(pattern)

    def estimated_false_positive_rate(self) -> float:
        """Estimate the false positive rate of the bloom filter."""
        # Calculate number of set bits
        set_bits = bin(self._bits).count("1")
        if set_bits == 0:
            return 0.0

        # Standard bloom filter false positive rate formula
        # FPR = (1 - e^(-k*n/m))^k
        # where k = hash_count, n = number of inserted elements (estimated), m = size
        import math

        # Estimate number of elements from set bits ratio
        estimated_elements = (
            -self.size * math.log(1 - set_bits / self.size) / self.hash_count
        )

        # Calculate false positive rate
        return (
            1 - math.exp(-self.hash_count * estimated_elements / self.size)
        ) ** self.hash_count

    @property
    def bit_array(self) -> bytes:
        """Get the bit array as bytes for memory usage calculation."""
        # Convert integer to bytes, calculating how many bytes we need
        byte_length = (self.size + 7) // 8
        return self._bits.to_bytes(byte_length, byteorder="big")
