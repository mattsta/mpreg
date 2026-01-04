"""
Hub Registration and Discovery System for MPREG Federation.

This module implements the distributed hub discovery and registration system
that enables automatic cluster-to-hub assignment, hub health monitoring,
and dynamic hub failover for the planet-scale federation architecture.

Key Features:
- Distributed hub registry with consensus-based updates
- Automatic cluster-to-hub assignment based on proximity and load
- Hub health monitoring with failure detection and recovery
- Dynamic hub failover and load redistribution
- Geographic-aware hub discovery
- Scalable registration protocols

This is Phase 2.3 of the Planet-Scale Federation Roadmap.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Any, Protocol
from weakref import WeakSet

from loguru import logger

from mpreg.fabric.federation_graph import GeographicCoordinate

from ..core.statistics import (
    AssignmentStatsData,
    MonitoringStatsData,
    RegistryPerformanceMetrics,
    RegistryPerformanceStats,
)
from .federation_optimized import ClusterIdentity, LatencyMetrics
from .hub_hierarchy import (
    HubSelector,
)
from .hubs import (
    HubCapabilities,
    HubTier,
)

# Data Classes for Type Safety


@dataclass(slots=True)
class ConnectionMetadata:
    """Connection metadata for hubs."""

    # Basic connection properties
    port: int | None = None
    protocol_version: str = "1.0"
    ssl_enabled: bool = False
    compression_enabled: bool = False

    # Authentication and security
    auth_method: str = "none"
    certificate_fingerprint: str | None = None
    encryption_type: str | None = None

    # Connection parameters
    timeout_seconds: float = 30.0
    keepalive_interval: float = 60.0
    max_retries: int = 3

    # Performance tuning
    buffer_size: int = 8192
    max_concurrent_connections: int = 100
    connection_pool_size: int = 10

    # Custom properties for extensions
    custom_properties: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class DiscoveryCriteria:
    """Criteria for hub discovery operations."""

    # Tier and region filtering
    hub_tier: str | None = None
    region: str | None = None

    # Geographic filtering
    latitude: float | None = None
    longitude: float | None = None
    max_distance_km: float = 1000.0

    # Capacity and performance filtering
    min_available_capacity: float = 0.0
    max_load_threshold: float = 0.9
    min_health_score: float = 0.5

    # Result limiting
    max_results: int = 10
    sort_by_distance: bool = True
    sort_by_load: bool = True

    # Discovery method preferences
    preferred_methods: list[str] = field(default_factory=list)
    exclude_failed_hubs: bool = True
    include_experimental: bool = False


@dataclass(slots=True)
class RegistryInfo:
    """Registry configuration and metadata."""

    registry_id: str
    discovery_methods: list[str]
    heartbeat_interval: float
    cleanup_interval: float


@dataclass(slots=True)
class RegistrationCounts:
    """Hub registration count statistics."""

    total_hubs: int
    healthy_hubs: int
    hubs_by_tier: dict[str, int]
    hubs_by_region: dict[str, int]


@dataclass(slots=True)
class RegistryStatistics:
    """Comprehensive registry statistics."""

    registry_info: RegistryInfo
    registration_counts: RegistrationCounts
    performance_stats: RegistryPerformanceStats
    performance_metrics: RegistryPerformanceMetrics


@dataclass(slots=True)
class AssignmentInfo:
    """Cluster assignment configuration information."""

    strategy: str
    total_assignments: int
    healthy_assignments: int


@dataclass(slots=True)
class AssignmentStatistics:
    """Comprehensive assignment statistics."""

    assignment_info: AssignmentInfo
    assignment_stats: AssignmentStatsData
    assignment_history: list[tuple[str, str, float]]  # (cluster_id, hub_id, timestamp)


@dataclass(slots=True)
class MonitoringInfo:
    """Monitoring configuration information."""

    monitoring_interval: float
    failure_threshold: int
    hubs_monitored: int


@dataclass(slots=True)
class HubHealthSummary:
    """Summary of hub health metrics."""

    current_health: float
    avg_health: float
    failure_count: int
    last_check: float


@dataclass(slots=True)
class MonitoringStatistics:
    """Comprehensive monitoring statistics."""

    monitoring_info: MonitoringInfo
    monitoring_stats: MonitoringStatsData
    hub_health_summary: dict[str, HubHealthSummary]


class RegistrationStatus(Enum):
    """Registration status for hubs and clusters."""

    PENDING = "pending"
    REGISTERED = "registered"
    FAILED = "failed"
    EXPIRED = "expired"
    DEREGISTERED = "deregistered"


class DiscoveryMethod(Enum):
    """Methods for hub discovery."""

    BROADCAST = "broadcast"
    MULTICAST = "multicast"
    GOSSIP = "gossip"
    STATIC = "static"
    GEOGRAPHIC = "geographic"


@dataclass(slots=True)
class HubRegistrationInfo:
    """Information about a registered hub."""

    hub_id: str
    hub_tier: HubTier
    hub_capabilities: HubCapabilities
    coordinates: GeographicCoordinate
    region: str

    # Registration metadata
    registration_time: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    heartbeat_interval: float = 30.0
    registration_ttl: float = 300.0  # 5 minutes

    # Connection info
    connection_url: str = ""
    connection_type: str = "websocket"
    connection_metadata: ConnectionMetadata = field(default_factory=ConnectionMetadata)

    # Health metrics
    current_load: float = 0.0
    health_score: float = 1.0
    consecutive_failures: int = 0
    last_failure_time: float = 0.0

    # Capability flags
    supports_discovery: bool = True
    supports_clustering: bool = True
    supports_failover: bool = True
    max_clusters: int = 1000

    def is_expired(self) -> bool:
        """Check if registration is expired."""
        return time.time() - self.last_heartbeat > self.registration_ttl

    def is_healthy(self) -> bool:
        """Check if hub is healthy."""
        return (
            not self.is_expired()
            and self.health_score > 0.5
            and self.consecutive_failures < 3
            and self.current_load < 0.9
        )

    def update_heartbeat(self) -> None:
        """Update heartbeat timestamp."""
        self.last_heartbeat = time.time()

    def record_failure(self) -> None:
        """Record a failure event."""
        self.consecutive_failures += 1
        self.last_failure_time = time.time()
        self.health_score = max(0.1, self.health_score * 0.8)

    def record_success(self) -> None:
        """Record a success event."""
        self.consecutive_failures = 0
        self.health_score = min(1.0, self.health_score * 1.1)


@dataclass(slots=True)
class ClusterRegistrationInfo:
    """Information about a cluster registration."""

    cluster_id: str
    cluster_identity: ClusterIdentity
    assigned_hub_id: str | None = None
    preferred_hub_tier: HubTier = HubTier.LOCAL

    # Registration metadata
    registration_time: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    heartbeat_interval: float = 30.0
    registration_ttl: float = 300.0

    # Assignment history
    assignment_history: list[str] = field(default_factory=list)
    failover_count: int = 0
    last_failover_time: float = 0.0

    # Performance metrics
    connection_quality: float = 1.0
    latency_metrics: LatencyMetrics = field(default_factory=LatencyMetrics)

    def is_expired(self) -> bool:
        """Check if registration is expired."""
        return time.time() - self.last_heartbeat > self.registration_ttl

    def is_healthy(self) -> bool:
        """Check if cluster registration is healthy."""
        return (
            not self.is_expired()
            and self.connection_quality > 0.5
            and self.latency_metrics.success_rate > 0.7
        )

    def update_heartbeat(self) -> None:
        """Update heartbeat timestamp."""
        self.last_heartbeat = time.time()

    def record_failover(self, new_hub_id: str) -> None:
        """Record a failover event."""
        if self.assigned_hub_id:
            self.assignment_history.append(self.assigned_hub_id)
        self.assigned_hub_id = new_hub_id
        self.failover_count += 1
        self.last_failover_time = time.time()


class HubDiscoveryProtocol(Protocol):
    """Protocol for hub discovery implementations."""

    async def discover_hubs(
        self, discovery_criteria: DiscoveryCriteria
    ) -> list[HubRegistrationInfo]:
        """Discover available hubs based on criteria."""
        ...

    async def announce_hub(self, hub_info: HubRegistrationInfo) -> bool:
        """Announce a hub to the network."""
        ...

    async def withdraw_hub(self, hub_id: str) -> bool:
        """Withdraw a hub from the network."""
        ...


@dataclass(slots=True)
class HubRegistry:
    """
    Distributed hub registry with consensus-based updates.

    Provides centralized hub discovery and registration services
    with distributed consensus for reliability and scalability.
    """

    registry_id: str
    discovery_methods: list[DiscoveryMethod] = field(
        default_factory=lambda: [DiscoveryMethod.GEOGRAPHIC]
    )
    heartbeat_interval: float = 30.0
    cleanup_interval: float = 60.0
    registered_hubs: dict[str, HubRegistrationInfo] = field(default_factory=dict)
    hub_by_tier: dict[HubTier, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    hub_by_region: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    hub_by_coordinates: dict[tuple[float, float], str] = field(default_factory=dict)
    discovery_protocols: dict[DiscoveryMethod, HubDiscoveryProtocol] = field(
        default_factory=dict
    )
    registry_stats: RegistryPerformanceStats = field(
        default_factory=RegistryPerformanceStats
    )
    performance_metrics: RegistryPerformanceMetrics = field(
        default_factory=RegistryPerformanceMetrics
    )
    _lock: RLock = field(default_factory=RLock)
    _background_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)
    hub_registered_callbacks: WeakSet[Callable[[HubRegistrationInfo], Any]] = field(
        default_factory=WeakSet
    )
    hub_deregistered_callbacks: WeakSet[Callable[[HubRegistrationInfo], Any]] = field(
        default_factory=WeakSet
    )
    hub_failed_callbacks: WeakSet[Callable[[HubRegistrationInfo], Any]] = field(
        default_factory=WeakSet
    )

    def __post_init__(self) -> None:
        """Initialize HubRegistry."""
        logger.info(
            f"Initialized HubRegistry {self.registry_id} with discovery methods: {self.discovery_methods}"
        )

    async def start(self) -> None:
        """Start the hub registry and background tasks."""
        logger.info(f"Starting HubRegistry {self.registry_id}")

        # Initialize discovery protocols
        await self._initialize_discovery_protocols()

        # Start background tasks
        self._start_background_tasks()

        self.registry_stats.registry_started = int(time.time())

    async def stop(self) -> None:
        """Stop the hub registry and cleanup resources."""
        logger.info(f"Stopping HubRegistry {self.registry_id}")

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()
        self.registry_stats.registry_stopped = int(time.time())

    def _start_background_tasks(self) -> None:
        """Start background tasks for registry operations."""
        # Heartbeat monitoring task
        heartbeat_task = asyncio.create_task(self._heartbeat_monitoring_loop())
        self._background_tasks.add(heartbeat_task)

        # Cleanup task
        cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._background_tasks.add(cleanup_task)

        # Discovery announcement task
        discovery_task = asyncio.create_task(self._discovery_announcement_loop())
        self._background_tasks.add(discovery_task)

    async def _initialize_discovery_protocols(self) -> None:
        """Initialize discovery protocols."""
        for method in self.discovery_methods:
            if method == DiscoveryMethod.GEOGRAPHIC:
                self.discovery_protocols[method] = GeographicDiscoveryProtocol(self)
            elif method == DiscoveryMethod.BROADCAST:
                self.discovery_protocols[method] = BroadcastDiscoveryProtocol(self)
            elif method == DiscoveryMethod.GOSSIP:
                self.discovery_protocols[method] = GossipDiscoveryProtocol(self)
            # Add more discovery protocols as needed

    async def register_hub(self, hub_info: HubRegistrationInfo) -> bool:
        """
        Register a hub with the registry.

        Args:
            hub_info: Hub registration information

        Returns:
            True if registration successful
        """
        hub_id = hub_info.hub_id

        with self._lock:
            # Check if already registered
            if hub_id in self.registered_hubs:
                # Update existing registration
                existing_info = self.registered_hubs[hub_id]
                existing_info.update_heartbeat()
                existing_info.current_load = hub_info.current_load
                existing_info.health_score = hub_info.health_score
                return True

            # Register new hub
            self.registered_hubs[hub_id] = hub_info
            self.hub_by_tier[hub_info.hub_tier].add(hub_id)
            self.hub_by_region[hub_info.region].add(hub_id)

            # Index by coordinates for geographic search
            coord_key = (hub_info.coordinates.latitude, hub_info.coordinates.longitude)
            self.hub_by_coordinates[coord_key] = hub_id

        # Announce to discovery protocols
        for protocol in self.discovery_protocols.values():
            try:
                await protocol.announce_hub(hub_info)
            except Exception as e:
                logger.error(f"Failed to announce hub {hub_id}: {e}")

        # Notify callbacks
        for callback in self.hub_registered_callbacks:
            try:
                await callback(hub_info)
            except Exception as e:
                logger.error(f"Hub registration callback failed: {e}")

        logger.info(
            f"Registered hub {hub_id} ({hub_info.hub_tier.value}) in region {hub_info.region}"
        )
        self.registry_stats.hubs_registered += 1

        return True

    async def deregister_hub(self, hub_id: str) -> bool:
        """
        Deregister a hub from the registry.

        Args:
            hub_id: Hub identifier

        Returns:
            True if deregistration successful
        """
        with self._lock:
            if hub_id not in self.registered_hubs:
                return False

            hub_info = self.registered_hubs[hub_id]

            # Remove from indices
            self.hub_by_tier[hub_info.hub_tier].discard(hub_id)
            self.hub_by_region[hub_info.region].discard(hub_id)

            coord_key = (hub_info.coordinates.latitude, hub_info.coordinates.longitude)
            if coord_key in self.hub_by_coordinates:
                del self.hub_by_coordinates[coord_key]

            # Remove from registry
            del self.registered_hubs[hub_id]

        # Withdraw from discovery protocols
        for protocol in self.discovery_protocols.values():
            try:
                await protocol.withdraw_hub(hub_id)
            except Exception as e:
                logger.error(f"Failed to withdraw hub {hub_id}: {e}")

        # Notify callbacks
        for callback in self.hub_deregistered_callbacks:
            try:
                await callback(hub_info)
            except Exception as e:
                logger.error(f"Hub deregistration callback failed: {e}")

        logger.info(f"Deregistered hub {hub_id}")
        self.registry_stats.hubs_deregistered += 1

        return True

    async def discover_hubs(
        self,
        hub_tier: HubTier | None = None,
        region: str | None = None,
        coordinates: GeographicCoordinate | None = None,
        max_distance_km: float = 1000.0,
        max_results: int = 10,
    ) -> list[HubRegistrationInfo]:
        """
        Discover hubs based on criteria.

        Args:
            hub_tier: Desired hub tier
            region: Desired region
            coordinates: Geographic coordinates for proximity search
            max_distance_km: Maximum distance for proximity search
            max_results: Maximum number of results

        Returns:
            List of matching hub registration info
        """
        candidates = []

        with self._lock:
            # Get candidates based on criteria
            if hub_tier:
                candidate_ids = self.hub_by_tier[hub_tier]
            elif region:
                candidate_ids = self.hub_by_region[region]
            else:
                candidate_ids = set(self.registered_hubs.keys())

            # Filter by health and availability
            for hub_id in candidate_ids:
                if hub_id not in self.registered_hubs:
                    continue

                hub_info = self.registered_hubs[hub_id]

                # Check health
                if not hub_info.is_healthy():
                    continue

                # Check geographic proximity if specified
                if coordinates:
                    distance = hub_info.coordinates.distance_to(coordinates)
                    if distance > max_distance_km:
                        continue

                candidates.append(hub_info)

        # Sort by relevance (health score, load, distance)
        if coordinates:
            candidates.sort(
                key=lambda h: (
                    -h.health_score,  # Higher health score first
                    h.current_load,  # Lower load first
                    h.coordinates.distance_to(coordinates),  # Closer first
                )
            )
        else:
            candidates.sort(
                key=lambda h: (
                    -h.health_score,  # Higher health score first
                    h.current_load,  # Lower load first
                )
            )

        # Limit results
        results = candidates[:max_results]

        self.registry_stats.discovery_requests += 1
        return results

    async def update_hub_health(
        self, hub_id: str, health_metrics: dict[str, float]
    ) -> bool:
        """
        Update hub health metrics.

        Args:
            hub_id: Hub identifier
            health_metrics: Health metrics to update

        Returns:
            True if update successful
        """
        with self._lock:
            if hub_id not in self.registered_hubs:
                return False

            hub_info = self.registered_hubs[hub_id]

            # Update metrics
            hub_info.update_heartbeat()
            hub_info.current_load = health_metrics.get("load", hub_info.current_load)
            hub_info.health_score = health_metrics.get(
                "health_score", hub_info.health_score
            )

            # Check if hub became unhealthy
            if not hub_info.is_healthy():
                # Notify failure callbacks
                for callback in self.hub_failed_callbacks:
                    try:
                        await callback(hub_info)
                    except Exception as e:
                        logger.error(f"Hub failure callback failed: {e}")

        return True

    async def _heartbeat_monitoring_loop(self) -> None:
        """Background task for heartbeat monitoring."""
        while not self._shutdown_event.is_set():
            try:
                await self._check_hub_heartbeats()
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat monitoring error: {e}")
                await asyncio.sleep(self.heartbeat_interval)

    async def _cleanup_loop(self) -> None:
        """Background task for cleanup operations."""
        while not self._shutdown_event.is_set():
            try:
                await self._cleanup_expired_registrations()
                await asyncio.sleep(self.cleanup_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(self.cleanup_interval)

    async def _discovery_announcement_loop(self) -> None:
        """Background task for discovery announcements."""
        while not self._shutdown_event.is_set():
            try:
                await self._announce_healthy_hubs()
                await asyncio.sleep(60.0)  # Announce every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Discovery announcement error: {e}")
                await asyncio.sleep(60.0)

    async def _check_hub_heartbeats(self) -> None:
        """Check hub heartbeats and mark unhealthy hubs."""
        unhealthy_hubs = []

        with self._lock:
            for hub_id, hub_info in self.registered_hubs.items():
                if not hub_info.is_healthy():
                    unhealthy_hubs.append(hub_id)

        # Handle unhealthy hubs
        for hub_id in unhealthy_hubs:
            logger.warning(f"Hub {hub_id} is unhealthy")
            hub_info = self.registered_hubs[hub_id]

            # Notify failure callbacks
            for callback in self.hub_failed_callbacks:
                try:
                    await callback(hub_info)
                except Exception as e:
                    logger.error(f"Hub failure callback failed: {e}")

    async def _cleanup_expired_registrations(self) -> None:
        """Clean up expired hub registrations."""
        expired_hubs = []

        with self._lock:
            for hub_id, hub_info in self.registered_hubs.items():
                if hub_info.is_expired():
                    expired_hubs.append(hub_id)

        # Remove expired hubs
        for hub_id in expired_hubs:
            await self.deregister_hub(hub_id)
            logger.info(f"Removed expired hub {hub_id}")

    async def _announce_healthy_hubs(self) -> None:
        """Announce healthy hubs to discovery protocols."""
        healthy_hubs = []

        with self._lock:
            for hub_info in self.registered_hubs.values():
                if hub_info.is_healthy():
                    healthy_hubs.append(hub_info)

        # Announce to discovery protocols
        for protocol in self.discovery_protocols.values():
            for hub_info in healthy_hubs:
                try:
                    await protocol.announce_hub(hub_info)
                except Exception as e:
                    logger.error(f"Failed to announce hub {hub_info.hub_id}: {e}")

    def get_registry_statistics(self) -> RegistryStatistics:
        """Get comprehensive registry statistics."""
        with self._lock:
            registry_info = RegistryInfo(
                registry_id=self.registry_id,
                discovery_methods=[m.value for m in self.discovery_methods],
                heartbeat_interval=self.heartbeat_interval,
                cleanup_interval=self.cleanup_interval,
            )

            registration_counts = RegistrationCounts(
                total_hubs=len(self.registered_hubs),
                healthy_hubs=len(
                    [h for h in self.registered_hubs.values() if h.is_healthy()]
                ),
                hubs_by_tier={
                    tier.value: len(hubs) for tier, hubs in self.hub_by_tier.items()
                },
                hubs_by_region={
                    region: len(hubs) for region, hubs in self.hub_by_region.items()
                },
            )

            return RegistryStatistics(
                registry_info=registry_info,
                registration_counts=registration_counts,
                performance_stats=self.registry_stats,
                performance_metrics=self.performance_metrics,
            )


@dataclass(slots=True)
class ClusterRegistrar:
    """
    Automatic cluster-to-hub assignment system.

    Provides intelligent cluster assignment based on proximity,
    load balancing, and performance optimization.
    """

    hub_registry: HubRegistry
    hub_selector: HubSelector
    assignment_strategy: str = "optimal"
    cluster_assignments: dict[str, ClusterRegistrationInfo] = field(
        default_factory=dict
    )
    assignment_stats: AssignmentStatsData = field(default_factory=AssignmentStatsData)
    assignment_history: list[tuple[str, str, float]] = field(default_factory=list)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize cluster registrar."""
        logger.info(
            f"Initialized ClusterRegistrar with strategy: {self.assignment_strategy}"
        )

    async def register_cluster(self, cluster_identity: ClusterIdentity) -> str | None:
        """
        Register a cluster and assign it to an optimal hub.

        Args:
            cluster_identity: Cluster identity information

        Returns:
            Assigned hub ID or None if assignment failed
        """
        cluster_id = cluster_identity.cluster_id
        coordinates = GeographicCoordinate(
            cluster_identity.geographic_coordinates[0],
            cluster_identity.geographic_coordinates[1],
        )

        # Find suitable hubs
        candidate_hubs = await self.hub_registry.discover_hubs(
            hub_tier=HubTier.LOCAL,
            region=cluster_identity.region,
            coordinates=coordinates,
            max_distance_km=2000.0,
            max_results=5,
        )

        if not candidate_hubs:
            # Try regional hubs if no local hubs available
            candidate_hubs = await self.hub_registry.discover_hubs(
                hub_tier=HubTier.REGIONAL,
                region=cluster_identity.region,
                coordinates=coordinates,
                max_distance_km=5000.0,
                max_results=3,
            )

        if not candidate_hubs:
            logger.warning(f"No suitable hubs found for cluster {cluster_id}")
            return None

        # Select best hub based on strategy
        best_hub = self._select_best_hub(candidate_hubs, cluster_identity)

        if not best_hub:
            return None

        # Create registration info
        registration_info = ClusterRegistrationInfo(
            cluster_id=cluster_id,
            cluster_identity=cluster_identity,
            assigned_hub_id=best_hub.hub_id,
            preferred_hub_tier=best_hub.hub_tier,
        )

        # Store assignment
        with self._lock:
            self.cluster_assignments[cluster_id] = registration_info
            self.assignment_history.append((cluster_id, best_hub.hub_id, time.time()))

        logger.info(f"Assigned cluster {cluster_id} to hub {best_hub.hub_id}")
        self.assignment_stats.clusters_assigned += 1

        return best_hub.hub_id

    async def deregister_cluster(self, cluster_id: str) -> bool:
        """
        Deregister a cluster.

        Args:
            cluster_id: Cluster identifier

        Returns:
            True if deregistration successful
        """
        with self._lock:
            if cluster_id not in self.cluster_assignments:
                return False

            registration_info = self.cluster_assignments[cluster_id]
            del self.cluster_assignments[cluster_id]

        logger.info(
            f"Deregistered cluster {cluster_id} from hub {registration_info.assigned_hub_id}"
        )
        self.assignment_stats.clusters_deregistered += 1

        return True

    async def reassign_cluster(
        self, cluster_id: str, force: bool = False
    ) -> str | None:
        """
        Reassign a cluster to a different hub.

        Args:
            cluster_id: Cluster identifier
            force: Force reassignment even if current assignment is healthy

        Returns:
            New hub ID or None if reassignment failed
        """
        with self._lock:
            if cluster_id not in self.cluster_assignments:
                return None

            registration_info = self.cluster_assignments[cluster_id]

        # Check if reassignment is needed
        if not force and registration_info.is_healthy():
            return registration_info.assigned_hub_id

        # Deregister and register again
        await self.deregister_cluster(cluster_id)
        new_hub_id = await self.register_cluster(registration_info.cluster_identity)

        if new_hub_id:
            with self._lock:
                if cluster_id in self.cluster_assignments:
                    self.cluster_assignments[cluster_id].record_failover(new_hub_id)

            logger.info(
                f"Reassigned cluster {cluster_id} from {registration_info.assigned_hub_id} to {new_hub_id}"
            )
            self.assignment_stats.clusters_reassigned += 1

        return new_hub_id

    def _select_best_hub(
        self,
        candidate_hubs: list[HubRegistrationInfo],
        cluster_identity: ClusterIdentity,
    ) -> HubRegistrationInfo | None:
        """Select the best hub for a cluster."""
        if not candidate_hubs:
            return None

        cluster_coordinates = GeographicCoordinate(
            cluster_identity.geographic_coordinates[0],
            cluster_identity.geographic_coordinates[1],
        )

        # Score each hub based on strategy
        scored_hubs = []
        for hub_info in candidate_hubs:
            score = self._score_hub_for_cluster(hub_info, cluster_coordinates)
            scored_hubs.append((score, hub_info))

        # Sort by score (higher is better)
        scored_hubs.sort(key=lambda x: x[0], reverse=True)

        return scored_hubs[0][1] if scored_hubs else None

    def _score_hub_for_cluster(
        self, hub_info: HubRegistrationInfo, cluster_coordinates: GeographicCoordinate
    ) -> float:
        """Score a hub for cluster assignment."""
        # Base score from hub health
        base_score = hub_info.health_score

        # Distance score (closer is better)
        distance = hub_info.coordinates.distance_to(cluster_coordinates)
        distance_score = 1.0 / (1.0 + distance / 1000.0)  # Normalize by 1000km

        # Load score (less loaded is better)
        load_score = 1.0 - hub_info.current_load

        # Capacity score
        capacity_score = 1.0 - (
            hub_info.current_load / 1.0
        )  # Assume max capacity is 1.0

        # Combine scores based on strategy
        if self.assignment_strategy == "optimal":
            return (
                0.3 * base_score
                + 0.4 * distance_score
                + 0.2 * load_score
                + 0.1 * capacity_score
            )
        elif self.assignment_strategy == "proximity":
            return 0.2 * base_score + 0.7 * distance_score + 0.1 * load_score
        elif self.assignment_strategy == "load_balanced":
            return (
                0.2 * base_score
                + 0.1 * distance_score
                + 0.5 * load_score
                + 0.2 * capacity_score
            )

        return base_score

    def get_assignment_statistics(self) -> AssignmentStatistics:
        """Get assignment statistics."""
        with self._lock:
            assignment_info = AssignmentInfo(
                strategy=self.assignment_strategy,
                total_assignments=len(self.cluster_assignments),
                healthy_assignments=len(
                    [a for a in self.cluster_assignments.values() if a.is_healthy()]
                ),
            )

            return AssignmentStatistics(
                assignment_info=assignment_info,
                assignment_stats=self.assignment_stats,
                assignment_history=self.assignment_history[
                    -100:
                ],  # Last 100 assignments
            )


@dataclass(slots=True)
class HubHealthMonitor:
    """
    Hub health monitoring and capacity tracking system.

    Monitors hub health, detects failures, and triggers
    automatic failover and load redistribution.
    """

    hub_registry: HubRegistry
    cluster_registrar: ClusterRegistrar
    monitoring_interval: float = 30.0
    failure_threshold: int = 3
    hub_health_history: dict[str, list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )
    hub_failure_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    hub_last_check: dict[str, float] = field(default_factory=dict)
    monitoring_stats: MonitoringStatsData = field(default_factory=MonitoringStatsData)
    _background_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize hub health monitor."""
        logger.info("Initialized HubHealthMonitor")

    async def start(self) -> None:
        """Start health monitoring."""
        await self.start_monitoring()

    async def stop(self) -> None:
        """Stop health monitoring."""
        await self.stop_monitoring()

    async def start_monitoring(self) -> None:
        """Start health monitoring."""
        logger.info("Starting hub health monitoring")

        # Start monitoring task
        monitoring_task = asyncio.create_task(self._monitoring_loop())
        self._background_tasks.add(monitoring_task)

        # Start failover task
        failover_task = asyncio.create_task(self._failover_loop())
        self._background_tasks.add(failover_task)

    async def stop_monitoring(self) -> None:
        """Stop health monitoring."""
        logger.info("Stopping hub health monitoring")

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()

    async def _monitoring_loop(self) -> None:
        """Background task for health monitoring."""
        while not self._shutdown_event.is_set():
            try:
                await self._check_hub_health()
                await asyncio.sleep(self.monitoring_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Hub health monitoring error: {e}")
                await asyncio.sleep(self.monitoring_interval)

    async def _failover_loop(self) -> None:
        """Background task for handling failovers."""
        while not self._shutdown_event.is_set():
            try:
                await self._handle_failed_hubs()
                await asyncio.sleep(60.0)  # Check for failovers every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Hub failover error: {e}")
                await asyncio.sleep(60.0)

    async def _check_hub_health(self) -> None:
        """Check health of all registered hubs."""
        registry_stats = self.hub_registry.get_registry_statistics()
        registered_hubs = registry_stats.registration_counts.total_hubs

        if registered_hubs == 0:
            return

        # Get all registered hubs
        with self.hub_registry._lock:
            hub_infos = list(self.hub_registry.registered_hubs.values())

        # Check each hub
        for hub_info in hub_infos:
            await self._check_single_hub_health(hub_info)

        self.monitoring_stats.health_checks += 1

    async def _check_single_hub_health(self, hub_info: HubRegistrationInfo) -> None:
        """Check health of a single hub."""
        hub_id = hub_info.hub_id
        current_time = time.time()

        # Record health check
        with self._lock:
            self.hub_last_check[hub_id] = current_time

            # Calculate health score
            health_score = self._calculate_health_score(hub_info)

            # Store in history
            self.hub_health_history[hub_id].append(health_score)

            # Keep only last 100 health scores
            if len(self.hub_health_history[hub_id]) > 100:
                self.hub_health_history[hub_id] = self.hub_health_history[hub_id][-100:]

            # Check if hub is failing
            if health_score < 0.5:
                self.hub_failure_counts[hub_id] += 1
            else:
                self.hub_failure_counts[hub_id] = 0

        # Update hub health in registry
        await self.hub_registry.update_hub_health(
            hub_id, {"health_score": health_score, "load": hub_info.current_load}
        )

    def _calculate_health_score(self, hub_info: HubRegistrationInfo) -> float:
        """Calculate health score for a hub."""
        # Base health score
        base_score = hub_info.health_score

        # Penalty for high load
        load_penalty = hub_info.current_load

        # Penalty for consecutive failures
        failure_penalty = min(0.5, hub_info.consecutive_failures * 0.1)

        # Penalty for expiration
        expiration_penalty = 0.5 if hub_info.is_expired() else 0.0

        # Calculate final score
        final_score = base_score - load_penalty - failure_penalty - expiration_penalty

        return max(0.0, min(1.0, final_score))

    async def _handle_failed_hubs(self) -> None:
        """Handle failed hubs and trigger failovers."""
        failed_hubs = []

        with self._lock:
            for hub_id, failure_count in self.hub_failure_counts.items():
                if failure_count >= self.failure_threshold:
                    failed_hubs.append(hub_id)

        # Handle each failed hub
        for hub_id in failed_hubs:
            await self._handle_hub_failure(hub_id)

    async def _handle_hub_failure(self, hub_id: str) -> None:
        """Handle failure of a specific hub."""
        logger.warning(f"Handling failure of hub {hub_id}")

        # Get clusters assigned to this hub
        affected_clusters = []
        with self.cluster_registrar._lock:
            for (
                cluster_id,
                assignment_info,
            ) in self.cluster_registrar.cluster_assignments.items():
                if assignment_info.assigned_hub_id == hub_id:
                    affected_clusters.append(cluster_id)

        # Reassign affected clusters
        for cluster_id in affected_clusters:
            try:
                new_hub_id = await self.cluster_registrar.reassign_cluster(
                    cluster_id, force=True
                )
                if new_hub_id:
                    logger.info(
                        f"Reassigned cluster {cluster_id} from failed hub {hub_id} to {new_hub_id}"
                    )
                else:
                    logger.error(
                        f"Failed to reassign cluster {cluster_id} from failed hub {hub_id}"
                    )
            except Exception as e:
                logger.error(f"Error reassigning cluster {cluster_id}: {e}")

        # Reset failure count
        with self._lock:
            self.hub_failure_counts[hub_id] = 0

        self.monitoring_stats.hub_failures_handled += 1

    def get_monitoring_statistics(self) -> MonitoringStatistics:
        """Get monitoring statistics."""
        with self._lock:
            monitoring_info = MonitoringInfo(
                monitoring_interval=self.monitoring_interval,
                failure_threshold=self.failure_threshold,
                hubs_monitored=len(self.hub_health_history),
            )

            hub_health_summary = {
                hub_id: HubHealthSummary(
                    current_health=history[-1] if history else 0.0,
                    avg_health=sum(history) / len(history) if history else 0.0,
                    failure_count=self.hub_failure_counts[hub_id],
                    last_check=self.hub_last_check.get(hub_id, 0.0),
                )
                for hub_id, history in self.hub_health_history.items()
            }

            return MonitoringStatistics(
                monitoring_info=monitoring_info,
                monitoring_stats=self.monitoring_stats,
                hub_health_summary=hub_health_summary,
            )


# Discovery Protocol Implementations


@dataclass(slots=True)
class GeographicDiscoveryProtocol:
    """Geographic-based hub discovery protocol."""

    hub_registry: HubRegistry

    async def discover_hubs(
        self, discovery_criteria: DiscoveryCriteria
    ) -> list[HubRegistrationInfo]:
        """Discover hubs based on geographic criteria."""
        # Convert discovery criteria to hub registry parameters
        hub_tier = None
        if discovery_criteria.hub_tier:
            # Convert string back to HubTier enum if needed
            for tier in HubTier:
                if tier.value == discovery_criteria.hub_tier:
                    hub_tier = tier
                    break

        region = discovery_criteria.region
        coordinates = None
        if (
            discovery_criteria.latitude is not None
            and discovery_criteria.longitude is not None
        ):
            coordinates = GeographicCoordinate(
                discovery_criteria.latitude, discovery_criteria.longitude
            )

        return await self.hub_registry.discover_hubs(
            hub_tier=hub_tier,
            region=region,
            coordinates=coordinates,
            max_distance_km=discovery_criteria.max_distance_km,
            max_results=discovery_criteria.max_results,
        )

    async def announce_hub(self, hub_info: HubRegistrationInfo) -> bool:
        """Announce hub (no-op for geographic protocol)."""
        return True

    async def withdraw_hub(self, hub_id: str) -> bool:
        """Withdraw hub (no-op for geographic protocol)."""
        return True


@dataclass(slots=True)
class BroadcastDiscoveryProtocol:
    """Broadcast-based hub discovery protocol."""

    hub_registry: HubRegistry

    async def discover_hubs(
        self, discovery_criteria: DiscoveryCriteria
    ) -> list[HubRegistrationInfo]:
        """Discover hubs via broadcast."""
        # Convert discovery criteria to hub registry parameters
        hub_tier = None
        if discovery_criteria.hub_tier:
            for tier in HubTier:
                if tier.value == discovery_criteria.hub_tier:
                    hub_tier = tier
                    break

        region = discovery_criteria.region
        coordinates = None
        if (
            discovery_criteria.latitude is not None
            and discovery_criteria.longitude is not None
        ):
            coordinates = GeographicCoordinate(
                discovery_criteria.latitude, discovery_criteria.longitude
            )

        return await self.hub_registry.discover_hubs(
            hub_tier=hub_tier,
            region=region,
            coordinates=coordinates,
            max_distance_km=discovery_criteria.max_distance_km,
            max_results=discovery_criteria.max_results,
        )

    async def announce_hub(self, hub_info: HubRegistrationInfo) -> bool:
        """Announce hub via broadcast."""
        # Simulate broadcast announcement
        return True

    async def withdraw_hub(self, hub_id: str) -> bool:
        """Withdraw hub via broadcast."""
        # Simulate broadcast withdrawal
        return True


@dataclass(slots=True)
class GossipDiscoveryProtocol:
    """Gossip-based hub discovery protocol."""

    hub_registry: HubRegistry

    async def discover_hubs(
        self, discovery_criteria: DiscoveryCriteria
    ) -> list[HubRegistrationInfo]:
        """Discover hubs via gossip."""
        # Convert discovery criteria to hub registry parameters
        hub_tier = None
        if discovery_criteria.hub_tier:
            for tier in HubTier:
                if tier.value == discovery_criteria.hub_tier:
                    hub_tier = tier
                    break

        region = discovery_criteria.region
        coordinates = None
        if (
            discovery_criteria.latitude is not None
            and discovery_criteria.longitude is not None
        ):
            coordinates = GeographicCoordinate(
                discovery_criteria.latitude, discovery_criteria.longitude
            )

        return await self.hub_registry.discover_hubs(
            hub_tier=hub_tier,
            region=region,
            coordinates=coordinates,
            max_distance_km=discovery_criteria.max_distance_km,
            max_results=discovery_criteria.max_results,
        )

    async def announce_hub(self, hub_info: HubRegistrationInfo) -> bool:
        """Announce hub via gossip."""
        # Simulate gossip announcement
        return True

    async def withdraw_hub(self, hub_id: str) -> bool:
        """Withdraw hub via gossip."""
        # Simulate gossip withdrawal
        return True
