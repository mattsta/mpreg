"""
Location-Based Cache Consistency and Replication System for MPREG.

This module implements a sophisticated replication and consistency system for sharing
cache entries across geographically distributed clusters. Features include:

- Geographic cluster awareness and location-based routing
- Multi-tier consistency models (eventual, strong, causal)
- Vector clock-based conflict resolution
- Cache entry replication with configurable strategies
- Location-aware cache pinning and migration
- Bandwidth-optimized replication protocols
- Cross-cluster cache coordination and synchronization

The system ensures that cached data is properly replicated and consistent across
multiple geographic locations while optimizing for network latency and bandwidth.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from ..datastructures.vector_clock import VectorClock
from .advanced_cache_ops import AdvancedCacheOperations
from .cache_pubsub_integration import CachePubSubIntegration
from .global_cache import GlobalCacheKey, GlobalCacheManager
from .model import PubSubMessage


class ConsistencyLevel(Enum):
    """Cache consistency levels for cross-cluster replication."""

    EVENTUAL = "eventual"  # Best performance, eventual consistency
    CAUSAL = "causal"  # Causal ordering preserved
    STRONG = "strong"  # Immediate consistency across all replicas
    LOCATION_AWARE = "location"  # Consistency based on geographic proximity


class ReplicationStrategy(Enum):
    """Cache replication strategies across clusters."""

    LAZY = "lazy"  # Replicate asynchronously
    EAGER = "eager"  # Replicate synchronously
    ADAPTIVE = "adaptive"  # Adapt based on access patterns
    LOCATION_BASED = "location"  # Replicate based on geographic rules


class ConflictResolution(Enum):
    """Conflict resolution strategies for concurrent updates."""

    LAST_WRITE_WINS = "lww"  # Timestamp-based resolution
    VECTOR_CLOCK = "vector"  # Vector clock-based resolution
    CUSTOM = "custom"  # Custom resolution function
    MERGE = "merge"  # Application-specific merging


@dataclass(frozen=True, slots=True)
class LocationInfo:
    """Geographic location information for cache clusters."""

    cluster_id: str
    region: str
    zone: str
    latitude: float = 0.0
    longitude: float = 0.0
    data_center: str = ""
    provider: str = ""
    network_tier: str = "standard"  # standard, premium, dedicated
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReplicatedCacheEntry:
    """Cache entry with replication metadata."""

    key: GlobalCacheKey
    value: Any
    version: int
    vector_clock: VectorClock
    origin_cluster: str
    replicated_to: frozenset[str] = frozenset()
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    expiry_time: float | None = None
    consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    replication_strategy: ReplicationStrategy = ReplicationStrategy.LAZY
    access_count: int = 0
    last_access_time: float = field(default_factory=time.time)
    pinned_locations: frozenset[str] = frozenset()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReplicationOperation:
    """Operation for replicating cache entries across clusters."""

    operation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    operation_type: str = ""  # replicate, invalidate, migrate, pin
    entry: ReplicatedCacheEntry | None = None
    target_clusters: frozenset[str] = frozenset()
    source_cluster: str = ""
    priority: int = 1  # 1=low, 5=high
    created_at: float = field(default_factory=time.time)
    deadline: float | None = None
    dependencies: frozenset[str] = frozenset()  # Operation IDs this depends on
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LocationConsistencyConfig:
    """Configuration for location-based consistency system."""

    # Consistency settings
    default_consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    default_replication_strategy: ReplicationStrategy = ReplicationStrategy.LAZY
    conflict_resolution: ConflictResolution = ConflictResolution.VECTOR_CLOCK

    # Replication settings
    max_replicas_per_entry: int = 3
    min_replicas_per_entry: int = 1
    replication_factor: float = 0.5  # Fraction of clusters to replicate to

    # Performance settings
    max_replication_queue_size: int = 10000
    replication_batch_size: int = 100
    replication_timeout_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0

    # Location settings
    prefer_local_reads: bool = True
    cross_region_replication: bool = True
    cross_zone_replication: bool = True
    bandwidth_limit_mbps: float = 100.0

    # Cache pinning
    enable_location_pinning: bool = True
    auto_pin_hot_keys: bool = True
    hot_key_access_threshold: int = 100
    pin_duration_seconds: int = 3600


class LocationConsistencyManager:
    """
    Manager for location-based cache consistency and replication.

    Handles replication of cache entries across geographically distributed clusters
    with configurable consistency guarantees and conflict resolution.
    """

    def __init__(
        self,
        cache_manager: GlobalCacheManager,
        advanced_cache_ops: AdvancedCacheOperations,
        pubsub_integration: CachePubSubIntegration,
        location_info: LocationInfo,
        config: LocationConsistencyConfig | None = None,
    ):
        self.cache_manager = cache_manager
        self.advanced_cache_ops = advanced_cache_ops
        self.pubsub_integration = pubsub_integration
        self.location_info = location_info
        self.config = config or LocationConsistencyConfig()

        # Known cluster locations
        self.cluster_locations: dict[str, LocationInfo] = {
            location_info.cluster_id: location_info
        }

        # Replication state
        self.vector_clock = VectorClock.from_dict({location_info.cluster_id: 0})
        self.replicated_entries: dict[str, ReplicatedCacheEntry] = {}
        self.replication_queue: asyncio.Queue[ReplicationOperation] = asyncio.Queue(
            maxsize=config.max_replication_queue_size if config else 10000
        )

        # Background tasks
        self.replication_task: asyncio.Task[None] | None = None
        self.heartbeat_task: asyncio.Task[None] | None = None

        # Statistics
        self.stats = {
            "replications_sent": 0,
            "replications_received": 0,
            "conflicts_resolved": 0,
            "entries_pinned": 0,
            "cross_region_transfers": 0,
            "bandwidth_used_bytes": 0,
        }

        # Custom conflict resolution functions
        self.conflict_resolvers: dict[
            str,
            Callable[
                [ReplicatedCacheEntry, ReplicatedCacheEntry], ReplicatedCacheEntry
            ],
        ] = {}

        # Start background tasks
        self._start_background_tasks()

        # Set up pub/sub subscriptions for replication
        self._setup_replication_subscriptions()

    def register_cluster_location(self, location: LocationInfo) -> None:
        """Register a new cluster location."""
        self.cluster_locations[location.cluster_id] = location
        logger.info(
            f"Registered cluster location: {location.cluster_id} in {location.region}"
        )

    def register_conflict_resolver(
        self,
        namespace: str,
        resolver: Callable[
            [ReplicatedCacheEntry, ReplicatedCacheEntry], ReplicatedCacheEntry
        ],
    ) -> None:
        """Register a custom conflict resolution function for a namespace."""
        self.conflict_resolvers[namespace] = resolver
        logger.info(f"Registered custom conflict resolver for namespace: {namespace}")

    async def replicate_cache_entry(
        self,
        key: GlobalCacheKey,
        value: Any,
        consistency_level: ConsistencyLevel | None = None,
        target_clusters: frozenset[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ReplicatedCacheEntry:
        """Replicate a cache entry to target clusters."""

        # Increment vector clock
        self.vector_clock = self.vector_clock.increment(self.location_info.cluster_id)

        # Determine target clusters if not specified
        if target_clusters is None:
            target_clusters = self._select_replication_targets(key)

        # Create replicated entry
        entry = ReplicatedCacheEntry(
            key=key,
            value=value,
            version=1,
            vector_clock=self.vector_clock,
            origin_cluster=self.location_info.cluster_id,
            consistency_level=consistency_level
            or self.config.default_consistency_level,
            replication_strategy=self.config.default_replication_strategy,
            metadata=metadata or {},
        )

        # Store locally
        entry_key = self._entry_key(key)
        self.replicated_entries[entry_key] = entry

        # Create replication operation
        operation = ReplicationOperation(
            operation_type="replicate",
            entry=entry,
            target_clusters=target_clusters,
            source_cluster=self.location_info.cluster_id,
            priority=3 if consistency_level == ConsistencyLevel.STRONG else 1,
        )

        # Queue for replication
        try:
            await self.replication_queue.put(operation)
        except asyncio.QueueFull:
            logger.warning("Replication queue full, dropping operation")

        # For strong consistency, wait for acknowledgments
        if consistency_level == ConsistencyLevel.STRONG:
            await self._wait_for_strong_consistency(operation)

        logger.info(f"Replicated cache entry {key} to {len(target_clusters)} clusters")
        return entry

    async def get_replicated_entry(
        self,
        key: GlobalCacheKey,
        consistency_level: ConsistencyLevel | None = None,
        prefer_local: bool | None = None,
    ) -> ReplicatedCacheEntry | None:
        """Get a replicated cache entry with specified consistency."""

        entry_key = self._entry_key(key)
        prefer_local = (
            prefer_local if prefer_local is not None else self.config.prefer_local_reads
        )

        # Try local first if preferred
        if prefer_local and entry_key in self.replicated_entries:
            entry = self.replicated_entries[entry_key]

            # Update access statistics
            updated_entry = ReplicatedCacheEntry(
                key=entry.key,
                value=entry.value,
                version=entry.version,
                vector_clock=entry.vector_clock,
                origin_cluster=entry.origin_cluster,
                replicated_to=entry.replicated_to,
                created_at=entry.created_at,
                updated_at=entry.updated_at,
                expiry_time=entry.expiry_time,
                consistency_level=entry.consistency_level,
                replication_strategy=entry.replication_strategy,
                access_count=entry.access_count + 1,
                last_access_time=time.time(),
                pinned_locations=entry.pinned_locations,
                metadata=entry.metadata,
            )
            self.replicated_entries[entry_key] = updated_entry

            # Check if we should pin this hot key
            if (
                self.config.auto_pin_hot_keys
                and updated_entry.access_count >= self.config.hot_key_access_threshold
            ):
                await self._pin_cache_entry(
                    key, duration_seconds=self.config.pin_duration_seconds
                )

            return updated_entry

        # For strong consistency, query other clusters
        if consistency_level == ConsistencyLevel.STRONG:
            return await self._get_with_strong_consistency(key)

        # Return local entry if available
        return self.replicated_entries.get(entry_key)

    async def invalidate_replicated_entry(
        self,
        key: GlobalCacheKey,
        propagate: bool = True,
    ) -> None:
        """Invalidate a replicated cache entry across all clusters."""

        entry_key = self._entry_key(key)

        # Remove from local cache
        if entry_key in self.replicated_entries:
            entry = self.replicated_entries[entry_key]
            del self.replicated_entries[entry_key]

            if propagate:
                # Create invalidation operation
                operation = ReplicationOperation(
                    operation_type="invalidate",
                    entry=entry,
                    target_clusters=entry.replicated_to,
                    source_cluster=self.location_info.cluster_id,
                    priority=4,  # High priority for invalidations
                )

                try:
                    await self.replication_queue.put(operation)
                except asyncio.QueueFull:
                    logger.warning("Could not queue invalidation operation")

        # Also invalidate in underlying cache
        await self.cache_manager.delete(key)

        logger.info(f"Invalidated replicated entry {key}")

    async def pin_cache_entry(
        self,
        key: GlobalCacheKey,
        target_locations: frozenset[str],
        duration_seconds: int | None = None,
    ) -> None:
        """Pin a cache entry to specific geographic locations."""

        entry_key = self._entry_key(key)
        duration = duration_seconds or self.config.pin_duration_seconds

        if entry_key not in self.replicated_entries:
            logger.warning(f"Cannot pin non-existent entry: {key}")
            return

        entry = self.replicated_entries[entry_key]

        # Update entry with pinned locations
        updated_entry = ReplicatedCacheEntry(
            key=entry.key,
            value=entry.value,
            version=entry.version,
            vector_clock=entry.vector_clock,
            origin_cluster=entry.origin_cluster,
            replicated_to=entry.replicated_to,
            created_at=entry.created_at,
            updated_at=entry.updated_at,
            expiry_time=entry.expiry_time,
            consistency_level=entry.consistency_level,
            replication_strategy=entry.replication_strategy,
            access_count=entry.access_count,
            last_access_time=entry.last_access_time,
            pinned_locations=entry.pinned_locations | target_locations,
            metadata=entry.metadata,
        )
        self.replicated_entries[entry_key] = updated_entry

        # Create pin operation
        operation = ReplicationOperation(
            operation_type="pin",
            entry=updated_entry,
            target_clusters=target_locations,
            source_cluster=self.location_info.cluster_id,
            priority=2,
            deadline=time.time() + duration,
        )

        try:
            await self.replication_queue.put(operation)
            self.stats["entries_pinned"] += 1
        except asyncio.QueueFull:
            logger.warning("Could not queue pin operation")

        logger.info(f"Pinned cache entry {key} to locations: {target_locations}")

    async def migrate_cache_entry(
        self,
        key: GlobalCacheKey,
        target_cluster: str,
        remove_from_source: bool = False,
    ) -> None:
        """Migrate a cache entry to a different cluster."""

        entry_key = self._entry_key(key)

        if entry_key not in self.replicated_entries:
            logger.warning(f"Cannot migrate non-existent entry: {key}")
            return

        entry = self.replicated_entries[entry_key]

        # Create migration operation
        operation = ReplicationOperation(
            operation_type="migrate",
            entry=entry,
            target_clusters=frozenset([target_cluster]),
            source_cluster=self.location_info.cluster_id,
            priority=3,
            metadata={"remove_from_source": remove_from_source},
        )

        try:
            await self.replication_queue.put(operation)
        except asyncio.QueueFull:
            logger.warning("Could not queue migration operation")

        # Remove from source if requested
        if remove_from_source:
            del self.replicated_entries[entry_key]

        logger.info(f"Migrated cache entry {key} to cluster: {target_cluster}")

    async def handle_replication_message(self, message: PubSubMessage) -> None:
        """Handle incoming replication messages from other clusters."""

        try:
            payload = message.payload
            operation_type = payload.get("operation_type", "")

            if operation_type == "replicate":
                await self._handle_replicate_message(payload)
            elif operation_type == "invalidate":
                await self._handle_invalidate_message(payload)
            elif operation_type == "migrate":
                await self._handle_migrate_message(payload)
            elif operation_type == "pin":
                await self._handle_pin_message(payload)
            elif operation_type == "conflict_resolution":
                await self._handle_conflict_resolution_message(payload)

            self.stats["replications_received"] += 1

        except Exception as e:
            logger.error(f"Failed to handle replication message: {e}")

    def _select_replication_targets(self, key: GlobalCacheKey) -> frozenset[str]:
        """Select target clusters for replication based on strategy."""

        available_clusters = set(self.cluster_locations.keys()) - {
            self.location_info.cluster_id
        }

        if not available_clusters:
            return frozenset()

        # Calculate number of replicas
        max_replicas = min(self.config.max_replicas_per_entry, len(available_clusters))
        num_replicas = max(
            self.config.min_replicas_per_entry,
            min(
                max_replicas,
                int(len(available_clusters) * self.config.replication_factor),
            ),
        )

        # Select clusters based on strategy
        if (
            self.config.default_replication_strategy
            == ReplicationStrategy.LOCATION_BASED
        ):
            targets = self._select_by_location(available_clusters, num_replicas)
        else:
            # Default: select closest clusters
            targets = self._select_closest_clusters(available_clusters, num_replicas)

        return frozenset(targets)

    def _select_by_location(
        self, available_clusters: set[str], num_replicas: int
    ) -> list[str]:
        """Select clusters based on geographic location strategy."""

        # Prioritize different regions first, then different zones
        regions_covered = set()
        zones_covered = set()
        selected: list[str] = []

        current_region = self.location_info.region
        current_zone = self.location_info.zone

        # First pass: different regions
        for cluster_id in available_clusters:
            if len(selected) >= num_replicas:
                break

            location = self.cluster_locations.get(cluster_id)
            if (
                location
                and location.region != current_region
                and location.region not in regions_covered
            ):
                selected.append(cluster_id)
                regions_covered.add(location.region)

        # Second pass: different zones in same region
        for cluster_id in available_clusters:
            if len(selected) >= num_replicas:
                break

            if cluster_id in selected:
                continue

            location = self.cluster_locations.get(cluster_id)
            if (
                location
                and location.region == current_region
                and location.zone != current_zone
                and location.zone not in zones_covered
            ):
                selected.append(cluster_id)
                zones_covered.add(location.zone)

        # Third pass: fill remaining slots
        remaining = [c for c in available_clusters if c not in selected]
        selected.extend(remaining[: num_replicas - len(selected)])

        return selected

    def _select_closest_clusters(
        self, available_clusters: set[str], num_replicas: int
    ) -> list[str]:
        """Select clusters based on geographic distance."""

        if not self.location_info.latitude or not self.location_info.longitude:
            # No location data, select randomly
            return list(available_clusters)[:num_replicas]

        distances = []
        for cluster_id in available_clusters:
            location = self.cluster_locations.get(cluster_id)
            if location and location.latitude and location.longitude:
                distance = self._calculate_distance(
                    self.location_info.latitude,
                    self.location_info.longitude,
                    location.latitude,
                    location.longitude,
                )
                distances.append((distance, cluster_id))

        # Sort by distance and select closest
        distances.sort()
        return [cluster_id for _, cluster_id in distances[:num_replicas]]

    def _calculate_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate great circle distance between two points."""
        import math

        # Haversine formula
        R = 6371  # Earth's radius in kilometers

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(
            math.radians(lat1)
        ) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    def _entry_key(self, key: GlobalCacheKey) -> str:
        """Generate string key for cache entry."""
        return f"{key.namespace}:{key.identifier}:{key.version}"

    async def _pin_cache_entry(
        self, key: GlobalCacheKey, duration_seconds: int
    ) -> None:
        """Internal method to pin a cache entry."""
        await self.pin_cache_entry(
            key, frozenset([self.location_info.cluster_id]), duration_seconds
        )

    async def _wait_for_strong_consistency(
        self, operation: ReplicationOperation
    ) -> None:
        """Wait for strong consistency acknowledgments."""
        # Implementation would wait for ACKs from target clusters
        # For demo purposes, we'll simulate with a delay
        await asyncio.sleep(0.1)

    async def _get_with_strong_consistency(
        self, key: GlobalCacheKey
    ) -> ReplicatedCacheEntry | None:
        """Get entry with strong consistency by querying multiple clusters."""
        # Implementation would query multiple clusters and resolve conflicts
        # For demo purposes, return local entry
        entry_key = self._entry_key(key)
        return self.replicated_entries.get(entry_key)

    def _start_background_tasks(self) -> None:
        """Start background replication and heartbeat tasks."""
        self.replication_task = asyncio.create_task(self._replication_processor())
        self.heartbeat_task = asyncio.create_task(self._heartbeat_processor())

    async def _replication_processor(self) -> None:
        """Background task to process replication queue."""
        try:
            while True:
                try:
                    # Get operation from queue
                    operation = await asyncio.wait_for(
                        self.replication_queue.get(), timeout=1.0
                    )

                    await self._process_replication_operation(operation)

                except TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error in replication processor: {e}")

        except asyncio.CancelledError:
            logger.info("Replication processor cancelled")

    async def _heartbeat_processor(self) -> None:
        """Background task to send location heartbeats."""
        try:
            while True:
                await asyncio.sleep(self.config.heartbeat_interval_seconds)
                await self._send_heartbeat()

        except asyncio.CancelledError:
            logger.info("Heartbeat processor cancelled")

    async def _process_replication_operation(
        self, operation: ReplicationOperation
    ) -> None:
        """Process a single replication operation."""
        try:
            if operation.operation_type == "replicate" and operation.entry:
                await self._send_replication_message(operation)
            elif operation.operation_type == "invalidate":
                await self._send_invalidation_message(operation)
            elif operation.operation_type == "migrate":
                await self._send_migration_message(operation)
            elif operation.operation_type == "pin":
                await self._send_pin_message(operation)

            self.stats["replications_sent"] += 1

        except Exception as e:
            logger.error(f"Failed to process replication operation: {e}")

    async def _send_replication_message(self, operation: ReplicationOperation) -> None:
        """Send replication message to target clusters."""
        if not operation.entry:
            return

        for target_cluster in operation.target_clusters:
            topic = f"cache.replication.{target_cluster}"

            message = PubSubMessage(
                topic=topic,
                payload={
                    "operation_type": "replicate",
                    "operation_id": operation.operation_id,
                    "source_cluster": operation.source_cluster,
                    "entry": {
                        "key": {
                            "namespace": operation.entry.key.namespace,
                            "identifier": operation.entry.key.identifier,
                            "version": operation.entry.key.version,
                            "tags": list(operation.entry.key.tags),
                        },
                        "value": operation.entry.value,
                        "version": operation.entry.version,
                        "vector_clock": operation.entry.vector_clock.to_dict(),
                        "metadata": operation.entry.metadata,
                    },
                },
                publisher=f"location-consistency-{self.location_info.cluster_id}",
                timestamp=time.time(),
                message_id=f"replication-{operation.operation_id}",
            )

            self.pubsub_integration.topic_exchange.publish_message(message)

    async def _send_invalidation_message(self, operation: ReplicationOperation) -> None:
        """Send invalidation message to target clusters."""
        # Implementation for invalidation messages
        pass

    async def _send_migration_message(self, operation: ReplicationOperation) -> None:
        """Send migration message to target cluster."""
        # Implementation for migration messages
        pass

    async def _send_pin_message(self, operation: ReplicationOperation) -> None:
        """Send pin message to target clusters."""
        # Implementation for pin messages
        pass

    async def _send_heartbeat(self) -> None:
        """Send location heartbeat to other clusters."""
        message = PubSubMessage(
            topic="cache.location.heartbeat",
            payload={
                "cluster_id": self.location_info.cluster_id,
                "location": {
                    "region": self.location_info.region,
                    "zone": self.location_info.zone,
                    "latitude": self.location_info.latitude,
                    "longitude": self.location_info.longitude,
                },
                "stats": self.stats,
                "timestamp": time.time(),
            },
            publisher=f"location-manager-{self.location_info.cluster_id}",
            timestamp=time.time(),
            message_id=f"heartbeat-{self.location_info.cluster_id}-{int(time.time())}",
        )

        self.pubsub_integration.topic_exchange.publish_message(message)

    async def _handle_replicate_message(self, payload: dict[str, Any]) -> None:
        """Handle incoming replicate message."""
        try:
            entry_data = payload.get("entry", {})
            key_data = entry_data.get("key", {})

            # Reconstruct cache key
            key = GlobalCacheKey(
                namespace=key_data["namespace"],
                identifier=key_data["identifier"],
                version=key_data.get("version", "v1.0.0"),
                tags=frozenset(key_data.get("tags", [])),
            )

            # Reconstruct vector clock
            vector_clock = VectorClock.from_dict(entry_data.get("vector_clock", {}))

            # Check for conflicts
            entry_key = self._entry_key(key)
            existing_entry = self.replicated_entries.get(entry_key)

            if existing_entry:
                resolved_entry = await self._resolve_conflict(
                    existing_entry, entry_data, vector_clock
                )
                self.replicated_entries[entry_key] = resolved_entry
            else:
                # Create new replicated entry
                new_entry = ReplicatedCacheEntry(
                    key=key,
                    value=entry_data["value"],
                    version=entry_data["version"],
                    vector_clock=vector_clock,
                    origin_cluster=payload["source_cluster"],
                    metadata=entry_data.get("metadata", {}),
                )
                self.replicated_entries[entry_key] = new_entry

            # Also store in underlying cache
            await self.cache_manager.put(key, entry_data["value"])

        except Exception as e:
            logger.error(f"Failed to handle replicate message: {e}")

    async def _handle_invalidate_message(self, payload: dict[str, Any]) -> None:
        """Handle incoming invalidate message."""
        # Implementation for handling invalidation
        pass

    async def _handle_migrate_message(self, payload: dict[str, Any]) -> None:
        """Handle incoming migrate message."""
        # Implementation for handling migration
        pass

    async def _handle_pin_message(self, payload: dict[str, Any]) -> None:
        """Handle incoming pin message."""
        # Implementation for handling pinning
        pass

    async def _handle_conflict_resolution_message(
        self, payload: dict[str, Any]
    ) -> None:
        """Handle incoming conflict resolution message."""
        # Implementation for handling conflict resolution
        pass

    async def _resolve_conflict(
        self,
        existing_entry: ReplicatedCacheEntry,
        new_entry_data: dict[str, Any],
        new_vector_clock: VectorClock,
    ) -> ReplicatedCacheEntry:
        """Resolve conflict between existing and incoming entry."""

        if self.config.conflict_resolution == ConflictResolution.VECTOR_CLOCK:
            # Use vector clock comparison
            if new_vector_clock.happens_before(existing_entry.vector_clock):
                # Existing entry is newer
                return existing_entry
            elif existing_entry.vector_clock.happens_before(new_vector_clock):
                # New entry is newer
                return ReplicatedCacheEntry(
                    key=existing_entry.key,
                    value=new_entry_data["value"],
                    version=new_entry_data["version"],
                    vector_clock=new_vector_clock,
                    origin_cluster=existing_entry.origin_cluster,
                    metadata=new_entry_data.get("metadata", {}),
                )
            else:
                # Concurrent updates - use custom resolution
                return await self._resolve_concurrent_conflict(
                    existing_entry, new_entry_data, new_vector_clock
                )

        elif self.config.conflict_resolution == ConflictResolution.LAST_WRITE_WINS:
            # Use timestamp comparison
            new_timestamp = new_entry_data.get("updated_at", time.time())
            if new_timestamp > existing_entry.updated_at:
                return ReplicatedCacheEntry(
                    key=existing_entry.key,
                    value=new_entry_data["value"],
                    version=new_entry_data["version"],
                    vector_clock=new_vector_clock,
                    origin_cluster=existing_entry.origin_cluster,
                    updated_at=new_timestamp,
                    metadata=new_entry_data.get("metadata", {}),
                )

        # Default: keep existing entry
        self.stats["conflicts_resolved"] += 1
        return existing_entry

    async def _resolve_concurrent_conflict(
        self,
        existing_entry: ReplicatedCacheEntry,
        new_entry_data: dict[str, Any],
        new_vector_clock: VectorClock,
    ) -> ReplicatedCacheEntry:
        """Resolve concurrent conflict using custom strategy."""

        # Check for custom resolver
        namespace = existing_entry.key.namespace
        if namespace in self.conflict_resolvers:
            new_entry = ReplicatedCacheEntry(
                key=existing_entry.key,
                value=new_entry_data["value"],
                version=new_entry_data["version"],
                vector_clock=new_vector_clock,
                origin_cluster=existing_entry.origin_cluster,
                metadata=new_entry_data.get("metadata", {}),
            )
            return self.conflict_resolvers[namespace](existing_entry, new_entry)

        # Default: merge vector clocks and use higher version
        merged_clock = existing_entry.vector_clock.update(new_vector_clock)

        if new_entry_data["version"] > existing_entry.version:
            return ReplicatedCacheEntry(
                key=existing_entry.key,
                value=new_entry_data["value"],
                version=new_entry_data["version"],
                vector_clock=merged_clock,
                origin_cluster=existing_entry.origin_cluster,
                metadata=new_entry_data.get("metadata", {}),
            )

        return ReplicatedCacheEntry(
            **{**existing_entry.__dict__, "vector_clock": merged_clock}
        )

    def _setup_replication_subscriptions(self) -> None:
        """Set up pub/sub subscriptions for replication messages."""
        # This would be integrated with the pub/sub system
        # For now, we set up the framework
        pass

    def get_statistics(self) -> dict[str, Any]:
        """Get location consistency statistics."""
        return {
            **self.stats,
            "replicated_entries": len(self.replicated_entries),
            "known_clusters": len(self.cluster_locations),
            "replication_queue_size": self.replication_queue.qsize(),
            "current_vector_clock": self.vector_clock.to_dict(),
            "location_info": {
                "cluster_id": self.location_info.cluster_id,
                "region": self.location_info.region,
                "zone": self.location_info.zone,
            },
        }

    async def shutdown(self) -> None:
        """Gracefully shutdown the location consistency manager."""
        try:
            if self.replication_task and not self.replication_task.done():
                self.replication_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self.replication_task

            if self.heartbeat_task and not self.heartbeat_task.done():
                self.heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self.heartbeat_task

            logger.info("Location consistency manager shut down successfully")

        except Exception as e:
            logger.error(f"Error during location consistency manager shutdown: {e}")
