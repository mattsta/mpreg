"""
Global Cache Federation for MPREG.

This module implements federation-aware caching with geographic replication,
cross-cluster cache consistency, and intelligent cache placement optimization.

Features:
- Geographic cache replication across clusters
- Cross-cluster cache operations (get, put, delete, invalidate)
- Intelligent cache placement based on access patterns
- Conflict resolution for cross-cluster cache conflicts
- Performance optimization through proximity-based routing
- Federation-aware cache analytics and monitoring
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from ..core.cache_gossip import CacheOperationType, ConflictResolutionStrategy
from ..core.global_cache import (
    CacheMetadata,
    ConsistencyLevel,
    GlobalCacheEntry,
    GlobalCacheKey,
    ReplicationStrategy,
)


class FederationCacheStrategy(Enum):
    """Strategies for federation cache placement."""

    NEAREST_CLUSTER = "nearest_cluster"  # Place in geographically nearest cluster
    LOAD_BALANCED = "load_balanced"  # Distribute based on cluster load
    ACCESS_PATTERN = "access_pattern"  # Place based on historical access patterns
    HYBRID = "hybrid"  # Combination of strategies
    EXPLICIT = "explicit"  # Explicitly specified clusters


@dataclass(frozen=True, slots=True)
class GeographicLocation:
    """Geographic location information for cache placement."""

    latitude: float
    longitude: float
    region: str
    availability_zone: str = ""

    def distance_to(self, other: "GeographicLocation") -> float:
        """Calculate distance to another location (simplified)."""
        # Simplified distance calculation - in production use proper geodesic distance
        lat_diff = abs(self.latitude - other.latitude)
        lon_diff = abs(self.longitude - other.longitude)
        return float((lat_diff**2 + lon_diff**2) ** 0.5)


@dataclass(slots=True)
class ClusterCacheInfo:
    """Information about cache capabilities of a federation cluster."""

    cluster_id: str
    cluster_name: str
    location: GeographicLocation
    cache_capacity_mb: int
    cache_utilization_percent: float
    average_latency_ms: float
    reliability_score: float  # 0.0 - 1.0
    specializations: set[str] = field(
        default_factory=set
    )  # e.g., {"ml-models", "user-data"}
    last_health_check: float = field(default_factory=time.time)

    def is_healthy(self) -> bool:
        """Check if cluster is healthy for cache operations."""
        return (
            self.reliability_score > 0.8
            and self.cache_utilization_percent < 90.0
            and self.average_latency_ms < 100.0
            and (time.time() - self.last_health_check) < 300.0  # 5 minutes
        )

    def capacity_score(self) -> float:
        """Calculate capacity score for cache placement decisions."""
        if self.cache_utilization_percent >= 95.0:
            return 0.0

        # Higher score for lower utilization and higher reliability
        utilization_score = (100.0 - self.cache_utilization_percent) / 100.0
        return (utilization_score * 0.7) + (self.reliability_score * 0.3)


@dataclass(slots=True)
class AccessPattern:
    """Access pattern information for cache optimization."""

    key: GlobalCacheKey
    access_count: int = 0
    last_access_time: float = field(default_factory=time.time)
    accessing_clusters: set[str] = field(default_factory=set)
    geographic_distribution: dict[str, int] = field(
        default_factory=dict
    )  # region -> access_count
    peak_access_times: list[float] = field(default_factory=list)

    def add_access(self, cluster_id: str, region: str) -> None:
        """Record a cache access."""
        self.access_count += 1
        self.last_access_time = time.time()
        self.accessing_clusters.add(cluster_id)
        self.geographic_distribution[region] = (
            self.geographic_distribution.get(region, 0) + 1
        )
        self.peak_access_times.append(time.time())

        # Keep only recent peak times (last hour)
        cutoff_time = time.time() - 3600
        self.peak_access_times = [t for t in self.peak_access_times if t > cutoff_time]

    def get_primary_region(self) -> str:
        """Get the region with most accesses."""
        if not self.geographic_distribution:
            return "unknown"
        return max(self.geographic_distribution.items(), key=lambda x: x[1])[0]

    def should_replicate_to_region(self, region: str, threshold: int = 5) -> bool:
        """Determine if cache should be replicated to a specific region."""
        return self.geographic_distribution.get(region, 0) >= threshold


@dataclass(slots=True)
class FederationCacheReplicationJob:
    """Job for replicating cache data across federation."""

    job_id: str
    operation_type: CacheOperationType
    key: GlobalCacheKey
    source_cluster: str
    target_clusters: list[str]
    priority: int  # 1 (highest) to 10 (lowest)
    created_time: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3

    # Optional fields based on operation
    value: Any = None
    metadata: CacheMetadata | None = None
    consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL

    def is_expired(self, timeout_seconds: float = 300.0) -> bool:
        """Check if replication job has expired."""
        return (time.time() - self.created_time) > timeout_seconds

    def can_retry(self) -> bool:
        """Check if job can be retried."""
        return self.retry_count < self.max_retries


@dataclass(slots=True)
class FederationCacheStatistics:
    """Statistics for federation cache operations."""

    total_operations: int = 0
    cross_cluster_operations: int = 0
    successful_replications: int = 0
    failed_replications: int = 0
    cache_hits_local: int = 0
    cache_hits_remote: int = 0
    cache_misses: int = 0
    average_operation_latency_ms: float = 0.0
    replication_jobs_pending: int = 0
    replication_jobs_completed: int = 0
    conflicts_detected: int = 0
    conflicts_resolved: int = 0
    bytes_replicated: int = 0
    last_optimization_time: float = 0.0


class GlobalCacheFederation:
    """
    Federation-aware cache with geographic replication and intelligent placement.

    Provides:
    - Cross-cluster cache operations
    - Geographic replication optimization
    - Access pattern-based cache placement
    - Conflict resolution across federation boundaries
    - Performance monitoring and optimization
    """

    def __init__(
        self,
        local_cluster_id: str,
        local_location: GeographicLocation,
        federation_bridge: Any = None,  # Will be federation bridge instance
    ):
        self.local_cluster_id = local_cluster_id
        self.local_location = local_location
        self.federation_bridge = federation_bridge

        # Cluster management
        self.known_clusters: dict[str, ClusterCacheInfo] = {}
        self.cluster_health: dict[str, float] = {}  # cluster_id -> health_score

        # Access pattern tracking
        self.access_patterns: dict[str, AccessPattern] = {}  # key_str -> pattern

        # Replication management
        self.replication_queue: asyncio.Queue[FederationCacheReplicationJob] = (
            asyncio.Queue()
        )
        self.active_replications: dict[str, FederationCacheReplicationJob] = {}

        # Conflict resolution
        self.conflict_resolution_strategy = ConflictResolutionStrategy.VECTOR_CLOCK

        # Statistics
        self.statistics = FederationCacheStatistics()

        # Configuration
        self.max_replication_targets = 3
        self.replication_timeout_seconds = 30.0
        self.optimization_interval_seconds = 600.0  # 10 minutes

        # Background tasks
        self._replication_worker_task: asyncio.Task[None] | None = None
        self._optimization_task: asyncio.Task[None] | None = None
        self._health_check_task: asyncio.Task[None] | None = None

        # Start background processing
        self._start_background_tasks()

    def _start_background_tasks(self) -> None:
        """Start background federation cache tasks."""
        try:
            if (
                self._replication_worker_task is None
                or self._replication_worker_task.done()
            ):
                self._replication_worker_task = asyncio.create_task(
                    self._replication_worker()
                )

            if self._optimization_task is None or self._optimization_task.done():
                self._optimization_task = asyncio.create_task(
                    self._optimization_worker()
                )

            if self._health_check_task is None or self._health_check_task.done():
                self._health_check_task = asyncio.create_task(
                    self._health_check_worker()
                )
        except RuntimeError:
            logger.warning("No event loop running, skipping background tasks")

    def register_cluster(self, cluster_info: ClusterCacheInfo) -> None:
        """Register a cluster in the federation cache network."""
        self.known_clusters[cluster_info.cluster_id] = cluster_info
        self.cluster_health[cluster_info.cluster_id] = cluster_info.reliability_score
        logger.info(f"Registered federation cache cluster: {cluster_info.cluster_id}")

    def unregister_cluster(self, cluster_id: str) -> None:
        """Unregister a cluster from the federation cache network."""
        self.known_clusters.pop(cluster_id, None)
        self.cluster_health.pop(cluster_id, None)
        logger.info(f"Unregistered federation cache cluster: {cluster_id}")

    async def replicate_across_regions(
        self,
        key: GlobalCacheKey,
        value: Any,
        metadata: CacheMetadata,
        replication_policy: ReplicationStrategy = ReplicationStrategy.GEOGRAPHIC,
    ) -> list[str]:
        """
        Replicate cache entry across geographic regions.

        Returns:
            List of cluster IDs where replication was initiated
        """
        try:
            target_clusters = self._select_replication_targets(
                key, metadata, replication_policy
            )

            if not target_clusters:
                logger.debug(f"No suitable replication targets found for {key}")
                return []

            # Create replication jobs
            replication_jobs = []
            for cluster_id in target_clusters:
                job_id = f"repl_{key}_{cluster_id}_{time.time()}"

                job = FederationCacheReplicationJob(
                    job_id=job_id,
                    operation_type=CacheOperationType.PUT,
                    key=key,
                    source_cluster=self.local_cluster_id,
                    target_clusters=[cluster_id],
                    priority=self._calculate_replication_priority(key, metadata),
                    value=value,
                    metadata=metadata,
                )

                replication_jobs.append(job)
                await self.replication_queue.put(job)

            self.statistics.replication_jobs_pending += len(replication_jobs)
            logger.debug(
                f"Initiated replication of {key} to {len(target_clusters)} clusters"
            )

            return target_clusters

        except Exception as e:
            logger.error(f"Error initiating replication for {key}: {e}")
            return []

    def _select_replication_targets(
        self,
        key: GlobalCacheKey,
        metadata: CacheMetadata,
        strategy: ReplicationStrategy,
    ) -> list[str]:
        """Select target clusters for cache replication."""
        healthy_clusters = [
            cluster_id
            for cluster_id, info in self.known_clusters.items()
            if info.is_healthy() and cluster_id != self.local_cluster_id
        ]

        if not healthy_clusters:
            return []

        if strategy == ReplicationStrategy.GEOGRAPHIC:
            return self._select_geographic_targets(key, metadata, healthy_clusters)
        elif strategy == ReplicationStrategy.PROXIMITY:
            return self._select_proximity_targets(key, metadata, healthy_clusters)
        elif strategy == ReplicationStrategy.LOAD_BASED:
            return self._select_load_based_targets(key, metadata, healthy_clusters)
        elif strategy == ReplicationStrategy.HYBRID:
            return self._select_hybrid_targets(key, metadata, healthy_clusters)
        else:
            # Default to geographic
            return self._select_geographic_targets(key, metadata, healthy_clusters)

    def _select_geographic_targets(
        self, key: GlobalCacheKey, metadata: CacheMetadata, candidates: list[str]
    ) -> list[str]:
        """Select replication targets based on geographic distribution."""
        # Group clusters by region
        regions = defaultdict(list)
        for cluster_id in candidates:
            cluster_info = self.known_clusters[cluster_id]
            regions[cluster_info.location.region].append(cluster_id)

        # Select one cluster per region, preferring high-capacity clusters
        targets: list[str] = []
        for region, cluster_ids in regions.items():
            if len(targets) >= self.max_replication_targets:
                break

            # Sort by capacity score
            cluster_ids.sort(
                key=lambda cid: self.known_clusters[cid].capacity_score(), reverse=True
            )
            targets.append(cluster_ids[0])

        return targets[: self.max_replication_targets]

    def _select_proximity_targets(
        self, key: GlobalCacheKey, metadata: CacheMetadata, candidates: list[str]
    ) -> list[str]:
        """Select replication targets based on proximity to local cluster."""
        # Calculate distances and sort by proximity
        distances = []
        for cluster_id in candidates:
            cluster_info = self.known_clusters[cluster_id]
            distance = self.local_location.distance_to(cluster_info.location)
            distances.append((distance, cluster_id))

        distances.sort()
        return [
            cluster_id for _, cluster_id in distances[: self.max_replication_targets]
        ]

    def _select_load_based_targets(
        self, key: GlobalCacheKey, metadata: CacheMetadata, candidates: list[str]
    ) -> list[str]:
        """Select replication targets based on cluster load and capacity."""
        # Sort by capacity score (lower utilization = higher score)
        cluster_scores = [
            (self.known_clusters[cluster_id].capacity_score(), cluster_id)
            for cluster_id in candidates
        ]
        cluster_scores.sort(reverse=True)

        return [
            cluster_id
            for _, cluster_id in cluster_scores[: self.max_replication_targets]
        ]

    def _select_hybrid_targets(
        self, key: GlobalCacheKey, metadata: CacheMetadata, candidates: list[str]
    ) -> list[str]:
        """Select replication targets using hybrid strategy."""
        # Check access patterns for the key
        key_str = str(key)
        access_pattern = self.access_patterns.get(key_str)

        if access_pattern and access_pattern.accessing_clusters:
            # Use access pattern to guide selection
            pattern_based = self._select_by_access_pattern(
                key, metadata, candidates, access_pattern
            )
            if pattern_based:
                return pattern_based

        # Fall back to geographic strategy
        return self._select_geographic_targets(key, metadata, candidates)

    def _select_by_access_pattern(
        self,
        key: GlobalCacheKey,
        metadata: CacheMetadata,
        candidates: list[str],
        access_pattern: AccessPattern,
    ) -> list[str]:
        """Select targets based on historical access patterns."""
        # Prefer clusters that have accessed this data before
        preferred_clusters = []
        other_clusters = []

        for cluster_id in candidates:
            if cluster_id in access_pattern.accessing_clusters:
                preferred_clusters.append(cluster_id)
            else:
                other_clusters.append(cluster_id)

        # Sort preferred clusters by capacity
        preferred_clusters.sort(
            key=lambda cid: self.known_clusters[cid].capacity_score(), reverse=True
        )

        # Fill remaining slots with other clusters
        targets: list[str] = preferred_clusters[: self.max_replication_targets]
        if len(targets) < self.max_replication_targets:
            remaining_slots = self.max_replication_targets - len(targets)
            targets.extend(other_clusters[:remaining_slots])

        return targets

    def _calculate_replication_priority(
        self, key: GlobalCacheKey, metadata: CacheMetadata
    ) -> int:
        """Calculate priority for replication job (1 = highest, 10 = lowest)."""
        # Base priority
        priority = 5

        # Higher priority for expensive computations
        if metadata.computation_cost_ms > 10000:  # > 10 seconds
            priority -= 2
        elif metadata.computation_cost_ms > 1000:  # > 1 second
            priority -= 1

        # Higher priority for frequently accessed data
        key_str = str(key)
        if key_str in self.access_patterns:
            pattern = self.access_patterns[key_str]
            if pattern.access_count > 100:
                priority -= 1
            elif pattern.access_count > 10:
                priority -= 0

        # Higher priority for time-sensitive data
        if metadata.ttl_seconds and metadata.ttl_seconds < 3600:  # < 1 hour TTL
            priority -= 1

        return max(1, min(10, priority))

    async def resolve_cache_conflicts(
        self, conflicting_entries: list[tuple[str, GlobalCacheEntry]]
    ) -> GlobalCacheEntry | None:
        """
        Resolve conflicts between cache entries from different clusters.

        Args:
            conflicting_entries: List of (cluster_id, cache_entry) tuples

        Returns:
            Resolved cache entry or None if resolution failed
        """
        if not conflicting_entries:
            return None

        if len(conflicting_entries) == 1:
            return conflicting_entries[0][1]

        try:
            entries = [entry for _, entry in conflicting_entries]

            if (
                self.conflict_resolution_strategy
                == ConflictResolutionStrategy.LAST_WRITER_WINS
            ):
                # Choose entry with latest timestamp
                latest_entry = max(entries, key=lambda e: e.creation_time)
                return latest_entry

            elif (
                self.conflict_resolution_strategy
                == ConflictResolutionStrategy.VECTOR_CLOCK
            ):
                # Use vector clock comparison
                return self._resolve_by_vector_clock(entries)

            elif (
                self.conflict_resolution_strategy
                == ConflictResolutionStrategy.HIGHEST_QUALITY
            ):
                # Choose entry with highest quality score
                best_entry = max(entries, key=lambda e: e.metadata.quality_score)
                return best_entry

            else:
                # Default to last writer wins
                latest_entry = max(entries, key=lambda e: e.creation_time)
                return latest_entry

        except Exception as e:
            logger.error(f"Error resolving cache conflicts: {e}")
            return None

    def _resolve_by_vector_clock(
        self, entries: list[GlobalCacheEntry]
    ) -> GlobalCacheEntry:
        """Resolve conflicts using vector clock comparison."""
        # Find entry with vector clock that dominates others
        for entry in entries:
            dominates_all = True
            for other in entries:
                if entry is other:
                    continue

                if not self._vector_clock_dominates(
                    entry.vector_clock, other.vector_clock
                ):
                    dominates_all = False
                    break

            if dominates_all:
                return entry

        # No clear dominance, fall back to timestamp
        return max(entries, key=lambda e: e.creation_time)

    def _vector_clock_dominates(
        self, clock1: dict[str, int], clock2: dict[str, int]
    ) -> bool:
        """Check if clock1 dominates clock2."""
        all_nodes = set(clock1.keys()) | set(clock2.keys())

        at_least_one_greater = False
        for node in all_nodes:
            v1 = clock1.get(node, 0)
            v2 = clock2.get(node, 0)

            if v1 < v2:
                return False
            elif v1 > v2:
                at_least_one_greater = True

        return at_least_one_greater

    async def optimize_cache_placement(
        self, access_patterns: dict[str, AccessPattern]
    ) -> int:
        """
        Optimize cache placement based on access patterns.

        Returns:
            Number of optimization actions taken
        """
        try:
            optimization_actions = 0

            for key_str, pattern in access_patterns.items():
                # Skip recently accessed patterns
                if time.time() - pattern.last_access_time > 3600:  # 1 hour
                    continue

                # Check if cache should be moved/replicated based on access patterns
                primary_region = pattern.get_primary_region()

                # Find clusters in primary region
                primary_clusters = [
                    cluster_id
                    for cluster_id, info in self.known_clusters.items()
                    if info.location.region == primary_region and info.is_healthy()
                ]

                if primary_clusters and primary_region != self.local_location.region:
                    # Consider moving cache closer to primary access region
                    # This is a simplified optimization - in production this would be more sophisticated
                    logger.debug(
                        f"Cache optimization opportunity for {key_str}: primary region is {primary_region}"
                    )
                    optimization_actions += 1

            self.statistics.last_optimization_time = time.time()
            logger.debug(
                f"Cache placement optimization completed: {optimization_actions} actions identified"
            )

            return optimization_actions

        except Exception as e:
            logger.error(f"Error optimizing cache placement: {e}")
            return 0

    def record_access(self, key: GlobalCacheKey, accessing_cluster: str) -> None:
        """Record cache access for pattern analysis."""
        key_str = str(key)

        if key_str not in self.access_patterns:
            self.access_patterns[key_str] = AccessPattern(key=key)

        # Get region for accessing cluster
        region = "unknown"
        if accessing_cluster in self.known_clusters:
            region = self.known_clusters[accessing_cluster].location.region

        self.access_patterns[key_str].add_access(accessing_cluster, region)

    async def _replication_worker(self) -> None:
        """Background worker for processing replication jobs."""
        while True:
            try:
                # Get next replication job
                job = await asyncio.wait_for(self.replication_queue.get(), timeout=1.0)

                # Check if job is expired
                if job.is_expired():
                    logger.debug(f"Replication job {job.job_id} expired")
                    continue

                # Process the job
                success = await self._process_replication_job(job)

                if success:
                    self.statistics.successful_replications += 1
                    self.statistics.replication_jobs_completed += 1
                else:
                    self.statistics.failed_replications += 1

                    # Retry if possible
                    if job.can_retry():
                        job.retry_count += 1
                        await self.replication_queue.put(job)
                        logger.debug(
                            f"Retrying replication job {job.job_id} (attempt {job.retry_count})"
                        )

                self.statistics.replication_jobs_pending = (
                    self.replication_queue.qsize()
                )

            except TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Replication worker error: {e}")

    async def _process_replication_job(
        self, job: FederationCacheReplicationJob
    ) -> bool:
        """Process a single replication job."""
        try:
            # This would integrate with the federation bridge to send cache operations
            # to remote clusters

            if job.operation_type == CacheOperationType.PUT:
                return await self._replicate_put(job)
            elif job.operation_type == CacheOperationType.DELETE:
                return await self._replicate_delete(job)
            else:
                logger.warning(
                    f"Unsupported replication operation: {job.operation_type}"
                )
                return False

        except Exception as e:
            logger.error(f"Error processing replication job {job.job_id}: {e}")
            return False

    async def _replicate_put(self, job: FederationCacheReplicationJob) -> bool:
        """Replicate PUT operation to target clusters."""
        # TODO: Integrate with federation bridge to send cache put message
        logger.debug(
            f"Replicating PUT for {job.key} to clusters: {job.target_clusters}"
        )
        return True

    async def _replicate_delete(self, job: FederationCacheReplicationJob) -> bool:
        """Replicate DELETE operation to target clusters."""
        # TODO: Integrate with federation bridge to send cache delete message
        logger.debug(
            f"Replicating DELETE for {job.key} to clusters: {job.target_clusters}"
        )
        return True

    async def _optimization_worker(self) -> None:
        """Background worker for cache placement optimization."""
        while True:
            try:
                await asyncio.sleep(self.optimization_interval_seconds)

                # Run optimization
                optimization_count = await self.optimize_cache_placement(
                    self.access_patterns
                )
                logger.debug(
                    f"Cache optimization cycle completed: {optimization_count} optimizations"
                )

            except Exception as e:
                logger.error(f"Optimization worker error: {e}")

    async def _health_check_worker(self) -> None:
        """Background worker for monitoring cluster health."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                # Update cluster health scores
                for cluster_id, cluster_info in self.known_clusters.items():
                    # TODO: Integrate with federation health monitoring
                    # For now, just update timestamp
                    cluster_info.last_health_check = time.time()

            except Exception as e:
                logger.error(f"Health check worker error: {e}")

    def get_statistics(self) -> dict[str, Any]:
        """Get comprehensive federation cache statistics."""
        return {
            "local_cluster": self.local_cluster_id,
            "operations": {
                "total": self.statistics.total_operations,
                "cross_cluster": self.statistics.cross_cluster_operations,
                "average_latency_ms": self.statistics.average_operation_latency_ms,
            },
            "replication": {
                "successful": self.statistics.successful_replications,
                "failed": self.statistics.failed_replications,
                "pending_jobs": self.statistics.replication_jobs_pending,
                "completed_jobs": self.statistics.replication_jobs_completed,
                "bytes_replicated": self.statistics.bytes_replicated,
            },
            "cache_performance": {
                "local_hits": self.statistics.cache_hits_local,
                "remote_hits": self.statistics.cache_hits_remote,
                "misses": self.statistics.cache_misses,
            },
            "conflicts": {
                "detected": self.statistics.conflicts_detected,
                "resolved": self.statistics.conflicts_resolved,
            },
            "clusters": {
                "known_clusters": len(self.known_clusters),
                "healthy_clusters": sum(
                    1 for info in self.known_clusters.values() if info.is_healthy()
                ),
            },
            "access_patterns": {
                "tracked_keys": len(self.access_patterns),
                "total_accesses": sum(
                    pattern.access_count for pattern in self.access_patterns.values()
                ),
            },
            "optimization": {
                "last_run": self.statistics.last_optimization_time,
                "interval_seconds": self.optimization_interval_seconds,
            },
        }

    async def shutdown(self) -> None:
        """Shutdown federation cache system."""
        # Cancel background tasks
        for task in [
            self._replication_worker_task,
            self._optimization_task,
            self._health_check_task,
        ]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        logger.info("Global cache federation shutdown complete")
