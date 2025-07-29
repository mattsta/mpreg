"""
Global Distributed Caching System for MPREG.

This module implements a sophisticated multi-tier global caching system with:
- L1: Memory Cache (S4LRU, Cost-Based Eviction)
- L2: Persistent Cache (SSD/NVMe Storage)
- L3: Distributed Cache (Gossip-Based)
- L4: Federation Cache (Global Replication)

The system follows MPREG's clean dataclass architecture and supports
gossip-based state synchronization and geographic replication.
"""

import asyncio
import hashlib
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar

from loguru import logger

from .caching import CacheConfiguration, SmartCacheManager
from .caching import CacheKey as LocalCacheKey
from .serialization import JsonSerializer
from .task_manager import ManagedObject

T = TypeVar("T")


class ConsistencyLevel(Enum):
    """Cache consistency levels for distributed operations."""

    EVENTUAL = "eventual"  # Best effort, eventual consistency
    STRONG = "strong"  # Wait for majority acknowledgment
    WEAK = "weak"  # Local cache only, no synchronization


class CacheLevel(Enum):
    """Multi-tier cache levels."""

    L1 = "L1"  # Memory cache
    L2 = "L2"  # Persistent cache
    L3 = "L3"  # Distributed cache
    L4 = "L4"  # Federation cache


class ReplicationStrategy(Enum):
    """Cache replication strategies."""

    NONE = "none"
    GEOGRAPHIC = "geographic"  # Replicate across regions
    PROXIMITY = "proximity"  # Replicate to nearby nodes
    LOAD_BASED = "load_based"  # Replicate based on access patterns
    HYBRID = "hybrid"  # Combination of strategies


@dataclass(frozen=True, slots=True)
class GlobalCacheKey:
    """Enhanced cache key for global distributed caching."""

    namespace: str  # e.g., "compute.results"
    identifier: str  # Content-based hash or unique ID
    version: str = "v1.0.0"  # Semantic version
    tags: frozenset[str] = field(
        default_factory=frozenset
    )  # e.g., {"expensive", "ml-model"}

    @classmethod
    def from_function_call(
        cls,
        namespace: str,
        function_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        version: str = "v1.0.0",
        tags: set[str] | None = None,
    ) -> "GlobalCacheKey":
        """Create cache key from function call parameters."""
        # Create content-based identifier
        content = f"{function_name}:{args}:{sorted(kwargs.items())}"
        identifier = hashlib.sha256(content.encode()).hexdigest()[:32]

        return cls(
            namespace=namespace,
            identifier=identifier,
            version=version,
            tags=frozenset(tags or set()),
        )

    @classmethod
    def from_data(
        cls,
        namespace: str,
        data: Any,
        version: str = "v1.0.0",
        tags: set[str] | None = None,
    ) -> "GlobalCacheKey":
        """Create cache key from arbitrary data."""
        # Create content-based identifier from data hash
        serializer = JsonSerializer()
        serialized = serializer.serialize(data)
        identifier = hashlib.sha256(serialized).hexdigest()[:32]

        return cls(
            namespace=namespace,
            identifier=identifier,
            version=version,
            tags=frozenset(tags or set()),
        )

    def to_local_key(self) -> LocalCacheKey:
        """Convert to local cache key format."""
        return LocalCacheKey.create(
            function_name=f"{self.namespace}.{self.identifier}",
            args=(),
            kwargs={"version": self.version, "tags": list(self.tags)},
        )

    def __str__(self) -> str:
        return f"{self.namespace}:{self.identifier}:{self.version}"


@dataclass(frozen=True, slots=True)
class CacheOptions:
    """Options for cache operations."""

    include_metadata: bool = True
    consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    timeout_ms: int = 5000
    cache_levels: frozenset[CacheLevel] = field(
        default_factory=lambda: frozenset([CacheLevel.L1, CacheLevel.L2, CacheLevel.L3])
    )
    replication_factor: int = 2
    prefer_local: bool = True
    max_staleness_seconds: float = 300.0


@dataclass(slots=True)
class CacheMetadata:
    """Metadata for cached entries."""

    computation_cost_ms: float = 0.0
    dependencies: set[GlobalCacheKey] = field(default_factory=set)
    ttl_seconds: float | None = None
    replication_policy: ReplicationStrategy = ReplicationStrategy.GEOGRAPHIC
    access_patterns: dict[str, Any] = field(default_factory=dict)
    geographic_hints: list[str] = field(default_factory=list)  # Preferred regions
    quality_score: float = 1.0  # Quality/importance score for eviction
    created_by: str = ""  # Node that created the entry
    size_estimate_bytes: int = 0


@dataclass(frozen=True, slots=True)
class CachePerformanceMetrics:
    """Performance metrics for cache operations."""

    lookup_time_ms: float
    network_hops: int
    cache_efficiency: float  # 0.0 - 1.0
    replication_latency_ms: float = 0.0
    conflict_resolution_time_ms: float = 0.0


@dataclass(slots=True)
class GlobalCacheEntry:
    """Enhanced cache entry with global distribution metadata."""

    key: GlobalCacheKey
    value: Any
    metadata: CacheMetadata
    creation_time: float = field(default_factory=time.time)
    last_access_time: float = field(default_factory=time.time)
    access_count: int = 0
    replication_sites: set[str] = field(
        default_factory=set
    )  # Node IDs where replicated
    vector_clock: dict[str, int] = field(
        default_factory=dict
    )  # For conflict resolution
    checksum: str = ""  # Data integrity verification

    def __post_init__(self) -> None:
        """Initialize computed fields."""
        if not self.checksum:
            # Create checksum for data integrity
            serializer = JsonSerializer()
            serialized = serializer.serialize(self.value)
            self.checksum = hashlib.sha256(serialized).hexdigest()[:16]

    def access(self) -> None:
        """Record access to this cache entry."""
        self.access_count += 1
        self.last_access_time = time.time()

    def is_expired(self) -> bool:
        """Check if entry has exceeded its TTL."""
        if self.metadata.ttl_seconds is None:
            return False
        return (time.time() - self.creation_time) > self.metadata.ttl_seconds

    def verify_integrity(self) -> bool:
        """Verify data integrity using checksum."""
        try:
            serializer = JsonSerializer()
            serialized = serializer.serialize(self.value)
            current_checksum = hashlib.sha256(serialized).hexdigest()[:16]
            return current_checksum == self.checksum
        except Exception as e:
            logger.warning(f"Cache integrity check failed for {self.key}: {e}")
            return False


@dataclass(frozen=True, slots=True)
class CacheOperationResult:
    """Result of a cache operation."""

    success: bool
    cache_level: CacheLevel | None = None
    entry: GlobalCacheEntry | None = None
    performance: CachePerformanceMetrics | None = None
    error_message: str | None = None
    operation_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(slots=True)
class CacheReplicationPolicy:
    """Policy for cache replication across nodes."""

    strategy: ReplicationStrategy = ReplicationStrategy.GEOGRAPHIC
    min_replicas: int = 2
    max_replicas: int = 5
    preferred_regions: list[str] = field(default_factory=list)
    consistency_model: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    conflict_resolution: str = (
        "last_writer_wins"  # "vector_clock", "timestamp", "custom"
    )
    replication_timeout_ms: int = 10000


@dataclass(slots=True)
class GlobalCacheConfiguration:
    """Configuration for global cache manager."""

    # Local cache configuration
    local_cache_config: CacheConfiguration = field(default_factory=CacheConfiguration)

    # Multi-tier settings
    enable_l2_persistent: bool = True
    enable_l3_distributed: bool = True
    enable_l4_federation: bool = True

    # Persistent cache settings (L2)
    persistent_cache_dir: Path = field(default_factory=lambda: Path("/tmp/mpreg_cache"))
    persistent_cache_size_mb: int = 1000  # 1GB

    # Distributed cache settings (L3)
    gossip_interval_seconds: float = 30.0
    gossip_batch_size: int = 100

    # Federation cache settings (L4)
    federation_sync_interval_seconds: float = 300.0
    cross_region_replication: bool = True

    # Replication settings
    default_replication_policy: CacheReplicationPolicy = field(
        default_factory=CacheReplicationPolicy
    )

    # Performance settings
    operation_timeout_ms: int = 30000
    max_concurrent_operations: int = 100
    enable_compression: bool = True

    # Geographic settings
    local_region: str = "unknown"
    local_cluster_id: str = ""


class GlobalCacheManager(ManagedObject):
    """
    Multi-tier global cache manager with intelligent replication and federation support.

    Provides a unified interface to:
    - L1: In-memory cache with S4LRU eviction
    - L2: Persistent SSD/NVMe cache
    - L3: Distributed cache via gossip protocol
    - L4: Federation cache with geographic replication
    """

    def __init__(self, config: GlobalCacheConfiguration) -> None:
        super().__init__(name=f"GlobalCacheManager-{id(self)}")
        self.config = config

        # Initialize L1 memory cache
        self.l1_cache = SmartCacheManager[Any](config.local_cache_config)

        # Initialize operation tracking
        self.active_operations: dict[str, asyncio.Task[Any]] = {}
        self.operation_semaphore = asyncio.Semaphore(config.max_concurrent_operations)

        # Initialize statistics
        self.operation_stats: dict[str, int] = defaultdict(int)
        self.performance_metrics: dict[CacheLevel, list[float]] = defaultdict(list)

        # Initialize replication tracking
        self.replication_state: dict[GlobalCacheKey, set[str]] = defaultdict(set)
        self.pending_replications: asyncio.Queue[tuple[str, GlobalCacheKey, Any]] = (
            asyncio.Queue()
        )

        # Initialize namespace index for efficient namespace operations
        self.namespace_index: dict[str, set[GlobalCacheKey]] = defaultdict(set)

        # Initialize L2 persistent cache if enabled
        self.l2_cache: dict[str, GlobalCacheEntry] = {}
        if config.enable_l2_persistent:
            self._init_persistent_cache()

        # Start background tasks using task manager
        self._start_background_tasks()

    def _init_persistent_cache(self) -> None:
        """Initialize persistent cache storage."""
        try:
            self.config.persistent_cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                f"Initialized persistent cache at {self.config.persistent_cache_dir}"
            )
        except Exception as e:
            logger.error(f"Failed to initialize persistent cache: {e}")
            self.config.enable_l2_persistent = False

    def _start_background_tasks(self) -> None:
        """Start background maintenance tasks using task manager."""
        try:
            # Create managed tasks that will be properly cleaned up
            self.create_task(self._replication_worker(), name="replication_worker")
            self.create_task(self._cleanup_worker(), name="cleanup_worker")

            if self.config.enable_l3_distributed:
                self.create_task(self._gossip_worker(), name="gossip_worker")

            logger.info(
                f"Started {len(self._task_manager)} background tasks for GlobalCacheManager"
            )
        except RuntimeError:
            # No event loop running, skip background tasks
            logger.warning("No event loop running, skipping background tasks")

    async def get(
        self, key: GlobalCacheKey, options: CacheOptions | None = None
    ) -> CacheOperationResult:
        """
        Retrieve value from multi-tier cache.

        Searches cache levels in order: L1 → L2 → L3 → L4
        """
        if options is None:
            options = CacheOptions()

        operation_id = str(uuid.uuid4())
        start_time = time.time()

        async with self.operation_semaphore:
            try:
                # Try L1 memory cache first
                if CacheLevel.L1 in options.cache_levels:
                    local_key = key.to_local_key()
                    l1_value = self.l1_cache.get(local_key)

                    if l1_value is not None:
                        self.operation_stats["l1_hits"] += 1
                        lookup_time = (time.time() - start_time) * 1000

                        # Convert back to GlobalCacheEntry if needed
                        if isinstance(l1_value, GlobalCacheEntry):
                            l1_value.access()
                            return CacheOperationResult(
                                success=True,
                                cache_level=CacheLevel.L1,
                                entry=l1_value,
                                performance=CachePerformanceMetrics(
                                    lookup_time_ms=lookup_time,
                                    network_hops=0,
                                    cache_efficiency=1.0,
                                ),
                            )

                # Try L2 persistent cache
                if (
                    CacheLevel.L2 in options.cache_levels
                    and self.config.enable_l2_persistent
                ):
                    l2_result = await self._get_from_l2(key, options)
                    if l2_result.success:
                        # Promote to L1
                        if l2_result.entry:
                            self._promote_to_l1(l2_result.entry)
                        self.operation_stats["l2_hits"] += 1
                        return l2_result

                # Try L3 distributed cache
                if (
                    CacheLevel.L3 in options.cache_levels
                    and self.config.enable_l3_distributed
                ):
                    l3_result = await self._get_from_l3(key, options)
                    if l3_result.success:
                        # Promote to L1 and L2
                        if l3_result.entry:
                            self._promote_to_l1(l3_result.entry)
                            if self.config.enable_l2_persistent:
                                await self._put_to_l2(l3_result.entry)
                        self.operation_stats["l3_hits"] += 1
                        return l3_result

                # Try L4 federation cache
                if (
                    CacheLevel.L4 in options.cache_levels
                    and self.config.enable_l4_federation
                ):
                    l4_result = await self._get_from_l4(key, options)
                    if l4_result.success:
                        # Promote to all lower levels
                        if l4_result.entry:
                            self._promote_to_l1(l4_result.entry)
                            if self.config.enable_l2_persistent:
                                await self._put_to_l2(l4_result.entry)
                        self.operation_stats["l4_hits"] += 1
                        return l4_result

                # Cache miss
                self.operation_stats["misses"] += 1
                return CacheOperationResult(
                    success=False, error_message="Cache miss across all levels"
                )

            except Exception as e:
                logger.error(f"Cache get operation failed for {key}: {e}")
                return CacheOperationResult(
                    success=False, error_message=f"Cache operation error: {e}"
                )

    async def put(
        self,
        key: GlobalCacheKey,
        value: Any,
        metadata: CacheMetadata | None = None,
        options: CacheOptions | None = None,
    ) -> CacheOperationResult:
        """
        Store value in multi-tier cache with replication.
        """
        if metadata is None:
            metadata = CacheMetadata()

        if options is None:
            options = CacheOptions()

        operation_id = str(uuid.uuid4())
        start_time = time.time()

        async with self.operation_semaphore:
            try:
                # Create cache entry
                entry = GlobalCacheEntry(key=key, value=value, metadata=metadata)
                entry.metadata.created_by = self.config.local_cluster_id

                # Update namespace index
                self._add_to_namespace_index(key)

                # Store in requested cache levels
                success_levels = []

                if CacheLevel.L1 in options.cache_levels:
                    self._put_to_l1(entry)
                    success_levels.append(CacheLevel.L1)

                if (
                    CacheLevel.L2 in options.cache_levels
                    and self.config.enable_l2_persistent
                ):
                    await self._put_to_l2(entry)
                    success_levels.append(CacheLevel.L2)

                if (
                    CacheLevel.L3 in options.cache_levels
                    and self.config.enable_l3_distributed
                ):
                    await self._put_to_l3(entry, options)
                    success_levels.append(CacheLevel.L3)

                if (
                    CacheLevel.L4 in options.cache_levels
                    and self.config.enable_l4_federation
                ):
                    await self._put_to_l4(entry, options)
                    success_levels.append(CacheLevel.L4)

                # Schedule replication if needed
                if options.replication_factor > 1:
                    await self.pending_replications.put(("put", key, entry))

                self.operation_stats["puts"] += 1
                lookup_time = (time.time() - start_time) * 1000

                return CacheOperationResult(
                    success=True,
                    cache_level=success_levels[0] if success_levels else None,
                    entry=entry,
                    performance=CachePerformanceMetrics(
                        lookup_time_ms=lookup_time, network_hops=0, cache_efficiency=1.0
                    ),
                )

            except Exception as e:
                logger.error(f"Cache put operation failed for {key}: {e}")
                return CacheOperationResult(
                    success=False, error_message=f"Cache put error: {e}"
                )

    async def delete(
        self, key: GlobalCacheKey, options: CacheOptions | None = None
    ) -> CacheOperationResult:
        """
        Delete value from multi-tier cache.
        """
        if options is None:
            options = CacheOptions()

        async with self.operation_semaphore:
            try:
                deleted_levels = []

                # Remove from namespace index
                self._remove_from_namespace_index(key)

                # Delete from L1
                if CacheLevel.L1 in options.cache_levels:
                    local_key = key.to_local_key()
                    if self.l1_cache.evict(local_key):
                        deleted_levels.append(CacheLevel.L1)

                # Delete from L2
                if (
                    CacheLevel.L2 in options.cache_levels
                    and self.config.enable_l2_persistent
                ):
                    if await self._delete_from_l2(key):
                        deleted_levels.append(CacheLevel.L2)

                # Delete from L3 (distributed)
                if (
                    CacheLevel.L3 in options.cache_levels
                    and self.config.enable_l3_distributed
                ):
                    await self.pending_replications.put(("delete", key, None))
                    deleted_levels.append(CacheLevel.L3)

                # Delete from L4 (federation)
                if (
                    CacheLevel.L4 in options.cache_levels
                    and self.config.enable_l4_federation
                ):
                    await self.pending_replications.put(
                        ("delete_federation", key, None)
                    )
                    deleted_levels.append(CacheLevel.L4)

                self.operation_stats["deletes"] += 1

                return CacheOperationResult(
                    success=len(deleted_levels) > 0,
                    cache_level=deleted_levels[0] if deleted_levels else None,
                )

            except Exception as e:
                logger.error(f"Cache delete operation failed for {key}: {e}")
                return CacheOperationResult(
                    success=False, error_message=f"Cache delete error: {e}"
                )

    async def invalidate(
        self, pattern: str, options: CacheOptions | None = None
    ) -> CacheOperationResult:
        """
        Invalidate cache entries matching pattern.
        """
        if options is None:
            options = CacheOptions()

        # This is a simplified implementation
        # In a real system, this would use pattern matching
        # across all cache levels and coordinate with gossip/federation

        try:
            invalidated_count = 0

            # For now, just clear relevant entries from L1
            # TODO: Implement proper pattern matching and distributed invalidation

            self.operation_stats["invalidations"] += 1

            return CacheOperationResult(
                success=True, error_message=f"Invalidated {invalidated_count} entries"
            )

        except Exception as e:
            logger.error(f"Cache invalidation failed for pattern {pattern}: {e}")
            return CacheOperationResult(
                success=False, error_message=f"Cache invalidation error: {e}"
            )

    def _promote_to_l1(self, entry: GlobalCacheEntry) -> None:
        """Promote cache entry to L1 memory cache."""
        local_key = entry.key.to_local_key()

        # Use the smart cache manager to store the entry
        self.l1_cache.put(
            key=local_key,
            value=entry,  # Store the entire GlobalCacheEntry
            computation_cost_ms=entry.metadata.computation_cost_ms,
            dependencies=set(),  # Convert GlobalCacheKey deps to LocalCacheKey later
            ttl_seconds=entry.metadata.ttl_seconds,
        )

    def _put_to_l1(self, entry: GlobalCacheEntry) -> None:
        """Store entry in L1 memory cache."""
        self._promote_to_l1(entry)

    async def _get_from_l2(
        self, key: GlobalCacheKey, options: CacheOptions
    ) -> CacheOperationResult:
        """Get entry from L2 persistent cache."""
        # Simplified L2 implementation - in production this would use a proper persistent store
        key_str = str(key)
        entry = self.l2_cache.get(key_str)

        if entry and not entry.is_expired():
            entry.access()
            return CacheOperationResult(
                success=True,
                cache_level=CacheLevel.L2,
                entry=entry,
                performance=CachePerformanceMetrics(
                    lookup_time_ms=1.0,  # Estimated SSD access time
                    network_hops=0,
                    cache_efficiency=0.9,
                ),
            )

        return CacheOperationResult(success=False, error_message="L2 miss")

    async def _put_to_l2(self, entry: GlobalCacheEntry) -> None:
        """Store entry in L2 persistent cache."""
        key_str = str(entry.key)
        self.l2_cache[key_str] = entry

    async def _delete_from_l2(self, key: GlobalCacheKey) -> bool:
        """Delete entry from L2 persistent cache."""
        key_str = str(key)
        return self.l2_cache.pop(key_str, None) is not None

    async def _get_from_l3(
        self, key: GlobalCacheKey, options: CacheOptions
    ) -> CacheOperationResult:
        """Get entry from L3 distributed cache via gossip."""
        # This would integrate with the gossip protocol to request cache data from peers
        # For now, return miss
        return CacheOperationResult(
            success=False, error_message="L3 distributed cache not yet implemented"
        )

    async def _put_to_l3(self, entry: GlobalCacheEntry, options: CacheOptions) -> None:
        """Store entry in L3 distributed cache."""
        # This would propagate the cache entry via gossip protocol
        pass

    async def _get_from_l4(
        self, key: GlobalCacheKey, options: CacheOptions
    ) -> CacheOperationResult:
        """Get entry from L4 federation cache."""
        # This would query federation partners for cache data
        return CacheOperationResult(
            success=False, error_message="L4 federation cache not yet implemented"
        )

    async def _put_to_l4(self, entry: GlobalCacheEntry, options: CacheOptions) -> None:
        """Store entry in L4 federation cache."""
        # This would replicate to federation partners
        pass

    async def _replication_worker(self) -> None:
        """Background worker for handling cache replication."""
        try:
            while True:
                try:
                    # Process pending replication operations
                    operation, key, data = await asyncio.wait_for(
                        self.pending_replications.get(), timeout=1.0
                    )

                    if operation == "put":
                        await self._handle_replication(key, data)
                    elif operation == "delete":
                        await self._handle_delete_replication(key)
                    elif operation == "delete_federation":
                        await self._handle_federation_delete_replication(key)

                except TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Replication worker error: {e}")
                    # If event loop is gone, break the loop
                    if "no running event loop" in str(
                        e
                    ) or "Event loop is closed" in str(e):
                        break
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            logger.info("Replication worker cancelled")
        except Exception as e:
            logger.error(f"Replication worker fatal error: {e}")
        finally:
            logger.info("Replication worker stopped")

    async def _cleanup_worker(self) -> None:
        """Background worker for cache cleanup and maintenance."""
        try:
            while True:
                try:
                    await asyncio.sleep(60)  # Run every minute

                    # Clean up expired L2 entries
                    expired_keys = []
                    for key_str, entry in self.l2_cache.items():
                        if entry.is_expired():
                            expired_keys.append(key_str)

                    for key_str in expired_keys:
                        self.l2_cache.pop(key_str, None)

                    if expired_keys:
                        logger.debug(
                            f"Cleaned up {len(expired_keys)} expired L2 cache entries"
                        )

                except Exception as e:
                    logger.error(f"Cleanup worker error: {e}")
                    # If event loop is gone, break the loop
                    if "no running event loop" in str(
                        e
                    ) or "Event loop is closed" in str(e):
                        break
        except asyncio.CancelledError:
            logger.info("Cleanup worker cancelled")
        except Exception as e:
            logger.error(f"Cleanup worker fatal error: {e}")
        finally:
            logger.info("Cleanup worker stopped")

    async def _gossip_worker(self) -> None:
        """Background worker for gossip-based cache synchronization."""
        while True:
            try:
                await asyncio.sleep(self.config.gossip_interval_seconds)
                # TODO: Implement gossip synchronization

            except Exception as e:
                logger.error(f"Gossip worker error: {e}")

    async def _handle_replication(
        self, key: GlobalCacheKey, entry: GlobalCacheEntry
    ) -> None:
        """Handle replication of cache entry to other nodes."""
        # TODO: Implement actual replication logic
        logger.debug(f"Replicating cache entry {key}")

    async def _handle_delete_replication(self, key: GlobalCacheKey) -> None:
        """Handle replication of cache deletion."""
        logger.debug(f"Replicating cache deletion {key}")

    async def _handle_federation_delete_replication(self, key: GlobalCacheKey) -> None:
        """Handle federation-level cache deletion."""
        logger.debug(f"Replicating federation cache deletion {key}")

    def get_statistics(self) -> dict[str, Any]:
        """Get comprehensive cache statistics."""
        l1_stats = self.l1_cache.get_statistics()

        return {
            "operation_stats": dict(self.operation_stats),
            "l1_statistics": {
                "hits": l1_stats.hits,
                "misses": l1_stats.misses,
                "evictions": l1_stats.evictions,
                "memory_bytes": l1_stats.memory_bytes,
                "entry_count": l1_stats.entry_count,
                "hit_rate": l1_stats.hit_rate(),
            },
            "l2_statistics": {
                "entry_count": len(self.l2_cache),
                "enabled": self.config.enable_l2_persistent,
            },
            "replication_statistics": {
                "pending_operations": self.pending_replications.qsize(),
                "tracked_replications": len(self.replication_state),
            },
            "performance_metrics": {
                level.value: {
                    "avg_time_ms": sum(times) / len(times) if times else 0.0,
                    "sample_count": len(times),
                }
                for level, times in self.performance_metrics.items()
            },
        }

    async def shutdown(self) -> None:
        """Shutdown cache manager and cleanup resources."""
        # Shutdown background tasks using task manager
        await super().shutdown()

        # Shutdown L1 cache
        await self.l1_cache.shutdown()

        logger.info("Global cache manager shutdown complete")

    def shutdown_sync(self) -> None:
        """Shutdown cache manager and cleanup resources (sync version for compatibility)."""
        # For sync context, cancel tasks directly and clear cache
        try:
            # Cancel tasks directly
            if self._task_manager.tasks:
                for task in self._task_manager.tasks:
                    if not task.done():
                        task.cancel()
                self._task_manager.tasks.clear()
                self._task_manager._shutdown_requested = True
        except Exception as e:
            logger.warning(f"Error during sync task cancellation: {e}")

        # Shutdown L1 cache synchronously
        self.l1_cache.shutdown_sync()

        logger.info("Global cache manager sync shutdown complete")

    def _add_to_namespace_index(self, key: GlobalCacheKey) -> None:
        """Add key to namespace index."""
        self.namespace_index[key.namespace].add(key)

    def _remove_from_namespace_index(self, key: GlobalCacheKey) -> None:
        """Remove key from namespace index."""
        namespace_keys = self.namespace_index.get(key.namespace)
        if namespace_keys:
            namespace_keys.discard(key)
            # Clean up empty namespace sets
            if not namespace_keys:
                del self.namespace_index[key.namespace]

    def get_namespace_keys(
        self, namespace: str, pattern: str | None = None
    ) -> list[GlobalCacheKey]:
        """Get all keys in a namespace, optionally filtered by pattern."""
        namespace_keys = self.namespace_index.get(namespace, set())

        if not pattern or pattern == "*":
            return list(namespace_keys)

        # Simple pattern matching - could be enhanced with regex/glob patterns
        filtered_keys = []
        for key in namespace_keys:
            if pattern in key.identifier:
                filtered_keys.append(key)

        return filtered_keys
