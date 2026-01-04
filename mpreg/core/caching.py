"""
Smart Result Caching & Lifecycle Management System for MPREG

This module provides a comprehensive multi-tier caching system with intelligent
eviction policies, result lifecycle management, and performance monitoring.
All data structures use dataclasses following MPREG's clean design principles.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeVar

from loguru import logger
from pympler import asizeof

from .task_manager import ManagedObject

cache_store_log = logger

T = TypeVar("T")

# Type alias for memory sizes that can be fractional MB
MemoryMB = int | float


class CacheLevel(Enum):
    """Cache tier levels for multi-tier architecture."""

    L1_MEMORY = "l1_memory"
    L2_PERSISTENT = "l2_persistent"
    L3_DISTRIBUTED = "l3_distributed"


class EvictionPolicy(Enum):
    """Cache eviction policy types."""

    LRU = "lru"
    LFU = "lfu"
    COST_BASED = "cost_based"
    DEPENDENCY_AWARE = "dependency_aware"
    TTL = "ttl"
    S4LRU = "s4lru"


@dataclass(frozen=True, slots=True)
class CacheKey:
    """Immutable cache key with content-based hashing."""

    function_name: str
    args_hash: str
    kwargs_hash: str
    schema_version: str = "1.0"

    @classmethod
    def create(
        cls, function_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> CacheKey:
        """Create cache key from function call parameters."""
        args_hash = hashlib.sha256(str(args).encode()).hexdigest()[:16]
        kwargs_hash = hashlib.sha256(str(sorted(kwargs.items())).encode()).hexdigest()[
            :16
        ]
        return cls(
            function_name=function_name, args_hash=args_hash, kwargs_hash=kwargs_hash
        )

    def __str__(self) -> str:
        return f"{self.function_name}:{self.args_hash}:{self.kwargs_hash}"


@dataclass(slots=True)
class CacheEntry:
    """Cache entry with metadata for intelligent management."""

    key: CacheKey
    value: Any
    creation_time: float
    access_count: int = 0
    last_access_time: float = field(default_factory=time.time)
    computation_cost_ms: float = 0.0
    size_bytes: int = 0  # Total size (key + value)
    key_size_bytes: int = 0  # Size of the key alone
    value_size_bytes: int = 0  # Size of the value alone
    dependencies: set[CacheKey] = field(default_factory=set)
    ttl_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.last_access_time == 0:
            self.last_access_time = self.creation_time

    def access(self) -> None:
        """Record cache entry access."""
        self.access_count += 1
        self.last_access_time = time.time()

    def is_expired(self) -> bool:
        """Check if entry has exceeded its TTL."""
        if self.ttl_seconds is None:
            return False
        return (time.time() - self.creation_time) > self.ttl_seconds

    def age_seconds(self) -> float:
        """Get entry age in seconds."""
        return time.time() - self.creation_time

    def frequency_score(self) -> float:
        """Calculate frequency-based score."""
        age = self.age_seconds()
        if age == 0:
            return float("inf")
        return self.access_count / age

    def cost_benefit_score(self) -> float:
        """Calculate cost-benefit score for eviction decisions."""
        if self.size_bytes == 0:
            return float("inf")

        # Higher computation cost = more valuable to cache
        # Higher frequency = more valuable to cache
        # Larger size = less valuable to cache
        benefit = self.computation_cost_ms * self.frequency_score()
        cost = self.size_bytes

        return benefit / cost if cost > 0 else float("inf")


@dataclass(slots=True)
class CacheStatistics:
    """Cache performance statistics."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    memory_bytes: int = 0
    key_memory_bytes: int = 0  # Memory used by keys
    value_memory_bytes: int = 0  # Memory used by values
    entry_count: int = 0
    avg_computation_cost_ms: float = 0.0
    last_reset_time: float = field(default_factory=time.time)

    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def key_to_value_ratio(self) -> float:
        """Calculate ratio of key memory to value memory."""
        if self.value_memory_bytes == 0:
            return float("inf") if self.key_memory_bytes > 0 else 0.0
        return self.key_memory_bytes / self.value_memory_bytes

    def memory_efficiency(self) -> float:
        """Calculate memory efficiency (value memory / total memory)."""
        if self.memory_bytes == 0:
            return 0.0
        return self.value_memory_bytes / self.memory_bytes

    def reset(self) -> None:
        """Reset statistics counters."""
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.key_memory_bytes = 0
        self.value_memory_bytes = 0
        self.last_reset_time = time.time()


@dataclass(slots=True)
class EvictionCandidate:
    """Candidate for cache eviction with scoring."""

    entry: CacheEntry
    score: float
    reason: str

    def __lt__(self, other: EvictionCandidate) -> bool:
        """Compare eviction candidates by score (lower = more likely to evict)."""
        return self.score < other.score


@dataclass(slots=True)
class CacheLimits:
    """Cache capacity limits configuration."""

    max_memory_bytes: MemoryMB | None = None  # Max total memory usage
    max_entries: int | None = None  # Max number of entries
    max_memory_per_segment_bytes: MemoryMB | None = None  # For S4LRU segments
    enforce_both_limits: bool = (
        False  # True = both must be satisfied, False = either limit triggers eviction
    )

    def __post_init__(self) -> None:
        """Validate that at least one limit is specified."""
        if self.max_memory_bytes is None and self.max_entries is None:
            # Set reasonable defaults
            self.max_memory_bytes = 100 * 1024 * 1024  # 100MB
            self.max_entries = 10000


@dataclass(slots=True)
class CacheConfiguration:
    """Configuration for cache behavior."""

    limits: CacheLimits = field(default_factory=CacheLimits)
    default_ttl_seconds: float | None = None
    eviction_policy: EvictionPolicy = EvictionPolicy.COST_BASED
    memory_pressure_threshold: float = 0.8  # Start eviction at 80% capacity
    eviction_batch_size: int = 100
    enable_compression: bool = True
    enable_dependency_tracking: bool = True
    enable_accurate_sizing: bool = True  # Use pympler for accurate object sizing
    # S4LRU configuration
    s4lru_segments: int = 4  # Number of segments for S4LRU (parameterizable)

    # Backward compatibility properties
    @property
    def max_memory_bytes(self) -> MemoryMB:
        """Backward compatibility for max_memory_bytes."""
        return self.limits.max_memory_bytes or (100 * 1024 * 1024)

    @property
    def max_entries(self) -> int:
        """Backward compatibility for max_entries."""
        return self.limits.max_entries or 10000

    def memory_limit_bytes(self) -> int:
        """Get memory limit for triggering eviction."""
        if self.limits.max_memory_bytes is None:
            return 2**64  # Effectively unlimited
        return int(self.limits.max_memory_bytes * self.memory_pressure_threshold)

    def should_evict_by_memory(self, current_memory: int) -> bool:
        """Check if eviction should occur based on memory usage."""
        if self.limits.max_memory_bytes is None:
            return False
        return current_memory >= self.memory_limit_bytes()

    def should_evict_by_count(self, current_count: int) -> bool:
        """Check if eviction should occur based on entry count."""
        if self.limits.max_entries is None:
            return False
        return current_count >= self.limits.max_entries

    def should_evict(self, current_memory: int, current_count: int) -> bool:
        """Check if eviction should occur based on configured limits."""
        memory_exceeded = self.should_evict_by_memory(current_memory)
        count_exceeded = self.should_evict_by_count(current_count)

        if self.limits.enforce_both_limits:
            return memory_exceeded and count_exceeded
        else:
            return memory_exceeded or count_exceeded


@dataclass(slots=True)
class S4LRUSegmentStats:
    """Statistics for an S4LRU cache segment."""

    segment_id: int
    current_size: int
    max_size: int
    utilization: float
    current_memory_bytes: MemoryMB
    max_memory_bytes: MemoryMB

    @property
    def memory_utilization(self) -> float:
        """Calculate memory utilization percentage."""
        if self.max_memory_bytes == 0:
            return 0.0
        return self.current_memory_bytes / self.max_memory_bytes


@dataclass(slots=True)
class S4LRUSegment:
    """A single segment in the S4LRU cache algorithm."""

    level: int
    max_size: int
    max_memory_bytes: MemoryMB = 0  # Memory limit per segment
    entries: deque[CacheKey] = field(default_factory=deque)
    entry_set: set[CacheKey] = field(default_factory=set)  # For O(1) membership testing

    def add_to_head(self, key: CacheKey) -> None:
        """Add entry to the head of this segment."""
        if key not in self.entry_set:
            self.entries.appendleft(key)
            self.entry_set.add(key)

    def remove(self, key: CacheKey) -> bool:
        """Remove entry from this segment."""
        if key in self.entry_set:
            self.entries.remove(key)
            self.entry_set.remove(key)
            return True
        return False

    def evict_tail(self) -> CacheKey | None:
        """Evict entry from tail of this segment."""
        if self.entries:
            key = self.entries.pop()
            self.entry_set.remove(key)
            return key
        return None

    def is_full(self) -> bool:
        """Check if segment is at capacity."""
        return len(self.entries) >= self.max_size

    def size(self) -> int:
        """Get current size of segment."""
        return len(self.entries)

    def contains(self, key: CacheKey) -> bool:
        """Check if segment contains key."""
        return key in self.entry_set


@dataclass(slots=True)
class S4LRUCache:
    """
    Segmented LRU cache implementation with parameterizable segment count.

    This implements the S4LRU algorithm which maintains multiple LRU segments.
    On cache hit, items are promoted to the next higher segment.
    On cache miss, items are inserted at the head of segment 0.
    Each segment maintains size invariants by evicting to lower segments.
    """

    total_capacity: int
    num_segments: int = 4
    segments: list[S4LRUSegment] = field(default_factory=list)
    key_to_segment: dict[CacheKey, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize segments with equal capacity distribution."""
        segment_size = max(1, self.total_capacity // self.num_segments)

        # Initialize segments
        for i in range(self.num_segments):
            self.segments.append(S4LRUSegment(level=i, max_size=segment_size))

        # Adjust last segment to handle remainder
        remainder = self.total_capacity % self.num_segments
        if remainder > 0:
            self.segments[-1].max_size += remainder

    def set_memory_limits(self, total_memory_bytes: MemoryMB) -> None:
        """Set memory limits for segments based on total memory budget."""
        memory_per_segment = total_memory_bytes // self.num_segments
        for segment in self.segments:
            segment.max_memory_bytes = memory_per_segment

        # Adjust last segment for remainder
        remainder = total_memory_bytes % self.num_segments
        if remainder > 0:
            self.segments[-1].max_memory_bytes += remainder

    def access(self, key: CacheKey) -> tuple[bool, list[CacheKey]]:
        """
        Record access to key, promoting it to next higher segment.
        Returns (was_hit, evicted_keys) where:
        - was_hit: True if key was found, False if cache miss
        - evicted_keys: List of keys that were evicted from the cache entirely
        """
        evicted_keys = []

        if key not in self.key_to_segment:
            # Cache miss - insert at head of segment 0
            evicted = self._insert_new(key)
            if evicted:
                evicted_keys.append(evicted)
            return False, evicted_keys

        # Cache hit - promote to next higher segment
        current_segment = self.key_to_segment[key]
        target_segment = min(current_segment + 1, self.num_segments - 1)

        if target_segment != current_segment:
            # Remove from current segment
            self.segments[current_segment].remove(key)

            # Add to target segment
            evicted = self._add_to_segment(key, target_segment)
            if evicted:
                evicted_keys.append(evicted)
        else:
            # Already in highest segment, move to head
            self.segments[current_segment].remove(key)
            self.segments[current_segment].add_to_head(key)

        return True, evicted_keys

    def _insert_new(self, key: CacheKey) -> CacheKey | None:
        """Insert new key into segment 0."""
        return self._add_to_segment(key, 0)

    def _add_to_segment(self, key: CacheKey, segment_level: int) -> CacheKey | None:
        """Add key to specified segment, handling capacity constraints."""
        segment = self.segments[segment_level]

        # If segment is full, evict from tail before adding new item
        while segment.is_full():
            evicted_key = segment.evict_tail()
            if evicted_key:
                if segment_level > 0:
                    # Move evicted item to next lower segment
                    recursively_evicted = self._add_to_segment(
                        evicted_key, segment_level - 1
                    )
                    if recursively_evicted:
                        # If recursive call evicted something, return it
                        return recursively_evicted
                else:
                    # Evicted from segment 0 - remove from cache entirely
                    if evicted_key in self.key_to_segment:
                        del self.key_to_segment[evicted_key]
                    # Return the evicted key so the cache manager can handle it
                    return evicted_key

        # Add new key to head of segment
        segment.add_to_head(key)
        self.key_to_segment[key] = segment_level
        return None

    def remove(self, key: CacheKey) -> bool:
        """Remove key from cache."""
        if key not in self.key_to_segment:
            return False

        segment_level = self.key_to_segment[key]
        self.segments[segment_level].remove(key)
        del self.key_to_segment[key]
        return True

    def contains(self, key: CacheKey) -> bool:
        """Check if cache contains key."""
        return key in self.key_to_segment

    def size(self) -> int:
        """Get total number of entries in cache."""
        return len(self.key_to_segment)

    def get_segment_stats(
        self, cache_entries: dict[CacheKey, Any] | None = None
    ) -> list[S4LRUSegmentStats]:
        """Get statistics for each segment with optional memory calculation."""
        stats = []
        for i, segment in enumerate(self.segments):
            current_memory = 0
            if cache_entries:
                # Calculate actual memory usage for this segment
                for key in segment.entries:
                    if key in cache_entries:
                        current_memory += cache_entries[key].size_bytes

            stats.append(
                S4LRUSegmentStats(
                    segment_id=i,
                    current_size=segment.size(),
                    max_size=segment.max_size,
                    utilization=segment.size() / segment.max_size
                    if segment.max_size > 0
                    else 0.0,
                    current_memory_bytes=current_memory,
                    max_memory_bytes=segment.max_memory_bytes,
                )
            )
        return stats

    def clear(self) -> None:
        """Clear all entries from cache."""
        for segment in self.segments:
            segment.entries.clear()
            segment.entry_set.clear()
        self.key_to_segment.clear()


class EvictionPolicyEngine:
    """Engine for implementing different eviction policies."""

    @staticmethod
    def lru_score(entry: CacheEntry) -> float:
        """LRU: Least Recently Used - older access time = higher eviction score."""
        return entry.last_access_time  # Higher timestamp = lower eviction score

    @staticmethod
    def lfu_score(entry: CacheEntry) -> float:
        """LFU: Least Frequently Used - lower access count = higher eviction score."""
        return entry.access_count  # Higher access count = lower eviction score

    @staticmethod
    def cost_based_score(entry: CacheEntry) -> float:
        """Cost-based: Lower cost-benefit ratio = higher eviction score."""
        return entry.cost_benefit_score()  # Higher cost-benefit = lower eviction score

    @staticmethod
    def ttl_score(entry: CacheEntry) -> float:
        """TTL: Time To Live - closer to expiration = higher eviction score."""
        if entry.ttl_seconds is None:
            return float("inf")  # No TTL = never evict by TTL

        remaining_time = entry.ttl_seconds - entry.age_seconds()
        return remaining_time  # Less remaining time = lower score = higher eviction priority

    @staticmethod
    def dependency_aware_score(
        entry: CacheEntry, dependency_graph: dict[CacheKey, set[CacheKey]]
    ) -> float:
        """Dependency-aware: Entries with fewer dependents = higher eviction score."""
        dependents = dependency_graph.get(entry.key, set())
        return len(dependents)  # More dependents = lower eviction score


class SmartCacheManager[T](ManagedObject):
    """
    Multi-tier smart cache manager with intelligent eviction and lifecycle management.

    Features:
    - Multi-tier caching (L1 memory, L2 persistent, L3 distributed)
    - Multiple eviction policies (LRU, LFU, cost-based, dependency-aware)
    - Automatic lifecycle management with TTL
    - Performance monitoring and statistics
    - Dependency tracking and coherence
    """

    def __init__(self, config: CacheConfiguration) -> None:
        super().__init__(name=f"SmartCacheManager-{id(self)}")
        self.config = config
        self.l1_cache: dict[CacheKey, CacheEntry] = {}
        self.access_order: deque[CacheKey] = deque()  # For LRU tracking
        self.dependency_graph: dict[CacheKey, set[CacheKey]] = defaultdict(set)
        self.reverse_deps: dict[CacheKey, set[CacheKey]] = defaultdict(set)
        self.statistics = CacheStatistics()
        self.eviction_engine = EvictionPolicyEngine()

        # Initialize S4LRU cache if using S4LRU policy
        self.s4lru_cache: S4LRUCache | None = None
        if self.config.eviction_policy == EvictionPolicy.S4LRU:
            self.s4lru_cache = S4LRUCache(
                total_capacity=self.config.max_entries,
                num_segments=self.config.s4lru_segments,
            )
            # Set up memory limits for S4LRU segments if configured
            if self.config.limits.max_memory_bytes:
                self.s4lru_cache.set_memory_limits(self.config.limits.max_memory_bytes)

        # Start background cleanup task
        self._start_cleanup_task()

    def _start_cleanup_task(self) -> None:
        """Start background task for periodic cleanup using task manager."""
        try:
            self.create_task(self._periodic_cleanup(), name="periodic_cleanup")
        except RuntimeError:
            # No event loop running, skip background cleanup
            cache_store_log.debug("No event loop running, skipping cleanup task")

    async def _periodic_cleanup(self) -> None:
        """Periodic cleanup of expired entries."""
        try:
            while True:
                try:
                    await asyncio.sleep(60)  # Cleanup every minute
                    expired_keys = [
                        key
                        for key, entry in self.l1_cache.items()
                        if entry.is_expired()
                    ]

                    for key in expired_keys:
                        self.evict(key, reason="TTL expired")

                    cache_store_log.debug(
                        f"Cleaned up {len(expired_keys)} expired cache entries"
                    )

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    cache_store_log.error(f"Error in cache cleanup: {e}")
                    # If event loop is gone, break the loop
                    if "no running event loop" in str(
                        e
                    ) or "Event loop is closed" in str(e):
                        break
        except asyncio.CancelledError:
            cache_store_log.debug("Cache cleanup task cancelled")
        except Exception as e:
            cache_store_log.error(f"Cache cleanup task fatal error: {e}")
        finally:
            cache_store_log.debug("Cache cleanup task stopped")

    def put(
        self,
        key: CacheKey,
        value: T,
        computation_cost_ms: float = 0.0,
        dependencies: set[CacheKey] | None = None,
        ttl_seconds: float | None = None,
    ) -> None:
        """Store value in cache with metadata."""

        # Use default TTL if not specified
        if ttl_seconds is None:
            ttl_seconds = self.config.default_ttl_seconds

        # Calculate approximate size including both key and value
        key_size = self._estimate_size(key)
        value_size = self._estimate_size(value)
        size_bytes = key_size + value_size

        # Create cache entry
        entry = CacheEntry(
            key=key,
            value=value,
            creation_time=time.time(),
            computation_cost_ms=computation_cost_ms,
            size_bytes=size_bytes,
            key_size_bytes=key_size,
            value_size_bytes=value_size,
            dependencies=dependencies or set(),
            ttl_seconds=ttl_seconds,
        )

        # Update dependency tracking
        if self.config.enable_dependency_tracking and dependencies:
            self._update_dependencies(key, dependencies)

        # Check if we need to evict before adding
        if self._should_evict():
            self._perform_eviction()

        # Add to cache
        self.l1_cache[key] = entry

        # Update access tracking based on eviction policy
        if self.config.eviction_policy == EvictionPolicy.S4LRU and self.s4lru_cache:
            # S4LRU handles its own access tracking
            was_hit, evicted_keys = self.s4lru_cache.access(key)
            # Handle any evictions from S4LRU
            for evicted_key in evicted_keys:
                if evicted_key in self.l1_cache:
                    evicted_entry = self.l1_cache.pop(evicted_key)
                    self.statistics.evictions += 1
                    self.statistics.memory_bytes -= evicted_entry.size_bytes
                    cache_store_log.debug(f"S4LRU evicted {evicted_key}")
        else:
            # Use traditional access order tracking
            self.access_order.append(key)

        # Update statistics
        self.statistics.entry_count = len(self.l1_cache)
        self.statistics.memory_bytes += size_bytes

        cache_store_log.debug(
            f"Cached {key} with cost {computation_cost_ms}ms, size {size_bytes} bytes"
        )

    def get(self, key: CacheKey) -> T | None:
        """Retrieve value from cache."""
        entry = self.l1_cache.get(key)

        if entry is None:
            self.statistics.misses += 1
            return None

        # Check if expired
        if entry.is_expired():
            self.evict(key, reason="TTL expired on access")
            self.statistics.misses += 1
            return None

        # Update access statistics
        entry.access()
        self.statistics.hits += 1

        # Update access tracking based on eviction policy
        if self.config.eviction_policy == EvictionPolicy.S4LRU and self.s4lru_cache:
            # S4LRU handles its own access tracking and promotion
            was_hit, evicted_keys = self.s4lru_cache.access(key)
            # Handle any evictions from S4LRU
            for evicted_key in evicted_keys:
                if evicted_key in self.l1_cache:
                    evicted_entry = self.l1_cache.pop(evicted_key)
                    self.statistics.evictions += 1
                    self.statistics.memory_bytes -= evicted_entry.size_bytes
                    cache_store_log.debug(f"S4LRU evicted {evicted_key}")
        else:
            # Update LRU order for traditional policies
            try:
                self.access_order.remove(key)
            except ValueError:
                pass  # Key might not be in access order
            self.access_order.append(key)

        cache_store_log.debug(f"Cache hit for {key}")
        return entry.value

    def contains(self, key: CacheKey) -> bool:
        """Check if key exists in cache and is not expired."""
        entry = self.l1_cache.get(key)
        if entry is None:
            return False

        if entry.is_expired():
            self.evict(key, reason="TTL expired on contains check")
            return False

        return True

    def evict(self, key: CacheKey, reason: str = "Manual eviction") -> bool:
        """Evict specific key from cache."""
        entry = self.l1_cache.pop(key, None)
        if entry is None:
            return False

        # Remove from access order tracking
        if self.config.eviction_policy == EvictionPolicy.S4LRU and self.s4lru_cache:
            # Remove from S4LRU cache
            self.s4lru_cache.remove(key)
        else:
            # Remove from traditional access order
            with contextlib.suppress(ValueError):
                self.access_order.remove(key)

        # Update statistics
        self.statistics.evictions += 1
        self.statistics.entry_count = len(self.l1_cache)
        self.statistics.memory_bytes -= entry.size_bytes

        # Clean up dependencies
        if self.config.enable_dependency_tracking:
            self._remove_dependencies(key)

        cache_store_log.debug(f"Evicted {key}: {reason}")
        return True

    def invalidate_dependencies(self, key: CacheKey) -> int:
        """Invalidate all entries that depend on the given key."""
        dependents = self.reverse_deps.get(key, set()).copy()

        for dependent_key in dependents:
            self.evict(dependent_key, reason=f"Dependency {key} invalidated")

        return len(dependents)

    def clear(self) -> None:
        """Clear all cache entries."""
        self.l1_cache.clear()
        self.access_order.clear()
        self.dependency_graph.clear()
        self.reverse_deps.clear()
        self.statistics = CacheStatistics()
        cache_store_log.info("Cache cleared")

    def get_statistics(self) -> CacheStatistics:
        """Get cache performance statistics."""
        # Update current memory usage with key/value breakdown
        self.statistics.memory_bytes = sum(
            entry.size_bytes for entry in self.l1_cache.values()
        )
        self.statistics.key_memory_bytes = sum(
            entry.key_size_bytes for entry in self.l1_cache.values()
        )
        self.statistics.value_memory_bytes = sum(
            entry.value_size_bytes for entry in self.l1_cache.values()
        )
        self.statistics.entry_count = len(self.l1_cache)

        # Calculate average computation cost
        if self.l1_cache:
            total_cost = sum(
                entry.computation_cost_ms for entry in self.l1_cache.values()
            )
            self.statistics.avg_computation_cost_ms = total_cost / len(self.l1_cache)

        return self.statistics

    def get_top_entries(self, limit: int = 10) -> list[CacheEntry]:
        """Get top cache entries by cost-benefit score."""
        entries = list(self.l1_cache.values())
        entries.sort(key=lambda e: e.cost_benefit_score(), reverse=True)
        return entries[:limit]

    def get_s4lru_stats(self) -> list[S4LRUSegmentStats] | None:
        """Get S4LRU segment statistics if using S4LRU policy."""
        if self.config.eviction_policy == EvictionPolicy.S4LRU and self.s4lru_cache:
            return self.s4lru_cache.get_segment_stats(self.l1_cache)
        return None

    def _should_evict(self) -> bool:
        """Check if cache should perform eviction based on configured limits."""
        current_memory = sum(entry.size_bytes for entry in self.l1_cache.values())
        current_count = len(self.l1_cache)
        return self.config.should_evict(current_memory, current_count)

    def _perform_eviction(self) -> None:
        """Perform cache eviction based on configured policy."""
        if self.config.eviction_policy == EvictionPolicy.S4LRU and self.s4lru_cache:
            # S4LRU handles eviction internally when new items are added
            # No explicit eviction needed here as it happens during access()
            cache_store_log.debug(
                "S4LRU policy handles eviction automatically during access"
            )
            return

        candidates = self._select_eviction_candidates()

        evicted_count = 0
        target_evictions = min(self.config.eviction_batch_size, len(candidates))

        for candidate in candidates[:target_evictions]:
            if self.evict(candidate.entry.key, candidate.reason):
                evicted_count += 1

        cache_store_log.info(
            f"Evicted {evicted_count} entries using {self.config.eviction_policy.value} policy"
        )

    def _select_eviction_candidates(self) -> list[EvictionCandidate]:
        """Select candidates for eviction based on policy."""
        candidates = []

        for entry in self.l1_cache.values():
            score = self._calculate_eviction_score(entry)
            reason = f"{self.config.eviction_policy.value} policy"

            candidates.append(
                EvictionCandidate(entry=entry, score=score, reason=reason)
            )

        # Sort by eviction score (lower = more likely to evict)
        candidates.sort()
        return candidates

    def _calculate_eviction_score(self, entry: CacheEntry) -> float:
        """Calculate eviction score for entry based on policy."""
        policy = self.config.eviction_policy

        if policy == EvictionPolicy.LRU:
            return self.eviction_engine.lru_score(entry)
        elif policy == EvictionPolicy.LFU:
            return self.eviction_engine.lfu_score(entry)
        elif policy == EvictionPolicy.COST_BASED:
            return self.eviction_engine.cost_based_score(entry)
        elif policy == EvictionPolicy.TTL:
            return self.eviction_engine.ttl_score(entry)
        elif policy == EvictionPolicy.DEPENDENCY_AWARE:
            return self.eviction_engine.dependency_aware_score(
                entry, self.dependency_graph
            )
        else:
            # Fallback to LRU
            return self.eviction_engine.lru_score(entry)

    def _update_dependencies(self, key: CacheKey, dependencies: set[CacheKey]) -> None:
        """Update dependency tracking for cache coherence."""
        # Remove old dependencies
        old_deps = self.dependency_graph.get(key, set())
        for dep in old_deps:
            self.reverse_deps[dep].discard(key)

        # Add new dependencies
        self.dependency_graph[key] = dependencies.copy()
        for dep in dependencies:
            self.reverse_deps[dep].add(key)

    def _remove_dependencies(self, key: CacheKey) -> None:
        """Remove dependency tracking for evicted key."""
        # Remove as dependent
        dependencies = self.dependency_graph.pop(key, set())
        for dep in dependencies:
            self.reverse_deps[dep].discard(key)

        # Remove as dependency
        dependents = self.reverse_deps.pop(key, set())
        for dependent in dependents:
            self.dependency_graph[dependent].discard(key)

    def _estimate_size(self, value: Any) -> int:
        """Estimate memory size of cached value using pympler for accuracy."""
        if self.config.enable_accurate_sizing:
            try:
                # Use pympler for accurate memory measurement
                return asizeof.asizeof(value)
            except Exception as e:
                cache_store_log.warning(
                    f"Failed to calculate accurate size with pympler: {e}"
                )
                # Fall back to simple estimation
                pass

        # Simple estimation fallback
        try:
            return len(str(value).encode("utf-8"))
        except Exception:
            return 1024  # Default estimate

    async def shutdown(self) -> None:
        """Shutdown cache manager and cleanup resources."""
        # Shutdown background tasks using task manager
        await super().shutdown()

        self.clear()
        cache_store_log.info("Cache manager shutdown complete")

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
            cache_store_log.warning(f"Error during sync task cancellation: {e}")

        self.clear()
        cache_store_log.info("Cache manager sync shutdown complete")


# Factory functions for common configurations
def create_default_cache_manager() -> SmartCacheManager[Any]:
    """Create cache manager with default configuration."""
    config = CacheConfiguration()
    return SmartCacheManager(config)


def create_memory_optimized_cache_manager(
    max_memory_mb: MemoryMB = 50,
) -> SmartCacheManager[Any]:
    """Create memory-optimized cache manager."""
    limits = CacheLimits(max_memory_bytes=max_memory_mb * 1024 * 1024)
    config = CacheConfiguration(
        limits=limits,
        eviction_policy=EvictionPolicy.LRU,
        memory_pressure_threshold=0.9,
        enable_compression=True,
    )
    return SmartCacheManager(config)


def create_performance_cache_manager() -> SmartCacheManager[Any]:
    """Create performance-optimized cache manager."""
    limits = CacheLimits(
        max_memory_bytes=200 * 1024 * 1024,  # 200MB
        max_entries=50000,
    )
    config = CacheConfiguration(
        limits=limits,
        eviction_policy=EvictionPolicy.COST_BASED,
        memory_pressure_threshold=0.8,
        enable_dependency_tracking=True,
    )
    return SmartCacheManager(config)


def create_s4lru_cache_manager(
    max_entries: int = 10000, segments: int = 4
) -> SmartCacheManager[Any]:
    """Create S4LRU (Segmented LRU) cache manager."""
    limits = CacheLimits(max_entries=max_entries)
    config = CacheConfiguration(
        limits=limits,
        eviction_policy=EvictionPolicy.S4LRU,
        s4lru_segments=segments,
        memory_pressure_threshold=0.9,  # Less aggressive since S4LRU handles retention well
        enable_dependency_tracking=True,
    )
    return SmartCacheManager(config)
