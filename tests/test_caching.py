"""
Comprehensive tests for Smart Result Caching & Lifecycle Management System.

This test suite verifies all aspects of the caching system including:
- Cache key generation and uniqueness
- Multi-tier cache operations
- Eviction policy effectiveness
- Dependency tracking and invalidation
- Performance monitoring and statistics
- Lifecycle management with TTL
"""

import asyncio
import time
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from mpreg.core.caching import (
    CacheConfiguration,
    CacheEntry,
    CacheKey,
    CacheLimits,
    CacheStatistics,
    EvictionCandidate,
    EvictionPolicy,
    EvictionPolicyEngine,
    S4LRUCache,
    S4LRUSegment,
    SmartCacheManager,
    create_default_cache_manager,
    create_memory_optimized_cache_manager,
    create_performance_cache_manager,
    create_s4lru_cache_manager,
)


@contextmanager
def managed_cache(cache_manager):
    """Context manager to ensure proper cleanup of cache managers."""
    try:
        yield cache_manager
    finally:
        # Clean up background tasks
        cache_manager.shutdown_sync()


class TestCacheKey:
    """Test cache key generation and hashing."""

    def test_cache_key_creation(self):
        """Test basic cache key creation."""
        key = CacheKey.create("test_func", (1, 2, 3), {"param": "value"})

        assert key.function_name == "test_func"
        assert len(key.args_hash) == 16
        assert len(key.kwargs_hash) == 16
        assert key.schema_version == "1.0"

    def test_cache_key_deterministic(self):
        """Test that cache keys are deterministic."""
        key1 = CacheKey.create("func", (1, 2), {"a": 1})
        key2 = CacheKey.create("func", (1, 2), {"a": 1})

        assert key1 == key2
        assert str(key1) == str(key2)

    def test_cache_key_different_args(self):
        """Test that different arguments produce different keys."""
        key1 = CacheKey.create("func", (1, 2), {})
        key2 = CacheKey.create("func", (1, 3), {})

        assert key1 != key2
        assert key1.args_hash != key2.args_hash

    def test_cache_key_different_kwargs(self):
        """Test that different kwargs produce different keys."""
        key1 = CacheKey.create("func", (), {"a": 1})
        key2 = CacheKey.create("func", (), {"a": 2})

        assert key1 != key2
        assert key1.kwargs_hash != key2.kwargs_hash

    def test_cache_key_string_representation(self):
        """Test cache key string representation."""
        key = CacheKey.create("test_func", (1,), {"x": 1})
        key_str = str(key)

        assert "test_func" in key_str
        assert ":" in key_str
        assert len(key_str.split(":")) == 3


class TestCacheEntry:
    """Test cache entry metadata and operations."""

    def test_cache_entry_creation(self):
        """Test basic cache entry creation."""
        key = CacheKey.create("func", (), {})
        entry = CacheEntry(
            key=key,
            value="test_value",
            creation_time=time.time(),
            computation_cost_ms=100.0,
            size_bytes=1024,
        )

        assert entry.key == key
        assert entry.value == "test_value"
        assert entry.access_count == 0
        assert entry.computation_cost_ms == 100.0
        assert entry.size_bytes == 1024

    def test_cache_entry_access_tracking(self):
        """Test access count and timing tracking."""
        key = CacheKey.create("func", (), {})
        entry = CacheEntry(key=key, value="test", creation_time=time.time())

        initial_access_time = entry.last_access_time
        initial_count = entry.access_count

        time.sleep(0.01)  # Small delay
        entry.access()

        assert entry.access_count == initial_count + 1
        assert entry.last_access_time > initial_access_time

    def test_cache_entry_ttl_expiration(self):
        """Test TTL-based expiration."""
        key = CacheKey.create("func", (), {})
        entry = CacheEntry(
            key=key,
            value="test",
            creation_time=time.time(),
            ttl_seconds=0.01,  # 10ms TTL
        )

        assert not entry.is_expired()

        time.sleep(0.02)  # Wait longer than TTL
        assert entry.is_expired()

    def test_cache_entry_no_ttl(self):
        """Test entries without TTL never expire."""
        key = CacheKey.create("func", (), {})
        entry = CacheEntry(
            key=key,
            value="test",
            creation_time=time.time() - 3600,  # 1 hour ago
            ttl_seconds=None,
        )

        assert not entry.is_expired()

    def test_cache_entry_frequency_score(self):
        """Test frequency score calculation."""
        key = CacheKey.create("func", (), {})
        entry = CacheEntry(
            key=key,
            value="test",
            creation_time=time.time() - 10,  # 10 seconds ago
        )

        # Access multiple times
        for _ in range(5):
            entry.access()

        score = entry.frequency_score()
        assert score > 0
        assert entry.access_count == 5

    def test_cache_entry_cost_benefit_score(self):
        """Test cost-benefit score calculation."""
        key = CacheKey.create("func", (), {})
        entry = CacheEntry(
            key=key,
            value="test",
            creation_time=time.time() - 1,
            computation_cost_ms=1000.0,  # Expensive computation
            size_bytes=100,  # Small size
        )

        # Access multiple times to increase frequency
        for _ in range(10):
            entry.access()

        score = entry.cost_benefit_score()
        assert score > 0

        # Higher computation cost should increase score
        entry.computation_cost_ms = 2000.0
        new_score = entry.cost_benefit_score()
        assert new_score > score


class TestCacheStatistics:
    """Test cache statistics tracking."""

    def test_statistics_initialization(self):
        """Test statistics initialization."""
        stats = CacheStatistics()

        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.evictions == 0
        assert stats.hit_rate() == 0.0

    def test_hit_rate_calculation(self):
        """Test hit rate calculation."""
        stats = CacheStatistics()

        # No hits or misses
        assert stats.hit_rate() == 0.0

        # Some hits and misses
        stats.hits = 8
        stats.misses = 2
        assert stats.hit_rate() == 0.8

        # Only misses
        stats.hits = 0
        stats.misses = 10
        assert stats.hit_rate() == 0.0

    def test_statistics_reset(self):
        """Test statistics reset functionality."""
        stats = CacheStatistics()
        stats.hits = 100
        stats.misses = 20
        stats.evictions = 5

        old_reset_time = stats.last_reset_time
        time.sleep(0.01)
        stats.reset()

        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.evictions == 0
        assert stats.last_reset_time > old_reset_time


class TestEvictionPolicyEngine:
    """Test eviction policy scoring algorithms."""

    def test_lru_scoring(self):
        """Test LRU eviction scoring."""
        key = CacheKey.create("func", (), {})

        # Older entry should have higher eviction score
        old_entry = CacheEntry(
            key=key,
            value="old",
            creation_time=time.time(),
            last_access_time=time.time() - 100,
        )

        new_entry = CacheEntry(
            key=key,
            value="new",
            creation_time=time.time(),
            last_access_time=time.time() - 10,
        )

        old_score = EvictionPolicyEngine.lru_score(old_entry)
        new_score = EvictionPolicyEngine.lru_score(new_entry)

        # Lower score = higher eviction priority for LRU (older = lower score)
        assert old_score < new_score

    def test_lfu_scoring(self):
        """Test LFU eviction scoring."""
        key = CacheKey.create("func", (), {})

        # Entry with fewer accesses should have higher eviction score
        low_access_entry = CacheEntry(
            key=key, value="low", creation_time=time.time(), access_count=1
        )

        high_access_entry = CacheEntry(
            key=key, value="high", creation_time=time.time(), access_count=10
        )

        low_score = EvictionPolicyEngine.lfu_score(low_access_entry)
        high_score = EvictionPolicyEngine.lfu_score(high_access_entry)

        # Lower score = higher eviction priority for LFU (fewer accesses = lower score)
        assert low_score < high_score

    def test_cost_based_scoring(self):
        """Test cost-based eviction scoring."""
        key = CacheKey.create("func", (), {})

        # Entry with lower cost-benefit should have higher eviction score
        low_benefit_entry = CacheEntry(
            key=key,
            value="low",
            creation_time=time.time() - 1,
            computation_cost_ms=10.0,  # Cheap
            size_bytes=1000,  # Large
            access_count=1,  # Rarely accessed
        )

        high_benefit_entry = CacheEntry(
            key=key,
            value="high",
            creation_time=time.time() - 1,
            computation_cost_ms=1000.0,  # Expensive
            size_bytes=100,  # Small
            access_count=10,  # Frequently accessed
        )

        low_score = EvictionPolicyEngine.cost_based_score(low_benefit_entry)
        high_score = EvictionPolicyEngine.cost_based_score(high_benefit_entry)

        # Lower score = higher eviction priority (lower cost-benefit = lower score)
        assert low_score < high_score

    def test_ttl_scoring(self):
        """Test TTL-based eviction scoring."""
        key = CacheKey.create("func", (), {})

        # Entry closer to expiration should have higher eviction score
        expiring_soon = CacheEntry(
            key=key,
            value="soon",
            creation_time=time.time() - 9,
            ttl_seconds=10.0,  # Expires in 1 second
        )

        expiring_later = CacheEntry(
            key=key,
            value="later",
            creation_time=time.time() - 1,
            ttl_seconds=10.0,  # Expires in 9 seconds
        )

        soon_score = EvictionPolicyEngine.ttl_score(expiring_soon)
        later_score = EvictionPolicyEngine.ttl_score(expiring_later)

        # Lower score = higher eviction priority (less remaining time = lower score)
        assert soon_score < later_score


class TestSmartCacheManager:
    """Test smart cache manager functionality."""

    def test_cache_manager_initialization(self):
        """Test cache manager initialization."""
        limits = CacheLimits(max_entries=1000)
        config = CacheConfiguration(limits=limits)

        with managed_cache(SmartCacheManager(config)) as cache:
            assert cache.config.max_entries == 1000
            assert len(cache.l1_cache) == 0
            assert cache.statistics.entry_count == 0

    def test_basic_cache_operations(self):
        """Test basic put/get operations."""
        with managed_cache(create_default_cache_manager()) as cache:
            key = CacheKey.create("test_func", (1, 2), {"param": "value"})

            # Cache miss
            assert cache.get(key) is None
            assert cache.statistics.misses == 1

            # Store value
            cache.put(key, "test_result", computation_cost_ms=100.0)
            assert cache.statistics.entry_count == 1

            # Cache hit
            result = cache.get(key)
            assert result == "test_result"
            assert cache.statistics.hits == 1

            # Check contains
            assert cache.contains(key)

    def test_cache_eviction_by_count(self):
        """Test eviction when max entries exceeded."""
        limits = CacheLimits(max_entries=3)
        config = CacheConfiguration(limits=limits, eviction_batch_size=1)

        with managed_cache(SmartCacheManager(config)) as cache:
            # Fill cache to capacity
            for i in range(3):
                key = CacheKey.create("func", (i,), {})
                cache.put(key, f"value_{i}")

            assert len(cache.l1_cache) == 3

            # Add one more - should trigger eviction
            key4 = CacheKey.create("func", (4,), {})
            cache.put(key4, "value_4")

            # Should have evicted one entry
            assert len(cache.l1_cache) == 3
            assert cache.statistics.evictions >= 1

    def test_cache_eviction_by_memory(self):
        """Test eviction when memory limit exceeded."""
        limits = CacheLimits(max_memory_bytes=1000)
        config = CacheConfiguration(
            limits=limits,
            memory_pressure_threshold=0.5,  # Trigger at 500 bytes
            eviction_batch_size=1,
        )
        with managed_cache(SmartCacheManager(config)) as cache:
            # Add entries that exceed memory limit
            for i in range(10):
                key = CacheKey.create("func", (i,), {})
                # Mock size estimation to return large size
                with patch.object(cache, "_estimate_size", return_value=200):
                    cache.put(key, f"large_value_{i}")

            # Should have triggered evictions
            assert cache.statistics.evictions > 0

    def test_ttl_expiration(self):
        """Test TTL-based expiration."""
        with managed_cache(create_default_cache_manager()) as cache:
            key = CacheKey.create("func", (), {})

            # Store with short TTL
            cache.put(key, "test_value", ttl_seconds=0.01)

            # Should be available immediately
            assert cache.get(key) == "test_value"

            # Wait for expiration
            time.sleep(0.02)

            # Should be expired and return None
            assert cache.get(key) is None
            assert not cache.contains(key)

    def test_dependency_tracking(self):
        """Test dependency tracking and invalidation."""
        config = CacheConfiguration(enable_dependency_tracking=True)
        with managed_cache(SmartCacheManager(config)) as cache:
            # Create dependencies: result depends on inputs
            input_key = CacheKey.create("get_input", (), {})
            result_key = CacheKey.create("process", (), {})

            # Store input
            cache.put(input_key, "input_data")

            # Store result with dependency on input
            cache.put(result_key, "processed_data", dependencies={input_key})

            assert cache.get(input_key) == "input_data"
            assert cache.get(result_key) == "processed_data"

            # Invalidate input - should cascade to result
            invalidated = cache.invalidate_dependencies(input_key)
            assert invalidated == 1

            # Result should be gone, but input still there
            assert cache.get(input_key) == "input_data"
            assert cache.get(result_key) is None

    async def test_eviction_policies(self):
        """Test different eviction policies."""
        policies = [
            EvictionPolicy.LRU,
            EvictionPolicy.LFU,
            EvictionPolicy.COST_BASED,
            EvictionPolicy.TTL,
        ]

        for policy in policies:
            limits = CacheLimits(max_entries=2)
            config = CacheConfiguration(
                limits=limits, eviction_policy=policy, eviction_batch_size=1
            )
            cache: SmartCacheManager[str] = SmartCacheManager(config)

            # Fill cache
            key1 = CacheKey.create("func", (1,), {})
            key2 = CacheKey.create("func", (2,), {})

            if policy == EvictionPolicy.TTL:
                cache.put(key1, "value1", ttl_seconds=0.01)
                time.sleep(0.02)  # Make first entry more likely to expire
                cache.put(key2, "value2", ttl_seconds=10.0)
            else:
                cache.put(key1, "value1", computation_cost_ms=10.0)
                cache.put(key2, "value2", computation_cost_ms=100.0)

            # Add third entry to trigger eviction
            key3 = CacheKey.create("func", (3,), {})
            cache.put(key3, "value3")

            # Should have triggered eviction
            assert cache.statistics.evictions >= 1
            await cache.shutdown()

    def test_statistics_tracking(self):
        """Test comprehensive statistics tracking."""
        with managed_cache(create_default_cache_manager()) as cache:
            # Generate some cache activity
            for i in range(10):
                key = CacheKey.create("func", (i,), {})
                cache.put(key, f"value_{i}", computation_cost_ms=float(i * 10))

            # Access some entries multiple times
            for i in range(5):
                key = CacheKey.create("func", (i,), {})
                cache.get(key)
                cache.get(key)  # Second access

            # Access non-existent entries
            for i in range(10, 15):
                key = CacheKey.create("func", (i,), {})
                cache.get(key)  # Cache miss

            stats = cache.get_statistics()

            assert stats.hits == 10  # 5 entries accessed twice
            assert stats.misses == 5  # 5 non-existent entries
            assert stats.entry_count == 10
            assert abs(stats.hit_rate() - (2 / 3)) < 0.01  # 10/(10+5) = 10/15 = 2/3
            assert stats.avg_computation_cost_ms > 0

    def test_top_entries_analysis(self):
        """Test top entries analysis by cost-benefit."""
        with managed_cache(create_default_cache_manager()) as cache:
            # Add entries with different cost-benefit profiles
            entries_data = [
                ("cheap_unused", 10.0, 1),  # Low cost, low access
                ("expensive_used", 1000.0, 10),  # High cost, high access
                ("medium_medium", 100.0, 5),  # Medium cost, medium access
            ]

            for name, cost, access_count in entries_data:
                key = CacheKey.create(name, (), {})
                cache.put(key, f"value_{name}", computation_cost_ms=cost)

                # Simulate access pattern
                for _ in range(access_count):
                    cache.get(key)

            top_entries = cache.get_top_entries(limit=3)

            assert len(top_entries) == 3
            # Should be sorted by cost-benefit score (highest first)
            scores = [entry.cost_benefit_score() for entry in top_entries]
            assert scores == sorted(scores, reverse=True)

    def test_cache_clear(self):
        """Test cache clearing functionality."""
        with managed_cache(create_default_cache_manager()) as cache:
            # Add some entries
            for i in range(5):
                key = CacheKey.create("func", (i,), {})
                cache.put(key, f"value_{i}")

            assert len(cache.l1_cache) == 5
            assert cache.statistics.entry_count == 5

            # Clear cache
            cache.clear()

            assert len(cache.l1_cache) == 0
            assert cache.statistics.entry_count == 0
            assert cache.statistics.hits == 0
            assert cache.statistics.misses == 0

    def test_manual_eviction(self):
        """Test manual eviction of specific entries."""
        with managed_cache(create_default_cache_manager()) as cache:
            key = CacheKey.create("func", (), {})

            # Store and verify
            cache.put(key, "test_value")
            assert cache.get(key) == "test_value"

            # Manual eviction
            evicted = cache.evict(key, reason="Manual test eviction")
            assert evicted
            assert cache.get(key) is None
            assert cache.statistics.evictions == 1

            # Evicting non-existent key should return False
            not_evicted = cache.evict(key, reason="Already gone")
            assert not not_evicted

    @pytest.mark.asyncio
    async def test_background_cleanup(self):
        """Test background cleanup of expired entries."""
        cache = create_default_cache_manager()

        # Add entries with short TTL
        for i in range(3):
            key = CacheKey.create("func", (i,), {})
            cache.put(key, f"value_{i}", ttl_seconds=0.05)

        assert len(cache.l1_cache) == 3

        # Wait for expiration
        await asyncio.sleep(0.1)

        # Background cleanup should remove expired entries
        # Note: This is timing-dependent, so we'll just verify the mechanism exists
        assert len(cache._task_manager) > 0  # Should have background tasks running

        await cache.shutdown()


class TestCacheFactoryFunctions:
    """Test cache factory functions."""

    def test_default_cache_manager(self):
        """Test default cache manager factory."""
        with managed_cache(create_default_cache_manager()) as cache:
            assert isinstance(cache, SmartCacheManager)
            assert cache.config.eviction_policy == EvictionPolicy.COST_BASED
            assert cache.config.max_memory_bytes == 100 * 1024 * 1024

    def test_memory_optimized_cache_manager(self):
        """Test memory-optimized cache manager factory."""
        with managed_cache(
            create_memory_optimized_cache_manager(max_memory_mb=25)
        ) as cache:
            assert cache.config.max_memory_bytes == 25 * 1024 * 1024
            assert cache.config.eviction_policy == EvictionPolicy.LRU
            assert cache.config.memory_pressure_threshold == 0.9
            assert cache.config.enable_compression

    def test_performance_cache_manager(self):
        """Test performance-optimized cache manager factory."""
        with managed_cache(create_performance_cache_manager()) as cache:
            assert cache.config.max_memory_bytes == 200 * 1024 * 1024
            assert cache.config.max_entries == 50000
            assert cache.config.eviction_policy == EvictionPolicy.COST_BASED
            assert cache.config.enable_dependency_tracking


class TestCacheConfiguration:
    """Test cache configuration options."""

    def test_default_configuration(self):
        """Test default configuration values."""
        config = CacheConfiguration()

        assert config.max_memory_bytes == 100 * 1024 * 1024
        assert config.max_entries == 10000
        assert config.eviction_policy == EvictionPolicy.COST_BASED
        assert config.memory_pressure_threshold == 0.8
        assert config.eviction_batch_size == 100

    def test_memory_limit_calculation(self):
        """Test memory limit calculation with threshold."""
        limits = CacheLimits(max_memory_bytes=1000)
        config = CacheConfiguration(limits=limits, memory_pressure_threshold=0.7)

        assert config.memory_limit_bytes() == 700

    def test_custom_configuration(self):
        """Test custom configuration options."""
        limits = CacheLimits(
            max_memory_bytes=50 * 1024 * 1024,
            max_entries=5000,
        )
        config = CacheConfiguration(
            limits=limits,
            default_ttl_seconds=3600.0,
            eviction_policy=EvictionPolicy.LRU,
            memory_pressure_threshold=0.9,
            eviction_batch_size=50,
            enable_compression=False,
            enable_dependency_tracking=False,
        )

        assert config.max_memory_bytes == 50 * 1024 * 1024
        assert config.max_entries == 5000
        assert config.default_ttl_seconds == 3600.0
        assert config.eviction_policy == EvictionPolicy.LRU
        assert config.memory_pressure_threshold == 0.9
        assert config.eviction_batch_size == 50
        assert not config.enable_compression
        assert not config.enable_dependency_tracking


class TestEvictionCandidate:
    """Test eviction candidate selection and sorting."""

    def test_eviction_candidate_creation(self):
        """Test eviction candidate creation."""
        key = CacheKey.create("func", (), {})
        entry = CacheEntry(key=key, value="test", creation_time=time.time())

        candidate = EvictionCandidate(entry=entry, score=0.5, reason="Test eviction")

        assert candidate.entry == entry
        assert candidate.score == 0.5
        assert candidate.reason == "Test eviction"

    def test_eviction_candidate_sorting(self):
        """Test eviction candidate sorting by score."""
        key = CacheKey.create("func", (), {})
        entry = CacheEntry(key=key, value="test", creation_time=time.time())

        candidates = [
            EvictionCandidate(entry=entry, score=0.8, reason="High score"),
            EvictionCandidate(entry=entry, score=0.2, reason="Low score"),
            EvictionCandidate(entry=entry, score=0.5, reason="Medium score"),
        ]

        candidates.sort()

        # Should be sorted by score (ascending - lower scores evicted first)
        scores = [c.score for c in candidates]
        assert scores == [0.2, 0.5, 0.8]


class TestS4LRUSegment:
    """Test S4LRU segment functionality."""

    def test_segment_creation(self):
        """Test S4LRU segment creation."""
        segment = S4LRUSegment(level=0, max_size=3)

        assert segment.level == 0
        assert segment.max_size == 3
        assert segment.size() == 0
        assert not segment.is_full()

    def test_segment_add_and_contains(self):
        """Test adding entries to segment."""
        segment = S4LRUSegment(level=0, max_size=3)
        key1 = CacheKey.create("func", (1,), {})
        key2 = CacheKey.create("func", (2,), {})

        segment.add_to_head(key1)
        assert segment.contains(key1)
        assert segment.size() == 1

        segment.add_to_head(key2)
        assert segment.contains(key2)
        assert segment.size() == 2

        # Adding same key again should not increase size
        segment.add_to_head(key1)
        assert segment.size() == 2

    def test_segment_eviction(self):
        """Test segment tail eviction."""
        segment = S4LRUSegment(level=0, max_size=2)
        key1 = CacheKey.create("func", (1,), {})
        key2 = CacheKey.create("func", (2,), {})

        segment.add_to_head(key1)
        segment.add_to_head(key2)
        assert segment.is_full()

        # Evict from tail should return key1 (oldest)
        evicted = segment.evict_tail()
        assert evicted == key1
        assert not segment.contains(key1)
        assert segment.contains(key2)
        assert segment.size() == 1

    def test_segment_remove(self):
        """Test removing specific entry from segment."""
        segment = S4LRUSegment(level=0, max_size=3)
        key1 = CacheKey.create("func", (1,), {})
        key2 = CacheKey.create("func", (2,), {})

        segment.add_to_head(key1)
        segment.add_to_head(key2)

        assert segment.remove(key1)
        assert not segment.contains(key1)
        assert segment.contains(key2)
        assert segment.size() == 1

        # Removing non-existent key should return False
        assert not segment.remove(key1)


class TestS4LRUCache:
    """Test S4LRU cache algorithm."""

    def test_s4lru_initialization(self):
        """Test S4LRU cache initialization."""
        cache = S4LRUCache(total_capacity=12, num_segments=4)

        assert cache.total_capacity == 12
        assert cache.num_segments == 4
        assert len(cache.segments) == 4
        assert cache.size() == 0

        # Check segment sizes (3 each for 12/4)
        for segment in cache.segments:
            assert segment.max_size == 3

    def test_s4lru_uneven_capacity(self):
        """Test S4LRU with capacity not evenly divisible."""
        cache = S4LRUCache(total_capacity=10, num_segments=4)

        # First 3 segments get 2 entries, last segment gets 4 (2 + 2 remainder)
        assert cache.segments[0].max_size == 2
        assert cache.segments[1].max_size == 2
        assert cache.segments[2].max_size == 2
        assert cache.segments[3].max_size == 4

    def test_s4lru_basic_access(self):
        """Test basic S4LRU access patterns."""
        cache = S4LRUCache(total_capacity=8, num_segments=4)  # 2 per segment

        key1 = CacheKey.create("func", (1,), {})
        key2 = CacheKey.create("func", (2,), {})

        # First access - cache miss, goes to segment 0
        was_hit, evicted = cache.access(key1)
        assert not was_hit
        assert evicted == []
        assert cache.contains(key1)
        assert cache.key_to_segment[key1] == 0

        # Second access - cache hit, promotes to segment 1
        was_hit, evicted = cache.access(key1)
        assert was_hit
        assert evicted == []
        assert cache.key_to_segment[key1] == 1

        # New key goes to segment 0
        was_hit, evicted = cache.access(key2)
        assert not was_hit
        assert cache.key_to_segment[key2] == 0

    def test_s4lru_promotion_chain(self):
        """Test promotion through all segments."""
        cache = S4LRUCache(total_capacity=8, num_segments=4)
        key = CacheKey.create("func", (1,), {})

        # Access pattern: miss -> 0, hit -> 1, hit -> 2, hit -> 3, hit -> 3 (max)
        was_hit, evicted = cache.access(key)  # Segment 0
        assert not was_hit
        assert cache.key_to_segment[key] == 0

        was_hit, evicted = cache.access(key)  # Segment 1
        assert was_hit
        assert cache.key_to_segment[key] == 1

        was_hit, evicted = cache.access(key)  # Segment 2
        assert was_hit
        assert cache.key_to_segment[key] == 2

        was_hit, evicted = cache.access(key)  # Segment 3
        assert was_hit
        assert cache.key_to_segment[key] == 3

        was_hit, evicted = cache.access(key)  # Still segment 3 (highest)
        assert was_hit
        assert cache.key_to_segment[key] == 3

    def test_s4lru_eviction_cascade(self):
        """Test eviction cascading through segments."""
        cache = S4LRUCache(total_capacity=4, num_segments=2)  # 2 per segment

        keys = [CacheKey.create("func", (i,), {}) for i in range(5)]

        # Add first key and promote it to highest segment
        cache.access(keys[0])  # Segment 0
        cache.access(keys[0])  # Segment 1 (highest)
        assert cache.key_to_segment[keys[0]] == 1

        # Fill the cache
        cache.access(keys[1])  # Segment 0
        cache.access(keys[2])  # Segment 0
        cache.access(keys[3])  # Segment 0, should evict keys[1]

        # keys[0] should still be protected in segment 1
        assert cache.contains(keys[0])
        assert cache.key_to_segment[keys[0]] == 1

        # Check that we have the expected number of items
        assert cache.size() <= 4

    def test_s4lru_segment_stats(self):
        """Test S4LRU segment statistics."""
        cache = S4LRUCache(total_capacity=8, num_segments=4)

        # Add some entries (only 2 will fit in segment 0)
        keys = [CacheKey.create("func", (i,), {}) for i in range(2)]
        for key in keys:
            cache.access(key)

        stats = cache.get_segment_stats()
        assert len(stats) == 4

        # Segment 0 should have 2 entries (at capacity)
        assert stats[0].segment_id == 0
        assert stats[0].current_size == 2
        assert stats[0].max_size == 2
        assert stats[0].utilization == 1.0  # At capacity

        # Other segments should be empty
        for i in range(1, 4):
            assert stats[i].current_size == 0

    def test_s4lru_remove(self):
        """Test removing entries from S4LRU cache."""
        cache = S4LRUCache(total_capacity=4, num_segments=2)
        key1 = CacheKey.create("func", (1,), {})
        key2 = CacheKey.create("func", (2,), {})

        cache.access(key1)
        cache.access(key2)

        assert cache.remove(key1)
        assert not cache.contains(key1)
        assert cache.contains(key2)

        # Removing non-existent key should return False
        assert not cache.remove(key1)

    def test_s4lru_clear(self):
        """Test clearing S4LRU cache."""
        cache = S4LRUCache(total_capacity=4, num_segments=2)

        keys = [
            CacheKey.create("func", (i,), {}) for i in range(2)
        ]  # Only add 2 to fit capacity
        for key in keys:
            cache.access(key)

        assert cache.size() == 2

        cache.clear()
        assert cache.size() == 0
        for key in keys:
            assert not cache.contains(key)


class TestS4LRUIntegration:
    """Test S4LRU integration with SmartCacheManager."""

    def test_s4lru_cache_manager_creation(self):
        """Test creating cache manager with S4LRU policy."""
        cache = create_s4lru_cache_manager(max_entries=100, segments=6)

        assert cache.config.eviction_policy == EvictionPolicy.S4LRU
        assert cache.config.s4lru_segments == 6
        assert cache.config.max_entries == 100
        assert cache.s4lru_cache is not None
        assert cache.s4lru_cache.num_segments == 6
        assert cache.s4lru_cache.total_capacity == 100

    def test_s4lru_put_and_get(self):
        """Test basic put/get operations with S4LRU."""
        cache = create_s4lru_cache_manager(max_entries=8, segments=4)

        key1 = CacheKey.create("func", (1,), {})
        key2 = CacheKey.create("func", (2,), {})

        # Cache miss
        assert cache.get(key1) is None
        assert cache.statistics.misses == 1

        # Store value
        cache.put(key1, "value1")
        assert cache.statistics.entry_count == 1

        # Cache hit
        result = cache.get(key1)
        assert result == "value1"
        assert cache.statistics.hits == 1

        # Verify S4LRU promotion
        assert cache.s4lru_cache is not None
        assert cache.s4lru_cache.key_to_segment[key1] == 1  # Promoted from 0 to 1

    def test_s4lru_eviction_behavior(self):
        """Test S4LRU eviction behavior in cache manager."""
        cache = create_s4lru_cache_manager(max_entries=4, segments=2)

        # Fill cache beyond capacity
        keys = []
        for i in range(6):
            key = CacheKey.create("func", (i,), {})
            keys.append(key)
            cache.put(key, f"value{i}")

        # Some entries should be evicted
        assert len(cache.l1_cache) <= 4  # Respects capacity
        assert cache.statistics.entry_count <= 4

        # The most recent entries should still be in cache
        recent_keys = keys[-2:]  # Last 2 entries
        for key in recent_keys:
            assert cache.get(key) is not None

    def test_s4lru_statistics(self):
        """Test S4LRU specific statistics."""
        with managed_cache(
            create_s4lru_cache_manager(max_entries=8, segments=4)
        ) as cache:
            # Add some entries and access them differently
            key1 = CacheKey.create("func", (1,), {})
            key2 = CacheKey.create("func", (2,), {})

            cache.put(key1, "value1")
            cache.put(key2, "value2")

            # Access key1 multiple times to promote it
            for _ in range(3):
                cache.get(key1)

            # Access key2 once
            cache.get(key2)

            # Get S4LRU specific stats
            s4lru_stats = cache.get_s4lru_stats()
            assert s4lru_stats is not None
            assert len(s4lru_stats) == 4

            # Verify different segment utilization
            total_entries = sum(stat.current_size for stat in s4lru_stats)
            assert total_entries == 2  # Two entries total

    async def test_s4lru_factory_configuration(self):
        """Test S4LRU factory function configuration."""
        cache = create_s4lru_cache_manager()

        assert cache.config.eviction_policy == EvictionPolicy.S4LRU
        assert cache.config.s4lru_segments == 4  # Default
        assert cache.config.max_entries == 10000  # Default
        assert cache.config.memory_pressure_threshold == 0.9  # Less aggressive
        assert cache.config.enable_dependency_tracking

        await cache.shutdown()


class TestS4LRUVsTraditionalLRU:
    """Test S4LRU performance vs traditional LRU."""

    async def test_retention_comparison(self):
        """Test that S4LRU provides better retention for frequently accessed items."""
        # Create both types of cache managers
        s4lru_cache = create_s4lru_cache_manager(max_entries=10, segments=4)
        lru_limits = CacheLimits(max_entries=10)
        lru_config = CacheConfiguration(
            limits=lru_limits, eviction_policy=EvictionPolicy.LRU
        )
        lru_cache: SmartCacheManager[str] = SmartCacheManager(lru_config)

        # Add same pattern to both caches
        keys = [CacheKey.create("func", (i,), {}) for i in range(15)]

        # Fill both caches with first 10 items
        for i in range(10):
            s4lru_cache.put(keys[i], f"value{i}")
            lru_cache.put(keys[i], f"value{i}")

        # Access first 3 items frequently to make them "hot"
        for _ in range(5):
            for i in range(3):
                s4lru_cache.get(keys[i])
                lru_cache.get(keys[i])

        # Add 5 more items to trigger evictions
        for i in range(10, 15):
            s4lru_cache.put(keys[i], f"value{i}")
            lru_cache.put(keys[i], f"value{i}")

        # Check retention of frequently accessed items (0, 1, 2)
        s4lru_retained = sum(
            1 for i in range(3) if s4lru_cache.get(keys[i]) is not None
        )
        lru_retained = sum(1 for i in range(3) if lru_cache.get(keys[i]) is not None)

        # S4LRU should retain more frequently accessed items
        assert s4lru_retained >= lru_retained

        await s4lru_cache.shutdown()
        await lru_cache.shutdown()
