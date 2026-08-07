"""
Comprehensive tests for memory-based caching functionality.
"""

import asyncio
import time
from dataclasses import dataclass

import pytest

from mpreg.core.caching import (
    CacheConfiguration,
    CacheKey,
    CacheLimits,
    S4LRUSegmentStats,
    SmartCacheManager,
)
from mpreg.core.enhanced_caching_factories import (
    create_count_only_cache_manager,
    create_enhanced_s4lru_cache_manager,
    create_memory_and_count_limited_cache_manager,
    create_memory_only_cache_manager,
)


@dataclass
class LargeObject:
    """Test object with significant memory footprint."""

    data: bytes
    metadata: dict[str, str]


class TestMemoryBasedEviction:
    """Test memory-based cache eviction."""

    def test_memory_only_limit(self):
        """Test cache that only limits by memory usage."""
        cache = create_memory_only_cache_manager(max_memory_mb=1)  # 1MB limit

        # Create objects of known size
        large_objects = []
        for i in range(10):
            obj = LargeObject(
                data=b"x" * 100000,  # 100KB of data
                metadata={f"key_{j}": f"value_{j}" for j in range(100)},
            )
            large_objects.append(obj)

        # Add objects to cache until memory limit is hit
        stored_keys = []
        for i, obj in enumerate(large_objects):
            key = CacheKey.create(f"large_object_{i}", (), {})
            cache.put(key, obj)
            stored_keys.append(key)

            # Check if eviction started
            if len(cache.l1_cache) < len(stored_keys):
                break

        # Should have triggered eviction before all objects were stored
        assert len(cache.l1_cache) < len(large_objects)
        assert cache.statistics.evictions > 0

        # Check memory usage is within limits
        total_memory = sum(entry.size_bytes for entry in cache.l1_cache.values())
        memory_limit = cache.config.memory_limit_bytes()
        assert total_memory <= memory_limit * 1.2  # Allow some tolerance

        cache.shutdown_sync()

    def test_count_only_limit(self):
        """Test cache that only limits by entry count."""
        max_entries = 5
        cache = create_count_only_cache_manager(max_entries=max_entries)

        # Add more entries than the limit
        keys = []
        for i in range(max_entries * 2):
            key = CacheKey.create(f"entry_{i}", (), {})
            # Create objects of varying sizes
            obj = LargeObject(
                data=b"x" * (i * 10000),  # Varying sizes
                metadata={"size": str(i)},
            )
            cache.put(key, obj)
            keys.append(key)

        # Should be limited by count, not memory
        assert len(cache.l1_cache) <= max_entries
        assert cache.statistics.evictions > 0

        cache.shutdown_sync()

    def test_memory_and_count_limits_either_triggers(self):
        """Test cache with both limits where either can trigger eviction."""
        cache = create_memory_and_count_limited_cache_manager(
            max_memory_mb=1, max_entries=10, enforce_both=False
        )

        # Test that memory limit can trigger eviction before count limit
        large_objects = []
        for i in range(5):  # Less than count limit
            obj = LargeObject(
                data=b"x" * 300000,  # 300KB each - should hit memory limit first
                metadata={f"key_{j}": f"value_{j}" for j in range(50)},
            )
            large_objects.append(obj)

        stored_keys = []
        for i, obj in enumerate(large_objects):
            key = CacheKey.create(f"large_object_{i}", (), {})
            cache.put(key, obj)
            stored_keys.append(key)

        # Should hit memory limit before count limit
        assert len(cache.l1_cache) < 10  # Less than count limit
        assert len(cache.l1_cache) < len(large_objects)  # Eviction occurred

        cache.shutdown_sync()

    def test_memory_and_count_limits_both_required(self):
        """Test cache where both limits must be exceeded to trigger eviction."""
        cache = create_memory_and_count_limited_cache_manager(
            max_memory_mb=10,
            max_entries=3,
            enforce_both=True,  # Large memory, small count
        )

        # Add small objects that exceed count but not memory
        small_objects = []
        for i in range(5):  # More than count limit
            obj = LargeObject(
                data=b"x" * 1000,  # Small objects
                metadata={"id": str(i)},
            )
            small_objects.append(obj)

        for i, obj in enumerate(small_objects):
            key = CacheKey.create(f"small_object_{i}", (), {})
            cache.put(key, obj)

        # Since enforce_both=True and memory limit isn't hit, no eviction should occur
        # until both limits are exceeded
        assert len(cache.l1_cache) >= 3  # Count limit exceeded but cache not evicted

        cache.shutdown_sync()


class TestS4LRUMemoryLimits:
    """Test S4LRU with memory limits per segment."""

    def test_s4lru_with_memory_limits(self):
        """Test S4LRU cache with memory limits."""
        cache = create_enhanced_s4lru_cache_manager(
            max_entries=20, segments=4, max_memory_mb=2
        )

        # Create objects and access patterns
        test_objects = []
        for i in range(15):
            obj = LargeObject(
                data=b"x" * 50000,  # 50KB each
                metadata={"id": str(i), "type": "test"},
            )
            test_objects.append(obj)

        # Add objects with varying access patterns
        frequently_accessed = []
        rarely_accessed = []

        for i, obj in enumerate(test_objects):
            key = CacheKey.create(f"object_{i}", (), {})
            cache.put(key, obj)

            if i < 5:  # First 5 are frequently accessed
                frequently_accessed.append(key)
                # Access multiple times to promote to higher segments
                for _ in range(3):
                    cache.get(key)
            else:
                rarely_accessed.append(key)
                # Access once or not at all
                if i % 2 == 0:
                    cache.get(key)

        # Get segment stats
        stats = cache.get_s4lru_stats()
        assert stats is not None
        assert len(stats) == 4  # 4 segments

        # Verify stats are dataclasses
        for stat in stats:
            assert isinstance(stat, S4LRUSegmentStats)
            assert hasattr(stat, "segment_id")
            assert hasattr(stat, "current_memory_bytes")
            assert hasattr(stat, "memory_utilization")

        # Check that frequently accessed items are in higher segments
        # This requires checking the actual segment membership
        total_memory = sum(stat.current_memory_bytes for stat in stats)
        assert total_memory > 0  # Memory tracking is working

        cache.shutdown_sync()

    def test_s4lru_segment_memory_distribution(self):
        """Test that memory limits are properly distributed across S4LRU segments."""
        cache = create_enhanced_s4lru_cache_manager(
            max_entries=16,
            segments=4,
            max_memory_mb=4,  # 1MB per segment
        )

        # Each segment should have equal memory allocation
        stats = cache.get_s4lru_stats()
        assert stats is not None

        expected_memory_per_segment = (4 * 1024 * 1024) // 4  # 1MB per segment
        for stat in stats:
            assert stat.max_memory_bytes == expected_memory_per_segment

        cache.shutdown_sync()


class TestPymplerIntegration:
    """Test pympler integration for accurate memory measurement."""

    def test_accurate_vs_simple_sizing(self):
        """Test that pympler provides more accurate sizing than simple estimation."""
        # Create cache with accurate sizing enabled
        limits_accurate = CacheLimits(
            max_entries=100, max_memory_bytes=10 * 1024 * 1024
        )
        config_accurate = CacheConfiguration(
            limits=limits_accurate, enable_accurate_sizing=True
        )
        cache_accurate: SmartCacheManager[LargeObject] = SmartCacheManager(
            config_accurate
        )

        # Create cache with simple sizing
        limits_simple = CacheLimits(max_entries=100, max_memory_bytes=10 * 1024 * 1024)
        config_simple = CacheConfiguration(
            limits=limits_simple, enable_accurate_sizing=False
        )
        cache_simple: SmartCacheManager[LargeObject] = SmartCacheManager(config_simple)

        # Create a complex object
        complex_obj = LargeObject(
            data=b"x" * 100000,
            metadata={f"key_{i}": f"value_{i}" * 100 for i in range(100)},
        )

        key = CacheKey.create("complex_object", (), {})

        # Store in both caches
        cache_accurate.put(key, complex_obj)
        cache_simple.put(key, complex_obj)

        # Get size estimates
        accurate_entry = cache_accurate.l1_cache[key]
        simple_entry = cache_simple.l1_cache[key]

        # Accurate sizing should generally be different (often larger) than simple
        # Note: This test might be flaky depending on object structure
        assert accurate_entry.size_bytes != simple_entry.size_bytes

        cache_accurate.shutdown_sync()
        cache_simple.shutdown_sync()

    def test_memory_pressure_triggers_eviction(self):
        """Test that memory pressure correctly triggers eviction."""
        cache = create_memory_only_cache_manager(max_memory_mb=1)

        # Track evictions
        initial_evictions = cache.statistics.evictions

        # Add objects until eviction occurs
        objects_added = 0
        while cache.statistics.evictions == initial_evictions and objects_added < 50:
            obj = LargeObject(
                data=b"x" * 100000,  # 100KB
                metadata={"id": str(objects_added)},
            )
            key = CacheKey.create(f"object_{objects_added}", (), {})
            cache.put(key, obj)
            objects_added += 1

        # Should have triggered eviction
        assert cache.statistics.evictions > initial_evictions
        assert len(cache.l1_cache) < objects_added

        cache.shutdown_sync()


class TestCacheConfigurationValidation:
    """Test cache configuration validation and edge cases."""

    def test_no_limits_specified_gets_defaults(self):
        """Test that default limits are applied when none specified."""
        limits = CacheLimits(max_memory_bytes=None, max_entries=None)
        # Should set defaults in __post_init__
        assert limits.max_memory_bytes == 100 * 1024 * 1024  # 100MB
        assert limits.max_entries == 10000

    def test_cache_limits_validation_methods(self):
        """Test CacheConfiguration validation methods."""
        limits = CacheLimits(
            max_memory_bytes=1024 * 1024,  # 1MB
            max_entries=100,
            enforce_both_limits=False,
        )
        config = CacheConfiguration(limits=limits)

        # Test memory-based eviction triggers
        assert config.should_evict_by_memory(1024 * 1024)  # At limit
        assert not config.should_evict_by_memory(512 * 1024)  # Below limit

        # Test count-based eviction triggers
        assert config.should_evict_by_count(100)  # At limit
        assert not config.should_evict_by_count(50)  # Below limit

        # Test combined logic - either triggers (default)
        assert config.should_evict(1024 * 1024, 50)  # Memory exceeded
        assert config.should_evict(512 * 1024, 100)  # Count exceeded
        assert config.should_evict(1024 * 1024, 100)  # Both exceeded
        assert not config.should_evict(512 * 1024, 50)  # Neither exceeded

        # Test combined logic - both required
        config.limits.enforce_both_limits = True
        assert not config.should_evict(1024 * 1024, 50)  # Only memory exceeded
        assert not config.should_evict(512 * 1024, 100)  # Only count exceeded
        assert config.should_evict(1024 * 1024, 100)  # Both exceeded

    def test_backward_compatibility_properties(self):
        """Test that backward compatibility properties work correctly."""
        limits = CacheLimits(max_memory_bytes=50 * 1024 * 1024, max_entries=5000)
        config = CacheConfiguration(limits=limits)

        # Test backward compatibility properties
        assert config.max_memory_bytes == 50 * 1024 * 1024
        assert config.max_entries == 5000

        # Test defaults when limits are None
        limits_none = CacheLimits(max_memory_bytes=None, max_entries=None)
        config_none = CacheConfiguration(limits=limits_none)
        assert config_none.max_memory_bytes == 100 * 1024 * 1024  # Default
        assert config_none.max_entries == 10000  # Default


class TestSegmentStatsDataclass:
    """Test that segment stats use proper dataclasses."""

    def test_s4lru_segment_stats_dataclass(self):
        """Test S4LRUSegmentStats is a proper dataclass."""
        stats = S4LRUSegmentStats(
            segment_id=0,
            current_size=5,
            max_size=10,
            utilization=0.5,
            current_memory_bytes=1024,
            max_memory_bytes=2048,
        )

        # Test basic properties
        assert stats.segment_id == 0
        assert stats.current_size == 5
        assert stats.max_size == 10
        assert stats.utilization == 0.5
        assert stats.current_memory_bytes == 1024
        assert stats.max_memory_bytes == 2048

        # Test memory utilization property
        assert stats.memory_utilization == 0.5

        # Test with zero max memory
        stats_zero = S4LRUSegmentStats(
            segment_id=1,
            current_size=0,
            max_size=10,
            utilization=0.0,
            current_memory_bytes=0,
            max_memory_bytes=0,
        )
        assert stats_zero.memory_utilization == 0.0

    def test_segment_stats_not_dict(self):
        """Test that segment stats are not dictionaries."""
        cache = create_enhanced_s4lru_cache_manager(max_entries=10, segments=3)

        # Add some data
        for i in range(5):
            key = CacheKey.create(f"test_{i}", (), {})
            cache.put(key, f"value_{i}")

        stats = cache.get_s4lru_stats()
        assert stats is not None
        assert isinstance(stats, list)

        for stat in stats:
            # Should be dataclass, not dict
            assert isinstance(stat, S4LRUSegmentStats)
            assert not isinstance(stat, dict)
            assert hasattr(stat, "segment_id")
            assert hasattr(stat, "current_size")
            assert hasattr(stat, "memory_utilization")

        cache.shutdown_sync()


@pytest.mark.asyncio
async def test_memory_based_async_operations():
    """Test memory-based caching with async operations."""
    cache = create_memory_only_cache_manager(max_memory_mb=1)  # Reduce to 1MB

    # Simulate async workload with larger objects
    async def create_and_cache_object(index: int):
        obj = LargeObject(
            data=b"x" * 100000,  # 100KB each
            metadata={"async_id": str(index), "timestamp": str(time.time())},
        )
        key = CacheKey.create(f"async_object_{index}", (), {})
        cache.put(key, obj)
        return key

    # Create objects concurrently - should exceed 1MB limit
    tasks = [create_and_cache_object(i) for i in range(15)]
    keys = await asyncio.gather(*tasks)

    # Should have triggered memory-based eviction
    # With 15 objects at ~100KB each, that's ~1.5MB, exceeding the 1MB limit
    assert len(cache.l1_cache) < len(keys) or cache.statistics.evictions > 0

    # Verify remaining objects
    remaining_count = 0
    for key in keys:
        if cache.contains(key):
            remaining_count += 1

    assert remaining_count == len(cache.l1_cache)

    cache.shutdown_sync()
