"""
Tests for cache key size tracking functionality.
"""

from mpreg.core.caching import (
    CacheConfiguration,
    CacheKey,
    CacheLimits,
    SmartCacheManager,
)
from mpreg.core.enhanced_caching_factories import create_memory_only_cache_manager


class TestKeySizeTracking:
    """Test that cache keys are properly sized and tracked."""

    def test_basic_key_size_tracking(self):
        """Test basic key and value size tracking."""
        cache = create_memory_only_cache_manager(max_memory_mb=10)

        # Create keys with different sizes
        small_key = CacheKey.create("small", (), {})
        large_key = CacheKey.create(
            "large_function_name_with_many_parameters",
            ("arg1", "arg2", "very_long_argument_string" * 100),
            {"key1": "value1", "key2": "very_long_value" * 50},
        )

        # Cache small and large objects
        cache.put(small_key, "small_value")
        cache.put(large_key, "large_value")

        # Get entries and check size tracking
        small_entry = cache.l1_cache[small_key]
        large_entry = cache.l1_cache[large_key]

        # Verify that key sizes are tracked
        assert small_entry.key_size_bytes > 0
        assert large_entry.key_size_bytes > 0
        assert small_entry.value_size_bytes > 0
        assert large_entry.value_size_bytes > 0

        # Large key should have significantly more overhead than small key
        assert large_entry.key_size_bytes > small_entry.key_size_bytes

        # Total size should equal key + value
        assert (
            small_entry.size_bytes
            == small_entry.key_size_bytes + small_entry.value_size_bytes
        )
        assert (
            large_entry.size_bytes
            == large_entry.key_size_bytes + large_entry.value_size_bytes
        )

        cache.shutdown_sync()

    def test_key_to_value_ratio_statistics(self):
        """Test that statistics correctly track key-to-value memory ratios."""
        cache = create_memory_only_cache_manager(max_memory_mb=10)

        # Add entries with different key/value size characteristics

        # Small keys, large values
        for i in range(5):
            key = CacheKey.create(f"k{i}", (), {})
            value = "large_value" * 1000  # Large values
            cache.put(key, value)

        # Large keys, small values
        for i in range(5):
            key = CacheKey.create(
                f"very_long_function_name_with_many_parameters_{i}",
                tuple(f"very_long_argument_{j}" for j in range(50)),
                {f"very_long_key_{j}": f"very_long_value_{j}" for j in range(20)},
            )
            value = "small"  # Small values
            cache.put(key, value)

        stats = cache.get_statistics()

        # Verify breakdown tracking
        assert stats.key_memory_bytes > 0
        assert stats.value_memory_bytes > 0
        assert stats.memory_bytes == stats.key_memory_bytes + stats.value_memory_bytes

        # Test ratio calculations
        ratio = stats.key_to_value_ratio()
        efficiency = stats.memory_efficiency()

        assert ratio >= 0
        assert 0 <= efficiency <= 1

        print(f"Key memory: {stats.key_memory_bytes} bytes")
        print(f"Value memory: {stats.value_memory_bytes} bytes")
        print(f"Key-to-value ratio: {ratio:.2f}")
        print(f"Memory efficiency: {efficiency:.2%}")

        cache.shutdown_sync()

    def test_large_key_eviction_scenario(self):
        """Test eviction behavior with large keys."""
        # Create cache with very small memory limit to force eviction
        cache = create_memory_only_cache_manager(max_memory_mb=0.005)  # 5KB limit

        # Create entries where keys dominate the memory usage
        large_keys = []
        for i in range(20):
            # Create a key with massive argument structure
            huge_args = tuple(
                f"argument_{j}" * 100
                for j in range(100)  # Many large arguments
            )
            huge_kwargs = {
                f"param_{j}": f"value_{j}" * 100
                for j in range(50)  # Many large parameters
            }
            key = CacheKey.create(f"function_{i}", huge_args, huge_kwargs)
            large_keys.append(key)

            # Small value - key should dominate memory
            cache.put(key, f"value_{i}")

        # Should have triggered eviction due to key size
        assert len(cache.l1_cache) < len(large_keys)
        assert cache.statistics.evictions > 0

        # Check that key memory is significant portion
        stats = cache.get_statistics()
        assert stats.key_memory_bytes > 0

        # Key-to-value ratio should be high (keys are larger than values)
        ratio = stats.key_to_value_ratio()
        assert ratio > 1  # Keys should be larger than values in this test

        cache.shutdown_sync()

    def test_memory_pressure_includes_key_overhead(self):
        """Test that memory pressure calculations include key overhead."""
        cache = create_memory_only_cache_manager(max_memory_mb=0.005)  # 5KB limit

        # Track initial state
        initial_count = len(cache.l1_cache)

        # Add entries until memory pressure triggers eviction
        entries_added = 0
        while cache.statistics.evictions == 0 and entries_added < 100:
            # Create key with moderate overhead
            key = CacheKey.create(
                f"moderate_function_name_{entries_added}",
                tuple(f"arg_{i}" for i in range(10)),
                {f"key_{i}": f"val_{i}" for i in range(10)},
            )
            value = "moderate_value" * 100  # 1300+ bytes value
            cache.put(key, value)
            entries_added += 1

        # Should have triggered eviction
        assert cache.statistics.evictions > 0

        # Final statistics should include key overhead
        stats = cache.get_statistics()
        assert stats.key_memory_bytes > 0
        assert stats.value_memory_bytes > 0
        assert stats.memory_bytes == stats.key_memory_bytes + stats.value_memory_bytes

        # Memory should be within configured limits (allowing for threshold)
        memory_limit = cache.config.memory_limit_bytes()
        assert stats.memory_bytes <= memory_limit * 1.2  # Allow 20% tolerance

        cache.shutdown_sync()

    def test_key_size_with_complex_objects(self):
        """Test key sizing with complex argument objects."""
        cache = create_memory_only_cache_manager(max_memory_mb=10)

        # Create keys with complex nested data structures
        complex_args = (
            {"nested": {"data": ["list", "of", "items"] * 100}},
            [{"dict": "in_list"} for _ in range(50)],
            ("tuple", "with", "many", "elements") * 25,
        )
        complex_kwargs = {
            "complex_param": {
                "deeply": {"nested": {"structure": ["with", "lists"] * 50}}
            },
            "another_param": list(range(100)),
        }

        simple_key = CacheKey.create("simple", (), {})
        complex_key = CacheKey.create("complex_func", complex_args, complex_kwargs)

        cache.put(simple_key, "simple_value")
        cache.put(complex_key, "complex_value")

        simple_entry = cache.l1_cache[simple_key]
        complex_entry = cache.l1_cache[complex_key]

        # Complex key should have larger size (though exact ratio depends on hash vs content)
        assert complex_entry.key_size_bytes > simple_entry.key_size_bytes

        # Values should be similar size
        value_size_diff = abs(
            complex_entry.value_size_bytes - simple_entry.value_size_bytes
        )
        assert value_size_diff < 100  # Should be within 100 bytes

        cache.shutdown_sync()

    def test_statistics_reset_clears_key_value_breakdown(self):
        """Test that statistics reset clears key/value memory tracking."""
        cache = create_memory_only_cache_manager(max_memory_mb=10)

        # Add some entries
        for i in range(10):
            key = CacheKey.create(f"func_{i}", (f"arg_{i}",), {"param": f"value_{i}"})
            cache.put(key, f"result_{i}")

        # Get statistics
        stats = cache.get_statistics()
        assert stats.key_memory_bytes > 0
        assert stats.value_memory_bytes > 0

        # Reset statistics
        cache.statistics.reset()

        # Should be cleared
        assert cache.statistics.key_memory_bytes == 0
        assert cache.statistics.value_memory_bytes == 0

        # But getting fresh statistics should recalculate
        fresh_stats = cache.get_statistics()
        assert fresh_stats.key_memory_bytes > 0
        assert fresh_stats.value_memory_bytes > 0

        cache.shutdown_sync()


class TestEdgeCases:
    """Test edge cases for key size tracking."""

    def test_zero_value_size_handling(self):
        """Test handling of entries with zero-sized values."""
        cache = create_memory_only_cache_manager(max_memory_mb=10)

        key = CacheKey.create("test", (), {})
        cache.put(key, None)  # None value

        entry = cache.l1_cache[key]
        assert entry.key_size_bytes > 0
        assert entry.value_size_bytes >= 0  # None might have some size
        assert entry.size_bytes == entry.key_size_bytes + entry.value_size_bytes

        cache.shutdown_sync()

    def test_empty_key_handling(self):
        """Test handling of minimal keys."""
        cache = create_memory_only_cache_manager(max_memory_mb=10)

        key = CacheKey.create("", (), {})  # Minimal key
        cache.put(key, "value")

        entry = cache.l1_cache[key]
        assert entry.key_size_bytes > 0  # Even minimal keys have some overhead
        assert entry.value_size_bytes > 0

        cache.shutdown_sync()

    def test_memory_efficiency_edge_cases(self):
        """Test memory efficiency calculations in edge cases."""
        limits = CacheLimits(max_memory_bytes=None, max_entries=100)
        config = CacheConfiguration(limits=limits)
        cache: SmartCacheManager[str] = SmartCacheManager(config)

        # Empty cache
        stats = cache.get_statistics()
        assert stats.memory_efficiency() == 0.0
        assert stats.key_to_value_ratio() == 0.0

        # Add entry with only key memory (hypothetically)
        key = CacheKey.create("test", (), {})
        cache.put(key, "")  # Empty string value

        stats = cache.get_statistics()
        efficiency = stats.memory_efficiency()
        ratio = stats.key_to_value_ratio()

        assert 0 <= efficiency <= 1
        assert ratio >= 0

        cache.shutdown_sync()
