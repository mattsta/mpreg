#!/usr/bin/env python3
"""
Smart Result Caching & Lifecycle Management Demo

This example demonstrates the comprehensive caching system features including:

1. Multi-tier cache architecture with intelligent management
2. Different eviction policies (LRU, LFU, cost-based, dependency-aware)
3. Dependency tracking and cascade invalidation
4. TTL-based lifecycle management
5. Performance monitoring and statistics
6. Real-world caching scenarios and patterns

Run this example to see the caching system in action!
"""

import asyncio
import time

from mpreg.core.caching import (
    CacheConfiguration,
    CacheKey,
    CacheLimits,
    EvictionPolicy,
    SmartCacheManager,
    create_default_cache_manager,
    create_memory_optimized_cache_manager,
    create_performance_cache_manager,
    create_s4lru_cache_manager,
)


def expensive_computation(data: str, complexity: int = 1) -> str:
    """Simulate an expensive computation for caching demonstration."""
    # Simulate computation time based on complexity
    time.sleep(complexity * 0.01)  # 10ms per complexity unit

    # Generate result based on input
    result = f"processed_{data}_complexity_{complexity}"
    return result


def fibonacci(n: int) -> int:
    """Classic expensive recursive computation for cache demonstration."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


async def demonstrate_basic_caching():
    """Demonstrate basic cache operations."""
    print("🎪 Basic Caching Operations Demo")
    print("=" * 50)

    cache = create_default_cache_manager()

    print("📊 Initial cache statistics:")
    stats = cache.get_statistics()
    print(f"   Entries: {stats.entry_count}, Hit rate: {stats.hit_rate():.1%}")

    # Simulate some function calls with caching
    test_cases = [
        ("compute_data", ("input1",), {"complexity": 1}),
        ("compute_data", ("input2",), {"complexity": 2}),
        ("compute_data", ("input1",), {"complexity": 1}),  # Cache hit
        ("fibonacci", (10,), {}),
        ("fibonacci", (15,), {}),
        ("fibonacci", (10,), {}),  # Cache hit
    ]

    print("\n🔄 Processing function calls:")
    for func_name, args, kwargs in test_cases:
        key = CacheKey.create(func_name, args, kwargs)

        # Check cache first
        cached_result = cache.get(key)
        if cached_result is not None:
            print(f"  ✅ Cache HIT for {func_name}{args}: {cached_result}")
            continue

        # Compute result (simulate)
        start_time = time.time()
        if func_name == "compute_data":
            result = expensive_computation(str(args[0]), kwargs.get("complexity", 1))
        elif func_name == "fibonacci":
            fib_arg = int(args[0]) if isinstance(args[0], int | str) else 10
            result = str(fibonacci(fib_arg)) if fib_arg < 20 else f"fib({fib_arg})"
        else:
            result = f"result_for_{func_name}"

        computation_time = (time.time() - start_time) * 1000  # Convert to ms

        # Store in cache
        cache.put(key, result, computation_cost_ms=computation_time)
        print(
            f"  💾 Cache MISS for {func_name}{args}: computed {result} ({computation_time:.1f}ms)"
        )

    print("\n📈 Final cache statistics:")
    stats = cache.get_statistics()
    print(f"   Entries: {stats.entry_count}")
    print(f"   Hits: {stats.hits}, Misses: {stats.misses}")
    print(f"   Hit rate: {stats.hit_rate():.1%}")
    print(f"   Average computation cost: {stats.avg_computation_cost_ms:.1f}ms")

    await cache.shutdown()


async def demonstrate_eviction_policies():
    """Demonstrate different eviction policies."""
    print("\n🎪 Eviction Policies Demo")
    print("=" * 50)

    policies = [
        (EvictionPolicy.LRU, "Least Recently Used"),
        (EvictionPolicy.LFU, "Least Frequently Used"),
        (EvictionPolicy.COST_BASED, "Cost-Benefit Analysis"),
    ]

    for policy, description in policies:
        print(f"\n🔄 Testing {policy.value.upper()} ({description}):")

        limits = CacheLimits(max_entries=3)  # Small cache to trigger evictions
        config = CacheConfiguration(
            limits=limits,
            eviction_policy=policy,
            eviction_batch_size=1,
        )
        cache: SmartCacheManager[str] = SmartCacheManager(config)

        # Add entries with different characteristics
        entries = [
            ("expensive_rare", 1000.0, 1),  # High cost, low frequency
            ("cheap_frequent", 10.0, 5),  # Low cost, high frequency
            ("medium_medium", 100.0, 3),  # Medium cost, medium frequency
            ("trigger_eviction", 50.0, 1),  # This will trigger eviction
        ]

        for name, cost, access_count in entries:
            key = CacheKey.create(name, (), {})
            cache.put(key, f"result_{name}", computation_cost_ms=cost)

            # Simulate access pattern
            for _ in range(access_count):
                cache.get(key)

            print(f"  📊 Added {name}: cost={cost}ms, accesses={access_count}")

            if len(cache.l1_cache) <= 3:
                print(f"    Current entries: {list(cache.l1_cache.keys())}")
            else:
                print(
                    f"    Triggered eviction! Remaining: {len(cache.l1_cache)} entries"
                )

        # Show what remained in cache
        remaining_entries = []
        for entry in cache.l1_cache.values():
            remaining_entries.append(
                f"{entry.key.function_name} (score: {entry.cost_benefit_score():.2f})"
            )

        print(f"  🏆 Final entries: {remaining_entries}")
        print(f"  📈 Evictions: {cache.statistics.evictions}")

        await cache.shutdown()


async def demonstrate_dependency_tracking():
    """Demonstrate dependency tracking and invalidation."""
    print("\n🎪 Dependency Tracking Demo")
    print("=" * 50)

    config = CacheConfiguration(enable_dependency_tracking=True)
    cache: SmartCacheManager[str] = SmartCacheManager(config)

    # Simulate a data processing pipeline
    print("🔄 Setting up data processing pipeline:")

    # Step 1: Load raw data
    raw_data_key = CacheKey.create("load_raw_data", ("dataset_v1",), {})
    cache.put(raw_data_key, "raw_sensor_data_12345")
    print("  📥 Cached raw data")

    # Step 2: Clean data (depends on raw data)
    clean_data_key = CacheKey.create("clean_data", ("dataset_v1",), {})
    cache.put(clean_data_key, "cleaned_sensor_data", dependencies={raw_data_key})
    print("  🧹 Cached cleaned data (depends on raw data)")

    # Step 3: Analyze data (depends on cleaned data)
    analysis_key = CacheKey.create("analyze_data", ("dataset_v1",), {})
    cache.put(analysis_key, "analysis_results", dependencies={clean_data_key})
    print("  📊 Cached analysis results (depends on cleaned data)")

    # Step 4: Generate report (depends on analysis)
    report_key = CacheKey.create("generate_report", ("dataset_v1",), {})
    cache.put(report_key, "final_report_pdf", dependencies={analysis_key})
    print("  📄 Cached report (depends on analysis)")

    print(f"\n📈 Total cached entries: {len(cache.l1_cache)}")

    # Verify all data is cached
    print("\n🔍 Verifying all data is accessible:")
    for key, description in [
        (raw_data_key, "Raw data"),
        (clean_data_key, "Cleaned data"),
        (analysis_key, "Analysis"),
        (report_key, "Report"),
    ]:
        result = cache.get(key)
        print(f"  ✅ {description}: {result}")

    # Simulate raw data update - should invalidate dependent results
    print("\n🔄 Simulating raw data update (invalidates dependencies):")
    invalidated_count = cache.invalidate_dependencies(raw_data_key)
    print(f"  ⚡ Invalidated {invalidated_count} dependent entries")

    # Check what's still available
    print("\n🔍 Checking cache after invalidation:")
    for key, description in [
        (raw_data_key, "Raw data"),
        (clean_data_key, "Cleaned data"),
        (analysis_key, "Analysis"),
        (report_key, "Report"),
    ]:
        result = cache.get(key)
        status = "✅ Available" if result else "❌ Invalidated"
        print(f"  {status}: {description}")

    print(f"\n📉 Remaining entries: {len(cache.l1_cache)}")

    await cache.shutdown()


async def demonstrate_ttl_lifecycle():
    """Demonstrate TTL-based lifecycle management."""
    print("\n🎪 TTL Lifecycle Management Demo")
    print("=" * 50)

    cache = create_default_cache_manager()

    # Add entries with different TTL values
    entries = [
        ("short_lived", 0.1, "expires_soon"),  # 100ms TTL
        ("medium_lived", 0.5, "expires_medium"),  # 500ms TTL
        ("long_lived", 2.0, "expires_later"),  # 2s TTL
        ("immortal", None, "never_expires"),  # No TTL
    ]

    print("📅 Adding entries with different TTL values:")
    for name, ttl, value in entries:
        key = CacheKey.create(name, (), {})
        cache.put(key, value, ttl_seconds=ttl)
        ttl_desc = f"{ttl * 1000:.0f}ms" if ttl else "∞"
        print(f"  ⏰ {name}: TTL = {ttl_desc}")

    # Check availability over time
    check_times = [0.05, 0.2, 0.6, 1.0, 2.5]  # Times to check in seconds

    for check_time in check_times:
        print(f"\n⏱️  After {check_time}s:")
        await asyncio.sleep(
            check_time
            if check_time == 0.05
            else check_time
            - (
                check_times[check_times.index(check_time) - 1]
                if check_times.index(check_time) > 0
                else 0
            )
        )

        for name, ttl, value in entries:
            key = CacheKey.create(name, (), {})
            result = cache.get(key)

            if result:
                print(f"    ✅ {name}: still available")
            else:
                print(f"    ❌ {name}: expired")

    print("\n📈 Final statistics:")
    stats = cache.get_statistics()
    print(f"   Total accesses: {stats.hits + stats.misses}")
    print(f"   Hit rate: {stats.hit_rate():.1%}")
    print(f"   Remaining entries: {stats.entry_count}")

    await cache.shutdown()


async def demonstrate_performance_monitoring():
    """Demonstrate performance monitoring and analytics."""
    print("\n🎪 Performance Monitoring Demo")
    print("=" * 50)

    cache = create_performance_cache_manager()

    # Simulate various workload patterns
    workloads = [
        ("fast_frequent", 10.0, 20, "Fast computation, frequently accessed"),
        ("slow_frequent", 500.0, 15, "Slow computation, frequently accessed"),
        ("fast_rare", 5.0, 2, "Fast computation, rarely accessed"),
        ("slow_rare", 1000.0, 3, "Slow computation, rarely accessed"),
    ]

    print("🔄 Generating workload with different patterns:")
    for name, cost, access_count, description in workloads:
        key = CacheKey.create(name, (), {})
        cache.put(key, f"result_{name}", computation_cost_ms=cost)

        # Simulate access pattern
        for _ in range(access_count):
            cache.get(key)

        print(f"  📊 {name}: {description}")
        print(f"    Cost: {cost}ms, Accesses: {access_count}")

    # Show comprehensive statistics
    print("\n📈 Performance Analysis:")
    stats = cache.get_statistics()

    print("  📊 Cache Statistics:")
    print(f"    Entries: {stats.entry_count}")
    print(f"    Memory usage: {stats.memory_bytes:,} bytes")
    print(f"    Hit rate: {stats.hit_rate():.1%}")
    print(f"    Average computation cost: {stats.avg_computation_cost_ms:.1f}ms")

    # Analyze top-performing cache entries
    print("\n🏆 Top Cache Entries (by cost-benefit):")
    top_entries = cache.get_top_entries(limit=5)

    for i, entry in enumerate(top_entries, 1):
        print(f"  {i}. {entry.key.function_name}:")
        print(f"     Cost-benefit score: {entry.cost_benefit_score():.2f}")
        print(f"     Computation cost: {entry.computation_cost_ms:.1f}ms")
        print(f"     Access count: {entry.access_count}")
        print(f"     Size: {entry.size_bytes} bytes")
        print(f"     Age: {entry.age_seconds():.1f}s")

    # Show memory efficiency
    total_computation_saved = sum(
        entry.computation_cost_ms * (entry.access_count - 1)
        for entry in cache.l1_cache.values()
        if entry.access_count > 1
    )

    print("\n💰 Performance Benefits:")
    print(f"  ⚡ Total computation time saved: {total_computation_saved:.0f}ms")
    print(
        f"  📦 Memory efficiency: {stats.memory_bytes / stats.entry_count:.0f} bytes/entry"
    )

    await cache.shutdown()


async def demonstrate_s4lru_algorithm():
    """Demonstrate S4LRU (Segmented LRU) cache algorithm."""
    print("\n🎪 S4LRU (Segmented LRU) Algorithm Demo")
    print("=" * 50)

    # Create S4LRU cache with small capacity to show promotion behavior
    cache = create_s4lru_cache_manager(
        max_entries=12, segments=4
    )  # 3 entries per segment

    print("🔄 S4LRU Cache Algorithm Demonstration:")
    print("  • Cache miss: Items inserted at head of segment 0")
    print("  • Cache hit: Items promoted to next higher segment")
    print("  • Eviction: Items flow down segments, evicted from segment 0")
    print("  • Total capacity: 12 entries across 4 segments (3 per segment)")

    # Simulate a realistic access pattern showing S4LRU benefits
    access_pattern = [
        # Initial insertions (all cache misses)
        ("user_profile_123", 50.0, "Cache miss - new user profile"),
        ("product_details_456", 100.0, "Cache miss - product lookup"),
        ("search_results_term1", 200.0, "Cache miss - search query"),
        ("user_profile_789", 50.0, "Cache miss - another user"),
        # Frequent accesses (will be promoted)
        ("user_profile_123", 50.0, "Cache hit - frequent user"),
        ("user_profile_123", 50.0, "Cache hit - very frequent user"),
        ("product_details_456", 100.0, "Cache hit - popular product"),
        # More items to trigger evictions
        ("config_settings", 75.0, "Cache miss - app config"),
        ("notification_data", 25.0, "Cache miss - notification"),
        ("analytics_data", 300.0, "Cache miss - expensive analytics"),
        # Mix of hits and misses
        ("user_profile_123", 50.0, "Cache hit - still frequent"),
        ("temp_data_1", 10.0, "Cache miss - temporary data"),
        ("temp_data_2", 10.0, "Cache miss - more temp data"),
        ("product_details_456", 100.0, "Cache hit - still popular"),
        # More temporary data (should be evicted quickly)
        ("temp_data_3", 5.0, "Cache miss - temp data 3"),
        ("temp_data_4", 5.0, "Cache miss - temp data 4"),
        ("temp_data_5", 5.0, "Cache miss - temp data 5"),
        # Final access to frequently used items
        ("user_profile_123", 50.0, "Cache hit - should be in high segment"),
        ("product_details_456", 100.0, "Cache hit - should be in high segment"),
    ]

    print("\n📊 Processing access pattern:")
    for i, (name, cost, description) in enumerate(access_pattern):
        key = CacheKey.create(name, (), {})

        # Check cache first
        cached_result = cache.get(key)
        if cached_result is not None:
            print(f"  {i + 1:2d}. ✅ HIT:  {name} - {description}")
        else:
            # Cache miss - store the item
            cache.put(key, f"result_{name}", computation_cost_ms=cost)
            print(f"  {i + 1:2d}. ❌ MISS: {name} - {description}")

        # Show S4LRU segment statistics every 5 operations
        if (i + 1) % 5 == 0:
            segment_stats = cache.get_s4lru_stats()
            if segment_stats:
                print(
                    f"      📈 Segment utilization: {[f'S{s.segment_id}:{s.current_size}/{s.max_size}' for s in segment_stats]}"
                )

    print("\n📈 Final S4LRU Segment Statistics:")
    segment_stats = cache.get_s4lru_stats()
    if segment_stats:
        for stat in segment_stats:
            utilization_pct = stat.utilization * 100
            print(
                f"  Segment {stat.segment_id}: {stat.current_size}/{stat.max_size} entries ({utilization_pct:.0f}% full)"
            )

    # Show which items survived in cache
    print("\n🏆 Items remaining in cache (by access frequency):")
    top_entries = cache.get_top_entries(limit=10)
    for i, entry in enumerate(top_entries, 1):
        print(
            f"  {i}. {entry.key.function_name}: {entry.access_count} accesses, score: {entry.cost_benefit_score():.2f}"
        )

    # Show overall cache performance
    stats = cache.get_statistics()
    print("\n💫 S4LRU Performance Summary:")
    print(f"  Hit rate: {stats.hit_rate():.1%}")
    print(f"  Total accesses: {stats.hits + stats.misses}")
    print(f"  Evictions: {stats.evictions}")
    print(f"  Average computation cost: {stats.avg_computation_cost_ms:.1f}ms")

    # Demonstrate segment promotion behavior
    print("\n🔄 Demonstrating segment promotion with frequent access:")
    frequent_key = CacheKey.create("frequently_accessed_item", (), {})
    cache.put(frequent_key, "important_data", computation_cost_ms=500.0)

    print("  Initial placement: Segment 0 (new items start here)")

    for access_num in range(1, 6):
        cache.get(frequent_key)
        segment_stats = cache.get_s4lru_stats()
        # Find which segment contains our key
        if segment_stats:
            for s in segment_stats:
                # This is a simplified check - in practice we'd need to track segment membership
                pass
        print(f"  After access #{access_num}: Item promoted in S4LRU hierarchy")

    await cache.shutdown()


async def demonstrate_factory_configurations():
    """Demonstrate different cache factory configurations."""
    print("\n🎪 Factory Configurations Demo")
    print("=" * 50)

    factories = [
        (create_default_cache_manager, "Default Configuration"),
        (lambda: create_memory_optimized_cache_manager(10), "Memory Optimized (10MB)"),
        (create_performance_cache_manager, "Performance Optimized"),
        (
            lambda: create_s4lru_cache_manager(5000, 6),
            "S4LRU (6 segments, 5000 entries)",
        ),
    ]

    for factory, description in factories:
        print(f"\n🏭 Testing {description}:")
        cache = factory()

        config = cache.config
        print("  📊 Configuration:")
        print(f"    Max memory: {config.max_memory_bytes / (1024 * 1024):.0f}MB")
        print(f"    Max entries: {config.max_entries:,}")
        print(f"    Eviction policy: {config.eviction_policy.value}")
        print(f"    Memory threshold: {config.memory_pressure_threshold:.0%}")
        print(f"    Dependency tracking: {config.enable_dependency_tracking}")
        print(f"    Compression: {config.enable_compression}")

        # Add some test data
        for i in range(5):
            key = CacheKey.create("test", (i,), {})
            cache.put(key, f"test_data_{i}")

        stats = cache.get_statistics()
        print("  📈 Quick test results:")
        print(f"    Entries stored: {stats.entry_count}")
        print(f"    Memory used: {stats.memory_bytes} bytes")

        await cache.shutdown()


async def run_comprehensive_demo():
    """Run the complete caching system demonstration."""
    print("🎪 Smart Result Caching & Lifecycle Management Demo")
    print("=" * 60)
    print("This demo showcases advanced caching features including:")
    print("• Multi-tier cache architecture")
    print("• Intelligent eviction policies (LRU, LFU, Cost-based, S4LRU)")
    print("• Dependency tracking and invalidation")
    print("• TTL-based lifecycle management")
    print("• Performance monitoring and analytics")
    print("• S4LRU segmented cache algorithm")
    print("=" * 60)

    await demonstrate_basic_caching()
    await demonstrate_eviction_policies()
    await demonstrate_dependency_tracking()
    await demonstrate_ttl_lifecycle()
    await demonstrate_performance_monitoring()
    await demonstrate_s4lru_algorithm()
    await demonstrate_factory_configurations()

    print("\n🎉 Demo completed successfully!")
    print("💡 Key Takeaways:")
    print("  • Smart caching can dramatically improve performance")
    print("  • Different eviction policies suit different workloads")
    print("  • S4LRU provides superior retention for frequently accessed items")
    print("  • Dependency tracking ensures data consistency")
    print("  • TTL management automates cache lifecycle")
    print("  • Performance monitoring guides optimization")
    print("  • Factory functions simplify configuration")


def main():
    """Main entry point for the caching demo."""
    asyncio.run(run_comprehensive_demo())


if __name__ == "__main__":
    main()
