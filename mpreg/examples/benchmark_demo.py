#!/usr/bin/env python3
"""
Quick demo of the enhanced benchmarking capabilities.

This demonstrates the improved timing, statistical analysis, and automated
recommendations with a shortened test run.
"""

import asyncio
import sys

# Add the parent directory to Python path for imports
sys.path.insert(0, ".")

from mpreg.core.topic_exchange import TopicMatchingBenchmark
from mpreg.datastructures.trie import TopicTrie

from .topic_exchange_benchmark import BenchmarkRun


async def demo_enhanced_benchmarking():
    """Demonstrate enhanced benchmarking with proper timing and statistics."""
    print("🎯 ENHANCED BENCHMARKING DEMO")
    print("=" * 50)
    print("⏱️  Ensuring minimum 3-second test duration for accuracy")
    print("📊 Comprehensive statistics: min/mean/median/max/stddev/p95/p99")
    print()

    # Demo 1: Pattern matching performance with statistics
    print("📈 Demo 1: Pattern Matching with Statistical Analysis")
    print("-" * 55)

    # Setup
    trie = TopicTrie()
    topics = TopicMatchingBenchmark.generate_test_topics(10_000)
    patterns = TopicMatchingBenchmark.generate_test_patterns(100)

    # Add patterns
    for i, pattern in enumerate(patterns):
        trie.add_pattern(pattern, f"sub_{i}")

    # Benchmark with proper timing
    def match_operation():
        topic = topics[len(topics) % len(topics)]
        return trie.match_topic(topic)

    benchmark = BenchmarkRun(
        name="pattern_matching_demo",
        min_duration_seconds=3.0,  # Ensure minimum duration
        warmup_iterations=500,
    )

    stats = benchmark.run_timed_benchmark(match_operation)

    print("📊 Performance Statistics:")
    print(f"  Mean:     {stats.mean:8,.0f} matches/sec")
    print(f"  Median:   {stats.median:8,.0f} matches/sec")
    print(f"  Std Dev:  {stats.std_dev:8,.0f} matches/sec")
    print(f"  Min:      {stats.min_val:8,.0f} matches/sec")
    print(f"  Max:      {stats.max_val:8,.0f} matches/sec")
    print(f"  P95:      {stats.p95:8,.0f} matches/sec")
    print(f"  P99:      {stats.p99:8,.0f} matches/sec")
    print(f"  CV:       {(stats.std_dev / stats.mean * 100):8.1f}%")

    # Demo 2: Memory efficiency analysis
    print("\n🧠 Demo 2: Memory Efficiency Analysis")
    print("-" * 40)

    trie_stats = trie.get_stats()
    print("📈 Trie Efficiency:")
    print(f"  Total nodes:     {trie_stats.total_nodes:6,}")
    print(f"  Cache hit ratio: {trie_stats.cache_hit_ratio:6.1%}")
    print(f"  Topics per node: {len(topics) / trie_stats.total_nodes:6.2f}")
    print(
        f"  Memory efficiency: {'Excellent' if len(topics) / trie_stats.total_nodes > 5 else 'Good'}"
    )

    # Demo 3: Performance consistency analysis
    print("\n📊 Demo 3: Performance Consistency")
    print("-" * 35)

    cv = stats.std_dev / stats.mean
    consistency_rating = (
        "Excellent"
        if cv < 0.1
        else "Good"
        if cv < 0.2
        else "Fair"
        if cv < 0.3
        else "Poor"
    )

    print(f"🎯 Coefficient of Variation: {cv:.1%}")
    print(f"⚡ Performance Consistency: {consistency_rating}")
    print(
        f"📈 Performance Range: {((stats.max_val - stats.min_val) / stats.mean * 100):.1f}% spread"
    )

    # Demo 4: Automated recommendations
    print("\n💡 Demo 4: Automated Performance Analysis")
    print("-" * 45)

    recommendations = []

    if stats.mean > 50_000:
        recommendations.append(
            "✅ Excellent throughput - suitable for high-frequency systems"
        )
    elif stats.mean > 10_000:
        recommendations.append(
            "✅ Good throughput - suitable for real-time applications"
        )
    else:
        recommendations.append(
            "⚠️  Moderate throughput - suitable for standard applications"
        )

    if cv < 0.15:
        recommendations.append("✅ Consistent performance - predictable latency")
    else:
        recommendations.append("⚠️  Variable performance - monitor for outliers")

    if trie_stats.cache_hit_ratio > 0.8:
        recommendations.append(
            "✅ Excellent cache efficiency - well-optimized patterns"
        )
    else:
        recommendations.append(
            "⚠️  Consider pattern optimization for better cache performance"
        )

    print("🔍 Automated Analysis:")
    for rec in recommendations:
        print(f"  {rec}")

    print("\n🎉 Enhanced Benchmarking Demo Complete!")
    print("✨ Key Improvements:")
    print(
        f"  • Minimum {benchmark.min_duration_seconds}s test duration eliminates timing noise"
    )
    print(f"  • {len(stats.samples)} statistical samples for accuracy")
    print("  • Comprehensive percentile analysis (P95, P99)")
    print("  • Automated performance recommendations")
    print("  • Warmup periods for consistent measurements")


def main():
    """Run the enhanced benchmarking demo."""
    asyncio.run(demo_enhanced_benchmarking())


if __name__ == "__main__":
    main()
