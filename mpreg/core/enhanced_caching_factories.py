"""
Enhanced factory functions for caching with proper memory and count limits.
"""

from __future__ import annotations

from typing import Any

from mpreg.core.caching import (
    CacheConfiguration,
    CacheLimits,
    EvictionPolicy,
    MemoryMB,
    SmartCacheManager,
)


def create_memory_and_count_limited_cache_manager(
    max_memory_mb: MemoryMB, max_entries: int, enforce_both: bool = False
) -> SmartCacheManager[Any]:
    """Create cache manager with both memory and entry count limits."""
    limits = CacheLimits(
        max_memory_bytes=max_memory_mb * 1024 * 1024,
        max_entries=max_entries,
        enforce_both_limits=enforce_both,
    )
    config = CacheConfiguration(
        limits=limits,
        eviction_policy=EvictionPolicy.COST_BASED,
        memory_pressure_threshold=0.8,
        enable_dependency_tracking=True,
        enable_accurate_sizing=True,
    )
    return SmartCacheManager(config)


def create_enhanced_s4lru_cache_manager(
    max_entries: int = 10000, segments: int = 4, max_memory_mb: MemoryMB | None = None
) -> SmartCacheManager[Any]:
    """Create enhanced S4LRU cache manager with memory limits per segment."""
    limits = CacheLimits(
        max_memory_bytes=max_memory_mb * 1024 * 1024 if max_memory_mb else None,
        max_entries=max_entries,
        enforce_both_limits=False,
    )
    config = CacheConfiguration(
        limits=limits,
        eviction_policy=EvictionPolicy.S4LRU,
        s4lru_segments=segments,
        memory_pressure_threshold=0.9,
        enable_dependency_tracking=True,
        enable_accurate_sizing=True,
    )
    return SmartCacheManager(config)


def create_memory_only_cache_manager(max_memory_mb: MemoryMB) -> SmartCacheManager[Any]:
    """Create cache manager limited only by memory usage."""
    limits = CacheLimits(
        max_memory_bytes=max_memory_mb * 1024 * 1024,
        max_entries=None,
        enforce_both_limits=False,
    )
    config = CacheConfiguration(
        limits=limits,
        eviction_policy=EvictionPolicy.LRU,
        memory_pressure_threshold=0.9,
        enable_dependency_tracking=True,
        enable_accurate_sizing=True,
    )
    return SmartCacheManager(config)


def create_count_only_cache_manager(max_entries: int) -> SmartCacheManager[Any]:
    """Create cache manager limited only by entry count."""
    limits = CacheLimits(
        max_memory_bytes=None, max_entries=max_entries, enforce_both_limits=False
    )
    config = CacheConfiguration(
        limits=limits,
        eviction_policy=EvictionPolicy.LRU,
        memory_pressure_threshold=0.8,
        enable_dependency_tracking=True,
        enable_accurate_sizing=True,
    )
    return SmartCacheManager(config)
