"""
Centralized cache datastructures for MPREG.

This module consolidates all the scattered cache-related datastructures
across the codebase into a unified, well-tested implementation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from hypothesis import strategies as st


class CacheLevel(Enum):
    """Cache level enumeration."""

    L1 = "l1"
    L2 = "l2"
    L3 = "l3"
    PERSISTENT = "persistent"
    DISTRIBUTED = "distributed"


@dataclass(frozen=True, slots=True)
class CacheNamespace:
    """Namespace for cache operations."""

    name: str
    description: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Cache namespace name cannot be empty")
        if not self.name.replace("_", "").replace("-", "").isalnum():
            raise ValueError(
                "Cache namespace name must be alphanumeric with underscores/hyphens"
            )


@dataclass(frozen=True, slots=True)
class CacheKey:
    """
    Unified cache key implementation.

    Consolidates all CacheKey implementations across MPREG into a single,
    well-encapsulated datastructure that ensures consistency and proper
    hash/equality semantics.
    """

    namespace: CacheNamespace
    key: str
    subkey: str = ""
    version: int = 1

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("Cache key cannot be empty")
        if self.version < 1:
            raise ValueError("Cache key version must be positive")

    @classmethod
    def simple(cls, key: str, namespace: str = "default") -> CacheKey:
        """Create simple cache key with default namespace."""
        return cls(namespace=CacheNamespace(name=namespace), key=key)

    @classmethod
    def global_key(
        cls, cluster_id: str, key: str, namespace: str = "global"
    ) -> CacheKey:
        """Create global cache key for federated caching."""
        return cls(
            namespace=CacheNamespace(
                name=namespace, description="Global federated cache"
            ),
            key=f"{cluster_id}:{key}",
        )

    def with_subkey(self, subkey: str) -> CacheKey:
        """Create new cache key with subkey."""
        return CacheKey(
            namespace=self.namespace, key=self.key, subkey=subkey, version=self.version
        )

    def with_version(self, version: int) -> CacheKey:
        """Create new cache key with different version."""
        return CacheKey(
            namespace=self.namespace, key=self.key, subkey=self.subkey, version=version
        )

    def full_key(self) -> str:
        """Get full key including namespace and subkey."""
        parts = [self.namespace.name, self.key]
        if self.subkey:
            parts.append(self.subkey)
        if self.version > 1:
            parts.append(f"v{self.version}")
        return ":".join(parts)

    def __str__(self) -> str:
        return self.full_key()

    def __hash__(self) -> int:
        return hash((self.namespace.name, self.key, self.subkey, self.version))


@dataclass(frozen=True, slots=True)
class CacheMetadata:
    """Metadata for cache entries."""

    size_bytes: int = 0
    access_count: int = 0
    last_access_time: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    expiry_time: float | None = None
    tags: frozenset[str] = frozenset()
    source: str = ""

    def __post_init__(self) -> None:
        if self.size_bytes < 0:
            raise ValueError("Size bytes cannot be negative")
        if self.access_count < 0:
            raise ValueError("Access count cannot be negative")

    def is_expired(self) -> bool:
        """Check if this cache entry has expired."""
        if self.expiry_time is None:
            return False
        return time.time() > self.expiry_time

    def age_seconds(self) -> float:
        """Get age of cache entry in seconds."""
        return time.time() - self.created_at

    def time_since_access(self) -> float:
        """Get time since last access in seconds."""
        return time.time() - self.last_access_time

    def with_access(self) -> CacheMetadata:
        """Create new metadata with updated access information."""
        new_access_time = max(time.time(), self.last_access_time)
        return CacheMetadata(
            size_bytes=self.size_bytes,
            access_count=self.access_count + 1,
            last_access_time=new_access_time,
            created_at=self.created_at,
            updated_at=self.updated_at,
            expiry_time=self.expiry_time,
            tags=self.tags,
            source=self.source,
        )


@dataclass(frozen=True, slots=True)
class CacheEntry:
    """
    Unified cache entry implementation.

    Consolidates all cache entry types across MPREG into a single,
    immutable datastructure with proper encapsulation.
    """

    key: CacheKey
    value: Any
    metadata: CacheMetadata = field(default_factory=CacheMetadata)
    level: CacheLevel = CacheLevel.L1

    def is_expired(self) -> bool:
        """Check if this cache entry has expired."""
        return self.metadata.is_expired()

    def with_access(self) -> CacheEntry:
        """Create new cache entry with updated access metadata."""
        return CacheEntry(
            key=self.key,
            value=self.value,
            metadata=self.metadata.with_access(),
            level=self.level,
        )

    def with_value(self, value: Any) -> CacheEntry:
        """Create new cache entry with updated value."""
        return CacheEntry(
            key=self.key,
            value=value,
            metadata=CacheMetadata(
                size_bytes=self.metadata.size_bytes,
                access_count=self.metadata.access_count,
                last_access_time=self.metadata.last_access_time,
                created_at=self.metadata.created_at,
                updated_at=time.time(),
                expiry_time=self.metadata.expiry_time,
                tags=self.metadata.tags,
                source=self.metadata.source,
            ),
            level=self.level,
        )

    def size_bytes(self) -> int:
        """Get size of this cache entry in bytes."""
        return self.metadata.size_bytes


@dataclass(frozen=True, slots=True)
class CacheStatistics:
    """
    Unified cache statistics implementation.

    Consolidates all cache statistics across MPREG into a single
    datastructure for consistent monitoring and reporting.
    """

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    entries: int = 0
    total_size_bytes: int = 0
    average_access_time_ms: float = 0.0
    max_size_bytes: int = 0

    def __post_init__(self) -> None:
        if self.hits < 0:
            raise ValueError("Hits cannot be negative")
        if self.misses < 0:
            raise ValueError("Misses cannot be negative")
        if self.evictions < 0:
            raise ValueError("Evictions cannot be negative")
        if self.entries < 0:
            raise ValueError("Entries cannot be negative")
        if self.total_size_bytes < 0:
            raise ValueError("Total size bytes cannot be negative")

    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def miss_rate(self) -> float:
        """Calculate cache miss rate."""
        total = self.hits + self.misses
        return self.misses / total if total > 0 else 1.0

    def fill_rate(self) -> float:
        """Calculate cache fill rate."""
        if self.max_size_bytes <= 0:
            return 0.0
        return min(1.0, self.total_size_bytes / self.max_size_bytes)

    def average_entry_size(self) -> float:
        """Calculate average entry size in bytes."""
        return self.total_size_bytes / self.entries if self.entries > 0 else 0.0

    def with_hit(self, access_time_ms: float = 0.0) -> CacheStatistics:
        """Create new statistics with additional hit."""
        new_avg_time = (
            self.average_access_time_ms * (self.hits + self.misses) + access_time_ms
        ) / (self.hits + self.misses + 1)

        return CacheStatistics(
            hits=self.hits + 1,
            misses=self.misses,
            evictions=self.evictions,
            entries=self.entries,
            total_size_bytes=self.total_size_bytes,
            average_access_time_ms=new_avg_time,
            max_size_bytes=self.max_size_bytes,
        )

    def with_miss(self, access_time_ms: float = 0.0) -> CacheStatistics:
        """Create new statistics with additional miss."""
        new_avg_time = (
            self.average_access_time_ms * (self.hits + self.misses) + access_time_ms
        ) / (self.hits + self.misses + 1)

        return CacheStatistics(
            hits=self.hits,
            misses=self.misses + 1,
            evictions=self.evictions,
            entries=self.entries,
            total_size_bytes=self.total_size_bytes,
            average_access_time_ms=new_avg_time,
            max_size_bytes=self.max_size_bytes,
        )

    def with_eviction(self, entry_size: int = 0) -> CacheStatistics:
        """Create new statistics with additional eviction."""
        return CacheStatistics(
            hits=self.hits,
            misses=self.misses,
            evictions=self.evictions + 1,
            entries=max(0, self.entries - 1),
            total_size_bytes=max(0, self.total_size_bytes - entry_size),
            average_access_time_ms=self.average_access_time_ms,
            max_size_bytes=self.max_size_bytes,
        )

    def with_entry_added(self, entry_size: int) -> CacheStatistics:
        """Create new statistics with entry added."""
        return CacheStatistics(
            hits=self.hits,
            misses=self.misses,
            evictions=self.evictions,
            entries=self.entries + 1,
            total_size_bytes=self.total_size_bytes + entry_size,
            average_access_time_ms=self.average_access_time_ms,
            max_size_bytes=self.max_size_bytes,
        )


# Hypothesis strategies for property-based testing
def cache_namespace_strategy() -> st.SearchStrategy[CacheNamespace]:
    """Generate valid CacheNamespace instances for testing."""
    return st.builds(
        CacheNamespace,
        name=st.text(
            min_size=1,
            max_size=50,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ).filter(lambda x: x and x.replace("_", "").replace("-", "").isalnum()),
        description=st.text(max_size=200),
    )


def cache_key_strategy() -> st.SearchStrategy[CacheKey]:
    """Generate valid CacheKey instances for testing."""
    return st.builds(
        CacheKey,
        namespace=cache_namespace_strategy(),
        key=st.text(min_size=1, max_size=100),
        subkey=st.text(max_size=50),
        version=st.integers(min_value=1, max_value=1000),
    )


def cache_metadata_strategy() -> st.SearchStrategy[CacheMetadata]:
    """Generate valid CacheMetadata instances for testing."""
    return st.builds(
        CacheMetadata,
        size_bytes=st.integers(min_value=0, max_value=1024 * 1024),
        access_count=st.integers(min_value=0, max_value=10000),
        last_access_time=st.floats(min_value=0, max_value=time.time() + 86400),
        created_at=st.floats(min_value=0, max_value=time.time()),
        updated_at=st.floats(min_value=0, max_value=time.time()),
        expiry_time=st.one_of(
            st.none(), st.floats(min_value=time.time(), max_value=time.time() + 86400)
        ),
        tags=st.frozensets(st.text(min_size=1, max_size=20), max_size=10),
        source=st.text(max_size=100),
    )


def cache_entry_strategy() -> st.SearchStrategy[CacheEntry]:
    """Generate valid CacheEntry instances for testing."""
    return st.builds(
        CacheEntry,
        key=cache_key_strategy(),
        value=st.one_of(
            st.none(),
            st.integers(),
            st.text(),
            st.lists(st.integers(), max_size=10),
            st.dictionaries(st.text(max_size=10), st.integers(), max_size=5),
        ),
        metadata=cache_metadata_strategy(),
        level=st.sampled_from(CacheLevel),
    )


def cache_statistics_strategy() -> st.SearchStrategy[CacheStatistics]:
    """Generate valid CacheStatistics instances for testing."""
    return st.builds(
        CacheStatistics,
        hits=st.integers(min_value=0, max_value=100000),
        misses=st.integers(min_value=0, max_value=100000),
        evictions=st.integers(min_value=0, max_value=10000),
        entries=st.integers(min_value=0, max_value=10000),
        total_size_bytes=st.integers(min_value=0, max_value=1024 * 1024 * 1024),
        average_access_time_ms=st.floats(min_value=0.0, max_value=1000.0),
        max_size_bytes=st.integers(min_value=0, max_value=1024 * 1024 * 1024),
    )
