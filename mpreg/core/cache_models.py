"""
Shared cache data models for MPREG.

These types are used across cache protocol, federation, and persistence layers
to avoid circular imports between core cache logic and fabric integrations.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from .caching import CacheKey as LocalCacheKey
from .serialization import JsonSerializer


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
    L4 = "L4"  # Fabric federation


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
    ) -> GlobalCacheKey:
        """Create cache key from function call parameters."""
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
    ) -> GlobalCacheKey:
        """Create cache key from arbitrary data."""
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
        default_factory=lambda: frozenset(
            [CacheLevel.L1, CacheLevel.L2, CacheLevel.L3, CacheLevel.L4]
        )
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
    replication_sites: set[str] = field(default_factory=set)
    vector_clock: dict[str, int] = field(default_factory=dict)
    checksum: str = ""

    def __post_init__(self) -> None:
        if not self.checksum:
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
            logger.warning("Cache integrity check failed for {}: {}", self.key, e)
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
