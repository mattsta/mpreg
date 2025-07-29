"""
Federated Cache Coherence Datastructures for MPREG.

This module provides foundational datastructures for implementing federated cache
coherence protocols across distributed clusters. It implements comprehensive
coherence strategies using topic-pattern routing and distributed coordination.

Key Features:
- Multiple coherence protocols (MSI, MESI, MOESI, directory-based)
- Topic-pattern based cache invalidation and synchronization
- Federation-aware cache partitioning and replication
- ULID-based tracking for distributed cache operations
- Vector clock integration for ordering guarantees
- Performance monitoring and coherence analytics

Design Principles:
- Immutable dataclasses with slots for thread safety
- Semantic type aliases for self-documenting interfaces
- Protocol-based extensible architecture
- Property-based testing with Hypothesis strategies
- Integration with existing MPREG topic routing infrastructure
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable

import ulid
from hypothesis import strategies as st

from mpreg.datastructures.cache_structures import CacheKey
from mpreg.datastructures.type_aliases import (
    ClusterId,
    EntryCount,
    HitCount,
    LatencyMetric,
    MissCount,
    PatternString,
)
from mpreg.datastructures.vector_clock import VectorClock

# Semantic type aliases for federated cache coherence
CoherenceProtocol = str
InvalidationID = str
ReplicationID = str
PartitionID = str
SynchronizationToken = str
ConsistencyLevel = str
CacheRegion = str
Priority = float


class CacheCoherenceState(Enum):
    """Cache coherence states following MSI/MESI/MOESI protocols."""

    # Basic MSI states
    MODIFIED = "modified"  # Cache line modified, exclusive owner
    SHARED = "shared"  # Cache line shared, read-only
    INVALID = "invalid"  # Cache line invalid, must fetch from source

    # Extended MESI states
    EXCLUSIVE = "exclusive"  # Cache line exclusive, unmodified

    # Extended MOESI states
    OWNED = "owned"  # Cache line owned, responsible for providing to others

    # Federation-specific states
    SYNCING = "syncing"  # Cache line being synchronized across clusters
    PENDING = "pending"  # Cache line operation pending federation response
    DEGRADED = "degraded"  # Cache line available but with reduced guarantees


class CoherenceProtocolType(Enum):
    """Types of cache coherence protocols supported."""

    MSI = "msi"  # Modified, Shared, Invalid
    MESI = "mesi"  # Modified, Exclusive, Shared, Invalid
    MOESI = "moesi"  # Modified, Owned, Exclusive, Shared, Invalid
    DIRECTORY_BASED = "directory"  # Directory-based coherence protocol
    WEAK_CONSISTENCY = "weak"  # Weak consistency for performance
    EVENTUAL_CONSISTENCY = "eventual"  # Eventual consistency across federation


class InvalidationType(Enum):
    """Types of cache invalidation operations."""

    SINGLE_KEY = "single_key"  # Invalidate specific cache key
    PATTERN_BASED = "pattern"  # Invalidate keys matching topic pattern
    NAMESPACE_FLUSH = "namespace"  # Flush entire cache namespace
    REGION_CLEAR = "region"  # Clear specific cache region
    CLUSTER_SYNC = "cluster_sync"  # Synchronize with other clusters
    DEPENDENCY_CASCADE = "cascade"  # Cascade invalidation through dependencies


class ReplicationStrategy(Enum):
    """Cache replication strategies across federation."""

    NO_REPLICATION = "none"  # No cross-cluster replication
    ASYNC_REPLICATION = "async"  # Asynchronous replication
    SYNC_REPLICATION = "sync"  # Synchronous replication
    QUORUM_REPLICATION = "quorum"  # Quorum-based replication
    CHAIN_REPLICATION = "chain"  # Chain replication across clusters
    RING_REPLICATION = "ring"  # Ring topology replication


class CacheCoherenceTransition(Enum):
    """Valid cache coherence state transitions for state machine."""

    # From INVALID
    INVALID_TO_EXCLUSIVE = "invalid_to_exclusive"  # Cache miss, fetch exclusive
    INVALID_TO_SHARED = "invalid_to_shared"  # Cache miss, fetch shared

    # From EXCLUSIVE
    EXCLUSIVE_TO_MODIFIED = "exclusive_to_modified"  # Local write
    EXCLUSIVE_TO_SHARED = "exclusive_to_shared"  # Remote read request
    EXCLUSIVE_TO_INVALID = "exclusive_to_invalid"  # Invalidation

    # From MODIFIED
    MODIFIED_TO_OWNED = "modified_to_owned"  # Remote read request (MOESI)
    MODIFIED_TO_SHARED = "modified_to_shared"  # Remote read request (MSI/MESI)
    MODIFIED_TO_INVALID = "modified_to_invalid"  # Invalidation

    # From SHARED
    SHARED_TO_MODIFIED = "shared_to_modified"  # Local write (invalidate others)
    SHARED_TO_INVALID = "shared_to_invalid"  # Invalidation

    # From OWNED (MOESI only)
    OWNED_TO_MODIFIED = "owned_to_modified"  # Local write
    OWNED_TO_INVALID = "owned_to_invalid"  # Invalidation

    # Federation-specific transitions
    SYNCING_TO_SHARED = "syncing_to_shared"  # Federation sync complete
    SYNCING_TO_INVALID = "syncing_to_invalid"  # Federation sync failed
    PENDING_TO_EXCLUSIVE = "pending_to_exclusive"  # Federation response
    PENDING_TO_SHARED = "pending_to_shared"  # Federation response
    PENDING_TO_INVALID = "pending_to_invalid"  # Federation timeout

    # Degraded state transitions
    DEGRADED_TO_SHARED = "degraded_to_shared"  # Recover from degraded
    DEGRADED_TO_INVALID = "degraded_to_invalid"  # Give up on degraded


class CacheOperationType(Enum):
    """Types of cache operations for structured metadata."""

    # Basic cache operations
    GET = "get"  # Cache read operation
    PUT = "put"  # Cache write operation
    DELETE = "delete"  # Cache delete operation

    # Invalidation operations
    SINGLE_KEY_INVALIDATION = "single_key_invalidation"  # Invalidate specific key
    PATTERN_INVALIDATION = "pattern_invalidation"  # Invalidate keys matching pattern
    NAMESPACE_FLUSH = "namespace_flush"  # Flush entire namespace
    REGION_CLEAR = "region_clear"  # Clear cache region

    # Replication operations
    ASYNC_REPLICATION = "async_replication"  # Asynchronous replication
    SYNC_REPLICATION = "sync_replication"  # Synchronous replication
    QUORUM_REPLICATION = "quorum_replication"  # Quorum-based replication

    # Federation operations
    FEDERATION_SYNC = "federation_sync"  # Federation synchronization
    CLUSTER_COORDINATION = "cluster_coordination"  # Cross-cluster coordination
    FEDERATION_BROADCAST = "federation_broadcast"  # Federation-wide broadcast

    # Dependency operations
    DEPENDENCY_CASCADE = "dependency_cascade"  # Dependency-driven invalidation
    DEPENDENCY_NOTIFICATION = (
        "dependency_notification"  # Dependency change notification
    )

    # Monitoring operations
    HEALTH_CHECK = "health_check"  # Cache health monitoring
    METRICS_COLLECTION = "metrics_collection"  # Performance metrics gathering
    STATISTICS_AGGREGATION = "statistics_aggregation"  # Statistics computation


@dataclass(frozen=True, slots=True)
class CacheOperationMetadata:
    """
    Structured metadata for cache operations.

    Replaces dict[str, Any] with proper dataclass structure for
    type safety and better encapsulation.
    """

    operation_type: CacheOperationType = CacheOperationType.GET
    source_operation_id: str = ""
    pattern: str = ""
    target_namespace: str = ""
    expiry_override: float | None = None
    priority_boost: float = 0.0
    correlation_tags: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.priority_boost < 0:
            raise ValueError("Priority boost cannot be negative")


@dataclass(frozen=True, slots=True)
class CacheCoherenceStateMachine:
    """
    State machine for managing cache coherence transitions.

    Implements MESI/MOESI protocol transitions with federation-aware
    extensions for distributed cache coordination.
    """

    protocol: CoherenceProtocolType

    @classmethod
    def valid_transitions(
        cls, protocol: CoherenceProtocolType
    ) -> dict[CacheCoherenceState, frozenset[CacheCoherenceState]]:
        """Get valid state transitions for the given protocol."""
        base_transitions = {
            CacheCoherenceState.INVALID: frozenset(
                [
                    CacheCoherenceState.EXCLUSIVE,
                    CacheCoherenceState.SHARED,
                    CacheCoherenceState.PENDING,
                ]
            ),
            CacheCoherenceState.EXCLUSIVE: frozenset(
                [
                    CacheCoherenceState.MODIFIED,
                    CacheCoherenceState.SHARED,
                    CacheCoherenceState.INVALID,
                ]
            ),
            CacheCoherenceState.SHARED: frozenset(
                [
                    CacheCoherenceState.MODIFIED,
                    CacheCoherenceState.INVALID,
                ]
            ),
            CacheCoherenceState.MODIFIED: frozenset(
                [
                    CacheCoherenceState.SHARED,
                    CacheCoherenceState.INVALID,
                ]
            ),
            # Federation states
            CacheCoherenceState.PENDING: frozenset(
                [
                    CacheCoherenceState.EXCLUSIVE,
                    CacheCoherenceState.SHARED,
                    CacheCoherenceState.INVALID,
                    CacheCoherenceState.DEGRADED,
                ]
            ),
            CacheCoherenceState.SYNCING: frozenset(
                [
                    CacheCoherenceState.SHARED,
                    CacheCoherenceState.INVALID,
                ]
            ),
            CacheCoherenceState.DEGRADED: frozenset(
                [
                    CacheCoherenceState.SHARED,
                    CacheCoherenceState.INVALID,
                ]
            ),
        }

        if protocol == CoherenceProtocolType.MOESI:
            # Add OWNED state for MOESI
            base_transitions[CacheCoherenceState.MODIFIED] = base_transitions[
                CacheCoherenceState.MODIFIED
            ] | frozenset([CacheCoherenceState.OWNED])
            base_transitions[CacheCoherenceState.OWNED] = frozenset(
                [
                    CacheCoherenceState.MODIFIED,
                    CacheCoherenceState.INVALID,
                ]
            )

        return base_transitions

    def is_valid_transition(
        self, from_state: CacheCoherenceState, to_state: CacheCoherenceState
    ) -> bool:
        """Check if a state transition is valid for this protocol."""
        valid_transitions = self.valid_transitions(self.protocol)
        return to_state in valid_transitions.get(from_state, frozenset())

    def get_transition_type(
        self, from_state: CacheCoherenceState, to_state: CacheCoherenceState
    ) -> CacheCoherenceTransition | None:
        """Get the transition type for a state change."""
        if not self.is_valid_transition(from_state, to_state):
            return None

        transition_map = {
            (
                CacheCoherenceState.INVALID,
                CacheCoherenceState.EXCLUSIVE,
            ): CacheCoherenceTransition.INVALID_TO_EXCLUSIVE,
            (
                CacheCoherenceState.INVALID,
                CacheCoherenceState.SHARED,
            ): CacheCoherenceTransition.INVALID_TO_SHARED,
            (
                CacheCoherenceState.EXCLUSIVE,
                CacheCoherenceState.MODIFIED,
            ): CacheCoherenceTransition.EXCLUSIVE_TO_MODIFIED,
            (
                CacheCoherenceState.EXCLUSIVE,
                CacheCoherenceState.SHARED,
            ): CacheCoherenceTransition.EXCLUSIVE_TO_SHARED,
            (
                CacheCoherenceState.EXCLUSIVE,
                CacheCoherenceState.INVALID,
            ): CacheCoherenceTransition.EXCLUSIVE_TO_INVALID,
            (
                CacheCoherenceState.MODIFIED,
                CacheCoherenceState.SHARED,
            ): CacheCoherenceTransition.MODIFIED_TO_SHARED,
            (
                CacheCoherenceState.MODIFIED,
                CacheCoherenceState.INVALID,
            ): CacheCoherenceTransition.MODIFIED_TO_INVALID,
            (
                CacheCoherenceState.SHARED,
                CacheCoherenceState.MODIFIED,
            ): CacheCoherenceTransition.SHARED_TO_MODIFIED,
            (
                CacheCoherenceState.SHARED,
                CacheCoherenceState.INVALID,
            ): CacheCoherenceTransition.SHARED_TO_INVALID,
            (
                CacheCoherenceState.PENDING,
                CacheCoherenceState.EXCLUSIVE,
            ): CacheCoherenceTransition.PENDING_TO_EXCLUSIVE,
            (
                CacheCoherenceState.PENDING,
                CacheCoherenceState.SHARED,
            ): CacheCoherenceTransition.PENDING_TO_SHARED,
            (
                CacheCoherenceState.PENDING,
                CacheCoherenceState.INVALID,
            ): CacheCoherenceTransition.PENDING_TO_INVALID,
            (
                CacheCoherenceState.SYNCING,
                CacheCoherenceState.SHARED,
            ): CacheCoherenceTransition.SYNCING_TO_SHARED,
            (
                CacheCoherenceState.SYNCING,
                CacheCoherenceState.INVALID,
            ): CacheCoherenceTransition.SYNCING_TO_INVALID,
            (
                CacheCoherenceState.DEGRADED,
                CacheCoherenceState.SHARED,
            ): CacheCoherenceTransition.DEGRADED_TO_SHARED,
            (
                CacheCoherenceState.DEGRADED,
                CacheCoherenceState.INVALID,
            ): CacheCoherenceTransition.DEGRADED_TO_INVALID,
        }

        if self.protocol == CoherenceProtocolType.MOESI:
            transition_map.update(
                {
                    (
                        CacheCoherenceState.MODIFIED,
                        CacheCoherenceState.OWNED,
                    ): CacheCoherenceTransition.MODIFIED_TO_OWNED,
                    (
                        CacheCoherenceState.OWNED,
                        CacheCoherenceState.MODIFIED,
                    ): CacheCoherenceTransition.OWNED_TO_MODIFIED,
                    (
                        CacheCoherenceState.OWNED,
                        CacheCoherenceState.INVALID,
                    ): CacheCoherenceTransition.OWNED_TO_INVALID,
                }
            )

        return transition_map.get((from_state, to_state))


@dataclass(frozen=True, slots=True)
class FederatedCacheKey:
    """
    Federated cache key with cluster and region information.

    Extends basic CacheKey with federation-aware metadata for
    cross-cluster cache operations and coherence tracking.
    """

    base_key: CacheKey
    cluster_id: ClusterId
    region: CacheRegion = "default"
    partition_id: PartitionID = "0"

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("Cluster ID cannot be empty")
        if not self.region:
            raise ValueError("Cache region cannot be empty")

    @classmethod
    def create(
        cls,
        namespace: str,
        key: str,
        cluster_id: ClusterId,
        region: CacheRegion = "default",
        partition_id: PartitionID = "0",
    ) -> FederatedCacheKey:
        """Create federated cache key from components."""
        base_key = CacheKey.simple(key=key, namespace=namespace)
        return cls(
            base_key=base_key,
            cluster_id=cluster_id,
            region=region,
            partition_id=partition_id,
        )

    def global_key(self) -> str:
        """Get globally unique key across federation."""
        return f"{self.cluster_id}:{self.region}:{self.partition_id}:{self.base_key.full_key()}"

    def local_key(self) -> str:
        """Get local key within cluster."""
        return self.base_key.full_key()

    def with_cluster(self, cluster_id: ClusterId) -> FederatedCacheKey:
        """Create new key with different cluster ID."""
        return FederatedCacheKey(
            base_key=self.base_key,
            cluster_id=cluster_id,
            region=self.region,
            partition_id=self.partition_id,
        )

    def with_region(self, region: CacheRegion) -> FederatedCacheKey:
        """Create new key with different cache region."""
        return FederatedCacheKey(
            base_key=self.base_key,
            cluster_id=self.cluster_id,
            region=region,
            partition_id=self.partition_id,
        )


@dataclass(frozen=True, slots=True)
class CacheCoherenceMetadata:
    """
    Metadata for cache coherence tracking and coordination.

    Maintains state information required for coherence protocols,
    vector clock ordering, and federation coordination.
    """

    state: CacheCoherenceState
    version: int
    last_modified_time: float
    last_access_time: float
    vector_clock: VectorClock
    owning_cluster: ClusterId
    sharing_clusters: frozenset[ClusterId] = frozenset()
    invalidation_pending: bool = False
    sync_token: SynchronizationToken = ""
    coherence_protocol: CoherenceProtocolType = CoherenceProtocolType.MESI

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ValueError("Version must be positive")
        if not self.owning_cluster:
            raise ValueError("Owning cluster cannot be empty")

    @classmethod
    def create_initial(
        cls,
        owning_cluster: ClusterId,
        protocol: CoherenceProtocolType = CoherenceProtocolType.MESI,
    ) -> CacheCoherenceMetadata:
        """Create initial coherence metadata for new cache entry."""
        current_time = time.time()
        return cls(
            state=CacheCoherenceState.EXCLUSIVE,
            version=1,
            last_modified_time=current_time,
            last_access_time=current_time,
            vector_clock=VectorClock.empty().increment(owning_cluster),
            owning_cluster=owning_cluster,
            coherence_protocol=protocol,
            sync_token=str(ulid.new()),
        )

    def with_state(self, new_state: CacheCoherenceState) -> CacheCoherenceMetadata:
        """Create new metadata with updated coherence state."""
        return CacheCoherenceMetadata(
            state=new_state,
            version=self.version + 1,
            last_modified_time=time.time()
            if new_state == CacheCoherenceState.MODIFIED
            else self.last_modified_time,
            last_access_time=time.time(),
            vector_clock=self.vector_clock.increment(self.owning_cluster),
            owning_cluster=self.owning_cluster,
            sharing_clusters=self.sharing_clusters,
            invalidation_pending=self.invalidation_pending,
            sync_token=self.sync_token,
            coherence_protocol=self.coherence_protocol,
        )

    def with_sharing_clusters(
        self, clusters: frozenset[ClusterId]
    ) -> CacheCoherenceMetadata:
        """Create new metadata with updated sharing clusters."""
        new_state = (
            CacheCoherenceState.SHARED if clusters else CacheCoherenceState.EXCLUSIVE
        )
        return CacheCoherenceMetadata(
            state=new_state,
            version=self.version + 1,
            last_modified_time=self.last_modified_time,
            last_access_time=time.time(),
            vector_clock=self.vector_clock.increment(self.owning_cluster),
            owning_cluster=self.owning_cluster,
            sharing_clusters=clusters,
            invalidation_pending=self.invalidation_pending,
            sync_token=self.sync_token,
            coherence_protocol=self.coherence_protocol,
        )

    def is_exclusive(self) -> bool:
        """Check if cache entry is in exclusive state."""
        return self.state in (
            CacheCoherenceState.EXCLUSIVE,
            CacheCoherenceState.MODIFIED,
        )

    def is_shared(self) -> bool:
        """Check if cache entry is shared across clusters."""
        return (
            self.state == CacheCoherenceState.SHARED and len(self.sharing_clusters) > 0
        )

    def is_valid(self) -> bool:
        """Check if cache entry is in valid state."""
        return self.state != CacheCoherenceState.INVALID

    def can_write(self) -> bool:
        """Check if local writes are allowed."""
        return self.state in (
            CacheCoherenceState.MODIFIED,
            CacheCoherenceState.EXCLUSIVE,
        )

    def can_read(self) -> bool:
        """Check if local reads are allowed."""
        return (
            self.state
            in (
                CacheCoherenceState.MODIFIED,
                CacheCoherenceState.EXCLUSIVE,
                CacheCoherenceState.SHARED,
                CacheCoherenceState.OWNED,
                CacheCoherenceState.SYNCING,  # Can read during sync (may be stale)
                CacheCoherenceState.DEGRADED,  # Can read in degraded mode (may be inconsistent)
            )
        )


@dataclass(frozen=True, slots=True)
class CacheInvalidationMessage:
    """
    Message for cache invalidation across federated clusters.

    Represents invalidation requests that are routed via topic patterns
    to coordinate cache coherence across distributed clusters.
    """

    invalidation_id: InvalidationID
    cache_key: FederatedCacheKey
    invalidation_type: InvalidationType
    source_cluster: ClusterId
    target_clusters: frozenset[ClusterId]
    topic_pattern: PatternString
    vector_clock: VectorClock
    priority: Priority = 1.0
    ttl_seconds: float = 300.0  # 5 minute TTL for invalidation messages
    metadata: CacheOperationMetadata = field(default_factory=CacheOperationMetadata)
    created_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not self.invalidation_id:
            raise ValueError("Invalidation ID cannot be empty")
        if not self.source_cluster:
            raise ValueError("Source cluster cannot be empty")
        if self.ttl_seconds <= 0:
            raise ValueError("TTL must be positive")

    @classmethod
    def create_single_key_invalidation(
        cls,
        cache_key: FederatedCacheKey,
        source_cluster: ClusterId,
        target_clusters: frozenset[ClusterId],
        vector_clock: VectorClock,
    ) -> CacheInvalidationMessage:
        """Create invalidation message for single cache key."""
        invalidation_id = f"inv-{ulid.new()}"
        topic_pattern = (
            f"cache.invalidation.{cache_key.region}.{cache_key.base_key.namespace.name}"
        )

        return cls(
            invalidation_id=invalidation_id,
            cache_key=cache_key,
            invalidation_type=InvalidationType.SINGLE_KEY,
            source_cluster=source_cluster,
            target_clusters=target_clusters,
            topic_pattern=topic_pattern,
            vector_clock=vector_clock,
            priority=2.0,  # High priority for single key invalidations
        )

    @classmethod
    def create_pattern_invalidation(
        cls,
        pattern: PatternString,
        region: CacheRegion,
        source_cluster: ClusterId,
        target_clusters: frozenset[ClusterId],
        vector_clock: VectorClock,
    ) -> CacheInvalidationMessage:
        """Create invalidation message for pattern-based invalidation."""
        invalidation_id = f"inv-pattern-{ulid.new()}"
        # Create placeholder key for pattern invalidation
        placeholder_key = FederatedCacheKey.create(
            namespace="pattern",
            key=pattern,
            cluster_id=source_cluster,
            region=region,
        )
        topic_pattern = f"cache.invalidation.{region}.pattern"

        return cls(
            invalidation_id=invalidation_id,
            cache_key=placeholder_key,
            invalidation_type=InvalidationType.PATTERN_BASED,
            source_cluster=source_cluster,
            target_clusters=target_clusters,
            topic_pattern=topic_pattern,
            vector_clock=vector_clock,
            priority=1.5,  # Medium-high priority for pattern invalidations
            metadata=CacheOperationMetadata(
                operation_type=CacheOperationType.PATTERN_INVALIDATION,
                pattern=pattern,
                target_namespace=region,
            ),
        )

    def is_expired(self) -> bool:
        """Check if invalidation message has expired."""
        return (time.time() - self.created_at) > self.ttl_seconds

    def affects_cluster(self, cluster_id: ClusterId) -> bool:
        """Check if invalidation affects specified cluster."""
        return cluster_id in self.target_clusters or len(self.target_clusters) == 0


@dataclass(frozen=True, slots=True)
class CacheReplicationMessage:
    """
    Message for cache replication across federated clusters.

    Handles cache value replication and synchronization to maintain
    consistency across distributed cache regions.
    """

    replication_id: ReplicationID
    cache_key: FederatedCacheKey
    cache_value: Any
    coherence_metadata: CacheCoherenceMetadata
    replication_strategy: ReplicationStrategy
    source_cluster: ClusterId
    target_clusters: frozenset[ClusterId]
    topic_pattern: PatternString
    consistency_level: ConsistencyLevel = "eventual"
    priority: Priority = 1.0
    created_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not self.replication_id:
            raise ValueError("Replication ID cannot be empty")
        if not self.source_cluster:
            raise ValueError("Source cluster cannot be empty")

    @classmethod
    def create_async_replication(
        cls,
        cache_key: FederatedCacheKey,
        cache_value: Any,
        coherence_metadata: CacheCoherenceMetadata,
        source_cluster: ClusterId,
        target_clusters: frozenset[ClusterId],
    ) -> CacheReplicationMessage:
        """Create asynchronous replication message."""
        replication_id = f"repl-async-{ulid.new()}"
        topic_pattern = f"cache.replication.{cache_key.region}.async"

        return cls(
            replication_id=replication_id,
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            replication_strategy=ReplicationStrategy.ASYNC_REPLICATION,
            source_cluster=source_cluster,
            target_clusters=target_clusters,
            topic_pattern=topic_pattern,
            consistency_level="eventual",
            priority=0.8,  # Lower priority for async replication
        )

    @classmethod
    def create_sync_replication(
        cls,
        cache_key: FederatedCacheKey,
        cache_value: Any,
        coherence_metadata: CacheCoherenceMetadata,
        source_cluster: ClusterId,
        target_clusters: frozenset[ClusterId],
    ) -> CacheReplicationMessage:
        """Create synchronous replication message."""
        replication_id = f"repl-sync-{ulid.new()}"
        topic_pattern = f"cache.replication.{cache_key.region}.sync"

        return cls(
            replication_id=replication_id,
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
            replication_strategy=ReplicationStrategy.SYNC_REPLICATION,
            source_cluster=source_cluster,
            target_clusters=target_clusters,
            topic_pattern=topic_pattern,
            consistency_level="strong",
            priority=2.0,  # High priority for sync replication
        )


@dataclass(frozen=True, slots=True)
class FederatedCacheStatistics:
    """
    Statistics for federated cache operations and coherence.

    Tracks performance metrics, coherence protocol effectiveness,
    and federation-specific cache analytics.
    """

    local_hits: HitCount = 0
    local_misses: MissCount = 0
    federation_hits: HitCount = 0
    federation_misses: MissCount = 0
    invalidations_sent: EntryCount = 0
    invalidations_received: EntryCount = 0
    replications_sent: EntryCount = 0
    replications_received: EntryCount = 0
    coherence_violations: EntryCount = 0
    cross_cluster_requests: EntryCount = 0
    average_invalidation_latency_ms: LatencyMetric = 0.0
    average_replication_latency_ms: LatencyMetric = 0.0
    protocol_transitions: dict[str, EntryCount] = field(default_factory=dict)
    cluster_synchronizations: dict[ClusterId, EntryCount] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Validate all counts are non-negative
        for field_name, field_value in [
            ("local_hits", self.local_hits),
            ("local_misses", self.local_misses),
            ("federation_hits", self.federation_hits),
            ("federation_misses", self.federation_misses),
            ("invalidations_sent", self.invalidations_sent),
            ("invalidations_received", self.invalidations_received),
            ("replications_sent", self.replications_sent),
            ("replications_received", self.replications_received),
            ("coherence_violations", self.coherence_violations),
            ("cross_cluster_requests", self.cross_cluster_requests),
        ]:
            if field_value < 0:
                raise ValueError(f"{field_name} cannot be negative")

    def total_hits(self) -> HitCount:
        """Calculate total cache hits across local and federation."""
        return self.local_hits + self.federation_hits

    def total_misses(self) -> MissCount:
        """Calculate total cache misses across local and federation."""
        return self.local_misses + self.federation_misses

    def total_requests(self) -> EntryCount:
        """Calculate total cache requests."""
        return self.total_hits() + self.total_misses()

    def local_hit_rate(self) -> float:
        """Calculate local cache hit rate."""
        local_total = self.local_hits + self.local_misses
        return self.local_hits / local_total if local_total > 0 else 0.0

    def federation_hit_rate(self) -> float:
        """Calculate federation cache hit rate."""
        federation_total = self.federation_hits + self.federation_misses
        return self.federation_hits / federation_total if federation_total > 0 else 0.0

    def overall_hit_rate(self) -> float:
        """Calculate overall cache hit rate."""
        total = self.total_requests()
        return self.total_hits() / total if total > 0 else 0.0

    def coherence_efficiency(self) -> float:
        """Calculate coherence protocol efficiency."""
        total_operations = self.invalidations_sent + self.replications_sent
        violations = self.coherence_violations
        return 1.0 - (violations / total_operations) if total_operations > 0 else 1.0

    def federation_efficiency(self) -> float:
        """Calculate federation efficiency (federation hits / cross-cluster requests)."""
        if self.cross_cluster_requests <= 0:
            return 0.0
        # Efficiency should be capped at 1.0 since federation hits shouldn't exceed cross-cluster requests
        return min(1.0, self.federation_hits / self.cross_cluster_requests)

    def with_local_hit(self) -> FederatedCacheStatistics:
        """Create new statistics with additional local hit."""
        return FederatedCacheStatistics(
            local_hits=self.local_hits + 1,
            local_misses=self.local_misses,
            federation_hits=self.federation_hits,
            federation_misses=self.federation_misses,
            invalidations_sent=self.invalidations_sent,
            invalidations_received=self.invalidations_received,
            replications_sent=self.replications_sent,
            replications_received=self.replications_received,
            coherence_violations=self.coherence_violations,
            cross_cluster_requests=self.cross_cluster_requests,
            average_invalidation_latency_ms=self.average_invalidation_latency_ms,
            average_replication_latency_ms=self.average_replication_latency_ms,
            protocol_transitions=self.protocol_transitions.copy(),
            cluster_synchronizations=self.cluster_synchronizations.copy(),
        )

    def with_federation_hit(self) -> FederatedCacheStatistics:
        """Create new statistics with additional federation hit."""
        # Ensure federation hits don't exceed cross-cluster requests
        new_cross_cluster_requests = max(
            self.cross_cluster_requests + 1, self.federation_hits + 1
        )
        return FederatedCacheStatistics(
            local_hits=self.local_hits,
            local_misses=self.local_misses,
            federation_hits=self.federation_hits + 1,
            federation_misses=self.federation_misses,
            invalidations_sent=self.invalidations_sent,
            invalidations_received=self.invalidations_received,
            replications_sent=self.replications_sent,
            replications_received=self.replications_received,
            coherence_violations=self.coherence_violations,
            cross_cluster_requests=new_cross_cluster_requests,
            average_invalidation_latency_ms=self.average_invalidation_latency_ms,
            average_replication_latency_ms=self.average_replication_latency_ms,
            protocol_transitions=self.protocol_transitions.copy(),
            cluster_synchronizations=self.cluster_synchronizations.copy(),
        )


@runtime_checkable
class FederatedCacheCoherenceProtocol(Protocol):
    """
    Protocol for federated cache coherence implementations.

    Defines the interface that cache coherence protocols must implement
    to integrate with the MPREG federated caching system.
    """

    async def handle_read_request(
        self,
        cache_key: FederatedCacheKey,
        requesting_cluster: ClusterId,
    ) -> tuple[Any, CacheCoherenceMetadata] | None:
        """Handle read request from another cluster."""
        ...

    async def handle_write_request(
        self,
        cache_key: FederatedCacheKey,
        cache_value: Any,
        requesting_cluster: ClusterId,
    ) -> CacheCoherenceMetadata:
        """Handle write request from another cluster."""
        ...

    async def handle_invalidation(
        self,
        invalidation_message: CacheInvalidationMessage,
    ) -> bool:
        """Handle cache invalidation message."""
        ...

    async def handle_replication(
        self,
        replication_message: CacheReplicationMessage,
    ) -> bool:
        """Handle cache replication message."""
        ...

    async def transition_coherence_state(
        self,
        cache_key: FederatedCacheKey,
        from_state: CacheCoherenceState,
        to_state: CacheCoherenceState,
        metadata: CacheCoherenceMetadata,
    ) -> CacheCoherenceMetadata:
        """Transition cache entry between coherence states."""
        ...


# Hypothesis strategies for property-based testing


def federated_cache_key_strategy() -> st.SearchStrategy[FederatedCacheKey]:
    """Generate valid FederatedCacheKey instances for testing."""
    return st.builds(
        FederatedCacheKey.create,
        namespace=st.text(
            min_size=1,
            max_size=50,
            alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-",
        ).filter(lambda x: x.replace("_", "").replace("-", "").isalnum()),
        key=st.text(min_size=1, max_size=100),
        cluster_id=st.text(
            min_size=1,
            max_size=20,
            alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-",
        ),
        region=st.text(
            min_size=1,
            max_size=30,
            alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_",
        ),
        partition_id=st.text(min_size=1, max_size=10, alphabet="0123456789"),
    )


def cache_coherence_metadata_strategy() -> st.SearchStrategy[CacheCoherenceMetadata]:
    """Generate valid CacheCoherenceMetadata instances for testing."""
    return st.builds(
        CacheCoherenceMetadata,
        state=st.sampled_from(CacheCoherenceState),
        version=st.integers(min_value=1, max_value=1000),
        last_modified_time=st.floats(min_value=0, max_value=time.time() + 86400),
        last_access_time=st.floats(min_value=0, max_value=time.time() + 86400),
        vector_clock=st.builds(
            lambda cid: VectorClock.empty().increment(cid),
            cid=st.text(min_size=1, max_size=20),
        ),
        owning_cluster=st.text(
            min_size=1,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-"
            ),
        ),
        sharing_clusters=st.frozensets(st.text(min_size=1, max_size=20), max_size=5),
        invalidation_pending=st.booleans(),
        sync_token=st.text(min_size=1, max_size=50),
        coherence_protocol=st.sampled_from(CoherenceProtocolType),
    )


def cache_invalidation_message_strategy() -> st.SearchStrategy[
    CacheInvalidationMessage
]:
    """Generate valid CacheInvalidationMessage instances for testing."""
    return st.builds(
        CacheInvalidationMessage,
        invalidation_id=st.text(min_size=1, max_size=50),
        cache_key=federated_cache_key_strategy(),
        invalidation_type=st.sampled_from(InvalidationType),
        source_cluster=st.text(min_size=1, max_size=20),
        target_clusters=st.frozensets(st.text(min_size=1, max_size=20), max_size=3),
        topic_pattern=st.text(min_size=1, max_size=100),
        vector_clock=st.builds(
            lambda cid: VectorClock.empty().increment(cid),
            cid=st.text(min_size=1, max_size=20),
        ),
        priority=st.floats(min_value=0.1, max_value=3.0),
        ttl_seconds=st.floats(min_value=1.0, max_value=3600.0),
        metadata=st.builds(CacheOperationMetadata),
        created_at=st.floats(min_value=0, max_value=time.time()),
    )


def federated_cache_statistics_strategy() -> st.SearchStrategy[
    FederatedCacheStatistics
]:
    """Generate valid FederatedCacheStatistics instances for testing."""

    @st.composite
    def build_consistent_stats(draw):
        local_hits = draw(st.integers(min_value=0, max_value=100000))
        local_misses = draw(st.integers(min_value=0, max_value=100000))
        federation_hits = draw(st.integers(min_value=0, max_value=50000))
        federation_misses = draw(st.integers(min_value=0, max_value=50000))

        # Ensure cross_cluster_requests is at least as large as federation_hits + federation_misses
        min_cross_cluster = federation_hits + federation_misses
        cross_cluster_requests = draw(
            st.integers(
                min_value=min_cross_cluster, max_value=50000 + min_cross_cluster
            )
        )

        invalidations_sent = draw(st.integers(min_value=0, max_value=10000))
        invalidations_received = draw(st.integers(min_value=0, max_value=10000))
        replications_sent = draw(st.integers(min_value=0, max_value=10000))
        replications_received = draw(st.integers(min_value=0, max_value=10000))

        # Ensure coherence violations don't exceed total operations
        total_operations = invalidations_sent + replications_sent
        coherence_violations = draw(
            st.integers(min_value=0, max_value=max(1, total_operations))
        )

        return FederatedCacheStatistics(
            local_hits=local_hits,
            local_misses=local_misses,
            federation_hits=federation_hits,
            federation_misses=federation_misses,
            invalidations_sent=invalidations_sent,
            invalidations_received=invalidations_received,
            replications_sent=replications_sent,
            replications_received=replications_received,
            coherence_violations=coherence_violations,
            cross_cluster_requests=cross_cluster_requests,
            average_invalidation_latency_ms=draw(
                st.floats(min_value=0.0, max_value=1000.0)
            ),
            average_replication_latency_ms=draw(
                st.floats(min_value=0.0, max_value=1000.0)
            ),
            protocol_transitions=draw(
                st.dictionaries(
                    st.text(max_size=20),
                    st.integers(min_value=0, max_value=1000),
                    max_size=10,
                )
            ),
            cluster_synchronizations=draw(
                st.dictionaries(
                    st.text(max_size=20),
                    st.integers(min_value=0, max_value=1000),
                    max_size=5,
                )
            ),
        )

    return build_consistent_stats()
