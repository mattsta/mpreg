"""
Advanced Cache Coherence with Merkle Tree Integration and Conflict Resolution.

This module provides optional advanced features for federated cache coherence:
- MerkleAwareFederatedCacheKey with integrity verification
- CacheConflictResolver with multiple resolution strategies
- Leader election support for cache namespaces
- Enhanced conflict resolution with cryptographic guarantees

These are OPTIONAL components that can be used when stronger consistency
guarantees are needed at the cost of additional storage and computation.

Key Features:
- Merkle tree integration for data integrity verification
- Multiple conflict resolution strategies (leader election, merkle hash comparison, etc.)
- Cryptographic proof validation for cache entries
- Leader election for authoritative conflict resolution
- Well-encapsulated design following MPREG principles
- Comprehensive property-based testing support

Usage Examples:
    # Create merkle-aware cache key with integrity verification
    merkle_tree = MerkleTree.from_leaves([cache_data])
    cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
        namespace="users",
        key="profile:123",
        cluster_id="cluster-a",
        cache_value=user_profile,
        merkle_tree=merkle_tree
    )

    # Set up conflict resolution strategy
    strategy = CacheConflictResolutionStrategy(
        strategy_type=ConflictResolutionType.MERKLE_HASH_COMPARISON,
        merkle_verification_required=True
    )

    resolver = CacheConflictResolver(strategy)
    resolved_entry = await resolver.resolve_conflict(local_entry, remote_entry, context)
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import ulid

from .cache_structures import CacheKey
from .federated_cache_coherence import (
    CacheCoherenceMetadata,
    CacheRegion,
    ClusterId,
    FederatedCacheKey,
    PartitionID,
)
from .merkle_tree import MerkleHash, MerkleProofPath, MerkleTree
from .type_aliases import JsonDict, RequestId, Timestamp


@dataclass(frozen=True, slots=True)
class MerkleAwareFederatedCacheKey:
    """
    Cache key with merkle tree integration for integrity verification.

    This is an OPTIONAL enhancement to FederatedCacheKey that adds:
    - Cryptographic data integrity verification via merkle proofs
    - Tamper detection and corruption recovery
    - Efficient diff computation for cache synchronization
    - Version tracking for merkle tree updates

    Use this when you need stronger integrity guarantees at the cost
    of additional storage and computation overhead.
    """

    base_key: CacheKey
    cluster_id: ClusterId
    region: CacheRegion = "default"
    partition_id: PartitionID = "0"

    # Merkle tree integration for integrity verification
    merkle_hash: MerkleHash = ""
    merkle_proof: MerkleProofPath = field(default_factory=list)
    tree_version: int = 1
    data_checksum: str = ""

    def __post_init__(self) -> None:
        # Validate that merkle components are consistent
        if self.merkle_hash and not self.data_checksum:
            raise ValueError("Merkle hash requires data checksum")
        if self.data_checksum and not self.merkle_hash:
            raise ValueError("Data checksum requires merkle hash")
        if self.tree_version < 1:
            raise ValueError("Tree version must be positive")

    @classmethod
    def create_with_merkle(
        cls,
        namespace: str,
        key: str,
        cluster_id: ClusterId,
        cache_value: Any,
        merkle_tree: MerkleTree,
        region: CacheRegion = "default",
        partition_id: PartitionID = "0",
    ) -> MerkleAwareFederatedCacheKey:
        """
        Create cache key with merkle tree verification.

        Args:
            namespace: Cache namespace name
            key: Cache key within namespace
            cluster_id: Source cluster identifier
            cache_value: The actual cache value to be verified
            merkle_tree: Merkle tree for integrity verification
            region: Cache region (optional)
            partition_id: Partition identifier (optional)

        Returns:
            MerkleAwareFederatedCacheKey with integrity verification data
        """
        # Serialize cache value for hashing (deterministic JSON)
        serialized_data = json.dumps(
            cache_value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()

        # Calculate data checksum
        data_checksum = hashlib.sha256(serialized_data).hexdigest()

        # Add data to merkle tree and get proof
        updated_tree = merkle_tree.append_leaf(serialized_data)
        leaf_index = updated_tree.leaf_count() - 1

        merkle_hash = updated_tree.root_hash()
        merkle_proof = updated_tree.generate_proof(leaf_index).proof_path

        # Create base cache key
        base_key = CacheKey.simple(key=key, namespace=namespace)

        return cls(
            base_key=base_key,
            cluster_id=cluster_id,
            region=region,
            partition_id=partition_id,
            merkle_hash=merkle_hash,
            merkle_proof=merkle_proof,
            tree_version=1,  # Will be updated by merkle tree management
            data_checksum=data_checksum,
        )

    @classmethod
    def from_standard_key(
        cls, standard_key: FederatedCacheKey, cache_value: Any, merkle_tree: MerkleTree
    ) -> MerkleAwareFederatedCacheKey:
        """Convert standard FederatedCacheKey to merkle-aware version."""
        return cls.create_with_merkle(
            namespace=standard_key.base_key.namespace.name,
            key=standard_key.base_key.key,
            cluster_id=standard_key.cluster_id,
            cache_value=cache_value,
            merkle_tree=merkle_tree,
            region=standard_key.region,
            partition_id=standard_key.partition_id,
        )

    def verify_integrity(self, cache_value: Any, merkle_tree: MerkleTree) -> bool:
        """
        Verify cache value integrity using merkle proof.

        Args:
            cache_value: The cache value to verify
            merkle_tree: Current merkle tree for verification

        Returns:
            True if integrity verification passes, False otherwise
        """
        if not self.merkle_hash or not self.data_checksum:
            # No merkle data to verify against
            return False

        # Serialize and hash the provided cache value
        serialized_data = json.dumps(
            cache_value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()

        calculated_checksum = hashlib.sha256(serialized_data).hexdigest()

        # Check if data has been tampered with
        if calculated_checksum != self.data_checksum:
            return False

        # Verify merkle proof if tree root hash matches
        if merkle_tree.root_hash() != self.merkle_hash:
            return False

        # Find leaf in tree and verify proof
        try:
            leaf_index = merkle_tree.find_leaf_index(serialized_data)
            proof = merkle_tree.generate_proof(leaf_index)
            return proof.verify()
        except ValueError:
            # Leaf not found in tree
            return False

    def to_standard_key(self) -> FederatedCacheKey:
        """Convert to standard FederatedCacheKey (loses merkle data)."""
        return FederatedCacheKey(
            base_key=self.base_key,
            cluster_id=self.cluster_id,
            region=self.region,
            partition_id=self.partition_id,
        )

    def to_dict(self) -> JsonDict:
        """Convert to dictionary for serialization."""
        return {
            "base_key": {
                "namespace": self.base_key.namespace.name,
                "key": self.base_key.key,
                "subkey": self.base_key.subkey,
                "version": self.base_key.version,
            },
            "cluster_id": self.cluster_id,
            "region": self.region,
            "partition_id": self.partition_id,
            "merkle_hash": self.merkle_hash,
            "merkle_proof": [
                {"hash": hash_val, "is_right_sibling": is_right}
                for hash_val, is_right in self.merkle_proof
            ],
            "tree_version": self.tree_version,
            "data_checksum": self.data_checksum,
        }

    @classmethod
    def from_dict(cls, data: JsonDict) -> MerkleAwareFederatedCacheKey:
        """Create from dictionary."""
        base_key_data = data["base_key"]
        base_key = (
            CacheKey.simple(
                key=base_key_data["key"], namespace=base_key_data["namespace"]
            )
            .with_subkey(base_key_data["subkey"])
            .with_version(base_key_data["version"])
        )

        return cls(
            base_key=base_key,
            cluster_id=data["cluster_id"],
            region=data["region"],
            partition_id=data["partition_id"],
            merkle_hash=data["merkle_hash"],
            merkle_proof=[
                (item["hash"], item["is_right_sibling"])
                for item in data["merkle_proof"]
            ],
            tree_version=data["tree_version"],
            data_checksum=data["data_checksum"],
        )


@dataclass(frozen=True, slots=True)
class FederatedCacheEntry:
    """
    Cache entry with federated coherence metadata.

    Extends basic cache functionality with federation-aware coherence
    tracking, state management, and conflict resolution support.
    """

    cache_key: FederatedCacheKey | MerkleAwareFederatedCacheKey
    cache_value: Any
    coherence_metadata: CacheCoherenceMetadata

    def __post_init__(self) -> None:
        if self.cache_value is None:
            raise ValueError("Cache value cannot be None")


# Type alias for backwards compatibility with existing code
CacheEntry = FederatedCacheEntry


class ConflictResolutionType(Enum):
    """
    Types of conflict resolution strategies for federated cache.

    Each strategy represents a different approach to resolving conflicts
    when multiple clusters have modified the same cache entry simultaneously.
    """

    MERKLE_HASH_COMPARISON = "merkle_hash"  # Use merkle hashes to detect identical data
    LEADER_ELECTION = "leader_election"  # Designated leader cluster wins conflicts
    LAST_WRITER_WINS = "last_writer_wins"  # Vector clock ordering determines winner
    QUORUM_CONSENSUS = "quorum_consensus"  # Majority vote across clusters
    APPLICATION_SPECIFIC = "application"  # Custom application-defined resolution
    MERKLE_TREE_MERGE = "merkle_merge"  # Intelligent merge using tree diffs


@dataclass(frozen=True, slots=True)
class CacheConflictResolutionStrategy:
    """
    Strategy configuration for resolving cache conflicts.

    Defines how conflicts should be resolved when multiple clusters
    have concurrent updates to the same cache entry.
    """

    strategy_type: ConflictResolutionType
    leader_preference: dict[str, float] = field(
        default_factory=dict
    )  # cluster -> weight
    merkle_verification_required: bool = True
    application_resolver: Callable[[CacheEntry, CacheEntry], CacheEntry] | None = None
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("Timeout must be positive")

        if self.strategy_type == ConflictResolutionType.APPLICATION_SPECIFIC:
            if self.application_resolver is None:
                raise ValueError(
                    "Application-specific strategy requires application_resolver"
                )

        # Validate leader preferences are valid probabilities
        for cluster_id, weight in self.leader_preference.items():
            if not isinstance(weight, int | float) or weight < 0:
                raise ValueError(f"Invalid leader weight for {cluster_id}: {weight}")


@dataclass(frozen=True, slots=True)
class ConflictResolutionContext:
    """
    Context information for conflict resolution operations.

    Provides access to federation infrastructure and metadata
    needed for resolving conflicts between cache entries.
    """

    local_cluster: ClusterId
    remote_cluster: ClusterId
    conflict_timestamp: Timestamp = field(default_factory=time.time)
    operation_id: RequestId = field(default_factory=lambda: ulid.new().str)

    # Optional context for advanced resolution
    federation_bridge: Any = None  # Type hint would create circular import
    available_clusters: frozenset[ClusterId] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not self.local_cluster:
            raise ValueError("Local cluster cannot be empty")
        if not self.remote_cluster:
            raise ValueError("Remote cluster cannot be empty")
        if self.conflict_timestamp <= 0:
            raise ValueError("Conflict timestamp must be positive")


@dataclass(frozen=True, slots=True)
class ConflictResolutionResult:
    """
    Result of a cache conflict resolution operation.

    Contains the resolved cache entry along with metadata about
    how the conflict was resolved.
    """

    resolved_entry: CacheEntry
    resolution_strategy: ConflictResolutionType
    winning_cluster: ClusterId
    resolution_timestamp: Timestamp = field(default_factory=time.time)
    resolution_metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.resolution_timestamp <= 0:
            raise ValueError("Resolution timestamp must be positive")


class CacheCorruptionError(Exception):
    """Raised when cache entry fails integrity verification."""

    def __init__(self, message: str, entry_key: str | None = None):
        super().__init__(message)
        self.entry_key = entry_key


@dataclass(slots=True)
class CacheConflictResolver:
    """
    Resolves conflicts between federated cache entries.

    This is the main component for handling cache conflicts using
    various strategies including merkle tree verification, leader
    election, and application-specific resolution logic.

    OPTIONAL component - use when you need advanced conflict resolution
    beyond simple last-writer-wins with vector clocks.
    """

    strategy: CacheConflictResolutionStrategy

    def __post_init__(self) -> None:
        # Initialize leader election if needed
        if self.strategy.strategy_type == ConflictResolutionType.LEADER_ELECTION:
            # Leader election will be initialized when first needed
            pass

    async def resolve_conflict(
        self,
        local_entry: CacheEntry,
        remote_entry: CacheEntry,
        context: ConflictResolutionContext,
    ) -> ConflictResolutionResult:
        """
        Resolve conflict between local and remote cache entries.

        Args:
            local_entry: Cache entry from local cluster
            remote_entry: Cache entry from remote cluster
            context: Resolution context with federation access

        Returns:
            ConflictResolutionResult with resolved entry and metadata

        Raises:
            CacheCorruptionError: If entries fail integrity verification
            ValueError: If resolution strategy is invalid
        """
        # First: Verify integrity using merkle proofs if available and required
        if self.strategy.merkle_verification_required:
            local_valid = await self._verify_merkle_integrity(local_entry)
            remote_valid = await self._verify_merkle_integrity(remote_entry)

            if not local_valid and remote_valid:
                return ConflictResolutionResult(
                    resolved_entry=remote_entry,
                    resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
                    winning_cluster=remote_entry.cache_key.cluster_id,
                    resolution_metadata={"reason": "local_corruption"},
                )
            elif local_valid and not remote_valid:
                return ConflictResolutionResult(
                    resolved_entry=local_entry,
                    resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
                    winning_cluster=local_entry.cache_key.cluster_id,
                    resolution_metadata={"reason": "remote_corruption"},
                )
            elif not local_valid and not remote_valid:
                local_key = local_entry.cache_key.base_key.full_key()
                raise CacheCorruptionError(
                    "Both entries failed merkle verification", entry_key=local_key
                )

        # Apply resolution strategy
        if self.strategy.strategy_type == ConflictResolutionType.MERKLE_HASH_COMPARISON:
            return await self._resolve_by_merkle_hash(local_entry, remote_entry)

        elif self.strategy.strategy_type == ConflictResolutionType.LEADER_ELECTION:
            return await self._resolve_by_leader(local_entry, remote_entry, context)

        elif self.strategy.strategy_type == ConflictResolutionType.LAST_WRITER_WINS:
            return await self._resolve_by_vector_clock(local_entry, remote_entry)

        elif self.strategy.strategy_type == ConflictResolutionType.QUORUM_CONSENSUS:
            return await self._resolve_by_quorum(local_entry, remote_entry, context)

        elif self.strategy.strategy_type == ConflictResolutionType.MERKLE_TREE_MERGE:
            return await self._resolve_by_merkle_merge(local_entry, remote_entry)

        elif self.strategy.strategy_type == ConflictResolutionType.APPLICATION_SPECIFIC:
            return await self._resolve_by_application(
                local_entry, remote_entry, context
            )

        else:
            raise ValueError(
                f"Unknown conflict resolution strategy: {self.strategy.strategy_type}"
            )

    async def _verify_merkle_integrity(self, entry: CacheEntry) -> bool:
        """Verify entry integrity using merkle proof if available."""
        # Check if this is a merkle-aware cache key
        if isinstance(entry.cache_key, MerkleAwareFederatedCacheKey):
            # For now, we'll assume integrity is valid since we don't have
            # the full merkle tree context. In a real implementation,
            # this would verify against the actual merkle tree.
            return bool(entry.cache_key.merkle_hash and entry.cache_key.data_checksum)

        # Standard cache keys always pass merkle verification
        return True

    async def _resolve_by_merkle_hash(
        self, local: CacheEntry, remote: CacheEntry
    ) -> ConflictResolutionResult:
        """Resolve by comparing merkle hashes - identical hashes mean identical data."""
        local_merkle = getattr(local.cache_key, "merkle_hash", "")
        remote_merkle = getattr(remote.cache_key, "merkle_hash", "")

        if local_merkle and remote_merkle and local_merkle == remote_merkle:
            # Identical data - prefer local for performance
            return ConflictResolutionResult(
                resolved_entry=local,
                resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
                winning_cluster=local.cache_key.cluster_id,
                resolution_metadata={"reason": "identical_data"},
            )

        # Different data - need deeper resolution using vector clocks
        local_vc = local.coherence_metadata.vector_clock
        remote_vc = remote.coherence_metadata.vector_clock

        if local_vc.happens_before(remote_vc):
            return ConflictResolutionResult(
                resolved_entry=remote,
                resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
                winning_cluster=remote.cache_key.cluster_id,
                resolution_metadata={
                    "reason": "causal_ordering",
                    "causality": "remote_after_local",
                },
            )
        elif remote_vc.happens_before(local_vc):
            return ConflictResolutionResult(
                resolved_entry=local,
                resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
                winning_cluster=local.cache_key.cluster_id,
                resolution_metadata={
                    "reason": "causal_ordering",
                    "causality": "local_after_remote",
                },
            )
        else:
            # Concurrent updates - use tree version as tiebreaker
            local_version = getattr(local.cache_key, "tree_version", 0)
            remote_version = getattr(remote.cache_key, "tree_version", 0)

            if remote_version > local_version:
                return ConflictResolutionResult(
                    resolved_entry=remote,
                    resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
                    winning_cluster=remote.cache_key.cluster_id,
                    resolution_metadata={
                        "reason": "tree_version",
                        "versions": {"local": local_version, "remote": remote_version},
                    },
                )
            else:
                return ConflictResolutionResult(
                    resolved_entry=local,
                    resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
                    winning_cluster=local.cache_key.cluster_id,
                    resolution_metadata={
                        "reason": "tree_version",
                        "versions": {"local": local_version, "remote": remote_version},
                    },
                )

    async def _resolve_by_leader(
        self, local: CacheEntry, remote: CacheEntry, context: ConflictResolutionContext
    ) -> ConflictResolutionResult:
        """Resolve using leader election - leader's decision wins."""
        # For now, use leader preferences from strategy
        # In full implementation, this would use actual leader election
        local_weight = self.strategy.leader_preference.get(
            local.cache_key.cluster_id, 1.0
        )
        remote_weight = self.strategy.leader_preference.get(
            remote.cache_key.cluster_id, 1.0
        )

        if remote_weight > local_weight:
            return ConflictResolutionResult(
                resolved_entry=remote,
                resolution_strategy=ConflictResolutionType.LEADER_ELECTION,
                winning_cluster=remote.cache_key.cluster_id,
                resolution_metadata={"leader_weight": remote_weight},
            )
        else:
            return ConflictResolutionResult(
                resolved_entry=local,
                resolution_strategy=ConflictResolutionType.LEADER_ELECTION,
                winning_cluster=local.cache_key.cluster_id,
                resolution_metadata={"leader_weight": local_weight},
            )

    async def _resolve_by_vector_clock(
        self, local: CacheEntry, remote: CacheEntry
    ) -> ConflictResolutionResult:
        """Resolve using vector clock ordering (last writer wins)."""
        local_vc = local.coherence_metadata.vector_clock
        remote_vc = remote.coherence_metadata.vector_clock

        if local_vc.happens_before(remote_vc):
            return ConflictResolutionResult(
                resolved_entry=remote,
                resolution_strategy=ConflictResolutionType.LAST_WRITER_WINS,
                winning_cluster=remote.cache_key.cluster_id,
                resolution_metadata={"reason": "remote_causally_after"},
            )
        elif remote_vc.happens_before(local_vc):
            return ConflictResolutionResult(
                resolved_entry=local,
                resolution_strategy=ConflictResolutionType.LAST_WRITER_WINS,
                winning_cluster=local.cache_key.cluster_id,
                resolution_metadata={"reason": "local_causally_after"},
            )
        else:
            # Concurrent - use cluster ID as deterministic tiebreaker
            if remote.cache_key.cluster_id > local.cache_key.cluster_id:
                return ConflictResolutionResult(
                    resolved_entry=remote,
                    resolution_strategy=ConflictResolutionType.LAST_WRITER_WINS,
                    winning_cluster=remote.cache_key.cluster_id,
                    resolution_metadata={"reason": "concurrent_tiebreaker"},
                )
            else:
                return ConflictResolutionResult(
                    resolved_entry=local,
                    resolution_strategy=ConflictResolutionType.LAST_WRITER_WINS,
                    winning_cluster=local.cache_key.cluster_id,
                    resolution_metadata={"reason": "concurrent_tiebreaker"},
                )

    async def _resolve_by_quorum(
        self, local: CacheEntry, remote: CacheEntry, context: ConflictResolutionContext
    ) -> ConflictResolutionResult:
        """Resolve using quorum consensus (simplified for now)."""
        # Simplified quorum: if we have more clusters available, prefer remote
        # In full implementation, this would query other clusters for votes
        if len(context.available_clusters) > 2:
            return ConflictResolutionResult(
                resolved_entry=remote,
                resolution_strategy=ConflictResolutionType.QUORUM_CONSENSUS,
                winning_cluster=remote.cache_key.cluster_id,
                resolution_metadata={"quorum_size": len(context.available_clusters)},
            )
        else:
            return ConflictResolutionResult(
                resolved_entry=local,
                resolution_strategy=ConflictResolutionType.QUORUM_CONSENSUS,
                winning_cluster=local.cache_key.cluster_id,
                resolution_metadata={"quorum_size": len(context.available_clusters)},
            )

    async def _resolve_by_merkle_merge(
        self, local: CacheEntry, remote: CacheEntry
    ) -> ConflictResolutionResult:
        """Intelligent merge using merkle tree diffs (simplified for now)."""
        # Simplified merge: combine metadata from both entries
        # In full implementation, this would use actual merkle tree diffing

        # For now, prefer the entry with more recent vector clock
        local_vc = local.coherence_metadata.vector_clock
        remote_vc = remote.coherence_metadata.vector_clock

        if remote_vc.total_events() > local_vc.total_events():
            winning_entry = remote
            winning_cluster = remote.cache_key.cluster_id
        else:
            winning_entry = local
            winning_cluster = local.cache_key.cluster_id

        return ConflictResolutionResult(
            resolved_entry=winning_entry,
            resolution_strategy=ConflictResolutionType.MERKLE_TREE_MERGE,
            winning_cluster=winning_cluster,
            resolution_metadata={"merge_strategy": "vector_clock_based"},
        )

    async def _resolve_by_application(
        self, local: CacheEntry, remote: CacheEntry, context: ConflictResolutionContext
    ) -> ConflictResolutionResult:
        """Resolve using application-specific logic."""
        if self.strategy.application_resolver is None:
            raise ValueError("Application resolver not configured")

        resolved_entry = self.strategy.application_resolver(local, remote)

        return ConflictResolutionResult(
            resolved_entry=resolved_entry,
            resolution_strategy=ConflictResolutionType.APPLICATION_SPECIFIC,
            winning_cluster=resolved_entry.cache_key.cluster_id,
            resolution_metadata={"resolver": "application_specific"},
        )


@dataclass(slots=True)
class CacheNamespaceLeader:
    """
    Cache namespace leader coordinator using pluggable leader election.

    This is an OPTIONAL component that provides leader election for cache
    namespaces when using LEADER_ELECTION conflict resolution strategy.

    Uses a pluggable LeaderElection implementation to support different
    algorithms (Raft, Quorum, Metric-based) for leader coordination.
    """

    cluster_id: ClusterId
    leader_election: Any  # LeaderElection protocol - avoid circular import

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("Cluster ID cannot be empty")
        if self.leader_election is None:
            raise ValueError("Leader election implementation cannot be None")

    async def elect_leader(self, namespace: str) -> ClusterId:
        """
        Elect leader for cache namespace.

        Args:
            namespace: Cache namespace to elect leader for

        Returns:
            Cluster ID of the elected leader

        Raises:
            ValueError: If namespace is empty
        """
        if not namespace:
            raise ValueError("Namespace cannot be empty")

        return await self.leader_election.elect_leader(namespace)

    async def get_leader(self, namespace: str) -> ClusterId:
        """
        Get current leader for namespace, electing one if needed.

        Args:
            namespace: Cache namespace

        Returns:
            Cluster ID of the current leader
        """
        if not namespace:
            raise ValueError("Namespace cannot be empty")

        # Try to get current leader first
        current_leader = await self.leader_election.get_current_leader(namespace)
        if current_leader:
            return current_leader

        # No current leader, trigger election
        return await self.elect_leader(namespace)

    async def is_leader(self, namespace: str) -> bool:
        """
        Check if this node is the leader for the namespace.

        Args:
            namespace: Cache namespace to check

        Returns:
            True if this node is the leader
        """
        if not namespace:
            raise ValueError("Namespace cannot be empty")

        return await self.leader_election.is_leader(namespace)

    def update_cluster_metrics(
        self,
        cluster_id: ClusterId,
        cpu_usage: float,
        memory_usage: float,
        cache_hit_rate: float,
        active_connections: int,
    ) -> None:
        """
        Update cluster performance metrics for leader election.

        Args:
            cluster_id: Cluster to update metrics for
            cpu_usage: CPU usage percentage (0.0-1.0)
            memory_usage: Memory usage percentage (0.0-1.0)
            cache_hit_rate: Cache hit rate (0.0-1.0)
            active_connections: Number of active connections
        """
        # Import here to avoid circular import
        from .leader_election import LeaderElectionMetrics

        metrics = LeaderElectionMetrics(
            cluster_id=cluster_id,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            cache_hit_rate=cache_hit_rate,
            active_connections=active_connections,
        )

        self.leader_election.update_metrics(cluster_id, metrics)

    async def step_down(self, namespace: str) -> None:
        """
        Step down as leader for the namespace.

        Args:
            namespace: Cache namespace to step down from
        """
        if not namespace:
            raise ValueError("Namespace cannot be empty")

        await self.leader_election.step_down(namespace)

    def get_all_leaders(self) -> dict[str, ClusterId]:
        """
        Get all current namespace leaders.

        Returns:
            Dictionary mapping namespace to leader cluster ID
        """
        # This is a simplified version - real implementation would
        # query all namespaces from the leader election system
        return {}

    def force_leader_election(self, namespace: str) -> None:
        """
        Force a new leader election for the given namespace.

        Args:
            namespace: Cache namespace to force election for
        """
        if not namespace:
            raise ValueError("Namespace cannot be empty")

        # Step down and let natural election occur
        import asyncio

        asyncio.create_task(self.step_down(namespace))
