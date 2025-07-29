"""
Comprehensive tests for advanced cache coherence with conflict resolution.

This test suite validates the advanced cache coherence functionality including:
- MerkleAwareFederatedCacheKey with integrity verification
- CacheConflictResolver with multiple resolution strategies
- Conflict resolution contexts and results
- Property-based testing for correctness verification
- Integration with existing merkle tree infrastructure

The tests ensure that all advanced features work correctly as optional
enhancements to the basic federated cache coherence system.
"""

import json
from collections.abc import Callable
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.datastructures.advanced_cache_coherence import (
    CacheConflictResolutionStrategy,
    CacheConflictResolver,
    CacheCorruptionError,
    CacheNamespaceLeader,
    ConflictResolutionContext,
    ConflictResolutionResult,
    ConflictResolutionType,
    FederatedCacheEntry,
    MerkleAwareFederatedCacheKey,
)
from mpreg.datastructures.cache_structures import CacheKey
from mpreg.datastructures.federated_cache_coherence import (
    CacheCoherenceMetadata,
    FederatedCacheKey,
)
from mpreg.datastructures.leader_election import (
    LeaderElectionMetrics,
    MetricBasedLeaderElection,
)
from mpreg.datastructures.merkle_tree import MerkleTree
from mpreg.datastructures.vector_clock import VectorClock

# Hypothesis strategies for property-based testing


def merkle_aware_cache_key_strategy() -> st.SearchStrategy[
    MerkleAwareFederatedCacheKey
]:
    """Generate valid MerkleAwareFederatedCacheKey instances for testing."""

    # Create strategy that ensures consistent merkle hash and checksum
    @st.composite
    def _merkle_key_strategy(draw):
        base_key = CacheKey.simple(
            namespace=draw(
                st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz")
            ),
            key=draw(
                st.text(
                    min_size=1,
                    max_size=50,
                    alphabet="abcdefghijklmnopqrstuvwxyz0123456789",
                )
            ),
        )

        cluster_id = draw(
            st.text(
                min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
            )
        )
        region = draw(
            st.sampled_from(["default", "us-east-1", "us-west-2", "eu-west-1"])
        )
        partition_id = draw(st.text(min_size=1, max_size=5, alphabet="0123456789"))
        tree_version = draw(st.integers(min_value=1, max_value=100))

        # Generate consistent merkle data - either both empty or both present
        has_merkle_data = draw(st.booleans())
        if has_merkle_data:
            merkle_hash = draw(
                st.text(min_size=64, max_size=64, alphabet="0123456789abcdef")
            )
            data_checksum = draw(
                st.text(min_size=64, max_size=64, alphabet="0123456789abcdef")
            )
            merkle_proof = draw(
                st.lists(
                    st.tuples(
                        st.text(min_size=64, max_size=64, alphabet="0123456789abcdef"),
                        st.booleans(),
                    ),
                    max_size=5,
                )
            )
        else:
            merkle_hash = ""
            data_checksum = ""
            merkle_proof = []

        return MerkleAwareFederatedCacheKey(
            base_key=base_key,
            cluster_id=cluster_id,
            region=region,
            partition_id=partition_id,
            merkle_hash=merkle_hash,
            merkle_proof=merkle_proof,
            tree_version=tree_version,
            data_checksum=data_checksum,
        )

    return _merkle_key_strategy()


def conflict_resolution_strategy_strategy() -> st.SearchStrategy[
    CacheConflictResolutionStrategy
]:
    """Generate valid CacheConflictResolutionStrategy instances for testing."""

    @st.composite
    def _strategy_strategy(draw):
        strategy_type = draw(st.sampled_from(ConflictResolutionType))
        leader_preference = draw(
            st.dictionaries(
                st.text(
                    min_size=1,
                    max_size=20,
                    alphabet="abcdefghijklmnopqrstuvwxyz0123456789",
                ),
                st.floats(min_value=0.0, max_value=10.0),
                max_size=3,
            )
        )
        merkle_verification_required = draw(st.booleans())
        timeout_seconds = draw(st.floats(min_value=1.0, max_value=300.0))

        # Handle application-specific strategy properly
        application_resolver: Callable[[Any, Any], Any] | None
        if strategy_type == ConflictResolutionType.APPLICATION_SPECIFIC:
            # Provide a simple application resolver for testing
            def application_resolver(local: Any, remote: Any) -> Any:
                return local
        else:
            application_resolver = None

        return CacheConflictResolutionStrategy(
            strategy_type=strategy_type,
            leader_preference=leader_preference,
            merkle_verification_required=merkle_verification_required,
            application_resolver=application_resolver,
            timeout_seconds=timeout_seconds,
        )

    return _strategy_strategy()


def conflict_resolution_context_strategy() -> st.SearchStrategy[
    ConflictResolutionContext
]:
    """Generate valid ConflictResolutionContext instances for testing."""
    return st.builds(
        ConflictResolutionContext,
        local_cluster=st.text(
            min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
        ),
        remote_cluster=st.text(
            min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
        ),
        conflict_timestamp=st.floats(min_value=1.0, max_value=2000000000.0),
        operation_id=st.text(
            min_size=1, max_size=30, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
        ),
        federation_bridge=st.none(),
        available_clusters=st.frozensets(
            st.text(
                min_size=1, max_size=15, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
            ),
            max_size=5,
        ),
    )


class TestMerkleAwareFederatedCacheKey:
    """Test merkle-aware cache key functionality."""

    def test_basic_creation_with_merkle(self) -> None:
        """Test creating merkle-aware cache key with merkle tree."""
        # Create test data
        cache_value = {"user_id": "123", "name": "Alice", "email": "alice@example.com"}
        merkle_tree = MerkleTree.empty()

        # Create merkle-aware cache key
        cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="users",
            key="profile:123",
            cluster_id="cluster-a",
            cache_value=cache_value,
            merkle_tree=merkle_tree,
        )

        # Verify basic properties
        assert cache_key.base_key.namespace.name == "users"
        assert cache_key.base_key.key == "profile:123"
        assert cache_key.cluster_id == "cluster-a"
        assert cache_key.region == "default"
        assert cache_key.partition_id == "0"

        # Verify merkle properties
        assert cache_key.merkle_hash != ""
        assert cache_key.data_checksum != ""
        assert cache_key.tree_version == 1
        assert len(cache_key.merkle_proof) >= 0

    def test_merkle_aware_key_validation(self) -> None:
        """Test validation of merkle-aware cache key parameters."""
        cache_value = {"test": "data"}
        merkle_tree = MerkleTree.empty()

        # Test with custom region and partition
        cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="test_namespace",
            key="test_key",
            cluster_id="test-cluster",
            cache_value=cache_value,
            merkle_tree=merkle_tree,
            region="us-west-2",
            partition_id="42",
        )

        assert cache_key.region == "us-west-2"
        assert cache_key.partition_id == "42"

    def test_conversion_from_standard_key(self) -> None:
        """Test converting standard FederatedCacheKey to merkle-aware version."""
        # Create standard federated cache key
        standard_key = FederatedCacheKey.create(
            namespace="data",
            key="item:456",
            cluster_id="standard-cluster",
            region="eu-west-1",
        )

        cache_value = ["item1", "item2", "item3"]
        merkle_tree = MerkleTree.empty()

        # Convert to merkle-aware
        merkle_key = MerkleAwareFederatedCacheKey.from_standard_key(
            standard_key=standard_key, cache_value=cache_value, merkle_tree=merkle_tree
        )

        # Verify conversion preserved standard key properties
        assert (
            merkle_key.base_key.namespace.name == standard_key.base_key.namespace.name
        )
        assert merkle_key.base_key.key == standard_key.base_key.key
        assert merkle_key.cluster_id == standard_key.cluster_id
        assert merkle_key.region == standard_key.region

        # Verify merkle properties were added
        assert merkle_key.merkle_hash != ""
        assert merkle_key.data_checksum != ""

    def test_integrity_verification_success(self) -> None:
        """Test successful integrity verification."""
        cache_value = {"verified": True, "data": "integrity_test"}
        merkle_tree = MerkleTree.empty()

        # Create merkle-aware cache key
        cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="verified",
            key="data:789",
            cluster_id="integrity-cluster",
            cache_value=cache_value,
            merkle_tree=merkle_tree,
        )

        # Add the data to merkle tree to simulate storage
        serialized_data = json.dumps(
            cache_value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
        updated_tree = merkle_tree.append_leaf(serialized_data)

        # Verify integrity should pass
        assert cache_key.verify_integrity(cache_value, updated_tree)

    def test_integrity_verification_failure_corrupted_data(self) -> None:
        """Test integrity verification failure with corrupted data."""
        original_value = {"original": "data"}
        corrupted_value = {"corrupted": "data"}
        merkle_tree = MerkleTree.empty()

        # Create cache key with original data
        cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="corruption_test",
            key="corrupt:123",
            cluster_id="corruption-cluster",
            cache_value=original_value,
            merkle_tree=merkle_tree,
        )

        # Try to verify with corrupted data
        assert not cache_key.verify_integrity(corrupted_value, merkle_tree)

    def test_conversion_to_standard_key(self) -> None:
        """Test converting merkle-aware key back to standard key."""
        cache_value = {"convertible": "data"}
        merkle_tree = MerkleTree.empty()

        # Create merkle-aware key
        merkle_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="conversion",
            key="convert:999",
            cluster_id="convert-cluster",
            cache_value=cache_value,
            merkle_tree=merkle_tree,
            region="ap-south-1",
            partition_id="7",
        )

        # Convert to standard key
        standard_key = merkle_key.to_standard_key()

        # Verify properties were preserved (except merkle data)
        assert (
            standard_key.base_key.namespace.name == merkle_key.base_key.namespace.name
        )
        assert standard_key.base_key.key == merkle_key.base_key.key
        assert standard_key.cluster_id == merkle_key.cluster_id
        assert standard_key.region == merkle_key.region
        assert standard_key.partition_id == merkle_key.partition_id

    def test_serialization_to_dict(self) -> None:
        """Test serialization of merkle-aware cache key to dictionary."""
        cache_value = {"serializable": True}
        merkle_tree = MerkleTree.empty()

        cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="serialization",
            key="serialize:001",
            cluster_id="serialize-cluster",
            cache_value=cache_value,
            merkle_tree=merkle_tree,
        )

        # Serialize to dict
        data_dict = cache_key.to_dict()

        # Verify required fields
        assert "base_key" in data_dict
        assert "cluster_id" in data_dict
        assert "merkle_hash" in data_dict
        assert "merkle_proof" in data_dict
        assert "data_checksum" in data_dict
        assert "tree_version" in data_dict

        # Verify base_key structure
        base_key = data_dict["base_key"]
        assert base_key["namespace"] == "serialization"
        assert base_key["key"] == "serialize:001"

    def test_deserialization_from_dict(self) -> None:
        """Test deserialization of merkle-aware cache key from dictionary."""
        cache_value = {"deserializable": True}
        merkle_tree = MerkleTree.empty()

        # Create original key
        original_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="deserialization",
            key="deserialize:002",
            cluster_id="deserialize-cluster",
            cache_value=cache_value,
            merkle_tree=merkle_tree,
        )

        # Serialize and deserialize
        data_dict = original_key.to_dict()
        restored_key = MerkleAwareFederatedCacheKey.from_dict(data_dict)

        # Verify restoration
        assert (
            restored_key.base_key.namespace.name == original_key.base_key.namespace.name
        )
        assert restored_key.base_key.key == original_key.base_key.key
        assert restored_key.cluster_id == original_key.cluster_id
        assert restored_key.merkle_hash == original_key.merkle_hash
        assert restored_key.data_checksum == original_key.data_checksum
        assert restored_key.tree_version == original_key.tree_version

    def test_merkle_aware_key_validation_errors(self) -> None:
        """Test validation errors for merkle-aware cache key."""
        # Test inconsistent merkle data
        with pytest.raises(ValueError, match="Merkle hash requires data checksum"):
            MerkleAwareFederatedCacheKey(
                base_key=CacheKey.simple("test", "test"),
                cluster_id="test-cluster",
                merkle_hash="abc123",
                data_checksum="",  # Missing checksum
            )

        with pytest.raises(ValueError, match="Data checksum requires merkle hash"):
            MerkleAwareFederatedCacheKey(
                base_key=CacheKey.simple("test", "test"),
                cluster_id="test-cluster",
                merkle_hash="",  # Missing hash
                data_checksum="def456",
            )

        with pytest.raises(ValueError, match="Tree version must be positive"):
            MerkleAwareFederatedCacheKey(
                base_key=CacheKey.simple("test", "test"),
                cluster_id="test-cluster",
                tree_version=0,  # Invalid version
            )


class TestFederatedCacheEntry:
    """Test federated cache entry functionality."""

    def test_federated_cache_entry_creation(self) -> None:
        """Test creating federated cache entry with coherence metadata."""
        # Create cache key
        cache_key = FederatedCacheKey.create("entries", "entry:123", "entry-cluster")

        # Create coherence metadata
        coherence_metadata = CacheCoherenceMetadata.create_initial("entry-cluster")

        # Create cache entry
        cache_entry = FederatedCacheEntry(
            cache_key=cache_key,
            cache_value={"entry": "data"},
            coherence_metadata=coherence_metadata,
        )

        assert cache_entry.cache_key == cache_key
        assert cache_entry.cache_value == {"entry": "data"}
        assert cache_entry.coherence_metadata == coherence_metadata

    def test_federated_cache_entry_with_merkle_key(self) -> None:
        """Test federated cache entry with merkle-aware cache key."""
        cache_value = {"merkle_entry": "data"}
        merkle_tree = MerkleTree.empty()

        # Create merkle-aware cache key
        merkle_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="merkle_entries",
            key="merkle:456",
            cluster_id="merkle-cluster",
            cache_value=cache_value,
            merkle_tree=merkle_tree,
        )

        coherence_metadata = CacheCoherenceMetadata.create_initial("merkle-cluster")

        # Create cache entry with merkle key
        cache_entry = FederatedCacheEntry(
            cache_key=merkle_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
        )

        assert isinstance(cache_entry.cache_key, MerkleAwareFederatedCacheKey)
        assert cache_entry.cache_key.merkle_hash != ""

    def test_federated_cache_entry_validation(self) -> None:
        """Test validation of federated cache entry."""
        cache_key = FederatedCacheKey.create("validation", "val:789", "val-cluster")
        coherence_metadata = CacheCoherenceMetadata.create_initial("val-cluster")

        # Test that None cache value raises error
        with pytest.raises(ValueError, match="Cache value cannot be None"):
            FederatedCacheEntry(
                cache_key=cache_key,
                cache_value=None,
                coherence_metadata=coherence_metadata,
            )


class TestConflictResolutionStrategy:
    """Test conflict resolution strategy configuration."""

    def test_strategy_creation_merkle_hash_comparison(self) -> None:
        """Test creating merkle hash comparison strategy."""
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.MERKLE_HASH_COMPARISON,
            merkle_verification_required=True,
            timeout_seconds=45.0,
        )

        assert strategy.strategy_type == ConflictResolutionType.MERKLE_HASH_COMPARISON
        assert strategy.merkle_verification_required is True
        assert strategy.timeout_seconds == 45.0
        assert len(strategy.leader_preference) == 0

    def test_strategy_creation_leader_election(self) -> None:
        """Test creating leader election strategy."""
        leader_preferences = {"cluster-a": 2.0, "cluster-b": 1.5, "cluster-c": 1.0}

        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.LEADER_ELECTION,
            leader_preference=leader_preferences,
            merkle_verification_required=False,
        )

        assert strategy.strategy_type == ConflictResolutionType.LEADER_ELECTION
        assert strategy.leader_preference == leader_preferences
        assert strategy.merkle_verification_required is False

    def test_strategy_validation_timeout(self) -> None:
        """Test strategy validation for timeout values."""
        with pytest.raises(ValueError, match="Timeout must be positive"):
            CacheConflictResolutionStrategy(
                strategy_type=ConflictResolutionType.LAST_WRITER_WINS,
                timeout_seconds=-1.0,
            )

    def test_strategy_validation_application_resolver(self) -> None:
        """Test strategy validation for application-specific resolver."""
        # Should require application_resolver for APPLICATION_SPECIFIC strategy
        with pytest.raises(
            ValueError,
            match="Application-specific strategy requires application_resolver",
        ):
            CacheConflictResolutionStrategy(
                strategy_type=ConflictResolutionType.APPLICATION_SPECIFIC,
                application_resolver=None,
            )

        # Should work with valid application_resolver
        def dummy_resolver(local, remote):
            return local

        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.APPLICATION_SPECIFIC,
            application_resolver=dummy_resolver,
        )

        assert strategy.application_resolver == dummy_resolver

    def test_strategy_validation_leader_preferences(self) -> None:
        """Test strategy validation for leader preferences."""
        # Invalid weight should raise error
        with pytest.raises(ValueError, match="Invalid leader weight"):
            CacheConflictResolutionStrategy(
                strategy_type=ConflictResolutionType.LEADER_ELECTION,
                leader_preference={"cluster-a": -1.0},  # Negative weight
            )


class TestConflictResolutionContext:
    """Test conflict resolution context."""

    def test_context_creation(self) -> None:
        """Test creating conflict resolution context."""
        context = ConflictResolutionContext(
            local_cluster="local-cluster",
            remote_cluster="remote-cluster",
            available_clusters=frozenset(["cluster-a", "cluster-b", "cluster-c"]),
        )

        assert context.local_cluster == "local-cluster"
        assert context.remote_cluster == "remote-cluster"
        assert context.available_clusters == frozenset(
            ["cluster-a", "cluster-b", "cluster-c"]
        )
        assert context.conflict_timestamp > 0
        assert context.operation_id != ""

    def test_context_validation(self) -> None:
        """Test context validation."""
        # Empty local cluster should raise error
        with pytest.raises(ValueError, match="Local cluster cannot be empty"):
            ConflictResolutionContext(local_cluster="", remote_cluster="remote-cluster")

        # Empty remote cluster should raise error
        with pytest.raises(ValueError, match="Remote cluster cannot be empty"):
            ConflictResolutionContext(local_cluster="local-cluster", remote_cluster="")

        # Invalid timestamp should raise error
        with pytest.raises(ValueError, match="Conflict timestamp must be positive"):
            ConflictResolutionContext(
                local_cluster="local-cluster",
                remote_cluster="remote-cluster",
                conflict_timestamp=-1.0,
            )


class TestCacheConflictResolver:
    """Test cache conflict resolver functionality."""

    def create_test_entry(
        self, cluster_id: str, cache_value: dict, vector_clock_increment: int = 1
    ) -> FederatedCacheEntry:
        """Create test cache entry for conflict resolution testing."""
        cache_key = FederatedCacheKey.create("test", "conflict:123", cluster_id)

        # Create vector clock with specified increments
        vector_clock = VectorClock.empty()
        for _ in range(vector_clock_increment):
            vector_clock = vector_clock.increment(cluster_id)

        coherence_metadata = CacheCoherenceMetadata.create_initial(cluster_id)
        # Update vector clock in metadata (need to reconstruct due to immutability)
        coherence_metadata = CacheCoherenceMetadata(
            state=coherence_metadata.state,
            version=coherence_metadata.version,
            last_modified_time=coherence_metadata.last_modified_time,
            last_access_time=coherence_metadata.last_access_time,
            vector_clock=vector_clock,
            owning_cluster=coherence_metadata.owning_cluster,
            sharing_clusters=coherence_metadata.sharing_clusters,
            invalidation_pending=coherence_metadata.invalidation_pending,
            sync_token=coherence_metadata.sync_token,
            coherence_protocol=coherence_metadata.coherence_protocol,
        )

        return FederatedCacheEntry(
            cache_key=cache_key,
            cache_value=cache_value,
            coherence_metadata=coherence_metadata,
        )

    @pytest.mark.asyncio
    async def test_resolve_conflict_merkle_hash_comparison(self) -> None:
        """Test conflict resolution using merkle hash comparison."""
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.MERKLE_HASH_COMPARISON,
            merkle_verification_required=False,
        )

        resolver = CacheConflictResolver(strategy)

        # Create test entries
        local_entry = self.create_test_entry("local-cluster", {"data": "local"}, 1)
        remote_entry = self.create_test_entry("remote-cluster", {"data": "remote"}, 2)

        context = ConflictResolutionContext(
            local_cluster="local-cluster", remote_cluster="remote-cluster"
        )

        # Resolve conflict
        result = await resolver.resolve_conflict(local_entry, remote_entry, context)

        assert isinstance(result, ConflictResolutionResult)
        assert (
            result.resolution_strategy == ConflictResolutionType.MERKLE_HASH_COMPARISON
        )
        assert result.winning_cluster in ["local-cluster", "remote-cluster"]
        assert result.resolution_timestamp > 0

    @pytest.mark.asyncio
    async def test_resolve_conflict_leader_election(self) -> None:
        """Test conflict resolution using leader election."""
        leader_preferences = {"local-cluster": 2.0, "remote-cluster": 1.0}

        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.LEADER_ELECTION,
            leader_preference=leader_preferences,
            merkle_verification_required=False,
        )

        resolver = CacheConflictResolver(strategy)

        # Create test entries
        local_entry = self.create_test_entry("local-cluster", {"data": "local"})
        remote_entry = self.create_test_entry("remote-cluster", {"data": "remote"})

        context = ConflictResolutionContext(
            local_cluster="local-cluster", remote_cluster="remote-cluster"
        )

        # Resolve conflict - local should win due to higher leader preference
        result = await resolver.resolve_conflict(local_entry, remote_entry, context)

        assert result.resolution_strategy == ConflictResolutionType.LEADER_ELECTION
        assert result.winning_cluster == "local-cluster"  # Higher leader weight
        assert "leader_weight" in result.resolution_metadata

    @pytest.mark.asyncio
    async def test_resolve_conflict_last_writer_wins(self) -> None:
        """Test conflict resolution using last writer wins."""
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.LAST_WRITER_WINS,
            merkle_verification_required=False,
        )

        resolver = CacheConflictResolver(strategy)

        # Create entries with different vector clock increments
        local_entry = self.create_test_entry("local-cluster", {"data": "local"}, 1)
        remote_entry = self.create_test_entry("remote-cluster", {"data": "remote"}, 3)

        context = ConflictResolutionContext(
            local_cluster="local-cluster", remote_cluster="remote-cluster"
        )

        # Resolve conflict - remote should win due to higher vector clock
        result = await resolver.resolve_conflict(local_entry, remote_entry, context)

        assert result.resolution_strategy == ConflictResolutionType.LAST_WRITER_WINS
        assert result.winning_cluster == "remote-cluster"  # Higher vector clock

    @pytest.mark.asyncio
    async def test_resolve_conflict_quorum_consensus(self) -> None:
        """Test conflict resolution using quorum consensus."""
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.QUORUM_CONSENSUS,
            merkle_verification_required=False,
        )

        resolver = CacheConflictResolver(strategy)

        local_entry = self.create_test_entry("local-cluster", {"data": "local"})
        remote_entry = self.create_test_entry("remote-cluster", {"data": "remote"})

        context = ConflictResolutionContext(
            local_cluster="local-cluster",
            remote_cluster="remote-cluster",
            available_clusters=frozenset(["cluster-a", "cluster-b", "cluster-c"]),
        )

        # Resolve conflict
        result = await resolver.resolve_conflict(local_entry, remote_entry, context)

        assert result.resolution_strategy == ConflictResolutionType.QUORUM_CONSENSUS
        assert "quorum_size" in result.resolution_metadata

    @pytest.mark.asyncio
    async def test_resolve_conflict_merkle_tree_merge(self) -> None:
        """Test conflict resolution using merkle tree merge."""
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.MERKLE_TREE_MERGE,
            merkle_verification_required=False,
        )

        resolver = CacheConflictResolver(strategy)

        local_entry = self.create_test_entry("local-cluster", {"data": "local"}, 2)
        remote_entry = self.create_test_entry("remote-cluster", {"data": "remote"}, 1)

        context = ConflictResolutionContext(
            local_cluster="local-cluster", remote_cluster="remote-cluster"
        )

        # Resolve conflict
        result = await resolver.resolve_conflict(local_entry, remote_entry, context)

        assert result.resolution_strategy == ConflictResolutionType.MERKLE_TREE_MERGE
        assert result.winning_cluster == "local-cluster"  # Higher vector clock events
        assert "merge_strategy" in result.resolution_metadata

    @pytest.mark.asyncio
    async def test_resolve_conflict_application_specific(self) -> None:
        """Test conflict resolution using application-specific logic."""

        def always_prefer_local(local_entry, remote_entry):
            return local_entry

        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.APPLICATION_SPECIFIC,
            application_resolver=always_prefer_local,
            merkle_verification_required=False,
        )

        resolver = CacheConflictResolver(strategy)

        local_entry = self.create_test_entry("local-cluster", {"data": "local"})
        remote_entry = self.create_test_entry("remote-cluster", {"data": "remote"})

        context = ConflictResolutionContext(
            local_cluster="local-cluster", remote_cluster="remote-cluster"
        )

        # Resolve conflict
        result = await resolver.resolve_conflict(local_entry, remote_entry, context)

        assert result.resolution_strategy == ConflictResolutionType.APPLICATION_SPECIFIC
        assert (
            result.winning_cluster == "local-cluster"
        )  # Application logic always prefers local
        assert result.resolved_entry == local_entry

    @pytest.mark.asyncio
    async def test_resolve_conflict_invalid_strategy(self) -> None:
        """Test handling of invalid conflict resolution strategy."""
        # Mock an invalid strategy type
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.MERKLE_HASH_COMPARISON,
            merkle_verification_required=False,
        )

        resolver = CacheConflictResolver(strategy)

        # Temporarily change strategy type to invalid
        resolver.strategy = CacheConflictResolutionStrategy(
            strategy_type="invalid_strategy",  # type: ignore
            merkle_verification_required=False,
        )

        local_entry = self.create_test_entry("local-cluster", {"data": "local"})
        remote_entry = self.create_test_entry("remote-cluster", {"data": "remote"})

        context = ConflictResolutionContext(
            local_cluster="local-cluster", remote_cluster="remote-cluster"
        )

        # Should raise ValueError for unknown strategy
        with pytest.raises(ValueError, match="Unknown conflict resolution strategy"):
            await resolver.resolve_conflict(local_entry, remote_entry, context)


class TestConflictResolutionResult:
    """Test conflict resolution result."""

    def test_result_creation(self) -> None:
        """Test creating conflict resolution result."""
        # Create test cache entry
        cache_key = FederatedCacheKey.create("result", "res:123", "result-cluster")
        coherence_metadata = CacheCoherenceMetadata.create_initial("result-cluster")
        resolved_entry = FederatedCacheEntry(
            cache_key=cache_key,
            cache_value={"resolved": True},
            coherence_metadata=coherence_metadata,
        )

        result = ConflictResolutionResult(
            resolved_entry=resolved_entry,
            resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
            winning_cluster="result-cluster",
            resolution_metadata={"method": "test"},
        )

        assert result.resolved_entry == resolved_entry
        assert (
            result.resolution_strategy == ConflictResolutionType.MERKLE_HASH_COMPARISON
        )
        assert result.winning_cluster == "result-cluster"
        assert result.resolution_metadata == {"method": "test"}
        assert result.resolution_timestamp > 0

    def test_result_validation(self) -> None:
        """Test result validation."""
        cache_key = FederatedCacheKey.create("validation", "val:456", "val-cluster")
        coherence_metadata = CacheCoherenceMetadata.create_initial("val-cluster")
        resolved_entry = FederatedCacheEntry(
            cache_key=cache_key,
            cache_value={"valid": True},
            coherence_metadata=coherence_metadata,
        )

        # Invalid timestamp should raise error
        with pytest.raises(ValueError, match="Resolution timestamp must be positive"):
            ConflictResolutionResult(
                resolved_entry=resolved_entry,
                resolution_strategy=ConflictResolutionType.LAST_WRITER_WINS,
                winning_cluster="val-cluster",
                resolution_timestamp=-1.0,
            )


class TestLeaderElectionMetrics:
    """Test leader election metrics functionality."""

    def test_metrics_creation(self) -> None:
        """Test creating leader election metrics."""
        metrics = LeaderElectionMetrics(
            cluster_id="test-cluster",
            cpu_usage=0.3,
            memory_usage=0.4,
            cache_hit_rate=0.85,
            active_connections=150,
            network_latency_ms=25.0,
        )

        assert metrics.cluster_id == "test-cluster"
        assert metrics.cpu_usage == 0.3
        assert metrics.memory_usage == 0.4
        assert metrics.cache_hit_rate == 0.85
        assert metrics.active_connections == 150
        assert metrics.network_latency_ms == 25.0

    def test_metrics_validation(self) -> None:
        """Test metrics validation."""
        # Empty cluster ID
        with pytest.raises(ValueError, match="Cluster ID cannot be empty"):
            LeaderElectionMetrics(cluster_id="")

        # Invalid CPU usage
        with pytest.raises(ValueError, match="CPU usage must be between 0.0 and 1.0"):
            LeaderElectionMetrics(cluster_id="test", cpu_usage=1.5)

        # Invalid memory usage
        with pytest.raises(
            ValueError, match="Memory usage must be between 0.0 and 1.0"
        ):
            LeaderElectionMetrics(cluster_id="test", memory_usage=-0.1)

        # Invalid cache hit rate
        with pytest.raises(
            ValueError, match="Cache hit rate must be between 0.0 and 1.0"
        ):
            LeaderElectionMetrics(cluster_id="test", cache_hit_rate=1.1)

        # Invalid active connections
        with pytest.raises(ValueError, match="Active connections cannot be negative"):
            LeaderElectionMetrics(cluster_id="test", active_connections=-10)

        # Invalid network latency
        with pytest.raises(ValueError, match="Network latency cannot be negative"):
            LeaderElectionMetrics(cluster_id="test", network_latency_ms=-5.0)

    def test_fitness_score_calculation(self) -> None:
        """Test fitness score calculation."""
        # Good metrics should get high score
        good_metrics = LeaderElectionMetrics(
            cluster_id="good-cluster",
            cpu_usage=0.1,  # Low CPU usage
            memory_usage=0.2,  # Low memory usage
            cache_hit_rate=0.95,  # High hit rate
            active_connections=500,  # Reasonable connections
            network_latency_ms=10.0,  # Low latency
        )

        good_score = good_metrics.fitness_score()
        assert 80.0 <= good_score <= 100.0  # Should be high score

        # Bad metrics should get low score
        bad_metrics = LeaderElectionMetrics(
            cluster_id="bad-cluster",
            cpu_usage=0.9,  # High CPU usage
            memory_usage=0.8,  # High memory usage
            cache_hit_rate=0.3,  # Low hit rate
            active_connections=50,  # Few connections
            network_latency_ms=500.0,  # High latency
        )

        bad_score = bad_metrics.fitness_score()
        assert bad_score < good_score  # Should be lower than good
        assert 0.0 <= bad_score <= 50.0  # Should be low score


class TestMetricBasedLeaderElection:
    """Test metric-based leader election implementation."""

    def test_election_creation(self) -> None:
        """Test creating metric-based leader election."""
        election = MetricBasedLeaderElection(cluster_id="test-cluster")

        assert election.cluster_id == "test-cluster"
        assert len(election.cluster_metrics) == 0
        assert len(election.namespace_leaders) == 0

    def test_election_validation(self) -> None:
        """Test election validation."""
        with pytest.raises(ValueError, match="Cluster ID cannot be empty"):
            MetricBasedLeaderElection(cluster_id="")

    @pytest.mark.asyncio
    async def test_elect_leader_no_metrics(self) -> None:
        """Test electing leader with no metrics."""
        election = MetricBasedLeaderElection(cluster_id="solo-cluster")

        # Should elect self when no other metrics
        leader = await election.elect_leader("test_namespace")

        assert leader == "solo-cluster"
        assert await election.is_leader("test_namespace") is True

    @pytest.mark.asyncio
    async def test_elect_leader_with_metrics(self) -> None:
        """Test electing leader with cluster metrics."""
        election = MetricBasedLeaderElection(cluster_id="primary-cluster")

        # Add metrics for multiple clusters
        good_metrics = LeaderElectionMetrics(
            cluster_id="good-cluster",
            cpu_usage=0.1,
            memory_usage=0.2,
            cache_hit_rate=0.95,
            active_connections=500,
        )

        bad_metrics = LeaderElectionMetrics(
            cluster_id="bad-cluster",
            cpu_usage=0.9,
            memory_usage=0.8,
            cache_hit_rate=0.3,
            active_connections=50,
        )

        election.update_metrics("good-cluster", good_metrics)
        election.update_metrics("bad-cluster", bad_metrics)

        # Should elect cluster with best metrics
        leader = await election.elect_leader("production")

        assert leader == "good-cluster"
        assert await election.is_leader("production") is False

    @pytest.mark.asyncio
    async def test_get_current_leader(self) -> None:
        """Test getting current leader."""
        election = MetricBasedLeaderElection(cluster_id="test-cluster")

        # No leader initially
        assert await election.get_current_leader("test") is None

        # Elect leader
        await election.elect_leader("test")

        # Should return elected leader
        current_leader = await election.get_current_leader("test")
        assert current_leader == "test-cluster"

    @pytest.mark.asyncio
    async def test_step_down(self) -> None:
        """Test stepping down as leader."""
        election = MetricBasedLeaderElection(cluster_id="leader-cluster")

        # Elect self as leader
        await election.elect_leader("test_namespace")
        assert await election.is_leader("test_namespace") is True

        # Step down
        await election.step_down("test_namespace")

        # Should no longer be leader
        assert await election.get_current_leader("test_namespace") is None


class TestCacheNamespaceLeader:
    """Test cache namespace leader coordination."""

    def test_leader_creation_with_election(self) -> None:
        """Test creating cache namespace leader with election implementation."""
        election_impl = MetricBasedLeaderElection(cluster_id="test-cluster")
        leader = CacheNamespaceLeader(
            cluster_id="test-cluster", leader_election=election_impl
        )

        assert leader.cluster_id == "test-cluster"
        assert leader.leader_election == election_impl

    def test_leader_validation(self) -> None:
        """Test leader validation."""
        # Empty cluster ID
        with pytest.raises(ValueError, match="Cluster ID cannot be empty"):
            CacheNamespaceLeader(cluster_id="", leader_election=None)

        # No leader election implementation
        with pytest.raises(
            ValueError, match="Leader election implementation cannot be None"
        ):
            CacheNamespaceLeader(cluster_id="test-cluster", leader_election=None)

    @pytest.mark.asyncio
    async def test_elect_leader(self) -> None:
        """Test electing leader through coordinator."""
        election_impl = MetricBasedLeaderElection(cluster_id="test-cluster")
        leader = CacheNamespaceLeader(
            cluster_id="test-cluster", leader_election=election_impl
        )

        # Elect leader
        elected = await leader.elect_leader("test_namespace")

        assert elected == "test-cluster"

    @pytest.mark.asyncio
    async def test_get_leader(self) -> None:
        """Test getting leader through coordinator."""
        election_impl = MetricBasedLeaderElection(cluster_id="test-cluster")
        leader = CacheNamespaceLeader(
            cluster_id="test-cluster", leader_election=election_impl
        )

        # Get leader (should trigger election)
        current_leader = await leader.get_leader("test_namespace")

        assert current_leader == "test-cluster"

    @pytest.mark.asyncio
    async def test_is_leader(self) -> None:
        """Test checking if this node is leader."""
        election_impl = MetricBasedLeaderElection(cluster_id="test-cluster")
        leader = CacheNamespaceLeader(
            cluster_id="test-cluster", leader_election=election_impl
        )

        # Not leader initially
        assert await leader.is_leader("test_namespace") is False

        # Elect self as leader
        await leader.elect_leader("test_namespace")

        # Should now be leader
        assert await leader.is_leader("test_namespace") is True

    def test_update_cluster_metrics(self) -> None:
        """Test updating cluster metrics through coordinator."""
        election_impl = MetricBasedLeaderElection(cluster_id="test-cluster")
        leader = CacheNamespaceLeader(
            cluster_id="test-cluster", leader_election=election_impl
        )

        # Update metrics
        leader.update_cluster_metrics(
            cluster_id="remote-cluster",
            cpu_usage=0.4,
            memory_usage=0.6,
            cache_hit_rate=0.85,
            active_connections=150,
        )

        # Verify metrics were passed to election implementation
        assert "remote-cluster" in election_impl.cluster_metrics
        remote_metrics = election_impl.cluster_metrics["remote-cluster"]
        assert remote_metrics.cpu_usage == 0.4
        assert remote_metrics.memory_usage == 0.6
        assert remote_metrics.cache_hit_rate == 0.85
        assert remote_metrics.active_connections == 150

    @pytest.mark.asyncio
    async def test_step_down(self) -> None:
        """Test stepping down through coordinator."""
        election_impl = MetricBasedLeaderElection(cluster_id="test-cluster")
        leader = CacheNamespaceLeader(
            cluster_id="test-cluster", leader_election=election_impl
        )

        # Become leader
        await leader.elect_leader("test_namespace")
        assert await leader.is_leader("test_namespace") is True

        # Step down
        await leader.step_down("test_namespace")

        # Should no longer be leader
        assert await leader.is_leader("test_namespace") is False

    @pytest.mark.asyncio
    async def test_validation_errors(self) -> None:
        """Test validation errors in coordinator methods."""
        election_impl = MetricBasedLeaderElection(cluster_id="test-cluster")
        leader = CacheNamespaceLeader(
            cluster_id="test-cluster", leader_election=election_impl
        )

        # Empty namespace should raise errors
        with pytest.raises(ValueError, match="Namespace cannot be empty"):
            await leader.elect_leader("")

        with pytest.raises(ValueError, match="Namespace cannot be empty"):
            await leader.get_leader("")

        with pytest.raises(ValueError, match="Namespace cannot be empty"):
            await leader.is_leader("")

        with pytest.raises(ValueError, match="Namespace cannot be empty"):
            await leader.step_down("")

        with pytest.raises(ValueError, match="Namespace cannot be empty"):
            leader.force_leader_election("")


class TestCacheCorruptionError:
    """Test cache corruption error handling."""

    def test_corruption_error_basic(self) -> None:
        """Test basic cache corruption error."""
        error = CacheCorruptionError("Data integrity check failed")

        assert str(error) == "Data integrity check failed"
        assert error.entry_key is None

    def test_corruption_error_with_entry_key(self) -> None:
        """Test cache corruption error with entry key."""
        error = CacheCorruptionError(
            "Merkle proof verification failed", entry_key="users:profile:123"
        )

        assert str(error) == "Merkle proof verification failed"
        assert error.entry_key == "users:profile:123"


# Property-based testing for advanced cache coherence


class TestAdvancedCacheCoherencePropertyBased:
    """Property-based tests for advanced cache coherence."""

    @given(merkle_aware_cache_key_strategy())
    @settings(max_examples=50, deadline=2000)
    def test_merkle_aware_key_serialization_roundtrip(
        self, cache_key: MerkleAwareFederatedCacheKey
    ) -> None:
        """Test that merkle-aware cache keys can roundtrip through serialization."""
        try:
            # Serialize to dict
            data_dict = cache_key.to_dict()

            # Deserialize from dict
            restored_key = MerkleAwareFederatedCacheKey.from_dict(data_dict)

            # Verify key properties are preserved
            assert (
                restored_key.base_key.namespace.name
                == cache_key.base_key.namespace.name
            )
            assert restored_key.base_key.key == cache_key.base_key.key
            assert restored_key.cluster_id == cache_key.cluster_id
            assert restored_key.region == cache_key.region
            assert restored_key.partition_id == cache_key.partition_id
            assert restored_key.merkle_hash == cache_key.merkle_hash
            assert restored_key.data_checksum == cache_key.data_checksum
            assert restored_key.tree_version == cache_key.tree_version
            assert restored_key.merkle_proof == cache_key.merkle_proof

        except Exception as e:
            pytest.fail(f"Merkle-aware key serialization failed: {e}")

    @given(conflict_resolution_strategy_strategy())
    @settings(max_examples=30, deadline=2000)
    def test_conflict_resolution_strategy_properties(
        self, strategy: CacheConflictResolutionStrategy
    ) -> None:
        """Test properties of conflict resolution strategies."""
        try:
            # Verify strategy type is valid
            assert isinstance(strategy.strategy_type, ConflictResolutionType)

            # Verify timeout is positive
            assert strategy.timeout_seconds > 0

            # Verify leader preferences have valid weights
            for cluster_id, weight in strategy.leader_preference.items():
                assert isinstance(cluster_id, str)
                assert isinstance(weight, int | float)
                assert weight >= 0

            # Verify merkle verification flag is boolean
            assert isinstance(strategy.merkle_verification_required, bool)

        except Exception as e:
            pytest.fail(f"Conflict resolution strategy validation failed: {e}")

    @given(conflict_resolution_context_strategy())
    @settings(max_examples=30, deadline=2000)
    def test_conflict_resolution_context_properties(
        self, context: ConflictResolutionContext
    ) -> None:
        """Test properties of conflict resolution contexts."""
        try:
            # Verify cluster IDs are non-empty strings
            assert isinstance(context.local_cluster, str)
            assert len(context.local_cluster) > 0
            assert isinstance(context.remote_cluster, str)
            assert len(context.remote_cluster) > 0

            # Verify timestamp is positive
            assert context.conflict_timestamp > 0

            # Verify operation ID is non-empty
            assert isinstance(context.operation_id, str)
            assert len(context.operation_id) > 0

            # Verify available clusters is a frozenset
            assert isinstance(context.available_clusters, frozenset)

        except Exception as e:
            pytest.fail(f"Conflict resolution context validation failed: {e}")


# Integration test for complete advanced workflow
class TestAdvancedCacheCoherenceIntegration:
    """Integration tests for complete advanced cache coherence workflows."""

    @pytest.mark.asyncio
    async def test_complete_merkle_aware_conflict_resolution_workflow(self) -> None:
        """Test complete workflow with merkle-aware keys and conflict resolution."""
        # Create test data
        user_profile_v1 = {
            "user_id": "123",
            "name": "Alice",
            "email": "alice@example.com",
        }
        user_profile_v2 = {
            "user_id": "123",
            "name": "Alice",
            "email": "alice@newdomain.com",
        }

        # Create merkle trees for integrity verification
        local_tree = MerkleTree.empty()
        remote_tree = MerkleTree.empty()

        # Create merkle-aware cache keys with different data
        local_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="users",
            key="profile:123",
            cluster_id="cluster-west",
            cache_value=user_profile_v1,
            merkle_tree=local_tree,
        )

        remote_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="users",
            key="profile:123",
            cluster_id="cluster-east",
            cache_value=user_profile_v2,
            merkle_tree=remote_tree,
        )

        # Create cache entries with coherence metadata
        local_coherence = CacheCoherenceMetadata.create_initial("cluster-west")
        remote_coherence = CacheCoherenceMetadata.create_initial("cluster-east")

        local_entry = FederatedCacheEntry(
            cache_key=local_key,
            cache_value=user_profile_v1,
            coherence_metadata=local_coherence,
        )

        remote_entry = FederatedCacheEntry(
            cache_key=remote_key,
            cache_value=user_profile_v2,
            coherence_metadata=remote_coherence,
        )

        # Set up conflict resolution with merkle verification
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.MERKLE_HASH_COMPARISON,
            merkle_verification_required=True,
            timeout_seconds=60.0,
        )

        resolver = CacheConflictResolver(strategy)

        context = ConflictResolutionContext(
            local_cluster="cluster-west",
            remote_cluster="cluster-east",
            available_clusters=frozenset(
                ["cluster-west", "cluster-east", "cluster-central"]
            ),
        )

        # Resolve the conflict
        result = await resolver.resolve_conflict(local_entry, remote_entry, context)

        # Verify resolution completed successfully
        assert isinstance(result, ConflictResolutionResult)
        assert (
            result.resolution_strategy == ConflictResolutionType.MERKLE_HASH_COMPARISON
        )
        assert result.winning_cluster in ["cluster-west", "cluster-east"]
        assert result.resolved_entry in [local_entry, remote_entry]
        assert result.resolution_timestamp > 0

        # Verify the winning entry has merkle-aware key
        assert isinstance(result.resolved_entry.cache_key, MerkleAwareFederatedCacheKey)
        assert result.resolved_entry.cache_key.merkle_hash != ""
        assert result.resolved_entry.cache_key.data_checksum != ""

        # Verify resolution metadata contains reasoning
        assert isinstance(result.resolution_metadata, dict)
        assert "reason" in result.resolution_metadata

    @pytest.mark.asyncio
    async def test_advanced_leader_election_workflow(self) -> None:
        """Test advanced workflow with leader election and merkle verification."""
        # Create test cache value
        product_data = {
            "product_id": "ABC123",
            "name": "Premium Widget",
            "price": 99.99,
            "inventory": 50,
        }

        # Create merkle tree for integrity
        merkle_tree = MerkleTree.empty()

        # Create merkle-aware cache key
        cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="products",
            key="product:ABC123",
            cluster_id="primary-cluster",
            cache_value=product_data,
            merkle_tree=merkle_tree,
        )

        # Create coherence metadata
        coherence_metadata = CacheCoherenceMetadata.create_initial("primary-cluster")

        # Create cache entry
        cache_entry = FederatedCacheEntry(
            cache_key=cache_key,
            cache_value=product_data,
            coherence_metadata=coherence_metadata,
        )

        # Set up leader election strategy
        leader_preferences = {
            "primary-cluster": 3.0,
            "secondary-cluster": 2.0,
            "backup-cluster": 1.0,
        }

        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.LEADER_ELECTION,
            leader_preference=leader_preferences,
            merkle_verification_required=True,
            timeout_seconds=30.0,
        )

        resolver = CacheConflictResolver(strategy)

        # Verify strategy configuration
        assert resolver.strategy.strategy_type == ConflictResolutionType.LEADER_ELECTION
        assert resolver.strategy.leader_preference == leader_preferences
        assert resolver.strategy.merkle_verification_required is True

        # Verify cache entry properties
        assert isinstance(cache_entry.cache_key, MerkleAwareFederatedCacheKey)
        assert cache_entry.cache_key.merkle_hash != ""
        assert cache_entry.cache_value == product_data
        assert cache_entry.coherence_metadata.owning_cluster == "primary-cluster"
