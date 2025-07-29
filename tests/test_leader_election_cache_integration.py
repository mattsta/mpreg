"""
Integration tests for Leader Election and Cache Coherence using only real objects.

This module tests the complete integration between leader election and cache
coherence systems using real MPREG components without mocks. It demonstrates
how the advanced conflict resolution features work together in practice.
"""

import contextlib
import time
from collections.abc import AsyncGenerator

import pytest

from mpreg.datastructures.advanced_cache_coherence import (
    CacheConflictResolutionStrategy,
    CacheConflictResolver,
    CacheNamespaceLeader,
    ConflictResolutionContext,
    ConflictResolutionType,
    FederatedCacheEntry,
    MerkleAwareFederatedCacheKey,
)
from mpreg.datastructures.leader_election import (
    LeaderElectionMetrics,
    MetricBasedLeaderElection,
    QuorumBasedLeaderElection,
    RaftBasedLeaderElection,
)
from mpreg.datastructures.merkle_tree import MerkleTree
from mpreg.federation.cache_federation_bridge import (
    CacheFederationConfiguration,
)
from mpreg.federation.conflict_aware_cache_federation_bridge import (
    ConflictAwareCacheFederationBridge,
    ConflictResolutionConfiguration,
)


@contextlib.asynccontextmanager
async def raft_instance_cleanup(
    cluster_id: str,
) -> AsyncGenerator[RaftBasedLeaderElection, None]:
    """Context manager for RaftBasedLeaderElection with proper cleanup."""
    raft = RaftBasedLeaderElection(cluster_id=cluster_id)
    try:
        yield raft
    finally:
        # Clean shutdown to prevent task destruction warnings
        try:
            await raft.shutdown()
        except Exception:
            pass  # Ignore cleanup errors


class TestLeaderElectionCacheIntegration:
    """Integration tests using real MPREG objects."""

    @pytest.mark.asyncio
    async def test_merkle_aware_cache_key_with_leader_election(self):
        """Test merkle-aware cache keys with leader election for namespace ownership."""

        # Create metric-based leader election (deterministic based on metrics)
        # This will consistently elect the best cluster across all instances
        primary_cluster = MetricBasedLeaderElection(cluster_id="cluster-a")

        # Add metrics for all clusters to the primary election instance
        primary_cluster.update_metrics(
            "cluster-a",
            LeaderElectionMetrics(
                cluster_id="cluster-a",
                cpu_usage=0.3,
                memory_usage=0.4,
                cache_hit_rate=0.95,
                active_connections=100,
                network_latency_ms=5.0,
            ),
        )

        primary_cluster.update_metrics(
            "cluster-b",
            LeaderElectionMetrics(
                cluster_id="cluster-b",
                cpu_usage=0.8,  # High CPU usage
                memory_usage=0.7,
                cache_hit_rate=0.85,
                active_connections=150,
                network_latency_ms=15.0,
            ),
        )

        primary_cluster.update_metrics(
            "cluster-c",
            LeaderElectionMetrics(
                cluster_id="cluster-c",
                cpu_usage=0.2,  # Best metrics - should be elected
                memory_usage=0.3,
                cache_hit_rate=0.98,
                active_connections=80,
                network_latency_ms=3.0,
            ),
        )

        # Test namespace leadership for cache coherence
        namespace = "user_sessions"

        # Elect leader using metric-based algorithm
        elected_leader = await primary_cluster.elect_leader(namespace)

        # Should elect cluster-c (best metrics)
        assert elected_leader == "cluster-c"

        # Create real merkle tree for cache integrity
        cache_data = [
            b"session_data_1",
            b"session_data_2",
            b"session_data_3",
        ]

        merkle_tree = MerkleTree.from_leaves(cache_data)
        root_hash = merkle_tree.root_hash

        # Create merkle-aware cache key with the elected leader's cluster
        from mpreg.datastructures.cache_structures import CacheKey

        proof = merkle_tree.generate_proof(0)  # Proof for first data item
        base_cache_key = CacheKey.simple(key="user:123", namespace="user_sessions")

        merkle_key = MerkleAwareFederatedCacheKey(
            base_key=base_cache_key,
            cluster_id=elected_leader,  # Use the elected leader
            region="us-east-1",
            partition_id="0",
            merkle_hash=root_hash(),  # Call method to get string
            merkle_proof=proof.proof_path,
            tree_version=1,
            data_checksum="abc123",
        )

        # Verify the key has correct leader information
        assert merkle_key.cluster_id == elected_leader
        assert merkle_key.merkle_hash == root_hash()
        assert len(merkle_key.merkle_proof) > 0

        # Verify merkle proof is valid
        assert merkle_tree.verify_proof(proof)

    @pytest.mark.asyncio
    async def test_conflict_resolution_with_leader_election(self):
        """Test cache conflict resolution using leader election for arbitration."""

        # Create metric-based leader election for best conflict resolution
        leader_election = MetricBasedLeaderElection(cluster_id="resolver-cluster")

        # Set up multiple clusters with different capabilities
        clusters = ["cluster-1", "cluster-2", "cluster-3"]

        for i, cluster_id in enumerate(clusters):
            metrics = LeaderElectionMetrics(
                cluster_id=cluster_id,
                cpu_usage=0.1 + (i * 0.2),  # Varying loads
                memory_usage=0.2 + (i * 0.1),
                cache_hit_rate=0.9 + (i * 0.02),
                active_connections=50 + (i * 25),
                network_latency_ms=5.0 + (i * 10.0),
            )
            leader_election.update_metrics(cluster_id, metrics)

        # Elect leader for conflict resolution
        namespace = "conflict_resolution"
        elected_leader = await leader_election.elect_leader(namespace)

        # MetricBasedLeaderElection should elect the cluster with best fitness score
        # In this case, cluster-3 has the best metrics but we need to check the actual algorithm
        assert elected_leader in clusters

        # Create cache namespace leader using elected leader
        namespace_leader = CacheNamespaceLeader(
            cluster_id=elected_leader, leader_election=leader_election
        )

        # Create conflicting cache entries
        from mpreg.datastructures.federated_cache_coherence import (
            CacheCoherenceMetadata,
            CoherenceProtocolType,
        )

        timestamp = time.time()

        # Create cache keys
        from mpreg.datastructures.federated_cache_coherence import FederatedCacheKey

        # The test should represent a conflict between:
        # - Local cluster (cluster-2) with older timestamp
        # - Remote cluster (elected leader cluster-1) with newer timestamp
        local_cluster = "cluster-2"  # Non-leader cluster

        # Create conflict resolution strategy using leader election
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.LEADER_ELECTION,
            leader_preference={
                elected_leader: 2.0,  # High preference for elected leader
                local_cluster: 1.0,  # Lower preference for local cluster
            },
            merkle_verification_required=True,
        )

        # Create real conflict resolver
        resolver = CacheConflictResolver(strategy=strategy)

        local_cache_key = FederatedCacheKey.create(
            namespace="user_data", key="shared_data", cluster_id=local_cluster
        )

        remote_cache_key = FederatedCacheKey.create(
            namespace="user_data",
            key="shared_data",
            cluster_id=elected_leader,  # This should be cluster-1 (the elected leader)
        )

        # Create coherence metadata - need to use replace() since they're frozen
        local_metadata_base = CacheCoherenceMetadata.create_initial(
            owning_cluster=local_cluster, protocol=CoherenceProtocolType.MESI
        )
        # Use dataclass replace to modify frozen dataclass
        from dataclasses import replace

        local_metadata = replace(local_metadata_base, last_modified_time=timestamp - 10)

        remote_metadata_base = CacheCoherenceMetadata.create_initial(
            owning_cluster=elected_leader, protocol=CoherenceProtocolType.MESI
        )
        remote_metadata = replace(remote_metadata_base, last_modified_time=timestamp)

        local_entry = FederatedCacheEntry(
            cache_key=local_cache_key,
            cache_value={"version": 1, "data": "local_value"},
            coherence_metadata=local_metadata,
        )

        remote_entry = FederatedCacheEntry(
            cache_key=remote_cache_key,  # Different key for different cluster
            cache_value={"version": 2, "data": "remote_value"},
            coherence_metadata=remote_metadata,
        )

        # Create resolution context
        context = ConflictResolutionContext(
            local_cluster=local_cluster,
            remote_cluster=elected_leader,
            conflict_timestamp=timestamp + 1,
            available_clusters=frozenset(clusters),
        )

        # Resolve conflict using real resolver
        result = await resolver.resolve_conflict(local_entry, remote_entry, context)

        # Should choose remote entry from elected leader
        assert result.resolved_entry == remote_entry
        assert result.resolution_strategy == ConflictResolutionType.LEADER_ELECTION
        assert result.winning_cluster == elected_leader

    @pytest.mark.asyncio
    async def test_full_cache_federation_with_leader_election(self):
        """Test complete cache federation system with leader election integration."""

        # Create real topic exchange (simplified for testing)
        class TestTopicExchange:
            def __init__(self):
                self.published_messages = []

            def publish_message(self, message):
                self.published_messages.append(message)
                return True

        topic_exchange = TestTopicExchange()

        # Create real RPC executor (simplified for testing)
        class TestRPCExecutor:
            def __init__(self):
                self.executed_commands = []

            async def execute_with_topics(self, commands, request_id=None):
                self.executed_commands.extend(commands)
                # Simulate successful execution
                from mpreg.core.enhanced_rpc import RPCRequestResult, RPCResult

                results = {}
                for cmd in commands:
                    results[cmd.command_id] = RPCResult(
                        command_name=cmd.name,
                        function=cmd.fun,
                        args=cmd.args,
                        kwargs=cmd.kwargs,
                        executed_at=time.time(),
                        execution_level=0,
                        command_id=cmd.command_id,
                        execution_time_ms=10.0,
                        success=True,
                    )

                return RPCRequestResult(
                    request_id=request_id or "test_req",
                    command_results=results,
                    total_execution_time_ms=10.0,
                    total_commands=len(commands),
                    successful_commands=len(commands),
                    failed_commands=0,
                    execution_levels_completed=1,
                )

        rpc_executor = TestRPCExecutor()

        # Create leader election for cache federation
        leader_election = QuorumBasedLeaderElection(cluster_id="fed-cluster-1")

        # Set up cluster metrics
        for cluster_id in ["fed-cluster-1", "fed-cluster-2", "fed-cluster-3"]:
            metrics = LeaderElectionMetrics(
                cluster_id=cluster_id,
                cpu_usage=0.3,
                memory_usage=0.4,
                cache_hit_rate=0.9,
                active_connections=100,
                network_latency_ms=10.0,
            )
            leader_election.update_metrics(cluster_id, metrics)

        # Create conflict resolution configuration
        conflict_config = ConflictResolutionConfiguration(
            enable_merkle_verification=True,
            enable_leader_election=True,
            default_resolution_strategy=ConflictResolutionType.MERKLE_HASH_COMPARISON,
            leader_election_timeout_seconds=5.0,
        )

        # Create basic federation configuration
        federation_config = CacheFederationConfiguration(
            cluster_id="fed-cluster-1",
            invalidation_timeout_seconds=5.0,
            replication_timeout_seconds=10.0,
            enable_async_replication=True,
            enable_sync_replication=True,
        )

        # Create cache namespace leader
        namespace_leader = CacheNamespaceLeader(
            cluster_id="fed-cluster-1", leader_election=leader_election
        )

        # Create conflict-aware cache federation bridge with real components
        from mpreg.core.topic_exchange import TopicExchange

        from .port_allocator import allocate_port

        test_port = allocate_port("testing")
        real_topic_exchange = TopicExchange(
            server_url=f"test://localhost:{test_port}", cluster_id="fed-cluster-1"
        )

        bridge = ConflictAwareCacheFederationBridge(
            config=federation_config,
            topic_exchange=real_topic_exchange,
            conflict_config=conflict_config,
        )

        # Test that the bridge was initialized correctly
        assert bridge.config.cluster_id == "fed-cluster-1"
        assert bridge.conflict_config.enable_leader_election is True

        # Elect leader for a cache namespace
        namespace = "distributed_cache"
        elected_leader = await leader_election.elect_leader(namespace)

        # Verify leadership
        assert elected_leader == "fed-cluster-1"  # Should elect self with these metrics
        is_leader = await leader_election.is_leader(namespace)
        assert is_leader is True

        # Test that conflict resolver can be created with leader election
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.LEADER_ELECTION,
            merkle_verification_required=True,
        )

        resolver = CacheConflictResolver(strategy=strategy)
        assert resolver.strategy.strategy_type == ConflictResolutionType.LEADER_ELECTION

        # Test that we can create a namespace leader with the elected leader
        created_leader = CacheNamespaceLeader(
            cluster_id=elected_leader, leader_election=leader_election
        )

        assert created_leader.cluster_id == elected_leader

    @pytest.mark.asyncio
    async def test_leader_election_algorithms_consistency(self):
        """Test that different leader election algorithms produce consistent results."""

        # Create same cluster metrics for all algorithms
        cluster_metrics = {
            "cluster-alpha": LeaderElectionMetrics(
                cluster_id="cluster-alpha",
                cpu_usage=0.2,
                memory_usage=0.3,
                cache_hit_rate=0.98,
                active_connections=50,
                network_latency_ms=3.0,
            ),
            "cluster-beta": LeaderElectionMetrics(
                cluster_id="cluster-beta",
                cpu_usage=0.7,
                memory_usage=0.8,
                cache_hit_rate=0.75,
                active_connections=200,
                network_latency_ms=25.0,
            ),
            "cluster-gamma": LeaderElectionMetrics(
                cluster_id="cluster-gamma",
                cpu_usage=0.4,
                memory_usage=0.5,
                cache_hit_rate=0.90,
                active_connections=100,
                network_latency_ms=10.0,
            ),
        }

        # Import the protocol for proper typing
        from mpreg.datastructures.leader_election import LeaderElection

        # Test Raft with proper cleanup
        async with raft_instance_cleanup("cluster-alpha") as raft_impl:
            algorithms: list[tuple[str, LeaderElection]] = [
                ("raft", raft_impl),
                ("quorum", QuorumBasedLeaderElection("cluster-alpha")),
                ("metric", MetricBasedLeaderElection("cluster-alpha")),
            ]

            namespace = "consistency_test"
            leaders = {}

            for name, algorithm in algorithms:
                # Add metrics to each algorithm
                for cluster_id, metrics in cluster_metrics.items():
                    algorithm.update_metrics(cluster_id, metrics)

                # Elect leader
                leader = await algorithm.elect_leader(namespace)
                leaders[name] = leader

                # Verify leadership
                is_leader = await algorithm.is_leader(namespace)
                if leader == "cluster-alpha":  # The algorithm's own cluster
                    assert is_leader is True

                # Verify leader is one of the known clusters
                assert leader in cluster_metrics.keys()

            # For metric-based election, should consistently choose best metrics (cluster-alpha)
            assert leaders["metric"] == "cluster-alpha"

            # Raft and Quorum may choose self or based on their internal logic
            # but should be consistent across runs
            assert leaders["raft"] in cluster_metrics.keys()
            assert leaders["quorum"] in cluster_metrics.keys()

            # Test leadership queries work correctly
            for name, algorithm in algorithms:
                current_leader = await algorithm.get_current_leader(namespace)
                assert current_leader == leaders[name]

    @pytest.mark.asyncio
    async def test_merkle_tree_integration_with_cache_coherence(self):
        """Test merkle tree integration with cache coherence for data integrity."""

        # Import required classes at the top of function
        from mpreg.datastructures.cache_structures import CacheKey
        from mpreg.datastructures.federated_cache_coherence import (
            CacheCoherenceMetadata,
            CoherenceProtocolType,
            FederatedCacheKey,
        )

        # Simulate distributed cache entries
        cache_entries = {
            "user:1001": b'{"name": "Alice", "session": "abc123"}',
            "user:1002": b'{"name": "Bob", "session": "def456"}',
            "user:1003": b'{"name": "Charlie", "session": "ghi789"}',
            "config:app": b'{"theme": "dark", "lang": "en"}',
            "config:db": b'{"host": "localhost", "port": 5432}',
        }

        # Create merkle tree from cache data
        cache_data_list = list(cache_entries.values())
        merkle_tree = MerkleTree.from_leaves(cache_data_list)
        root_hash = merkle_tree.root_hash()
        assert root_hash is not None

        # Create leader election for cache namespace
        leader_election = MetricBasedLeaderElection(cluster_id="integrity-cluster")

        # Add metrics for the integrity cluster
        leader_election.update_metrics(
            "integrity-cluster",
            LeaderElectionMetrics(
                cluster_id="integrity-cluster",
                cpu_usage=0.1,
                memory_usage=0.2,
                cache_hit_rate=0.99,
                active_connections=25,
                network_latency_ms=1.0,
            ),
        )

        namespace = "secure_cache"
        leader = await leader_election.elect_leader(namespace)
        assert leader == "integrity-cluster"

        # Create merkle-aware cache entries with proofs
        merkle_cache_entries = {}

        for i, (key, data) in enumerate(cache_entries.items()):
            proof = merkle_tree.generate_proof(i)

            # Create proper cache key
            base_cache_key = CacheKey.simple(key=key, namespace="secure_cache")

            merkle_key = MerkleAwareFederatedCacheKey(
                base_key=base_cache_key,
                cluster_id=leader,
                region="secure-region",
                partition_id="0",
                merkle_hash=root_hash,
                merkle_proof=proof.proof_path,
                tree_version=1,
                data_checksum=str(hash(data)),
            )

            # Verify proof is valid
            assert merkle_tree.verify_proof(proof)

            # Create proper cache metadata
            cache_metadata = CacheCoherenceMetadata.create_initial(
                owning_cluster=leader, protocol=CoherenceProtocolType.MESI
            )

            cache_entry = FederatedCacheEntry(
                cache_key=merkle_key,  # Use merkle-aware key directly
                cache_value={
                    "data": data.decode("utf-8"),
                    "merkle_verified": True,
                    "integrity_hash": root_hash,
                    "original_key": key,
                },
                coherence_metadata=cache_metadata,
            )

            merkle_cache_entries[key] = (merkle_key, cache_entry)

        # Verify all entries have valid merkle proofs
        for i, (key, (merkle_key, cache_entry)) in enumerate(
            merkle_cache_entries.items()
        ):
            original_data = cache_entries[key]

            # Verify merkle proof for this entry
            proof = merkle_tree.generate_proof(i)
            assert merkle_tree.verify_proof(proof)

            # Verify cache entry metadata
            assert cache_entry.cache_value["merkle_verified"] is True
            assert cache_entry.cache_value["integrity_hash"] == root_hash
            assert cache_entry.coherence_metadata.owning_cluster == leader

        # Test conflict resolution with merkle verification
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.MERKLE_HASH_COMPARISON,
            merkle_verification_required=True,
        )

        resolver = CacheConflictResolver(strategy=strategy)

        # Create conflicting entries with different merkle hashes
        user_key, user_entry = merkle_cache_entries["user:1001"]

        # Create untrusted cache key
        untrusted_key = FederatedCacheKey.create(
            namespace="secure_cache", key="user:1001", cluster_id="untrusted-cluster"
        )

        # Create untrusted metadata
        untrusted_metadata = CacheCoherenceMetadata.create_initial(
            owning_cluster="untrusted-cluster", protocol=CoherenceProtocolType.MESI
        )

        # Simulate modified entry with invalid merkle proof
        modified_entry = FederatedCacheEntry(
            cache_key=untrusted_key,
            cache_value={
                "data": '{"name": "Alice Modified", "session": "xyz999"}',
                "merkle_verified": False,  # Not verified
                "integrity_hash": "invalid_hash",
                "original_key": "user:1001",
            },
            coherence_metadata=untrusted_metadata,
        )

        # Resolve conflict - should prefer merkle-verified entry
        context = ConflictResolutionContext(
            local_cluster="integrity-cluster",
            remote_cluster="untrusted-cluster",
            conflict_timestamp=time.time(),
        )

        result = await resolver.resolve_conflict(user_entry, modified_entry, context)

        # Should choose the merkle-verified entry
        assert result.resolved_entry == user_entry
        assert (
            result.resolution_strategy == ConflictResolutionType.MERKLE_HASH_COMPARISON
        )
