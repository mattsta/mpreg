# Cache Federation Conflict Resolution & Merkle Tree Integration

## Current Implementation Gaps

**CRITICAL**: The current cache federation implementation has significant gaps in conflict resolution and does not leverage MPREG's existing merkle tree infrastructure. This document outlines the problems and proposed solutions.

## Problems with Current Design

### 1. No Conflict Resolution Mechanism

```python
# Current implementation - PROBLEMATIC:
# Two clusters modify same cache entry simultaneously

Cluster A: user:123 = {"name": "Alice", "email": "alice@new.com"}
Cluster B: user:123 = {"name": "Alice", "phone": "+1234567890"}

# Current system: Uses vector clocks for ordering, but...
# Vector clocks only tell us order of operations, not which DATA is correct
# Result: Undefined behavior, potential data loss
```

### 2. No Merkle Tree Integration

```python
# MPREG has merkle trees for data integrity, but cache federation ignores them
# Current cache federation:
cache_key = FederatedCacheKey.create("users", "123", "cluster-a")
# Missing: merkle_hash, integrity_proof, diff_detection

# Should be:
cache_key = FederatedCacheKey.create("users", "123", "cluster-a",
                                   merkle_hash="abc123...",
                                   integrity_proof=merkle_proof)
```

### 3. No Leader Election

```python
# Current: All clusters are equal peers
# Problem: No authoritative source for conflict resolution
# Should have: Leader election for each cache namespace or key range
```

## Proposed Solution: Merkle-Tree-Based Cache Federation

### 1. Enhanced FederatedCacheKey with Merkle Integration

```python
@dataclass(frozen=True, slots=True)
class MerkleAwareFederatedCacheKey:
    """Cache key with merkle tree integration for integrity verification."""

    base_key: CacheKey
    cluster_id: ClusterId
    region: CacheRegion = "default"
    partition_id: PartitionID = "0"

    # NEW: Merkle tree integration
    merkle_hash: MerkleHash = ""
    merkle_proof: MerkleProofPath = field(default_factory=list)
    tree_version: int = 1
    data_checksum: str = ""

    @classmethod
    def create_with_merkle(
        cls,
        namespace: str,
        key: str,
        cluster_id: ClusterId,
        cache_value: Any,
        merkle_tree: MerkleTree,
        **kwargs
    ) -> MerkleAwareFederatedCacheKey:
        """Create cache key with merkle tree verification."""

        # Serialize cache value for hashing
        serialized_data = json.dumps(cache_value, sort_keys=True).encode()
        data_checksum = hashlib.sha256(serialized_data).hexdigest()

        # Add to merkle tree and get proof
        leaf_index = merkle_tree.add_leaf(serialized_data)
        merkle_hash = merkle_tree.get_root_hash()
        merkle_proof = merkle_tree.generate_proof(leaf_index)

        base_key = CacheKey.create(namespace, key)

        return cls(
            base_key=base_key,
            cluster_id=cluster_id,
            merkle_hash=merkle_hash,
            merkle_proof=merkle_proof,
            tree_version=merkle_tree.version,
            data_checksum=data_checksum,
            **kwargs
        )

    def verify_integrity(self, cache_value: Any, merkle_tree: MerkleTree) -> bool:
        """Verify cache value integrity using merkle proof."""
        serialized_data = json.dumps(cache_value, sort_keys=True).encode()
        calculated_checksum = hashlib.sha256(serialized_data).hexdigest()

        # Check data hasn't been tampered with
        if calculated_checksum != self.data_checksum:
            return False

        # Verify merkle proof
        return merkle_tree.verify_proof(
            leaf_data=serialized_data,
            proof_path=self.merkle_proof,
            root_hash=self.merkle_hash
        )
```

### 2. Conflict Resolution Strategy

```python
@dataclass(frozen=True, slots=True)
class CacheConflictResolutionStrategy:
    """Strategy for resolving cache conflicts using multiple approaches."""

    strategy_type: ConflictResolutionType
    leader_preference: dict[str, float] = field(default_factory=dict)  # cluster -> weight
    merkle_verification_required: bool = True
    application_resolver: Callable | None = None

class ConflictResolutionType(Enum):
    MERKLE_HASH_COMPARISON = "merkle_hash"      # Use merkle hashes to detect diffs
    LEADER_ELECTION = "leader_election"         # Designated leader wins
    LAST_WRITER_WINS = "last_writer_wins"       # Vector clock ordering
    QUORUM_CONSENSUS = "quorum_consensus"       # Majority vote
    APPLICATION_SPECIFIC = "application"        # Custom conflict resolution
    MERKLE_TREE_MERGE = "merkle_merge"         # Intelligent merge using tree diffs

class CacheConflictResolver:
    """Resolves conflicts between federated cache entries."""

    def __init__(self, strategy: CacheConflictResolutionStrategy):
        self.strategy = strategy
        self.leader_election = LeaderElection() if strategy.strategy_type == ConflictResolutionType.LEADER_ELECTION else None

    async def resolve_conflict(self,
                              local_entry: CacheEntry,
                              remote_entry: CacheEntry,
                              context: ConflictResolutionContext) -> CacheEntry:
        """Resolve conflict between local and remote cache entries."""

        # First: Verify integrity using merkle proofs
        if self.strategy.merkle_verification_required:
            local_valid = await self._verify_merkle_integrity(local_entry)
            remote_valid = await self._verify_merkle_integrity(remote_entry)

            if not local_valid and remote_valid:
                return remote_entry  # Local is corrupted
            elif local_valid and not remote_valid:
                return local_entry   # Remote is corrupted
            elif not local_valid and not remote_valid:
                raise CacheCorruptionError("Both entries failed merkle verification")

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
            return await self._resolve_by_application(local_entry, remote_entry, context)

        else:
            raise ValueError(f"Unknown conflict resolution strategy: {self.strategy.strategy_type}")

    async def _resolve_by_merkle_hash(self, local: CacheEntry, remote: CacheEntry) -> CacheEntry:
        """Resolve by comparing merkle hashes - identical hashes mean identical data."""
        if local.cache_key.merkle_hash == remote.cache_key.merkle_hash:
            # Identical data - prefer local for performance
            return local

        # Different data - need deeper resolution
        # Check vector clocks for causality
        local_vc = local.coherence_metadata.vector_clock
        remote_vc = remote.coherence_metadata.vector_clock

        if local_vc.happens_before(remote_vc):
            return remote  # Remote is causally after local
        elif remote_vc.happens_before(local_vc):
            return local   # Local is causally after remote
        else:
            # Concurrent updates - use tree version as tiebreaker
            if remote.cache_key.tree_version > local.cache_key.tree_version:
                return remote
            else:
                return local

    async def _resolve_by_leader(self, local: CacheEntry, remote: CacheEntry, context: ConflictResolutionContext) -> CacheEntry:
        """Resolve using leader election - leader's decision wins."""

        # Get current leader for this cache namespace
        namespace = local.cache_key.base_key.namespace.name
        leader_cluster = await self.leader_election.get_leader(namespace)

        if leader_cluster == local.cache_key.cluster_id:
            return local   # Local cluster is leader
        elif leader_cluster == remote.cache_key.cluster_id:
            return remote  # Remote cluster is leader
        else:
            # Neither is leader - need to contact actual leader
            leader_entry = await context.fetch_from_leader(leader_cluster, local.cache_key)
            return leader_entry

    async def _resolve_by_merkle_merge(self, local: CacheEntry, remote: CacheEntry) -> CacheEntry:
        """Intelligent merge using merkle tree diffs."""

        # Get merkle trees for both entries
        local_tree = await self._reconstruct_merkle_tree(local)
        remote_tree = await self._reconstruct_merkle_tree(remote)

        # Find differences between trees
        diffs = local_tree.compute_diff(remote_tree)

        if not diffs:
            return local  # No differences

        # Attempt to merge non-conflicting changes
        merged_data = await self._merge_cache_data(
            local.cache_value,
            remote.cache_value,
            diffs
        )

        # Create new cache entry with merged data
        merged_key = await self._create_merged_cache_key(
            local.cache_key,
            remote.cache_key,
            merged_data
        )

        return CacheEntry(
            cache_key=merged_key,
            cache_value=merged_data,
            coherence_metadata=self._merge_coherence_metadata(local, remote)
        )
```

### 3. Leader Election for Cache Namespaces

```python
class CacheNamespaceLeader:
    """Leader election for cache namespace coordination."""

    def __init__(self, cluster_id: str, federation_bridge: CacheFederationBridge):
        self.cluster_id = cluster_id
        self.federation_bridge = federation_bridge
        self.leaders: dict[str, str] = {}  # namespace -> leader_cluster
        self.leader_election_in_progress: set[str] = set()

    async def elect_leader(self, namespace: str) -> str:
        """Elect leader for cache namespace using modified Raft algorithm."""

        if namespace in self.leader_election_in_progress:
            # Election already in progress, wait for result
            return await self._wait_for_election_result(namespace)

        self.leader_election_in_progress.add(namespace)

        try:
            # Get all clusters in federation
            all_clusters = await self._discover_federation_clusters()

            # Use deterministic leader selection based on:
            # 1. Cluster load (lower is better)
            # 2. Network latency (lower is better)
            # 3. Lexicographic ordering (for tie-breaking)

            cluster_scores = {}
            for cluster_id in all_clusters:
                cluster_stats = self.federation_bridge.get_cluster_statistics(cluster_id)
                load_score = self._calculate_load_score(cluster_stats)
                latency_score = await self._measure_network_latency(cluster_id)

                cluster_scores[cluster_id] = {
                    'load': load_score,
                    'latency': latency_score,
                    'total_score': load_score + latency_score
                }

            # Select cluster with lowest total score
            leader_cluster = min(cluster_scores.keys(),
                               key=lambda c: (cluster_scores[c]['total_score'], c))

            # Broadcast leader election result
            await self._broadcast_leader_decision(namespace, leader_cluster)

            self.leaders[namespace] = leader_cluster
            return leader_cluster

        finally:
            self.leader_election_in_progress.discard(namespace)

    async def get_leader(self, namespace: str) -> str:
        """Get current leader for namespace, electing one if needed."""
        if namespace not in self.leaders:
            return await self.elect_leader(namespace)

        # Verify leader is still alive
        leader_cluster = self.leaders[namespace]
        if not await self._verify_leader_health(leader_cluster):
            # Leader is down, elect new one
            return await self.elect_leader(namespace)

        return leader_cluster
```

### 4. Enhanced Cache Federation Bridge with Conflict Resolution

```python
class ConflictAwareCacheFederationBridge(CacheFederationBridge):
    """Enhanced cache federation bridge with conflict resolution."""

    def __init__(self,
                 config: CacheFederationConfiguration,
                 topic_exchange: TopicExchange,
                 conflict_resolver: CacheConflictResolver):
        super().__init__(config, topic_exchange)
        self.conflict_resolver = conflict_resolver
        self.merkle_trees: dict[str, MerkleTree] = {}  # namespace -> merkle tree
        self.leader_election = CacheNamespaceLeader(config.cluster_id, self)

    async def handle_cache_conflict(self,
                                  local_entry: CacheEntry,
                                  remote_entry: CacheEntry) -> CacheEntry:
        """Handle conflict between local and remote cache entries."""

        context = ConflictResolutionContext(
            federation_bridge=self,
            local_cluster=self.local_cluster_id,
            remote_cluster=remote_entry.cache_key.cluster_id
        )

        resolved_entry = await self.conflict_resolver.resolve_conflict(
            local_entry, remote_entry, context
        )

        # Update local cache with resolved entry
        await self._update_local_cache(resolved_entry)

        # Notify other clusters of resolution
        await self._broadcast_conflict_resolution(resolved_entry)

        return resolved_entry

    async def send_cache_invalidation_with_merkle(self,
                                                cache_key: MerkleAwareFederatedCacheKey,
                                                cache_value: Any,
                                                **kwargs) -> str:
        """Send invalidation with merkle tree verification."""

        # Update merkle tree
        namespace = cache_key.base_key.namespace.name
        merkle_tree = self._get_or_create_merkle_tree(namespace)

        # Create merkle-aware cache key
        enhanced_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace=namespace,
            key=cache_key.base_key.key,
            cluster_id=self.local_cluster_id,
            cache_value=cache_value,
            merkle_tree=merkle_tree
        )

        return await super().send_cache_invalidation(enhanced_key, **kwargs)
```

## Usage Examples with Conflict Resolution

### Example 1: E-Commerce Product Updates with Leader Election

```python
class ProductCatalogWithConflictResolution:
    def __init__(self, cluster_id: str, topic_exchange):
        # Set up conflict resolution strategy
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.LEADER_ELECTION,
            merkle_verification_required=True
        )

        conflict_resolver = CacheConflictResolver(strategy)

        config = CacheFederationConfiguration(cluster_id=cluster_id)
        self.cache_federation = ConflictAwareCacheFederationBridge(
            config=config,
            topic_exchange=topic_exchange,
            conflict_resolver=conflict_resolver
        )

    async def update_product_price(self, product_id: str, new_price: float):
        """Update product price with conflict resolution."""

        # Check if we're the leader for product catalog
        leader = await self.cache_federation.leader_election.get_leader("product_catalog")

        if leader != self.cache_federation.local_cluster_id:
            # We're not the leader - forward request to leader
            return await self._forward_to_leader(leader, "update_product_price",
                                               product_id, new_price)

        # We're the leader - proceed with update
        cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="product_catalog",
            key=f"product:{product_id}:price",
            cluster_id=self.cache_federation.local_cluster_id,
            cache_value=new_price,
            merkle_tree=self.cache_federation._get_or_create_merkle_tree("product_catalog")
        )

        await self.cache_federation.send_cache_invalidation_with_merkle(
            cache_key=cache_key,
            cache_value=new_price,
            reason=f"leader_price_update_{product_id}"
        )
```

### Example 2: User Profile Updates with Merkle Tree Merge

```python
class UserProfileWithMerkleResolution:
    def __init__(self, cluster_id: str, topic_exchange):
        # Use merkle tree merging for user profiles
        strategy = CacheConflictResolutionStrategy(
            strategy_type=ConflictResolutionType.MERKLE_TREE_MERGE,
            merkle_verification_required=True
        )

        conflict_resolver = CacheConflictResolver(strategy)

        config = CacheFederationConfiguration(cluster_id=cluster_id)
        self.cache_federation = ConflictAwareCacheFederationBridge(
            config=config,
            topic_exchange=topic_exchange,
            conflict_resolver=conflict_resolver
        )

    async def update_user_profile(self, user_id: str, profile_updates: dict):
        """Update user profile with intelligent conflict resolution."""

        # Get current profile
        current_profile = await self._get_local_profile(user_id)

        # Apply updates
        updated_profile = {**current_profile, **profile_updates}

        # Create merkle-aware cache key
        cache_key = MerkleAwareFederatedCacheKey.create_with_merkle(
            namespace="user_profiles",
            key=f"user:{user_id}",
            cluster_id=self.cache_federation.local_cluster_id,
            cache_value=updated_profile,
            merkle_tree=self.cache_federation._get_or_create_merkle_tree("user_profiles")
        )

        # If there's a concurrent update from another cluster, the merkle merge
        # strategy will intelligently combine non-conflicting changes
        await self.cache_federation.send_cache_invalidation_with_merkle(
            cache_key=cache_key,
            cache_value=updated_profile
        )
```

## Integration with Existing MPREG Infrastructure

### 1. Leverage Existing Merkle Trees

```python
from mpreg.datastructures.merkle_tree import MerkleTree

# Use MPREG's existing merkle tree implementation
def _get_or_create_merkle_tree(self, namespace: str) -> MerkleTree:
    if namespace not in self.merkle_trees:
        self.merkle_trees[namespace] = MerkleTree.empty()
    return self.merkle_trees[namespace]
```

### 2. Integrate with Vector Clocks

```python
# Use both vector clocks AND merkle trees for comprehensive conflict resolution
def _resolve_complex_conflict(self, local: CacheEntry, remote: CacheEntry) -> CacheEntry:
    # 1. Check vector clock causality
    if local.vector_clock.happens_before(remote.vector_clock):
        return remote  # Causal ordering

    # 2. If concurrent, use merkle hash comparison
    if local.merkle_hash != remote.merkle_hash:
        return self._resolve_by_merkle_merge(local, remote)

    # 3. If identical hashes, prefer local
    return local
```

## Summary

The current cache federation implementation lacks:

1. **❌ Merkle tree integration** - No data integrity verification
2. **❌ Conflict resolution** - No strategy for handling concurrent updates
3. **❌ Leader election** - No authoritative source for decisions
4. **❌ Data corruption detection** - No way to detect/recover from corruption

**Recommended Next Steps:**

1. Implement `MerkleAwareFederatedCacheKey` with integrity verification
2. Add `CacheConflictResolver` with multiple resolution strategies
3. Implement `CacheNamespaceLeader` for leader election
4. Enhance the federation bridge with conflict handling
5. Add comprehensive tests for conflict scenarios
6. Update documentation with conflict resolution patterns

This would transform the cache federation from a "happy path" system into a production-ready distributed cache with proper consistency guarantees.
