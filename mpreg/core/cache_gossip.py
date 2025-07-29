"""
Cache Gossip Protocol for MPREG.

This module implements gossip-based cache state synchronization that integrates
with MPREG's existing gossip infrastructure to provide distributed caching.

Features:
- Cache operation propagation (put, delete, invalidate)
- Conflict resolution using vector clocks
- Anti-entropy for cache state reconciliation
- Efficient delta synchronization
- Geographic awareness for optimal replication
"""

import asyncio
import hashlib
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from .global_cache import (
    CacheMetadata,
    ConsistencyLevel,
    GlobalCacheEntry,
    GlobalCacheKey,
)
from .serialization import JsonSerializer


class CacheOperationType(Enum):
    """Types of cache operations that can be gossiped."""

    PUT = "put"
    DELETE = "delete"
    INVALIDATE = "invalidate"
    QUERY = "query"
    ANTI_ENTROPY = "anti_entropy"


class ConflictResolutionStrategy(Enum):
    """Strategies for resolving cache conflicts."""

    LAST_WRITER_WINS = "last_writer_wins"
    VECTOR_CLOCK = "vector_clock"
    MOST_RECENT_ACCESS = "most_recent_access"
    HIGHEST_QUALITY = "highest_quality"
    MANUAL = "manual"


@dataclass(frozen=True, slots=True)
class CacheOperationMessage:
    """Message for cache operations in gossip protocol."""

    operation_type: CacheOperationType
    operation_id: str
    key: GlobalCacheKey
    timestamp: float
    source_node: str
    vector_clock: dict[str, int] = field(default_factory=dict)
    ttl_hops: int = 5

    # Optional fields based on operation type
    value: Any = None
    metadata: CacheMetadata | None = None
    value_hash: str = ""  # For integrity verification
    invalidation_pattern: str = ""  # For pattern-based invalidation
    consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL

    def __post_init__(self) -> None:
        """Initialize computed fields."""
        if self.value is not None and not self.value_hash:
            # Create value hash for integrity verification
            serializer = JsonSerializer()
            serialized = serializer.serialize(self.value)
            object.__setattr__(
                self, "value_hash", hashlib.sha256(serialized).hexdigest()[:16]
            )


@dataclass(slots=True)
class CacheDigestEntry:
    """Entry in cache digest for anti-entropy."""

    key: GlobalCacheKey
    version: int
    timestamp: float
    value_hash: str
    size_bytes: int
    node_id: str


@dataclass(frozen=True, slots=True)
class CacheDigest:
    """Digest of cache state for anti-entropy protocol."""

    node_id: str
    timestamp: float
    entries: dict[str, CacheDigestEntry] = field(default_factory=dict)
    total_entries: int = 0
    total_size_bytes: int = 0
    digest_hash: str = ""

    def __post_init__(self) -> None:
        """Initialize computed fields."""
        if not self.digest_hash:
            # Create digest hash for quick comparison
            content = f"{self.node_id}:{self.timestamp}:{len(self.entries)}"
            object.__setattr__(
                self, "digest_hash", hashlib.sha256(content.encode()).hexdigest()[:16]
            )


@dataclass(slots=True)
class CacheConflict:
    """Represents a conflict between cache entries."""

    key: GlobalCacheKey
    local_entry: GlobalCacheEntry | None
    remote_entry: GlobalCacheEntry | None
    conflict_type: str  # "version", "timestamp", "value"
    detection_time: float = field(default_factory=time.time)
    resolution_strategy: ConflictResolutionStrategy = (
        ConflictResolutionStrategy.LAST_WRITER_WINS
    )
    resolved: bool = False
    resolution_result: GlobalCacheEntry | None = None


@dataclass(slots=True)
class CacheGossipStatistics:
    """Statistics for cache gossip operations."""

    operations_sent: int = 0
    operations_received: int = 0
    operations_applied: int = 0
    operations_rejected: int = 0
    conflicts_detected: int = 0
    conflicts_resolved: int = 0
    anti_entropy_runs: int = 0
    digest_exchanges: int = 0
    bytes_transferred: int = 0
    last_gossip_time: float = 0.0


class CacheGossipProtocol:
    """
    Gossip-based cache state synchronization protocol.

    Provides distributed cache consistency through:
    - Operation propagation (put, delete, invalidate)
    - Anti-entropy reconciliation
    - Conflict detection and resolution
    - Geographic replication awareness
    """

    def __init__(
        self,
        node_id: str,
        gossip_interval: float = 30.0,
        anti_entropy_interval: float = 300.0,
        max_gossip_targets: int = 3,
    ):
        self.node_id = node_id
        self.gossip_interval = gossip_interval
        self.anti_entropy_interval = anti_entropy_interval
        self.max_gossip_targets = max_gossip_targets

        # Vector clock for ordering operations
        self.vector_clock: dict[str, int] = defaultdict(int)

        # Cache state tracking
        self.cache_operations: dict[str, CacheOperationMessage] = {}
        self.cache_entries: dict[str, GlobalCacheEntry] = {}
        self.pending_operations: list[CacheOperationMessage] = []

        # Conflict management
        self.active_conflicts: dict[str, CacheConflict] = {}
        self.conflict_resolution_handlers: dict[
            ConflictResolutionStrategy,
            Callable[[CacheConflict], GlobalCacheEntry | None],
        ] = {
            ConflictResolutionStrategy.LAST_WRITER_WINS: self._resolve_by_timestamp,
            ConflictResolutionStrategy.VECTOR_CLOCK: self._resolve_by_vector_clock,
            ConflictResolutionStrategy.MOST_RECENT_ACCESS: self._resolve_by_access_time,
            ConflictResolutionStrategy.HIGHEST_QUALITY: self._resolve_by_quality_score,
        }

        # Peer management
        self.known_peers: set[str] = set()
        self.peer_cache_digests: dict[str, CacheDigest] = {}

        # Statistics
        self.statistics = CacheGossipStatistics()

        # Background tasks
        self._gossip_task: asyncio.Task[None] | None = None
        self._anti_entropy_task: asyncio.Task[None] | None = None

        # Start background processing
        self._start_background_tasks()

    def _start_background_tasks(self) -> None:
        """Start background gossip and anti-entropy tasks."""
        try:
            if self._gossip_task is None or self._gossip_task.done():
                self._gossip_task = asyncio.create_task(self._gossip_worker())

            if self._anti_entropy_task is None or self._anti_entropy_task.done():
                self._anti_entropy_task = asyncio.create_task(
                    self._anti_entropy_worker()
                )
        except RuntimeError:
            # No event loop running
            logger.warning("No event loop running, skipping background tasks")

    async def propagate_cache_operation(
        self,
        operation_type: CacheOperationType,
        key: GlobalCacheKey,
        value: Any = None,
        metadata: CacheMetadata | None = None,
        consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
    ) -> str:
        """
        Propagate cache operation to cluster nodes.

        Returns:
            Operation ID for tracking
        """
        # Increment vector clock
        self.vector_clock[self.node_id] += 1

        # Create operation message
        operation_id = f"{self.node_id}:{self.vector_clock[self.node_id]}:{time.time()}"

        operation = CacheOperationMessage(
            operation_type=operation_type,
            operation_id=operation_id,
            key=key,
            timestamp=time.time(),
            source_node=self.node_id,
            vector_clock=dict(self.vector_clock),
            value=value,
            metadata=metadata,
            consistency_level=consistency_level,
        )

        # Store operation for deduplication
        self.cache_operations[operation_id] = operation

        # Add to pending operations for gossip
        self.pending_operations.append(operation)

        logger.debug(f"Queued cache operation {operation_type.value} for key {key}")
        self.statistics.operations_sent += 1

        return operation_id

    async def handle_cache_gossip_message(self, message: CacheOperationMessage) -> bool:
        """
        Handle incoming cache gossip message.

        Returns:
            True if message was processed successfully
        """
        try:
            # Check if we've already seen this operation
            if message.operation_id in self.cache_operations:
                return True  # Already processed

            # Update vector clock
            for node, clock in message.vector_clock.items():
                self.vector_clock[node] = max(self.vector_clock[node], clock)

            # Verify message integrity if value is present
            if message.value is not None and message.value_hash:
                serializer = JsonSerializer()
                computed_hash = hashlib.sha256(
                    serializer.serialize(message.value)
                ).hexdigest()[:16]
                if computed_hash != message.value_hash:
                    logger.warning(
                        f"Cache gossip message integrity check failed for {message.key}"
                    )
                    self.statistics.operations_rejected += 1
                    return False

            # Process the operation
            success = await self._process_cache_operation(message)

            if success:
                # Store operation to prevent reprocessing
                self.cache_operations[message.operation_id] = message
                self.statistics.operations_received += 1
                self.statistics.operations_applied += 1

                # Propagate to other nodes if TTL allows
                if message.ttl_hops > 0:
                    propagated_message = CacheOperationMessage(
                        operation_type=message.operation_type,
                        operation_id=message.operation_id,
                        key=message.key,
                        timestamp=message.timestamp,
                        source_node=message.source_node,
                        vector_clock=message.vector_clock,
                        ttl_hops=message.ttl_hops - 1,
                        value=message.value,
                        metadata=message.metadata,
                        value_hash=message.value_hash,
                        consistency_level=message.consistency_level,
                    )
                    self.pending_operations.append(propagated_message)
            else:
                self.statistics.operations_rejected += 1

            return success

        except Exception as e:
            logger.error(f"Error handling cache gossip message: {e}")
            self.statistics.operations_rejected += 1
            return False

    async def _process_cache_operation(self, message: CacheOperationMessage) -> bool:
        """Process a specific cache operation."""
        try:
            key_str = str(message.key)

            if message.operation_type == CacheOperationType.PUT:
                return await self._process_cache_put(message)

            elif message.operation_type == CacheOperationType.DELETE:
                return await self._process_cache_delete(message)

            elif message.operation_type == CacheOperationType.INVALIDATE:
                return await self._process_cache_invalidate(message)

            elif message.operation_type == CacheOperationType.QUERY:
                return await self._process_cache_query(message)

            else:
                logger.warning(
                    f"Unknown cache operation type: {message.operation_type}"
                )
                return False

        except Exception as e:
            logger.error(
                f"Error processing cache operation {message.operation_type}: {e}"
            )
            return False

    async def _process_cache_put(self, message: CacheOperationMessage) -> bool:
        """Process cache PUT operation."""
        if message.value is None or message.metadata is None:
            logger.warning("Cache PUT operation missing value or metadata")
            return False

        key_str = str(message.key)
        existing_entry = self.cache_entries.get(key_str)

        # Create new cache entry
        new_entry = GlobalCacheEntry(
            key=message.key,
            value=message.value,
            metadata=message.metadata,
            creation_time=message.timestamp,
            vector_clock=message.vector_clock.copy(),
        )

        # Check for conflicts
        if existing_entry:
            conflict = self._detect_conflict(existing_entry, new_entry)
            if conflict:
                await self._handle_conflict(conflict)
                return True  # Conflict handled

        # Store the entry
        self.cache_entries[key_str] = new_entry
        logger.debug(f"Applied cache PUT for key {message.key}")
        return True

    async def _process_cache_delete(self, message: CacheOperationMessage) -> bool:
        """Process cache DELETE operation."""
        key_str = str(message.key)

        if key_str in self.cache_entries:
            del self.cache_entries[key_str]
            logger.debug(f"Applied cache DELETE for key {message.key}")

        return True

    async def _process_cache_invalidate(self, message: CacheOperationMessage) -> bool:
        """Process cache INVALIDATE operation."""
        if not message.invalidation_pattern:
            logger.warning("Cache INVALIDATE operation missing pattern")
            return False

        # Simple pattern matching - in production this would be more sophisticated
        invalidated_count = 0
        keys_to_remove = []

        for key_str in self.cache_entries.keys():
            if self._matches_pattern(key_str, message.invalidation_pattern):
                keys_to_remove.append(key_str)

        for key_str in keys_to_remove:
            del self.cache_entries[key_str]
            invalidated_count += 1

        logger.debug(f"Applied cache INVALIDATE: removed {invalidated_count} entries")
        return True

    async def _process_cache_query(self, message: CacheOperationMessage) -> bool:
        """Process cache QUERY operation."""
        # TODO: Implement cache query handling
        return True

    def _detect_conflict(
        self, existing: GlobalCacheEntry, new: GlobalCacheEntry
    ) -> CacheConflict | None:
        """Detect conflict between existing and new cache entries."""
        # Check if entries are truly conflicting
        if existing.checksum == new.checksum:
            return None  # Same data, no conflict

        # Determine conflict type
        conflict_type = "value"
        if existing.creation_time != new.creation_time:
            conflict_type = "timestamp"

        conflict = CacheConflict(
            key=existing.key,
            local_entry=existing,
            remote_entry=new,
            conflict_type=conflict_type,
        )

        key_str = str(existing.key)
        self.active_conflicts[key_str] = conflict
        self.statistics.conflicts_detected += 1

        logger.debug(f"Detected cache conflict for key {existing.key}: {conflict_type}")
        return conflict

    async def _handle_conflict(self, conflict: CacheConflict) -> None:
        """Handle cache conflict using configured resolution strategy."""
        try:
            handler = self.conflict_resolution_handlers.get(
                conflict.resolution_strategy
            )
            if handler:
                resolution_result = handler(conflict)
                if resolution_result:
                    conflict.resolution_result = resolution_result
                    conflict.resolved = True

                    # Apply resolution
                    key_str = str(conflict.key)
                    self.cache_entries[key_str] = resolution_result

                    # Remove from active conflicts
                    self.active_conflicts.pop(key_str, None)
                    self.statistics.conflicts_resolved += 1

                    logger.debug(f"Resolved cache conflict for key {conflict.key}")
            else:
                logger.warning(
                    f"No handler for conflict resolution strategy: {conflict.resolution_strategy}"
                )

        except Exception as e:
            logger.error(f"Error handling cache conflict: {e}")

    def _resolve_by_timestamp(self, conflict: CacheConflict) -> GlobalCacheEntry | None:
        """Resolve conflict by timestamp (last writer wins)."""
        if not conflict.local_entry or not conflict.remote_entry:
            return conflict.local_entry or conflict.remote_entry

        if conflict.remote_entry.creation_time > conflict.local_entry.creation_time:
            return conflict.remote_entry
        else:
            return conflict.local_entry

    def _resolve_by_vector_clock(
        self, conflict: CacheConflict
    ) -> GlobalCacheEntry | None:
        """Resolve conflict using vector clock comparison."""
        if not conflict.local_entry or not conflict.remote_entry:
            return conflict.local_entry or conflict.remote_entry

        # Compare vector clocks to determine causality
        local_clock = conflict.local_entry.vector_clock
        remote_clock = conflict.remote_entry.vector_clock

        # Check if one clock dominates the other
        local_dominates = all(
            local_clock.get(node, 0) >= remote_clock.get(node, 0)
            for node in set(local_clock.keys()) | set(remote_clock.keys())
        )

        remote_dominates = all(
            remote_clock.get(node, 0) >= local_clock.get(node, 0)
            for node in set(local_clock.keys()) | set(remote_clock.keys())
        )

        if remote_dominates and not local_dominates:
            return conflict.remote_entry
        elif local_dominates and not remote_dominates:
            return conflict.local_entry
        else:
            # Concurrent events, fall back to timestamp
            return self._resolve_by_timestamp(conflict)

    def _resolve_by_access_time(
        self, conflict: CacheConflict
    ) -> GlobalCacheEntry | None:
        """Resolve conflict by most recent access time."""
        if not conflict.local_entry or not conflict.remote_entry:
            return conflict.local_entry or conflict.remote_entry

        if (
            conflict.remote_entry.last_access_time
            > conflict.local_entry.last_access_time
        ):
            return conflict.remote_entry
        else:
            return conflict.local_entry

    def _resolve_by_quality_score(
        self, conflict: CacheConflict
    ) -> GlobalCacheEntry | None:
        """Resolve conflict by entry quality score."""
        if not conflict.local_entry or not conflict.remote_entry:
            return conflict.local_entry or conflict.remote_entry

        local_quality = conflict.local_entry.metadata.quality_score
        remote_quality = conflict.remote_entry.metadata.quality_score

        if remote_quality > local_quality:
            return conflict.remote_entry
        else:
            return conflict.local_entry

    def _matches_pattern(self, key_str: str, pattern: str) -> bool:
        """Simple pattern matching for cache invalidation."""
        # Simplified pattern matching - in production this would use proper glob/regex
        if pattern.endswith("*"):
            return key_str.startswith(pattern[:-1])
        elif pattern.startswith("*"):
            return key_str.endswith(pattern[1:])
        else:
            return key_str == pattern

    async def sync_cache_state(self, peer_node: str) -> bool:
        """Synchronize cache state with a specific peer node."""
        try:
            # Generate our cache digest
            our_digest = self._generate_cache_digest()

            # TODO: Send digest to peer and get their digest
            # TODO: Compare digests and exchange missing/different entries

            logger.debug(f"Synchronized cache state with peer {peer_node}")
            return True

        except Exception as e:
            logger.error(f"Error synchronizing cache state with {peer_node}: {e}")
            return False

    def _generate_cache_digest(self) -> CacheDigest:
        """Generate digest of current cache state."""
        digest_entries = {}
        total_size = 0

        for key_str, entry in self.cache_entries.items():
            digest_entry = CacheDigestEntry(
                key=entry.key,
                version=max(entry.vector_clock.values(), default=0),
                timestamp=entry.creation_time,
                value_hash=entry.checksum,
                size_bytes=entry.metadata.size_estimate_bytes,
                node_id=self.node_id,
            )
            digest_entries[key_str] = digest_entry
            total_size += digest_entry.size_bytes

        return CacheDigest(
            node_id=self.node_id,
            timestamp=time.time(),
            entries=digest_entries,
            total_entries=len(digest_entries),
            total_size_bytes=total_size,
        )

    async def _gossip_worker(self) -> None:
        """Background worker for gossiping cache operations."""
        while True:
            try:
                await asyncio.sleep(self.gossip_interval)

                if self.pending_operations:
                    # Send pending operations to random peers
                    operations_to_send = self.pending_operations[:10]  # Batch size
                    self.pending_operations = self.pending_operations[10:]

                    # TODO: Integrate with actual gossip protocol to send messages

                    self.statistics.last_gossip_time = time.time()
                    logger.debug(f"Gossiped {len(operations_to_send)} cache operations")

            except Exception as e:
                logger.error(f"Gossip worker error: {e}")

    async def _anti_entropy_worker(self) -> None:
        """Background worker for anti-entropy reconciliation."""
        while True:
            try:
                await asyncio.sleep(self.anti_entropy_interval)

                # Run anti-entropy with random peers
                if self.known_peers:
                    import random

                    peer = random.choice(list(self.known_peers))
                    await self.sync_cache_state(peer)
                    self.statistics.anti_entropy_runs += 1

            except Exception as e:
                logger.error(f"Anti-entropy worker error: {e}")

    def add_peer(self, peer_id: str) -> None:
        """Add a peer node for gossip communication."""
        self.known_peers.add(peer_id)
        logger.debug(f"Added cache gossip peer: {peer_id}")

    def remove_peer(self, peer_id: str) -> None:
        """Remove a peer node."""
        self.known_peers.discard(peer_id)
        self.peer_cache_digests.pop(peer_id, None)
        logger.debug(f"Removed cache gossip peer: {peer_id}")

    def get_statistics(self) -> dict[str, Any]:
        """Get comprehensive cache gossip statistics."""
        return {
            "node_id": self.node_id,
            "vector_clock": dict(self.vector_clock),
            "operations": {
                "sent": self.statistics.operations_sent,
                "received": self.statistics.operations_received,
                "applied": self.statistics.operations_applied,
                "rejected": self.statistics.operations_rejected,
            },
            "conflicts": {
                "detected": self.statistics.conflicts_detected,
                "resolved": self.statistics.conflicts_resolved,
                "active": len(self.active_conflicts),
            },
            "anti_entropy": {
                "runs": self.statistics.anti_entropy_runs,
                "digest_exchanges": self.statistics.digest_exchanges,
            },
            "cache_state": {
                "entries": len(self.cache_entries),
                "operations_tracked": len(self.cache_operations),
                "pending_operations": len(self.pending_operations),
            },
            "peers": {
                "known_peers": len(self.known_peers),
                "peer_digests": len(self.peer_cache_digests),
            },
            "last_gossip_time": self.statistics.last_gossip_time,
        }

    async def shutdown(self) -> None:
        """Shutdown the cache gossip protocol."""
        # Cancel background tasks
        for task in [self._gossip_task, self._anti_entropy_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        logger.info("Cache gossip protocol shutdown complete")
