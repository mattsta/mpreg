"""
Fabric Cache Federation Protocol for MPREG.

This module implements fabric-routed cache state synchronization to provide
distributed caching over the unified message plane.

Features:
- Cache operation propagation (put, delete, invalidate)
- Conflict resolution using vector clocks
- Anti-entropy for cache state reconciliation
- Efficient delta synchronization
- Geographic awareness for optimal replication
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from mpreg.core.cache_models import (
    CacheMetadata,
    ConsistencyLevel,
    GlobalCacheEntry,
    GlobalCacheKey,
)
from mpreg.core.cache_protocol import CacheKeyMessage, CacheMetadataMessage
from mpreg.core.serialization import JsonSerializer
from mpreg.datastructures.type_aliases import NodeId

from .cache_transport import CacheTransport

fabric_cache_log = logger


class CacheOperationType(Enum):
    """Types of cache operations that can be propagated."""

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
    """Message for cache operations routed over the fabric."""

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

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for transport serialization."""
        return {
            "operation_type": self.operation_type.value,
            "operation_id": self.operation_id,
            "key": CacheKeyMessage.from_global_cache_key(self.key).to_dict(),
            "timestamp": self.timestamp,
            "source_node": self.source_node,
            "vector_clock": dict(self.vector_clock),
            "ttl_hops": self.ttl_hops,
            "value": self.value,
            "metadata": (
                CacheMetadataMessage.from_cache_metadata(self.metadata).to_dict()
                if self.metadata
                else None
            ),
            "value_hash": self.value_hash,
            "invalidation_pattern": self.invalidation_pattern,
            "consistency_level": self.consistency_level.value,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CacheOperationMessage:
        """Create from dictionary (transport deserialization)."""
        key_message = CacheKeyMessage.from_dict(payload.get("key", {}))
        metadata_payload = payload.get("metadata")
        metadata = (
            CacheMetadataMessage.from_dict(metadata_payload).to_cache_metadata()
            if isinstance(metadata_payload, dict)
            else None
        )
        consistency_level = ConsistencyLevel(
            payload.get("consistency_level", ConsistencyLevel.EVENTUAL.value)
        )
        return cls(
            operation_type=CacheOperationType(payload["operation_type"]),
            operation_id=str(payload.get("operation_id", "")),
            key=key_message.to_global_cache_key(),
            timestamp=float(payload.get("timestamp", time.time())),
            source_node=str(payload.get("source_node", "")),
            vector_clock=dict(payload.get("vector_clock", {})),
            ttl_hops=int(payload.get("ttl_hops", 5)),
            value=payload.get("value"),
            metadata=metadata,
            value_hash=str(payload.get("value_hash", "")),
            invalidation_pattern=str(payload.get("invalidation_pattern", "")),
            consistency_level=consistency_level,
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

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for transport serialization."""
        return {
            "key": CacheKeyMessage.from_global_cache_key(self.key).to_dict(),
            "version": self.version,
            "timestamp": self.timestamp,
            "value_hash": self.value_hash,
            "size_bytes": self.size_bytes,
            "node_id": self.node_id,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CacheDigestEntry:
        """Create from dictionary (transport deserialization)."""
        key_message = CacheKeyMessage.from_dict(payload.get("key", {}))
        return cls(
            key=key_message.to_global_cache_key(),
            version=int(payload.get("version", 0)),
            timestamp=float(payload.get("timestamp", 0.0)),
            value_hash=str(payload.get("value_hash", "")),
            size_bytes=int(payload.get("size_bytes", 0)),
            node_id=str(payload.get("node_id", "")),
        )


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

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for transport serialization."""
        return {
            "node_id": self.node_id,
            "timestamp": self.timestamp,
            "entries": {key: entry.to_dict() for key, entry in self.entries.items()},
            "total_entries": self.total_entries,
            "total_size_bytes": self.total_size_bytes,
            "digest_hash": self.digest_hash,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CacheDigest:
        """Create from dictionary (transport deserialization)."""
        entries_payload = payload.get("entries", {})
        entries = {
            key: CacheDigestEntry.from_dict(entry_payload)
            for key, entry_payload in entries_payload.items()
        }
        return cls(
            node_id=str(payload.get("node_id", "")),
            timestamp=float(payload.get("timestamp", 0.0)),
            entries=entries,
            total_entries=int(payload.get("total_entries", len(entries))),
            total_size_bytes=int(payload.get("total_size_bytes", 0)),
            digest_hash=str(payload.get("digest_hash", "")),
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
class CacheFederationStatistics:
    """Statistics for cache federation operations."""

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


class FabricCacheProtocol:
    """
    Fabric-based cache state synchronization protocol.

    Provides distributed cache consistency through:
    - Operation propagation (put, delete, invalidate)
    - Anti-entropy reconciliation
    - Conflict detection and resolution
    - Geographic replication awareness
    """

    def __init__(
        self,
        node_id: NodeId,
        transport: CacheTransport,
        gossip_interval: float = 30.0,
        anti_entropy_interval: float = 300.0,
        max_gossip_targets: int = 3,
    ):
        self.node_id = node_id
        self.transport = transport
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
        self.peer_cache_digests: dict[NodeId, CacheDigest] = {}

        # Statistics
        self.statistics = CacheFederationStatistics()

        # Background tasks
        self._gossip_task: asyncio.Task[None] | None = None
        self._anti_entropy_task: asyncio.Task[None] | None = None

        # Start background processing
        self.transport.register(self)
        self._start_background_tasks()

    def _start_background_tasks(self) -> None:
        """Start background propagation and anti-entropy tasks."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            fabric_cache_log.debug("No event loop running, skipping background tasks")
            return

        if self._gossip_task is None or self._gossip_task.done():
            self._gossip_task = loop.create_task(self._gossip_worker())

        if self._anti_entropy_task is None or self._anti_entropy_task.done():
            self._anti_entropy_task = loop.create_task(self._anti_entropy_worker())

    async def propagate_cache_operation(
        self,
        operation_type: CacheOperationType,
        key: GlobalCacheKey,
        value: Any = None,
        metadata: CacheMetadata | None = None,
        consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
        invalidation_pattern: str = "",
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
            invalidation_pattern=invalidation_pattern,
            consistency_level=consistency_level,
        )

        # Store operation for deduplication
        self.cache_operations[operation_id] = operation

        # Apply locally before propagating
        applied = await self._process_cache_operation(operation)
        if applied:
            self.statistics.operations_applied += 1
        else:
            self.statistics.operations_rejected += 1

        # Add to pending operations for propagation
        self.pending_operations.append(operation)

        fabric_cache_log.debug(
            f"Queued cache operation {operation_type.value} for key {key}"
        )
        self.statistics.operations_sent += 1

        return operation_id

    async def handle_cache_message(self, message: CacheOperationMessage) -> bool:
        """
        Handle incoming cache federation message.

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
                    fabric_cache_log.warning(
                        f"Cache federation message integrity check failed for {message.key}"
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
                        invalidation_pattern=message.invalidation_pattern,
                        consistency_level=message.consistency_level,
                    )
                    self.pending_operations.append(propagated_message)
            else:
                self.statistics.operations_rejected += 1

            return success

        except Exception as e:
            fabric_cache_log.error(f"Error handling cache federation message: {e}")
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
                fabric_cache_log.warning(
                    f"Unknown cache operation type: {message.operation_type}"
                )
                return False

        except Exception as e:
            fabric_cache_log.error(
                f"Error processing cache operation {message.operation_type}: {e}"
            )
            return False

    async def _process_cache_put(self, message: CacheOperationMessage) -> bool:
        """Process cache PUT operation."""
        if message.value is None or message.metadata is None:
            fabric_cache_log.warning("Cache PUT operation missing value or metadata")
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
        fabric_cache_log.debug(f"Applied cache PUT for key {message.key}")
        return True

    async def _process_cache_delete(self, message: CacheOperationMessage) -> bool:
        """Process cache DELETE operation."""
        key_str = str(message.key)

        if key_str in self.cache_entries:
            del self.cache_entries[key_str]
            fabric_cache_log.debug(f"Applied cache DELETE for key {message.key}")

        return True

    async def _process_cache_invalidate(self, message: CacheOperationMessage) -> bool:
        """Process cache INVALIDATE operation."""
        if not message.invalidation_pattern:
            fabric_cache_log.warning("Cache INVALIDATE operation missing pattern")
            return False

        # Simple pattern matching - in production this would be more sophisticated
        invalidated_count = 0
        keys_to_remove = []

        for key_str in self.cache_entries:
            if self._matches_pattern(key_str, message.invalidation_pattern):
                keys_to_remove.append(key_str)

        for key_str in keys_to_remove:
            del self.cache_entries[key_str]
            invalidated_count += 1

        fabric_cache_log.debug(
            f"Applied cache INVALIDATE: removed {invalidated_count} entries"
        )
        return True

    async def _process_cache_query(self, message: CacheOperationMessage) -> bool:
        """Process cache QUERY operation."""
        try:
            pattern = message.invalidation_pattern
            matches = []

            if pattern:
                for key_str, entry in self.cache_entries.items():
                    if self._matches_pattern(key_str, pattern):
                        matches.append(entry)
            else:
                key_str = str(message.key)
                entry = self.cache_entries.get(key_str)
                if entry:
                    matches.append(entry)

            if not matches:
                fabric_cache_log.debug("Cache QUERY found no matches")
                return True

            for entry in matches:
                operation = CacheOperationMessage(
                    operation_type=CacheOperationType.PUT,
                    operation_id=f"query-response-{uuid.uuid4()}",
                    key=entry.key,
                    timestamp=time.time(),
                    source_node=self.node_id,
                    vector_clock=entry.vector_clock.copy(),
                    ttl_hops=1,
                    value=entry.value,
                    metadata=entry.metadata,
                    consistency_level=ConsistencyLevel.EVENTUAL,
                )
                self.pending_operations.append(operation)

            fabric_cache_log.debug(f"Cache QUERY responded with {len(matches)} entries")
            return True

        except Exception as e:
            fabric_cache_log.error(f"Cache QUERY handling failed: {e}")
            return False

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

        fabric_cache_log.debug(
            f"Detected cache conflict for key {existing.key}: {conflict_type}"
        )
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

                    fabric_cache_log.debug(
                        f"Resolved cache conflict for key {conflict.key}"
                    )
            else:
                fabric_cache_log.warning(
                    f"No handler for conflict resolution strategy: {conflict.resolution_strategy}"
                )

        except Exception as e:
            fabric_cache_log.error(f"Error handling cache conflict: {e}")

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

    async def sync_cache_state(self, peer_node: NodeId) -> bool:
        """Synchronize cache state with a specific peer node."""
        try:
            # Generate our cache digest
            our_digest = self._generate_cache_digest()

            peer_digest = await self.transport.fetch_digest(peer_node)
            if peer_digest is None:
                fabric_cache_log.debug(
                    f"No cache federation peer found for {peer_node}"
                )
                return False

            # Exchange missing entries from local to peer
            for key_str, entry in our_digest.entries.items():
                peer_entry = peer_digest.entries.get(key_str)
                if peer_entry is None or entry.timestamp > peer_entry.timestamp:
                    operation_id = f"sync:{self.node_id}:{time.time()}"
                    message = CacheOperationMessage(
                        operation_type=CacheOperationType.PUT,
                        operation_id=operation_id,
                        key=entry.key,
                        timestamp=entry.timestamp,
                        source_node=self.node_id,
                        vector_clock=dict(self.vector_clock),
                        value=self.cache_entries[key_str].value,
                        metadata=self.cache_entries[key_str].metadata,
                        consistency_level=ConsistencyLevel.EVENTUAL,
                    )
                    await self.transport.send_operation(peer_node, message)

            # Pull missing entries from peer
            for key_str, entry in peer_digest.entries.items():
                local_entry = our_digest.entries.get(key_str)
                if local_entry is None or entry.timestamp > local_entry.timestamp:
                    peer_cache_entry = await self.transport.fetch_entry(
                        peer_node, entry.key
                    )
                    if peer_cache_entry and not peer_cache_entry.is_expired():
                        self.cache_entries[key_str] = peer_cache_entry

            fabric_cache_log.debug(f"Synchronized cache state with peer {peer_node}")
            return True

        except Exception as e:
            fabric_cache_log.error(
                f"Error synchronizing cache state with {peer_node}: {e}"
            )
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
        """Background worker for propagating cache operations."""
        while True:
            try:
                await asyncio.sleep(self.gossip_interval)

                if self.pending_operations:
                    # Send pending operations to random peers
                    operations_to_send = self.pending_operations[:10]  # Batch size
                    self.pending_operations = self.pending_operations[10:]

                    for operation in operations_to_send:
                        peer_ids = self.transport.select_peers(
                            operation,
                            exclude=self.node_id,
                            max_peers=self.max_gossip_targets,
                        )
                        for peer_id in peer_ids:
                            await self.transport.send_operation(peer_id, operation)

                    self.statistics.last_gossip_time = time.time()
                    fabric_cache_log.debug(
                        f"Gossiped {len(operations_to_send)} cache operations"
                    )

            except Exception as e:
                fabric_cache_log.error(f"Gossip worker error: {e}")

    async def _anti_entropy_worker(self) -> None:
        """Background worker for anti-entropy reconciliation."""
        while True:
            try:
                await asyncio.sleep(self.anti_entropy_interval)

                # Run anti-entropy with random peers
                peer_ids = self.transport.peer_ids(exclude=self.node_id)
                if peer_ids:
                    import random

                    peer = random.choice(list(peer_ids))
                    await self.sync_cache_state(peer)
                    self.statistics.anti_entropy_runs += 1

            except Exception as e:
                fabric_cache_log.error(f"Anti-entropy worker error: {e}")

    def create_cache_digest(self) -> CacheDigest:
        """Return a digest of current cache state for cache transport."""
        return self._generate_cache_digest()

    def get_cache_entry(self, key: GlobalCacheKey) -> GlobalCacheEntry | None:
        """Return a local cache entry for transport fetches."""
        entry = self.cache_entries.get(str(key))
        if entry and not entry.is_expired():
            return entry
        return None

    def peer_ids(self) -> tuple[NodeId, ...]:
        """Return peer IDs visible to this cache protocol."""
        return self.transport.peer_ids(exclude=self.node_id)

    def get_statistics(self) -> dict[str, Any]:
        """Get comprehensive cache federation statistics."""
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
                "known_peers": len(self.transport.peer_ids(exclude=self.node_id)),
                "peer_digests": len(self.peer_cache_digests),
            },
            "last_gossip_time": self.statistics.last_gossip_time,
        }

    async def shutdown(self) -> None:
        """Shutdown the cache federation protocol."""
        # Cancel background tasks
        for task in [self._gossip_task, self._anti_entropy_task]:
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        self.transport.unregister(self.node_id)

        fabric_cache_log.info("Cache federation protocol shutdown complete")

    async def fetch_entry(
        self, key: GlobalCacheKey
    ) -> tuple[GlobalCacheEntry | None, str | None]:
        """Fetch cache entry locally or from transport peers."""
        key_str = str(key)
        entry = self.cache_entries.get(key_str)
        if entry and not entry.is_expired():
            return entry, self.node_id

        peer_ids = self.transport.peer_ids(exclude=self.node_id)
        for peer_id in peer_ids:
            peer_entry = await self.transport.fetch_entry(peer_id, key)
            if peer_entry and not peer_entry.is_expired():
                self.cache_entries[key_str] = peer_entry
                return peer_entry, peer_id

        return None, None
