"""
Cache Protocol Message Types for MPREG.

This module defines the message types and handlers for the global distributed
caching protocol as specified in the MPREG Protocol Specification v2.0.

Implements the cache-request and cache-response message types with full
compatibility with the existing MPREG message infrastructure.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .advanced_cache_ops import (
    AtomicOperationRequest,
    DataStructureOperation,
    NamespaceOperation,
)
from .cache_interfaces import CacheManagerProtocol
from .cache_models import (
    CacheLevel,
    CacheMetadata,
    CacheOperationResult,
    CacheOptions,
    CachePerformanceMetrics,
    ConsistencyLevel,
    GlobalCacheEntry,
    GlobalCacheKey,
    ReplicationStrategy,
)


class CacheMessageRole(Enum):
    """Message roles for cache protocol."""

    CACHE_REQUEST = "cache-request"
    CACHE_RESPONSE = "cache-response"
    CACHE_ANALYTICS = "cache-analytics"


class CacheOperation(Enum):
    """Cache operations supported by the protocol."""

    GET = "get"
    PUT = "put"
    DELETE = "delete"
    INVALIDATE = "invalidate"
    QUERY = "query"
    STATS = "stats"

    # Advanced atomic operations
    ATOMIC = "atomic"

    # Data structure operations
    STRUCTURE = "structure"

    # Namespace operations
    NAMESPACE = "namespace"


class CacheResponseStatus(Enum):
    """Status codes for cache responses."""

    HIT = "hit"
    MISS = "miss"
    ERROR = "error"
    PARTIAL = "partial"
    TIMEOUT = "timeout"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class CacheKeyMessage:
    """Serializable cache key for protocol messages."""

    namespace: str
    identifier: str
    version: str = "v1.0.0"
    tags: list[str] = field(default_factory=list)

    @classmethod
    def from_global_cache_key(cls, key: GlobalCacheKey) -> CacheKeyMessage:
        """Convert GlobalCacheKey to message format."""
        return cls(
            namespace=key.namespace,
            identifier=key.identifier,
            version=key.version,
            tags=list(key.tags),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "namespace": self.namespace,
            "identifier": self.identifier,
            "version": self.version,
            "tags": list(self.tags),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CacheKeyMessage:
        """Create from dictionary (JSON deserialization)."""
        return cls(
            namespace=str(payload.get("namespace", "")),
            identifier=str(payload.get("identifier", "")),
            version=str(payload.get("version", "v1.0.0")),
            tags=list(payload.get("tags", [])),
        )

    def to_global_cache_key(self) -> GlobalCacheKey:
        """Convert to GlobalCacheKey."""
        return GlobalCacheKey(
            namespace=self.namespace,
            identifier=self.identifier,
            version=self.version,
            tags=frozenset(self.tags),
        )


@dataclass(frozen=True, slots=True)
class AtomicOperationMessage:
    """Serializable atomic operation for protocol messages."""

    operation_type: str  # test_and_set, compare_and_swap, increment, etc.
    expected_value: Any = None
    new_value: Any = None
    increment_by: float = 0.0
    ttl_seconds: float | None = None
    conditions: dict[str, Any] = field(default_factory=dict)
    create_if_missing: bool = False
    initial_value: Any = None

    @classmethod
    def from_atomic_operation_request(
        cls, request: AtomicOperationRequest
    ) -> AtomicOperationMessage:
        """Convert AtomicOperationRequest to message format."""
        return cls(
            operation_type=request.operation.value,
            expected_value=request.expected_value,
            new_value=request.new_value,
            increment_by=request.delta,
            ttl_seconds=request.ttl_seconds,
            conditions={},  # Convert list[ValueConstraint] to dict if needed
            create_if_missing=request.if_not_exists,
            initial_value=request.data,
        )


@dataclass(frozen=True, slots=True)
class DataStructureOperationMessage:
    """Serializable data structure operation for protocol messages."""

    structure_type: str  # set, list, map, sorted_set, counter, queue, stack
    operation: str  # add, remove, get, etc.
    values: list[Any] = field(default_factory=list)
    fields: list[str] = field(default_factory=list)
    field_updates: dict[str, Any] = field(default_factory=dict)
    scored_values: list[dict[str, Any]] = field(default_factory=list)
    index: int | None = None
    range_spec: dict[str, Any] | None = None
    limit: int | None = None
    options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data_structure_operation(
        cls, operation: DataStructureOperation
    ) -> DataStructureOperationMessage:
        """Convert DataStructureOperation to message format."""
        return cls(
            structure_type=operation.structure_type.value,
            operation=operation.operation,
            values=[operation.member] if operation.member is not None else [],
            index=operation.index,
        )


@dataclass(frozen=True, slots=True)
class NamespaceOperationMessage:
    """Serializable namespace operation for protocol messages."""

    operation_type: str  # clear, statistics, list_keys, backup, restore
    namespace: str
    pattern: str = "*"
    conditions: dict[str, Any] = field(default_factory=dict)
    max_entries: int | None = None
    dry_run: bool = False
    include_detailed_breakdown: bool = False
    sample_size: int | None = None
    backup_format: str = "json"
    compression: bool = False
    data: Any = None
    merge_strategy: str = "overwrite"

    @classmethod
    def from_namespace_operation(
        cls, operation: NamespaceOperation
    ) -> NamespaceOperationMessage:
        """Convert NamespaceOperation to message format."""
        return cls(
            operation_type=operation.operation,
            namespace=operation.namespace,
            pattern=operation.pattern,
            max_entries=operation.limit,
            include_detailed_breakdown=operation.include_metadata,
        )


@dataclass(frozen=True, slots=True)
class CacheOptionsMessage:
    """Serializable cache options for protocol messages."""

    include_metadata: bool = True
    consistency_level: str = "eventual"  # ConsistencyLevel enum value
    timeout_ms: int = 5000
    cache_levels: list[str] = field(default_factory=lambda: ["L1", "L2", "L3", "L4"])
    replication_factor: int = 2
    prefer_local: bool = True
    max_staleness_seconds: float = 300.0

    @classmethod
    def from_cache_options(cls, options: CacheOptions) -> CacheOptionsMessage:
        """Convert CacheOptions to message format."""
        return cls(
            include_metadata=options.include_metadata,
            consistency_level=options.consistency_level.value,
            timeout_ms=options.timeout_ms,
            cache_levels=[level.value for level in options.cache_levels],
            replication_factor=options.replication_factor,
            prefer_local=options.prefer_local,
            max_staleness_seconds=options.max_staleness_seconds,
        )

    def to_cache_options(self) -> CacheOptions:
        """Convert to CacheOptions."""
        return CacheOptions(
            include_metadata=self.include_metadata,
            consistency_level=ConsistencyLevel(self.consistency_level),
            timeout_ms=self.timeout_ms,
            cache_levels=frozenset(CacheLevel(level) for level in self.cache_levels),
            replication_factor=self.replication_factor,
            prefer_local=self.prefer_local,
            max_staleness_seconds=self.max_staleness_seconds,
        )


@dataclass(frozen=True, slots=True)
class CacheMetadataMessage:
    """Serializable cache metadata for protocol messages."""

    computation_cost_ms: float = 0.0
    dependencies: list[CacheKeyMessage] = field(default_factory=list)
    ttl_seconds: float | None = None
    replication_policy: str = "geographic"
    access_patterns: dict[str, Any] = field(default_factory=dict)
    geographic_hints: list[str] = field(default_factory=list)
    quality_score: float = 1.0
    created_by: str = ""
    size_estimate_bytes: int = 0

    @classmethod
    def from_cache_metadata(cls, metadata: CacheMetadata) -> CacheMetadataMessage:
        """Convert CacheMetadata to message format."""
        return cls(
            computation_cost_ms=metadata.computation_cost_ms,
            dependencies=[
                CacheKeyMessage.from_global_cache_key(dep)
                for dep in metadata.dependencies
            ],
            ttl_seconds=metadata.ttl_seconds,
            replication_policy=metadata.replication_policy.value,
            access_patterns=metadata.access_patterns,
            geographic_hints=metadata.geographic_hints,
            quality_score=metadata.quality_score,
            created_by=metadata.created_by,
            size_estimate_bytes=metadata.size_estimate_bytes,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "computation_cost_ms": self.computation_cost_ms,
            "dependencies": [dep.to_dict() for dep in self.dependencies],
            "ttl_seconds": self.ttl_seconds,
            "replication_policy": self.replication_policy,
            "access_patterns": self.access_patterns,
            "geographic_hints": list(self.geographic_hints),
            "quality_score": self.quality_score,
            "created_by": self.created_by,
            "size_estimate_bytes": self.size_estimate_bytes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CacheMetadataMessage:
        """Create from dictionary (JSON deserialization)."""
        dependencies = [
            CacheKeyMessage.from_dict(item) for item in payload.get("dependencies", [])
        ]
        return cls(
            computation_cost_ms=float(payload.get("computation_cost_ms", 0.0)),
            dependencies=dependencies,
            ttl_seconds=payload.get("ttl_seconds"),
            replication_policy=str(payload.get("replication_policy", "geographic")),
            access_patterns=dict(payload.get("access_patterns", {})),
            geographic_hints=list(payload.get("geographic_hints", [])),
            quality_score=float(payload.get("quality_score", 1.0)),
            created_by=str(payload.get("created_by", "")),
            size_estimate_bytes=int(payload.get("size_estimate_bytes", 0)),
        )

    def to_cache_metadata(self) -> CacheMetadata:
        """Convert message metadata to CacheMetadata."""
        try:
            replication_policy = ReplicationStrategy(self.replication_policy)
        except ValueError:
            replication_policy = ReplicationStrategy.GEOGRAPHIC

        return CacheMetadata(
            computation_cost_ms=self.computation_cost_ms,
            dependencies={dep.to_global_cache_key() for dep in self.dependencies},
            ttl_seconds=self.ttl_seconds,
            replication_policy=replication_policy,
            access_patterns=self.access_patterns,
            geographic_hints=self.geographic_hints,
            quality_score=self.quality_score,
            created_by=self.created_by,
            size_estimate_bytes=self.size_estimate_bytes,
        )


@dataclass(frozen=True, slots=True)
class CacheEntryMessage:
    """Serializable cache entry for protocol messages."""

    key: CacheKeyMessage
    value: Any
    metadata: CacheMetadataMessage
    creation_time: float
    last_access_time: float
    access_count: int
    replication_sites: list[str] = field(default_factory=list)
    vector_clock: dict[str, int] = field(default_factory=dict)
    checksum: str = ""

    @classmethod
    def from_global_cache_entry(cls, entry: GlobalCacheEntry) -> CacheEntryMessage:
        """Convert GlobalCacheEntry to message format."""
        return cls(
            key=CacheKeyMessage.from_global_cache_key(entry.key),
            value=entry.value,
            metadata=CacheMetadataMessage.from_cache_metadata(entry.metadata),
            creation_time=entry.creation_time,
            last_access_time=entry.last_access_time,
            access_count=entry.access_count,
            replication_sites=list(entry.replication_sites),
            vector_clock=entry.vector_clock,
            checksum=entry.checksum,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "key": self.key.to_dict(),
            "value": self.value,
            "metadata": self.metadata.to_dict(),
            "creation_time": self.creation_time,
            "last_access_time": self.last_access_time,
            "access_count": self.access_count,
            "replication_sites": list(self.replication_sites),
            "vector_clock": dict(self.vector_clock),
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CacheEntryMessage:
        """Create from dictionary (JSON deserialization)."""
        key_payload = payload.get("key", {})
        metadata_payload = payload.get("metadata", {})
        return cls(
            key=CacheKeyMessage.from_dict(key_payload),
            value=payload.get("value"),
            metadata=CacheMetadataMessage.from_dict(metadata_payload),
            creation_time=float(payload.get("creation_time", 0.0)),
            last_access_time=float(payload.get("last_access_time", 0.0)),
            access_count=int(payload.get("access_count", 0)),
            replication_sites=list(payload.get("replication_sites", [])),
            vector_clock=dict(payload.get("vector_clock", {})),
            checksum=str(payload.get("checksum", "")),
        )

    def to_global_cache_entry(self) -> GlobalCacheEntry:
        """Convert message entry to GlobalCacheEntry."""
        return GlobalCacheEntry(
            key=self.key.to_global_cache_key(),
            value=self.value,
            metadata=self.metadata.to_cache_metadata(),
            creation_time=self.creation_time,
            last_access_time=self.last_access_time,
            access_count=self.access_count,
            replication_sites=set(self.replication_sites),
            vector_clock=self.vector_clock,
            checksum=self.checksum,
        )


@dataclass(frozen=True, slots=True)
class CachePerformanceMessage:
    """Serializable cache performance metrics for protocol messages."""

    lookup_time_ms: float
    network_hops: int
    cache_efficiency: float
    replication_latency_ms: float = 0.0
    conflict_resolution_time_ms: float = 0.0

    @classmethod
    def from_cache_performance(
        cls, perf: CachePerformanceMetrics
    ) -> CachePerformanceMessage:
        """Convert CachePerformanceMetrics to message format."""
        return cls(
            lookup_time_ms=perf.lookup_time_ms,
            network_hops=perf.network_hops,
            cache_efficiency=perf.cache_efficiency,
            replication_latency_ms=perf.replication_latency_ms,
            conflict_resolution_time_ms=perf.conflict_resolution_time_ms,
        )


@dataclass(frozen=True, slots=True)
class CacheRequestMessage:
    """Cache request message following MPREG protocol specification."""

    # Standard MPREG message fields
    role: str = "cache-request"
    u: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    version: str = "2.0"

    # Cache-specific fields
    operation: str = "get"  # CacheOperation enum value
    key: CacheKeyMessage = field(default_factory=lambda: CacheKeyMessage("", ""))
    options: CacheOptionsMessage = field(default_factory=CacheOptionsMessage)

    # Optional fields based on operation type
    value: Any = None
    metadata: CacheMetadataMessage | None = None
    invalidation_pattern: str = ""

    # Advanced operation fields
    atomic_operation: AtomicOperationMessage | None = None
    structure_operation: DataStructureOperationMessage | None = None
    namespace_operation: NamespaceOperationMessage | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "role": self.role,
            "u": self.u,
            "timestamp": self.timestamp,
            "version": self.version,
            "operation": self.operation,
            "key": {
                "namespace": self.key.namespace,
                "identifier": self.key.identifier,
                "version": self.key.version,
                "tags": self.key.tags,
            },
            "options": {
                "include_metadata": self.options.include_metadata,
                "consistency_level": self.options.consistency_level,
                "timeout_ms": self.options.timeout_ms,
                "cache_levels": self.options.cache_levels,
                "replication_factor": self.options.replication_factor,
                "prefer_local": self.options.prefer_local,
                "max_staleness_seconds": self.options.max_staleness_seconds,
            },
        }

        # Add optional fields if present
        if self.value is not None:
            result["value"] = self.value

        if self.metadata is not None:
            result["metadata"] = {
                "computation_cost_ms": self.metadata.computation_cost_ms,
                "dependencies": [
                    {
                        "namespace": dep.namespace,
                        "identifier": dep.identifier,
                        "version": dep.version,
                        "tags": dep.tags,
                    }
                    for dep in self.metadata.dependencies
                ],
                "ttl_seconds": self.metadata.ttl_seconds,
                "replication_policy": self.metadata.replication_policy,
                "access_patterns": self.metadata.access_patterns,
                "geographic_hints": self.metadata.geographic_hints,
                "quality_score": self.metadata.quality_score,
                "created_by": self.metadata.created_by,
                "size_estimate_bytes": self.metadata.size_estimate_bytes,
            }

        if self.invalidation_pattern:
            result["invalidation_pattern"] = self.invalidation_pattern

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CacheRequestMessage:
        """Create from dictionary (JSON deserialization)."""
        key_data = data["key"]
        key = CacheKeyMessage(
            namespace=key_data["namespace"],
            identifier=key_data["identifier"],
            version=key_data.get("version", "v1.0.0"),
            tags=key_data.get("tags", []),
        )

        options_data = data["options"]
        options = CacheOptionsMessage(
            include_metadata=options_data.get("include_metadata", True),
            consistency_level=options_data.get("consistency_level", "eventual"),
            timeout_ms=options_data.get("timeout_ms", 5000),
            cache_levels=options_data.get("cache_levels", ["L1", "L2", "L3"]),
            replication_factor=options_data.get("replication_factor", 2),
            prefer_local=options_data.get("prefer_local", True),
            max_staleness_seconds=options_data.get("max_staleness_seconds", 300.0),
        )

        metadata = None
        if "metadata" in data:
            metadata_data = data["metadata"]
            dependencies = []
            for dep_data in metadata_data.get("dependencies", []):
                dep_key = CacheKeyMessage(
                    namespace=dep_data["namespace"],
                    identifier=dep_data["identifier"],
                    version=dep_data.get("version", "v1.0.0"),
                    tags=dep_data.get("tags", []),
                )
                dependencies.append(dep_key)

            metadata = CacheMetadataMessage(
                computation_cost_ms=metadata_data.get("computation_cost_ms", 0.0),
                dependencies=dependencies,
                ttl_seconds=metadata_data.get("ttl_seconds"),
                replication_policy=metadata_data.get(
                    "replication_policy", "geographic"
                ),
                access_patterns=metadata_data.get("access_patterns", {}),
                geographic_hints=metadata_data.get("geographic_hints", []),
                quality_score=metadata_data.get("quality_score", 1.0),
                created_by=metadata_data.get("created_by", ""),
                size_estimate_bytes=metadata_data.get("size_estimate_bytes", 0),
            )

        return cls(
            role=data.get("role", "cache-request"),
            u=data.get("u", str(uuid.uuid4())),
            timestamp=data.get("timestamp", time.time()),
            version=data.get("version", "2.0"),
            operation=data["operation"],
            key=key,
            options=options,
            value=data.get("value"),
            metadata=metadata,
            invalidation_pattern=data.get("invalidation_pattern", ""),
        )


@dataclass(frozen=True, slots=True)
class CacheResponseMessage:
    """Cache response message following MPREG protocol specification."""

    # Standard MPREG message fields
    role: str = "cache-response"
    u: str = ""  # Should match request ID
    timestamp: float = field(default_factory=time.time)
    version: str = "2.0"

    # Cache response fields
    status: str = "miss"  # CacheResponseStatus enum value
    cache_level: str | None = None  # CacheLevel enum value
    entry: CacheEntryMessage | None = None
    performance: CachePerformanceMessage | None = None
    error_message: str | None = None
    operation_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "role": self.role,
            "u": self.u,
            "timestamp": self.timestamp,
            "version": self.version,
            "status": self.status,
            "operation_id": self.operation_id,
        }

        # Add optional fields if present
        if self.cache_level is not None:
            result["cache_level"] = self.cache_level

        if self.entry is not None:
            result["entry"] = {
                "key": {
                    "namespace": self.entry.key.namespace,
                    "identifier": self.entry.key.identifier,
                    "version": self.entry.key.version,
                    "tags": self.entry.key.tags,
                },
                "value": self.entry.value,
                "creation_time": self.entry.creation_time,
                "access_count": self.entry.access_count,
                "last_access_time": self.entry.last_access_time,
                "computation_cost_ms": self.entry.metadata.computation_cost_ms,
                "size_bytes": self.entry.metadata.size_estimate_bytes,
                "dependencies": [
                    f"{dep.namespace}:{dep.identifier}:{dep.version}"
                    for dep in self.entry.metadata.dependencies
                ],
                "ttl_seconds": self.entry.metadata.ttl_seconds,
            }

        if self.performance is not None:
            result["performance"] = {
                "lookup_time_ms": self.performance.lookup_time_ms,
                "network_hops": self.performance.network_hops,
                "cache_efficiency": self.performance.cache_efficiency,
            }

        if self.error_message is not None:
            result["error_message"] = self.error_message

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CacheResponseMessage:
        """Create from dictionary (JSON deserialization)."""
        entry = None
        if "entry" in data:
            entry_data = data["entry"]
            key_data = entry_data["key"]

            key = CacheKeyMessage(
                namespace=key_data["namespace"],
                identifier=key_data["identifier"],
                version=key_data.get("version", "v1.0.0"),
                tags=key_data.get("tags", []),
            )

            # Create minimal metadata from entry data
            metadata = CacheMetadataMessage(
                computation_cost_ms=entry_data.get("computation_cost_ms", 0.0),
                ttl_seconds=entry_data.get("ttl_seconds"),
                size_estimate_bytes=entry_data.get("size_bytes", 0),
            )

            entry = CacheEntryMessage(
                key=key,
                value=entry_data["value"],
                metadata=metadata,
                creation_time=entry_data["creation_time"],
                last_access_time=entry_data["last_access_time"],
                access_count=entry_data["access_count"],
            )

        performance = None
        if "performance" in data:
            perf_data = data["performance"]
            performance = CachePerformanceMessage(
                lookup_time_ms=perf_data["lookup_time_ms"],
                network_hops=perf_data["network_hops"],
                cache_efficiency=perf_data["cache_efficiency"],
            )

        return cls(
            role=data.get("role", "cache-response"),
            u=data.get("u", ""),
            timestamp=data.get("timestamp", time.time()),
            version=data.get("version", "2.0"),
            status=data["status"],
            cache_level=data.get("cache_level"),
            entry=entry,
            performance=performance,
            error_message=data.get("error_message"),
            operation_id=data.get("operation_id", str(uuid.uuid4())),
        )


@dataclass(frozen=True, slots=True)
class CacheAnalyticsMessage:
    """Cache analytics message for performance monitoring."""

    # Standard MPREG message fields
    role: str = "cache-analytics"
    u: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    version: str = "2.0"

    # Analytics fields
    period_start: float = 0.0
    period_end: float = 0.0
    duration_seconds: float = 0.0

    # Operation statistics
    operations: dict[str, int] = field(default_factory=dict)

    # Performance metrics
    performance: dict[str, float] = field(default_factory=dict)

    # Cost savings
    cost_savings: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "role": self.role,
            "u": self.u,
            "timestamp": self.timestamp,
            "version": self.version,
            "period": {
                "start": self.period_start,
                "end": self.period_end,
                "duration_seconds": self.duration_seconds,
            },
            "statistics": {
                "operations": self.operations,
                "performance": self.performance,
                "cost_savings": self.cost_savings,
            },
        }


class CacheProtocolHandler:
    """Handler for cache protocol messages."""

    def __init__(
        self,
        cache_manager: CacheManagerProtocol | None = None,
        advanced_cache_ops=None,
    ):
        self.cache_manager = cache_manager
        self.advanced_cache_ops = advanced_cache_ops

        # Message handlers
        self.handlers = {
            CacheMessageRole.CACHE_REQUEST.value: self.handle_cache_request,
            CacheMessageRole.CACHE_RESPONSE.value: self.handle_cache_response,
            CacheMessageRole.CACHE_ANALYTICS.value: self.handle_cache_analytics,
        }

    async def handle_message(
        self, message_data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Handle incoming cache protocol message."""
        role = message_data.get("role")
        if role is None:
            return None
        handler = self.handlers.get(role)

        if handler:
            return await handler(message_data)
        else:
            return None

    async def handle_cache_request(
        self, message_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle cache request message."""
        try:
            request = CacheRequestMessage.from_dict(message_data)

            if not self.cache_manager:
                # Return error if no cache manager available
                response = CacheResponseMessage(
                    u=request.u,
                    status=CacheResponseStatus.ERROR.value,
                    error_message="Cache manager not available",
                )
                return response.to_dict()

            # Convert message types to internal types
            key = request.key.to_global_cache_key()
            options = request.options.to_cache_options()

            # Process operation
            if request.operation == CacheOperation.GET.value:
                result = await self.cache_manager.get(key, options)

            elif request.operation == CacheOperation.PUT.value:
                if request.value is None:
                    response = CacheResponseMessage(
                        u=request.u,
                        status=CacheResponseStatus.ERROR.value,
                        error_message="PUT operation requires value",
                    )
                    return response.to_dict()

                # Convert metadata if present
                metadata = None
                if request.metadata:
                    metadata = request.metadata.to_cache_metadata()

                result = await self.cache_manager.put(
                    key, request.value, metadata, options
                )

            elif request.operation == CacheOperation.DELETE.value:
                result = await self.cache_manager.delete(key, options)

            elif request.operation == CacheOperation.INVALIDATE.value:
                result = await self.cache_manager.invalidate(
                    request.invalidation_pattern, options
                )

            elif request.operation == CacheOperation.QUERY.value:
                pattern = request.invalidation_pattern or "*"
                namespace = request.key.namespace
                matched_keys = self.cache_manager.get_namespace_keys(
                    namespace, pattern=pattern
                )
                result_entry = GlobalCacheEntry(
                    key=key,
                    value=[str(k) for k in matched_keys],
                    metadata=CacheMetadata(),
                )
                result = CacheOperationResult(success=True, entry=result_entry)

            elif request.operation == CacheOperation.STATS.value:
                stats = self.cache_manager.get_statistics()
                result_entry = GlobalCacheEntry(
                    key=key,
                    value=stats,
                    metadata=CacheMetadata(),
                )
                result = CacheOperationResult(success=True, entry=result_entry)

            elif request.operation == CacheOperation.ATOMIC.value:
                if not self.advanced_cache_ops or not request.atomic_operation:
                    response = CacheResponseMessage(
                        u=request.u,
                        status=CacheResponseStatus.ERROR.value,
                        error_message="Advanced cache operations not available or missing atomic_operation",
                    )
                    return response.to_dict()

                # Convert AtomicOperationMessage to AtomicOperationRequest
                # Map operation_type to AtomicOperation enum
                from .advanced_cache_ops import AtomicOperation

                atomic_op = (
                    AtomicOperation.TEST_AND_SET
                )  # Default, should map from operation_type
                if request.atomic_operation.operation_type == "compare_and_swap":
                    atomic_op = AtomicOperation.COMPARE_AND_SWAP
                elif request.atomic_operation.operation_type == "increment":
                    atomic_op = AtomicOperation.INCREMENT
                elif request.atomic_operation.operation_type == "decrement":
                    atomic_op = AtomicOperation.DECREMENT
                elif request.atomic_operation.operation_type == "append":
                    atomic_op = AtomicOperation.APPEND
                elif request.atomic_operation.operation_type == "prepend":
                    atomic_op = AtomicOperation.PREPEND

                atomic_request = AtomicOperationRequest(
                    operation=atomic_op,
                    key=key,
                    expected_value=request.atomic_operation.expected_value,
                    new_value=request.atomic_operation.new_value,
                    delta=request.atomic_operation.increment_by,
                    data=request.atomic_operation.new_value,
                    ttl_seconds=request.atomic_operation.ttl_seconds,
                    if_not_exists=request.atomic_operation.create_if_missing,
                )

                atomic_result = await self.advanced_cache_ops.atomic_operation(
                    atomic_request, options
                )
                # Convert atomic result to cache operation result format
                result = CacheOperationResult(
                    success=atomic_result.success,
                    error_message=atomic_result.error_message,
                    # Note: atomic operations don't return cache entries directly
                )

            elif request.operation == CacheOperation.STRUCTURE.value:
                if not self.advanced_cache_ops or not request.structure_operation:
                    response = CacheResponseMessage(
                        u=request.u,
                        status=CacheResponseStatus.ERROR.value,
                        error_message="Advanced cache operations not available or missing structure_operation",
                    )
                    return response.to_dict()

                # Convert DataStructureOperationMessage to DataStructureOperation
                from .advanced_cache_ops import DataStructureType

                # Map structure type string to enum
                structure_type_enum = DataStructureType.SET  # Default
                if request.structure_operation.structure_type == "list":
                    structure_type_enum = DataStructureType.LIST
                elif request.structure_operation.structure_type == "map":
                    structure_type_enum = DataStructureType.MAP
                elif request.structure_operation.structure_type == "sorted_set":
                    structure_type_enum = DataStructureType.SORTED_SET
                elif request.structure_operation.structure_type == "counter":
                    structure_type_enum = DataStructureType.COUNTER
                elif request.structure_operation.structure_type == "queue":
                    structure_type_enum = DataStructureType.QUEUE
                elif request.structure_operation.structure_type == "stack":
                    structure_type_enum = DataStructureType.STACK

                structure_op = DataStructureOperation(
                    structure_type=structure_type_enum,
                    operation=request.structure_operation.operation,
                    key=key,
                    member=request.structure_operation.values[0]
                    if request.structure_operation.values
                    else None,
                    index=request.structure_operation.index,
                )

                structure_result = (
                    await self.advanced_cache_ops.data_structure_operation(
                        structure_op, options
                    )
                )
                # Convert structure result to cache operation result format
                result = CacheOperationResult(
                    success=structure_result.success,
                    error_message=structure_result.error_message,
                )

            elif request.operation == CacheOperation.NAMESPACE.value:
                if not self.advanced_cache_ops or not request.namespace_operation:
                    response = CacheResponseMessage(
                        u=request.u,
                        status=CacheResponseStatus.ERROR.value,
                        error_message="Advanced cache operations not available or missing namespace_operation",
                    )
                    return response.to_dict()

                # Convert NamespaceOperationMessage to NamespaceOperation
                namespace_op = NamespaceOperation(
                    namespace=request.namespace_operation.namespace,
                    operation=request.namespace_operation.operation_type,
                    pattern=request.namespace_operation.pattern,
                    limit=request.namespace_operation.max_entries or 1000,
                    include_metadata=request.namespace_operation.include_detailed_breakdown,
                )

                namespace_result = await self.advanced_cache_ops.namespace_operation(
                    namespace_op, options
                )
                # Convert namespace result to cache operation result format
                result = CacheOperationResult(
                    success=namespace_result.success,
                    error_message=namespace_result.error_message,
                )

            else:
                response = CacheResponseMessage(
                    u=request.u,
                    status=CacheResponseStatus.ERROR.value,
                    error_message=f"Unsupported operation: {request.operation}",
                )
                return response.to_dict()

            # Convert result to response message
            response = self._create_response_from_result(request.u, result)
            return response.to_dict()

        except Exception as e:
            response = CacheResponseMessage(
                u=message_data.get("u", ""),
                status=CacheResponseStatus.ERROR.value,
                error_message=f"Cache request error: {e}",
            )
            return response.to_dict()

    async def handle_cache_response(
        self, message_data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Handle cache response message."""
        # Cache responses are typically handled by the requesting client
        # This would be used for logging or monitoring
        return None

    async def handle_cache_analytics(
        self, message_data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Handle cache analytics message."""
        # Analytics messages are typically for monitoring systems
        # This would be used for storing analytics data
        return None

    def _create_response_from_result(
        self, request_id: str, result: CacheOperationResult
    ) -> CacheResponseMessage:
        """Create response message from cache operation result."""
        if result.success:
            status = (
                CacheResponseStatus.HIT.value
                if result.entry
                else CacheResponseStatus.MISS.value
            )

            entry_message = None
            if result.entry:
                entry_message = CacheEntryMessage.from_global_cache_entry(result.entry)

            performance_message = None
            if result.performance:
                performance_message = CachePerformanceMessage.from_cache_performance(
                    result.performance
                )

            return CacheResponseMessage(
                u=request_id,
                status=status,
                cache_level=result.cache_level.value if result.cache_level else None,
                entry=entry_message,
                performance=performance_message,
            )
        else:
            return CacheResponseMessage(
                u=request_id,
                status=CacheResponseStatus.ERROR.value,
                error_message=result.error_message,
            )
