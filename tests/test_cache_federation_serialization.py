import time

from mpreg.core.cache_protocol import CacheEntryMessage
from mpreg.core.global_cache import (
    CacheMetadata,
    ConsistencyLevel,
    GlobalCacheEntry,
    GlobalCacheKey,
    ReplicationStrategy,
)
from mpreg.fabric.cache_federation import (
    CacheDigest,
    CacheDigestEntry,
    CacheOperationMessage,
    CacheOperationType,
)


def test_cache_operation_message_roundtrip() -> None:
    key = GlobalCacheKey(namespace="cache", identifier="alpha", version="v1.2.3")
    dependency = GlobalCacheKey(namespace="cache", identifier="dep", version="v1.0.0")
    metadata = CacheMetadata(
        computation_cost_ms=42.0,
        dependencies={dependency},
        ttl_seconds=120.0,
        replication_policy=ReplicationStrategy.PROXIMITY,
        access_patterns={"access": "pattern"},
        geographic_hints=["us-east-1"],
        quality_score=0.8,
        created_by="node-a",
        size_estimate_bytes=256,
    )

    message = CacheOperationMessage(
        operation_type=CacheOperationType.PUT,
        operation_id="op-123",
        key=key,
        timestamp=time.time(),
        source_node="node-a",
        vector_clock={"node-a": 1},
        ttl_hops=4,
        value={"payload": "value"},
        metadata=metadata,
        invalidation_pattern="",
        consistency_level=ConsistencyLevel.STRONG,
    )

    payload = message.to_dict()
    restored = CacheOperationMessage.from_dict(payload)

    assert restored.operation_type == message.operation_type
    assert restored.operation_id == message.operation_id
    assert restored.key == message.key
    assert restored.metadata == message.metadata
    assert restored.value == message.value
    assert restored.consistency_level == message.consistency_level
    assert restored.value_hash == message.value_hash


def test_cache_digest_roundtrip() -> None:
    key = GlobalCacheKey(namespace="cache", identifier="beta", version="v2.0.0")
    entry = CacheDigestEntry(
        key=key,
        version=3,
        timestamp=1234.5,
        value_hash="digest-hash",
        size_bytes=512,
        node_id="node-b",
    )
    digest = CacheDigest(
        node_id="node-b",
        timestamp=1234.5,
        entries={str(key): entry},
        total_entries=1,
        total_size_bytes=512,
    )

    payload = digest.to_dict()
    restored = CacheDigest.from_dict(payload)

    assert restored.node_id == digest.node_id
    assert restored.total_entries == digest.total_entries
    assert restored.entries[str(key)].key == entry.key
    assert restored.entries[str(key)].value_hash == entry.value_hash


def test_cache_entry_message_roundtrip() -> None:
    key = GlobalCacheKey(namespace="cache", identifier="gamma", version="v3.1.0")
    metadata = CacheMetadata(
        computation_cost_ms=10.0,
        dependencies=set(),
        ttl_seconds=60.0,
        replication_policy=ReplicationStrategy.GEOGRAPHIC,
        access_patterns={"hits": 3},
        geographic_hints=["eu-west-1"],
        quality_score=0.9,
        created_by="node-c",
        size_estimate_bytes=128,
    )
    entry = GlobalCacheEntry(
        key=key,
        value={"payload": "data"},
        metadata=metadata,
        creation_time=1000.0,
        last_access_time=1100.0,
        access_count=2,
        replication_sites={"node-c"},
        vector_clock={"node-c": 2},
        checksum="checksum123",
    )

    message = CacheEntryMessage.from_global_cache_entry(entry)
    payload = message.to_dict()
    restored_message = CacheEntryMessage.from_dict(payload)
    rebuilt_entry = restored_message.to_global_cache_entry()

    assert rebuilt_entry.key == entry.key
    assert rebuilt_entry.value == entry.value
    assert rebuilt_entry.metadata == entry.metadata
    assert rebuilt_entry.replication_sites == entry.replication_sites
    assert rebuilt_entry.vector_clock == entry.vector_clock
    assert rebuilt_entry.checksum == entry.checksum
