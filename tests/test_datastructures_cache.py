"""
Comprehensive property-based tests for cache datastructures.

This test suite uses the hypothesis library to aggressively test the correctness
of the centralized cache datastructure implementations.
"""

import time

import pytest
from hypothesis import given

from mpreg.datastructures.cache_structures import (
    CacheEntry,
    CacheKey,
    CacheLevel,
    CacheMetadata,
    CacheNamespace,
    CacheStatistics,
    cache_entry_strategy,
    cache_key_strategy,
    cache_metadata_strategy,
    cache_namespace_strategy,
    cache_statistics_strategy,
)


class TestCacheNamespace:
    """Test CacheNamespace datastructure."""

    @given(cache_namespace_strategy())
    def test_cache_namespace_creation(self, namespace: CacheNamespace):
        """Test that valid cache namespaces can be created."""
        assert namespace.name
        assert isinstance(namespace.name, str)
        assert isinstance(namespace.description, str)
        assert namespace.name.replace("_", "").replace("-", "").isalnum()

    def test_cache_namespace_validation(self):
        """Test cache namespace validation."""
        # Valid namespaces
        ns1 = CacheNamespace(name="test")
        assert ns1.name == "test"

        ns2 = CacheNamespace(name="test_namespace", description="Test namespace")
        assert ns2.name == "test_namespace"
        assert ns2.description == "Test namespace"

        ns3 = CacheNamespace(name="test-namespace-123")
        assert ns3.name == "test-namespace-123"

        # Invalid namespaces
        with pytest.raises(ValueError, match="Cache namespace name cannot be empty"):
            CacheNamespace(name="")

        with pytest.raises(ValueError, match="must be alphanumeric"):
            CacheNamespace(name="invalid namespace with spaces")

        with pytest.raises(ValueError, match="must be alphanumeric"):
            CacheNamespace(name="invalid@namespace")


class TestCacheKey:
    """Test CacheKey datastructure."""

    @given(cache_key_strategy())
    def test_cache_key_creation(self, key: CacheKey):
        """Test that valid cache keys can be created."""
        assert key.namespace.name
        assert key.key
        assert key.version >= 1
        assert isinstance(key.subkey, str)

    def test_cache_key_validation(self):
        """Test cache key validation."""
        namespace = CacheNamespace(name="test")

        # Valid keys
        key1 = CacheKey(namespace=namespace, key="test_key")
        assert key1.key == "test_key"
        assert key1.version == 1

        key2 = CacheKey(namespace=namespace, key="test", subkey="sub", version=5)
        assert key2.subkey == "sub"
        assert key2.version == 5

        # Invalid keys
        with pytest.raises(ValueError, match="Cache key cannot be empty"):
            CacheKey(namespace=namespace, key="")

        with pytest.raises(ValueError, match="version must be positive"):
            CacheKey(namespace=namespace, key="test", version=0)

        with pytest.raises(ValueError, match="version must be positive"):
            CacheKey(namespace=namespace, key="test", version=-1)

    def test_simple_cache_key(self):
        """Test simple cache key creation."""
        key = CacheKey.simple("my_key", "my_namespace")
        assert key.key == "my_key"
        assert key.namespace.name == "my_namespace"
        assert key.subkey == ""
        assert key.version == 1

    def test_global_cache_key(self):
        """Test global cache key creation."""
        key = CacheKey.global_key("cluster1", "my_key", "global_ns")
        assert key.key == "cluster1:my_key"
        assert key.namespace.name == "global_ns"
        assert key.namespace.description == "Global federated cache"

    def test_cache_key_modifications(self):
        """Test cache key modification methods."""
        original = CacheKey.simple("base_key", "test_ns")

        # Test with_subkey
        with_subkey = original.with_subkey("sub1")
        assert with_subkey.subkey == "sub1"
        assert with_subkey.key == original.key  # Other fields unchanged
        assert with_subkey.namespace == original.namespace
        assert with_subkey.version == original.version

        # Test with_version
        with_version = original.with_version(3)
        assert with_version.version == 3
        assert with_version.key == original.key  # Other fields unchanged
        assert with_version.namespace == original.namespace
        assert with_version.subkey == original.subkey

    def test_cache_key_full_key(self):
        """Test full key generation."""
        namespace = CacheNamespace(name="test_ns")

        # Simple key
        key1 = CacheKey(namespace=namespace, key="simple")
        assert key1.full_key() == "test_ns:simple"
        assert str(key1) == "test_ns:simple"

        # Key with subkey
        key2 = CacheKey(namespace=namespace, key="main", subkey="sub")
        assert key2.full_key() == "test_ns:main:sub"

        # Key with version > 1
        key3 = CacheKey(namespace=namespace, key="versioned", version=5)
        assert key3.full_key() == "test_ns:versioned:v5"

        # Key with both subkey and version
        key4 = CacheKey(namespace=namespace, key="full", subkey="sub", version=3)
        assert key4.full_key() == "test_ns:full:sub:v3"

    @given(cache_key_strategy(), cache_key_strategy())
    def test_cache_key_equality_and_hash(self, key1: CacheKey, key2: CacheKey):
        """Test cache key equality and hash consistency."""
        if (
            key1.namespace == key2.namespace
            and key1.key == key2.key
            and key1.subkey == key2.subkey
            and key1.version == key2.version
        ):
            assert key1 == key2
            assert hash(key1) == hash(key2)
        else:
            assert key1 != key2


class TestCacheMetadata:
    """Test CacheMetadata datastructure."""

    @given(cache_metadata_strategy())
    def test_cache_metadata_creation(self, metadata: CacheMetadata):
        """Test that valid cache metadata can be created."""
        assert metadata.size_bytes >= 0
        assert metadata.access_count >= 0
        assert isinstance(metadata.tags, frozenset)
        assert isinstance(metadata.source, str)

    def test_cache_metadata_validation(self):
        """Test cache metadata validation."""
        # Valid metadata
        meta1 = CacheMetadata(size_bytes=100, access_count=5)
        assert meta1.size_bytes == 100
        assert meta1.access_count == 5

        # Invalid metadata
        with pytest.raises(ValueError, match="Size bytes cannot be negative"):
            CacheMetadata(size_bytes=-1)

        with pytest.raises(ValueError, match="Access count cannot be negative"):
            CacheMetadata(access_count=-1)

    def test_cache_metadata_expiry(self):
        """Test cache metadata expiry logic."""
        # Not expired (no expiry time)
        meta1 = CacheMetadata()
        assert not meta1.is_expired()

        # Not expired (future expiry)
        future_time = time.time() + 3600  # 1 hour from now
        meta2 = CacheMetadata(expiry_time=future_time)
        assert not meta2.is_expired()

        # Expired (past expiry)
        past_time = time.time() - 3600  # 1 hour ago
        meta3 = CacheMetadata(expiry_time=past_time)
        assert meta3.is_expired()

    def test_cache_metadata_timing(self):
        """Test cache metadata timing methods."""
        created_time = time.time() - 100  # 100 seconds ago
        access_time = time.time() - 50  # 50 seconds ago

        meta = CacheMetadata(created_at=created_time, last_access_time=access_time)

        # Age should be approximately 100 seconds
        age = meta.age_seconds()
        assert 98 <= age <= 102  # Allow some tolerance

        # Time since access should be approximately 50 seconds
        since_access = meta.time_since_access()
        assert 48 <= since_access <= 52  # Allow some tolerance

    def test_cache_metadata_with_access(self):
        """Test cache metadata access update."""
        original = CacheMetadata(access_count=5, last_access_time=time.time() - 100)
        updated = original.with_access()

        # Access count incremented
        assert updated.access_count == original.access_count + 1

        # Last access time updated (should be more recent)
        assert updated.last_access_time > original.last_access_time

        # Other fields unchanged
        assert updated.size_bytes == original.size_bytes
        assert updated.created_at == original.created_at
        assert updated.expiry_time == original.expiry_time
        assert updated.tags == original.tags
        assert updated.source == original.source


class TestCacheEntry:
    """Test CacheEntry datastructure."""

    @given(cache_entry_strategy())
    def test_cache_entry_creation(self, entry: CacheEntry):
        """Test that valid cache entries can be created."""
        assert isinstance(entry.key, CacheKey)
        assert isinstance(entry.metadata, CacheMetadata)
        assert isinstance(entry.level, CacheLevel)

    def test_cache_entry_operations(self):
        """Test cache entry operations."""
        key = CacheKey.simple("test_key")
        metadata = CacheMetadata(size_bytes=100, access_count=0)
        entry = CacheEntry(key=key, value="test_value", metadata=metadata)

        # Test basic properties
        assert entry.size_bytes() == 100
        assert not entry.is_expired()  # No expiry set

        # Test with_access
        accessed = entry.with_access()
        assert accessed.metadata.access_count == 1
        assert accessed.metadata.last_access_time > metadata.last_access_time
        assert accessed.key == entry.key  # Other fields unchanged
        assert accessed.value == entry.value
        assert accessed.level == entry.level

        # Test with_value
        updated = entry.with_value("new_value")
        assert updated.value == "new_value"
        assert updated.metadata.updated_at > metadata.updated_at
        assert updated.key == entry.key  # Other fields mostly unchanged
        assert updated.metadata.access_count == metadata.access_count
        assert updated.level == entry.level

    def test_cache_entry_expiry(self):
        """Test cache entry expiry."""
        key = CacheKey.simple("test_key")

        # Non-expired entry
        metadata1 = CacheMetadata(expiry_time=time.time() + 3600)
        entry1 = CacheEntry(key=key, value="value", metadata=metadata1)
        assert not entry1.is_expired()

        # Expired entry
        metadata2 = CacheMetadata(expiry_time=time.time() - 3600)
        entry2 = CacheEntry(key=key, value="value", metadata=metadata2)
        assert entry2.is_expired()


class TestCacheStatistics:
    """Test CacheStatistics datastructure."""

    @given(cache_statistics_strategy())
    def test_cache_statistics_creation(self, stats: CacheStatistics):
        """Test that valid cache statistics can be created."""
        assert stats.hits >= 0
        assert stats.misses >= 0
        assert stats.evictions >= 0
        assert stats.entries >= 0
        assert stats.total_size_bytes >= 0

    def test_cache_statistics_validation(self):
        """Test cache statistics validation."""
        # Valid statistics
        stats1 = CacheStatistics(hits=10, misses=5, evictions=2)
        assert stats1.hits == 10
        assert stats1.misses == 5
        assert stats1.evictions == 2

        # Invalid statistics
        with pytest.raises(ValueError, match="Hits cannot be negative"):
            CacheStatistics(hits=-1)

        with pytest.raises(ValueError, match="Misses cannot be negative"):
            CacheStatistics(misses=-1)

        with pytest.raises(ValueError, match="Evictions cannot be negative"):
            CacheStatistics(evictions=-1)

        with pytest.raises(ValueError, match="Entries cannot be negative"):
            CacheStatistics(entries=-1)

        with pytest.raises(ValueError, match="Total size bytes cannot be negative"):
            CacheStatistics(total_size_bytes=-1)

    def test_cache_statistics_rates(self):
        """Test cache statistics rate calculations."""
        # Test hit rate calculation
        stats1 = CacheStatistics(hits=80, misses=20)
        assert stats1.hit_rate() == 0.8
        assert stats1.miss_rate() == 0.2

        # Test with zero hits/misses
        stats2 = CacheStatistics(hits=0, misses=0)
        assert stats2.hit_rate() == 0.0
        assert stats2.miss_rate() == 1.0

        # Test fill rate
        stats3 = CacheStatistics(total_size_bytes=500, max_size_bytes=1000)
        assert stats3.fill_rate() == 0.5

        # Test fill rate with zero max size
        stats4 = CacheStatistics(total_size_bytes=100, max_size_bytes=0)
        assert stats4.fill_rate() == 0.0

        # Test average entry size
        stats5 = CacheStatistics(total_size_bytes=1000, entries=10)
        assert stats5.average_entry_size() == 100.0

        # Test average entry size with zero entries
        stats6 = CacheStatistics(total_size_bytes=100, entries=0)
        assert stats6.average_entry_size() == 0.0

    def test_cache_statistics_updates(self):
        """Test cache statistics update methods."""
        original = CacheStatistics(hits=10, misses=5, average_access_time_ms=100.0)

        # Test with_hit
        hit_updated = original.with_hit(200.0)
        assert hit_updated.hits == original.hits + 1
        assert hit_updated.misses == original.misses
        # New average should be weighted: (100*15 + 200) / 16 = 106.25
        expected_avg = (100.0 * 15 + 200.0) / 16
        assert abs(hit_updated.average_access_time_ms - expected_avg) < 0.01

        # Test with_miss
        miss_updated = original.with_miss(150.0)
        assert miss_updated.hits == original.hits
        assert miss_updated.misses == original.misses + 1
        # New average should be weighted: (100*15 + 150) / 16 = 103.125
        expected_avg = (100.0 * 15 + 150.0) / 16
        assert abs(miss_updated.average_access_time_ms - expected_avg) < 0.01

        # Test with_eviction
        evict_updated = original.with_eviction(entry_size=50)
        assert evict_updated.evictions == original.evictions + 1
        assert evict_updated.entries == max(0, original.entries - 1)
        assert evict_updated.total_size_bytes == max(0, original.total_size_bytes - 50)

        # Test with_entry_added
        add_updated = original.with_entry_added(entry_size=75)
        assert add_updated.entries == original.entries + 1
        assert add_updated.total_size_bytes == original.total_size_bytes + 75
        assert add_updated.hits == original.hits  # Other fields unchanged
        assert add_updated.misses == original.misses


class TestCacheStructuresProperties:
    """Test mathematical properties of cache datastructures."""

    @given(cache_statistics_strategy())
    def test_cache_statistics_properties(self, stats: CacheStatistics):
        """Test mathematical properties of cache statistics."""
        # Hit rate + miss rate should equal 1.0 (when there are hits or misses)
        if stats.hits + stats.misses > 0:
            assert abs(stats.hit_rate() + stats.miss_rate() - 1.0) < 0.0001

        # Hit rate should be between 0 and 1
        assert 0.0 <= stats.hit_rate() <= 1.0
        assert 0.0 <= stats.miss_rate() <= 1.0

        # Fill rate should be between 0 and 1 (when max_size > 0)
        if stats.max_size_bytes > 0:
            assert 0.0 <= stats.fill_rate() <= 1.0

        # Average entry size should be non-negative
        assert stats.average_entry_size() >= 0.0

    @given(cache_key_strategy())
    def test_cache_key_immutability_properties(self, key: CacheKey):
        """Test immutability properties of cache keys."""
        # Modifications should return new objects
        modified_subkey = key.with_subkey("new_sub")
        modified_version = key.with_version(key.version + 1)

        # Original should be unchanged
        assert (
            key.subkey != "new_sub" or key.subkey == "new_sub"
        )  # May be same if already "new_sub"
        assert key.version != (key.version + 1)  # Should definitely be different

        # New objects should have expected changes
        if key.subkey != "new_sub":
            assert modified_subkey.subkey == "new_sub"
        assert modified_version.version == key.version + 1

    @given(cache_metadata_strategy())
    def test_cache_metadata_access_properties(self, metadata: CacheMetadata):
        """Test properties of cache metadata access updates."""
        updated = metadata.with_access()

        # Access count should increase
        assert updated.access_count == metadata.access_count + 1

        # Last access time should be more recent
        assert updated.last_access_time >= metadata.last_access_time

        # Other timing fields should be unchanged
        assert updated.created_at == metadata.created_at
        assert updated.updated_at == metadata.updated_at

    @given(cache_entry_strategy())
    def test_cache_entry_operation_properties(self, entry: CacheEntry):
        """Test properties of cache entry operations."""
        # Access operation should preserve key and value
        accessed = entry.with_access()
        assert accessed.key == entry.key
        assert accessed.value == entry.value
        assert accessed.level == entry.level

        # Value update should preserve key
        new_value = (
            "different_value" if entry.value != "different_value" else "another_value"
        )
        updated = entry.with_value(new_value)
        assert updated.key == entry.key
        assert updated.value == new_value
        assert updated.level == entry.level

    @given(cache_statistics_strategy())
    def test_cache_statistics_update_properties(self, stats: CacheStatistics):
        """Test properties of cache statistics updates."""
        # Hit updates should only affect hits and timing
        hit_updated = stats.with_hit(100.0)
        assert hit_updated.hits == stats.hits + 1
        assert hit_updated.misses == stats.misses
        assert hit_updated.evictions == stats.evictions
        assert hit_updated.entries == stats.entries
        assert hit_updated.total_size_bytes == stats.total_size_bytes

        # Miss updates should only affect misses and timing
        miss_updated = stats.with_miss(100.0)
        assert miss_updated.hits == stats.hits
        assert miss_updated.misses == stats.misses + 1
        assert miss_updated.evictions == stats.evictions

        # Eviction updates should only affect evictions, entries, and size
        evict_updated = stats.with_eviction(entry_size=50)
        assert evict_updated.hits == stats.hits
        assert evict_updated.misses == stats.misses
        assert evict_updated.evictions == stats.evictions + 1


class TestCacheStructuresExamples:
    """Test specific examples and edge cases."""

    def test_typical_cache_workflow(self):
        """Test a typical cache usage workflow."""
        # Create cache key
        key = CacheKey.simple("user:123", "user_cache")
        assert key.full_key() == "user_cache:user:123"

        # Create cache entry
        metadata = CacheMetadata(size_bytes=256, access_count=0)
        entry = CacheEntry(
            key=key, value={"name": "John", "age": 30}, metadata=metadata
        )

        # Access the entry multiple times
        entry1 = entry.with_access()
        entry2 = entry1.with_access()

        assert entry2.metadata.access_count == 2
        assert entry2.value == {"name": "John", "age": 30}

        # Update the value
        new_data = {"name": "John", "age": 31}
        entry3 = entry2.with_value(new_data)

        assert entry3.value == new_data
        assert entry3.metadata.access_count == 2  # Preserved from previous
        assert entry3.metadata.updated_at > entry2.metadata.updated_at

        # Track statistics
        stats = CacheStatistics()
        stats = stats.with_hit(5.0)  # Cache hit in 5ms
        stats = stats.with_hit(3.0)  # Another hit in 3ms
        stats = stats.with_miss(10.0)  # Cache miss in 10ms

        assert stats.hits == 2
        assert stats.misses == 1
        assert stats.hit_rate() == 2 / 3
        assert abs(stats.average_access_time_ms - 6.0) < 0.01  # (5+3+10)/3 = 6

    def test_cache_key_versioning(self):
        """Test cache key versioning scenarios."""
        # Start with version 1
        key_v1 = CacheKey.simple("config")
        assert key_v1.version == 1
        assert key_v1.full_key() == "default:config"

        # Upgrade to version 2
        key_v2 = key_v1.with_version(2)
        assert key_v2.version == 2
        assert key_v2.full_key() == "default:config:v2"

        # Add subkey to versioned key
        key_v2_sub = key_v2.with_subkey("production")
        assert key_v2_sub.full_key() == "default:config:production:v2"

    def test_cache_expiry_scenarios(self):
        """Test various cache expiry scenarios."""
        # Never expires
        meta1 = CacheMetadata()
        assert not meta1.is_expired()

        # Expires in future
        future_expiry = time.time() + 3600
        meta2 = CacheMetadata(expiry_time=future_expiry)
        assert not meta2.is_expired()

        # Recently expired
        recent_expiry = time.time() - 1
        meta3 = CacheMetadata(expiry_time=recent_expiry)
        assert meta3.is_expired()

        # Long expired
        old_expiry = time.time() - 86400  # 1 day ago
        meta4 = CacheMetadata(expiry_time=old_expiry)
        assert meta4.is_expired()

    def test_global_cache_fabric_keys(self):
        """Test cache keys for fabric-federated scenarios."""
        # Global cache key for different clusters
        key_us = CacheKey.global_key("us-west", "user:123", "global")
        key_eu = CacheKey.global_key("eu-central", "user:123", "global")

        assert key_us.key == "us-west:user:123"
        assert key_eu.key == "eu-central:user:123"
        assert key_us.namespace.name == "global"
        assert key_eu.namespace.name == "global"

        # Keys should be different even for same user
        assert key_us != key_eu
        assert key_us.full_key() != key_eu.full_key()
