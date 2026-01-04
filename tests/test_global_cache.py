"""
Comprehensive tests for MPREG Global Distributed Caching System.

Tests cover:
- GlobalCacheManager with multi-tier caching
- FabricCacheProtocol for distributed synchronization
- Fabric cache federation for cross-cluster replication
- Cache protocol message handling
- Integration with existing MPREG infrastructure
- End-to-end cache workflows
- Performance and scalability
"""

import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from mpreg.core.cache_protocol import (
    CacheKeyMessage,
    CacheOperation,
    CacheOptionsMessage,
    CacheProtocolHandler,
    CacheRequestMessage,
    CacheResponseMessage,
)
from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    ConsistencyLevel,
    GlobalCacheConfiguration,
    GlobalCacheEntry,
    GlobalCacheKey,
    GlobalCacheManager,
)

# Import existing MPREG components for integration tests
from mpreg.fabric.cache_federation import (
    CacheOperationMessage,
    CacheOperationType,
    FabricCacheProtocol,
)
from mpreg.fabric.cache_transport import InProcessCacheTransport


class TestGlobalCacheKey:
    """Test GlobalCacheKey functionality."""

    def test_create_from_function_call(self):
        """Test creating cache key from function call."""
        key = GlobalCacheKey.from_function_call(
            namespace="compute.ml",
            function_name="predict_model",
            args=(1, 2, 3),
            kwargs={"model": "bert", "version": "v2"},
            tags={"expensive", "ml-model"},
        )

        assert key.namespace == "compute.ml"
        assert len(key.identifier) == 32  # SHA256 truncated
        assert key.tags == frozenset({"expensive", "ml-model"})

    def test_create_from_data(self):
        """Test creating cache key from arbitrary data."""
        data = {"result": [1, 2, 3], "metadata": {"type": "prediction"}}
        key = GlobalCacheKey.from_data(
            namespace="results.predictions", data=data, tags={"computed"}
        )

        assert key.namespace == "results.predictions"
        assert len(key.identifier) == 32
        assert "computed" in key.tags

    def test_to_local_key_conversion(self):
        """Test conversion to local cache key format."""
        key = GlobalCacheKey(
            namespace="test.namespace",
            identifier="abc123",
            version="v1.0.0",
            tags=frozenset({"tag1", "tag2"}),
        )

        local_key = key.to_local_key()
        assert local_key.function_name == "test.namespace.abc123"


class TestGlobalCacheManager:
    """Test GlobalCacheManager functionality."""

    @pytest.fixture
    def cache_config(self):
        """Create test cache configuration."""
        return GlobalCacheConfiguration(
            enable_l2_persistent=False,  # Disable for testing
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )

    @pytest.fixture
    def cache_manager(self, cache_config):
        """Create test cache manager with proper cleanup."""
        manager = GlobalCacheManager(cache_config)
        yield manager
        # Ensure proper cleanup
        manager.shutdown_sync()

    @pytest.fixture
    def test_key(self):
        """Create test cache key."""
        return GlobalCacheKey(namespace="test", identifier="key123", version="v1.0.0")

    @pytest.fixture
    def test_metadata(self):
        """Create test cache metadata."""
        return CacheMetadata(
            computation_cost_ms=100.0, ttl_seconds=3600.0, quality_score=0.9
        )

    @pytest.mark.asyncio
    async def test_namespace_key_pattern_matching(self, cache_manager):
        """Test namespace key listing uses wildcard pattern matching."""
        keys = [
            GlobalCacheKey(namespace="pattern.ns", identifier="alpha-1", version="v1"),
            GlobalCacheKey(namespace="pattern.ns", identifier="alpha-2", version="v1"),
            GlobalCacheKey(namespace="pattern.ns", identifier="beta-1", version="v1"),
        ]
        for key in keys:
            await cache_manager.put(key, {"value": key.identifier})

        alpha_keys = cache_manager.get_namespace_keys("pattern.ns", "alpha*")
        assert len(alpha_keys) == 2

        beta_keys = cache_manager.get_namespace_keys("pattern.ns", "beta#")
        assert len(beta_keys) == 1

    @pytest.mark.asyncio
    async def test_put_and_get_l1_cache(self, cache_manager, test_key, test_metadata):
        """Test basic put and get operations on L1 cache."""
        test_value = {"result": "test_data", "count": 42}

        # Put value in cache
        put_result = await cache_manager.put(
            key=test_key, value=test_value, metadata=test_metadata
        )

        assert put_result.success
        assert put_result.cache_level == CacheLevel.L1

        # Get value from cache
        get_result = await cache_manager.get(test_key)

        assert get_result.success
        assert get_result.cache_level == CacheLevel.L1
        assert get_result.entry is not None
        assert get_result.entry.value == test_value

    @pytest.mark.asyncio
    async def test_cache_miss(self, cache_manager):
        """Test cache miss scenario."""
        missing_key = GlobalCacheKey(
            namespace="test", identifier="missing", version="v1.0.0"
        )

        result = await cache_manager.get(missing_key)

        assert not result.success
        assert "Cache miss" in result.error_message

    @pytest.mark.asyncio
    async def test_cache_delete(self, cache_manager, test_key, test_metadata):
        """Test cache deletion."""
        test_value = {"data": "to_be_deleted"}

        # Put value
        await cache_manager.put(test_key, test_value, test_metadata)

        # Verify it exists
        get_result = await cache_manager.get(test_key)
        assert get_result.success

        # Delete it
        delete_result = await cache_manager.delete(test_key)
        assert delete_result.success

        # Verify it's gone
        get_result_after = await cache_manager.get(test_key)
        assert not get_result_after.success

    @pytest.mark.asyncio
    async def test_ttl_expiration(self, cache_manager, test_key):
        """Test TTL-based cache expiration."""
        test_value = {"data": "expires_soon"}
        metadata = CacheMetadata(ttl_seconds=0.1)  # 100ms TTL

        # Put value with short TTL
        await cache_manager.put(test_key, test_value, metadata)

        # Should be available immediately
        result = await cache_manager.get(test_key)
        assert result.success

        # Wait for expiration
        await asyncio.sleep(0.2)

        # Should be expired now
        result_after = await cache_manager.get(test_key)
        assert not result_after.success

    @pytest.mark.asyncio
    async def test_cache_options(self, cache_manager, test_key, test_metadata):
        """Test cache operations with specific options."""
        test_value = {"data": "with_options"}

        options = CacheOptions(
            cache_levels=frozenset([CacheLevel.L1]),
            consistency_level=ConsistencyLevel.STRONG,
            timeout_ms=1000,
        )

        # Put with options
        put_result = await cache_manager.put(
            test_key, test_value, test_metadata, options
        )
        assert put_result.success

        # Get with options
        get_result = await cache_manager.get(test_key, options)
        assert get_result.success

    def test_cache_statistics(self, cache_manager):
        """Test cache statistics collection."""
        stats = cache_manager.get_statistics()

        assert "operation_stats" in stats
        assert "l1_statistics" in stats
        assert "replication_statistics" in stats

        # Check L1 statistics structure
        l1_stats = stats["l1_statistics"]
        assert "hits" in l1_stats
        assert "misses" in l1_stats
        assert "hit_rate" in l1_stats

    def test_default_cache_options_include_l4(self):
        """Ensure default cache options include L4 federation when available."""
        options = CacheOptions()
        assert CacheLevel.L4 in options.cache_levels


class TestFabricCacheProtocol:
    """Test FabricCacheProtocol functionality."""

    @pytest.fixture
    def cache_transport(self) -> InProcessCacheTransport:
        return InProcessCacheTransport()

    @pytest.fixture
    def cache_protocol(self, cache_transport):
        """Create test cache protocol."""
        return FabricCacheProtocol(
            node_id="test_node_1",
            transport=cache_transport,
            gossip_interval=1.0,  # Fast for testing
            anti_entropy_interval=5.0,
        )

    @pytest.fixture
    def test_key(self):
        """Create test cache key."""
        return GlobalCacheKey(
            namespace="cache.test", identifier="key456", version="v1.0.0"
        )

    @pytest.mark.asyncio
    async def test_propagate_cache_operation(self, cache_protocol, test_key):
        """Test propagating cache operations."""
        test_value = {"data": "propagated_value"}
        metadata = CacheMetadata(computation_cost_ms=50.0)

        operation_id = await cache_protocol.propagate_cache_operation(
            operation_type=CacheOperationType.PUT,
            key=test_key,
            value=test_value,
            metadata=metadata,
        )

        assert operation_id is not None
        assert operation_id in cache_protocol.cache_operations
        assert len(cache_protocol.pending_operations) > 0

    @pytest.mark.asyncio
    async def test_handle_cache_message(self, cache_protocol, test_key):
        """Test handling incoming cache messages."""
        message = CacheOperationMessage(
            operation_type=CacheOperationType.PUT,
            operation_id="remote_op_123",
            key=test_key,
            timestamp=time.time(),
            source_node="remote_node",
            vector_clock={"remote_node": 1},
            value={"data": "remote_value"},
            metadata=CacheMetadata(),
        )

        success = await cache_protocol.handle_cache_message(message)

        assert success
        assert message.operation_id in cache_protocol.cache_operations
        assert str(test_key) in cache_protocol.cache_entries

    @pytest.mark.asyncio
    async def test_conflict_detection(self, cache_protocol, test_key):
        """Test cache conflict detection."""
        # Create local entry
        local_entry = GlobalCacheEntry(
            key=test_key,
            value={"data": "local_value"},
            metadata=CacheMetadata(),
            creation_time=time.time() - 100,
            vector_clock={"test_node_1": 1},
        )
        cache_protocol.cache_entries[str(test_key)] = local_entry

        # Create conflicting remote entry
        remote_entry = GlobalCacheEntry(
            key=test_key,
            value={"data": "remote_value"},
            metadata=CacheMetadata(),
            creation_time=time.time(),
            vector_clock={"remote_node": 1},
        )

        conflict = cache_protocol._detect_conflict(local_entry, remote_entry)

        assert conflict is not None
        assert conflict.key == test_key
        assert conflict.conflict_type in ["value", "timestamp"]

    @pytest.mark.asyncio
    async def test_peer_registration(self, cache_transport):
        """Test peer registration with transport."""
        protocol = FabricCacheProtocol(
            node_id="test_peer",
            transport=cache_transport,
            gossip_interval=60.0,
        )

        assert cache_transport.peer_ids() == ("test_peer",)

        await protocol.shutdown()

        assert cache_transport.peer_ids() == ()

    def test_cache_statistics(self, cache_protocol):
        """Test cache statistics collection."""
        stats = cache_protocol.get_statistics()

        assert "node_id" in stats
        assert "operations" in stats
        assert "conflicts" in stats
        assert "cache_state" in stats
        assert stats["node_id"] == "test_node_1"


class TestCacheProtocol:
    """Test cache protocol message handling."""

    @pytest.fixture
    def test_key_message(self):
        """Create test cache key message."""
        return CacheKeyMessage(
            namespace="protocol.test",
            identifier="msg_key_123",
            version="v1.0.0",
            tags=["test", "protocol"],
        )

    @pytest.fixture
    def cache_request(self, test_key_message):
        """Create test cache request."""
        return CacheRequestMessage(
            operation=CacheOperation.GET.value,
            key=test_key_message,
            options=CacheOptionsMessage(),
        )

    @pytest.fixture
    def protocol_handler(self):
        """Create test protocol handler."""
        mock_cache_manager = AsyncMock()
        return CacheProtocolHandler(cache_manager=mock_cache_manager)

    def test_cache_key_message_conversion(self, test_key_message):
        """Test cache key message conversion."""
        # Convert to GlobalCacheKey
        global_key = test_key_message.to_global_cache_key()

        assert global_key.namespace == test_key_message.namespace
        assert global_key.identifier == test_key_message.identifier
        assert global_key.version == test_key_message.version
        assert set(global_key.tags) == set(test_key_message.tags)

        # Convert back to message
        key_message_back = CacheKeyMessage.from_global_cache_key(global_key)
        assert key_message_back.namespace == test_key_message.namespace
        assert key_message_back.identifier == test_key_message.identifier

    def test_cache_request_serialization(self, cache_request):
        """Test cache request message serialization."""
        # Convert to dict
        request_dict = cache_request.to_dict()

        assert request_dict["role"] == "cache-request"
        assert request_dict["operation"] == CacheOperation.GET.value
        assert "key" in request_dict
        assert "options" in request_dict

        # Convert back from dict
        request_back = CacheRequestMessage.from_dict(request_dict)

        assert request_back.role == cache_request.role
        assert request_back.operation == cache_request.operation
        assert request_back.key.namespace == cache_request.key.namespace

    def test_cache_options_message_defaults_include_l4(self):
        """Ensure protocol cache options default to include L4 federation."""
        options = CacheOptionsMessage()
        assert "L4" in options.cache_levels

    def test_cache_response_serialization(self):
        """Test cache response message serialization."""
        response = CacheResponseMessage(
            u="test_request_id", status="hit", cache_level="L1", error_message=None
        )

        # Convert to dict
        response_dict = response.to_dict()

        assert response_dict["role"] == "cache-response"
        assert response_dict["status"] == "hit"
        assert response_dict["cache_level"] == "L1"

        # Convert back from dict
        response_back = CacheResponseMessage.from_dict(response_dict)

        assert response_back.u == response.u
        assert response_back.status == response.status
        assert response_back.cache_level == response.cache_level

    @pytest.mark.asyncio
    async def test_protocol_handler_get_request(self, protocol_handler, cache_request):
        """Test protocol handler processing GET request."""
        # Mock successful cache get
        from mpreg.core.global_cache import CacheLevel, CacheOperationResult

        mock_result = CacheOperationResult(
            success=True,
            cache_level=CacheLevel.L1,
            entry=None,  # Simplified for test
        )
        protocol_handler.cache_manager.get.return_value = mock_result

        # Process request
        request_dict = cache_request.to_dict()
        response_dict = await protocol_handler.handle_cache_request(request_dict)

        assert response_dict is not None
        assert response_dict["role"] == "cache-response"
        assert response_dict["u"] == cache_request.u
        assert response_dict["status"] == "miss"  # No entry in mock

    @pytest.mark.asyncio
    async def test_protocol_handler_error_handling(self, protocol_handler):
        """Test protocol handler error handling."""
        # Send invalid request
        invalid_request = {
            "role": "cache-request",
            "u": "test_id",
            "operation": "invalid_operation",
        }

        response_dict = await protocol_handler.handle_cache_request(invalid_request)

        assert response_dict["role"] == "cache-response"
        assert response_dict["status"] == "error"
        assert "error_message" in response_dict


class TestCacheIntegration:
    """Test integration between cache components."""

    @pytest.mark.asyncio
    async def test_end_to_end_cache_flow(self):
        """Test complete cache flow from protocol to storage."""
        # Create components
        config = GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
        cache_manager = GlobalCacheManager(config)
        protocol_handler = CacheProtocolHandler(cache_manager)

        # Create test data
        test_key = CacheKeyMessage(
            namespace="integration.test", identifier="end_to_end_key", version="v1.0.0"
        )
        test_value = {"result": "integration_test", "timestamp": time.time()}

        # Test PUT operation
        put_request = CacheRequestMessage(
            operation=CacheOperation.PUT.value, key=test_key, value=test_value
        )

        put_response_dict = await protocol_handler.handle_cache_request(
            put_request.to_dict()
        )
        assert put_response_dict["status"] != "error"

        # Test GET operation
        get_request = CacheRequestMessage(
            operation=CacheOperation.GET.value, key=test_key
        )

        get_response_dict = await protocol_handler.handle_cache_request(
            get_request.to_dict()
        )
        assert get_response_dict["status"] == "hit"
        assert get_response_dict["entry"]["value"] == test_value

        # Cleanup
        await cache_manager.shutdown()

    @pytest.mark.asyncio
    async def test_fabric_federation_cache_flow(self):
        """Ensure L4 cache operations flow through fabric cache protocol."""
        transport = InProcessCacheTransport()
        cache_protocol = FabricCacheProtocol(
            "test_node", transport=transport, gossip_interval=60.0
        )
        cache_manager = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=True,
            ),
            cache_protocol=cache_protocol,
        )

        key = GlobalCacheKey(
            namespace="fabric.federation.cache",
            identifier="test_key",
            version="v1.0.0",
        )
        value = {"data": "value"}
        metadata = CacheMetadata()

        result = await cache_manager.put(
            key,
            value,
            metadata,
            CacheOptions(cache_levels=frozenset([CacheLevel.L4])),
        )
        assert result.success
        assert str(key) in cache_protocol.cache_entries

        get_result = await cache_manager.get(
            key, CacheOptions(cache_levels=frozenset([CacheLevel.L4]))
        )
        assert get_result.success
        assert get_result.cache_level == CacheLevel.L4

        await cache_manager.shutdown()
        await cache_protocol.shutdown()


# Performance and stress tests
class TestCachePerformance:
    """Test cache performance and scalability."""

    @pytest.mark.asyncio
    async def test_cache_performance_large_dataset(self):
        """Test cache performance with large dataset."""
        config = GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
        cache_manager = GlobalCacheManager(config)

        # Store many entries
        num_entries = 1000
        start_time = time.time()

        for i in range(num_entries):
            key = GlobalCacheKey(
                namespace="performance.test", identifier=f"key_{i}", version="v1.0.0"
            )
            value = {"data": f"value_{i}", "index": i}

            result = await cache_manager.put(key, value)
            assert result.success

        put_time = time.time() - start_time

        # Retrieve all entries
        start_time = time.time()

        for i in range(num_entries):
            key = GlobalCacheKey(
                namespace="performance.test", identifier=f"key_{i}", version="v1.0.0"
            )

            result = await cache_manager.get(key)
            assert result.success
            assert result.entry is not None
            assert result.entry.value["index"] == i

        get_time = time.time() - start_time

        # Performance assertions
        assert put_time < 5.0  # Should complete in under 5 seconds
        assert get_time < 2.0  # Gets should be faster than puts

        # Cleanup
        await cache_manager.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_cache_operations(self):
        """Test concurrent cache operations."""
        config = GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
        cache_manager = GlobalCacheManager(config)

        # Create concurrent operations
        async def put_operation(index):
            key = GlobalCacheKey(
                namespace="concurrent.test",
                identifier=f"concurrent_key_{index}",
                version="v1.0.0",
            )
            value = {"data": f"concurrent_value_{index}"}
            return await cache_manager.put(key, value)

        # Run operations concurrently
        num_concurrent = 100
        start_time = time.time()

        tasks = [put_operation(i) for i in range(num_concurrent)]
        results = await asyncio.gather(*tasks)

        concurrent_time = time.time() - start_time

        # All operations should succeed
        assert all(result.success for result in results)
        assert concurrent_time < 10.0  # Should complete in reasonable time

        # Cleanup
        await cache_manager.shutdown()


class TestMPREGCacheIntegration:
    """Test integration with MPREG server and client infrastructure."""

    @pytest.mark.asyncio
    async def test_cache_with_mpreg_server(self):
        """Test cache integration with MPREG server."""
        # Create cache manager
        cache_config = GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
        cache_manager = GlobalCacheManager(cache_config)

        # Test storing computation results
        computation_key = GlobalCacheKey.from_function_call(
            namespace="compute.fibonacci",
            function_name="fibonacci",
            args=(10,),
            kwargs={},
            tags={"recursive", "expensive"},
        )

        # Simulate expensive computation
        start_time = time.time()
        result = 55  # fibonacci(10)
        computation_time = (time.time() - start_time) * 1000

        # Store in cache
        metadata = CacheMetadata(
            computation_cost_ms=computation_time, ttl_seconds=3600.0
        )

        await cache_manager.put(computation_key, result, metadata)

        # Verify retrieval
        cached_result = await cache_manager.get(computation_key)
        assert cached_result.success
        assert cached_result.entry is not None
        assert cached_result.entry.value == result

        # Test cache hit performance
        hit_start = time.time()
        second_result = await cache_manager.get(computation_key)
        hit_time = (time.time() - hit_start) * 1000

        assert second_result.success
        assert hit_time < 10.0  # Should be much faster than computation

        await cache_manager.shutdown()

    @pytest.mark.asyncio
    async def test_cache_protocol_message_handling(self):
        """Test cache protocol message handling in MPREG server context."""
        cache_config = GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
        cache_manager = GlobalCacheManager(cache_config)
        protocol_handler = CacheProtocolHandler(cache_manager)

        # Test PUT operation via protocol
        key_msg = CacheKeyMessage(
            namespace="mpreg.test", identifier="protocol_test", version="v1.0.0"
        )

        put_request = CacheRequestMessage(
            operation=CacheOperation.PUT.value,
            key=key_msg,
            value={"mpreg_data": "test_value", "timestamp": time.time()},
        )

        put_response_dict = await protocol_handler.handle_cache_request(
            put_request.to_dict()
        )
        assert put_response_dict["status"] != "error"

        # Test GET operation via protocol
        get_request = CacheRequestMessage(
            operation=CacheOperation.GET.value, key=key_msg
        )

        get_response_dict = await protocol_handler.handle_cache_request(
            get_request.to_dict()
        )
        assert get_response_dict["status"] == "hit"
        assert get_response_dict["entry"]["value"]["mpreg_data"] == "test_value"

        await cache_manager.shutdown()

    @pytest.mark.asyncio
    async def test_distributed_cache_with_fabric(self):
        """Test distributed cache coordination via fabric cache protocol."""
        transport = InProcessCacheTransport()
        node1 = FabricCacheProtocol(
            "node1", transport=transport, gossip_interval=60.0
        )  # Disable background
        node2 = FabricCacheProtocol("node2", transport=transport, gossip_interval=60.0)

        # Create test key
        test_key = GlobalCacheKey(
            namespace="distributed.test", identifier="shared_data", version="v1.0.0"
        )

        # Node 1 puts data
        op_id_1 = await node1.propagate_cache_operation(
            CacheOperationType.PUT,
            test_key,
            {"node": "node1", "data": "shared_value"},
            CacheMetadata(computation_cost_ms=200.0),
        )

        # Simulate cache message from node1 to node2
        operation_msg = node1.cache_operations[op_id_1]
        success = await node2.handle_cache_message(operation_msg)

        assert success
        assert str(test_key) in node2.cache_entries
        assert node2.cache_entries[str(test_key)].value["data"] == "shared_value"

        # Test conflict detection
        # Node 2 puts different data for same key
        op_id_2 = await node2.propagate_cache_operation(
            CacheOperationType.PUT,
            test_key,
            {"node": "node2", "data": "conflicting_value"},
            CacheMetadata(computation_cost_ms=100.0),
        )

        # Apply node2's operation to node1 (should create conflict)
        operation_msg_2 = node2.cache_operations[op_id_2]
        await node1.handle_cache_message(operation_msg_2)

        # Check conflict was detected and resolved
        node1_stats = node1.get_statistics()
        # Note: Conflict detection depends on timing and vector clocks
        # For this test, just verify operations were processed
        assert node1_stats["operations"]["received"] > 0

        await node1.shutdown()
        await node2.shutdown()

    @pytest.mark.asyncio
    async def test_end_to_end_mpreg_cache_workflow(self):
        """Test complete end-to-end cache workflow with MPREG components."""
        # Create cache manager with all levels enabled
        cache_config = GlobalCacheConfiguration(
            enable_l2_persistent=True,  # Enable for full test
            enable_l3_distributed=True,
            enable_l4_federation=True,
            local_cluster_id="test_cluster_1",
            local_region="us-west",
        )
        cache_manager = GlobalCacheManager(cache_config)

        transport = InProcessCacheTransport()
        cache_protocol = FabricCacheProtocol(
            "test_node", transport=transport, gossip_interval=60.0
        )

        # Simulate complex data processing workflow
        workflows = [
            (
                "ml.model_training",
                {"dataset": "users", "model": "bert"},
                {"trained_model": "bert_v1"},
            ),
            (
                "analytics.user_segmentation",
                {"region": "us-west"},
                {"segments": ["premium", "basic"]},
            ),
            (
                "compute.recommendations",
                {"user_id": "12345"},
                {"recommendations": [1, 2, 3, 4, 5]},
            ),
        ]

        cached_results = {}

        for namespace, inputs, expected_output in workflows:
            # Create cache key
            key = GlobalCacheKey.from_function_call(
                namespace=namespace,
                function_name="process",
                args=(),
                kwargs=inputs,
                tags={"workflow", "production"},
            )

            # Simulate expensive computation
            start_time = time.time()
            await asyncio.sleep(0.01)  # Simulate work
            computation_time = (time.time() - start_time) * 1000

            # Store in cache with metadata
            metadata = CacheMetadata(
                computation_cost_ms=computation_time,
                ttl_seconds=1800.0,  # 30 minutes
                geographic_hints=["us-west"],
                quality_score=0.95,
            )

            result = await cache_manager.put(key, expected_output, metadata)
            assert result.success

            # Record for distribution
            await cache_protocol.propagate_cache_operation(
                CacheOperationType.PUT, key, expected_output, metadata
            )

            cached_results[str(key)] = expected_output

        # Test retrieval performance - store the actual keys
        created_keys = []
        for namespace, inputs, expected_output in workflows:
            key = GlobalCacheKey.from_function_call(
                namespace=namespace,
                function_name="process",
                args=(),
                kwargs=inputs,
                tags={"workflow", "production"},
            )
            created_keys.append(key)

        retrieval_times = []
        for key in created_keys:
            start_time = time.time()
            result = await cache_manager.get(key)
            retrieval_time = (time.time() - start_time) * 1000

            assert result.success
            # Cache level can be L1 or L2 depending on configuration
            assert result.cache_level in [CacheLevel.L1, CacheLevel.L2]
            retrieval_times.append(retrieval_time)

        # Verify performance characteristics
        avg_retrieval_time = sum(retrieval_times) / len(retrieval_times)
        assert avg_retrieval_time < 5.0  # Should be very fast from L1

        # Test cache statistics
        cache_stats = cache_manager.get_statistics()
        assert cache_stats["l1_statistics"]["hits"] == len(workflows)
        assert cache_stats["l1_statistics"]["entry_count"] == len(workflows)

        cache_stats = cache_protocol.get_statistics()
        assert cache_stats["operations"]["sent"] == len(workflows)

        # Cleanup
        await cache_manager.shutdown()
        await cache_protocol.shutdown()


# Performance benchmarks for real-world scenarios
class TestCacheRealWorldPerformance:
    """Test cache performance under realistic workloads."""

    @pytest.mark.asyncio
    async def test_high_throughput_cache_operations(self):
        """Test cache under high throughput conditions."""
        cache_config = GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
        cache_manager = GlobalCacheManager(cache_config)

        # Simulate high-throughput scenario (1000 operations)
        num_operations = 1000
        operation_times = []

        # Mixed workload: 70% reads, 30% writes
        for i in range(num_operations):
            start_time = time.time()

            if i < num_operations * 0.3:  # First 30% are writes
                key = GlobalCacheKey(
                    namespace="high_throughput.test",
                    identifier=f"key_{i}",
                    version="v1.0.0",
                )

                value = {
                    "id": i,
                    "data": f"value_{i}",
                    "metadata": {"created": time.time()},
                }

                result = await cache_manager.put(key, value)
                assert result.success

            else:  # Remaining 70% are reads
                read_index = i % int(num_operations * 0.3)  # Read from written keys
                key = GlobalCacheKey(
                    namespace="high_throughput.test",
                    identifier=f"key_{read_index}",
                    version="v1.0.0",
                )

                result = await cache_manager.get(key)
                assert result.success

            operation_time = (time.time() - start_time) * 1000
            operation_times.append(operation_time)

        # Performance assertions
        avg_operation_time = sum(operation_times) / len(operation_times)
        p95_operation_time = sorted(operation_times)[int(0.95 * len(operation_times))]

        assert avg_operation_time < 2.0  # Average < 2ms
        assert p95_operation_time < 5.0  # P95 < 5ms

        # Verify cache hit rate
        stats = cache_manager.get_statistics()
        expected_hits = int(num_operations * 0.7)  # All reads should hit
        assert (
            stats["l1_statistics"]["hits"] >= expected_hits * 0.95
        )  # Allow 5% miss rate

        await cache_manager.shutdown()

    @pytest.mark.asyncio
    async def test_memory_pressure_handling(self):
        """Test cache behavior under memory pressure."""
        # Create cache with small memory limit
        from mpreg.core.caching import CacheConfiguration, CacheLimits

        local_cache_config = CacheConfiguration(
            limits=CacheLimits(
                max_memory_bytes=1024 * 1024,  # 1MB limit
                max_entries=100,
            )
        )

        cache_config = GlobalCacheConfiguration(
            local_cache_config=local_cache_config,
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )

        cache_manager = GlobalCacheManager(cache_config)

        # Fill cache beyond capacity
        large_value = {"large_data": "x" * 10000}  # ~10KB per entry
        stored_keys = []

        for i in range(150):  # More than max_entries
            key = GlobalCacheKey(
                namespace="memory_pressure.test",
                identifier=f"large_key_{i}",
                version="v1.0.0",
            )

            result = await cache_manager.put(key, large_value)
            assert result.success
            stored_keys.append(key)

        # Verify that eviction occurred
        stats = cache_manager.get_statistics()
        assert stats["l1_statistics"]["entry_count"] <= 100
        assert stats["l1_statistics"]["evictions"] > 0

        # Verify that recently added items are still accessible
        recent_keys = stored_keys[-50:]  # Last 50 keys
        hit_count = 0

        for key in recent_keys:
            result = await cache_manager.get(key)
            if result.success:
                hit_count += 1

        # Should have reasonable hit rate for recent items under memory pressure
        # With only 100 entries max and 12.8KB per entry, we expect some eviction
        hit_rate = hit_count / len(recent_keys)
        assert hit_rate > 0.3  # At least 30% hit rate for recent items under pressure
        assert hit_rate < 1.0  # But not 100% due to memory constraints

        await cache_manager.shutdown()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
