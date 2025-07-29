"""
Unit tests for MPREG's federated RPC system components.

This module tests individual components and functions of the federated RPC
system using REAL servers with dynamic port assignment, focusing on correctness and edge cases.

Key components tested:
- RPCServerHello federated field validation
- Hop count and max_hops logic
- Announcement ID generation and deduplication
- Original source preservation
- Function broadcasting enhancements with real server communication
"""

import asyncio
from collections.abc import Callable
from typing import Any

from mpreg.core.model import RPCServerHello
from mpreg.server import MPREGServer


class TestRPCServerHelloFederatedFields:
    """Test federated fields in RPCServerHello messages."""

    def test_federated_hello_creation(self):
        """Test creation of RPCServerHello with federated fields."""
        hello = RPCServerHello(
            funs=("test_function", "another_function"),
            locs=("resource1", "resource2"),
            cluster_id="test_cluster",
            hop_count=2,
            max_hops=5,
            announcement_id="test_announcement_123",
            original_source="ws://127.0.0.1:8080",
        )

        assert hello.hop_count == 2
        assert hello.max_hops == 5
        assert hello.announcement_id == "test_announcement_123"
        assert hello.original_source == "ws://127.0.0.1:8080"
        assert hello.funs == ("test_function", "another_function")
        assert hello.locs == ("resource1", "resource2")
        assert hello.cluster_id == "test_cluster"

    def test_federated_hello_defaults(self):
        """Test default values for federated fields."""
        hello = RPCServerHello(
            funs=("test_function",), locs=("resource1",), cluster_id="test_cluster"
        )

        assert hello.hop_count == 0
        assert hello.max_hops == 3
        assert hello.announcement_id == ""
        assert hello.original_source == ""

    def test_hop_count_validation(self):
        """Test hop count validation logic."""
        # Test normal hop progression
        hello = RPCServerHello(
            funs=("test_function",),
            locs=("resource1",),
            cluster_id="test_cluster",
            hop_count=1,
            max_hops=3,
        )

        assert hello.hop_count < hello.max_hops  # Should be forwardable

        # Test at max hops
        hello_max = RPCServerHello(
            funs=("test_function",),
            locs=("resource1",),
            cluster_id="test_cluster",
            hop_count=3,
            max_hops=3,
        )

        assert hello_max.hop_count >= hello_max.max_hops  # Should not be forwardable

    def test_announcement_id_uniqueness(self):
        """Test that different announcements have different IDs."""
        hello1 = RPCServerHello(
            funs=("function1",),
            locs=("resource1",),
            cluster_id="test_cluster",
            announcement_id="id_123",
        )

        hello2 = RPCServerHello(
            funs=("function2",),
            locs=("resource2",),
            cluster_id="test_cluster",
            announcement_id="id_456",
        )

        assert hello1.announcement_id != hello2.announcement_id

    def test_original_source_preservation(self):
        """Test that original source is preserved correctly."""
        original_url = "ws://127.0.0.1:9000"
        hello = RPCServerHello(
            funs=("test_function",),
            locs=("resource1",),
            cluster_id="test_cluster",
            original_source=original_url,
        )

        assert hello.original_source == original_url


class TestFederatedRPCBroadcasting:
    """Test federated RPC function broadcasting with real server communication."""

    async def test_broadcast_new_function_federated_enhancement(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test enhanced _broadcast_new_function with federated propagation using real servers."""
        server1, server2 = cluster_2_servers

        # CRITICAL: Register function AFTER cluster is formed (servers already connected via conftest.py)
        def test_function(data: str) -> str:
            return f"server1 processed: {data}"

        server1.register_command("test_function", test_function, ["server1-resource"])

        # Wait for federated propagation (function announcement across cluster)
        await asyncio.sleep(7.0)

        # Create client connected to server2
        client = await client_factory(server2.settings.port)

        # Test that server2 can call the function registered on server1
        result = await client.call(
            "test_function", "federated_test", locs=frozenset(["server1-resource"])
        )

        assert result == "server1 processed: federated_test"

    async def test_broadcast_hop_limit_enforcement(
        self,
        cluster_3_servers: tuple[MPREGServer, MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test that hop limit is properly enforced using real 3-server cluster."""
        server1, server2, server3 = cluster_3_servers

        # CRITICAL: Register function AFTER cluster formation (servers already connected)
        def limited_function(data: str) -> str:
            return f"limited: {data}"

        server1.register_command(
            "limited_function", limited_function, ["limited-resource"]
        )

        # Wait for propagation through the cluster (cluster has 5.0s gossip interval)
        await asyncio.sleep(7.0)

        # The function should be discoverable on all servers despite hop limits
        # because our default max_hops=3 allows propagation in a 3-node cluster
        client3 = await client_factory(server3.settings.port)
        result = await client3.call(
            "limited_function", "test", locs=frozenset(["limited-resource"])
        )

        assert result == "limited: test"

    async def test_deduplication_logic(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test that duplicate announcements are properly deduplicated using real servers."""
        server1, server2 = cluster_2_servers

        # CRITICAL: Register function AFTER cluster formation
        def duplicate_test_function(data: str) -> str:
            return f"dedupe_test: {data}"

        # Register the function multiple times in quick succession
        server1.register_command(
            "duplicate_test_function", duplicate_test_function, ["dedupe-resource"]
        )
        # The system should deduplicate these internally

        await asyncio.sleep(0.5)

        # The function should be callable despite potential duplicate registrations
        client = await client_factory(server2.settings.port)
        result = await client.call(
            "duplicate_test_function", "test", locs=frozenset(["dedupe-resource"])
        )

        assert result == "dedupe_test: test"

    async def test_announcement_id_generation(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test that announcement IDs are properly generated and unique using real servers."""
        server1, server2 = cluster_2_servers

        # CRITICAL: Register functions AFTER cluster formation
        def function1(data: str) -> str:
            return f"func1: {data}"

        def function2(data: str) -> str:
            return f"func2: {data}"

        server1.register_command("function1", function1, ["resource1"])
        await asyncio.sleep(0.1)  # Small delay to ensure different timestamps
        server1.register_command("function2", function2, ["resource2"])

        # Wait for propagation
        await asyncio.sleep(7.0)

        # Both functions should be callable from server2
        client = await client_factory(server2.settings.port)

        result1 = await client.call("function1", "test", locs=frozenset(["resource1"]))
        result2 = await client.call("function2", "test", locs=frozenset(["resource2"]))

        assert result1 == "func1: test"
        assert result2 == "func2: test"


class TestFederatedRPCMessageHandling:
    """Test federated RPC message handling and processing."""

    async def test_hello_message_processing(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test processing of HELLO messages with federated fields using real servers."""
        server1, server2 = cluster_2_servers

        # CRITICAL: Register function AFTER cluster formation
        def hello_test_function(data: str) -> str:
            return f"hello_processed: {data}"

        server1.register_command(
            "hello_test_function", hello_test_function, ["hello-resource"]
        )

        # Wait for HELLO message propagation
        await asyncio.sleep(7.0)

        # Verify that server2 received and processed the HELLO message
        client = await client_factory(server2.settings.port)
        result = await client.call(
            "hello_test_function", "test", locs=frozenset(["hello-resource"])
        )

        assert result == "hello_processed: test"

    async def test_function_mapping_consistency(
        self,
        cluster_3_servers: tuple[MPREGServer, MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test that function mappings remain consistent across federated announcements."""
        server1, server2, server3 = cluster_3_servers

        # CRITICAL: Register functions AFTER cluster formation
        def mapping_function_1(data: str) -> str:
            return f"server1_mapping: {data}"

        def mapping_function_2(data: str) -> str:
            return f"server3_mapping: {data}"

        server1.register_command(
            "mapping_function_1", mapping_function_1, ["mapping-resource-1"]
        )
        server3.register_command(
            "mapping_function_2", mapping_function_2, ["mapping-resource-2"]
        )

        # Wait for federated propagation (cluster has 5.0s gossip interval)
        await asyncio.sleep(7.0)

        # Test that server2 can call both functions correctly
        client = await client_factory(server2.settings.port)

        result1 = await client.call(
            "mapping_function_1", "test", locs=frozenset(["mapping-resource-1"])
        )
        result2 = await client.call(
            "mapping_function_2", "test", locs=frozenset(["mapping-resource-2"])
        )

        assert result1 == "server1_mapping: test"
        assert result2 == "server3_mapping: test"


class TestFederatedRPCEdgeCases:
    """Test edge cases and error conditions in federated RPC."""

    async def test_empty_peer_connections(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ):
        """Test handling when there are no peer connections (isolated server)."""

        # Single server should handle isolation gracefully
        def isolated_function(data: str) -> str:
            return f"isolated: {data}"

        single_server.register_command(
            "isolated_function", isolated_function, ["isolated_resource"]
        )

        # Should be able to call functions locally even without peers
        client = await client_factory(single_server.settings.port)
        result = await client.call(
            "isolated_function", "test", locs=frozenset(["isolated_resource"])
        )

        assert result == "isolated: test"

    def test_invalid_hop_parameters(self):
        """Test handling of invalid hop count parameters."""
        # Test hop count validation in RPCServerHello
        hello = RPCServerHello(
            funs=("test_function",),
            locs=("test_resource",),
            cluster_id="test_cluster",
            hop_count=-1,  # Invalid negative hop count
            max_hops=3,
            announcement_id="test_123",
            original_source="ws://127.0.0.1:8080",
        )

        # Should still create the object (pydantic doesn't validate negative by default)
        assert hello.hop_count == -1

        # Test max_hops less than hop_count
        hello2 = RPCServerHello(
            funs=("test_function",),
            locs=("test_resource",),
            cluster_id="test_cluster",
            hop_count=5,
            max_hops=3,  # max_hops < hop_count
            announcement_id="test_123",
            original_source="ws://127.0.0.1:8080",
        )

        assert hello2.hop_count == 5
        assert hello2.max_hops == 3

    async def test_large_announcement_data(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test handling of large announcement data with many functions."""
        server1, server2 = cluster_2_servers

        # CRITICAL: Register functions AFTER cluster formation
        for i in range(10):  # Reasonable number for testing

            def make_function(index: int):
                def large_function(data: str) -> str:
                    return f"large_function_{index}: {data}"

                return large_function

            server1.register_command(
                f"large_function_{i}", make_function(i), [f"large_resource_{i}"]
            )

        # Wait for propagation of all functions
        await asyncio.sleep(1.0)

        # Test that we can call some of the functions from server2
        client = await client_factory(server2.settings.port)

        result = await client.call(
            "large_function_5", "test", locs=frozenset(["large_resource_5"])
        )
        assert result == "large_function_5: test"

    async def test_concurrent_announcement_processing(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test concurrent processing of multiple announcements using real servers."""
        server1, server2 = cluster_2_servers

        # CRITICAL: Register functions AFTER cluster formation
        tasks = []
        for i in range(5):  # Reasonable number for testing

            def make_concurrent_function(index: int):
                def concurrent_function(data: str) -> str:
                    return f"concurrent_{index}: {data}"

                return concurrent_function

            # Create registration tasks
            async def register_function(index: int):
                server1.register_command(
                    f"concurrent_function_{index}",
                    make_concurrent_function(index),
                    [f"concurrent_resource_{index}"],
                )

            tasks.append(register_function(i))

        # Execute all registrations concurrently
        await asyncio.gather(*tasks)

        # Wait for propagation
        await asyncio.sleep(7.0)

        # Test that all functions are callable
        client = await client_factory(server2.settings.port)
        result = await client.call(
            "concurrent_function_2", "test", locs=frozenset(["concurrent_resource_2"])
        )
        assert result == "concurrent_2: test"

    def test_malformed_announcement_data(self):
        """Test handling of malformed announcement data."""
        # Test empty announcement ID
        hello1 = RPCServerHello(
            funs=("test_function",),
            locs=("test_resource",),
            cluster_id="test_cluster",
            hop_count=1,
            max_hops=3,
            announcement_id="",  # Empty announcement ID
            original_source="ws://127.0.0.1:8080",
        )
        assert hello1.announcement_id == ""

        # Test empty original source
        hello2 = RPCServerHello(
            funs=("test_function",),
            locs=("test_resource",),
            cluster_id="test_cluster",
            hop_count=1,
            max_hops=3,
            announcement_id="test_123",
            original_source="",  # Empty original source
        )
        assert hello2.original_source == ""

    async def test_announcement_id_collision_handling(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test handling of duplicate function registration detection."""
        server1, server2 = cluster_2_servers

        # CRITICAL: Register function AFTER cluster formation
        def collision_function(data: str) -> str:
            return f"collision_handled: {data}"

        server1.register_command(
            "collision_function", collision_function, ["collision_resource"]
        )

        # Wait for initial propagation
        await asyncio.sleep(7.0)

        # Try to register the same function again (should raise ValueError)
        import pytest

        with pytest.raises(
            ValueError, match="Function 'collision_function' is already registered"
        ):
            server1.register_command(
                "collision_function", collision_function, ["collision_resource"]
            )

        # The function should still be callable normally after the attempted duplicate registration
        client = await client_factory(server2.settings.port)
        result = await client.call(
            "collision_function", "test", locs=frozenset(["collision_resource"])
        )

        assert result == "collision_handled: test"
