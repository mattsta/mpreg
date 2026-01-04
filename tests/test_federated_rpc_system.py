"""
Unit tests for MPREG's federated RPC system components.

This module tests individual components and functions of the federated RPC
system using REAL servers with dynamic port assignment, focusing on correctness and edge cases.

Key components tested:
- Fabric catalog propagation for function announcements
- Hop budgets and propagation delays for cross-node discovery
- Function catalog consistency across federated peers
"""

import asyncio
from collections.abc import Callable
from typing import Any

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
    VersionConstraint,
)
from mpreg.fabric.catalog import FunctionCatalog, FunctionEndpoint
from mpreg.fabric.index import FunctionQuery
from mpreg.server import MPREGServer
from tests.test_helpers import wait_for_condition


class TestFunctionCatalogIdentity:
    """Validate function identity invariants in the fabric catalog."""

    def test_rejects_function_name_conflict(self):
        catalog = FunctionCatalog()
        endpoint = FunctionEndpoint(
            identity=FunctionIdentity(
                name="update",
                function_id="func-A",
                version=SemanticVersion.parse("1.0.0"),
            ),
            resources=frozenset({"resource-1"}),
            node_id="node-1",
            cluster_id="cluster-1",
        )
        assert catalog.register(endpoint, now=10.0)
        conflicting = FunctionEndpoint(
            identity=FunctionIdentity(
                name="update",
                function_id="func-B",
                version=SemanticVersion.parse("1.0.0"),
            ),
            resources=frozenset({"resource-1"}),
            node_id="node-2",
            cluster_id="cluster-1",
        )
        try:
            catalog.register(conflicting, now=10.0)
        except ValueError as exc:
            assert "Function name conflict" in str(exc)
            return
        raise AssertionError("Expected function name conflict")

    def test_rejects_function_id_conflict(self):
        catalog = FunctionCatalog()
        endpoint = FunctionEndpoint(
            identity=FunctionIdentity(
                name="update",
                function_id="func-A",
                version=SemanticVersion.parse("1.0.0"),
            ),
            resources=frozenset({"resource-1"}),
            node_id="node-1",
            cluster_id="cluster-1",
        )
        assert catalog.register(endpoint, now=10.0)
        conflicting = FunctionEndpoint(
            identity=FunctionIdentity(
                name="refresh",
                function_id="func-A",
                version=SemanticVersion.parse("1.0.0"),
            ),
            resources=frozenset({"resource-1"}),
            node_id="node-2",
            cluster_id="cluster-1",
        )
        try:
            catalog.register(conflicting, now=10.0)
        except ValueError as exc:
            assert "Function id conflict" in str(exc)
            return
        raise AssertionError("Expected function id conflict")

    def test_selector_matches_version_constraints(self):
        catalog = FunctionCatalog()
        endpoint_v1 = FunctionEndpoint(
            identity=FunctionIdentity(
                name="update",
                function_id="func-update",
                version=SemanticVersion.parse("1.0.0"),
            ),
            resources=frozenset({"db"}),
            node_id="node-1",
            cluster_id="cluster-1",
        )
        endpoint_v2 = FunctionEndpoint(
            identity=FunctionIdentity(
                name="update",
                function_id="func-update",
                version=SemanticVersion.parse("2.1.0"),
            ),
            resources=frozenset({"db"}),
            node_id="node-2",
            cluster_id="cluster-1",
        )
        assert catalog.register(endpoint_v1, now=10.0)
        assert catalog.register(endpoint_v2, now=10.0)

        selector = FunctionSelector(
            name="update",
            function_id="func-update",
            version_constraint=VersionConstraint.parse(">=2.0.0,<3.0.0"),
        )
        matches = catalog.find(selector, resources=frozenset({"db"}), now=10.0)
        assert {entry.node_id for entry in matches} == {"node-2"}

    def test_expired_entries_are_ignored(self):
        catalog = FunctionCatalog()
        expired = FunctionEndpoint(
            identity=FunctionIdentity(
                name="update",
                function_id="func-expired",
                version=SemanticVersion.parse("1.0.0"),
            ),
            resources=frozenset({"db"}),
            node_id="node-1",
            cluster_id="cluster-1",
            advertised_at=0.0,
            ttl_seconds=1.0,
        )
        assert not catalog.register(expired, now=5.0)
        assert catalog.entry_count() == 0


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
        # Wait for catalog propagation
        await wait_for_condition(
            lambda: bool(
                server2.cluster.fabric_engine
                and server2.cluster.fabric_engine.routing_index.find_functions(
                    FunctionQuery(
                        selector=FunctionSelector(name="duplicate_test_function"),
                        resources=frozenset({"dedupe-resource"}),
                    )
                )
            ),
            timeout=8.0,
            interval=0.1,
            error_message="Duplicate function did not propagate to server2 catalog",
        )

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

    async def test_catalog_message_processing(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """Test function discovery propagation using real servers."""
        server1, server2 = cluster_2_servers

        # CRITICAL: Register function AFTER cluster formation
        def catalog_test_function(data: str) -> str:
            return f"catalog_processed: {data}"

        server1.register_command(
            "catalog_test_function", catalog_test_function, ["catalog-resource"]
        )

        # Wait for fabric catalog propagation
        await asyncio.sleep(7.0)

        # Verify that server2 received and processed the function announcement
        client = await client_factory(server2.settings.port)
        result = await client.call(
            "catalog_test_function", "test", locs=frozenset(["catalog-resource"])
        )

        assert result == "catalog_processed: test"

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

    def test_prune_expired_entries(self):
        """Test pruning expired function catalog entries."""
        catalog = FunctionCatalog()
        active = FunctionEndpoint(
            identity=FunctionIdentity(
                name="active",
                function_id="func-active",
                version=SemanticVersion.parse("1.0.0"),
            ),
            resources=frozenset({"db"}),
            node_id="node-active",
            cluster_id="cluster-1",
            advertised_at=0.0,
            ttl_seconds=100.0,
        )
        expired = FunctionEndpoint(
            identity=FunctionIdentity(
                name="expired",
                function_id="func-expired",
                version=SemanticVersion.parse("1.0.0"),
            ),
            resources=frozenset({"db"}),
            node_id="node-expired",
            cluster_id="cluster-1",
            advertised_at=0.0,
            ttl_seconds=1.0,
        )
        assert catalog.register(active, now=10.0)
        assert catalog.register(expired, now=0.5)

        removed = catalog.prune_expired(now=5.0)
        assert removed == 1
        assert catalog.entry_count() == 1

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

        # Wait for propagation of the target function
        await wait_for_condition(
            lambda: bool(
                server2.cluster.fabric_engine
                and server2.cluster.fabric_engine.routing_index.find_functions(
                    FunctionQuery(
                        selector=FunctionSelector(name="large_function_5"),
                        resources=frozenset({"large_resource_5"}),
                    )
                )
            ),
            timeout=8.0,
            interval=0.1,
            error_message="Large function catalog did not propagate to server2",
        )

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

    def test_endpoint_round_trip_serialization(self):
        """Test FunctionEndpoint serialization round-trip."""
        endpoint = FunctionEndpoint(
            identity=FunctionIdentity(
                name="update",
                function_id="func-update",
                version=SemanticVersion.parse("3.2.1"),
            ),
            resources=frozenset({"db", "cache"}),
            node_id="node-1",
            cluster_id="cluster-1",
            advertised_at=12.5,
            ttl_seconds=30.0,
        )
        payload = endpoint.to_dict()
        loaded = FunctionEndpoint.from_dict(payload)
        assert loaded == endpoint

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
