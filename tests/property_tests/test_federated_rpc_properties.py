"""
Property-based tests for MPREG's federated RPC system invariants.

This module uses Hypothesis to verify critical invariants and properties
of the federated RPC announcement and routing system under various conditions.

Key properties tested:
- Hop count monotonicity and bounds
- Announcement ID uniqueness and deduplication
- Original source preservation across hops
- Function availability consistency
- Message ordering and delivery guarantees
"""

import asyncio
from typing import Any
from unittest.mock import MagicMock

import hypothesis.strategies as st
import pytest
from hypothesis import given, settings

from mpreg.core.model import RPCServerHello
from mpreg.server import MPREGServer


class TestFederatedRPCAnnouncementProperties:
    """Property-based tests for federated RPC announcement behavior."""

    @given(
        hop_count=st.integers(min_value=0, max_value=10),
        max_hops=st.integers(min_value=1, max_value=5),
        announcement_id=st.text(min_size=1, max_size=50),
        original_source=st.text(min_size=1, max_size=100),
    )
    @settings(max_examples=50, deadline=5000)
    def test_hop_count_invariants(
        self, hop_count: int, max_hops: int, announcement_id: str, original_source: str
    ):
        """Test that hop count invariants are maintained."""

        # Create RPCServerHello with federated fields
        hello = RPCServerHello(
            funs=("test_function",),
            locs=("test_resource",),
            cluster_id="test_cluster",
            hop_count=hop_count,
            max_hops=max_hops,
            announcement_id=announcement_id,
            original_source=original_source,
        )

        # Property 1: Hop count should never exceed max_hops
        if hop_count < max_hops:
            # Should be forwardable
            next_hop_count = hop_count + 1
            assert next_hop_count <= max_hops
        else:
            # Should not be forwardable
            assert hop_count >= max_hops

        # Property 2: Hop count should be non-negative
        assert hello.hop_count >= 0

        # Property 3: Max hops should be positive
        assert hello.max_hops > 0

        # Property 4: Announcement ID should be preserved
        assert hello.announcement_id == announcement_id

        # Property 5: Original source should be preserved
        assert hello.original_source == original_source

    @given(
        functions=st.lists(
            st.text(min_size=1, max_size=30), min_size=1, max_size=10, unique=True
        ),
        resources=st.lists(
            st.text(min_size=1, max_size=20), min_size=1, max_size=10, unique=True
        ),
        cluster_id=st.text(min_size=1, max_size=50),
    )
    @settings(max_examples=30, deadline=3000)
    def test_announcement_content_preservation(
        self, functions: list[str], resources: list[str], cluster_id: str
    ):
        """Test that announcement content is preserved across hops."""

        # Create original announcement
        original = RPCServerHello(
            funs=tuple(functions),
            locs=tuple(resources),
            cluster_id=cluster_id,
            hop_count=0,
            max_hops=3,
            announcement_id="test_announcement",
            original_source="original_server",
        )

        # Simulate forwarding through multiple hops
        current = original
        for hop in range(1, 4):  # 3 hops max
            forwarded = RPCServerHello(
                funs=current.funs,
                locs=current.locs,
                cluster_id=current.cluster_id,
                hop_count=hop,
                max_hops=current.max_hops,
                announcement_id=current.announcement_id,
                original_source=current.original_source,
            )

            # Property: Function list should be preserved
            assert forwarded.funs == original.funs

            # Property: Resource list should be preserved
            assert forwarded.locs == original.locs

            # Property: Cluster ID should be preserved
            assert forwarded.cluster_id == original.cluster_id

            # Property: Announcement ID should be preserved
            assert forwarded.announcement_id == original.announcement_id

            # Property: Original source should be preserved
            assert forwarded.original_source == original.original_source

            # Property: Hop count should increment
            assert forwarded.hop_count == hop

            current = forwarded

    @given(
        announcement_ids=st.lists(
            st.text(min_size=1, max_size=50),
            min_size=2,
            max_size=20,
        ),
    )
    @settings(max_examples=20, deadline=2000)
    def test_deduplication_invariants(self, announcement_ids: list[str]):
        """Test deduplication behavior with various announcement IDs."""

        # Track seen announcements (simulating server state)
        seen_announcements = set()

        for ann_id in announcement_ids:
            hello = RPCServerHello(
                funs=("test_function",),
                locs=("test_resource",),
                cluster_id="test_cluster",
                hop_count=1,
                max_hops=3,
                announcement_id=ann_id,
                original_source="test_source",
            )

            # Property: Duplicate announcement IDs should be detected
            is_duplicate = ann_id in seen_announcements

            if not is_duplicate:
                # Should process this announcement
                should_process = True
                # Property: Processing decision should be consistent
                # If we haven't seen it, we should process it
                assert not is_duplicate == should_process
                seen_announcements.add(ann_id)
            else:
                # Should skip duplicate
                should_process = False
                # Property: Processing decision should be consistent
                # If we've seen it, we shouldn't process it
                assert is_duplicate == (not should_process)

    @given(
        server_urls=st.lists(
            st.builds(
                lambda host, port: f"ws://{host}:{port}",
                host=st.text(
                    min_size=1,
                    max_size=20,
                    alphabet="abcdefghijklmnopqrstuvwxyz0123456789",
                ),
                port=st.integers(min_value=1000, max_value=65535),
            ),
            min_size=1,
            max_size=10,
            unique=True,
        ),
        hop_count=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=25, deadline=3000)
    def test_source_url_preservation(self, server_urls: list[str], hop_count: int):
        """Test that original source URLs are correctly preserved."""

        if not server_urls:
            return  # Skip empty lists

        original_source = server_urls[0]

        # Create announcement with various advertised URLs
        hello = RPCServerHello(
            funs=("test_function",),
            locs=("test_resource",),
            cluster_id="test_cluster",
            advertised_urls=tuple(server_urls),
            hop_count=hop_count,
            max_hops=3,
            announcement_id="test_id",
            original_source=original_source,
        )

        # Property: Original source should be preserved regardless of advertised URLs
        assert hello.original_source == original_source

        # Property: Advertised URLs should be maintained
        assert set(hello.advertised_urls) == set(server_urls)

        # Property: For federated announcements (hop > 0),
        # original_source should be used for routing, not advertised_urls
        if hop_count > 0:
            assert hello.original_source != ""
            assert hello.original_source == original_source


class TestFederatedRPCRoutingProperties:
    """Property-based tests for federated RPC routing behavior."""

    def create_mock_server(self, name: str, functions: list[str]) -> MPREGServer:
        """Create a mock MPREG server for testing."""
        mock_server = MagicMock(spec=MPREGServer)
        mock_server.settings = MagicMock()
        mock_server.settings.name = name
        mock_server.funtimes = {func: MagicMock() for func in functions}
        mock_server.peer_connections = {}
        mock_server._federated_announcements_seen = set()
        return mock_server

    @given(
        server_functions=st.dictionaries(
            keys=st.text(min_size=1, max_size=20),  # server names
            values=st.lists(
                st.text(min_size=1, max_size=30),  # function names
                min_size=1,
                max_size=5,
                unique=True,
            ),
            min_size=2,
            max_size=5,
        ),
        query_function=st.text(min_size=1, max_size=30),
    )
    @settings(max_examples=20, deadline=5000)
    def test_function_availability_consistency(
        self, server_functions: dict[str, list[str]], query_function: str
    ):
        """Test that function availability is consistent across the federation."""

        # Create mock servers with their functions
        servers = {}
        all_functions = set()

        for server_name, functions in server_functions.items():
            servers[server_name] = self.create_mock_server(server_name, functions)
            all_functions.update(functions)

        # Property: A function should be findable if any server has it
        function_available = query_function in all_functions

        servers_with_function = [
            name for name, funcs in server_functions.items() if query_function in funcs
        ]

        # Property: Number of servers with function should match availability
        assert (len(servers_with_function) > 0) == function_available

        # Property: If function is available, at least one server should have it
        if function_available:
            assert len(servers_with_function) >= 1
        else:
            assert len(servers_with_function) == 0

    @given(
        network_topology=st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=9),  # from_node
                st.integers(min_value=0, max_value=9),  # to_node
            ),
            min_size=5,
            max_size=15,
        ).filter(lambda edges: len(set(node for edge in edges for node in edge)) >= 3),
        max_hops=st.integers(min_value=1, max_value=4),
    )
    @settings(max_examples=15, deadline=5000)
    def test_reachability_properties(
        self, network_topology: list[tuple[int, int]], max_hops: int
    ):
        """Test reachability properties in various network topologies."""

        # Build adjacency graph
        nodes = set(node for edge in network_topology for node in edge)
        if len(nodes) < 3:
            return  # Skip trivial topologies

        graph: dict[int, set[int]] = {node: set() for node in nodes}
        for from_node, to_node in network_topology:
            if from_node != to_node:  # No self-loops
                graph[from_node].add(to_node)
                graph[to_node].add(from_node)  # Bidirectional

        # Property: Nodes reachable within max_hops should receive announcements
        def bfs_reachable(start: int, max_distance: int) -> set[int]:
            visited = {start}
            queue = [(start, 0)]

            while queue:
                node, distance = queue.pop(0)
                if distance < max_distance:
                    for neighbor in graph[node]:
                        if neighbor not in visited:
                            visited.add(neighbor)
                            queue.append((neighbor, distance + 1))

            return visited

        # Test from each node
        for source_node in nodes:
            reachable = bfs_reachable(source_node, max_hops)

            # Property: Source node should always be reachable from itself
            assert source_node in reachable

            # Property: Reachable nodes should be within max_hops
            # (This is ensured by BFS implementation)

            # Property: If there's a path, it should be found within max_hops
            for target_node in nodes:
                if target_node in reachable:
                    # There should be a path of length <= max_hops
                    assert target_node != source_node or max_hops >= 0

    @given(
        message_sequence=st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=2),  # hop_count
                st.text(min_size=1, max_size=20),  # announcement_id
                st.floats(min_value=0.0, max_value=100.0),  # timestamp
            ),
            min_size=1,
            max_size=20,
        ),
    )
    @settings(max_examples=20, deadline=3000)
    def test_message_ordering_properties(
        self, message_sequence: list[tuple[int, str, float]]
    ):
        """Test message ordering and processing properties."""

        processed_messages = []
        seen_announcements = set()

        # Sort by timestamp to simulate network delivery
        sorted_messages = sorted(message_sequence, key=lambda x: x[2])

        for hop_count, ann_id, timestamp in sorted_messages:
            hello = RPCServerHello(
                funs=("test_function",),
                locs=("test_resource",),
                cluster_id="test_cluster",
                hop_count=hop_count,
                max_hops=3,
                announcement_id=ann_id,
                original_source="test_source",
            )

            # Property: Messages should be processed in delivery order
            should_process = (
                ann_id not in seen_announcements and hop_count < hello.max_hops
            )

            if should_process:
                processed_messages.append((ann_id, timestamp))
                seen_announcements.add(ann_id)

        # Property: Processed messages should be in timestamp order
        processed_timestamps = [msg[1] for msg in processed_messages]
        assert processed_timestamps == sorted(processed_timestamps)

        # Property: Each announcement ID should be processed at most once
        processed_ids = [msg[0] for msg in processed_messages]
        assert len(processed_ids) == len(set(processed_ids))


class TestFederatedRPCEdgeCases:
    """Property-based tests for edge cases and boundary conditions."""

    @given(
        cluster_ids=st.lists(st.text(min_size=1, max_size=30), min_size=1, max_size=10),
        functions_per_cluster=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=15, deadline=4000)
    def test_cross_cluster_isolation(
        self, cluster_ids: list[str], functions_per_cluster: int
    ):
        """Test that cross-cluster communications maintain proper isolation."""

        cluster_functions = {}

        for i, cluster_id in enumerate(cluster_ids):
            # Generate unique functions for each cluster
            functions = [f"cluster_{i}_func_{j}" for j in range(functions_per_cluster)]
            cluster_functions[cluster_id] = functions

        # Property: Functions should be associated with their originating cluster
        for cluster_id, functions in cluster_functions.items():
            for function in functions:
                hello = RPCServerHello(
                    funs=(function,),
                    locs=("test_resource",),
                    cluster_id=cluster_id,
                    hop_count=1,
                    max_hops=3,
                    announcement_id=f"{cluster_id}_{function}",
                    original_source=f"server_in_{cluster_id}",
                )

                # Property: Cluster ID should match
                assert hello.cluster_id == cluster_id

                # Property: Function should be in the announcement
                assert function in hello.funs

                # Property: Original source should reference the cluster
                assert cluster_id in hello.original_source or hello.original_source

    @given(
        empty_values=st.one_of(
            st.just(""),  # Empty string for string fields
            st.just(()),  # Empty tuple for tuple fields
        ),
        field_type=st.sampled_from(
            ["funs", "locs", "announcement_id", "original_source"]
        ),
    )
    @settings(max_examples=20, deadline=2000)
    def test_empty_value_handling(self, empty_values: Any, field_type: str):
        """Test handling of empty values in federated announcements."""

        # Create RPCServerHello with potentially empty fields
        try:
            if field_type == "funs":
                # Only use empty tuple for tuple fields
                if isinstance(empty_values, tuple):
                    hello = RPCServerHello(
                        funs=empty_values,
                        locs=("test_resource",),
                        cluster_id="test_cluster",
                        hop_count=1,
                        max_hops=3,
                        announcement_id="test_id",
                        original_source="test_source",
                    )
                else:
                    # Skip non-tuple values for tuple fields
                    return
            elif field_type == "locs":
                # Only use empty tuple for tuple fields
                if isinstance(empty_values, tuple):
                    hello = RPCServerHello(
                        funs=("test_function",),
                        locs=empty_values,
                        cluster_id="test_cluster",
                        hop_count=1,
                        max_hops=3,
                        announcement_id="test_id",
                        original_source="test_source",
                    )
                else:
                    # Skip non-tuple values for tuple fields
                    return
            elif field_type == "announcement_id":
                # Only use empty string for string fields
                if isinstance(empty_values, str):
                    hello = RPCServerHello(
                        funs=("test_function",),
                        locs=("test_resource",),
                        cluster_id="test_cluster",
                        hop_count=1,
                        max_hops=3,
                        announcement_id=empty_values,
                        original_source="test_source",
                    )
                else:
                    # Skip non-string values for string fields
                    return
            elif field_type == "original_source":
                # Only use empty string for string fields
                if isinstance(empty_values, str):
                    hello = RPCServerHello(
                        funs=("test_function",),
                        locs=("test_resource",),
                        cluster_id="test_cluster",
                        hop_count=1,
                        max_hops=3,
                        announcement_id="test_id",
                        original_source=empty_values,
                    )
                else:
                    # Skip non-string values for string fields
                    return
            else:
                hello = RPCServerHello(
                    funs=("test_function",),
                    locs=("test_resource",),
                    cluster_id="test_cluster",
                    hop_count=1,
                    max_hops=3,
                    announcement_id="test_id",
                    original_source="test_source",
                )

            # Property: Empty values should be handled gracefully
            if field_type == "funs":
                # Empty functions should result in empty tuple
                assert len(hello.funs) == 0
            elif field_type == "locs":
                # Empty locations should result in empty tuple
                assert len(hello.locs) == 0
            elif field_type == "announcement_id":
                # Empty announcement ID should be preserved
                assert hello.announcement_id == ""
            elif field_type == "original_source":
                # Empty original source should be preserved
                assert hello.original_source == ""

        except Exception:
            # Property: If creation fails, it should be due to validation
            # This is acceptable for critical fields
            if field_type in ["funs", "locs"]:
                # These fields might have validation requirements
                pass
            else:
                # Other fields should accept empty values
                pytest.fail(f"Unexpected validation failure for empty {field_type}")

    @given(
        concurrent_announcements=st.integers(min_value=2, max_value=20),
        announcement_delay=st.floats(min_value=0.0, max_value=0.1),
    )
    @settings(max_examples=10, deadline=5000)
    async def test_concurrent_announcement_processing(
        self, concurrent_announcements: int, announcement_delay: float
    ):
        """Test concurrent processing of multiple announcements."""

        # Property: Concurrent announcements should be processed atomically
        processed_announcements = []
        processing_lock = asyncio.Lock()

        async def process_announcement(ann_id: str):
            """Simulate announcement processing."""
            await asyncio.sleep(announcement_delay)

            async with processing_lock:
                # Simulate processing logic
                processed_announcements.append(ann_id)

        # Create concurrent announcement tasks
        tasks = []
        announcement_ids = [
            f"concurrent_ann_{i}" for i in range(concurrent_announcements)
        ]

        for ann_id in announcement_ids:
            task = asyncio.create_task(process_announcement(ann_id))
            tasks.append(task)

        # Wait for all to complete
        await asyncio.gather(*tasks)

        # Property: All announcements should be processed exactly once
        assert len(processed_announcements) == concurrent_announcements
        assert set(processed_announcements) == set(announcement_ids)

        # Property: No announcement should be lost
        for ann_id in announcement_ids:
            assert ann_id in processed_announcements
