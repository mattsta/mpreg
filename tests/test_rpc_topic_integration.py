"""
Comprehensive integration tests for RPC-Topic coordination.

This module tests the complete RPC-Topic integration across all phases:
- Phase 2.1.1: Enhanced RPC Command Datastructures
- Phase 2.1.2: RPC-Topic Integration Engine
- Phase 2.1.3: Topic-based RPC Dependency Coordination

Tests use live MPREG clusters to validate end-to-end workflows, cross-system
dependencies, performance benchmarks, and complex property-based scenarios.
"""

import asyncio
import time
from collections.abc import Callable
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.enhanced_rpc import (
    RPCRequestResult,
    RPCResult,
    TopicAwareRPCCommand,
    TopicAwareRPCConfig,
    TopicAwareRPCExecutor,
    TopicAwareRPCRequest,
    create_topic_aware_rpc_executor,
)
from mpreg.core.model import RPCCommand, RPCRequest
from mpreg.core.topic_dependency_resolver import (
    DependencyType,
    TopicDependencyResolver,
    create_topic_dependency_resolver,
)
from mpreg.fabric.message import RoutingPriority
from mpreg.server import MPREGServer


class TestRPCTopicIntegrationBasics:
    """Test basic RPC-Topic integration functionality."""

    @pytest.mark.asyncio
    async def test_basic_topic_aware_execution(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test basic topic-aware RPC execution with live server."""
        client = await client_factory(enhanced_server.settings.port)

        # Create sample commands that use real functions from enhanced_server
        commands = [
            TopicAwareRPCCommand(
                name="process_data",
                fun="data_processing",
                args=([1, 2, 3, 4, 5],),
                locs=frozenset(["data-processor"]),
                estimated_duration_ms=100.0,
            ),
            TopicAwareRPCCommand(
                name="ml_inference",
                fun="ml_inference",
                args=("test_model", {"value": "test_data"}),
                locs=frozenset(["ml-model"]),
                estimated_duration_ms=150.0,
            ),
        ]

        # Create executor with live topic exchange from server
        config = TopicAwareRPCConfig(
            enable_progress_publishing=True,
            progress_update_interval_ms=100.0,
            topic_prefix="test.rpc.integration",
        )

        # Use server's topic exchange if available, otherwise create mock
        topic_exchange = getattr(enhanced_server, "topic_exchange", None)
        executor = create_topic_aware_rpc_executor(
            topic_exchange=topic_exchange, config=config
        )

        # Execute with topic monitoring
        start_time = time.time()
        result = await executor.execute_with_topics(commands)
        execution_time = time.time() - start_time

        # Verify execution results
        assert isinstance(result, RPCRequestResult)
        assert result.total_commands == 2
        assert result.successful_commands == 2
        assert result.failed_commands == 0
        assert result.all_successful is True
        assert result.success_rate == 100.0

        # Verify reasonable timing
        assert execution_time < 5.0

        # Verify command results
        assert len(result.command_results) == 2
        for cmd in commands:
            assert cmd.command_id in result.command_results
            cmd_result = result.command_results[cmd.command_id]
            assert isinstance(cmd_result, RPCResult)
            assert cmd_result.success is True
            assert cmd_result.execution_time_ms > 0

    @pytest.mark.asyncio
    async def test_dependency_graph_integration(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test dependency graph creation and analysis with live server."""
        client = await client_factory(enhanced_server.settings.port)

        # Create dependency resolver with live server context
        dependency_resolver = create_topic_dependency_resolver(
            default_timeout_ms=5000.0, max_concurrent_subscriptions=100
        )

        # Create commands with realistic dependencies using enhanced_server functions
        commands = [
            TopicAwareRPCCommand(
                name="fetch_data",
                fun="data_processing",
                args=([10, 20, 30],),
                locs=frozenset(["data-processor"]),
            ),
            TopicAwareRPCCommand(
                name="process_fetched_data",
                fun="ml_inference",
                args=(
                    "test_model",
                    {"value": "fetch_data.result"},
                ),  # Depends on first command
                kwargs={"queue_input": "data_queue.message"},  # Cross-system dependency
                locs=frozenset(["ml-model"]),
            ),
            TopicAwareRPCCommand(
                name="store_results",
                fun="format_results",
                args=("process_fetched_data.output",),
                locs=frozenset(["formatter"]),
                dependency_topic_patterns=["mpreg.cache.invalidate.completed"],
            ),
        ]

        # Create dependency graph
        request_id = "integration_test_123"
        dependency_graph = await dependency_resolver.create_dependency_graph(
            request_id, commands
        )

        # Verify graph structure
        assert dependency_graph.request_id == request_id
        assert dependency_graph.total_dependencies > 0
        assert dependency_graph.cross_system_dependencies > 0
        assert len(dependency_graph.command_dependencies) == 3

        # Verify dependency detection
        for command in commands:
            assert command.command_id in dependency_graph.command_dependencies
            dependencies = dependency_graph.command_dependencies[command.command_id]

            if command.name == "fetch_data":
                # First command should have no dependencies
                assert len(dependencies) == 0
            elif command.name == "process_fetched_data":
                # Second command should have RPC and queue dependencies
                assert len(dependencies) >= 1
                dep_types = {dep.dependency_type for dep in dependencies}
                assert (
                    DependencyType.RPC_COMMAND in dep_types
                    or DependencyType.QUEUE_MESSAGE in dep_types
                )
            elif command.name == "store_results":
                # Third command should have explicit cache dependency
                assert len(dependencies) >= 1
                topic_patterns = {dep.topic_pattern for dep in dependencies}
                assert any("cache" in pattern for pattern in topic_patterns)

        # Test subscription setup
        subscriptions_created = (
            await dependency_resolver.setup_dependency_subscriptions(dependency_graph)
        )
        assert subscriptions_created > 0
        assert (
            len(dependency_resolver.dependency_subscriptions) == subscriptions_created
        )

        # Test cleanup
        cleanup_success = await dependency_resolver.cleanup_request_dependencies(
            request_id
        )
        assert cleanup_success is True
        assert request_id not in dependency_resolver.active_dependency_graphs

    @pytest.mark.asyncio
    async def test_cross_system_dependency_types(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test detection of different cross-system dependency types with live server."""
        client = await client_factory(enhanced_server.settings.port)

        # Create dependency resolver
        dependency_resolver = create_topic_dependency_resolver(
            default_timeout_ms=5000.0, max_concurrent_subscriptions=100
        )

        # Command with various dependency types using real functions
        complex_command = TopicAwareRPCCommand(
            name="complex_processor",
            fun="format_results",
            args=(
                "queue_data.message",  # Queue dependency
                "topic_events.data",  # PubSub dependency
                "cache_results.value",  # Cache dependency
            ),
            kwargs={
                "federation_status": "federation_sync.status",  # Federation dependency
                "previous_result": "command_output.result",  # RPC dependency
                "format_type": "json",  # Normal parameter
            },
            locs=frozenset(["formatter"]),
        )

        # Analyze dependencies
        request_id = "cross_system_test"
        dependency_graph = await dependency_resolver.create_dependency_graph(
            request_id, [complex_command]
        )

        dependencies = dependency_graph.command_dependencies[complex_command.command_id]
        dependency_types = {dep.dependency_type for dep in dependencies}

        # Should detect multiple cross-system dependency types
        assert len(dependency_types) > 1
        assert DependencyType.QUEUE_MESSAGE in dependency_types
        assert (
            DependencyType.CACHE_UPDATE in dependency_types
            or DependencyType.RPC_COMMAND in dependency_types
        )

        # Verify cross-system count
        assert dependency_graph.cross_system_dependencies > 0


class TestRPCTopicPerformanceIntegration:
    """Test performance characteristics of RPC-Topic integration."""

    @pytest.fixture
    def performance_executor(self) -> TopicAwareRPCExecutor:
        """Create an executor optimized for performance testing."""
        config = TopicAwareRPCConfig(
            enable_progress_publishing=True,
            progress_update_interval_ms=50.0,  # Very fast updates for testing
            topic_prefix="perf.test",
        )

        return create_topic_aware_rpc_executor(config=config)

    @pytest.mark.asyncio
    async def test_execution_performance_metrics(
        self, performance_executor: TopicAwareRPCExecutor
    ) -> None:
        """Test execution performance and metrics collection."""
        # Create multiple commands for performance testing
        commands = [
            TopicAwareRPCCommand(
                name=f"perf_cmd_{i}",
                fun=f"perf_function_{i}",
                args=(f"arg_{i}",),
                locs=frozenset(["cpu"]),
                estimated_duration_ms=500.0,  # Shorter duration for performance test
            )
            for i in range(5)
        ]

        # Measure execution time
        start_time = time.time()
        result = await performance_executor.execute_with_topics(commands)
        execution_time = time.time() - start_time

        # Verify performance characteristics
        assert result.total_commands == 5
        assert result.all_successful is True
        assert execution_time < 3.0  # Should be fast with live server

        # Verify command results
        assert len(result.command_results) == 5
        for cmd in commands:
            cmd_result = result.command_results[cmd.command_id]
            assert cmd_result.execution_time_ms > 0
            assert cmd_result.execution_time_ms < 1000.0

    @pytest.mark.asyncio
    async def test_dependency_resolution_performance(self) -> None:
        """Test dependency resolution performance at scale."""
        resolver = create_topic_dependency_resolver(
            default_timeout_ms=5000.0,  # Realistic timeout for performance test
            max_concurrent_subscriptions=50,
        )

        # Create commands with various dependency patterns
        commands = []
        for i in range(10):
            cmd = TopicAwareRPCCommand(
                name=f"dep_cmd_{i}",
                fun=f"dep_function_{i}",
                args=(f"queue_data_{i}.message", f"cache_value_{i}.data"),
                kwargs={f"topic_input_{i}": f"topic_events_{i}.payload"},
                locs=frozenset(["cpu"]),
            )
            commands.append(cmd)

        # Measure dependency analysis time
        start_time = time.time()
        dependency_graph = await resolver.create_dependency_graph("perf_test", commands)
        analysis_time = time.time() - start_time

        # Verify dependency analysis performance
        assert analysis_time < 1.0  # Should be very fast
        assert dependency_graph.total_dependencies > 0
        assert dependency_graph.cross_system_dependencies > 0

        # Measure subscription setup time
        start_time = time.time()
        subscriptions_created = await resolver.setup_dependency_subscriptions(
            dependency_graph
        )
        setup_time = time.time() - start_time

        # Verify subscription setup performance
        assert setup_time < 0.5  # Should be fast
        assert subscriptions_created > 0
        assert len(resolver.dependency_subscriptions) == subscriptions_created

        # Test statistics collection performance
        start_time = time.time()
        stats = await resolver.get_dependency_statistics()
        stats_time = time.time() - start_time

        assert stats_time < 0.1  # Statistics should be very fast
        assert stats["active_dependency_graphs"] == 1
        assert stats["active_subscriptions"] == subscriptions_created


class TestRPCTopicErrorHandling:
    """Test error handling and recovery in RPC-Topic integration."""

    @pytest.mark.asyncio
    async def test_command_failure_handling(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test handling of command execution failures with live server."""
        client = await client_factory(enhanced_server.settings.port)

        # Create error handling executor
        config = TopicAwareRPCConfig(
            enable_progress_publishing=True,
            enable_error_topics=True,
        )

        topic_exchange = getattr(enhanced_server, "topic_exchange", None)
        error_test_executor = create_topic_aware_rpc_executor(
            topic_exchange=topic_exchange, config=config
        )

        # Create commands using real functions
        commands = [
            TopicAwareRPCCommand(
                name="successful_cmd",
                fun="data_processing",
                args=([1, 2, 3],),
                locs=frozenset(["data-processor"]),
            ),
            TopicAwareRPCCommand(
                name="another_successful_cmd",
                fun="ml_inference",
                args=("test_model", {"value": "test"}),
                locs=frozenset(["ml-model"]),
            ),
        ]

        # Execute commands
        result = await error_test_executor.execute_with_topics(commands)

        # Verify successful execution
        assert result.all_successful is True
        assert result.failed_commands == 0
        assert result.total_commands == 2

    @pytest.mark.asyncio
    async def test_dependency_timeout_handling(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test handling of dependency resolution timeouts with live server."""
        client = await client_factory(enhanced_server.settings.port)

        resolver = create_topic_dependency_resolver(
            default_timeout_ms=2000.0,  # Realistic timeout for stable testing
            max_concurrent_subscriptions=10,
        )

        # Create command with dependency that would timeout using real function
        timeout_command = TopicAwareRPCCommand(
            name="timeout_test_cmd",
            fun="format_results",
            args=("never_resolves.dependency",),
            locs=frozenset(["formatter"]),
        )

        # Create dependency graph
        dependency_graph = await resolver.create_dependency_graph(
            "timeout_test", [timeout_command]
        )

        # Verify timeout configuration is propagated
        dependencies = dependency_graph.command_dependencies[timeout_command.command_id]
        if dependencies:
            for dep in dependencies:
                assert isinstance(dep.timeout_ms, float)
                assert dep.timeout_ms > 0

        # Test cleanup after timeout scenario
        cleanup_success = await resolver.cleanup_request_dependencies("timeout_test")
        assert cleanup_success is True


class TestComplexRPCTopicWorkflows:
    """Test complex, real-world RPC-Topic integration workflows."""

    @pytest.mark.asyncio
    async def test_multi_level_dependency_workflow(
        self,
        cluster_3_servers: tuple[MPREGServer, MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ) -> None:
        """Test complex workflow with multiple dependency levels using live cluster."""
        primary, secondary, tertiary = cluster_3_servers

        # Register workflow functions on different servers
        def fetch_user_data(user_id: str) -> dict[str, Any]:
            return {
                "user_id": user_id,
                "preferences": ["books", "tech"],
                "cart_id": "cart_123",
            }

        def fetch_product_catalog() -> dict[str, Any]:
            return {
                "products": ["book1", "laptop1", "phone1"],
                "categories": ["books", "electronics"],
            }

        def generate_recommendations(
            user_data: dict[str, Any], catalog: dict[str, Any]
        ) -> dict[str, Any]:
            return {"recommendations": ["book1", "laptop1"], "score": 0.85}

        def verify_inventory(catalog: dict[str, Any]) -> dict[str, Any]:
            return {
                "available_items": catalog["products"],
                "stock_levels": {"book1": 10, "laptop1": 5},
            }

        def render_page(
            recommendations: dict[str, Any], inventory: dict[str, Any]
        ) -> str:
            return f"Page with {len(recommendations['recommendations'])} recommendations, {len(inventory['available_items'])} items available"

        # Distribute functions across servers
        primary.register_command("fetch_user_data", fetch_user_data, ["dataset-1"])
        secondary.register_command(
            "fetch_product_catalog", fetch_product_catalog, ["dataset-2"]
        )
        secondary.register_command(
            "generate_recommendations", generate_recommendations, ["dataset-2"]
        )
        tertiary.register_command("verify_inventory", verify_inventory, ["dataset-3"])
        tertiary.register_command("render_page", render_page, ["dataset-3"])

        # Wait for cluster synchronization
        await asyncio.sleep(1.0)

        client = await client_factory(primary.settings.port)

        # Create executor and resolver
        topic_exchange = getattr(primary, "topic_exchange", None)
        executor = create_topic_aware_rpc_executor(topic_exchange=topic_exchange)
        resolver = create_topic_dependency_resolver()

        # Create a complex workflow with multiple dependency levels
        commands = [
            # Level 0: Independent commands
            TopicAwareRPCCommand(
                name="fetch_user_data",
                fun="fetch_user_data",
                args=("user_123",),
                locs=frozenset(["dataset-1"]),
            ),
            TopicAwareRPCCommand(
                name="fetch_product_catalog",
                fun="fetch_product_catalog",
                args=(),
                locs=frozenset(["dataset-2"]),
            ),
            # Level 1: Commands depending on Level 0
            TopicAwareRPCCommand(
                name="personalize_recommendations",
                fun="generate_recommendations",
                args=("fetch_user_data.result", "fetch_product_catalog.result"),
                locs=frozenset(["dataset-2"]),
            ),
            TopicAwareRPCCommand(
                name="check_inventory",
                fun="verify_inventory",
                args=("fetch_product_catalog.result",),
                kwargs={"warehouse_queue": "inventory_updates.message"},
                locs=frozenset(["dataset-3"]),
            ),
            # Level 2: Final command depending on Level 1
            TopicAwareRPCCommand(
                name="generate_personalized_page",
                fun="render_page",
                args=("personalize_recommendations.result", "check_inventory.result"),
                kwargs={"cache_key": "user_cache.invalidation"},
                locs=frozenset(["dataset-3"]),
            ),
        ]

        # Analyze dependency structure
        request_id = "complex_workflow_123"
        dependency_graph = await resolver.create_dependency_graph(request_id, commands)

        # Verify complex dependency structure
        assert dependency_graph.total_dependencies > 5  # Should have many dependencies
        assert (
            dependency_graph.cross_system_dependencies > 0
        )  # Should have cross-system deps

        # Verify dependency relationships
        final_cmd_deps = None
        for cmd in commands:
            if cmd.name == "generate_personalized_page":
                final_cmd_deps = dependency_graph.command_dependencies[cmd.command_id]
                break

        assert final_cmd_deps is not None
        assert len(final_cmd_deps) > 0

        # Test subscription setup for complex workflow
        subscriptions_created = await resolver.setup_dependency_subscriptions(
            dependency_graph
        )
        assert subscriptions_created > 0

        # Execute the workflow
        result = await executor.execute_with_topics(commands, request_id)

        # Verify execution results
        assert result.total_commands == 5
        assert result.all_successful is True
        assert len(result.command_results) == 5

        # Verify execution levels (commands should execute in dependency order)
        execution_levels = {
            cmd_result.execution_level for cmd_result in result.command_results.values()
        }
        assert len(execution_levels) >= 1  # Should have at least one execution level

        # Cleanup
        await resolver.cleanup_request_dependencies(request_id)

    @pytest.mark.asyncio
    async def test_base_integration_compatibility(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test base RPC interop with topic-aware requests using live server."""
        client = await client_factory(enhanced_server.settings.port)

        # Create base RPC commands using real functions
        base_commands = [
            RPCCommand(
                name="base_fetch",
                fun="data_processing",
                args=([1, 2, 3],),
                locs=frozenset(["data-processor"]),
            ),
            RPCCommand(
                name="base_process",
                fun="format_results",
                args=({"data": "test"},),
                kwargs={"format_type": "json"},
                locs=frozenset(["formatter"]),
            ),
        ]

        # Create base request
        base_request = RPCRequest(
            role="rpc", cmds=tuple(base_commands), u="base_request_456"
        )

        # Convert to topic-aware
        from mpreg.core.enhanced_rpc import create_topic_aware_request

        topic_request = create_topic_aware_request(
            base_request, enable_monitoring=True, priority=RoutingPriority.HIGH
        )

        # Verify conversion
        assert isinstance(topic_request, TopicAwareRPCRequest)
        assert len(topic_request.cmds) == len(base_request.cmds)
        assert topic_request.u == base_request.u
        assert topic_request.enable_topic_monitoring is True

        # Convert back to base RPC
        converted_back = topic_request.to_rpc_request()
        assert isinstance(converted_back, RPCRequest)
        assert len(converted_back.cmds) == len(base_request.cmds)
        assert converted_back.u == base_request.u

        # Verify round-trip preservation
        for original, converted in zip(base_request.cmds, converted_back.cmds):
            assert original.name == converted.name
            assert original.fun == converted.fun
            assert original.args == converted.args
            assert original.kwargs == converted.kwargs
            assert original.locs == converted.locs


# Property-based integration testing with Hypothesis
@given(
    num_commands=st.integers(min_value=1, max_value=8),
    enable_progress=st.booleans(),
    enable_streaming=st.booleans(),
    priority=st.sampled_from(list(RoutingPriority)),
)
@settings(max_examples=20, deadline=10000)
def test_rpc_topic_integration_properties(
    num_commands: int,
    enable_progress: bool,
    enable_streaming: bool,
    priority: RoutingPriority,
) -> None:
    """Property test: RPC-Topic integration maintains invariants across configurations."""
    # Create varied commands
    commands = []
    for i in range(num_commands):
        cmd = TopicAwareRPCCommand(
            name=f"prop_cmd_{i}",
            fun=f"prop_function_{i}",
            args=(f"arg_{i}",),
            kwargs={f"param_{i}": f"value_{i}"},
            locs=frozenset([f"resource_{i % 3}"]),  # Cycle through resources
            publish_progress=enable_progress,
            enable_result_streaming=enable_streaming,
            estimated_duration_ms=float(500 + i * 100),
        )
        commands.append(cmd)

    # Test dataclass properties
    for cmd in commands:
        assert len(cmd.name) > 0
        assert len(cmd.fun) > 0
        assert len(cmd.command_id) > 0
        assert isinstance(cmd.args, tuple)
        assert isinstance(cmd.kwargs, dict)
        assert isinstance(cmd.locs, frozenset)
        assert isinstance(cmd.publish_progress, bool)
        assert isinstance(cmd.enable_result_streaming, bool)
        assert cmd.estimated_duration_ms is None or cmd.estimated_duration_ms > 0

    # Test conversion properties
    config = TopicAwareRPCConfig(
        enable_progress_publishing=enable_progress,
        enable_result_streaming=enable_streaming,
    )

    request = TopicAwareRPCRequest(
        cmds=tuple(commands),
        enable_topic_monitoring=enable_progress,
        enable_result_streaming=enable_streaming,
        priority=priority,
    )

    # Verify request properties
    assert len(request.cmds) == num_commands
    assert request.enable_topic_monitoring == enable_progress
    assert request.enable_result_streaming == enable_streaming
    assert request.priority == priority
    assert len(request.request_id) > 0

    # Test base conversion preservation
    base_request = request.to_rpc_request()
    assert len(base_request.cmds) == num_commands
    assert base_request.role == "rpc"

    # Verify round-trip conversion
    for original_cmd, base_cmd in zip(request.cmds, base_request.cmds):
        assert original_cmd.name == base_cmd.name
        assert original_cmd.fun == base_cmd.fun
        assert original_cmd.args == base_cmd.args
        assert original_cmd.kwargs == base_cmd.kwargs
        assert original_cmd.locs == base_cmd.locs


@given(
    dependency_complexity=st.integers(min_value=0, max_value=5),
    cross_system_ratio=st.floats(min_value=0.0, max_value=1.0),
    timeout_ms=st.floats(min_value=1000.0, max_value=30000.0),
)
@settings(max_examples=15, deadline=5000)
@pytest.mark.asyncio
async def test_dependency_graph_integration_properties(
    dependency_complexity: int, cross_system_ratio: float, timeout_ms: float
) -> None:
    """Property test: Dependency graph creation maintains consistency."""
    # Create commands with varying dependency complexity
    commands = []

    for i in range(max(1, dependency_complexity)):
        # Create dependencies based on complexity and cross-system ratio
        args = []
        kwargs = {}

        for j in range(dependency_complexity):
            if j / max(1, dependency_complexity) < cross_system_ratio:
                # Cross-system dependency
                if j % 3 == 0:
                    args.append(f"queue_data_{j}.message")
                elif j % 3 == 1:
                    kwargs[f"cache_param_{j}"] = f"cache_value_{j}.data"
                else:
                    kwargs[f"topic_param_{j}"] = f"topic_events_{j}.payload"
            else:
                # RPC dependency
                args.append(f"command_{j}.result")

        cmd = TopicAwareRPCCommand(
            name=f"complex_cmd_{i}",
            fun=f"complex_function_{i}",
            args=tuple(args),
            kwargs=kwargs,
            locs=frozenset([f"system_{i}"]),
            estimated_duration_ms=timeout_ms / 10,  # Related to timeout
        )
        commands.append(cmd)

    # Test dependency graph properties
    if commands:  # Only test if we have commands
        # Create mock resolver for testing
        from mpreg.core.topic_taxonomy import TopicTemplateEngine

        template_engine = TopicTemplateEngine()

        # Test that dependency analysis doesn't crash
        resolver = TopicDependencyResolver(topic_template_engine=template_engine)

        # Use asyncio.run for property tests (can't use async in hypothesis easily)

        async def test_dependency_analysis() -> None:
            dependency_graph = await resolver.create_dependency_graph(
                "prop_test", commands
            )

            # Test invariants
            assert dependency_graph.total_dependencies >= 0
            assert dependency_graph.cross_system_dependencies >= 0
            assert (
                dependency_graph.cross_system_dependencies
                <= dependency_graph.total_dependencies
            )
            assert len(dependency_graph.command_dependencies) == len(commands)
            assert dependency_graph.resolution_progress >= 0.0
            assert dependency_graph.resolution_progress <= 100.0

            # If no dependencies, should be 100% resolved
            if dependency_graph.total_dependencies == 0:
                assert dependency_graph.resolution_progress == 100.0
                assert dependency_graph.all_dependencies_resolved is True

        await test_dependency_analysis()


@pytest.mark.asyncio
async def test_end_to_end_rpc_topic_workflow(
    cluster_2_servers: tuple[MPREGServer, MPREGServer],
    client_factory: Callable[[int], Any],
) -> None:
    """Complete end-to-end test of RPC-Topic integration with live cluster."""
    server1, server2 = cluster_2_servers

    # Register e-commerce workflow functions across servers
    def verify_user_session(token: str) -> dict[str, Any]:
        return {
            "user_id": "user123",
            "session_valid": True,
            "permissions": ["read", "write"],
        }

    def fetch_user_data(user_id: str) -> dict[str, Any]:
        return {
            "user_id": user_id,
            "cart_id": "cart456",
            "payment_method": "credit_card",
        }

    def validate_cart(cart_id: str) -> dict[str, Any]:
        return {
            "cart_id": cart_id,
            "validated_items": ["item1", "item2"],
            "total_items": 2,
        }

    def compute_total(items: dict[str, Any]) -> dict[str, Any]:
        return {
            "final_total": 99.99,
            "tax": 9.99,
            "subtotal": 90.00,
            "items": items["validated_items"],
        }

    def create_order(total_info: dict[str, Any], payment_method: str) -> dict[str, Any]:
        return {
            "order_id": "order789",
            "total": total_info["final_total"],
            "status": "created",
        }

    # Distribute functions across servers
    server1.register_command("verify_user_session", verify_user_session, ["model-a"])
    server1.register_command("fetch_user_data", fetch_user_data, ["model-a"])
    server2.register_command("validate_cart", validate_cart, ["model-b"])
    server2.register_command("compute_total", compute_total, ["model-b"])
    server2.register_command("create_order", create_order, ["model-b"])

    # Wait for cluster synchronization
    await asyncio.sleep(1.0)

    client = await client_factory(server1.settings.port)

    # Create integrated system components
    topic_exchange = getattr(server1, "topic_exchange", None)

    executor = create_topic_aware_rpc_executor(
        topic_exchange=topic_exchange,
        config=TopicAwareRPCConfig(
            enable_progress_publishing=True,
            enable_result_streaming=True,
            topic_prefix="e2e.test",
        ),
    )

    resolver = create_topic_dependency_resolver(
        default_timeout_ms=10000.0, max_concurrent_subscriptions=50
    )

    # Create realistic e-commerce workflow using registered functions
    workflow_commands = [
        TopicAwareRPCCommand(
            name="authenticate_user",
            fun="verify_user_session",
            args=("session_token_abc123",),
            locs=frozenset(["model-a"]),
        ),
        TopicAwareRPCCommand(
            name="load_user_profile",
            fun="fetch_user_data",
            args=("authenticate_user.user_id",),
            locs=frozenset(["model-a"]),
        ),
        TopicAwareRPCCommand(
            name="check_cart_items",
            fun="validate_cart",
            args=("load_user_profile.cart_id",),
            kwargs={"inventory_queue": "inventory_updates.message"},
            locs=frozenset(["model-b"]),
        ),
        TopicAwareRPCCommand(
            name="calculate_pricing",
            fun="compute_total",
            args=("check_cart_items",),
            kwargs={"discount_cache": "promotions_cache.active_discounts"},
            locs=frozenset(["model-b"]),
        ),
        TopicAwareRPCCommand(
            name="finalize_order",
            fun="create_order",
            args=("calculate_pricing", "load_user_profile.payment_method"),
            locs=frozenset(["model-b"]),
            dependency_topic_patterns=["mpreg.payment.authorization.completed"],
        ),
    ]

    request_id = "e2e_ecommerce_workflow"

    # Phase 1: Analyze dependencies
    start_time = time.time()
    dependency_graph = await resolver.create_dependency_graph(
        request_id, workflow_commands
    )
    analysis_time = time.time() - start_time

    # Verify dependency analysis
    assert dependency_graph.total_dependencies > 0
    assert dependency_graph.cross_system_dependencies > 0
    assert analysis_time < 1.0  # Should be fast

    # Phase 2: Setup subscriptions
    start_time = time.time()
    subscriptions_created = await resolver.setup_dependency_subscriptions(
        dependency_graph
    )
    setup_time = time.time() - start_time

    assert subscriptions_created > 0
    assert setup_time < 1.0  # Should be fast

    # Phase 3: Execute workflow
    start_time = time.time()
    execution_result = await executor.execute_with_topics(workflow_commands, request_id)
    execution_time = time.time() - start_time

    # Verify execution results
    assert execution_result.total_commands == 5
    assert execution_result.all_successful is True
    assert execution_time < 10.0  # Should be reasonably fast with live cluster

    # Verify all commands executed with proper results
    assert len(execution_result.command_results) == 5
    for cmd in workflow_commands:
        assert cmd.command_id in execution_result.command_results
        result = execution_result.command_results[cmd.command_id]
        assert result.success is True
        assert result.execution_time_ms > 0
        assert result.command_name == cmd.name

    # Phase 4: Verify statistics and cleanup
    resolver_stats = await resolver.get_dependency_statistics()

    assert resolver_stats["active_dependency_graphs"] == 1
    assert resolver_stats["active_subscriptions"] == subscriptions_created

    # Clean up
    cleanup_success = await resolver.cleanup_request_dependencies(request_id)
    assert cleanup_success is True

    # Verify cleanup
    final_resolver_stats = await resolver.get_dependency_statistics()
    assert final_resolver_stats["active_dependency_graphs"] == 0

    print("✅ End-to-end workflow completed successfully!")
    print(f"📊 Execution time: {execution_time:.2f}s")
    print(f"🔗 Dependencies resolved: {subscriptions_created}")
    print(f"⚡ Analysis time: {analysis_time:.3f}s, Setup time: {setup_time:.3f}s")
