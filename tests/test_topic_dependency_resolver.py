"""
Tests for topic-based RPC dependency coordination.

This module tests the Phase 2.1.3 implementation:
- Topic-based dependency resolution
- Cross-system dependency support
- Enhanced dependency analytics
- Self-managing subscription lifecycle
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.enhanced_rpc import TopicAwareRPCCommand
from mpreg.core.topic_dependency_resolver import (
    DependencyGraph,
    DependencyResolutionEvent,
    DependencyResolutionStrategy,
    DependencySpecification,
    DependencyStatus,
    DependencySubscription,
    DependencyType,
    TopicDependencyResolver,
    create_topic_dependency_resolver,
)
from mpreg.core.topic_taxonomy import TopicTemplateEngine


class TestDependencySpecification:
    """Test the dependency specification dataclass."""

    def test_dependency_specification_creation(self) -> None:
        """Test creating dependency specifications."""
        dep_spec = DependencySpecification(
            dependency_id="dep_123",
            dependency_type=DependencyType.RPC_COMMAND,
            topic_pattern="mpreg.rpc.*.command.*.completed",
            field_path="result.data",
            timeout_ms=5000.0,
            required=True,
            description="Test dependency",
        )

        assert dep_spec.dependency_id == "dep_123"
        assert dep_spec.dependency_type == DependencyType.RPC_COMMAND
        assert dep_spec.topic_pattern == "mpreg.rpc.*.command.*.completed"
        assert dep_spec.field_path == "result.data"
        assert dep_spec.timeout_ms == 5000.0
        assert dep_spec.required is True
        assert dep_spec.description == "Test dependency"
        assert dep_spec.resolution_strategy == DependencyResolutionStrategy.IMMEDIATE

    def test_dependency_specification_defaults(self) -> None:
        """Test dependency specification with default values."""
        dep_spec = DependencySpecification(
            dependency_id="dep_minimal",
            dependency_type=DependencyType.QUEUE_MESSAGE,
            topic_pattern="mpreg.queue.*.completed",
        )

        assert dep_spec.resolution_strategy == DependencyResolutionStrategy.IMMEDIATE
        assert dep_spec.timeout_ms == 30000.0
        assert dep_spec.required is True
        assert dep_spec.expected_value is None
        assert dep_spec.value_transformer is None
        assert dep_spec.description == ""


class TestDependencyResolutionEvent:
    """Test the dependency resolution event dataclass."""

    def test_resolution_event_creation(self) -> None:
        """Test creating dependency resolution events."""
        event = DependencyResolutionEvent(
            dependency_id="dep_456",
            command_id="cmd_789",
            request_id="req_123",
            status=DependencyStatus.RESOLVED,
            resolved_value={"result": "success"},
            resolution_time_ms=1500.0,
            source_topic="mpreg.rpc.test.completed",
            source_message_id="msg_abc",
        )

        assert event.dependency_id == "dep_456"
        assert event.command_id == "cmd_789"
        assert event.request_id == "req_123"
        assert event.status == DependencyStatus.RESOLVED
        assert event.resolved_value == {"result": "success"}
        assert event.resolution_time_ms == 1500.0
        assert event.source_topic == "mpreg.rpc.test.completed"
        assert event.source_message_id == "msg_abc"
        assert isinstance(event.timestamp, float)
        assert event.timestamp > 0

    def test_resolution_event_with_error(self) -> None:
        """Test creating resolution events with error information."""
        event = DependencyResolutionEvent(
            dependency_id="dep_failed",
            command_id="cmd_failed",
            request_id="req_failed",
            status=DependencyStatus.FAILED,
            error_message="Dependency timeout",
        )

        assert event.status == DependencyStatus.FAILED
        assert event.error_message == "Dependency timeout"
        assert event.resolved_value is None


class TestDependencyGraph:
    """Test the dependency graph dataclass."""

    def test_dependency_graph_creation(self) -> None:
        """Test creating dependency graphs."""
        # Create sample dependencies
        dep1 = DependencySpecification(
            dependency_id="dep1",
            dependency_type=DependencyType.RPC_COMMAND,
            topic_pattern="mpreg.rpc.*.command.cmd1.completed",
        )
        dep2 = DependencySpecification(
            dependency_id="dep2",
            dependency_type=DependencyType.QUEUE_MESSAGE,
            topic_pattern="mpreg.queue.test.completed",
        )

        command_dependencies = {"cmd1": [dep1], "cmd2": [dep1, dep2]}

        dependency_edges = {"dep1": {"cmd1", "cmd2"}, "dep2": {"cmd2"}}

        pending_dependencies = {"dep1", "dep2"}

        graph = DependencyGraph(
            request_id="req_test",
            command_dependencies=command_dependencies,
            dependency_edges=dependency_edges,
            pending_dependencies=pending_dependencies,
            total_dependencies=2,
            cross_system_dependencies=1,
        )

        assert graph.request_id == "req_test"
        assert len(graph.command_dependencies) == 2
        assert len(graph.dependency_edges) == 2
        assert len(graph.pending_dependencies) == 2
        assert graph.total_dependencies == 2
        assert graph.cross_system_dependencies == 1
        assert isinstance(graph.created_at, float)

    def test_dependency_graph_progress_calculation(self) -> None:
        """Test dependency resolution progress calculation."""
        # Create graph with no dependencies
        empty_graph = DependencyGraph(
            request_id="req_empty",
            command_dependencies={},
            dependency_edges={},
            total_dependencies=0,
        )
        assert empty_graph.resolution_progress == 100.0
        assert empty_graph.all_dependencies_resolved is True

        # Create graph with dependencies
        graph = DependencyGraph(
            request_id="req_test",
            command_dependencies={},
            dependency_edges={},
            pending_dependencies={"dep1", "dep2", "dep3"},
            total_dependencies=3,
        )

        # No dependencies resolved yet
        assert graph.resolution_progress == 0.0
        assert graph.all_dependencies_resolved is False

        # Resolve one dependency
        resolution_event = DependencyResolutionEvent(
            dependency_id="dep1",
            command_id="cmd1",
            request_id="req_test",
            status=DependencyStatus.RESOLVED,
        )
        graph.resolved_dependencies["dep1"] = resolution_event
        graph.pending_dependencies.remove("dep1")

        assert abs(graph.resolution_progress - 33.33) < 0.1  # Approximately 33.33%
        assert graph.all_dependencies_resolved is False

    def test_get_ready_commands(self) -> None:
        """Test getting commands that are ready to execute."""
        # Create dependencies
        dep1 = DependencySpecification(
            dependency_id="dep1",
            dependency_type=DependencyType.RPC_COMMAND,
            topic_pattern="test.pattern1",
            required=True,
        )
        dep2 = DependencySpecification(
            dependency_id="dep2",
            dependency_type=DependencyType.QUEUE_MESSAGE,
            topic_pattern="test.pattern2",
            required=False,  # Optional dependency
        )

        command_dependencies = {
            "cmd1": [],  # No dependencies - should be ready
            "cmd2": [dep1],  # Has required dependency - not ready until resolved
            "cmd3": [dep2],  # Has optional dependency - should be ready
        }

        graph = DependencyGraph(
            request_id="req_test",
            command_dependencies=command_dependencies,
            dependency_edges={"dep1": {"cmd2"}, "dep2": {"cmd3"}},
            pending_dependencies={"dep1", "dep2"},
            total_dependencies=2,
        )

        # Initially, only cmd1 should be ready (no dependencies)
        ready_commands = graph.get_ready_commands()
        assert "cmd1" in ready_commands
        assert "cmd2" not in ready_commands  # Has unresolved required dependency
        assert "cmd3" in ready_commands  # Optional dependency can be ignored

        # Resolve dep1
        resolution_event = DependencyResolutionEvent(
            dependency_id="dep1",
            command_id="cmd2",
            request_id="req_test",
            status=DependencyStatus.RESOLVED,
        )
        graph.resolved_dependencies["dep1"] = resolution_event
        graph.pending_dependencies.remove("dep1")

        # Now cmd2 should also be ready
        ready_commands = graph.get_ready_commands()
        assert "cmd1" in ready_commands
        assert "cmd2" in ready_commands
        assert "cmd3" in ready_commands


class TestDependencySubscription:
    """Test the dependency subscription dataclass."""

    def test_dependency_subscription_creation(self) -> None:
        """Test creating dependency subscriptions."""
        subscription = DependencySubscription(
            dependency_id="dep_sub_test",
            topic_pattern="mpreg.test.pattern",
            subscription_id="sub_123",
            command_ids={"cmd1", "cmd2"},
            timeout_ms=10000.0,
            resolution_strategy=DependencyResolutionStrategy.BATCH,
        )

        assert subscription.dependency_id == "dep_sub_test"
        assert subscription.topic_pattern == "mpreg.test.pattern"
        assert subscription.subscription_id == "sub_123"
        assert subscription.command_ids == {"cmd1", "cmd2"}
        assert subscription.timeout_ms == 10000.0
        assert subscription.resolution_strategy == DependencyResolutionStrategy.BATCH
        assert isinstance(subscription.created_at, float)
        assert subscription.message_count == 0


class TestTopicDependencyResolver:
    """Test the topic-based dependency resolver."""

    @pytest.fixture
    def sample_commands(self) -> list[TopicAwareRPCCommand]:
        """Create sample commands for testing."""
        cmd1 = TopicAwareRPCCommand(
            name="load_data",
            fun="load_dataset",
            args=("dataset.csv",),
            kwargs={},
            locs=frozenset(["storage"]),
        )

        cmd2 = TopicAwareRPCCommand(
            name="process_data",
            fun="analyze_dataset",
            args=("load_data.result",),  # Depends on cmd1 result
            kwargs={"queue_input": "data_queue.message"},  # Depends on queue message
            locs=frozenset(["cpu"]),
        )

        cmd3 = TopicAwareRPCCommand(
            name="save_results",
            fun="save_to_database",
            args=("process_data.output",),  # Depends on cmd2 output
            kwargs={},
            locs=frozenset(["database"]),
            dependency_topic_patterns=[
                "mpreg.cache.invalidate.completed"
            ],  # Explicit dependency
        )

        return [cmd1, cmd2, cmd3]

    @pytest.fixture
    def resolver(self) -> TopicDependencyResolver:
        """Create a topic dependency resolver for testing."""
        template_engine = TopicTemplateEngine()
        return create_topic_dependency_resolver(
            topic_template_engine=template_engine,
            default_timeout_ms=5000.0,
            max_concurrent_subscriptions=100,
        )

    @pytest.mark.asyncio
    async def test_resolver_creation(self, resolver: TopicDependencyResolver) -> None:
        """Test resolver creation and initialization."""
        assert resolver.topic_template_engine is not None
        assert resolver.default_timeout_ms == 5000.0
        assert resolver.max_concurrent_subscriptions == 100
        assert len(resolver.active_dependency_graphs) == 0
        assert len(resolver.dependency_subscriptions) == 0
        assert resolver.total_dependencies_resolved == 0

    @pytest.mark.asyncio
    async def test_dependency_graph_creation(
        self,
        resolver: TopicDependencyResolver,
        sample_commands: list[TopicAwareRPCCommand],
    ) -> None:
        """Test creating dependency graphs from commands."""
        request_id = "test_request_123"

        dependency_graph = await resolver.create_dependency_graph(
            request_id, sample_commands
        )

        assert dependency_graph.request_id == request_id
        assert len(dependency_graph.command_dependencies) == 3
        assert dependency_graph.total_dependencies > 0
        assert dependency_graph.cross_system_dependencies > 0
        assert request_id in resolver.active_dependency_graphs

        # Verify specific command dependencies were detected
        for command in sample_commands:
            assert command.command_id in dependency_graph.command_dependencies
            dependencies = dependency_graph.command_dependencies[command.command_id]
            assert isinstance(dependencies, list)

    @pytest.mark.asyncio
    async def test_dependency_subscription_setup(
        self,
        resolver: TopicDependencyResolver,
        sample_commands: list[TopicAwareRPCCommand],
    ) -> None:
        """Test setting up topic subscriptions for dependencies."""
        request_id = "test_request_456"

        # Create dependency graph
        dependency_graph = await resolver.create_dependency_graph(
            request_id, sample_commands
        )

        # Set up subscriptions
        subscriptions_created = await resolver.setup_dependency_subscriptions(
            dependency_graph
        )

        assert subscriptions_created > 0
        assert len(resolver.dependency_subscriptions) == subscriptions_created
        assert len(resolver.subscription_callbacks) == subscriptions_created

        # Verify subscription details
        for subscription in resolver.dependency_subscriptions.values():
            assert isinstance(subscription.dependency_id, str)
            assert isinstance(subscription.topic_pattern, str)
            assert isinstance(subscription.subscription_id, str)
            assert len(subscription.command_ids) > 0
            assert subscription.timeout_ms > 0

    @pytest.mark.asyncio
    async def test_dependency_pattern_extraction(
        self, resolver: TopicDependencyResolver
    ) -> None:
        """Test extracting dependency patterns from commands."""
        # Test command with dependency references
        cmd_with_deps = TopicAwareRPCCommand(
            name="dependent_cmd",
            fun="process",
            args=("previous_result.data", "queue_input.message"),
            kwargs={"cache_key": "cache_data.value", "normal_param": "no_dependency"},
            locs=frozenset(["cpu"]),
        )

        patterns = await resolver._extract_dependency_patterns(cmd_with_deps)

        # Should find dependencies in args and kwargs
        assert len(patterns) > 0

        # Verify pattern structure
        for pattern in patterns:
            assert "name" in pattern
            assert "type" in pattern
            assert "topic_pattern" in pattern
            assert isinstance(pattern["type"], DependencyType)

    @pytest.mark.asyncio
    async def test_dependency_type_inference(
        self, resolver: TopicDependencyResolver
    ) -> None:
        """Test dependency type inference from reference strings."""
        # Test different reference types
        assert (
            resolver._infer_dependency_type("queue_input.message")
            == DependencyType.QUEUE_MESSAGE
        )
        assert (
            resolver._infer_dependency_type("topic_data.event")
            == DependencyType.PUBSUB_MESSAGE
        )
        assert (
            resolver._infer_dependency_type("cache_key.value")
            == DependencyType.CACHE_UPDATE
        )
        assert (
            resolver._infer_dependency_type("federation_sync.status")
            == DependencyType.FEDERATION_SYNC
        )
        assert (
            resolver._infer_dependency_type("command_result.output")
            == DependencyType.RPC_COMMAND
        )

    @pytest.mark.asyncio
    async def test_topic_pattern_generation(
        self, resolver: TopicDependencyResolver
    ) -> None:
        """Test topic pattern generation for different dependency types."""
        # Test pattern generation for different reference types
        queue_pattern = resolver._generate_topic_pattern_for_dependency(
            "queue_data.message"
        )
        assert "mpreg.queue" in queue_pattern

        topic_pattern = resolver._generate_topic_pattern_for_dependency(
            "topic_events.data"
        )
        assert "mpreg.pubsub" in topic_pattern

        cache_pattern = resolver._generate_topic_pattern_for_dependency(
            "cache_results.value"
        )
        assert "mpreg.cache" in cache_pattern

        rpc_pattern = resolver._generate_topic_pattern_for_dependency(
            "command_output.result"
        )
        assert "mpreg.rpc" in rpc_pattern

    @pytest.mark.asyncio
    async def test_cleanup_request_dependencies(
        self,
        resolver: TopicDependencyResolver,
        sample_commands: list[TopicAwareRPCCommand],
    ) -> None:
        """Test cleaning up dependencies for completed requests."""
        request_id = "test_cleanup_789"

        # Create dependency graph and subscriptions
        dependency_graph = await resolver.create_dependency_graph(
            request_id, sample_commands
        )
        subscriptions_created = await resolver.setup_dependency_subscriptions(
            dependency_graph
        )

        # Verify setup
        assert request_id in resolver.active_dependency_graphs
        assert len(resolver.dependency_subscriptions) == subscriptions_created
        assert len(resolver.subscription_callbacks) == subscriptions_created

        # Clean up
        cleanup_success = await resolver.cleanup_request_dependencies(request_id)

        assert cleanup_success is True
        assert request_id not in resolver.active_dependency_graphs
        # Note: In this test, subscriptions are cleaned up from the resolver's tracking
        # but not from the actual topic exchange (which would be mocked in a full implementation)

    @pytest.mark.asyncio
    async def test_dependency_statistics(
        self, resolver: TopicDependencyResolver
    ) -> None:
        """Test dependency resolution statistics collection."""
        # Get initial statistics
        stats = await resolver.get_dependency_statistics()

        assert isinstance(stats, dict)
        assert "active_dependency_graphs" in stats
        assert "active_subscriptions" in stats
        assert "total_dependencies_resolved" in stats
        assert "total_cross_system_resolutions" in stats
        assert "average_resolution_time_ms" in stats
        assert "max_concurrent_subscriptions" in stats
        assert "subscription_utilization_percent" in stats

        # Verify initial values
        assert stats["active_dependency_graphs"] == 0
        assert stats["active_subscriptions"] == 0
        assert stats["total_dependencies_resolved"] == 0
        assert stats["max_concurrent_subscriptions"] == 100  # From fixture
        assert stats["subscription_utilization_percent"] == 0.0

    @pytest.mark.asyncio
    async def test_empty_command_list(self, resolver: TopicDependencyResolver) -> None:
        """Test handling empty command lists."""
        request_id = "test_empty"

        dependency_graph = await resolver.create_dependency_graph(request_id, [])

        assert dependency_graph.request_id == request_id
        assert len(dependency_graph.command_dependencies) == 0
        assert dependency_graph.total_dependencies == 0
        assert dependency_graph.cross_system_dependencies == 0
        assert dependency_graph.resolution_progress == 100.0
        assert dependency_graph.all_dependencies_resolved is True

        # Setup subscriptions for empty graph
        subscriptions_created = await resolver.setup_dependency_subscriptions(
            dependency_graph
        )
        assert subscriptions_created == 0


class TestFactoryFunctions:
    """Test the factory functions for dependency resolution components."""

    def test_create_topic_dependency_resolver_with_defaults(self) -> None:
        """Test creating resolver with default parameters."""
        resolver = create_topic_dependency_resolver()

        assert isinstance(resolver, TopicDependencyResolver)
        assert resolver.topic_template_engine is not None
        assert resolver.default_timeout_ms == 30000.0
        assert resolver.max_concurrent_subscriptions == 1000
        assert resolver.enable_dependency_analytics is True

    def test_create_topic_dependency_resolver_with_custom_params(self) -> None:
        """Test creating resolver with custom parameters."""
        template_engine = TopicTemplateEngine()

        resolver = create_topic_dependency_resolver(
            topic_template_engine=template_engine,
            default_timeout_ms=10000.0,
            max_concurrent_subscriptions=500,
        )

        assert resolver.topic_template_engine is template_engine
        assert resolver.default_timeout_ms == 10000.0
        assert resolver.max_concurrent_subscriptions == 500


# Property-based testing with Hypothesis
@given(
    dependency_count=st.integers(min_value=0, max_value=10),
    command_count=st.integers(min_value=1, max_value=5),
    timeout_ms=st.floats(min_value=1000.0, max_value=60000.0),
)
@settings(max_examples=30, deadline=3000)
def test_dependency_graph_properties(
    dependency_count: int, command_count: int, timeout_ms: float
) -> None:
    """Property test: Dependency graph invariants."""
    # Create mock command dependencies
    command_dependencies: dict[str, list[DependencySpecification]] = {}
    dependency_edges: dict[str, set[str]] = {}
    pending_dependencies: set[str] = set()

    for i in range(command_count):
        command_id = f"cmd_{i}"
        command_deps = []

        for j in range(min(dependency_count, 3)):  # Limit dependencies per command
            dep_id = f"dep_{i}_{j}"
            dep_spec = DependencySpecification(
                dependency_id=dep_id,
                dependency_type=DependencyType.RPC_COMMAND,
                topic_pattern=f"test.pattern.{dep_id}",
                timeout_ms=timeout_ms,
            )
            command_deps.append(dep_spec)
            pending_dependencies.add(dep_id)

            if dep_id not in dependency_edges:
                dependency_edges[dep_id] = set()
            dependency_edges[dep_id].add(command_id)

        command_dependencies[command_id] = command_deps

    total_deps = len(pending_dependencies)

    graph = DependencyGraph(
        request_id="test_property",
        command_dependencies=command_dependencies,
        dependency_edges=dependency_edges,
        pending_dependencies=pending_dependencies,
        total_dependencies=total_deps,
    )

    # Test invariants
    assert graph.total_dependencies >= 0
    assert len(graph.pending_dependencies) <= graph.total_dependencies
    assert len(graph.resolved_dependencies) <= graph.total_dependencies
    assert 0.0 <= graph.resolution_progress <= 100.0

    # If no dependencies, should be 100% resolved
    if graph.total_dependencies == 0:
        assert graph.resolution_progress == 100.0
        assert graph.all_dependencies_resolved is True

    # If all dependencies are pending, progress should be 0%
    if len(graph.pending_dependencies) == graph.total_dependencies > 0:
        assert graph.resolution_progress == 0.0
        assert graph.all_dependencies_resolved is False


@given(
    num_dependencies=st.integers(min_value=1, max_value=5),
    resolution_strategy=st.sampled_from(list(DependencyResolutionStrategy)),
    required=st.booleans(),
)
@settings(max_examples=20, deadline=2000)
def test_dependency_specification_properties(
    num_dependencies: int,
    resolution_strategy: DependencyResolutionStrategy,
    required: bool,
) -> None:
    """Property test: Dependency specification creation and validation."""
    dependencies = []

    for i in range(num_dependencies):
        dep_spec = DependencySpecification(
            dependency_id=f"prop_dep_{i}",
            dependency_type=DependencyType.RPC_COMMAND,
            topic_pattern=f"mpreg.test.{i}.pattern",
            resolution_strategy=resolution_strategy,
            required=required,
            timeout_ms=float(1000 * (i + 1)),  # Different timeouts
        )
        dependencies.append(dep_spec)

    # Test properties
    for dep_spec in dependencies:
        assert len(dep_spec.dependency_id) > 0
        assert dep_spec.dependency_type in DependencyType
        assert len(dep_spec.topic_pattern) > 0
        assert dep_spec.resolution_strategy == resolution_strategy
        assert dep_spec.required == required
        assert dep_spec.timeout_ms > 0

        # Field path can be None or non-empty string
        if dep_spec.field_path is not None:
            assert len(dep_spec.field_path) > 0
