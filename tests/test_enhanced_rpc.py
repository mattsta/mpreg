"""
Tests for enhanced RPC system with topic-aware capabilities.

This module tests the Phase 2.1.1 and 2.1.2 implementations:
- Topic-aware RPC command datastructures
- RPC-Topic Integration Engine with progress publishing
"""

import time

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
    create_topic_aware_command,
    create_topic_aware_request,
    create_topic_aware_rpc_executor,
)
from mpreg.core.model import RPCCommand, RPCRequest
from mpreg.core.unified_routing import RoutingPriority


class TestTopicAwareRPCDatastructures:
    """Test the topic-aware RPC datastructure conversions and functionality."""

    def test_topic_aware_command_creation(self) -> None:
        """Test creating topic-aware commands from legacy commands."""
        # Create legacy command
        legacy_cmd = RPCCommand(
            name="test_command",
            fun="echo",
            args=("hello", "world"),
            kwargs={"timeout": 30},
            locs=frozenset(["cpu", "memory"]),
        )

        # Convert to topic-aware command
        topic_cmd = TopicAwareRPCCommand.from_rpc_command(
            legacy_cmd,
            publish_progress=True,
            enable_result_streaming=True,
            estimated_duration_ms=2000.0,
        )

        # Verify conversion
        assert topic_cmd.name == legacy_cmd.name
        assert topic_cmd.fun == legacy_cmd.fun
        assert topic_cmd.args == legacy_cmd.args
        assert topic_cmd.kwargs == legacy_cmd.kwargs
        assert topic_cmd.locs == legacy_cmd.locs
        assert topic_cmd.publish_progress is True
        assert topic_cmd.enable_result_streaming is True
        assert topic_cmd.estimated_duration_ms == 2000.0
        assert isinstance(topic_cmd.command_id, str)
        assert len(topic_cmd.command_id) > 0

    def test_topic_aware_command_to_legacy_conversion(self) -> None:
        """Test converting topic-aware commands back to legacy format."""
        # Create topic-aware command
        topic_cmd = TopicAwareRPCCommand(
            name="test_command",
            fun="process_data",
            args=("input",),
            kwargs={"batch_size": 100},
            locs=frozenset(["gpu"]),
            publish_progress=True,
            estimated_duration_ms=5000.0,
        )

        # Convert to legacy
        legacy_cmd = topic_cmd.to_rpc_command()

        # Verify conversion preserves core fields
        assert legacy_cmd.name == topic_cmd.name
        assert legacy_cmd.fun == topic_cmd.fun
        assert legacy_cmd.args == topic_cmd.args
        assert legacy_cmd.kwargs == topic_cmd.kwargs
        assert legacy_cmd.locs == topic_cmd.locs

    def test_topic_aware_request_creation(self) -> None:
        """Test creating topic-aware requests from legacy requests."""
        # Create legacy commands
        cmd1 = RPCCommand(
            name="step1",
            fun="load_data",
            args=("file.txt",),
            locs=frozenset(["storage"]),
        )
        cmd2 = RPCCommand(name="step2", fun="process", args=(), locs=frozenset(["cpu"]))

        # Create legacy request
        legacy_req = RPCRequest(role="rpc", cmds=(cmd1, cmd2), u="test_request_123")

        # Convert to topic-aware request
        topic_req = TopicAwareRPCRequest.from_rpc_request(
            legacy_req,
            enable_topic_monitoring=True,
            enable_result_streaming=True,
            priority=RoutingPriority.HIGH,
        )

        # Verify conversion
        assert topic_req.role == legacy_req.role
        assert len(topic_req.cmds) == len(legacy_req.cmds)
        assert topic_req.u == legacy_req.u
        assert topic_req.enable_topic_monitoring is True
        assert topic_req.enable_result_streaming is True
        assert topic_req.priority == RoutingPriority.HIGH
        assert isinstance(topic_req.request_id, str)

        # Verify commands were converted properly
        for i, topic_cmd in enumerate(topic_req.cmds):
            original_cmd = legacy_req.cmds[i]
            assert topic_cmd.name == original_cmd.name
            assert topic_cmd.fun == original_cmd.fun
            assert topic_cmd.args == original_cmd.args

    def test_topic_aware_request_to_legacy_conversion(self) -> None:
        """Test converting topic-aware requests back to legacy format."""
        # Create topic-aware commands
        topic_cmd1 = TopicAwareRPCCommand(
            name="cmd1", fun="func1", args=("arg1",), locs=frozenset(["cpu"])
        )
        topic_cmd2 = TopicAwareRPCCommand(
            name="cmd2", fun="func2", args=("arg2",), locs=frozenset(["gpu"])
        )

        # Create topic-aware request
        topic_req = TopicAwareRPCRequest(
            role="rpc",
            cmds=(topic_cmd1, topic_cmd2),
            u="test_request_456",
            enable_topic_monitoring=True,
        )

        # Convert to legacy
        legacy_req = topic_req.to_rpc_request()

        # Verify conversion
        assert legacy_req.role == topic_req.role
        assert len(legacy_req.cmds) == len(topic_req.cmds)
        assert legacy_req.u == topic_req.u

        # Verify commands were converted properly
        for i, legacy_cmd in enumerate(legacy_req.cmds):
            topic_cmd = topic_req.cmds[i]
            assert legacy_cmd.name == topic_cmd.name
            assert legacy_cmd.fun == topic_cmd.fun
            assert legacy_cmd.args == topic_cmd.args


class TestTopicAwareRPCConfig:
    """Test the configuration dataclass for topic-aware RPC."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = TopicAwareRPCConfig()

        assert config.enable_progress_publishing is True
        assert config.enable_result_streaming is True
        assert config.enable_dependency_topics is True
        assert config.enable_error_topics is True
        assert config.progress_update_interval_ms == 1000.0
        assert config.heartbeat_interval_ms == 5000.0
        assert config.topic_prefix == "mpreg.rpc"
        assert config.use_correlation_ids is True
        assert config.fallback_to_legacy is True

    def test_custom_config(self) -> None:
        """Test creating configuration with custom values."""
        config = TopicAwareRPCConfig(
            enable_progress_publishing=False,
            progress_update_interval_ms=500.0,
            topic_prefix="custom.rpc",
            max_intermediate_results=200,
        )

        assert config.enable_progress_publishing is False
        assert config.progress_update_interval_ms == 500.0
        assert config.topic_prefix == "custom.rpc"
        assert config.max_intermediate_results == 200
        # Verify defaults are preserved for unspecified fields
        assert config.enable_result_streaming is True
        assert config.heartbeat_interval_ms == 5000.0


class TestRPCResult:
    """Test the RPCResult dataclass."""

    def test_rpc_result_creation(self) -> None:
        """Test creating RPCResult instances."""
        result = RPCResult(
            command_name="test_cmd",
            function="test_func",
            args=("arg1", "arg2"),
            kwargs={"key": "value"},
            executed_at=time.time(),
            execution_level=0,
            command_id="cmd_123",
            execution_time_ms=1500.0,
            success=True,
        )

        assert result.command_name == "test_cmd"
        assert result.function == "test_func"
        assert result.args == ("arg1", "arg2")
        assert result.kwargs == {"key": "value"}
        assert result.execution_level == 0
        assert result.command_id == "cmd_123"
        assert result.execution_time_ms == 1500.0
        assert result.success is True
        assert result.error_message is None
        assert result.error_type is None

    def test_rpc_result_with_error(self) -> None:
        """Test creating RPCResult with error information."""
        result = RPCResult(
            command_name="failed_cmd",
            function="failing_func",
            args=(),
            kwargs={},
            executed_at=time.time(),
            execution_level=1,
            command_id="cmd_456",
            execution_time_ms=500.0,
            success=False,
            error_message="Command failed due to timeout",
            error_type="TimeoutError",
        )

        assert result.success is False
        assert result.error_message == "Command failed due to timeout"
        assert result.error_type == "TimeoutError"


class TestRPCRequestResult:
    """Test the RPCRequestResult dataclass."""

    def test_rpc_request_result_creation(self) -> None:
        """Test creating RPCRequestResult instances."""
        # Create mock command results
        cmd_result1 = RPCResult(
            command_name="cmd1",
            function="func1",
            args=(),
            kwargs={},
            executed_at=time.time(),
            execution_level=0,
            command_id="cmd_1",
            execution_time_ms=1000.0,
            success=True,
        )
        cmd_result2 = RPCResult(
            command_name="cmd2",
            function="func2",
            args=(),
            kwargs={},
            executed_at=time.time(),
            execution_level=0,
            command_id="cmd_2",
            execution_time_ms=1500.0,
            success=True,
        )

        command_results = {"cmd_1": cmd_result1, "cmd_2": cmd_result2}

        request_result = RPCRequestResult(
            request_id="req_123",
            command_results=command_results,
            total_execution_time_ms=2500.0,
            total_commands=2,
            successful_commands=2,
            failed_commands=0,
            execution_levels_completed=1,
        )

        assert request_result.request_id == "req_123"
        assert len(request_result.command_results) == 2
        assert request_result.total_execution_time_ms == 2500.0
        assert request_result.total_commands == 2
        assert request_result.successful_commands == 2
        assert request_result.failed_commands == 0
        assert request_result.execution_levels_completed == 1

    def test_success_rate_calculation(self) -> None:
        """Test success rate calculation."""
        # All successful
        result1 = RPCRequestResult(
            request_id="req_1",
            command_results={},
            total_execution_time_ms=1000.0,
            total_commands=3,
            successful_commands=3,
            failed_commands=0,
            execution_levels_completed=1,
        )
        assert result1.success_rate == 100.0
        assert result1.all_successful is True

        # Partial success
        result2 = RPCRequestResult(
            request_id="req_2",
            command_results={},
            total_execution_time_ms=1000.0,
            total_commands=4,
            successful_commands=3,
            failed_commands=1,
            execution_levels_completed=1,
        )
        assert result2.success_rate == 75.0
        assert result2.all_successful is False

        # No commands (edge case)
        result3 = RPCRequestResult(
            request_id="req_3",
            command_results={},
            total_execution_time_ms=0.0,
            total_commands=0,
            successful_commands=0,
            failed_commands=0,
            execution_levels_completed=0,
        )
        assert result3.success_rate == 100.0
        assert result3.all_successful is True


class TestTopicAwareRPCExecutor:
    """Test the topic-aware RPC executor."""

    @pytest.fixture
    def sample_commands(self) -> list[TopicAwareRPCCommand]:
        """Create sample topic-aware commands for testing."""
        cmd1 = TopicAwareRPCCommand(
            name="load_data",
            fun="load_dataset",
            args=("dataset.csv",),
            kwargs={"format": "csv"},
            locs=frozenset(["storage"]),
            estimated_duration_ms=1000.0,
        )

        cmd2 = TopicAwareRPCCommand(
            name="process_data",
            fun="analyze_dataset",
            args=(),
            kwargs={"algorithm": "ml"},
            locs=frozenset(["cpu", "gpu"]),
            estimated_duration_ms=2000.0,
        )

        return [cmd1, cmd2]

    @pytest.fixture
    def executor(self) -> TopicAwareRPCExecutor:
        """Create a topic-aware RPC executor for testing."""
        config = TopicAwareRPCConfig(
            enable_progress_publishing=True,
            progress_update_interval_ms=100.0,  # Faster for testing
            topic_prefix="test.rpc",
        )

        return create_topic_aware_rpc_executor(
            topic_exchange=None,  # Mock for testing
            config=config,
        )

    @pytest.mark.asyncio
    async def test_executor_creation(self, executor: TopicAwareRPCExecutor) -> None:
        """Test executor creation and initialization."""
        assert executor.config.topic_prefix == "test.rpc"
        assert executor.config.progress_update_interval_ms == 100.0
        assert executor.subscription_manager is not None
        assert len(executor.active_commands) == 0
        assert executor.total_commands_executed == 0

    @pytest.mark.asyncio
    async def test_command_execution_with_topics(
        self,
        executor: TopicAwareRPCExecutor,
        sample_commands: list[TopicAwareRPCCommand],
    ) -> None:
        """Test executing commands with topic monitoring."""
        start_time = time.time()

        result = await executor.execute_with_topics(sample_commands)

        execution_time = time.time() - start_time

        # Verify result structure
        assert isinstance(result, RPCRequestResult)
        assert result.total_commands == 2
        assert result.successful_commands == 2
        assert result.failed_commands == 0
        assert result.execution_levels_completed == 1
        assert result.all_successful is True
        assert result.success_rate == 100.0

        # Verify command results
        assert len(result.command_results) == 2
        for cmd in sample_commands:
            assert cmd.command_id in result.command_results
            cmd_result = result.command_results[cmd.command_id]
            assert cmd_result.command_name == cmd.name
            assert cmd_result.function == cmd.fun
            assert cmd_result.success is True
            assert cmd_result.execution_time_ms > 0

        # Verify execution completed in reasonable time (allowing for simulation)
        assert execution_time >= 2.0  # Should take at least 2 seconds due to simulation
        assert execution_time < 10.0  # But not too long

        # Verify cleanup occurred (no active commands after completion)
        assert len(executor.active_commands) == 0

    @pytest.mark.asyncio
    async def test_execution_statistics(
        self,
        executor: TopicAwareRPCExecutor,
        sample_commands: list[TopicAwareRPCCommand],
    ) -> None:
        """Test execution statistics collection."""
        # Execute commands
        await executor.execute_with_topics(sample_commands)

        # Get statistics
        stats = await executor.get_execution_statistics()

        assert isinstance(stats, dict)
        assert "total_commands_executed" in stats
        assert "active_commands" in stats
        assert "total_progress_events_published" in stats
        assert "average_execution_time_ms" in stats
        assert "config" in stats

        # Verify statistics values
        assert stats["active_commands"] == 0  # Should be cleaned up
        assert (
            stats["total_progress_events_published"] > 0
        )  # Should have published progress
        assert isinstance(stats["config"], dict)
        assert stats["config"]["enable_progress_publishing"] is True

    @pytest.mark.asyncio
    async def test_empty_command_list(self, executor: TopicAwareRPCExecutor) -> None:
        """Test executing with empty command list."""
        result = await executor.execute_with_topics([])

        assert isinstance(result, RPCRequestResult)
        assert result.total_commands == 0
        assert result.successful_commands == 0
        assert result.failed_commands == 0
        assert result.execution_levels_completed == 0
        assert result.all_successful is True
        assert result.success_rate == 100.0
        assert len(result.command_results) == 0

    @pytest.mark.asyncio
    async def test_custom_request_id(
        self,
        executor: TopicAwareRPCExecutor,
        sample_commands: list[TopicAwareRPCCommand],
    ) -> None:
        """Test executing with custom request ID."""
        custom_request_id = "custom_req_12345"

        result = await executor.execute_with_topics(
            sample_commands, request_id=custom_request_id
        )

        assert result.request_id == custom_request_id


class TestFactoryFunctions:
    """Test the factory functions for creating topic-aware RPC components."""

    def test_create_topic_aware_command_factory(self) -> None:
        """Test the create_topic_aware_command factory function."""
        legacy_cmd = RPCCommand(
            name="test", fun="echo", args=("hello",), kwargs={}, locs=frozenset(["cpu"])
        )

        topic_cmd = create_topic_aware_command(
            legacy_cmd, enable_progress=True, enable_streaming=False
        )

        assert isinstance(topic_cmd, TopicAwareRPCCommand)
        assert topic_cmd.name == legacy_cmd.name
        assert topic_cmd.publish_progress is True
        assert topic_cmd.enable_result_streaming is False

    def test_create_topic_aware_request_factory(self) -> None:
        """Test the create_topic_aware_request factory function."""
        cmd = RPCCommand(name="test", fun="echo", args=(), kwargs={}, locs=frozenset())
        legacy_req = RPCRequest(role="rpc", cmds=(cmd,), u="test_u")

        topic_req = create_topic_aware_request(
            legacy_req, enable_monitoring=True, priority=RoutingPriority.HIGH
        )

        assert isinstance(topic_req, TopicAwareRPCRequest)
        assert topic_req.enable_topic_monitoring is True
        assert topic_req.priority == RoutingPriority.HIGH
        assert len(topic_req.cmds) == 1

    def test_create_topic_aware_rpc_executor_factory(self) -> None:
        """Test the create_topic_aware_rpc_executor factory function."""
        config = TopicAwareRPCConfig(topic_prefix="test.factory")

        executor = create_topic_aware_rpc_executor(topic_exchange=None, config=config)

        assert isinstance(executor, TopicAwareRPCExecutor)
        assert executor.config.topic_prefix == "test.factory"
        assert executor.subscription_manager is not None


# Property-based testing with Hypothesis
@given(
    command_name=st.text(min_size=1, max_size=50).filter(lambda x: x.strip()),
    function_name=st.text(min_size=1, max_size=50).filter(lambda x: x.strip()),
    num_args=st.integers(min_value=0, max_value=5),
    enable_progress=st.booleans(),
    enable_streaming=st.booleans(),
)
@settings(max_examples=50, deadline=2000)
def test_topic_aware_command_conversion_property(
    command_name: str,
    function_name: str,
    num_args: int,
    enable_progress: bool,
    enable_streaming: bool,
) -> None:
    """Property test: Topic-aware command conversion preserves core data."""
    # Generate args tuple
    args = tuple(f"arg_{i}" for i in range(num_args))

    # Create legacy command
    legacy_cmd = RPCCommand(
        name=command_name.strip(),
        fun=function_name.strip(),
        args=args,
        kwargs={"test": "value"},
        locs=frozenset(["cpu"]),
    )

    # Convert to topic-aware and back
    topic_cmd = TopicAwareRPCCommand.from_rpc_command(
        legacy_cmd,
        publish_progress=enable_progress,
        enable_result_streaming=enable_streaming,
    )

    converted_back = topic_cmd.to_rpc_command()

    # Verify round-trip conversion preserves data
    assert converted_back.name == legacy_cmd.name
    assert converted_back.fun == legacy_cmd.fun
    assert converted_back.args == legacy_cmd.args
    assert converted_back.kwargs == legacy_cmd.kwargs
    assert converted_back.locs == legacy_cmd.locs

    # Verify topic-specific features
    assert topic_cmd.publish_progress == enable_progress
    assert topic_cmd.enable_result_streaming == enable_streaming
    assert isinstance(topic_cmd.command_id, str)
    assert len(topic_cmd.command_id) > 0


@given(
    num_commands=st.integers(min_value=1, max_value=5),
    enable_monitoring=st.booleans(),
    enable_streaming=st.booleans(),
)
@settings(max_examples=30, deadline=3000)
def test_topic_aware_request_conversion_property(
    num_commands: int, enable_monitoring: bool, enable_streaming: bool
) -> None:
    """Property test: Topic-aware request conversion preserves command data."""
    # Create legacy commands
    commands = [
        RPCCommand(
            name=f"cmd_{i}",
            fun=f"func_{i}",
            args=(f"arg_{i}",),
            kwargs={f"key_{i}": f"value_{i}"},
            locs=frozenset([f"loc_{i}"]),
        )
        for i in range(num_commands)
    ]

    # Create legacy request
    legacy_req = RPCRequest(
        role="rpc", cmds=tuple(commands), u=f"test_request_{num_commands}"
    )

    # Convert to topic-aware and back
    topic_req = TopicAwareRPCRequest.from_rpc_request(
        legacy_req,
        enable_topic_monitoring=enable_monitoring,
        enable_result_streaming=enable_streaming,
    )

    converted_back = topic_req.to_rpc_request()

    # Verify round-trip conversion preserves data
    assert converted_back.role == legacy_req.role
    assert len(converted_back.cmds) == len(legacy_req.cmds)
    assert converted_back.u == legacy_req.u

    # Verify each command was preserved
    for i, (original, converted) in enumerate(
        zip(legacy_req.cmds, converted_back.cmds)
    ):
        assert original.name == converted.name
        assert original.fun == converted.fun
        assert original.args == converted.args
        assert original.kwargs == converted.kwargs
        assert original.locs == converted.locs

    # Verify topic-specific features
    assert topic_req.enable_topic_monitoring == enable_monitoring
    assert topic_req.enable_result_streaming == enable_streaming
    assert isinstance(topic_req.request_id, str)
    assert len(topic_req.request_id) > 0
