"""
Enhanced RPC system with topic-aware capabilities for MPREG.

This module extends MPREG's existing RPC dependency resolution and execution
system with topic-based progress monitoring, real-time result streaming, and
cross-system coordination.

Key enhancements:
- Topic-based RPC progress monitoring and lifecycle events
- Real-time result streaming via topic subscriptions
- Topic-driven dependency coordination with pub/sub integration
- Enhanced observability and debugging capabilities
- Cross-system correlation (RPC → Queue → PubSub coordination)

Design principles:
- Base RPC envelopes remain unchanged
- Progressive enhancement: Topic features are opt-in
- Well-encapsulated: Proper dataclasses with type safety
- Self-managing: Clean interfaces with automatic resource management
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from mpreg.core.model import RPCCommand, RPCRequest, RPCResponse
from mpreg.core.topic_taxonomy import TopicTemplateEngine
from mpreg.datastructures.type_aliases import CorrelationId
from mpreg.fabric.message import RoutingPriority

# Enhanced RPC type aliases following MPREG conventions
type RPCRequestId = str
type RPCCommandId = str
type RPCExecutionLevel = int
type RPCProgressPercentage = float
type TopicSubscriptionId = str
type RPCStreamToken = str


class RPCExecutionStage(Enum):
    """RPC execution lifecycle stages for topic coordination."""

    RECEIVED = "received"  # Request received and validated
    DEPENDENCY_ANALYSIS = "dependency_analysis"  # Building dependency graph
    EXECUTION_PLANNING = "execution_planning"  # Creating execution levels
    LEVEL_STARTED = "level_started"  # Execution level started
    COMMAND_STARTED = "command_started"  # Individual command started
    COMMAND_PROGRESS = "command_progress"  # Command progress update
    COMMAND_COMPLETED = "command_completed"  # Individual command completed
    COMMAND_FAILED = "command_failed"  # Individual command failed
    LEVEL_COMPLETED = "level_completed"  # Execution level completed
    REQUEST_COMPLETED = "request_completed"  # Entire request completed
    REQUEST_FAILED = "request_failed"  # Request failed


class RPCProgressType(Enum):
    """Types of RPC progress updates."""

    STAGE_TRANSITION = "stage_transition"  # Moving between execution stages
    EXECUTION_PROGRESS = "execution_progress"  # Numeric progress (0-100%)
    INTERMEDIATE_RESULT = "intermediate_result"  # Partial results available
    RESOURCE_ALLOCATION = "resource_allocation"  # Resource usage information
    DEPENDENCY_RESOLVED = "dependency_resolved"  # Dependency became available
    ERROR_RECOVERABLE = "error_recoverable"  # Recoverable error occurred


@dataclass(frozen=True, slots=True)
class TopicAwareRPCConfig:
    """Configuration for topic-aware RPC enhancements."""

    # Topic publishing settings
    enable_progress_publishing: bool = True
    enable_result_streaming: bool = True
    enable_dependency_topics: bool = True
    enable_error_topics: bool = True

    # Progress update frequency
    progress_update_interval_ms: float = 1000.0  # Update every second
    heartbeat_interval_ms: float = 5000.0  # Heartbeat every 5 seconds

    # Topic configuration
    topic_prefix: str = "mpreg.rpc"
    use_correlation_ids: bool = True

    # Performance settings
    max_intermediate_results: int = 100
    result_stream_buffer_size: int = 1000
    subscription_timeout_ms: float = 30000.0  # 30 second subscription timeout


@dataclass(frozen=True, slots=True)
class RPCProgressEvent:
    """Real-time RPC execution progress event."""

    request_id: RPCRequestId
    command_id: RPCCommandId | None = None
    execution_level: RPCExecutionLevel | None = None

    # Progress information
    stage: RPCExecutionStage = RPCExecutionStage.RECEIVED
    progress_type: RPCProgressType = RPCProgressType.STAGE_TRANSITION
    progress_percentage: RPCProgressPercentage = 0.0

    # Detailed information
    current_step: str = ""
    estimated_completion_ms: float | None = None
    resource_usage: dict[str, Any] = field(default_factory=dict)

    # Results and errors
    intermediate_result: Any = None
    error_message: str | None = None
    recoverable_error: bool = True

    # Metadata
    timestamp: float = field(default_factory=time.time)
    correlation_id: CorrelationId | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RPCDependencyEvent:
    """Event indicating RPC dependency resolution."""

    request_id: RPCRequestId
    dependent_command_id: RPCCommandId
    dependency_command_id: RPCCommandId

    # Dependency information
    field_path: str | None = None  # e.g., "step1_result.field"
    resolved_value: Any = None
    resolution_timestamp: float = field(default_factory=time.time)

    # Coordination
    triggers_execution: bool = False  # Whether this resolves the last dependency
    execution_level: RPCExecutionLevel | None = None
    correlation_id: CorrelationId | None = None


@dataclass(frozen=True, slots=True)
class RPCResult:
    """Result of a single RPC command execution."""

    command_name: str
    function: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    executed_at: float
    execution_level: int
    command_id: RPCCommandId
    execution_time_ms: float

    # Optional success/error information
    success: bool = True
    error_message: str | None = None
    error_type: str | None = None


@dataclass(frozen=True, slots=True)
class RPCRequestResult:
    """Result of a complete RPC request execution."""

    request_id: RPCRequestId
    command_results: dict[RPCCommandId, RPCResult]
    total_execution_time_ms: float
    total_commands: int
    successful_commands: int
    failed_commands: int
    execution_levels_completed: int

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_commands == 0:
            return 100.0
        return (self.successful_commands / self.total_commands) * 100.0

    @property
    def all_successful(self) -> bool:
        """Check if all commands completed successfully."""
        return self.failed_commands == 0


@dataclass(frozen=True, slots=True)
class RPCResultStream:
    """Streaming RPC result for real-time delivery."""

    request_id: RPCRequestId
    command_id: RPCCommandId

    # Result information
    result_type: str  # "partial", "intermediate", "final"
    result_data: Any
    sequence_number: int  # For ordering

    # Metadata
    timestamp: float = field(default_factory=time.time)
    estimated_total_results: int | None = None
    correlation_id: CorrelationId | None = None

    @property
    def is_final_result(self) -> bool:
        """Check if this is the final result in the stream."""
        return self.result_type == "final"


@dataclass(frozen=True, slots=True)
class TopicAwareRPCCommand:
    """Enhanced RPC command with topic-aware capabilities."""

    # Core RPC command fields (pure dataclass, no Pydantic)
    name: str
    fun: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    locs: frozenset[str] = field(default_factory=frozenset)

    # Topic publishing configuration
    publish_progress: bool = True
    progress_topic_pattern: str = (
        "mpreg.rpc.request.{request_id}.command.{command_id}.progress"
    )
    result_topic_pattern: str = (
        "mpreg.rpc.request.{request_id}.command.{command_id}.result"
    )

    # Dependency topic subscriptions
    subscribe_to_dependencies: bool = True
    dependency_topic_patterns: list[str] = field(default_factory=list)

    # Streaming configuration
    enable_result_streaming: bool = False
    stream_intermediate_results: bool = False
    max_stream_buffer_size: int = 100

    # Enhanced metadata
    command_id: RPCCommandId = field(default_factory=lambda: str(uuid.uuid4()))
    estimated_duration_ms: float | None = None
    resource_requirements: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_rpc_command(
        cls, command: RPCCommand, **kwargs: Any
    ) -> TopicAwareRPCCommand:
        """Create topic-aware command from base RPCCommand."""
        return cls(
            name=command.name,
            fun=command.fun,
            args=command.args,
            kwargs=command.kwargs,
            locs=command.locs,
            **kwargs,
        )

    def to_rpc_command(self) -> RPCCommand:
        """Convert back to RPCCommand."""
        return RPCCommand(
            name=self.name,
            fun=self.fun,
            args=self.args,
            kwargs=self.kwargs,
            locs=self.locs,
        )


@dataclass(frozen=True, slots=True)
class TopicAwareRPCRequest:
    """Enhanced RPC request with topic-aware capabilities."""

    # Core RPC request fields (pure dataclass, no Pydantic)
    role: str = "rpc"
    cmds: tuple[TopicAwareRPCCommand, ...] = field(default_factory=tuple)
    u: str = field(default_factory=lambda: f"rpc_req_{uuid.uuid4().hex[:16]}")

    # Topic configuration
    enable_topic_monitoring: bool = True
    progress_subscription_patterns: list[str] = field(default_factory=list)
    result_subscription_patterns: list[str] = field(default_factory=list)

    # Request metadata
    request_id: RPCRequestId = field(
        default_factory=lambda: f"rpc_req_{uuid.uuid4().hex[:16]}"
    )
    correlation_id: CorrelationId | None = None
    client_subscription_token: TopicSubscriptionId | None = None

    # Enhanced configuration
    topic_config: TopicAwareRPCConfig = field(default_factory=TopicAwareRPCConfig)
    priority: RoutingPriority = RoutingPriority.NORMAL

    # Streaming and monitoring
    enable_result_streaming: bool = False
    stream_token: RPCStreamToken | None = None
    monitor_dependencies: bool = True

    @classmethod
    def from_rpc_request(
        cls, request: RPCRequest, **kwargs: Any
    ) -> TopicAwareRPCRequest:
        """Create topic-aware request from base RPCRequest."""
        # Convert commands to topic-aware commands
        topic_aware_commands = tuple(
            TopicAwareRPCCommand.from_rpc_command(cmd) for cmd in request.cmds
        )

        return cls(role=request.role, cmds=topic_aware_commands, u=request.u, **kwargs)

    def to_rpc_request(self) -> RPCRequest:
        """Convert back to RPCRequest."""
        base_commands = tuple(cmd.to_rpc_command() for cmd in self.cmds)
        return RPCRequest(
            role="rpc",  # Fixed: Use literal "rpc" value
            cmds=base_commands,
            u=self.u,
        )


@dataclass(frozen=True, slots=True)
class TopicAwareRPCResponse:
    """Enhanced RPC response with topic monitoring information."""

    # Core RPC response fields (pure dataclass, no Pydantic)
    r: Any = None  # Result of the RPC call, if successful
    error: dict[str, Any] | None = None  # Error information, if the RPC failed

    # Topic monitoring metadata
    request_id: RPCRequestId | None = None
    correlation_id: CorrelationId | None = None

    # Execution summary
    total_execution_time_ms: float | None = None
    execution_levels_completed: int = 0
    progress_events_published: int = 0

    # Topic subscription information
    progress_topics_used: list[str] = field(default_factory=list)
    result_topics_used: list[str] = field(default_factory=list)
    subscription_cleanup_status: str = "completed"  # "completed", "partial", "failed"

    # Streaming information
    total_streamed_results: int = 0
    stream_completion_status: str = "completed"  # "completed", "partial", "timeout"

    @property
    def has_topic_monitoring_data(self) -> bool:
        """Check if response includes topic monitoring information."""
        return self.request_id is not None and len(self.progress_topics_used) > 0

    @classmethod
    def from_rpc_response(
        cls, response: RPCResponse, **kwargs: Any
    ) -> TopicAwareRPCResponse:
        """Create topic-aware response from base RPCResponse."""
        error_dict = None
        if response.error:
            # Convert RPCError to dict if needed
            error_dict = (
                response.error
                if isinstance(response.error, dict)
                else {"message": str(response.error)}
            )

        return cls(r=response.r, error=error_dict, **kwargs)

    def to_rpc_response(self) -> RPCResponse:
        """Convert back to RPCResponse."""
        # Convert error dict back to RPCError if needed
        error_obj = None
        if self.error:
            from mpreg.core.model import RPCError

            if isinstance(self.error, dict):
                error_obj = RPCError(
                    code=self.error.get("code", -1),
                    message=self.error.get("message", "Unknown error"),
                    details=self.error.get("details"),
                )
            else:
                error_obj = self.error

        return RPCResponse(
            r=self.r,
            error=error_obj,
            u=self.request_id or "unknown",  # Fixed: Add required u field
        )


@dataclass(slots=True)
class RPCTopicSubscriptionManager:
    """Manager for RPC-related topic subscriptions."""

    topic_template_engine: TopicTemplateEngine
    active_subscriptions: dict[TopicSubscriptionId, dict[str, Any]] = field(
        default_factory=dict
    )
    request_subscriptions: dict[RPCRequestId, set[TopicSubscriptionId]] = field(
        default_factory=dict
    )
    subscription_callbacks: dict[TopicSubscriptionId, Callable[[Any], None]] = field(
        default_factory=dict
    )

    def generate_progress_topic(
        self, request_id: RPCRequestId, command_id: RPCCommandId | None = None
    ) -> str:
        """Generate topic pattern for RPC progress monitoring."""
        if command_id:
            return f"mpreg.rpc.request.{request_id}.command.{command_id}.progress"
        else:
            return f"mpreg.rpc.request.{request_id}.progress"

    def generate_result_topic(
        self, request_id: RPCRequestId, command_id: RPCCommandId
    ) -> str:
        """Generate topic pattern for RPC result streaming."""
        return f"mpreg.rpc.request.{request_id}.command.{command_id}.result"

    def generate_dependency_topic(
        self, request_id: RPCRequestId, dependency_command_id: RPCCommandId
    ) -> str:
        """Generate topic pattern for dependency resolution."""
        return f"mpreg.rpc.request.{request_id}.dependency.{dependency_command_id}.resolved"

    def generate_error_topic(self, request_id: RPCRequestId) -> str:
        """Generate topic pattern for RPC error notifications."""
        return f"mpreg.rpc.request.{request_id}.error"

    async def subscribe_to_request_progress(
        self, request_id: RPCRequestId, callback: Callable[[RPCProgressEvent], None]
    ) -> TopicSubscriptionId:
        """Subscribe to all progress events for an RPC request."""
        subscription_id = f"rpc_progress_{request_id}_{uuid.uuid4().hex[:8]}"
        topic_pattern = (
            self.generate_progress_topic(request_id) + ".#"
        )  # Subscribe to all progress subtopics

        # Store subscription information
        self.active_subscriptions[subscription_id] = {
            "request_id": request_id,
            "topic_pattern": topic_pattern,
            "type": "progress",
            "created_at": time.time(),
        }

        # Track subscriptions by request
        if request_id not in self.request_subscriptions:
            self.request_subscriptions[request_id] = set()
        self.request_subscriptions[request_id].add(subscription_id)

        # Store callback
        self.subscription_callbacks[subscription_id] = callback

        return subscription_id

    async def cleanup_request_subscriptions(self, request_id: RPCRequestId) -> bool:
        """Clean up all subscriptions for a completed RPC request."""
        if request_id not in self.request_subscriptions:
            return True

        subscription_ids = self.request_subscriptions[request_id].copy()
        cleanup_success = True

        for subscription_id in subscription_ids:
            try:
                # Remove from active subscriptions
                self.active_subscriptions.pop(subscription_id, None)
                self.subscription_callbacks.pop(subscription_id, None)
            except Exception:
                cleanup_success = False

        # Remove request tracking
        del self.request_subscriptions[request_id]

        return cleanup_success


@dataclass(slots=True)
class RPCCommandState:
    """Tracks the execution state of a topic-aware RPC command."""

    command: TopicAwareRPCCommand
    execution_stage: RPCExecutionStage = RPCExecutionStage.RECEIVED
    progress_percentage: float = 0.0
    start_time: float = field(default_factory=time.time)
    last_update_time: float = field(default_factory=time.time)

    # Subscription management
    progress_subscription_id: TopicSubscriptionId | None = None
    dependency_subscriptions: set[TopicSubscriptionId] = field(default_factory=set)

    # Results and errors
    intermediate_results: list[Any] = field(default_factory=list)
    final_result: Any = None
    error: dict[str, Any] | None = None

    # Metadata
    estimated_completion_time: float | None = None
    resource_usage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TopicAwareRPCExecutor:
    """Enhanced RPC executor with topic-based progress monitoring and coordination."""

    topic_exchange: Any  # TopicExchange - using Any to avoid circular imports
    config: TopicAwareRPCConfig = field(default_factory=TopicAwareRPCConfig)
    subscription_manager: RPCTopicSubscriptionManager | None = None

    # Execution state
    active_commands: dict[RPCCommandId, RPCCommandState] = field(default_factory=dict)
    request_execution_levels: dict[RPCRequestId, list[list[RPCCommandId]]] = field(
        default_factory=dict
    )

    # Statistics
    total_commands_executed: int = 0
    total_progress_events_published: int = 0
    average_execution_time_ms: float = 0.0
    total_requests_executed: int = 0
    total_request_execution_time_ms: float = 0.0

    def __post_init__(self) -> None:
        """Initialize subscription manager if not provided."""
        if self.subscription_manager is None:
            from mpreg.core.topic_taxonomy import TopicTemplateEngine

            template_engine = TopicTemplateEngine()
            self.subscription_manager = RPCTopicSubscriptionManager(
                topic_template_engine=template_engine
            )

    async def execute_with_topics(
        self,
        commands: list[TopicAwareRPCCommand],
        request_id: RPCRequestId | None = None,
    ) -> RPCRequestResult:
        """Execute RPC commands with topic-based progress monitoring and coordination."""
        if request_id is None:
            request_id = f"rpc_req_{uuid.uuid4().hex[:16]}"

        # Initialize command states
        command_states = {}
        for cmd in commands:
            state = RPCCommandState(command=cmd)
            self.active_commands[cmd.command_id] = state
            command_states[cmd.command_id] = state

        try:
            # Analyze dependencies and create execution levels
            execution_levels = await self._analyze_dependencies(commands)
            self.request_execution_levels[request_id] = execution_levels

            # Publish request started event
            await self._publish_request_event(
                request_id,
                RPCExecutionStage.DEPENDENCY_ANALYSIS,
                f"Analyzing {len(commands)} commands",
            )

            # Execute commands level by level with topic coordination
            command_results: dict[RPCCommandId, RPCResult] = {}
            total_levels = len(execution_levels)
            start_time = time.time()
            successful_commands = 0
            failed_commands = 0

            for level_index, level_commands in enumerate(execution_levels):
                await self._publish_request_event(
                    request_id,
                    RPCExecutionStage.LEVEL_STARTED,
                    f"Starting level {level_index + 1}/{total_levels}",
                )

                # Execute commands in this level in parallel with topic monitoring
                level_results = await self._execute_level_with_topics(
                    level_commands, request_id, level_index
                )
                command_results.update(level_results)

                # Count successes and failures
                for result in level_results.values():
                    if result.success:
                        successful_commands += 1
                    else:
                        failed_commands += 1

                await self._publish_request_event(
                    request_id,
                    RPCExecutionStage.LEVEL_COMPLETED,
                    f"Completed level {level_index + 1}/{total_levels}",
                )

            # Calculate total execution time
            total_execution_time = (time.time() - start_time) * 1000.0  # Convert to ms

            # Publish completion event
            await self._publish_request_event(
                request_id,
                RPCExecutionStage.REQUEST_COMPLETED,
                f"All {len(commands)} commands completed successfully",
            )

            self.total_requests_executed += 1
            self.total_request_execution_time_ms += total_execution_time
            self.average_execution_time_ms = (
                self.total_request_execution_time_ms / self.total_requests_executed
            )

            # Return proper dataclass result
            return RPCRequestResult(
                request_id=request_id,
                command_results=command_results,
                total_execution_time_ms=total_execution_time,
                total_commands=len(commands),
                successful_commands=successful_commands,
                failed_commands=failed_commands,
                execution_levels_completed=total_levels,
            )

        except Exception as e:
            # Publish failure event
            await self._publish_request_event(
                request_id,
                RPCExecutionStage.REQUEST_FAILED,
                f"Request failed: {str(e)}",
            )
            raise
        finally:
            # Cleanup command states and subscriptions
            await self._cleanup_request_state(request_id)

    async def _analyze_dependencies(
        self, commands: list[TopicAwareRPCCommand]
    ) -> list[list[RPCCommandId]]:
        """Analyze command dependencies and create execution levels."""
        # For now, implement simple dependency analysis
        # In a full implementation, this would parse args/kwargs for references to other command names

        # Simple implementation: all commands in one level (no dependencies)
        if not commands:
            return []

        # Create single level with all commands
        return [[cmd.command_id for cmd in commands]]

    async def _execute_level_with_topics(
        self,
        command_ids: list[RPCCommandId],
        request_id: RPCRequestId,
        level_index: int,
    ) -> dict[RPCCommandId, RPCResult]:
        """Execute a level of commands with topic-based monitoring."""
        tasks = []

        for command_id in command_ids:
            if command_id in self.active_commands:
                state = self.active_commands[command_id]
                task = asyncio.create_task(
                    self._execute_command_with_topics(state, request_id, level_index)
                )
                tasks.append((command_id, task))

        # Wait for all commands in this level to complete
        results: dict[RPCCommandId, RPCResult] = {}
        for command_id, task in tasks:
            try:
                result = await task
                results[command_id] = result
            except Exception as e:
                # Handle individual command failure
                await self._handle_command_failure(command_id, request_id, e)
                raise

        return results

    async def _execute_command_with_topics(
        self, state: RPCCommandState, request_id: RPCRequestId, level_index: int
    ) -> RPCResult:
        """Execute a single command with topic monitoring."""
        command = state.command

        try:
            # Publish command started event
            await self._publish_command_progress(
                state,
                request_id,
                RPCExecutionStage.COMMAND_STARTED,
                0.0,
                f"Starting command {command.name}",
            )

            # Simulate command execution (in real implementation, this would call the actual RPC function)
            execution_start = time.time()
            await self._simulate_command_execution(state, request_id)
            execution_time_ms = (time.time() - execution_start) * 1000.0

            # Create proper RPCResult dataclass
            result = RPCResult(
                command_name=command.name,
                function=command.fun,
                args=command.args,
                kwargs=command.kwargs,
                executed_at=time.time(),
                execution_level=level_index,
                command_id=command.command_id,
                execution_time_ms=execution_time_ms,
                success=True,
            )

            state.final_result = result

            # Publish command completed event
            await self._publish_command_progress(
                state,
                request_id,
                RPCExecutionStage.COMMAND_COMPLETED,
                100.0,
                f"Command {command.name} completed successfully",
            )

            return result

        except Exception as e:
            state.error = {"message": str(e), "type": type(e).__name__}
            await self._publish_command_progress(
                state,
                request_id,
                RPCExecutionStage.COMMAND_FAILED,
                state.progress_percentage,
                f"Command {command.name} failed: {str(e)}",
            )
            raise

    async def _simulate_command_execution(
        self, state: RPCCommandState, request_id: RPCRequestId
    ) -> None:
        """Simulate command execution with progress updates."""
        command = state.command

        # Simulate execution time based on estimated duration
        total_duration = command.estimated_duration_ms or 2000.0  # Default 2 seconds
        steps = 5
        step_duration = total_duration / (steps * 1000.0)  # Convert to seconds

        for step in range(steps):
            await asyncio.sleep(step_duration)

            progress = ((step + 1) / steps) * 100.0
            state.progress_percentage = progress

            await self._publish_command_progress(
                state,
                request_id,
                RPCExecutionStage.COMMAND_PROGRESS,
                progress,
                f"Processing step {step + 1}/{steps}",
            )

    async def _publish_command_progress(
        self,
        state: RPCCommandState,
        request_id: RPCRequestId,
        stage: RPCExecutionStage,
        progress: float,
        current_step: str,
    ) -> None:
        """Publish command progress via topic exchange."""
        if not state.command.publish_progress:
            return

        # Create progress event
        progress_event = RPCProgressEvent(
            request_id=request_id,
            command_id=state.command.command_id,
            execution_level=None,  # Would be populated from execution context
            stage=stage,
            progress_type=RPCProgressType.EXECUTION_PROGRESS
            if stage == RPCExecutionStage.COMMAND_PROGRESS
            else RPCProgressType.STAGE_TRANSITION,
            progress_percentage=progress,
            current_step=current_step,
            estimated_completion_ms=state.estimated_completion_time,
            resource_usage=state.resource_usage,
            timestamp=time.time(),
        )

        # Generate topic for progress event
        topic = state.command.progress_topic_pattern.format(
            request_id=request_id, command_id=state.command.command_id
        )

        # Publish to topic exchange (in real implementation)
        # await self.topic_exchange.publish(topic, progress_event)

        # Update statistics
        self.total_progress_events_published += 1
        state.last_update_time = time.time()

    async def _publish_request_event(
        self, request_id: RPCRequestId, stage: RPCExecutionStage, description: str
    ) -> None:
        """Publish request-level progress event."""
        if not self.config.enable_progress_publishing:
            return

        # Create request-level progress event
        progress_event = RPCProgressEvent(
            request_id=request_id,
            command_id=None,
            stage=stage,
            progress_type=RPCProgressType.STAGE_TRANSITION,
            current_step=description,
            timestamp=time.time(),
        )

        # Generate topic for request progress
        topic = f"{self.config.topic_prefix}.request.{request_id}.progress"

        # Publish to topic exchange (in real implementation)
        # await self.topic_exchange.publish(topic, progress_event)

    async def _handle_command_failure(
        self, command_id: RPCCommandId, request_id: RPCRequestId, error: Exception
    ) -> None:
        """Handle command execution failure."""
        if command_id in self.active_commands:
            state = self.active_commands[command_id]
            state.error = {
                "message": str(error),
                "type": type(error).__name__,
                "timestamp": time.time(),
            }

    async def _cleanup_request_state(self, request_id: RPCRequestId) -> None:
        """Clean up request state and subscriptions."""
        # Remove command states
        if request_id in self.request_execution_levels:
            for level_commands in self.request_execution_levels[request_id]:
                for command_id in level_commands:
                    self.active_commands.pop(command_id, None)

            del self.request_execution_levels[request_id]

        # Clean up subscriptions via subscription manager
        if self.subscription_manager:
            await self.subscription_manager.cleanup_request_subscriptions(request_id)

    async def get_command_progress(
        self, command_id: RPCCommandId
    ) -> RPCProgressEvent | None:
        """Get current progress for a specific command."""
        if command_id not in self.active_commands:
            return None

        state = self.active_commands[command_id]
        return RPCProgressEvent(
            request_id="unknown",  # Would need to track request mapping
            command_id=command_id,
            stage=state.execution_stage,
            progress_percentage=state.progress_percentage,
            current_step="Execution in progress",
            timestamp=state.last_update_time,
        )

    async def get_execution_statistics(self) -> dict[str, Any]:
        """Get execution statistics for monitoring."""
        active_count = len(self.active_commands)

        return {
            "total_commands_executed": self.total_commands_executed,
            "active_commands": active_count,
            "total_progress_events_published": self.total_progress_events_published,
            "average_execution_time_ms": self.average_execution_time_ms,
            "total_requests_executed": self.total_requests_executed,
            "config": {
                "enable_progress_publishing": self.config.enable_progress_publishing,
                "enable_result_streaming": self.config.enable_result_streaming,
                "progress_update_interval_ms": self.config.progress_update_interval_ms,
            },
        }


# Factory functions for creating topic-aware RPC components


def create_topic_aware_command(
    command: RPCCommand,
    enable_progress: bool = True,
    enable_streaming: bool = False,
    **kwargs: Any,
) -> TopicAwareRPCCommand:
    """Create a topic-aware RPC command from a base command."""
    return TopicAwareRPCCommand.from_rpc_command(
        command,
        publish_progress=enable_progress,
        enable_result_streaming=enable_streaming,
        **kwargs,
    )


def create_topic_aware_request(
    request: RPCRequest,
    enable_monitoring: bool = True,
    enable_streaming: bool = False,
    priority: RoutingPriority = RoutingPriority.NORMAL,
    **kwargs: Any,
) -> TopicAwareRPCRequest:
    """Create a topic-aware RPC request from a base request."""
    # Convert commands to topic-aware commands using the cmds field
    topic_aware_commands = tuple(
        create_topic_aware_command(
            cmd, enable_progress=enable_monitoring, enable_streaming=enable_streaming
        )
        for cmd in request.cmds  # Fixed: Use cmds field instead of commands
    )

    return TopicAwareRPCRequest(
        role=request.role,
        cmds=topic_aware_commands,  # Fixed: Use cmds field instead of commands
        u=request.u,
        enable_topic_monitoring=enable_monitoring,
        enable_result_streaming=enable_streaming,
        priority=priority,
        **kwargs,
    )


def create_rpc_subscription_manager(
    topic_template_engine: TopicTemplateEngine | None = None,
) -> RPCTopicSubscriptionManager:
    """Create an RPC topic subscription manager."""
    if topic_template_engine is None:
        topic_template_engine = TopicTemplateEngine()

    return RPCTopicSubscriptionManager(topic_template_engine=topic_template_engine)


def create_topic_aware_rpc_executor(
    topic_exchange: Any = None,
    config: TopicAwareRPCConfig | None = None,
    subscription_manager: RPCTopicSubscriptionManager | None = None,
) -> TopicAwareRPCExecutor:
    """Create a topic-aware RPC executor.

    Args:
        topic_exchange: TopicExchange instance for publishing progress events
        config: Configuration for topic-aware RPC behavior
        subscription_manager: Manager for topic subscriptions (created automatically if None)

    Returns:
        Configured TopicAwareRPCExecutor instance
    """
    if config is None:
        config = TopicAwareRPCConfig()

    if subscription_manager is None:
        subscription_manager = create_rpc_subscription_manager()

    return TopicAwareRPCExecutor(
        topic_exchange=topic_exchange,
        config=config,
        subscription_manager=subscription_manager,
    )
