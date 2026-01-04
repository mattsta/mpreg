"""
Topic-based RPC dependency coordination for MPREG.

This module implements Phase 2.1.3: Topic-based RPC dependency coordination,
replacing polling-based dependency resolution with event-driven topic subscriptions.

Key features:
- Real-time dependency resolution via topic subscriptions
- Cross-system dependency support (RPC → Queue → PubSub)
- Enhanced dependency analytics and tracking
- Self-managing subscription lifecycle
- Type-safe dependency event handling

Design principles:
- Event-driven: Dependencies resolved via topic notifications, not polling
- Cross-system: RPC commands can depend on queue messages and pub/sub events
- Self-managing: Automatic subscription management and cleanup
- Well-encapsulated: Proper dataclasses with type safety
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from mpreg.core.enhanced_rpc import (
    RPCCommandId,
    RPCRequestId,
    TopicAwareRPCCommand,
    TopicSubscriptionId,
)
from mpreg.core.topic_taxonomy import TopicTemplateEngine
from mpreg.datastructures.type_aliases import CorrelationId
from mpreg.fabric.message import MessageType

# Enhanced RPC dependency type aliases
type DependencyId = str
type DependencyFieldPath = str
type DependencyValue = Any
type DependencyPattern = str


class DependencyType(Enum):
    """Types of dependencies that RPC commands can have."""

    RPC_COMMAND = "rpc_command"  # Depends on another RPC command completion
    QUEUE_MESSAGE = "queue_message"  # Depends on a message queue message
    PUBSUB_MESSAGE = "pubsub_message"  # Depends on a pub/sub message
    EXTERNAL_EVENT = "external_event"  # Depends on an external system event
    CACHE_UPDATE = "cache_update"  # Depends on a cache update/invalidation
    FEDERATION_SYNC = "federation_sync"  # Depends on federation synchronization


class DependencyStatus(Enum):
    """Status of a dependency resolution."""

    PENDING = "pending"  # Dependency not yet resolved
    RESOLVING = "resolving"  # Dependency resolution in progress
    RESOLVED = "resolved"  # Dependency successfully resolved
    FAILED = "failed"  # Dependency resolution failed
    TIMEOUT = "timeout"  # Dependency resolution timed out
    CANCELLED = "cancelled"  # Dependency resolution cancelled


class DependencyResolutionStrategy(Enum):
    """Strategies for resolving dependencies."""

    IMMEDIATE = "immediate"  # Resolve immediately when available
    BATCH = "batch"  # Batch resolve with other dependencies
    DELAYED = "delayed"  # Resolve after a specified delay
    CONDITIONAL = "conditional"  # Resolve only if conditions are met


@dataclass(frozen=True, slots=True)
class DependencySpecification:
    """Specification for a single dependency."""

    dependency_id: DependencyId
    dependency_type: DependencyType
    topic_pattern: str  # Topic pattern to subscribe to for resolution
    field_path: DependencyFieldPath | None = None  # e.g., "result.data.field"

    # Resolution configuration
    resolution_strategy: DependencyResolutionStrategy = (
        DependencyResolutionStrategy.IMMEDIATE
    )
    timeout_ms: float = 30000.0  # 30 second default timeout
    required: bool = True  # Whether this dependency is required for execution

    # Value matching criteria
    expected_value: Any = None  # Expected value for conditional resolution
    value_transformer: str | None = None  # Function name to transform resolved value

    # Metadata
    description: str = ""
    correlation_id: CorrelationId | None = None


@dataclass(frozen=True, slots=True)
class DependencyResolutionEvent:
    """Event indicating a dependency has been resolved."""

    dependency_id: DependencyId
    command_id: RPCCommandId
    request_id: RPCRequestId

    # Resolution information
    status: DependencyStatus
    resolved_value: DependencyValue = None
    resolution_time_ms: float = 0.0

    # Source information
    source_topic: str = ""
    source_message_id: str = ""
    source_system: MessageType | None = None

    # Metadata
    timestamp: float = field(default_factory=time.time)
    correlation_id: CorrelationId | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class DependencyGraph:
    """Represents the dependency graph for a set of RPC commands."""

    request_id: RPCRequestId
    command_dependencies: dict[RPCCommandId, list[DependencySpecification]]
    dependency_edges: dict[
        DependencyId, set[RPCCommandId]
    ]  # Which commands depend on each dependency

    # Resolution tracking
    resolved_dependencies: dict[DependencyId, DependencyResolutionEvent] = field(
        default_factory=dict
    )
    pending_dependencies: set[DependencyId] = field(default_factory=set)

    # Analytics
    total_dependencies: int = 0
    cross_system_dependencies: int = 0
    created_at: float = field(default_factory=time.time)

    @property
    def resolution_progress(self) -> float:
        """Calculate dependency resolution progress as percentage."""
        if self.total_dependencies == 0:
            return 100.0
        return (len(self.resolved_dependencies) / self.total_dependencies) * 100.0

    @property
    def all_dependencies_resolved(self) -> bool:
        """Check if all required dependencies are resolved."""
        return len(self.pending_dependencies) == 0

    def get_ready_commands(self) -> set[RPCCommandId]:
        """Get commands that have all dependencies resolved and are ready to execute."""
        ready_commands = set()

        for command_id, dependencies in self.command_dependencies.items():
            command_ready = True
            for dep_spec in dependencies:
                if (
                    dep_spec.required
                    and dep_spec.dependency_id not in self.resolved_dependencies
                ):
                    command_ready = False
                    break

            if command_ready:
                ready_commands.add(command_id)

        return ready_commands


@dataclass(slots=True)
class DependencySubscription:
    """Tracks a topic subscription for dependency resolution."""

    dependency_id: DependencyId
    topic_pattern: str
    subscription_id: TopicSubscriptionId
    command_ids: set[RPCCommandId]  # Commands that depend on this

    # Subscription metadata
    created_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)
    message_count: int = 0

    # Resolution configuration
    timeout_ms: float = 30000.0
    resolution_strategy: DependencyResolutionStrategy = (
        DependencyResolutionStrategy.IMMEDIATE
    )


@dataclass(slots=True)
class TopicDependencyResolver:
    """Advanced topic-based dependency resolver for RPC commands."""

    topic_template_engine: TopicTemplateEngine

    # Dependency tracking
    active_dependency_graphs: dict[RPCRequestId, DependencyGraph] = field(
        default_factory=dict
    )
    dependency_subscriptions: dict[DependencyId, DependencySubscription] = field(
        default_factory=dict
    )
    subscription_callbacks: dict[TopicSubscriptionId, Callable[[Any], Any]] = field(
        default_factory=dict
    )

    # Resolution statistics
    total_dependencies_resolved: int = 0
    total_cross_system_resolutions: int = 0
    average_resolution_time_ms: float = 0.0

    # Configuration
    default_timeout_ms: float = 30000.0
    max_concurrent_subscriptions: int = 1000
    enable_dependency_analytics: bool = True

    async def create_dependency_graph(
        self, request_id: RPCRequestId, commands: list[TopicAwareRPCCommand]
    ) -> DependencyGraph:
        """Create and analyze the dependency graph for a set of commands."""
        command_dependencies: dict[RPCCommandId, list[DependencySpecification]] = {}
        dependency_edges: dict[DependencyId, set[RPCCommandId]] = {}
        pending_dependencies: set[DependencyId] = set()
        total_dependencies = 0
        cross_system_dependencies = 0

        for command in commands:
            # Analyze command for dependencies
            dependencies = await self._analyze_command_dependencies(command, request_id)
            command_dependencies[command.command_id] = dependencies

            for dep_spec in dependencies:
                total_dependencies += 1
                pending_dependencies.add(dep_spec.dependency_id)

                # Track which commands depend on this dependency
                if dep_spec.dependency_id not in dependency_edges:
                    dependency_edges[dep_spec.dependency_id] = set()
                dependency_edges[dep_spec.dependency_id].add(command.command_id)

                # Count cross-system dependencies
                if dep_spec.dependency_type != DependencyType.RPC_COMMAND:
                    cross_system_dependencies += 1

        dependency_graph = DependencyGraph(
            request_id=request_id,
            command_dependencies=command_dependencies,
            dependency_edges=dependency_edges,
            pending_dependencies=pending_dependencies,
            total_dependencies=total_dependencies,
            cross_system_dependencies=cross_system_dependencies,
        )

        self.active_dependency_graphs[request_id] = dependency_graph
        return dependency_graph

    async def _analyze_command_dependencies(
        self, command: TopicAwareRPCCommand, request_id: RPCRequestId
    ) -> list[DependencySpecification]:
        """Analyze a command to extract its dependencies."""
        dependencies: list[DependencySpecification] = []

        # Analyze command arguments and kwargs for dependency references
        dependency_patterns = await self._extract_dependency_patterns(command)

        for pattern_info in dependency_patterns:
            dep_spec = DependencySpecification(
                dependency_id=f"{request_id}_{pattern_info['name']}",
                dependency_type=pattern_info["type"],
                topic_pattern=pattern_info["topic_pattern"],
                field_path=pattern_info.get("field_path"),
                timeout_ms=pattern_info.get("timeout_ms", self.default_timeout_ms),
                required=pattern_info.get("required", True),
                description=f"Dependency for command {command.name}",
            )
            dependencies.append(dep_spec)

        # Add explicit dependency topic patterns from command configuration
        for topic_pattern in command.dependency_topic_patterns:
            dep_id = f"{request_id}_{uuid.uuid4().hex[:8]}"
            dep_spec = DependencySpecification(
                dependency_id=dep_id,
                dependency_type=DependencyType.RPC_COMMAND,  # Default to RPC for explicit patterns
                topic_pattern=topic_pattern,
                description=f"Explicit dependency pattern for {command.name}",
            )
            dependencies.append(dep_spec)

        return dependencies

    async def _extract_dependency_patterns(
        self, command: TopicAwareRPCCommand
    ) -> list[dict[str, Any]]:
        """Extract dependency patterns from command arguments and kwargs."""
        patterns: list[dict[str, Any]] = []

        # Simple pattern extraction - in a full implementation, this would be more sophisticated
        # Look for references to other command names in args and kwargs

        # Check arguments for string patterns that look like dependencies
        for i, arg in enumerate(command.args):
            if isinstance(arg, str) and "." in arg:
                # Assume format like "command_name.result" or "queue_name.message"
                if self._looks_like_dependency_reference(arg):
                    patterns.append(
                        {
                            "name": f"arg_{i}_{arg}",
                            "type": self._infer_dependency_type(arg),
                            "topic_pattern": self._generate_topic_pattern_for_dependency(
                                arg
                            ),
                            "field_path": arg if "." in arg else None,
                        }
                    )

        # Check kwargs for dependency references
        for key, value in command.kwargs.items():
            if isinstance(value, str) and "." in value:
                if self._looks_like_dependency_reference(value):
                    patterns.append(
                        {
                            "name": f"kwarg_{key}_{value}",
                            "type": self._infer_dependency_type(value),
                            "topic_pattern": self._generate_topic_pattern_for_dependency(
                                value
                            ),
                            "field_path": value if "." in value else None,
                        }
                    )

        return patterns

    def _looks_like_dependency_reference(self, value: str) -> bool:
        """Check if a string value looks like a dependency reference."""
        # Simple heuristics - could be made more sophisticated
        dependency_indicators = [
            "_result",
            "_output",
            "_data",
            "queue_",
            "topic_",
            "cache_",
            ".result",
            ".output",
            ".message",
            ".data",
        ]
        return any(indicator in value.lower() for indicator in dependency_indicators)

    def _infer_dependency_type(self, reference: str) -> DependencyType:
        """Infer the dependency type from a reference string."""
        reference_lower = reference.lower()

        if "queue" in reference_lower or ".message" in reference_lower:
            return DependencyType.QUEUE_MESSAGE
        elif "topic" in reference_lower or "pubsub" in reference_lower:
            return DependencyType.PUBSUB_MESSAGE
        elif "cache" in reference_lower:
            return DependencyType.CACHE_UPDATE
        elif "federation" in reference_lower:
            return DependencyType.FEDERATION_SYNC
        else:
            return DependencyType.RPC_COMMAND  # Default assumption

    def _generate_topic_pattern_for_dependency(self, reference: str) -> str:
        """Generate a topic pattern for dependency resolution."""
        # Simple pattern generation - would be more sophisticated in full implementation
        if "queue" in reference.lower():
            return "mpreg.queue.*.completed"
        elif "topic" in reference.lower():
            return f"mpreg.pubsub.{reference}.#"
        elif "cache" in reference.lower():
            return f"mpreg.cache.{reference}.updated"
        else:
            # Assume RPC command dependency
            command_name = reference.split(".")[0]
            return "mpreg.rpc.*.command.*.completed"

    async def setup_dependency_subscriptions(
        self, dependency_graph: DependencyGraph
    ) -> int:
        """Set up topic subscriptions for all dependencies in the graph."""
        subscriptions_created = 0

        for dependencies in dependency_graph.command_dependencies.values():
            for dep_spec in dependencies:
                if dep_spec.dependency_id not in self.dependency_subscriptions:
                    subscription = await self._create_dependency_subscription(
                        dep_spec, dependency_graph
                    )
                    if subscription:
                        self.dependency_subscriptions[dep_spec.dependency_id] = (
                            subscription
                        )
                        subscriptions_created += 1
                else:
                    # Add command to existing subscription
                    existing_subscription = self.dependency_subscriptions[
                        dep_spec.dependency_id
                    ]
                    for cmd_id in dependency_graph.dependency_edges[
                        dep_spec.dependency_id
                    ]:
                        existing_subscription.command_ids.add(cmd_id)

        return subscriptions_created

    async def _create_dependency_subscription(
        self, dep_spec: DependencySpecification, dependency_graph: DependencyGraph
    ) -> DependencySubscription | None:
        """Create a topic subscription for a dependency."""
        try:
            # Generate unique subscription ID
            subscription_id = f"dep_{dep_spec.dependency_id}_{uuid.uuid4().hex[:8]}"

            # Get commands that depend on this dependency
            dependent_commands = dependency_graph.dependency_edges.get(
                dep_spec.dependency_id, set()
            )

            subscription = DependencySubscription(
                dependency_id=dep_spec.dependency_id,
                topic_pattern=dep_spec.topic_pattern,
                subscription_id=subscription_id,
                command_ids=dependent_commands.copy(),
                timeout_ms=dep_spec.timeout_ms,
                resolution_strategy=dep_spec.resolution_strategy,
            )

            # Create callback for dependency resolution
            callback = self._create_dependency_callback(
                dep_spec, dependency_graph.request_id
            )
            self.subscription_callbacks[subscription_id] = callback

            return subscription

        except Exception as e:
            # Log error and continue - don't fail entire dependency setup
            logger.warning(
                "Failed to create subscription for dependency {}: {}",
                dep_spec.dependency_id,
                e,
            )
            return None

    def _create_dependency_callback(
        self, dep_spec: DependencySpecification, request_id: RPCRequestId
    ) -> Callable[[Any], Any]:
        """Create a callback function for dependency resolution."""

        async def dependency_callback(message: Any) -> None:
            try:
                # Extract dependency value from message
                resolved_value = await self._extract_dependency_value(message, dep_spec)

                # Create resolution event
                resolution_event = DependencyResolutionEvent(
                    dependency_id=dep_spec.dependency_id,
                    command_id="",  # Will be set when notifying specific commands
                    request_id=request_id,
                    status=DependencyStatus.RESOLVED,
                    resolved_value=resolved_value,
                    resolution_time_ms=time.time() * 1000,
                    source_topic=dep_spec.topic_pattern,
                    source_message_id=getattr(message, "message_id", "unknown"),
                    correlation_id=dep_spec.correlation_id,
                )

                # Process dependency resolution
                await self._process_dependency_resolution(resolution_event)

            except Exception as e:
                # Handle callback error
                error_event = DependencyResolutionEvent(
                    dependency_id=dep_spec.dependency_id,
                    command_id="",
                    request_id=request_id,
                    status=DependencyStatus.FAILED,
                    error_message=str(e),
                )
                await self._process_dependency_resolution(error_event)

        return dependency_callback

    async def _extract_dependency_value(
        self, message: Any, dep_spec: DependencySpecification
    ) -> Any:
        """Extract the dependency value from a resolved message."""
        if dep_spec.field_path:
            # Navigate the field path to extract specific value
            value = message
            for field in dep_spec.field_path.split("."):
                # Navigate field path - fields are known to exist based on dependency specification
                if isinstance(value, dict):
                    value = value[field]
                else:
                    value = getattr(value, field)
            return value
        else:
            # Return entire message as dependency value
            return message

    async def _process_dependency_resolution(
        self, resolution_event: DependencyResolutionEvent
    ) -> None:
        """Process a dependency resolution event."""
        request_id = resolution_event.request_id

        if request_id not in self.active_dependency_graphs:
            return  # Request no longer active

        dependency_graph = self.active_dependency_graphs[request_id]

        # Update dependency graph with resolution
        if resolution_event.status == DependencyStatus.RESOLVED:
            dependency_graph.resolved_dependencies[resolution_event.dependency_id] = (
                resolution_event
            )
            dependency_graph.pending_dependencies.discard(
                resolution_event.dependency_id
            )

            # Update statistics
            self.total_dependencies_resolved += 1

            # Check if this enables any commands to execute
            newly_ready_commands = dependency_graph.get_ready_commands()

            if newly_ready_commands:
                await self._notify_commands_ready(
                    newly_ready_commands, dependency_graph
                )

        elif resolution_event.status == DependencyStatus.FAILED:
            # Handle dependency failure
            await self._handle_dependency_failure(resolution_event, dependency_graph)

    async def _notify_commands_ready(
        self, ready_commands: set[RPCCommandId], dependency_graph: DependencyGraph
    ) -> None:
        """Notify that commands are ready to execute due to dependency resolution."""
        # In a full implementation, this would integrate with the RPC executor
        # to trigger command execution
        logger.debug(
            "Commands ready for execution: {}",
            sorted(ready_commands),
        )

    async def _handle_dependency_failure(
        self,
        resolution_event: DependencyResolutionEvent,
        dependency_graph: DependencyGraph,
    ) -> None:
        """Handle a dependency resolution failure."""
        # Mark dependent commands as failed or retry depending on configuration
        dependent_commands = dependency_graph.dependency_edges.get(
            resolution_event.dependency_id, set()
        )
        logger.debug(
            "Dependency {} failed, affects commands: {}",
            resolution_event.dependency_id,
            sorted(dependent_commands),
        )

    async def cleanup_request_dependencies(self, request_id: RPCRequestId) -> bool:
        """Clean up all dependencies and subscriptions for a completed request."""
        if request_id not in self.active_dependency_graphs:
            return True

        dependency_graph = self.active_dependency_graphs[request_id]
        cleanup_success = True

        # Clean up all subscriptions for this request
        dependencies_to_remove = []
        for dep_id in dependency_graph.pending_dependencies.union(
            dependency_graph.resolved_dependencies.keys()
        ):
            if dep_id in self.dependency_subscriptions:
                subscription = self.dependency_subscriptions[dep_id]
                try:
                    # Remove subscription callback
                    self.subscription_callbacks.pop(subscription.subscription_id, None)
                    dependencies_to_remove.append(dep_id)
                except Exception:
                    cleanup_success = False

        # Remove dependency subscriptions
        for dep_id in dependencies_to_remove:
            self.dependency_subscriptions.pop(dep_id, None)

        # Remove dependency graph
        del self.active_dependency_graphs[request_id]

        return cleanup_success

    async def get_dependency_statistics(self) -> dict[str, Any]:
        """Get comprehensive dependency resolution statistics."""
        active_graphs = len(self.active_dependency_graphs)
        active_subscriptions = len(self.dependency_subscriptions)

        # Calculate average resolution time
        total_resolution_time = 0.0
        resolved_count = 0

        for graph in self.active_dependency_graphs.values():
            for resolution_event in graph.resolved_dependencies.values():
                total_resolution_time += resolution_event.resolution_time_ms
                resolved_count += 1

        avg_resolution_time = (
            total_resolution_time / resolved_count if resolved_count > 0 else 0.0
        )

        return {
            "active_dependency_graphs": active_graphs,
            "active_subscriptions": active_subscriptions,
            "total_dependencies_resolved": self.total_dependencies_resolved,
            "total_cross_system_resolutions": self.total_cross_system_resolutions,
            "average_resolution_time_ms": avg_resolution_time,
            "max_concurrent_subscriptions": self.max_concurrent_subscriptions,
            "subscription_utilization_percent": (
                active_subscriptions / self.max_concurrent_subscriptions
            )
            * 100.0,
        }


# Factory functions for creating dependency resolution components


def create_topic_dependency_resolver(
    topic_template_engine: TopicTemplateEngine | None = None,
    default_timeout_ms: float = 30000.0,
    max_concurrent_subscriptions: int = 1000,
) -> TopicDependencyResolver:
    """Create a topic-based dependency resolver.

    Args:
        topic_template_engine: Topic template engine for pattern generation
        default_timeout_ms: Default timeout for dependency resolution
        max_concurrent_subscriptions: Maximum number of concurrent subscriptions

    Returns:
        Configured TopicDependencyResolver instance
    """
    if topic_template_engine is None:
        topic_template_engine = TopicTemplateEngine()

    return TopicDependencyResolver(
        topic_template_engine=topic_template_engine,
        default_timeout_ms=default_timeout_ms,
        max_concurrent_subscriptions=max_concurrent_subscriptions,
    )
