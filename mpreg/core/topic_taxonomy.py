"""
Topic taxonomy system for MPREG's unified communication platform.

This module defines the canonical topic namespace organization for MPREG's
integrated RPC, Topic Pub/Sub, and Message Queue systems. It establishes
clear separation between control plane (internal system) and data plane
(user-controlled) message flows.

Key features:
- Reserved mpreg.* namespace for internal system coordination
- Hierarchical topic organization with semantic meaning
- Type-safe topic pattern generation with validation
- Control plane vs data plane separation
- Cross-system topic correlation and routing
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from mpreg.datastructures.type_aliases import ClusterId, CorrelationId

# Topic taxonomy type aliases
type TopicNamespaceString = str
type TopicComponent = str
type TopicTemplate = str
type UserTopicPattern = str
type SystemTopicPattern = str


class TopicNamespace(Enum):
    """Reserved topic namespaces in MPREG."""

    # Internal system namespaces (control plane only)
    SYSTEM_ROOT = "mpreg"
    RPC_NAMESPACE = "mpreg.rpc"
    QUEUE_NAMESPACE = "mpreg.queue"
    PUBSUB_NAMESPACE = "mpreg.pubsub"
    FEDERATION_NAMESPACE = "mpreg.fabric"
    GOSSIP_NAMESPACE = "mpreg.gossip"
    MONITORING_NAMESPACE = "mpreg.monitoring"
    SECURITY_NAMESPACE = "mpreg.security"

    # User data namespaces (data plane)
    USER_ROOT = "user"
    APPLICATION_ROOT = "app"
    SERVICE_ROOT = "service"
    BUSINESS_ROOT = "business"


class TopicAccessLevel(Enum):
    """Access control levels for topic patterns."""

    CONTROL_PLANE = "control_plane"  # Internal system only
    DATA_PLANE = "data_plane"  # User-controlled data
    MIXED = "mixed"  # Both internal and user data


@dataclass(frozen=True, slots=True)
class TopicPattern:
    """Type-safe topic pattern with validation and metadata."""

    pattern: str
    access_level: TopicAccessLevel
    description: str
    example_topics: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate topic pattern on creation."""
        if not self._is_valid_pattern(self.pattern):
            raise ValueError(f"Invalid topic pattern: {self.pattern}")

        if (
            self._is_internal_pattern(self.pattern)
            and self.access_level != TopicAccessLevel.CONTROL_PLANE
        ):
            raise ValueError(
                f"Internal pattern {self.pattern} must be CONTROL_PLANE access level"
            )

    @staticmethod
    def _is_valid_pattern(pattern: str) -> bool:
        """Validate topic pattern syntax."""
        # Basic validation - can be enhanced
        if not pattern:
            return False

        # Check for valid characters and structure (including template braces)
        valid_chars = re.match(r"^[a-zA-Z0-9._*#{}-]+$", pattern)
        return valid_chars is not None

    @staticmethod
    def _is_internal_pattern(pattern: str) -> bool:
        """Check if pattern is in internal mpreg.* namespace."""
        return pattern.startswith("mpreg.")

    def matches_topic(self, topic: str) -> bool:
        """Check if this pattern matches a specific topic."""
        return TopicValidator.matches_pattern(topic, self.pattern)

    def generate_example_topic(self, **kwargs: Any) -> str:
        """Generate a concrete topic from this pattern template."""
        return self.pattern.format(**kwargs)


@dataclass(frozen=True, slots=True)
class TopicTaxonomy:
    """Centralized topic taxonomy for MPREG unified communication."""

    # === CONTROL PLANE TOPICS (Internal System Only) ===

    # RPC System Topics
    RPC_COMMAND_STARTED = TopicPattern(
        pattern="mpreg.rpc.command.{command_id}.started",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="RPC command execution started",
        example_topics=["mpreg.rpc.command.cmd_123.started"],
    )

    RPC_COMMAND_PROGRESS = TopicPattern(
        pattern="mpreg.rpc.command.{command_id}.progress",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="RPC command execution progress updates",
        example_topics=["mpreg.rpc.command.cmd_123.progress"],
    )

    RPC_COMMAND_COMPLETED = TopicPattern(
        pattern="mpreg.rpc.command.{command_id}.completed",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="RPC command execution completed",
        example_topics=["mpreg.rpc.command.cmd_123.completed"],
    )

    RPC_COMMAND_FAILED = TopicPattern(
        pattern="mpreg.rpc.command.{command_id}.failed",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="RPC command execution failed",
        example_topics=["mpreg.rpc.command.cmd_123.failed"],
    )

    RPC_DEPENDENCY_RESOLVED = TopicPattern(
        pattern="mpreg.rpc.dependency.{from_command}.{to_command}.resolved",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="RPC dependency resolution between commands",
        example_topics=["mpreg.rpc.dependency.cmd_123.cmd_456.resolved"],
    )

    RPC_LEVEL_STARTED = TopicPattern(
        pattern="mpreg.rpc.level.{level_id}.started",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="RPC execution level started (parallel commands)",
        example_topics=["mpreg.rpc.level.level_1.started"],
    )

    RPC_LEVEL_COMPLETED = TopicPattern(
        pattern="mpreg.rpc.level.{level_id}.completed",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="RPC execution level completed",
        example_topics=["mpreg.rpc.level.level_1.completed"],
    )

    # Queue System Topics
    QUEUE_MESSAGE_ENQUEUED = TopicPattern(
        pattern="mpreg.queue.{queue_name}.enqueued",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Message enqueued to queue",
        example_topics=["mpreg.queue.high_priority.enqueued"],
    )

    QUEUE_MESSAGE_PROCESSING = TopicPattern(
        pattern="mpreg.queue.{queue_name}.processing",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Message being processed from queue",
        example_topics=["mpreg.queue.high_priority.processing"],
    )

    QUEUE_MESSAGE_COMPLETED = TopicPattern(
        pattern="mpreg.queue.{queue_name}.completed",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Message processing completed",
        example_topics=["mpreg.queue.high_priority.completed"],
    )

    QUEUE_MESSAGE_FAILED = TopicPattern(
        pattern="mpreg.queue.{queue_name}.failed",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Message processing failed",
        example_topics=["mpreg.queue.high_priority.failed"],
    )

    QUEUE_DLQ_MESSAGE = TopicPattern(
        pattern="mpreg.queue.{queue_name}.dlq",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Message moved to dead letter queue",
        example_topics=["mpreg.queue.high_priority.dlq"],
    )

    QUEUE_TOPIC_ROUTED = TopicPattern(
        pattern="mpreg.queue.topic_routed.{topic_pattern}.{queue_name}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Message routed to queue via topic pattern",
        example_topics=["mpreg.queue.topic_routed.user.*.order_processing"],
    )

    # Federation System Topics
    FEDERATION_CLUSTER_JOIN = TopicPattern(
        pattern="mpreg.fabric.cluster.{cluster_id}.join",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cluster joining federation",
        example_topics=["mpreg.fabric.cluster.west_coast.join"],
    )

    FEDERATION_CLUSTER_LEAVE = TopicPattern(
        pattern="mpreg.fabric.cluster.{cluster_id}.leave",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cluster leaving federation",
        example_topics=["mpreg.fabric.cluster.west_coast.leave"],
    )

    FEDERATION_MESSAGE_FORWARD = TopicPattern(
        pattern="mpreg.fabric.forward.{source_cluster}.{target_cluster}.#",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Message forwarded between clusters",
        example_topics=["mpreg.fabric.forward.west_coast.east_coast.user.order"],
    )

    FEDERATION_ROUTE_DISCOVERED = TopicPattern(
        pattern="mpreg.fabric.route.discovered.{target_cluster}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="New federation route discovered",
        example_topics=["mpreg.fabric.route.discovered.europe"],
    )

    FEDERATION_HEALTH_UPDATE = TopicPattern(
        pattern="mpreg.fabric.health.{cluster_id}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Federation cluster health status update",
        example_topics=["mpreg.fabric.health.west_coast"],
    )

    RAFT_CONTROL_MESSAGE = TopicPattern(
        pattern="mpreg.fabric.raft.rpc",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Raft control-plane RPC message over the fabric",
        example_topics=["mpreg.fabric.raft.rpc"],
    )

    # Cache System Topics
    CACHE_INVALIDATION = TopicPattern(
        pattern="mpreg.cache.invalidation.{namespace}.{key_pattern}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cache invalidation notification for specific namespace and key pattern",
        example_topics=["mpreg.cache.invalidation.user_data.user_123"],
    )

    CACHE_COORDINATION = TopicPattern(
        pattern="mpreg.cache.coordination.{operation}.{namespace}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cache coordination operations between nodes",
        example_topics=["mpreg.cache.coordination.replication.global"],
    )

    CACHE_SYNC_STATE = TopicPattern(
        pattern="mpreg.cache.sync.state.{node_id}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cache state synchronization via fabric cache sync",
        example_topics=["mpreg.cache.sync.state.node_west_1"],
    )

    CACHE_FEDERATION_SYNC = TopicPattern(
        pattern="mpreg.cache.federation.sync.{source_cluster}.{target_cluster}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cross-cluster cache synchronization",
        example_topics=["mpreg.cache.federation.sync.west_coast.east_coast"],
    )

    CACHE_EVENT_NOTIFICATION = TopicPattern(
        pattern="mpreg.cache.events.{namespace}.{event_type}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cache event notifications for monitoring and coordination",
        example_topics=["mpreg.cache.events.user_data.eviction"],
    )

    CACHE_ANALYTICS_UPDATE = TopicPattern(
        pattern="mpreg.cache.analytics.{metric_type}.{namespace}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cache analytics and performance metrics",
        example_topics=["mpreg.cache.analytics.hit_rate.global"],
    )

    # Pub/Sub System Topics
    PUBSUB_SUBSCRIPTION_CREATED = TopicPattern(
        pattern="mpreg.pubsub.subscription.{subscription_id}.created",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="New pub/sub subscription created",
        example_topics=["mpreg.pubsub.subscription.sub_123.created"],
    )

    PUBSUB_SUBSCRIPTION_DELETED = TopicPattern(
        pattern="mpreg.pubsub.subscription.{subscription_id}.deleted",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Pub/sub subscription deleted",
        example_topics=["mpreg.pubsub.subscription.sub_123.deleted"],
    )

    PUBSUB_MESSAGE_PUBLISHED = TopicPattern(
        pattern="mpreg.pubsub.published.{topic_pattern}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Message published to pub/sub topic",
        example_topics=["mpreg.pubsub.published.user.order.created"],
    )

    PUBSUB_BACKLOG_DELIVERED = TopicPattern(
        pattern="mpreg.pubsub.backlog.{subscription_id}.delivered",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Backlog messages delivered to new subscriber",
        example_topics=["mpreg.pubsub.backlog.sub_123.delivered"],
    )

    # Gossip Protocol Topics
    GOSSIP_STATE_UPDATE = TopicPattern(
        pattern="mpreg.gossip.state.{node_id}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Gossip state update from node",
        example_topics=["mpreg.gossip.state.node_west_1"],
    )

    GOSSIP_MEMBERSHIP_CHANGE = TopicPattern(
        pattern="mpreg.gossip.membership.{change_type}.{node_id}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Gossip membership change (join/leave/fail)",
        example_topics=["mpreg.gossip.membership.join.node_west_1"],
    )

    # Monitoring and Observability Topics
    MONITORING_METRICS_UPDATE = TopicPattern(
        pattern="mpreg.monitoring.metrics.{system}.{node_id}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="System metrics update",
        example_topics=["mpreg.monitoring.metrics.rpc.node_west_1"],
    )

    MONITORING_ALERT_TRIGGERED = TopicPattern(
        pattern="mpreg.monitoring.alert.{severity}.{alert_type}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Monitoring alert triggered",
        example_topics=["mpreg.monitoring.alert.critical.high_latency"],
    )

    MONITORING_CORRELATION_TRACE = TopicPattern(
        pattern="mpreg.monitoring.trace.{correlation_id}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Cross-system message correlation trace",
        example_topics=["mpreg.monitoring.trace.corr_abc123"],
    )

    # Security and Authentication Topics
    SECURITY_AUTH_SUCCESS = TopicPattern(
        pattern="mpreg.security.auth.success.{user_id}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Authentication success event",
        example_topics=["mpreg.security.auth.success.user_123"],
    )

    SECURITY_AUTH_FAILURE = TopicPattern(
        pattern="mpreg.security.auth.failure.{source_ip}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Authentication failure event",
        example_topics=["mpreg.security.auth.failure.192.168.1.100"],
    )

    SECURITY_PERMISSION_DENIED = TopicPattern(
        pattern="mpreg.security.permission.denied.{user_id}.{resource}",
        access_level=TopicAccessLevel.CONTROL_PLANE,
        description="Permission denied for resource access",
        example_topics=["mpreg.security.permission.denied.user_123.admin_queue"],
    )

    # === DATA PLANE TOPICS (User-Controlled) ===

    # Common user topic patterns
    USER_EVENTS = TopicPattern(
        pattern="user.{user_id}.{event_type}",
        access_level=TopicAccessLevel.DATA_PLANE,
        description="User-specific events",
        example_topics=["user.123.login", "user.456.logout"],
    )

    APPLICATION_EVENTS = TopicPattern(
        pattern="app.{app_name}.{event_type}",
        access_level=TopicAccessLevel.DATA_PLANE,
        description="Application-specific events",
        example_topics=["app.ecommerce.order_created"],
    )

    SERVICE_EVENTS = TopicPattern(
        pattern="service.{service_name}.{event_type}",
        access_level=TopicAccessLevel.DATA_PLANE,
        description="Service-specific events",
        example_topics=["service.payment.transaction_completed"],
    )

    BUSINESS_EVENTS = TopicPattern(
        pattern="business.{domain}.{event_type}",
        access_level=TopicAccessLevel.DATA_PLANE,
        description="Business domain events",
        example_topics=["business.orders.created", "business.inventory.updated"],
    )


class TopicValidator:
    """Validator for topic patterns and access control."""

    @staticmethod
    def validate_topic_access(topic: str, requesting_system: str) -> bool:
        """Validate if a system can access a specific topic."""
        # Control plane topics are restricted to internal systems
        if topic.startswith("mpreg."):
            return requesting_system.startswith("mpreg_")

        # Data plane topics are open to all systems
        return True

    @staticmethod
    def validate_topic_pattern(pattern: str) -> tuple[bool, str]:
        """Validate topic pattern syntax and return error message if invalid."""
        if not pattern:
            return False, "Topic pattern cannot be empty"

        if not re.match(r"^[a-zA-Z0-9._*#-]+$", pattern):
            return False, "Topic pattern contains invalid characters"

        # Check for proper wildcard usage
        if "##" in pattern or "**" in pattern:
            return False, "Invalid wildcard usage (double wildcards not allowed)"

        # Allow inline wildcards for single-segment patterns (non-dotted identifiers)
        if "." not in pattern:
            return True, ""

        # Check that # appears only as a full segment
        for segment in pattern.split("."):
            if "#" in segment and segment != "#":
                return False, "# wildcard must be its own segment"
            if "*" in segment and segment != "*":
                return False, "* wildcard must be its own segment"

        return True, ""

    @staticmethod
    def matches_pattern(topic: str, pattern: str) -> bool:
        """Check if a topic matches a pattern supporting * and # wildcards."""
        if not pattern:
            return False
        if pattern == topic:
            return True

        if "." not in pattern and "." not in topic:
            # Single-segment glob semantics for identifiers like queue names.
            glob_pattern = re.escape(pattern).replace(r"\*", ".*").replace(r"\#", ".*")
            return bool(re.fullmatch(glob_pattern, topic))

        pattern_parts = pattern.split(".")
        topic_parts = topic.split(".")

        def _match(i: int, j: int, memo: dict[tuple[int, int], bool]) -> bool:
            key = (i, j)
            if key in memo:
                return memo[key]

            if i == len(pattern_parts) and j == len(topic_parts):
                memo[key] = True
                return True
            if i == len(pattern_parts):
                memo[key] = False
                return False

            part = pattern_parts[i]
            if part == "#":
                if i == len(pattern_parts) - 1:
                    memo[key] = True
                    return True
                for skip in range(j, len(topic_parts) + 1):
                    if _match(i + 1, skip, memo):
                        memo[key] = True
                        return True
                memo[key] = False
                return False

            if j == len(topic_parts):
                memo[key] = False
                return False

            if part == "*" or part == topic_parts[j]:
                memo[key] = _match(i + 1, j + 1, memo)
                return memo[key]

            memo[key] = False
            return False

        return _match(0, 0, {})

    @staticmethod
    def is_internal_topic(topic: str) -> bool:
        """Check if topic is in internal mpreg.* namespace."""
        return topic.startswith("mpreg.")

    @staticmethod
    def extract_topic_components(topic: str) -> list[TopicComponent]:
        """Extract topic components (segments) from a topic string."""
        return topic.split(".")

    @staticmethod
    def get_topic_access_level(topic: str) -> TopicAccessLevel:
        """Determine access level for a topic."""
        if TopicValidator.is_internal_topic(topic):
            return TopicAccessLevel.CONTROL_PLANE
        return TopicAccessLevel.DATA_PLANE


class TopicTemplateEngine:
    """Engine for generating topics from templates with validation."""

    def __init__(self):
        self.taxonomy = TopicTaxonomy()

    def generate_rpc_command_topic(self, command_id: str, event_type: str) -> str:
        """Generate RPC command topic."""
        if event_type == "started":
            return self.taxonomy.RPC_COMMAND_STARTED.generate_example_topic(
                command_id=command_id
            )
        elif event_type == "progress":
            return self.taxonomy.RPC_COMMAND_PROGRESS.generate_example_topic(
                command_id=command_id
            )
        elif event_type == "completed":
            return self.taxonomy.RPC_COMMAND_COMPLETED.generate_example_topic(
                command_id=command_id
            )
        elif event_type == "failed":
            return self.taxonomy.RPC_COMMAND_FAILED.generate_example_topic(
                command_id=command_id
            )
        else:
            raise ValueError(f"Unknown RPC command event type: {event_type}")

    def generate_queue_topic(self, queue_name: str, event_type: str) -> str:
        """Generate queue-related topic."""
        if event_type == "enqueued":
            return self.taxonomy.QUEUE_MESSAGE_ENQUEUED.generate_example_topic(
                queue_name=queue_name
            )
        elif event_type == "processing":
            return self.taxonomy.QUEUE_MESSAGE_PROCESSING.generate_example_topic(
                queue_name=queue_name
            )
        elif event_type == "completed":
            return self.taxonomy.QUEUE_MESSAGE_COMPLETED.generate_example_topic(
                queue_name=queue_name
            )
        elif event_type == "failed":
            return self.taxonomy.QUEUE_MESSAGE_FAILED.generate_example_topic(
                queue_name=queue_name
            )
        elif event_type == "dlq":
            return self.taxonomy.QUEUE_DLQ_MESSAGE.generate_example_topic(
                queue_name=queue_name
            )
        else:
            raise ValueError(f"Unknown queue event type: {event_type}")

    def generate_federation_topic(self, cluster_id: ClusterId, event_type: str) -> str:
        """Generate federation-related topic."""
        if event_type == "join":
            return self.taxonomy.FEDERATION_CLUSTER_JOIN.generate_example_topic(
                cluster_id=cluster_id
            )
        elif event_type == "leave":
            return self.taxonomy.FEDERATION_CLUSTER_LEAVE.generate_example_topic(
                cluster_id=cluster_id
            )
        elif event_type == "health":
            return self.taxonomy.FEDERATION_HEALTH_UPDATE.generate_example_topic(
                cluster_id=cluster_id
            )
        else:
            raise ValueError(f"Unknown federation event type: {event_type}")

    def generate_monitoring_topic(self, correlation_id: CorrelationId) -> str:
        """Generate monitoring correlation topic."""
        return self.taxonomy.MONITORING_CORRELATION_TRACE.generate_example_topic(
            correlation_id=correlation_id
        )

    def validate_and_generate(
        self, template: TopicTemplate, **kwargs: Any
    ) -> tuple[bool, str, str]:
        """Validate template and generate topic, returning (success, topic, error_message)."""
        try:
            # Basic template validation
            if not template:
                return False, "", "Template cannot be empty"

            # Generate topic
            generated_topic = template.format(**kwargs)

            # Validate generated topic
            is_valid, error_msg = TopicValidator.validate_topic_pattern(generated_topic)
            if not is_valid:
                return False, "", error_msg

            return True, generated_topic, ""

        except KeyError as e:
            return False, "", f"Missing template parameter: {e}"
        except Exception as e:
            return False, "", f"Template generation error: {e}"
