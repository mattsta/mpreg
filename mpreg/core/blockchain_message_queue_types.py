"""
Type definitions for blockchain-backed message queue system.

Provides type-safe definitions for messages, routes, governance policies,
and metrics used in the democratic message queue implementation.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# Type aliases for semantic clarity
MessageQueueId = str
RouteId = str
QueuePriority = int
MessageSize = int
ProcessingFee = int


class MessagePriority(Enum):
    """Message priority levels with democratic governance."""

    EMERGENCY = "emergency"  # DAO emergency actions
    HIGH = "high"  # Critical federation operations
    NORMAL = "normal"  # Standard operations
    LOW = "low"  # Background tasks
    BULK = "bulk"  # Batch operations


class DeliveryGuarantee(Enum):
    """Delivery guarantee levels."""

    AT_MOST_ONCE = "at_most_once"  # Fire and forget
    AT_LEAST_ONCE = "at_least_once"  # Retry until success
    EXACTLY_ONCE = "exactly_once"  # Guaranteed single delivery
    ORDERED = "ordered"  # Maintain message order


class RouteStatus(Enum):
    """Route health status."""

    ACTIVE = "active"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class MessageRoute:
    """Blockchain-recorded message route."""

    route_id: RouteId = field(default_factory=lambda: f"route_{uuid.uuid4()}")
    source_hub: str = ""
    destination_hub: str = ""
    path_hops: list[str] = field(default_factory=list)
    latency_ms: int = 0
    bandwidth_mbps: int = 0
    reliability_score: float = 0.0
    cost_per_mb: int = 0
    status: RouteStatus = RouteStatus.ACTIVE
    created_at: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate message route."""
        if not self.route_id:
            raise ValueError("Route ID cannot be empty")
        if not self.source_hub or not self.destination_hub:
            raise ValueError("Source and destination hubs required")
        if self.latency_ms < 0:
            raise ValueError("Latency cannot be negative")
        if not (0.0 <= self.reliability_score <= 1.0):
            raise ValueError("Reliability score must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class BlockchainMessage:
    """Message with blockchain integration."""

    message_id: str = field(default_factory=lambda: f"msg_{uuid.uuid4()}")
    sender_id: str = ""
    recipient_id: str = ""
    message_type: str = ""
    priority: MessagePriority = MessagePriority.NORMAL
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE
    payload: bytes = b""
    route_id: RouteId | None = None
    processing_fee: ProcessingFee = 0
    created_at: float = field(default_factory=time.time)
    expires_at: float | None = None
    retry_count: int = 0
    max_retries: int = 3
    blockchain_record: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate blockchain message."""
        if not self.message_id:
            raise ValueError("Message ID cannot be empty")
        if not self.sender_id or not self.recipient_id:
            raise ValueError("Sender and recipient required")
        if self.processing_fee < 0:
            raise ValueError("Processing fee cannot be negative")
        if self.retry_count < 0 or self.max_retries < 0:
            raise ValueError("Retry counts cannot be negative")


@dataclass(frozen=True, slots=True)
class QueueGovernancePolicy:
    """DAO-governed queue management policy using type-safe dataclasses."""

    policy_id: str = field(default_factory=lambda: f"policy_{uuid.uuid4()}")
    policy_name: str = ""
    policy_type: str = ""  # routing, prioritization, fee, capacity
    parameters: PolicyParameters = field(default_factory=lambda: PolicyParameters())
    applicable_routes: list[RouteId] = field(default_factory=list)
    effective_from: float = field(default_factory=time.time)
    effective_until: float | None = None
    dao_proposal_id: str = ""
    created_by: str = ""
    approved_by_dao: bool = False
    metadata: PolicyMetadata = field(default_factory=lambda: PolicyMetadata())

    def is_active(self, current_time: float | None = None) -> bool:
        """Check if policy is currently active."""
        if current_time is None:
            current_time = time.time()

        if not self.approved_by_dao:
            return False

        if current_time < self.effective_from:
            return False

        return not (self.effective_until and current_time > self.effective_until)


@dataclass(frozen=True, slots=True)
class PolicyParameters:
    """Type-safe policy parameters dataclass."""

    # Common routing parameters
    emergency_latency_max_ms: float = 100.0
    high_priority_latency_max_ms: float = 500.0
    normal_latency_max_ms: float = 2000.0
    cost_optimization_enabled: bool = True
    preferred_routes: list[str] = field(default_factory=list)

    # Fee parameters
    base_fee_per_mb: int = 5
    priority_fee_multiplier: float = 2.0
    progressive_fee_enabled: bool = True

    # Capacity parameters
    max_queue_depth: int = 10000
    max_retry_count: int = 3
    batch_size_limit: int = 100

    # Custom parameters as key-value pairs (for extensibility)
    custom_parameters: dict[str, str | int | float | bool] = field(default_factory=dict)

    def get_parameter(self, name: str) -> str | int | float | bool | list[str]:
        """Get parameter value by name from known policy config fields."""
        # Access known policy configuration parameters directly
        match name:
            case "emergency_latency_max_ms":
                return self.emergency_latency_max_ms
            case "high_priority_latency_max_ms":
                return self.high_priority_latency_max_ms
            case "normal_latency_max_ms":
                return self.normal_latency_max_ms
            case "cost_optimization_enabled":
                return self.cost_optimization_enabled
            case "preferred_routes":
                return self.preferred_routes
            case "base_fee_per_mb":
                return self.base_fee_per_mb
            case "priority_fee_multiplier":
                return self.priority_fee_multiplier
            case "progressive_fee_enabled":
                return self.progressive_fee_enabled
            case "max_queue_depth":
                return self.max_queue_depth
            case "max_retry_count":
                return self.max_retry_count
            case "batch_size_limit":
                return self.batch_size_limit
            case _:
                # Custom parameters
                return self.custom_parameters.get(name, "")


@dataclass(frozen=True, slots=True)
class PolicyMetadata:
    """Type-safe policy metadata dataclass."""

    scope: str = "local"
    policy_type: str = "general"
    affected_hubs: list[str] = field(default_factory=list)
    affected_routes: list[str] = field(default_factory=list)
    fairness_score: float = 0.8
    democratic_routing: bool = False
    cross_region_impact: bool = False

    # Custom metadata as key-value pairs (for extensibility)
    custom_metadata: dict[str, str | int | float | bool] = field(default_factory=dict)

    def get_metadata(self, key: str) -> str | int | float | bool | list[str]:
        """Get metadata value by key from known policy metadata fields."""
        # Access known policy metadata fields directly
        match key:
            case "scope":
                return self.scope
            case "policy_type":
                return self.policy_type
            case "affected_hubs":
                return self.affected_hubs
            case "affected_routes":
                return self.affected_routes
            case "fairness_score":
                return self.fairness_score
            case "democratic_routing":
                return self.democratic_routing
            case "cross_region_impact":
                return self.cross_region_impact
            case _:
                # Custom metadata
                return self.custom_metadata.get(key, "")


@dataclass(frozen=True, slots=True)
class RoutingCriteria:
    """Type-safe routing criteria for democratic route selection."""

    latency_threshold_ms: float = 2000.0
    cost_threshold: float = 100.0
    min_reliability: float = 0.95
    latency_weight: float = 0.4
    cost_weight: float = 0.3
    reliability_weight: float = 0.3
    prefer_direct: bool = True
    cost_optimization: bool = True
    preferred_routes: list[str] = field(default_factory=list)

    def calculate_route_score(
        self, latency_ms: float, cost: float, reliability: float
    ) -> float:
        """Calculate weighted score for a route."""
        # Normalize values (lower is better for latency and cost, higher for reliability)
        latency_score = max(
            0, (self.latency_threshold_ms - latency_ms) / self.latency_threshold_ms
        )
        cost_score = max(0, (self.cost_threshold - cost) / self.cost_threshold)
        reliability_score = reliability

        return (
            self.latency_weight * latency_score
            + self.cost_weight * cost_score
            + self.reliability_weight * reliability_score
        )


@dataclass(frozen=True, slots=True)
class QueueMetrics:
    """Queue performance metrics recorded on blockchain."""

    metric_id: str = field(default_factory=lambda: f"metric_{uuid.uuid4()}")
    queue_id: MessageQueueId = ""
    route_id: RouteId | None = None
    timestamp: float = field(default_factory=time.time)

    # Performance metrics
    messages_processed: int = 0
    average_latency_ms: float = 0.0
    throughput_msgs_per_sec: float = 0.0
    error_rate: float = 0.0
    queue_depth: int = 0

    # Economic metrics
    total_fees_collected: int = 0
    average_fee_per_message: float = 0.0

    # Quality metrics
    delivery_success_rate: float = 0.0
    sla_compliance_rate: float = 0.0

    # Resource metrics
    cpu_utilization: float = 0.0
    memory_utilization: float = 0.0
    network_utilization: float = 0.0

    metadata: dict[str, Any] = field(default_factory=dict)
