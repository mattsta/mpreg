"""
Federation-specific datastructures and type definitions.

Provides type-safe datastructures for blockchain-federation integration
using semantic type aliases and frozen dataclasses for immutability.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from ..core.blockchain_message_queue_types import MessageRoute
from ..datastructures.type_aliases import (
    FederationRouteId,
    GeographicDistanceKm,
    HopCount,
    # Federation types
    HubId,
    MessageIdString,
    # Message queue types
    MessagesPerSecond,
    PathLatencyMs,
    ProcessingFeeAmount,
    QueueDepth,
    RegionName,
    ReliabilityScore,
    RouteCostScore,
    # General types
    Timestamp,
)


@dataclass(frozen=True, slots=True)
class FederationMessageRoute:
    """Enhanced message route with federation-specific metadata."""

    base_route: MessageRoute
    source_hub_id: HubId
    destination_hub_id: HubId
    hub_path: list[HubId] = field(default_factory=list)
    geographic_distance_km: GeographicDistanceKm = 0.0
    estimated_hops: HopCount = 1
    preferred_by_dao: bool = False
    cross_region: bool = False
    sla_tier: int = 1  # 1=premium, 2=standard, 3=best-effort

    @property
    def route_id(self) -> FederationRouteId:
        """Get the route ID."""
        return self.base_route.route_id

    @property
    def total_cost_score(self) -> RouteCostScore:
        """Calculate total cost considering distance, hops, and fees."""
        distance_cost = self.geographic_distance_km * 0.01  # 1 cent per km
        hop_cost = float(self.estimated_hops) * 2.0  # 2 units per hop
        base_cost = float(self.base_route.cost_per_mb)

        # DAO preference discount
        dao_discount = 0.9 if self.preferred_by_dao else 1.0

        return (distance_cost + hop_cost + base_cost) * dao_discount


@dataclass(frozen=True, slots=True)
class HubPerformanceMetrics:
    """Performance metrics for a federation hub."""

    hub_id: HubId
    timestamp: Timestamp = field(default_factory=time.time)

    # Message processing metrics
    messages_routed: int = 0
    messages_failed: int = 0
    total_hops_served: HopCount = 0
    cross_region_messages: int = 0

    # Queue metrics
    current_queue_depth: QueueDepth = 0
    average_processing_time_ms: PathLatencyMs = 0.0
    messages_per_second: MessagesPerSecond = 0.0

    # Federation metrics
    connected_hubs_count: int = 0
    active_routes_count: int = 0
    dao_governed_routes: int = 0

    # Financial metrics
    total_fees_collected: ProcessingFeeAmount = 0
    total_fees_paid: ProcessingFeeAmount = 0

    @property
    def success_rate(self) -> ReliabilityScore:
        """Calculate message processing success rate."""
        total_messages = self.messages_routed + self.messages_failed
        if total_messages == 0:
            return 1.0
        return float(self.messages_routed) / float(total_messages)

    @property
    def average_hops_per_message(self) -> float:
        """Calculate average hops per message."""
        if self.messages_routed == 0:
            return 0.0
        return float(self.total_hops_served) / float(self.messages_routed)


@dataclass(frozen=True, slots=True)
class CrossRegionDeliveryRequest:
    """Request for cross-region message delivery coordination."""

    message_id: MessageIdString
    original_message_id: MessageIdString
    source_region: RegionName
    destination_region: RegionName
    source_hub_id: HubId
    destination_hub_id: HubId
    priority_level: int = 2  # 1=emergency, 2=high, 3=normal, 4=low, 5=bulk
    processing_fee: ProcessingFeeAmount = 0
    coordination_fee: ProcessingFeeAmount = 0
    requires_exactly_once: bool = False
    max_delivery_time_ms: PathLatencyMs = 30000.0  # 30 seconds default
    created_at: Timestamp = field(default_factory=time.time)


@dataclass(frozen=True, slots=True)
class CrossRegionPerformanceMetrics:
    """Performance metrics for cross-region coordination."""

    route_key: str  # "{source_region}_{destination_region}"
    timestamp: Timestamp = field(default_factory=time.time)

    # Delivery metrics
    messages_delivered: int = 0
    messages_failed: int = 0
    total_delivery_time_ms: PathLatencyMs = 0.0
    total_cost: RouteCostScore = 0.0
    total_hops: HopCount = 0

    # Coordination metrics
    coordination_attempts: int = 0
    coordination_failures: int = 0

    @property
    def average_delivery_time_ms(self) -> PathLatencyMs:
        """Calculate average delivery time."""
        if self.messages_delivered == 0:
            return 0.0
        return self.total_delivery_time_ms / float(self.messages_delivered)

    @property
    def average_cost(self) -> RouteCostScore:
        """Calculate average delivery cost."""
        if self.messages_delivered == 0:
            return 0.0
        return self.total_cost / float(self.messages_delivered)

    @property
    def average_hops(self) -> float:
        """Calculate average hops per delivery."""
        if self.messages_delivered == 0:
            return 0.0
        return float(self.total_hops) / float(self.messages_delivered)

    @property
    def delivery_success_rate(self) -> ReliabilityScore:
        """Calculate delivery success rate."""
        total_attempts = self.messages_delivered + self.messages_failed
        if total_attempts == 0:
            return 1.0
        return float(self.messages_delivered) / float(total_attempts)


@dataclass(frozen=True, slots=True)
class FederationPolicySpec:
    """Specification for federation-wide governance policies."""

    policy_name: str
    policy_type: str  # routing, fees, prioritization, capacity
    description: str
    scope: str = "global_federation"

    # Target configuration
    affected_hubs: set[HubId] = field(default_factory=set)
    affected_regions: set[RegionName] = field(default_factory=set)
    cross_region_impact: bool = True

    # Policy parameters (type-safe configuration)
    emergency_latency_max_ms: PathLatencyMs = 100.0
    high_priority_latency_max_ms: PathLatencyMs = 500.0
    normal_latency_max_ms: PathLatencyMs = 2000.0

    # Cost optimization settings
    cost_optimization_enabled: bool = True
    dao_preference_discount: float = 0.9
    cross_region_fee_multiplier: float = 2.0

    # Capacity management
    max_queue_depth_per_hub: QueueDepth = 10000
    max_cross_region_percentage: float = 0.3  # 30% max cross-region traffic

    # Quality of service
    minimum_reliability_score: ReliabilityScore = 0.95
    sla_compliance_threshold: float = 0.98

    # Democratic governance
    requires_dao_approval: bool = True
    voting_period_hours: int = 168  # 1 week
    quorum_threshold: float = 0.1
    approval_threshold: float = 0.6


@dataclass(frozen=True, slots=True)
class FederationStatus:
    """Comprehensive status of the federation system."""

    timestamp: Timestamp = field(default_factory=time.time)

    # Hub statistics
    total_hubs: int = 0
    local_hubs: int = 0
    regional_hubs: int = 0
    global_hubs: int = 0
    active_hubs: int = 0

    # Governance statistics
    dao_members_total: int = 0
    active_global_policies: int = 0
    pending_proposals: int = 0

    # Performance statistics
    total_messages_processed: int = 0
    cross_region_messages: int = 0
    average_processing_latency_ms: PathLatencyMs = 0.0
    overall_success_rate: ReliabilityScore = 1.0

    # Economic statistics
    total_fees_collected: ProcessingFeeAmount = 0
    total_fees_distributed: ProcessingFeeAmount = 0
    average_message_cost: RouteCostScore = 0.0

    # Network statistics
    total_active_routes: int = 0
    dao_preferred_routes: int = 0
    cross_region_routes: int = 0
    average_route_reliability: ReliabilityScore = 1.0


@dataclass(frozen=True, slots=True)
class RouteGovernanceDecision:
    """Decision made by DAO governance affecting routes."""

    decision_id: str
    proposal_id: str
    route_id: FederationRouteId
    decision_type: str  # prefer, deprioritize, block, optimize

    # Decision details
    rationale: str
    effective_from: Timestamp
    effective_until: Timestamp | None = None

    # Impact metrics
    affected_hubs: set[HubId] = field(default_factory=set)
    expected_cost_change: RouteCostScore = 0.0
    expected_latency_change_ms: PathLatencyMs = 0.0
    expected_reliability_change: ReliabilityScore = 0.0

    # Governance metadata
    approved_by_votes: int = 0
    total_votes: int = 0
    dao_consensus_level: float = 0.0


@dataclass(slots=True)
class MutableHubState:
    """Mutable state for hub operations (non-frozen for performance)."""

    hub_id: HubId
    last_updated: Timestamp = field(default_factory=time.time)

    # Dynamic connectivity
    connected_hubs: set[HubId] = field(default_factory=set)
    active_routes: set[FederationRouteId] = field(default_factory=set)

    # Performance tracking
    recent_metrics: list[HubPerformanceMetrics] = field(default_factory=list)
    current_load: float = 0.0
    health_score: float = 1.0

    # Governance state
    active_policy_ids: set[str] = field(default_factory=set)
    pending_policy_updates: list[str] = field(default_factory=list)

    def add_connected_hub(self, hub_id: HubId) -> None:
        """Add a connected hub."""
        self.connected_hubs.add(hub_id)
        self.last_updated = time.time()

    def remove_connected_hub(self, hub_id: HubId) -> None:
        """Remove a connected hub."""
        self.connected_hubs.discard(hub_id)
        self.last_updated = time.time()

    def update_health_score(self, new_score: float) -> None:
        """Update hub health score."""
        self.health_score = max(0.0, min(1.0, new_score))  # Clamp to [0,1]
        self.last_updated = time.time()

    def add_performance_metrics(self, metrics: HubPerformanceMetrics) -> None:
        """Add new performance metrics (keep last 100)."""
        self.recent_metrics.append(metrics)
        if len(self.recent_metrics) > 100:
            self.recent_metrics.pop(0)
        self.last_updated = time.time()


@dataclass(slots=True)
class MutableCrossRegionState:
    """Mutable state for cross-region coordination."""

    last_updated: Timestamp = field(default_factory=time.time)

    # Regional coordinators
    region_coordinators: dict[RegionName, HubId] = field(default_factory=dict)

    # Performance tracking
    route_metrics: dict[str, CrossRegionPerformanceMetrics] = field(
        default_factory=dict
    )

    # Active coordination
    pending_deliveries: set[MessageIdString] = field(default_factory=set)
    failed_deliveries: set[MessageIdString] = field(default_factory=set)

    def register_coordinator(self, region: RegionName, hub_id: HubId) -> None:
        """Register regional coordinator."""
        self.region_coordinators[region] = hub_id
        self.last_updated = time.time()

    def add_delivery_metrics(
        self, route_key: str, metrics: CrossRegionPerformanceMetrics
    ) -> None:
        """Add or update delivery metrics for a route."""
        if route_key in self.route_metrics:
            # Aggregate with existing metrics
            existing = self.route_metrics[route_key]
            self.route_metrics[route_key] = CrossRegionPerformanceMetrics(
                route_key=route_key,
                timestamp=metrics.timestamp,
                messages_delivered=existing.messages_delivered
                + metrics.messages_delivered,
                messages_failed=existing.messages_failed + metrics.messages_failed,
                total_delivery_time_ms=existing.total_delivery_time_ms
                + metrics.total_delivery_time_ms,
                total_cost=existing.total_cost + metrics.total_cost,
                total_hops=existing.total_hops + metrics.total_hops,
                coordination_attempts=existing.coordination_attempts
                + metrics.coordination_attempts,
                coordination_failures=existing.coordination_failures
                + metrics.coordination_failures,
            )
        else:
            self.route_metrics[route_key] = metrics
        self.last_updated = time.time()
