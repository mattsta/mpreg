"""
Statistics Dataclasses for MPREG Federation.

This module provides well-typed dataclasses to replace dict[str, Any] returns
from statistics methods throughout the MPREG codebase. These dataclasses
provide proper type safety and clear documentation of statistics structures.
"""

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mpreg.datastructures.trie import TrieStatistics


@dataclass(frozen=True, slots=True)
class PolicyConfiguration:
    """Configuration settings for routing policy."""

    default_strategy: str
    max_load_threshold: float
    cache_ttl_seconds: float


@dataclass(frozen=True, slots=True)
class HubSelectionStatistics:
    """Statistics for hub selection operations."""

    selection_counts: dict[str, int]
    cache_size: int
    cache_hit_rate: float
    policy_config: PolicyConfiguration


@dataclass(frozen=True, slots=True)
class ZoneDistribution:
    """Distribution of hubs across zones."""

    local_hubs: int
    regional_hubs: int
    global_hubs: int


@dataclass(frozen=True, slots=True)
class ZoneStatistics:
    """Statistics for zone partitioning operations."""

    total_zones: int
    zone_hierarchy_depth: int
    cache_size: int
    partition_stats: dict[str, int]
    zone_distribution: ZoneDistribution


@dataclass(frozen=True, slots=True)
class CacheStatistics:
    """Cache performance statistics."""

    cache_size: int
    cache_hit_rate: float
    max_cache_entries: int


@dataclass(frozen=True, slots=True)
class RoutingPolicyInfo:
    """Routing policy configuration information."""

    default_strategy: str
    max_hops: int
    max_latency_ms: float
    max_load_threshold: float


@dataclass(frozen=True, slots=True)
class HierarchicalRoutingStatistics:
    """Comprehensive statistics for hierarchical routing operations."""

    routing_stats: dict[str, int]
    performance_metrics: dict[str, float]
    cache_statistics: CacheStatistics
    hub_selector_stats: HubSelectionStatistics
    zone_partitioner_stats: ZoneStatistics
    routing_policy: RoutingPolicyInfo


@dataclass(frozen=True, slots=True)
class GraphCollectionStatus:
    """Status information for graph metrics collection."""

    is_collecting: bool
    last_collection: float | None
    collection_interval: float
    active_collectors: int


@dataclass(frozen=True, slots=True)
class MetricsStatistics:
    """Statistics about metrics collection performance."""

    total_collected: int
    update_successes: int
    collection_errors: int
    success_rate: float
    error_rate: float
    stored_metrics: int


@dataclass(frozen=True, slots=True)
class GraphMetricsCollectorStatistics:
    """Statistics for graph metrics collection."""

    collection_status: GraphCollectionStatus
    metrics_statistics: MetricsStatistics
    recent_metrics_by_type: dict[str, int]


@dataclass(frozen=True, slots=True)
class InvalidationThresholds:
    """Thresholds for cache invalidation triggers."""

    latency_change: float
    utilization_change: float
    health_change: float


@dataclass(frozen=True, slots=True)
class PathCacheManagerStatistics:
    """Statistics for path cache management."""

    invalidation_strategy: str
    invalidations_triggered: int
    invalidations_by_reason: dict[str, int]
    thresholds: InvalidationThresholds


@dataclass(frozen=True, slots=True)
class OptimizationStatus:
    """Status information for graph optimization."""

    is_optimizing: bool
    last_optimization: float
    optimization_interval: float


@dataclass(frozen=True, slots=True)
class OptimizationHistory:
    """Historical information about optimizations performed."""

    total_optimizations: int
    optimizations_by_type: dict[str, int]


@dataclass(frozen=True, slots=True)
class GraphOptimizerStatistics:
    """Statistics for graph optimization operations."""

    optimization_status: OptimizationStatus
    optimization_history: OptimizationHistory
    current_suggestions: int
    suggestions_by_priority: dict[int, int]


@dataclass(frozen=True, slots=True)
class GraphCacheStatistics:
    """Graph-specific cache performance statistics."""

    hit_rate: float
    total_entries: int
    cache_hits: int
    cache_misses: int


@dataclass(frozen=True, slots=True)
class GraphStatistics:
    """Statistics about the federation graph structure."""

    total_nodes: int
    healthy_nodes: int
    total_edges: int
    usable_edges: int
    node_types: dict[str, int]
    average_connectivity: float
    cache_statistics: GraphCacheStatistics
    path_computations: int
    health_ratio: float
    usability_ratio: float


@dataclass(frozen=True, slots=True)
class MonitoringStatus:
    """Overall monitoring system status."""

    is_monitoring: bool
    uptime_seconds: float
    start_time: float


@dataclass(frozen=True, slots=True)
class GraphMonitoringStatistics:
    """Comprehensive statistics for federation graph monitoring."""

    monitoring_status: MonitoringStatus
    metrics_collector: GraphMetricsCollectorStatistics
    cache_manager: PathCacheManagerStatistics
    optimizer: GraphOptimizerStatistics
    graph_statistics: GraphStatistics


@dataclass(frozen=True, slots=True)
class HubUtilization:
    """Hub utilization by tier."""

    global_hubs: dict[str, float]
    regional_hubs: dict[str, float]
    local_hubs: dict[str, float]


@dataclass(frozen=True, slots=True)
class TopologyStatistics:
    """Comprehensive topology statistics."""

    hub_counts: dict[str, int]
    total_hubs: int
    total_clusters: int
    total_subscriptions: int
    hierarchy_depth: int
    hub_utilization: HubUtilization
    aggregation_ratios: dict[str, float]


@dataclass(frozen=True, slots=True)
class ClientStatistics:
    """Statistics for pub/sub client operations."""

    client_id: str
    active_subscriptions: int
    running: bool
    oldest_subscription: float | None


# Client API Dataclasses for replacing dict[str, Any] usage


@dataclass(frozen=True, slots=True)
class RawMessageResponse:
    """Response from send_raw_message operations."""

    status: str
    message_id: str | None = None
    error: str | None = None
    timestamp: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for compatibility with existing code."""
        result: dict[str, Any] = {"status": self.status}
        if self.message_id is not None:
            result["message_id"] = self.message_id
        if self.error is not None:
            result["error"] = self.error
        if self.timestamp is not None:
            result["timestamp"] = self.timestamp
        return result


class RawMessageDict(dict[str, Any]):
    """A dict subclass that can be returned from send_raw_message to maintain compatibility."""

    def __init__(self, data: dict[str, Any] | None = None, **kwargs):
        """Initialize with data dict or keyword arguments."""
        if data is not None:
            super().__init__(data)
        else:
            super().__init__()
        self.update(kwargs)


@dataclass(frozen=True, slots=True)
class MessageHeaders:
    """Headers for pub/sub messages."""

    content_type: str | None = None
    correlation_id: str | None = None
    reply_to: str | None = None
    priority: int | None = None
    ttl_seconds: int | None = None
    custom_headers: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert headers to dict format for compatibility with PubSubMessage."""
        result: dict[str, Any] = {}
        if self.content_type is not None:
            result["content_type"] = self.content_type
        if self.correlation_id is not None:
            result["correlation_id"] = self.correlation_id
        if self.reply_to is not None:
            result["reply_to"] = self.reply_to
        if self.priority is not None:
            result["priority"] = self.priority
        if self.ttl_seconds is not None:
            result["ttl_seconds"] = self.ttl_seconds
        if self.custom_headers is not None:
            result.update(self.custom_headers)
        return result


@dataclass(frozen=True, slots=True)
class SubscriptionInfo:
    """Information about an active subscription."""

    subscription_id: str
    patterns: list[str]
    created_at: float
    subscriber: str | None = None
    backlog_enabled: bool = False


@dataclass(slots=True)  # Not frozen because fields are mutable
class TopicMetrics:
    """Metrics collected from topic messages."""

    subscription_id: str
    message_count: int = 0
    topics_seen: set[str] | None = None
    publishers_seen: set[str] | None = None
    last_message_time: float | None = None

    def __post_init__(self):
        """Initialize mutable fields."""
        if self.topics_seen is None:
            self.topics_seen = set()
        if self.publishers_seen is None:
            self.publishers_seen = set()


@dataclass(frozen=True, slots=True)
class PublishResponse:
    """Response from publish operations with reply support."""

    success: bool
    message_id: str
    reply_payload: Any | None = None
    reply_headers: MessageHeaders | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ClusterInfo:
    """Cluster configuration information."""

    local_cluster_id: str
    total_remote_clusters: int
    active_clusters: int
    max_clusters: int


@dataclass(frozen=True, slots=True)
class RoutingConfiguration:
    """Routing system configuration."""

    graph_routing_enabled: bool
    monitoring_enabled: bool
    graph_routing_threshold: int


@dataclass(frozen=True, slots=True)
class RemoteClusterInfo:
    """Information about a remote cluster connection."""

    healthy: bool
    status: str
    latency_ms: float
    success_rate: float


@dataclass(frozen=True, slots=True)
class FederationBridgeStatistics:
    """Comprehensive statistics for federation bridge."""

    cluster_info: ClusterInfo
    routing_configuration: RoutingConfiguration
    message_statistics: dict[str, int]
    performance_metrics: dict[str, float]
    graph_routing_stats: dict[str, int]
    last_sync: float
    uptime_seconds: float
    remote_clusters: dict[str, RemoteClusterInfo]
    graph_routing_performance: "GraphRouterStatistics | None" = None
    monitoring: "GraphMonitoringStatistics | None" = None


@dataclass(frozen=True, slots=True)
class RoutingPerformance:
    """Performance metrics for routing operations."""

    total_requests: int
    successful_routes: int
    success_rate: float
    average_computation_time_ms: float


@dataclass(frozen=True, slots=True)
class AlgorithmPerformance:
    """Performance metrics for routing algorithms."""

    dijkstra_last_computation_ms: float
    astar_last_computation_ms: float


@dataclass(frozen=True, slots=True)
class GraphRouterStatistics:
    """Comprehensive statistics for graph-based router."""

    graph_statistics: GraphStatistics
    routing_performance: RoutingPerformance
    algorithm_performance: AlgorithmPerformance


@dataclass(slots=True)  # Not frozen so fields can be updated
class GossipProtocolStatistics:
    """Protocol-level statistics for gossip operations."""

    messages_sent: int = 0
    messages_received: int = 0
    messages_filtered: int = 0
    gossip_cycles: int = 0
    anti_entropy_runs: int = 0
    convergence_events: int = 0
    broadcast_messages: int = 0
    unicast_messages: int = 0
    protocol_started: int = 0
    protocol_stopped: int = 0
    total_messages_sent: int = 0
    messages_created: int = 0


@dataclass(frozen=True, slots=True)
class GossipConvergenceTracking:
    """Convergence tracking metrics for gossip protocol."""

    last_convergence_time: float = 0.0
    average_convergence_time: float = 0.0
    convergence_count: int = 0
    pending_convergence: float = 0.0


@dataclass(frozen=True, slots=True)
class LoadMetrics:
    """Load metrics for node monitoring."""

    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    network_usage: float = 0.0
    message_throughput: float = 0.0


@dataclass(frozen=True, slots=True)
class NodeCapabilities:
    """Node capability information."""

    supports_gossip: bool = True
    supports_federation: bool = True
    max_connections: int = 100
    version: str = "1.0.0"


@dataclass(frozen=True, slots=True)
class ConnectionInfo:
    """Node connection information."""

    endpoint: str = ""
    port: int = 0
    protocol: str = "tcp"
    is_secure: bool = False


@dataclass(slots=True)  # Not frozen because fields need to be mutable for counters
class MonitoringStatsData:
    """Monitoring statistics data."""

    health_checks: int = 0
    hub_failures_handled: int = 0
    last_health_check: float = 0.0
    total_monitoring_cycles: int = 0


@dataclass(slots=True)  # Not frozen because these are counters
class RegistryPerformanceStats:
    """Performance statistics for registry operations."""

    total_registrations: int = 0
    successful_registrations: int = 0
    failed_registrations: int = 0
    registration_timeouts: int = 0
    hub_discoveries: int = 0
    cluster_assignments: int = 0
    registry_started: int = 0
    registry_stopped: int = 0
    hubs_registered: int = 0
    hubs_deregistered: int = 0
    discovery_requests: int = 0


@dataclass(slots=True)  # Not frozen because these are metrics
class RegistryPerformanceMetrics:
    """Performance metrics for registry operations."""

    avg_registration_time_ms: float = 0.0
    avg_discovery_time_ms: float = 0.0
    avg_assignment_time_ms: float = 0.0
    success_rate: float = 0.0


@dataclass(slots=True)  # Not frozen because these are counters
class AssignmentStatsData:
    """Statistics for cluster-to-hub assignments."""

    total_assignments: int = 0
    successful_assignments: int = 0
    failed_assignments: int = 0
    reassignments: int = 0
    auto_assignments: int = 0
    manual_assignments: int = 0
    clusters_assigned: int = 0
    clusters_deregistered: int = 0
    clusters_reassigned: int = 0


# Topic Exchange Statistics
@dataclass(frozen=True, slots=True)
class SubscriptionHandlerStatistics:
    """Statistics for subscription handler performance."""

    total_subscriptions: int = 0
    active_subscriptions: int = 0
    subscriptions_by_pattern: int = 0
    total_deliveries: int = 0
    failed_deliveries: int = 0
    delivery_success_rate: float = 0.0


@dataclass(frozen=True, slots=True)
class TopicExchangeStatistics:
    """Statistics for overall topic exchange performance."""

    total_published: int = 0
    total_delivered: int = 0
    active_subscriptions: int = 0
    delivery_rate: float = 0.0
    subscription_count: int = 0


@dataclass(frozen=True, slots=True)
class BacklogStatistics:
    """Statistics for message backlog."""

    total_messages: int = 0
    total_size_bytes: int = 0
    total_size_mb: float = 0.0
    active_topics: int = 0
    cleanup_queue_size: int = 0


@dataclass(frozen=True, slots=True)
class FederationInfo:
    """Federation configuration and status information."""

    enabled: bool = False
    cluster_id: str = ""
    cluster_name: str = ""
    bridge_url: str = ""
    connected_clusters: int = 0
    cross_cluster_messages: int = 0
    federation_latency_ms: float = 0.0
    federated_messages_sent: int = 0
    federated_messages_received: int = 0
    federation_errors: int = 0
    bloom_filter_memory_bytes: int = 0
    local_patterns: int = 0
    bloom_filter_fp_rate: float = 0.0
    total_remote_subscriptions: int = 0
    routing_table_size: int = 0


@dataclass(frozen=True, slots=True)
class ClusterHealthInfo:
    """Health information for a specific cluster."""

    status: str
    health_score: float
    consecutive_failures: int
    average_latency_ms: float
    cpu_usage_percent: float
    memory_usage_percent: float
    last_check: float


@dataclass(frozen=True, slots=True)
class FederationHealth:
    """Health status of federation system."""

    federation_enabled: bool = True
    overall_health: str = "unknown"  # "healthy", "degraded", "unhealthy", "disabled"
    global_status: str = "unknown"  # HealthStatus enum value as string
    total_clusters: int = 0
    healthy_clusters: int = 0
    degraded_clusters: int = 0
    unhealthy_clusters: int = 0
    health_percentage: float = 0.0
    last_health_check: float = 0.0
    connectivity_issues: list[str] = field(default_factory=list)
    cluster_health: dict[str, ClusterHealthInfo] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TopicExchangeComprehensiveStats:
    """Comprehensive statistics for the main TopicExchange."""

    server_url: str
    cluster_id: str
    active_subscriptions: int
    active_subscribers: int
    messages_published: int
    messages_delivered: int
    delivery_ratio: float
    trie_stats: "TrieStatistics"
    backlog_stats: BacklogStatistics
    remote_servers: int


@dataclass(frozen=True, slots=True)
class FederatedTopicExchangeStats(TopicExchangeComprehensiveStats):
    """Comprehensive statistics for the federated TopicExchange."""

    federation: FederationInfo


@dataclass(frozen=True, slots=True)
class ClusterMetrics:
    """Metrics for a specific cluster in routing table."""

    avg_latency_ms: float
    p95_latency_ms: float
    success_rate: float
    routing_weight: float
    total_requests: int


@dataclass(frozen=True, slots=True)
class IntelligentRoutingStatistics:
    """Statistics for intelligent routing table."""

    total_routes: int
    total_clusters: int
    cluster_metrics: dict[str, ClusterMetrics]
    last_update: float


@dataclass(frozen=True, slots=True)
class ClusterDelta:
    """Delta changes for a cluster configuration."""

    version: int
    patterns: list[str]
    subscription_count: int
    last_updated: float


@dataclass(frozen=True, slots=True)
class HubInfo:
    """Basic hub identification information."""

    hub_id: str
    hub_tier: str
    region: str
    coordinates: tuple[float, float]


@dataclass(frozen=True, slots=True)
class HubCapacity:
    """Hub capacity limits."""

    max_clusters: int
    max_child_hubs: int
    max_subscriptions: int


@dataclass(frozen=True, slots=True)
class HubCurrentLoad:
    """Current load metrics for hub."""

    active_clusters: int
    active_child_hubs: int
    active_subscriptions: int
    utilization_score: float
    is_healthy: bool


@dataclass(frozen=True, slots=True)
class HubAggregationInfo:
    """Hub aggregation state information."""

    total_subscriptions: int
    compression_ratio: float
    popular_topics_count: int
    last_aggregated: float


@dataclass(frozen=True, slots=True)
class HubComprehensiveStatistics:
    """Comprehensive statistics for federation hub."""

    hub_info: HubInfo
    capacity: HubCapacity
    current_load: HubCurrentLoad
    routing_statistics: dict[str, int]
    aggregation_info: HubAggregationInfo
    child_hubs: list[str]
    parent_hub: str | None


@dataclass(frozen=True, slots=True)
class ConflictSummary:
    """Summary of a consensus conflict."""

    conflict_id: str
    state_key: str
    num_conflicting_values: int
    value_sources: list[str]
    value_timestamps: list[float]
    resolution_strategy: str
    resolved: bool
    age_seconds: float


@dataclass(frozen=True, slots=True)
class ProposalSummary:
    """Summary of a consensus proposal."""

    proposal_id: str
    proposer: str
    state_key: str
    status: str
    votes: str
    required_votes: int
    has_consensus: bool
    is_expired: bool
    age_seconds: float


@dataclass(frozen=True, slots=True)
class ResolutionStatistics:
    """Statistics for conflict resolution."""

    resolution_counts: dict[str, int]
    recent_resolutions: int
    avg_resolution_time_ms: float
    custom_resolvers: int
    resolution_history_size: int


@dataclass(frozen=True, slots=True)
class ConsensusInfo:
    """Basic consensus configuration information."""

    node_id: str
    known_nodes: int
    default_threshold: float
    proposal_timeout: float


@dataclass(frozen=True, slots=True)
class ConsensusStatistics:
    """Statistics for consensus protocol."""

    consensus_info: ConsensusInfo
    consensus_stats: dict[str, int]
    active_proposals: int
    proposal_history_size: int
    active_proposal_summaries: list[ProposalSummary]


# Federation Resilience Dataclasses


@dataclass(frozen=True, slots=True)
class CircuitBreakerState:
    """State information for a circuit breaker."""

    state: str  # "open", "closed", "half_open"
    failure_count: int
    success_count: int
    current_timeout: float


@dataclass(frozen=True, slots=True)
class ResourceMetrics:
    """Resource utilization metrics for a cluster."""

    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    connection_count: int = 0
    queue_depth: int = 0


@dataclass(frozen=True, slots=True)
class HealthCheckResult:
    """Result from a cluster health check operation."""

    cluster_id: str
    status: str  # HealthStatus enum value as string
    latency_ms: float
    timestamp: float
    error_message: str | None = None
    resource_metrics: ResourceMetrics | None = None


@dataclass(frozen=True, slots=True)
class ResilienceMetrics:
    """Comprehensive resilience metrics."""

    enabled: bool
    health_summary: FederationHealth
    circuit_breaker_states: dict[str, CircuitBreakerState]
    active_recovery_strategies: dict[str, str]
    total_registered_clusters: int


# Auto-Discovery Statistics Dataclasses


@dataclass(frozen=True, slots=True)
class DiscoveryBackendStatistics:
    """Statistics for a specific discovery backend."""

    protocol: str
    discoveries: int = 0
    new_clusters: int = 0
    lost_clusters: int = 0
    errors: int = 0
    registrations: int = 0
    registration_failures: int = 0
    registration_errors: int = 0
    last_discovery: float = 0.0


@dataclass(frozen=True, slots=True)
class ClustersByRegion:
    """Clusters grouped by region."""

    region_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ClustersByHealth:
    """Clusters grouped by health status."""

    healthy: int = 0
    degraded: int = 0
    unhealthy: int = 0
    critical: int = 0
    unknown: int = 0


@dataclass(frozen=True, slots=True)
class AutoDiscoveryStatistics:
    """Comprehensive statistics for auto-discovery service."""

    total_clusters: int
    clusters_by_region: ClustersByRegion
    clusters_by_health: ClustersByHealth
    discovery_backends: int
    backend_statistics: dict[str, DiscoveryBackendStatistics]
    local_cluster_registered: bool
    discovery_running: bool = False
    total_events: int = 0


@dataclass(frozen=True, slots=True)
class CLIDiscoveredCluster:
    """CLI representation of a discovered cluster for display purposes."""

    cluster_id: str
    cluster_name: str
    region: str
    bridge_url: str
    server_url: str
    status: str  # "active", "inactive", "maintenance"
    health: str  # "healthy", "degraded", "unhealthy", "unknown"
    discovery_source: str
    health_score: float = 100.0

    @classmethod
    def from_discovered_cluster(cls, cluster) -> "CLIDiscoveredCluster":
        """Create CLI cluster from auto-discovery DiscoveredCluster."""
        status = "active" if cluster.health_status.value == "healthy" else "degraded"
        if cluster.health_score < 25.0:
            status = "inactive"

        return cls(
            cluster_id=cluster.cluster_id,
            cluster_name=cluster.cluster_name,
            region=cluster.region,
            bridge_url=cluster.bridge_url,
            server_url=cluster.server_url,
            status=status,
            health=cluster.health_status.value,
            discovery_source=cluster.discovery_source,
            health_score=cluster.health_score,
        )

    @classmethod
    def from_config_dict(cls, config_data: dict[str, Any]) -> "CLIDiscoveredCluster":
        """Create CLI cluster from configuration dictionary."""
        return cls(
            cluster_id=config_data["cluster_id"],
            cluster_name=config_data.get("cluster_name", config_data["cluster_id"]),
            region=config_data.get("region", "unknown"),
            bridge_url=config_data["bridge_url"],
            server_url=config_data["server_url"],
            status=config_data.get("status", "active"),
            health=config_data.get("health", "unknown"),
            discovery_source="config",
            health_score=config_data.get("health_score", 100.0),
        )


@dataclass(frozen=True, slots=True)
class ClusterPerformanceSummary:
    """Performance summary for a single cluster."""

    cluster_id: str
    health_score: float
    avg_latency_ms: float
    throughput_rps: float
    error_rate_percent: float


@dataclass(frozen=True, slots=True)
class AlertsSummary:
    """Summary of active alerts by severity."""

    info: int = 0
    warning: int = 0
    error: int = 0
    critical: int = 0
    total: int = 0


@dataclass(frozen=True, slots=True)
class CollectionStatus:
    """Status of metrics collection system."""

    collecting: bool
    collectors: int
    collection_interval: float


@dataclass(frozen=True, slots=True)
class FederationPerformanceMetrics:
    """Aggregated federation-wide performance metrics."""

    # Federation overview
    total_clusters: int = 0
    healthy_clusters: int = 0
    degraded_clusters: int = 0
    unhealthy_clusters: int = 0

    # Aggregated performance
    federation_avg_latency_ms: float = 0.0
    federation_p95_latency_ms: float = 0.0
    federation_total_throughput_rps: float = 0.0
    federation_error_rate_percent: float = 0.0

    # Cross-cluster communication
    total_cross_cluster_messages: int = 0
    avg_cross_cluster_latency_ms: float = 0.0
    cross_cluster_error_rate_percent: float = 0.0

    # Resource utilization (federation-wide averages)
    avg_cpu_usage_percent: float = 0.0
    avg_memory_usage_percent: float = 0.0
    total_network_io_mbps: float = 0.0

    # Health and availability
    federation_health_score: float = 100.0
    total_uptime_hours: float = 0.0

    # Message flow
    total_messages_sent: int = 0
    total_messages_received: int = 0
    total_messages_failed: int = 0
    average_queue_depth: float = 0.0

    # Timestamp
    collected_at: float = field(default_factory=time.time)


@dataclass(frozen=True, slots=True)
class PerformanceSummary:
    """Comprehensive performance summary for federation."""

    federation_metrics: FederationPerformanceMetrics | None
    cluster_count: int
    clusters: dict[str, ClusterPerformanceSummary]
    active_alerts: int
    alerts_by_severity: AlertsSummary
    collection_status: CollectionStatus
    timestamp: float
