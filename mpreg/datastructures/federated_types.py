"""
Type aliases and dataclasses for Federated RPC System.

This module provides well-typed interfaces for the federated RPC announcement
and propagation system, replacing primitive types with semantic type aliases
and structured dataclasses for better type safety and code clarity.
"""

from dataclasses import dataclass, field

from .type_aliases import (
    ClusterID,
    DurationSeconds,
    FunctionName,
    NodeURL,
    ResourceName,
    Timestamp,
)

# ============================================================================
# FEDERATED RPC TYPE ALIASES
# ============================================================================

# Announcement and propagation types
type AnnouncementID = str
"""Unique identifier for federated RPC announcements to prevent duplicates."""

type HopCount = int
"""Number of hops a federated announcement has traveled from its origin."""

type MaxHops = int
"""Maximum number of hops allowed before dropping a federated announcement."""

# Function collections
type FunctionNames = tuple[FunctionName, ...]
"""Immutable collection of function names provided by a server."""

type ResourceNames = tuple[ResourceName, ...]
"""Immutable collection of resource names associated with functions."""

type AdvertisedURLs = tuple[NodeURL, ...]
"""Immutable collection of URLs that a server advertises for connections."""

# Registry and tracking types
type AnnouncementRegistry = dict[AnnouncementID, Timestamp]
"""Registry tracking seen announcements with their timestamps for deduplication."""

type FunctionRegistry = dict[FunctionName, NodeURL]
"""Registry mapping function names to the nodes that provide them."""


# ============================================================================
# FEDERATED RPC DATACLASSES
# ============================================================================


@dataclass(frozen=True, slots=True)
class FederatedPropagationInfo:
    """
    Encapsulates federated RPC propagation metadata.

    This replaces the loose collection of primitive fields with a structured
    type that clearly represents the federated propagation state.
    """

    hop_count: HopCount
    max_hops: MaxHops
    announcement_id: AnnouncementID
    original_source: NodeURL

    @classmethod
    def create_initial(
        cls, original_source: NodeURL, max_hops: MaxHops = 3
    ) -> "FederatedPropagationInfo":
        """Create initial propagation info for a new announcement."""
        import time

        announcement_id: AnnouncementID = f"{original_source}:{time.time()}"
        return cls(
            hop_count=0,
            max_hops=max_hops,
            announcement_id=announcement_id,
            original_source=original_source,
        )

    def create_forwarded(self) -> "FederatedPropagationInfo":
        """Create propagation info for forwarding to next hop."""
        return FederatedPropagationInfo(
            hop_count=self.hop_count + 1,
            max_hops=self.max_hops,
            announcement_id=self.announcement_id,
            original_source=self.original_source,
        )

    @property
    def can_forward(self) -> bool:
        """Check if this announcement can be forwarded to another hop."""
        return self.hop_count < self.max_hops

    @property
    def is_initial_announcement(self) -> bool:
        """Check if this is the initial announcement (hop_count == 0)."""
        return self.hop_count == 0

    @property
    def is_federated(self) -> bool:
        """Check if this is a federated announcement (hop_count > 0)."""
        return self.hop_count > 0


@dataclass(frozen=True, slots=True)
class ServerCapabilities:
    """
    Encapsulates a server's advertised capabilities and functions.

    This replaces the primitive tuples with a structured type that clearly
    represents what a server can do.
    """

    functions: FunctionNames
    resources: ResourceNames
    cluster_id: ClusterID
    advertised_urls: AdvertisedURLs

    @classmethod
    def create(
        self,
        functions: tuple[str, ...],
        resources: tuple[str, ...],
        cluster_id: str,
        advertised_urls: tuple[str, ...] = (),
    ) -> "ServerCapabilities":
        """Create server capabilities from primitive types."""
        return ServerCapabilities(
            functions=tuple(f for f in functions),
            resources=tuple(r for r in resources),
            cluster_id=cluster_id,
            advertised_urls=tuple(url for url in advertised_urls),
        )

    @property
    def function_count(self) -> int:
        """Number of functions provided by this server."""
        return len(self.functions)

    @property
    def resource_count(self) -> int:
        """Number of resources associated with this server."""
        return len(self.resources)


# ============================================================================
# CROSS-DATACENTER FEDERATION DATACLASSES
# ============================================================================


@dataclass(frozen=True, slots=True)
class DatacenterConfig:
    """Configuration for a datacenter in cross-datacenter federation."""

    name: str
    nodes: int
    base_latency_ms: int
    region_code: str


@dataclass(frozen=True, slots=True)
class LatencyMapEntry:
    """Latency configuration between two datacenters."""

    from_region: str
    to_region: str
    latency_ms: int


# ============================================================================
# PARTITION SCENARIO DATACLASSES
# ============================================================================


@dataclass(frozen=True, slots=True)
class PartitionScenarioConfig:
    """Configuration for a single partition scenario."""

    name: str
    partitions: list[list[int]]
    isolation_time_ms: int
    expected_recovery_time_ms: int


@dataclass(frozen=True, slots=True)
class ResilientMeshConfig:
    """Configuration for resilient mesh testing."""

    cluster_size: int
    partition_scenarios: list[PartitionScenarioConfig]


# ============================================================================
# TOPOLOGY COMPARISON DATACLASSES
# ============================================================================


@dataclass(frozen=True, slots=True)
class TopologyComparisonConfig:
    """Configuration for topology comparison testing."""

    name: str
    description: str
    node_count: int
    topology_type: str
    expected_efficiency: float
    test_phases: list[str]


@dataclass(frozen=True, slots=True)
class TopologyComparisonResult:
    """Results from topology comparison testing."""

    name: str
    description: str
    node_count: int
    topology_type: str
    setup_time_ms: float
    performance_time_ms: float
    connection_efficiency: float
    propagation_success_rate: float
    avg_execution_time_ms: float
    total_connections: int
    expected_efficiency: float
    efficiency_ratio: float
    test_phases: list[str]


@dataclass(frozen=True, slots=True)
class PartitionScenarioResult:
    """Results from a partition scenario test."""

    name: str
    partitions: list[list[int]]
    partition_sizes: list[int]
    isolation_time_ms: int
    partition_time_ms: float
    recovery_time_ms: float
    scenario_total_time_ms: float
    pre_partition_connections: int
    post_recovery_connections: int
    connection_recovery_rate: float
    partition_propagation_rate: float
    post_recovery_propagation_rate: float
    propagation_recovery_rate: float
    recovery_connections_established: int


# ============================================================================
# HIERARCHICAL FEDERATION DATACLASSES
# ============================================================================


@dataclass(frozen=True, slots=True)
class RegionConfig:
    """Configuration for a single region in multi-regional testing."""

    name: str
    size: int


@dataclass(frozen=True, slots=True)
class HierarchicalTierConfig:
    """Configuration for a tier in hierarchical federation."""

    name: str
    regions: int
    nodes_per_region: int

    @property
    def total_nodes(self) -> int:
        """Calculate total nodes in this tier."""
        return self.regions * self.nodes_per_region


@dataclass(frozen=True, slots=True)
class HierarchicalRegionData:
    """Data for a region within a hierarchical tier."""

    name: str
    coordinator_port: int
    servers: list  # List of MPREGServer instances
    ports: list[int]


@dataclass(frozen=True, slots=True)
class HierarchicalTierData:
    """Data for a complete hierarchical tier."""

    name: str
    regions: list[HierarchicalRegionData]
    coordinators: list  # List of coordinator server instances

    @property
    def total_servers(self) -> int:
        """Total servers across all regions in this tier."""
        return sum(len(region.servers) for region in self.regions)


# ============================================================================
# PLANET-SCALE FEDERATION DATACLASSES
# ============================================================================


@dataclass(frozen=True, slots=True)
class ContinentalRegion:
    """Represents a regional cluster within a continental federation."""

    name: str
    nodes_per_region: int
    base_port: int
    vector_clock_sync: bool
    merkle_tree_verification: bool


@dataclass(frozen=True, slots=True)
class ContinentalConfig:
    """Configuration for a continental federation cluster."""

    name: str
    regions: tuple[str, ...]
    nodes_per_region: int
    base_port: int
    vector_clock_sync: bool
    merkle_tree_verification: bool

    @property
    def total_nodes(self) -> int:
        """Calculate total nodes in this continent."""
        return len(self.regions) * self.nodes_per_region


@dataclass(frozen=True, slots=True)
class PlanetScaleConfig:
    """Complete configuration for planet-scale federation."""

    continents: tuple[ContinentalConfig, ...]
    federation_features: tuple[str, ...]

    @property
    def total_nodes(self) -> int:
        """Calculate total nodes across all continents."""
        return sum(continent.total_nodes for continent in self.continents)

    @property
    def total_continents(self) -> int:
        """Total number of continents in the federation."""
        return len(self.continents)


@dataclass(frozen=True, slots=True)
class ContinentalBridge:
    """Represents a bridge between two continental leaders."""

    continent_a: str
    continent_b: str
    leader_a_name: str
    leader_b_name: str
    features_enabled: tuple[str, ...]
    establishment_time_ms: float


@dataclass(frozen=True, slots=True)
class VectorClockData:
    """Vector clock information for distributed coordination."""

    continent: str
    node_id: str
    logical_timestamp: int
    physical_timestamp: int


@dataclass(frozen=True, slots=True)
class MerkleVerification:
    """Merkle tree verification data for integrity checking."""

    data_hash: int
    tree_depth: int
    verification_path: str
    integrity_verified: bool


@dataclass(frozen=True, slots=True)
class PlanetScaleFunction:
    """Planet-scale function with distributed system integration."""

    name: str
    continent: str
    leader_name: str
    vector_clock: VectorClockData
    merkle_verification: MerkleVerification
    raft_consensus_ready: bool
    cache_coherence_level: str
    federation_hops: int


@dataclass(frozen=True, slots=True)
class PlanetScaleMetrics:
    """Comprehensive metrics for planet-scale federation."""

    vector_clocks_created: int
    merkle_trees_built: int
    raft_bridges_established: int
    cache_coherence_enabled: int


@dataclass(frozen=True, slots=True)
class CrossContinentalResult:
    """Results from cross-continental function execution."""

    function_name: str
    continent: str
    propagation_rate: float
    nodes_reached: int
    total_nodes: int

    @property
    def success_percentage(self) -> float:
        """Get propagation rate as percentage."""
        return self.propagation_rate * 100.0


@dataclass(frozen=True, slots=True)
class PlanetScaleResults:
    """Complete results from planet-scale federation benchmarking."""

    topology: str
    continents: int
    total_nodes: int
    continental_bridges: int
    setup_time_ms: float
    bridge_establishment_time_ms: float
    integration_time_ms: float
    cross_continental_time_ms: float
    connection_efficiency: float
    function_propagation_rate: float
    total_connections: int
    planet_metrics: PlanetScaleMetrics
    federation_features: tuple[str, ...]

    @property
    def efficiency_percentage(self) -> float:
        """Get connection efficiency as percentage."""
        return self.connection_efficiency * 100.0

    @property
    def propagation_percentage(self) -> float:
        """Get function propagation rate as percentage."""
        return self.function_propagation_rate * 100.0


@dataclass(slots=True)
class FederatedAnnouncementTracker:
    """
    Manages tracking of federated announcements for deduplication.

    This replaces the primitive dict[str, float] with a structured type
    that encapsulates the announcement tracking logic.
    """

    seen_announcements: AnnouncementRegistry = field(default_factory=dict)
    ttl_seconds: DurationSeconds = field(default=300.0)  # 5 minutes

    def has_seen(self, announcement_id: AnnouncementID) -> bool:
        """Check if we've already seen this announcement."""
        return announcement_id in self.seen_announcements

    def mark_seen(self, announcement_id: AnnouncementID, timestamp: Timestamp) -> None:
        """Mark an announcement as seen at the given timestamp."""
        # We need to modify the dict, so we create a new one
        new_registry = dict(self.seen_announcements)
        new_registry[announcement_id] = timestamp
        self.seen_announcements = new_registry

    def cleanup_expired(self, current_time: Timestamp) -> int:
        """Remove expired announcements and return the number removed."""
        expired_ids = [
            aid
            for aid, timestamp in self.seen_announcements.items()
            if current_time - timestamp > self.ttl_seconds
        ]

        if expired_ids:
            new_registry = {
                aid: timestamp
                for aid, timestamp in self.seen_announcements.items()
                if aid not in expired_ids
            }
            self.seen_announcements = new_registry

        return len(expired_ids)

    @property
    def announcement_count(self) -> int:
        """Number of announcements currently being tracked."""
        return len(self.seen_announcements)


@dataclass(frozen=True, slots=True)
class FederatedRPCAnnouncement:
    """
    Complete federated RPC announcement combining server capabilities and propagation info.

    This is the high-level interface that combines all the federated RPC data
    into a single, well-typed structure.
    """

    capabilities: ServerCapabilities
    propagation: FederatedPropagationInfo

    @classmethod
    def create_initial(
        cls,
        functions: tuple[str, ...],
        resources: tuple[str, ...],
        cluster_id: str,
        original_source: str,
        advertised_urls: tuple[str, ...] = (),
        max_hops: int = 3,
    ) -> "FederatedRPCAnnouncement":
        """Create an initial federated RPC announcement."""
        capabilities = ServerCapabilities.create(
            functions, resources, cluster_id, advertised_urls
        )
        propagation = FederatedPropagationInfo.create_initial(original_source, max_hops)
        return cls(capabilities=capabilities, propagation=propagation)

    def create_forwarded(self) -> "FederatedRPCAnnouncement":
        """Create a forwarded version of this announcement for the next hop."""
        return FederatedRPCAnnouncement(
            capabilities=self.capabilities,
            propagation=self.propagation.create_forwarded(),
        )

    @property
    def can_forward(self) -> bool:
        """Check if this announcement can be forwarded."""
        return self.propagation.can_forward

    def is_loop_back(self, local_node: NodeURL) -> bool:
        """Check if forwarding this would create a loop back to the original source."""
        return self.propagation.original_source == local_node

    def should_process(
        self, local_node: NodeURL, tracker: FederatedAnnouncementTracker
    ) -> bool:
        """
        Check if this announcement should be processed by the local node.

        Returns False if:
        - Already seen this announcement (deduplication)
        - Exceeded hop limit
        - Would create a loop back to original source (but not for initial announcements)
        """
        # For initial announcements (hop_count=0), allow the server to broadcast its own functions
        if self.propagation.is_initial_announcement:
            return (
                not tracker.has_seen(self.propagation.announcement_id)
                and self.propagation.can_forward
            )

        # For forwarded announcements, include loop detection
        return (
            not tracker.has_seen(self.propagation.announcement_id)
            and self.propagation.can_forward
            and not self.is_loop_back(local_node)
        )


# ============================================================================
# TYPE CONVERSION UTILITIES
# ============================================================================


def create_federated_propagation_from_primitives(
    hop_count: int, max_hops: int, announcement_id: str, original_source: str
) -> FederatedPropagationInfo:
    """Convert primitive values to FederatedPropagationInfo."""
    return FederatedPropagationInfo(
        hop_count=hop_count,
        max_hops=max_hops,
        announcement_id=announcement_id,
        original_source=original_source,
    )


def create_server_capabilities_from_primitives(
    functions: tuple[str, ...],
    resources: tuple[str, ...],
    cluster_id: str,
    advertised_urls: tuple[str, ...] = (),
) -> ServerCapabilities:
    """Convert primitive values to ServerCapabilities."""
    return ServerCapabilities.create(functions, resources, cluster_id, advertised_urls)
