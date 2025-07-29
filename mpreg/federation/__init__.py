"""
MPREG Federation Module

Planet-scale federation system with graph-based routing, hub architecture,
gossip protocol, distributed state management, and failure detection.
"""

# Graph-based routing (known to work)
# Distributed consensus (known to work)
from .federation_consensus import (
    ConflictResolver,
    ConsensusManager,
    ConsensusProposal,
    StateConflict,
    StateType,
    StateValue,
)

# Gossip protocol (known to work)
from .federation_gossip import (
    GossipFilter,
    GossipMessage,
    GossipMessageType,
    GossipProtocol,
    GossipScheduler,
    GossipStrategy,
    VectorClock,
)
from .federation_graph import (
    DijkstraRouter,
    FederationGraph,
    GeographicAStarRouter,
    GeographicCoordinate,
    GraphBasedFederationRouter,
    MultiPathRouter,
)

# Hub architecture (known to work)
from .federation_hubs import (
    AggregatedSubscriptionState,
    FederationHub,
    GlobalHub,
    HubCapabilities,
    HubTier,
    HubTopology,
    LocalHub,
    RegionalHub,
    TopicBloomFilter,
)

# Membership and failure detection (known to work)
from .federation_membership import (
    MembershipEvent,
    MembershipInfo,
    MembershipProtocol,
    MembershipState,
    ProbeRequest,
)

# Optimized federation (known to work)
from .federation_optimized import (
    ClusterIdentity,
)

# Hub registry and discovery (known to work)
from .federation_registry import (
    ClusterRegistrar,
    HubHealthMonitor,
    HubRegistrationInfo,
    HubRegistry,
)

__all__ = [
    # Graph-based routing
    "FederationGraph",
    "GraphBasedFederationRouter",
    "GeographicCoordinate",
    "DijkstraRouter",
    "MultiPathRouter",
    "GeographicAStarRouter",
    # Hub architecture
    "FederationHub",
    "LocalHub",
    "RegionalHub",
    "GlobalHub",
    "HubTopology",
    "HubTier",
    "HubCapabilities",
    "AggregatedSubscriptionState",
    "TopicBloomFilter",
    # Hub registry
    "HubRegistry",
    "ClusterRegistrar",
    "HubHealthMonitor",
    "HubRegistrationInfo",
    # Gossip protocol
    "GossipProtocol",
    "GossipMessage",
    "GossipMessageType",
    "VectorClock",
    "GossipStrategy",
    "GossipScheduler",
    "GossipFilter",
    # Distributed consensus
    "ConsensusManager",
    "StateValue",
    "StateType",
    "ConflictResolver",
    "StateConflict",
    "ConsensusProposal",
    # Membership
    "MembershipProtocol",
    "MembershipInfo",
    "MembershipState",
    "ProbeRequest",
    "MembershipEvent",
    # Optimized federation
    "ClusterIdentity",
]
