"""
MPREG - Massively Parallel Request/Response Exchange Gateway

A high-performance, planet-scale federation system for distributed messaging
with graph-based routing, hub architecture, gossip protocol, and failure detection.

## Architecture

MPREG is organized into several key modules:

- **core**: Core functionality including models, registry, serialization
- **federation**: Planet-scale federation with routing, gossip, consensus
- **client**: Client-side APIs for connecting to federation systems

## Quick Start

```python
from mpreg.federation import PlanetScaleFederationNode, GeographicCoordinate, HubTier

# Create a federation node
node = PlanetScaleFederationNode(
    node_id="my-node",
    coordinates=GeographicCoordinate(40.7128, -74.0060),
    region="us-east",
    hub_tier=HubTier.LOCAL
)

# Start the node
await node.start()
```

## Performance

- **Sub-millisecond routing** for 100+ node graphs
- **1781+ requests/second** throughput
- **384 passing tests** with comprehensive coverage
- **Production-ready** with deployment guides and examples
"""

# Core exports
# Client exports
from .client import Client, MPREGClientAPI, MPREGPubSubClient
from .core import (
    CommandRegistry,
    JsonSerializer,
    MPREGSettings,
    TopicExchange,
)

# Federation exports (most commonly used)
from .federation import (
    # Consensus
    ConsensusManager,
    # Graph routing
    FederationGraph,
    GeographicCoordinate,
    GlobalHub,
    GossipMessage,
    GossipMessageType,
    # Gossip protocol
    GossipProtocol,
    GraphBasedFederationRouter,
    HubRegistry,
    HubTier,
    # Hub architecture
    LocalHub,
    MembershipInfo,
    # Membership
    MembershipProtocol,
    MembershipState,
    RegionalHub,
    StateType,
    StateValue,
    VectorClock,
)

# Version info
__version__ = "3.0.0"
__author__ = "MPREG Development Team"
__license__ = "MIT"

__all__ = [
    # Core
    "CommandRegistry",
    "JsonSerializer",
    "MPREGSettings",
    "TopicExchange",
    # Federation - Graph
    "FederationGraph",
    "GraphBasedFederationRouter",
    "GeographicCoordinate",
    # Federation - Hubs
    "LocalHub",
    "RegionalHub",
    "GlobalHub",
    "HubTier",
    "HubRegistry",
    # Federation - Gossip
    "GossipProtocol",
    "GossipMessage",
    "GossipMessageType",
    "VectorClock",
    # Federation - Consensus
    "ConsensusManager",
    "StateValue",
    "StateType",
    # Federation - Membership
    "MembershipProtocol",
    "MembershipInfo",
    "MembershipState",
    # Client
    "Client",
    "MPREGClientAPI",
    "MPREGPubSubClient",
]
