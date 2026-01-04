"""
MPREG - Massively Parallel Request/Response Exchange Gateway

A high-performance, planet-scale fabric system for distributed messaging
with graph-based routing, gossip-based discovery, and failure detection.

## Architecture

MPREG is organized into several key modules:

- **core**: Core functionality including models, registry, serialization
- **fabric**: Unified routing/control plane with gossip, routing, and consensus
- **client**: Client-side APIs for connecting to federation systems

## Quick Start

```python
from mpreg.core import MPREGSettings
from mpreg.core.port_allocator import allocate_port
from mpreg.server import MPREGServer

server_port = allocate_port("servers")
server = MPREGServer(
    MPREGSettings(
        host="127.0.0.1",
        port=server_port,
        name="example",
        cluster_id="cluster-a",
    )
)
print(f"MPREG_URL=ws://127.0.0.1:{server_port}")
await server.server()
```

## Performance

- **Sub-millisecond routing** for 100+ node graphs
- **1781+ requests/second** throughput
- **384 passing tests** with comprehensive coverage
- **Production-ready** with deployment guides and examples
"""

from __future__ import annotations

# Core exports
# Client exports
from .client import Client, MPREGClientAPI, MPREGPubSubClient
from .core import (
    CommandRegistry,
    JsonSerializer,
    MPREGSettings,
    TopicExchange,
)

# Fabric exports (most commonly used)
from .fabric import (
    FederationGraph,
    GeographicCoordinate,
    GraphBasedFederationRouter,
)
from .fabric.consensus import ConsensusManager, StateType, StateValue
from .fabric.gossip import (
    GossipMessage,
    GossipMessageType,
    GossipProtocol,
    GossipStrategy,
    VectorClock,
)
from .fabric.hub_registry import HubRegistry
from .fabric.hubs import (
    GlobalHub,
    HubTier,
    LocalHub,
    RegionalHub,
)
from .fabric.membership import (
    MembershipInfo,
    MembershipProtocol,
    MembershipState,
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
    # Fabric - Graph
    "FederationGraph",
    "GraphBasedFederationRouter",
    "GeographicCoordinate",
    # Fabric - Hubs
    "LocalHub",
    "RegionalHub",
    "GlobalHub",
    "HubTier",
    "HubRegistry",
    # Fabric - Gossip
    "GossipProtocol",
    "GossipMessage",
    "GossipMessageType",
    "GossipStrategy",
    "VectorClock",
    # Fabric - Consensus
    "ConsensusManager",
    "StateValue",
    "StateType",
    # Fabric - Membership
    "MembershipProtocol",
    "MembershipInfo",
    "MembershipState",
    # Client
    "Client",
    "MPREGClientAPI",
    "MPREGPubSubClient",
]
