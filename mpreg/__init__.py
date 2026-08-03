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

- Sub-millisecond routing for large node graphs
- Comprehensive unit, integration, and property-based tests
- Production-oriented deployment guides and examples

"""

from __future__ import annotations

# Core exports
# Client exports
from .client import Client, MPREGClientAPI, MPREGDnsClient, MPREGPubSubClient
from .client.unified_client import MPREGClient, UnifiedMPREGClient
from .core import (
    JsonSerializer,
    MPREGSettings,
    RpcRegistry,
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

# Version info — single source is pyproject.toml [project].version
from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("mpreg")
except PackageNotFoundError:  # pragma: no cover - editable/source tree fallback
    __version__ = "0.2.0"

__author__ = "Matt Stancliff"
__license__ = "Apache-2.0"

from .client.cluster_client import MPREGClusterClient
from .core.port_allocator import allocate_port
from .server import MPREGServer

__all__ = [
    # Core
    "RpcRegistry",
    "JsonSerializer",
    "MPREGSettings",
    "TopicExchange",
    "allocate_port",
    "MPREGServer",
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
    "MPREGClient",
    "UnifiedMPREGClient",
    "MPREGClusterClient",
    "MPREGDnsClient",
    "MPREGPubSubClient",
    # Meta
    "__version__",
]
