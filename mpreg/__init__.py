"""
MPREG - Massively Parallel Request/Response Exchange Gateway

A high-performance fabric messaging platform with graph-assisted routing,
gossip-based discovery, and CFT Raft consensus (not BFT). Prefer profiles
and ``MPREGClient`` for four-plane (RPC/pubsub/queue/cache) usage.

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

# Version info — single source is pyproject.toml [project].version
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

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

try:
    __version__ = _pkg_version("mpreg")
except PackageNotFoundError:  # pragma: no cover - editable/source tree fallback
    __version__ = "0.3.0"

__author__ = "Matt Stancliff"
__license__ = "Apache-2.0"

from .client.cluster_client import MPREGClusterClient
from .core.port_allocator import allocate_port
from .server import MPREGServer

__all__ = [
    # Client
    "Client",
    # Fabric - Consensus
    "ConsensusManager",
    # Fabric - Graph
    "FederationGraph",
    "GeographicCoordinate",
    "GlobalHub",
    "GossipMessage",
    "GossipMessageType",
    # Fabric - Gossip
    "GossipProtocol",
    "GossipStrategy",
    "GraphBasedFederationRouter",
    "HubRegistry",
    "HubTier",
    "JsonSerializer",
    # Fabric - Hubs
    "LocalHub",
    "MPREGClient",
    "MPREGClientAPI",
    "MPREGClusterClient",
    "MPREGDnsClient",
    "MPREGPubSubClient",
    "MPREGServer",
    "MPREGSettings",
    "MembershipInfo",
    # Fabric - Membership
    "MembershipProtocol",
    "MembershipState",
    "RegionalHub",
    # Core
    "RpcRegistry",
    "StateType",
    "StateValue",
    "TopicExchange",
    "UnifiedMPREGClient",
    "VectorClock",
    # Meta
    "__version__",
    "allocate_port",
]
