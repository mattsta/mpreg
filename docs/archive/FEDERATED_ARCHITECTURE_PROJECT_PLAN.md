# MPREG Platform Improvements: GOODBYE Protocol + Example Fixes

NOTE
This document is historical. GOODBYE protocol work is complete and tracked in
`docs/UNIFIED_UNIFICATION_PLAN.md`. The planet-scale example refactor remains open
and should be re-scoped against the unified fabric architecture.

## Executive Summary

This project has TWO distinct parts:

**PART 1: GOODBYE Protocol (Platform Infrastructure)**
Add GOODBYE messaging to the core MPREG platform at all levels (node, cluster, federation, gossip, connection). This is fundamental infrastructure that all MPREG applications will benefit from.

**PART 2: Planet Scale Example Fix (Demonstration Code)**
Fix the current planet scale integration example to properly demonstrate federated star topology instead of the current full mesh masquerading as star topology.
Run via: uv run python mpreg/examples/tier3_full_system_expansion.py > scale_example.log 2>&1

## Problem Analysis

### Current Issues

1. **False Star Topology**: Claims star topology but creates full mesh (25 nodes = 600 potential connections instead of 24)
2. **Gossip-Driven Full Mesh**: All nodes discover each other via gossip and attempt connections to everyone
3. **No Graceful Departure**: Nodes simply disappear, causing 96+ connection retry attempts
4. **Scale Issues**: Connection storms during startup and shutdown
5. **Missing Federation Logic**: Not using the built-in federated clustering capabilities

### Root Cause

The example conflates intra-cluster networking (star topology within a cluster) with inter-cluster federation (multiple clusters connected via bridges).

## Proposed Architecture

### Federated Cluster Design

```
Fabric Routing Plane (Global Hub)
├── Regional Cluster: US-East (3-5 nodes)
├── Regional Cluster: US-West (3-5 nodes)
├── Regional Cluster: EU-West (3-5 nodes)
├── Regional Cluster: EU-Central (3-5 nodes)
├── Regional Cluster: Asia-East (3-5 nodes)
└── Regional Cluster: Asia-Southeast (3-5 nodes)
```

**Key Principles:**

- Each regional cluster operates independently with its own consensus
- Federation bridge coordinates cross-cluster communication
- True star topology: N clusters connect to 1 federation bridge
- Proper cluster membership with join/leave semantics

## Implementation Plan

## PART 1: GOODBYE Protocol (Platform Infrastructure)

### Phase 1: Core GOODBYE Message Types (HIGH PRIORITY)

#### 1.1 Add GOODBYE to Core Model Types

**Location**: `mpreg/core/model.py`

```python
class RPCServerGoodbye(BaseModel):
    """Server goodbye message for graceful departure from cluster."""

    what: Literal["GOODBYE"] = "GOODBYE"
    departing_node_url: str = Field(description="URL of the departing node")
    cluster_id: str = Field(description="Cluster the node is leaving")
    reason: GoodbyeReason = Field(description="Reason for departure")
    timestamp: float = Field(default_factory=time.time)
    # Optional: Add signature for authenticated departures

class GoodbyeReason(Enum):
    GRACEFUL_SHUTDOWN = "graceful_shutdown"
    MAINTENANCE = "maintenance"
    CLUSTER_REBALANCE = "cluster_rebalance"
    MANUAL_REMOVAL = "manual_removal"

# Update RPCServerMessage union type (HELLO removed; catalog handles discovery)
type RPCServerMessage = (
    RPCServerGoodbye
    | RPCServerStatus
)
```

#### 1.2 Server-Level GOODBYE Handling

**Location**: `mpreg/server.py`

Add GOODBYE processing to the core server:

```python
class MPREGServer:
    async def _handle_goodbye_message(self, goodbye: RPCServerGoodbye, sender_url: str) -> None:
        """Handle GOODBYE message from departing peer."""
        logger.info(f"[{self.settings.name}] Received GOODBYE from {goodbye.departing_node_url} (reason: {goodbye.reason.value})")

        # Remove from cluster immediately
        await self.cluster.remove_peer(goodbye.departing_node_url)

        # Close connection to departing node
        if goodbye.departing_node_url in self.peer_connections:
            await self.peer_connections[goodbye.departing_node_url].disconnect()
            del self.peer_connections[goodbye.departing_node_url]

        # Propagate GOODBYE via gossip
        await self._propagate_goodbye_via_gossip(goodbye)

    async def send_goodbye(self, reason: GoodbyeReason) -> None:
        """Send GOODBYE to all peers before shutdown."""
        goodbye = RPCServerGoodbye(
            departing_node_url=f"ws://{self.settings.host}:{self.settings.port}",
            cluster_id=self.settings.cluster_id,
            reason=reason
        )

        goodbye_request = RPCServerRequest(
            server=goodbye,
            u=str(ulid.new())
        )

        # Broadcast to all connected peers
        message_bytes = self.serializer.serialize(goodbye_request.model_dump())
        for peer_url, connection in list(self.peer_connections.items()):
            try:
                await connection.send(message_bytes)
                logger.info(f"[{self.settings.name}] Sent GOODBYE to {peer_url}")
            except Exception as e:
                logger.warning(f"[{self.settings.name}] Failed to send GOODBYE to {peer_url}: {e}")
```

#### 1.3 Connection Handling with GOODBYE

- GOODBYE is ONLY sent manually by the departing node itself
- Failed connections do NOT trigger automatic node removal
- Connection retries continue until manual GOODBYE is received
- Node removal happens ONLY when explicit GOODBYE is processed

### Phase 2: Gossip Integration (HIGH PRIORITY)

**Location**: Update gossip protocol to handle GOODBYE propagation

#### 2.1 Cluster-Level GOODBYE Processing

**Location**: `mpreg/server.py` - Cluster class

```python
class Cluster:
    async def remove_peer(self, peer_url: str) -> bool:
        """Remove peer from cluster with proper cleanup."""
        if peer_url not in self.peers_info:
            return False

        # Remove from peers_info
        peer_info = self.peers_info.pop(peer_url)
        logger.info(f"Removed peer {peer_url} from cluster")

        # Remove from function map
        self._remove_peer_functions(peer_url)

        # Publish peer removed event
        # (Integration with existing event system)

        return True

    def _remove_peer_functions(self, peer_url: str) -> None:
        """Remove all functions provided by departing peer."""
        for function_name, providers in list(self.function_map.items()):
            if peer_url in providers:
                providers.discard(peer_url)
                if not providers:  # No more providers
                    del self.function_map[function_name]
                    logger.info(f"Function {function_name} no longer available after {peer_url} departure")
```

### Phase 3: Shutdown Integration (HIGH PRIORITY)

**Location**: Update server shutdown to send GOODBYE

```python
# In MPREGServer.shutdown_async()
async def shutdown_async(self) -> None:
    """Complete async shutdown with GOODBYE protocol."""
    logger.info(f"[{self.settings.name}] Starting shutdown with GOODBYE...")

    # Send GOODBYE to all peers FIRST
    await self.send_goodbye(GoodbyeReason.GRACEFUL_SHUTDOWN)

    # Brief pause to let GOODBYE propagate
    await asyncio.sleep(0.5)

    # Continue with existing shutdown logic
    self._shutdown_event.set()
    # ... rest of shutdown
```

## PART 2: Planet Scale Example Fix (Demonstration Code)

### Phase 4: Example Architecture Fix (MEDIUM PRIORITY)

#### 2.1 Fabric Routing Plane Implementation

```python
class FabricRoutingPlane:
    """Central coordinator for fabric-based cluster routing."""

    def __init__(self, fabric_id: str, supported_regions: list[str]):
        self.fabric_id = fabric_id
        self.regional_clusters: dict[str, ClusterProxy] = {}
        self.cross_cluster_router = CrossClusterRouter()
        self.fabric_gossip = GossipProtocol()

    async def register_cluster(self, cluster_info: RegionalClusterInfo) -> bool:
        """Register a regional cluster with the federation bridge."""

    async def route_cross_cluster_message(
        self, message: FederatedMessage, target_cluster: str
    ) -> None:
        """Route messages between clusters via federation."""
```

#### 2.2 Regional Cluster Design

Each regional cluster:

- 3-5 nodes in true star topology (1 cluster leader + spokes)
- Independent consensus within cluster
- Single federation connection to bridge
- Local gossip protocol for intra-cluster communication

#### 2.3 Cross-Cluster Communication

- Fabric routing plane routes messages between clusters
- Cross-cluster RPC with proper routing headers
- Federation-aware topic exchange for pub/sub
- Cluster-local vs federation-wide resource resolution

### Phase 3: Planet Scale Example Refactor (MEDIUM PRIORITY)

#### 3.1 Architecture Changes

Replace current single-cluster design with:

```python
class PlanetScaleFederationDemo:
    def __init__(self):
        self.fabric_routing_plane = FabricRoutingPlane("global-fabric")
        self.regional_clusters: dict[str, RegionalCluster] = {}

    async def create_regional_clusters(self) -> None:
        """Create 6 regional clusters instead of 25 individual nodes."""
        regions = ["us-east", "us-west", "eu-west", "eu-central", "asia-east", "asia-southeast"]

        for region in regions:
            cluster = RegionalCluster(
                region_id=region,
                node_count=4,  # 1 leader + 3 spokes
                fabric_router_url=self.fabric_routing_plane.get_url()
            )
            await cluster.start()
            await self.fabric_routing_plane.register_cluster(cluster.get_info())
            self.regional_clusters[region] = cluster
```

#### 3.2 Demonstration Scenarios

1. **Cross-Cluster Consensus**: Propose state changes that span multiple regions
2. **Graceful Cluster Departure**: Demonstrate GOODBYE protocol with one cluster leaving
3. **Fabric Routing Plane Failover**: Show resilience when fabric routing temporarily fails
4. **Cross-Region Load Balancing**: Route requests based on cluster capacity
5. **Cascading Failure Recovery**: One cluster fails, others continue operating

### Phase 4: Enhanced Connection Management (MEDIUM PRIORITY)

#### 4.1 Improved Connection Retry Logic

```python
class ConnectionManager:
    def __init__(self):
        self.connection_state: dict[str, ConnectionHealth] = {}
        self.backoff_strategy = ExponentialBackoffStrategy()

    async def attempt_connection(self, peer_url: str) -> ConnectionResult:
        """Attempt connection with adaptive backoff (but never auto-remove nodes)."""

    async def handle_connection_failure(self, peer_url: str, error: Exception) -> None:
        """Handle failed connections with backoff but keep trying until GOODBYE."""
```

#### 4.2 Connection Retry Strategy

- Exponential backoff for failed connection attempts
- Adaptive timeout based on network conditions
- Continue retrying indefinitely until GOODBYE received
- NO automatic node removal based on connection failures

### Phase 5: Membership Management (MEDIUM PRIORITY)

#### 5.1 Cluster Join Protocol

```python
class ClusterMembershipManager:
    async def join_cluster(self, cluster_id: str, node_info: NodeInfo) -> JoinResult:
        """Handle new node joining cluster with proper validation."""

    async def leave_cluster(self, reason: GoodbyeReason) -> LeaveResult:
        """Gracefully leave cluster with GOODBYE protocol."""

    # NO automatic node failure handling - only manual GOODBYE processing
```

#### 5.2 Manual Cluster Management

- Manual cluster leader election/assignment
- Manual cluster rebalancing operations
- Manual cluster splitting/merging
- Manual federation bridge capacity management
- All changes require explicit GOODBYE and catalog updates for node movement

### Phase 6: Validation and Testing (LOW PRIORITY)

#### 6.1 Comprehensive Test Suite

1. **Unit Tests**: GOODBYE protocol, federation routing, failure detection
2. **Integration Tests**: Cross-cluster scenarios, failover testing
3. **Performance Tests**: Scalability with varying cluster sizes
4. **Chaos Testing**: Random node failures, network partitions
5. **Long-Running Tests**: Multi-hour stability validation

#### 6.2 Metrics and Observability

- Federation bridge metrics (cross-cluster message rates, latency)
- Cluster health metrics (membership stability, consensus performance)
- GOODBYE protocol metrics (departure success rate, gossip propagation time)
- Connection management metrics (retry rates, backoff effectiveness)

## Technical Specifications

### GOODBYE Message Flow

```
1. Node initiates departure
   └── Broadcasts GOODBYE to cluster members

2. Cluster consensus
   ├── Members vote on GOODBYE acceptance
   ├── Majority required for confirmation
   └── Timeout handling for non-responsive members

3. Gossip propagation
   ├── Confirmed GOODBYE enters gossip protocol
   ├── Spreads to all cluster members
   └── Eventually propagates to federation bridge

4. Node removal
   ├── Remove from active peer lists
   ├── Stop advertising in function maps
   ├── Close all connections to departing node
   └── Update cluster membership records

5. Confirmation
   ├── All nodes acknowledge removal
   ├── GOODBYE removed from gossip state
   └── Departure process complete
```

### Federation Message Routing

```
Source Cluster → Fabric Routing Plane → Target Cluster
     ↓                    ↓                 ↓
  Local Node         Route Decision     Local Node
     ↓                    ↓                 ↓
  Serialize           Add Routing       Deserialize
     ↓                Headers              ↓
  Send via                ↓            Execute RPC
  Fed Connection      Forward to          ↓
                    Target Cluster    Return Result
```

## CURRENT STATUS - January 2025

### ✅ COMPLETED: GOODBYE Protocol Implementation (PART 1)

**Phase 1: Core GOODBYE Message Types - COMPLETED**

- ✅ Added `GoodbyeReason` enum with 4 reason types (graceful_shutdown, maintenance, cluster_rebalance, manual_removal)
- ✅ Added `RPCServerGoodbye` message type to core model with proper Pydantic validation
- ✅ Updated `RPCServerMessage` union type to include GOODBYE messages
- ✅ Integrated GOODBYE handling in server message routing (`run_server` method)

**Phase 2: Gossip Integration - COMPLETED**

- ✅ Implemented `remove_peer` method with proper function registry cleanup
- ✅ Added departed peers tracking system (`_departed_peers` dict with timestamps) to prevent race conditions
- ✅ Modified `process_gossip_message` to skip departed peers and prevent re-adding via gossip
- ✅ Fixed critical race condition where gossip would restore removed peers
- ✅ Added temporal cleanup of departed peers (5-minute timeout) to prevent memory leaks

**Phase 3: Shutdown Integration - COMPLETED**

- ✅ Implemented `_handle_goodbye_message` for processing incoming GOODBYEs
- ✅ Implemented `send_goodbye` method for broadcasting GOODBYE to all peers
- ✅ Integrated GOODBYE sending into `shutdown_async` process
- ✅ Added connection cleanup when receiving GOODBYE messages

**Phase 4: Immediate Re-entry Support - COMPLETED**

- ✅ Fixed critical distinction: GOODBYE blocklist only blocks gossip, NOT direct status-driven reconnections
- ✅ Implemented status-driven departed peer clearing when nodes reconnect
- ✅ Added `_broadcast_peer_reentry` method for propagating re-entry via gossip
- ✅ Created re-entry broadcast system where catalog updates act as "new node broadcast event"
- ✅ All nodes clear departed peers when ANY node receives new status/catelog update from departed peer
- ✅ Fixed asyncio task handling in re-entry broadcast (proper task scheduling)

**Testing & Validation - COMPLETED**

- ✅ Created comprehensive test suite (`tests/test_goodbye_protocol.py`) with 9 test methods
- ✅ Added immediate re-entry test suite (`tests/test_goodbye_immediate_reentry.py`) with 4 test methods
- ✅ Fixed timing issues and race conditions in tests
- ✅ Verified GOODBYE protocol works correctly in 2-node and 3-node clusters
- ✅ Tested all GOODBYE reason types and edge cases
- ✅ Confirmed gossip blocklist prevents departed peer re-addition
- ✅ Verified immediate reconnection works despite being in departed peers list
- ✅ Tested re-entry broadcast propagation across multi-node clusters

**Key Implementation Features:**

- **Manual-only departure**: Nodes only removed on explicit GOODBYE (no auto-removal)
- **Temporal blocklist**: Prevents gossip from re-adding departed peers with 5-minute cleanup
- **Immediate re-entry**: Departed peers can immediately reconnect via status + catalog updates
- **Re-entry broadcast**: Catalog updates from departed peer trigger gossip broadcast to clear all blocklists
- **Graceful shutdown**: GOODBYE automatically sent during shutdown
- **Complete cleanup**: Connections and function registrations properly removed
- **Race condition prevention**: Gossip integration prevents conflicts
- **Connection retry storm prevention**: Connection management respects GOODBYE blocklist
- **Memory leak prevention**: Temporal cleanup of departed peers prevents unbounded growth

### ✅ COMPLETED: Planet Scale Example Fixes (PART 2)

**Completed Goals:**

1. ✅ **Updated connection management** to leverage GOODBYE protocol (prevent retry storms)
2. ✅ **Fixed planet scale example** to use proper federated star topology
3. ✅ **Refactored architecture** from 25-node full mesh to N clusters + 1 federation bridge

**Key Achievements:**

- **True Federated Architecture**: 6 independent regional clusters + 1 federation bridge
- **Separate Cluster IDs**: Each region has its own cluster_id (cluster-us-east, cluster-eu-west, etc.)
- **Independent Consensus**: Each regional cluster operates with separate consensus
- **Proper Connection Topology**: 24 total connections (18 intra-cluster + 6 inter-cluster)
- **GOODBYE Integration**: Graceful shutdown prevents connection retry storms
- **Fabric Routing Plane**: Global hub acts as federation coordinator, not cluster member

## Implementation Timeline

### Week 1-2: Foundation ✅ COMPLETED

- ✅ Design and implement GOODBYE message protocol
- ✅ Add gossip integration for GOODBYE propagation
- ✅ Create comprehensive test suite

### Week 3-4: Federation Architecture ✅ COMPLETED

- ✅ Update connection management to handle GOODBYE (prevent auto-removal)
- ✅ Fix planet scale example to use proper federated star topology
- ✅ Refactor: N clusters + 1 federation bridge instead of full mesh

### Week 5-6: Example Enhancement (PENDING)

- [ ] Implement demonstration scenarios with GOODBYE protocol
- [ ] Add comprehensive logging and metrics
- [ ] Performance testing with proper federation architecture

### Week 7-8: Polish and Validation (PENDING)

- [ ] Long-running stability tests
- [ ] Performance optimization
- [ ] Documentation updates

## Success Criteria

1. **No More Connection Storms**: Clean shutdown with zero retry attempts after GOODBYE
2. **True Star Topology**: 6 clusters + 1 bridge = 6 connections (not 600)
3. **Graceful Departures**: GOODBYE protocol works reliably with majority consensus
4. **Federation Functionality**: Cross-cluster RPC and pub/sub working seamlessly
5. **Scalability**: Can handle 50+ clusters without performance degradation
6. **Resilience**: Single cluster failure doesn't affect others

## Risk Mitigation

- **Backward Compatibility**: Maintain existing MPREG server API
- **Incremental Rollout**: Implement changes in phases with testing
- **Fallback Mechanisms**: Graceful degradation if federation bridge fails
- **Performance Monitoring**: Continuous performance validation during development

This project plan transforms the current flawed example into a true federated architecture showcase while adding essential node lifecycle management that the entire MPREG ecosystem needs.
