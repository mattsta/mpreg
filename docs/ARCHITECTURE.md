# MPREG Architecture Documentation

## Overview

MPREG is a unified distributed computing platform. RPC, pub/sub, queues, and
cache all share one control plane, one routing catalog, and one message envelope.
The system is designed for clean interfaces, minimal duplication, and predictable
operations at scale.

At a glance:

```
+-------------------------------+
| Application Systems           |
| RPC / PubSub / Queue / Cache  |
+-------------------------------+
| Fabric Routing + Policy       |
| RoutingCatalog + RouteTable   |
+-------------------------------+
| Fabric Gossip Control Plane   |
| Catalog/Route/Membership      |
+-------------------------------+
| UnifiedMessage Envelope       |
| FabricMessage + Headers       |
+-------------------------------+
| Transport (WebSocket/TCP/TLS) |
+-------------------------------+
```

## Design Goals

- **Single fabric**: one canonical control plane and routing envelope.
- **Strong typing**: no opaque tuple keys; explicit identities everywhere.
- **Self-managing components**: subsystems own their lifecycle and cleanup.
- **Performance-first**: minimal layers on the hot path.
- **Observability**: structured logs, module-filtered debug output, and metrics.
- **Testability**: unit + integration + live topology tests on every subsystem.

## Core Subsystems

### 1) Fabric Control Plane

The control plane is the source of truth for discovery and routing.

- **RoutingCatalog** (`mpreg/fabric/catalog.py`): Stores function endpoints,
  queue/topic registrations, cache endpoints, and node metadata (including
  advertised transport endpoints for client/internal connections).
- **RoutingIndex** (`mpreg/fabric/index.py`): Query surface for fast lookup.
- **GossipProtocol** (`mpreg/fabric/gossip.py`): Distributes catalog deltas,
  route announcements, membership updates, and consensus signals.
- **Route Control** (`mpreg/fabric/route_control.py`): Path-vector route table
  for cross-cluster routing with TTL, policy weights, withdrawals, and
  deterministic tie-breakers.
- **Route Security (optional)** (`mpreg/fabric/route_keys.py`,
  `mpreg/fabric/route_security.py`): Signed announcements + key registry with
  rotation overlap, enforced via `RouteSecurityConfig`.
- **Neighbor Policy Directory (optional)** (`mpreg/fabric/route_policy_directory.py`):
  Per-neighbor import policy overrides with default fallback.
- **Link-State Control (optional)** (`mpreg/fabric/link_state.py`): Gossips
  adjacency updates and builds a shortest-path graph when enabled.
  See `docs/FABRIC_LINK_STATE_ROUTING.md` for usage guidance.

Why this design:

- **Benefit**: One shared catalog eliminates duplicate discovery paths.
- **Drawback**: Eventually consistent state; must handle TTL staleness.
- **Optional add-on**: Link-state mode adds a global topology view for stable,
  predictable routing at larger scales, at the cost of extra control-plane
  traffic.
- **Optional add-on**: Signed route announcements + key registry prevent spoofing
  and allow controlled key rotation when security policies require it.

### 2) Unified Routing + Envelope

All node-to-node traffic is carried in a single envelope:

- **UnifiedMessage** (`mpreg/fabric/message.py`): Common message schema with
  routing headers.
- **FabricMessageEnvelope** (`mpreg/core/model.py`): External transport wrapper.
- **FabricRouter** (`mpreg/fabric/router.py`): Chooses local vs remote endpoints
  and enforces routing policies.

Routing headers used across all systems:

- `routing_path`: node path, loop prevention.
- `federation_path`: cluster path, loop prevention.
- `hop_budget`: hard bound on multi-hop forwarding.
- `source_cluster` / `target_cluster`: scope constraints.

Why this design:

- **Benefit**: One envelope means one routing policy and consistent tracing.
- **Drawback**: Extra header overhead vs per-system ad-hoc formats.

Transport note:

- Server, peer, and client connections all flow through the core transport
  layer (`TransportFactory`, `TransportInterface`, `TransportListener`), so there
  are no ad-hoc websocket/TCP stacks outside `mpreg/core/transport`.
- Multi-protocol adapters support auto-assigned ports (`base_port=0`) with a
  callback to capture assigned endpoints for cluster bootstrapping.
- Fabric node advertisements now derive transport endpoints strictly from each
  server’s own `local_url`/`advertised_urls`, not the global adapter registry.
  The registry is shared across the process and can include unrelated endpoints
  from other test fixtures or adapters; using it in catalog deltas caused Raft
  peers to attempt TCP connects to non-existent ports under full-suite load.
  Scoping announcements to per-node URLs keeps discovery stable, avoids leaking
  unrelated endpoints, and improves correctness/security (only self-owned
  endpoints are advertised). Full-suite `-n 10` runs now pass with this change.

### 3) RPC Execution + Dependency Resolver

RPC supports dependency graphs and resource-aware routing.

- **Registry + FunctionIdentity** (`mpreg/fabric/function_registry.py` and
  `mpreg/datastructures/function_identity.py`): Every function has a name,
  unique function_id, and semantic version.
- **Dependency Resolver** (`mpreg/core/topic_dependency_resolver.py`): Resolves
  topological execution order.
- **Server Integration** (`mpreg/server.py`): Adapts RPC requests to the fabric
  router and execution engine.

Why this design:

- **Benefit**: Safe routing across versions with deterministic matching.
- **Drawback**: Slightly more metadata on registration and selection.

### 4) Pub/Sub, Queue, Cache

Each system uses the same fabric routing plane, but retains its own semantics:

- **Pub/Sub** (`mpreg/core/topic_exchange.py`): AMQP-style matching with
  fabric forwarding for global topics.
- **Queues** (`mpreg/fabric/queue_federation.py`): Cross-cluster queue delivery
  with delivery guarantees and consensus integration.
- **Cache** (`mpreg/fabric/cache_federation.py`): Multi-tier cache sync via
  fabric messages and catalog-driven selection.

Why this design:

- **Benefit**: Shared routing and discovery reduces duplicated protocols.
- **Drawback**: Coupled evolution; changes in the fabric touch all systems.

### 4b) Persistence Layer

Persistence is unified via `mpreg/core/persistence` with pluggable backends.
Queues and cache (L2) use the same key/value + queue store abstractions, while
the fabric catalog + route key registry can snapshot metadata for faster restarts.

Why this design:

- **Benefit**: Single persistence surface with consistent crash recovery.
- **Drawback**: Requires careful schema/versioning as new subsystems join.

### 5) Membership + Health

Membership is managed via gossip and server lifecycle messages.

- **Server lifecycle** (`RPCServerStatus`, `RPCServerGoodbye` in
  `mpreg/core/model.py`): Diagnostics and shutdown signaling.
- **Membership** (`mpreg/fabric/membership.py`): Failure detection and
  cluster-wide liveness signals.

Why this design:

- **Benefit**: Fast convergence for joins/leaves.
- **Drawback**: Requires robust debounce to avoid stormy reconnections.

### 6) Consensus (Raft over Fabric)

Consensus uses `ProductionRaft` running on the fabric control plane rather than
raw TCP. Raft RPCs are serialized into control messages and routed through the
UnifiedMessage envelope, so consensus traffic follows the same routing, policy,
and observability paths as the rest of the platform.

- **Canonical transport**: `FabricRaftTransport` over the control plane.
- **RPC payloads**: `mpreg/fabric/raft_messages.py` + `mpreg/datastructures/raft_codec.py`.
- **Server wiring**: `MPREGServer.register_raft_node()` registers a Raft node with
  the server's fabric transport.

Deployment patterns:

- **Intra-cluster Raft** (default): Best for fast elections and low latency.
  Use fabric routing with tight hop budgets and local peers only.
- **Cross-cluster Raft** (optional): Use when you need geo-distributed consensus.
  Expect higher latency and plan for wider election timeouts and conservative
  failure detection thresholds.

Why this design:

- **Benefit**: One routing/control plane for consensus and application traffic.
- **Drawback**: Consensus latency couples to fabric routing health.
- **Mitigation**: Use conservative timeouts for cross-cluster Raft and monitor
  control-plane lag with route traces.

## End-to-End Flows

### A) Local RPC (Single Cluster)

1. Client sends `rpc` to a node.
2. Server resolves dependency graph.
3. Router picks local endpoint by function identity + resources.
4. Local execution returns `rpc-response`.

### B) Cross-Cluster RPC (Multi-Hop)

1. Client sends `rpc` to a node.
2. Router selects a remote endpoint from the catalog.
3. Route table chooses next hop; server sends `fabric-message`.
4. Intermediate nodes forward using hop budget + path tracking.
5. Reply uses the recorded path or re-routes if a hop fails.

### C) Pub/Sub Fan-Out

1. Subscriber registers patterns locally.
2. Catalog delta gossips subscription metadata to the fabric.
3. Publisher sends event to any node.
4. Router forwards to matching clusters, then local delivery occurs.

### D) Queue Delivery (Global)

1. Queue endpoint advertised via catalog delta.
2. Sender submits message to local manager.
3. Route table forwards to the owning cluster.
4. Delivery acknowledgments flow back via the fabric.

## Design Decisions and Tradeoffs

| Decision                       | Benefit                         | Drawback              | Mitigation                               |
| ------------------------------ | ------------------------------- | --------------------- | ---------------------------------------- |
| Single fabric envelope         | Unified routing + observability | Larger payloads       | Keep headers minimal, compress if needed |
| Gossip-based discovery         | Scales to large clusters        | Eventual consistency  | TTL + retry + anti-entropy               |
| Path-vector routing            | Loop-free multi-hop routing     | Convergence delay     | TTL + re-advertise on topology changes   |
| Function identity + versioning | Safe routing across versions    | More metadata         | Provide helper APIs + defaults           |
| Resource-based routing         | Deterministic placement         | Requires accurate ads | Catalog TTL + diagnostics                |

## Observability and Operations

Logging is centralized in `mpreg/core/logging.py` with module-based filtering.

- **Module filtering**: enable targeted debug output (`fabric.router`, `goodbye`, `cluster`).
- **Structured context**: correlation IDs and routing paths in logs.
- **Metrics**: `mpreg/core/statistics.py` exports fabric and system metrics.
- **Route trace**: use `RouteTable.explain_selection()` for next-hop decisions.

Operational recommendations:

- Keep default log level at INFO for throughput tests.
- Use module filters for narrow triage (avoid global DEBUG in large clusters).
- Validate routing convergence with the fabric auto-discovery tests.

See `docs/OBSERVABILITY_TROUBLESHOOTING.md` for a focused troubleshooting flow.

## Extensibility

- Add new control-plane catalog entries by extending `RoutingCatalogDelta`.
- Add new message types by extending `UnifiedMessage` + topic taxonomy.
- Add routing policies via `RoutePolicy` and router scoring hooks.

For protocol specifics, see `docs/MPREG_PROTOCOL_SPECIFICATION.md`.

## See Also

- `docs/MANAGEMENT_UI_CLI_NEXT_STEPS.md`
- `docs/FABRIC_LINK_STATE_ROUTING.md`
- `docs/FABRIC_ROUTE_POLICIES.md`
- `docs/FABRIC_ROUTE_SECURITY.md`
