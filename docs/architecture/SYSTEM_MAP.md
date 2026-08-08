# System Map — MPREG Product Composition

**Authority for:** package layout, boot path, data/control planes, Raft placement.  
**Code root:** `/Users/matt/repos/mpreg` (paths below are repo-relative).

---

## 1. Package responsibilities

| Package | Responsibility | Key entry |
|---------|----------------|-----------|
| `mpreg/core/` | Kernel: settings, envelopes, RPC registry, transport, cache/queue, logging, ports, **error taxonomy** | `core/config.py`, `core/errors.py` |
| `mpreg/fabric/` | Control + routing plane: gossip, catalog, routes, federation adapters, **Raft wire**, lightweight consensus | `fabric/control_plane.py`, `fabric/raft_transport.py` |
| `mpreg/datastructures/` | Algorithms/types: **ProductionRaft**, storage, codec, task manager, vector clocks, merkle, blockchain | `datastructures/production_raft_implementation.py` |
| `mpreg/consensus/` | **Canonical app façade** for strong consensus | `consensus/__init__.py` |
| `mpreg/server.py` | **Composition root** `MPREGServer` | `server.py` |
| `mpreg/server_pkg/` | Peeled planes: RPC, RaftPlane, audit, metrics, peer dial | `server_pkg/raft_handlers.py` |
| `mpreg/client/` | Client façades; wire errors via `map_exception` | `client/call_policy.py` |
| `mpreg/dns/` | DNS gateway for discovery records | `dns/server.py` |
| `mpreg/cli/` | Operator CLI | `cli/main.py` |
| `mpreg/testing/` | Concurrent runner, DistLab, hang observe | `testing/concurrent_runner.py` |
| `mpreg/examples/` | Demos + curriculum apps | `examples/apps/` |
| `tests/` | Unit / integration / chaos / invariants / release | see VALIDATION.md |

---

## 2. Runtime composition (boot)

```text
profiles/*.toml / CLI / MPREGSettings
        │
        ▼
MPREGServer.__post_init__          # sync wire
  RpcRegistry, Cluster, FederationConnectionManager
  ConsensusManager (lightweight — NOT Raft)
  TopicExchange, Fabric control plane + FabricRouter
  FabricRaftTransport + RaftPlane (empty node set)
        │
        ▼
await server.server()              # listen + loops
  TransportFactory listener, accept, peer manage
  monitoring HTTP, optional DNS
  consensus_manager.start()
  _initialize_default_systems (cache/queue/STRONG if flags)
  _start_fabric_control_plane
```

| Stage | Source |
|-------|--------|
| Settings | `mpreg/core/config.py` |
| Port alloc | `mpreg/core/port_allocator.py` |
| `__post_init__` / Raft pipe | `mpreg/server.py` ~1650–2535 (`_initialize_fabric_raft_transport`) |
| `server()` loop | `mpreg/server.py` ~12751+ |
| Default cache/queue | `mpreg/server.py` `_initialize_default_systems` |
| CONTROL → Raft | `mpreg/server.py` ~4329 → `FabricRaftTransport.handle_message` |
| Register node | `mpreg/server.py` `register_raft_node` ~2523 |

**Default:** fabric builds an empty Raft **pipe**. No `ProductionRaft` runs until
an app/test constructs one and calls `server.register_raft_node(node)`.

---

## 3. Cross-cutting planes

```text
┌─ Data ──────────────────────────────────────────┐
│ RPC │ PubSub │ Queues │ Cache (EVENTUAL + STRONG) │
└─────────────────┬────────────────────────────────┘
                  │ UnifiedMessage
┌─────────────────▼────────────────────────────────┐
│ Fabric: Catalog · Gossip · Routes · Membership   │
└─────────────────┬────────────────────────────────┘
                  │
┌─────────────────▼────────────────────────────────┐
│ Coordination: LightweightConsensusManager (always)│
│             FabricRaftTransport (pipe if fabric)  │
│             ProductionRaft nodes (opt-in)         │
└──────────────────────────────────────────────────┘
```

| Plane | Not Raft? | Notes |
|-------|-----------|-------|
| Cache STRONG | **Yes — separate** majority prepare/commit | `core/cache_strong*` |
| Shared audit | **Yes — G-Set gossip** | `server_pkg/shared_audit*` |
| Queue quorum delivery | **Yes — queue federation** | `fabric/queue_*` |
| Lightweight consensus | **Yes — gossip votes** | `fabric/consensus.py` |
| ProductionRaft | Strong replicated log | opt-in registration |

Dual-API footgun matrix: `mpreg/server_pkg/consensus.md`.

---

## 4. Client → server → peer flow

```text
App / MPREGClient
    │ WS (usually)
    ▼
MPREGServer A ── local execute / TopicExchange / GCM / queue
    │ if remote
    ▼
FabricRouter + RoutingIndex → UnifiedMessage over peer WS
    ▼
MPREGServer B ── execute / match / apply → response hops back

Control (continuous): announce → gossip deltas → route/membership apply

Opt-in Raft: CONTROL RAFT_RPC RequestVote|AppendEntries|InstallSnapshot
             via FabricRaftTransport correlation futures
```

---

## 5. Concurrent-test risk surfaces (product-wide)

| Risk | Where |
|------|--------|
| Port bands + xdist worker offset (cap 20) | `core/port_allocator.py` |
| FD limits on macOS | `testing/resource_limits.py` |
| Task/connection leaks across tests | `tests/conftest.py` AsyncTestContext |
| Fixed sleep “ready” fixtures | `tests/conftest.py` cluster fixtures |
| Real timers under CPU load | Raft + gossip + peer dial |
| Dual consensus confusion | tests wiring both LW + Raft |

Detail: [VALIDATION.md](VALIDATION.md), [RAFT.md](RAFT.md).
