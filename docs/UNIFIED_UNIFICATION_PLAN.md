# Unified Unification Plan (Routing Fabric)

VISION
Build a single, coherent routing fabric that powers RPC, pub/sub, queues, cache,
and federation through one catalog, one gossip protocol, and one message
envelope. No legacy compatibility layers; this is a clean long-term platform.

PRINCIPLES (NON-NEGOTIABLES)

- No backwards compatibility or dual-stack behavior.
- One canonical message envelope + routing policy interface.
- One canonical routing engine (catalog + planner + policy).
- One canonical gossip protocol + catalog delta propagation.
- SPOR / SOLID design, explicit types (no tuple[str, str] keys).
- Platform routes itself using the fabric control plane.
- No fixed ports in tests; always use the port allocator.

STATUS LEGEND

- DONE: implemented and verified
- PARTIAL: implemented but needs docs/tests/cleanup
- TODO: not started

CURRENT STATE (STATUS)

- Fabric catalog + index + routing engine implemented. (DONE)
- Catalog delta publisher/applier + gossip ingestion. (DONE)
- Function/queue/cache/topic announcers and adapters. (DONE)
- UnifiedMessage codec + fabric message envelope + server transport. (DONE)
- RPC routing via fabric (Cluster now uses FabricRouter; router honors function_id/version/resources; cache key includes hop budget/federation path). (DONE)
- Pubsub forwarding via fabric routing. (DONE)
- Queue federation + delivery coordinator via fabric. (DONE)
- Cache federation via fabric transport + catalog + cache profiles. (DONE)
- Route control plane (path-vector) + gossip propagation. (DONE)
- HELLO/ACK + legacy function announcement fallback removed. (DONE)
- Status-based catalog snapshot fallback removed; direct catalog snapshot sync on peer connect. (DONE)
- Cross-cluster parity E2E suites executed (pubsub/RPC/queue/cache + unified RPC federation). (DONE)
- Test parity matrix created in `docs/TEST_PARITY_MATRIX.md`. (DONE)
- Hub module naming consolidated (`hubs.py`, `hub_registry.py`, `hub_hierarchy.py`). (DONE)
- Loguru logging centralized with scoped debug filters. (DONE)
- Client networking uses the core transport factory (no direct websockets in client). (DONE)

REMAINING WORKSTREAMS (PRIORITIZED)

1. SPEC FINALIZATION (DONE)
   - UnifiedMessage schema + routing headers documented in protocol spec.
   - Topic taxonomy documented and linked to `mpreg/core/topic_taxonomy.py`.
2. DOCS + EXAMPLE PARITY (DONE)
   - Protocol/architecture/docs refreshed for unified fabric.
   - Update `mpreg/examples/README.md` + `docs/EXAMPLES.md` to reflect fabric-only APIs. (DONE)
   - Update blockchain guide to use hub registry naming. (DONE)
   - Audit remaining federation-era docs for fabric naming consistency. (DONE: advanced guide, topology matrix, CLI/quick-start/roadmap/queue docs, testing summary note)
3. OBSERVABILITY + OPERATIONS (DONE)
   - Routing decision traces added to `mpreg/fabric/router.py` (DEBUG logs).
   - Document logging scopes + troubleshooting workflow. (DONE: `docs/OBSERVABILITY_TROUBLESHOOTING.md`)
   - Monitoring endpoints + topology path telemetry documented in deployment docs. (DONE)
4. PERFORMANCE + SCALE VALIDATION (DONE)
   - Add soak/scale tests for large cluster sizes and sustained routing load. (DONE: `tests/test_fabric_soak_churn.py`)
   - Validate convergence time + failure recovery under churn. (DONE: soak + churn tests passing)
5. TEST PARITY MAINTENANCE (PARTIAL)
   - Keep `docs/TEST_PARITY_MATRIX.md` current; add coverage if gaps appear.
6. RAFT INTEGRATION WITH FABRIC PRIMITIVES (DONE)
   - Audit: ProductionRaft now runs over the fabric control plane in the live
     integration tests and leader election wrapper, so Raft traffic uses the
     unified message envelope and routing fabric. The raw TCP transport has
     been removed to eliminate redundant interfaces.
   - Duplication: there are two Raft-ish APIs: `ProductionRaft` (full Raft) and
     `RaftBasedLeaderElection` (election-only). These should be consolidated.
   - Goal: make `ProductionRaft` the canonical Raft API and run it over the
     unified fabric control/data plane (optional, opt-in).
   - Implemented:
     - Added shared Raft codec in `mpreg/datastructures/raft_codec.py`.
     - Added fabric Raft control payloads in `mpreg/fabric/raft_messages.py`.
     - Added `FabricRaftTransport` and integration hook in `mpreg/server.py`
       (CONTROL message handler + init wiring).
     - Added unit tests in `tests/test_fabric_raft_transport.py`.
     - Replaced `RaftBasedLeaderElection` with a `ProductionRaft` wrapper and
       migrated leader election tests to the fabric transport.
     - Migrated live raft integration tests to the fabric transport.
     - Removed the legacy `RaftTcpTransport` / `RaftRpcServer` TCP transport
       to eliminate redundant interfaces.
   - Validation:
     - `tests/test_live_raft_integration.py` (fabric transport) passed.
7. TRANSPORT UNIFICATION (DONE)
   - Goal: all internal and external connections flow through the core transport
     layer (TransportFactory + TransportInterface), with no ad-hoc websocket
     usage outside the transport modules.
   - Implemented:
     - Client networking migrated to core transport factory.
     - Peer connections use transport-backed Connection wrapper.
     - Server listener uses the transport listener adapter (no direct websockets).

NEXT UP (SHORTLIST)

1. Expand long-running performance validation (multi-minute soak under churn)
   once the next scale benchmarks are defined.
2. Continue doc scrub for any new transport or federation references introduced
   during future refactors.
3. Periodic transport audit to ensure no ad-hoc WebSocket/TCP usage appears
   outside `mpreg/core/transport/` and fabric connection adapters.

RECENTLY COMPLETED

- Removed remaining legacy-compat paths (`enhanced_rpc` config helpers, test
  coverage updates, cluster types + raft task manager cleanup, membership dict
  handling).
- Transport/doc example sweep: fixed-port URLs replaced with `<port>` or
  allocator-driven examples in transport docs, client usage samples, and CLI
  templates.
- Transport usage audit: no ad-hoc WebSocket/TCP usage outside
  `mpreg/core/transport/`; only the port allocator uses sockets for availability
  checks.
- Multi-protocol adapters now support per-protocol auto-allocation (base_port=0)
  with callback reporting for assigned endpoints.
- Documentation updates for auto-port adapters and cluster bootstrap examples
  (`docs/MPREG_PROTOCOL_SPECIFICATION.md`, `docs/ARCHITECTURE.md`,
  `docs/GETTING_STARTED.md`).
- Added adapter auto-port tests (`tests/test_transport_adapter.py`) and enhanced
  adapter callback coverage.
- Added auto-port cluster bootstrap demo (`mpreg/examples/auto_port_cluster_bootstrap.py`).
- Port audit sweep: no fixed transport ports in docs/code; only non-port numeric
  examples remain (DAO/token counts).
- Adapter endpoint registry added and wired into multi-protocol adapters
  (auto-registration + release callbacks) with monitoring endpoint support
  (`/transport/endpoints`) and CLI command `mpreg monitor transport-endpoints`.
- Transport endpoints now advertised via NodeDescriptor in catalog deltas, with
  adapter/server wiring + unit tests for serialization and registry adapters.
- Peer connection manager now selects dial targets from NodeDescriptor
  transport endpoints (integration test added).
- Link-state neighbor sets now prioritize peer connection ordering when enabled.
- Transport audit complete: no ad-hoc WebSocket/TCP usage outside
  `mpreg/core/transport/` and port availability checks; only HTTP (aiohttp)
  for monitoring/auto-discovery/alerting.
- Added transport audit test to prevent ad-hoc websockets usage outside the
  transport layer (`tests/test_transport_audit.py`).
- Consolidated test port allocation onto `mpreg/core/port_allocator.py`
  (removed `tests/port_allocator.py`, fixtures now in `tests/conftest.py`).
- `docs/TEST_PARITY_MATRIX.md` updated to include Raft transport unification and
  route export targeting coverage.
- Route-control gaps closed per `docs/archive/FABRIC_PATH_VECTOR_ROUTING_PLAN.md`
  (key distribution automation, policy reloads, export targeting, route-trace
  surface, optional link-state area scoping).
- Performance baselines refreshed: `tests/test_fabric_soak_churn.py`,
  `tests/performance/test_performance_baselines.py`.
- Documentation audit for raw TCP references complete; client/protocol guides now
  call out the transport factory as the canonical internal entry point.

PHASE SNAPSHOT (MERGED)

- Phase 0 (spec groundwork): DONE.
- Phase 1 (core data structures): DONE.
- Phase 2 (gossip + catalog ingestion): DONE.
- Phase 3 (transport + adapters): DONE (cleanup/doc polish remains).
- Phase 4 (system replacement + cleanup): DONE.
- Phase 5 (observability + docs): DONE.
- Phase 6 (route control plane): DONE.

VALIDATION (RECENT PASSES)

- `tests/test_fabric_router_impl.py`
- `tests/integration/test_fabric_rpc_forwarding.py::test_fabric_rpc_forwarding_via_intermediate`
- `tests/test_fabric_soak_churn.py`
- Full suite pass (user-confirmed)

SOURCE OF TRUTH

- Test parity: `docs/TEST_PARITY_MATRIX.md`
- Detailed historical logs: `docs/archive/UNIFIED_DISTRIBUTED_FABRIC_PLAN.md`, `docs/archive/MESH_ROUTING_RPC_PLAN.md`
