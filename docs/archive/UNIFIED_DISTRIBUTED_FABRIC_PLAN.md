UNIFIED DISTRIBUTED FABRIC PLAN

NOTE
This document is a historical log. The current unified source of truth is
`docs/UNIFIED_UNIFICATION_PLAN.md`.

VISION
Build a single, coherent distributed control + data plane ("Routing Fabric") that
routes RPC, pub/sub, queues, cache coordination, and federation through one shared
message envelope, routing engine, and gossip-driven discovery. No legacy layers or
compat shims; this is a clean, long-term architecture.

PRINCIPLES (NON-NEGOTIABLES)

- No backwards compatibility or dual-stack behavior.
- One canonical message envelope and routing policy interface.
- One canonical routing engine (graph + local selection + policy).
- One canonical gossip protocol and discovery catalog.
- SPOR / SOLID design with single-purpose data structures and clean interfaces.
- All internal types are explicit (no tuple[str, str] keys; use named dataclasses).
- The platform routes itself using the same internal fabric.
- Tests are rewritten to target the new interfaces as they are introduced.
- No fixed ports in tests; always use the port allocator.
- Capability parity or better: preserve cross-cluster federation by moving it into the fabric
  (legacy components only removed after fabric parity exists).

AUDIT SUMMARY (SYSTEMS TO UNIFY)

- graph_algorithms: generic pathfinding primitives (Dijkstra, A\*, multipath).
- federation_graph: graph model + GraphBasedFederationRouter (moved into fabric).
- unified_routing + unified_router_impl: legacy unified envelope + partial router.
- federation_gossip: core gossip protocol (membership/state/config) now used as the fabric gossip engine.
- federated_queue_gossip: queue discovery on top of gossip (removed).
- topic_exchange: local pub/sub exchange.
- federated_topic_exchange: cross-cluster pub/sub wrapper (replace with fabric transport).
- federation_bridge: graph-aware cross-cluster pub/sub transport.
- unified_federation + system_federation_bridge: second topic-based federation plane (removed; replaced by fabric).
- federated_message_queue + federated_delivery_guarantees: cross-cluster queues (removed; replaced by fabric queue federation + queue delivery coordinator).
- cache sync + cache_federation_bridge: cache-specific sync + federation.
- RPC mesh/federated routing: separate routing path + catalogs.

TARGET ARCHITECTURE (UNIFIED ROUTING FABRIC)

- UnifiedMessage: single envelope for RPC, pub/sub, queue, cache, control.
- RoutingCatalog: unified registry of function, topic, queue, cache, node resources.
- RoutingIndex: topic trie + selector indexes (function_id/version/resources).
- RoutingEngine: local selection + graph routing + policy enforcement.
- UnifiedGossip: federation_gossip extended with catalog updates.
- RoutingTransport: federation_bridge adapted for UnifiedMessage.
- Thin adapters: RPC, TopicExchange, QueueManager, CacheManager.

WORK PLAN (PHASES + STATUS)

PHASE 0: SPEC + GROUNDWORK

- Define UnifiedMessage schema and routing headers.
- Define unified topic taxonomy (rpc/, queue/, cache/, control/).
- Define RoutingEngine API and adapter contracts.
  Status: IN PROGRESS (needs UnifiedMessage schema finalization).

PHASE 1: CORE FABRIC DATA STRUCTURES + UNIT TESTS

- RoutingCatalog + typed keys + selector matching (function_id/version/resources).
- RoutingIndex (topic trie + selector indexes).
- RoutingEngine (local selection + graph planner hook).
- Catalog delta + applier + dict serialization.
- Unit tests for catalog/index/engine/applier/message.
  Status: DONE.

PHASE 2: UNIFIED GOSSIP + CATALOG INGESTION

- Extend federation_gossip with CATALOG_UPDATE.
- Publish catalog deltas (publisher/broadcaster/announcers).
- Replace queue gossip with fabric catalog updates.
- Replace cache gossip with fabric catalog updates.
- Advertise topic subscriptions via fabric catalog updates.
- Tests: gossip propagation -> catalog convergence.
  Status:
- Catalog update support: DONE.
- Queue gossip replacement: DONE (queue manager refactor + catalog gossip tests).
- Cache sync replacement: DONE (cache ops routed via fabric cache transport; cache_gossip removed).
- Topic subscription announcements: DONE (adapter + announcer + gossip tests + server wiring).

PHASE 3: UNIFIED TRANSPORT + SYSTEM ADAPTERS

- Adapt federation_bridge transport for UnifiedMessage.
- Replace federated_topic_exchange path with RoutingEngine decisions.
- Plug RPC mesh routing into RoutingCatalog + RoutingEngine.
- Plug QueueManager and TopicExchange into RoutingEngine.
- Integration tests for multi-hop routing and hop policies.
  Status: IN PROGRESS (fabric transport + pubsub routing wired; RPC routing moved to fabric; multi-hop response routing done; server envelope transport consolidation done; cache/queue parity verified; remaining: legacy transport cleanup).

PHASE 4: SYSTEM REPLACEMENT + CLEANUP

- Remove unified_federation + system_federation_bridge. (DONE)
- Remove federated_message_queue + federated_delivery_guarantees. (DONE)
- Remove federated_topic_exchange. (DONE)
- Remove mesh routing data structures/tests. (DONE)
- Remove cache_gossip protocol and legacy routing paths in server.py. (DONE)
- Remove unified_router_impl (replace with Fabric routing). (DONE)
- Rewrite tests to use fabric interfaces only.
- Rename federation-era modules to fabric names (gossip/consensus/membership) once all imports are migrated. (DONE)
  Note: removals only after parity for cross-cluster federation features.
  Status: IN PROGRESS (unified routing + unified federation removed; cache sync replacement done; unified_router_impl removed; cache federation bridge removed; gossip/consensus/membership renamed to fabric).

PHASE 5: OBSERVABILITY + DOCS

- Unified tracing for routing decisions and hop paths.
- Unified metrics for routing performance, gossip convergence, transport health.
- Documentation: architecture, protocol spec, config, examples.
  Status: IN PROGRESS (plan only).

PHASE 6: ROUTE CONTROL PLANE (PATH-VECTOR)

- RouteDestination/RoutePath/RouteMetrics/RoutePolicy/RouteTable data structures.
- Route announcements over gossip (publisher + processor).
- Periodic local route announcer with TTL/interval config.
- Planner integration: route table first, graph router fallback.
- Tests: unit for data structures/announcer, gossip propagation, reroute E2E.
  Status: DONE (data structures + gossip integration complete; reroute E2E passing).

CURRENT STATUS SNAPSHOT

- Fabric core data structures implemented with typed keys and unit tests.
- Catalog delta publisher/applier + gossip ingestion done.
- Catalog broadcaster + function/queue/cache announcers added.
- Local function registry + catalog adapter for function announcements.
- Queue gossip removed and queue manager refactored to use RoutingIndex.
- Queue tests updated to register QueueEndpoint in RoutingCatalog.
- Fabric cache transport added; FabricCacheProtocol uses transport + catalog.
- Cache sync serialization added for operations/digests/entries.
- Server-side fabric cache transport integrated (UnifiedMessage + request/response).
- Removed CacheFederationBridge + GlobalCacheFederation; L4 cache uses FabricCacheProtocol with allowed-cluster gating.
- Added CacheNodeProfile catalog entries + adapter + selector for geo/latency/capacity-aware cache replication.
- Server-side fabric control plane + gossip transport wired for catalog propagation.
- Added periodic fabric catalog refresh loop to re-announce local functions/nodes on TTL cadence.
- Added direct catalog snapshot sync on peer connect and removed status-based catalog fallback.
- Topic exchange announcements wired into fabric catalog updates and server subscription lifecycle.
- PubSub forwarding metadata added with unit tests.
- PubSub forwarding wired into server publish path with cross-server integration test.
- UnifiedMessage codec + fabric-message envelope + server transport added.
- PubSub forwarding now uses unified fabric transport (fabric-message).
- TopicExchange federation bridge removed (legacy pubsub federation decoupled).
- Cache pubsub integration no longer depends on FederatedTopicExchange.
- Removed legacy unified routing modules/config; server no longer initializes unified routing.
- Rewrote unified routing unit/integration/property tests to target FabricRouter + fabric messages.
- Fixed FabricRPCRequest kind detection in FabricRouter RPC command extraction.
- Removed unified_federation + system_federation_bridge; rewired leader election to use cluster discovery provider.
- Added fabric replacement tests for unified federation coverage (unit + live workflow).
- Added ClusterMessenger and refactored queue federation forwarding to use it.
- Added FabricQueueDeliveryCoordinator + queue consensus control messages with unit tests.
- Added QueueCatalog.entries for explicit queue enumeration.
- Rewrote federated queue tests as fabric queue federation + delivery tests (unit + live).
- Removed legacy federated_message_queue + federated_delivery_guarantees and queue/consensus handling in federation_bridge.
- Updated federation architecture docs + protocol spec to align with fabric queue APIs.
- Cleaned server logs to remove debugging-only emoji markers.
- Removed obsolete federation queue debug scripts that referenced removed modules.
- Updated README federation examples to use fabric-backed cluster config.
- Removed mesh routing data structures; replaced mesh tests with fabric catalog/policy/engine coverage.
- Rewrote mesh forwarding integration tests to validate fabric RPC forwarding + hop budgets.
- Updated docs to remove mesh routing references and document fabric routing + envelopes.
- Moved federation graph + graph monitor into fabric; replaced FederatedRPCRouter with FabricFederationPlanner.
- Added route control plane data structures (RouteDestination/Path/Metrics/Policy/Table) with unit tests.
- Added route announcement publisher/processor + periodic announcer with unit tests.
- Added ROUTE_ADVERTISEMENT gossip message type and route gossip propagation test.
- Wired route table into FabricFederationPlanner with graph fallback.
- Filtered disconnected peers when selecting fabric send targets to allow reroute on link loss.
- Consolidated server envelope transport logic for fabric/gossip message delivery.
- PeerDirectory now participates in connection event bus (hashable subscriber).

NEXT UP (SHORTLIST)

1. Audit remaining docs/TODOs for references to removed legacy modules and clean them up.
2. Finalize protocol/schema docs (UnifiedMessage + routing headers + topic taxonomy).
3. Extend `docs/TEST_PARITY_MATRIX.md` if any new parity gaps are discovered.

TEST STRATEGY

- Unit: catalog selectors, version constraints, resource filters.
- Unit: routing engine decisions (local + multi-hop).
- Unit: gossip message processing and catalog convergence.
- Integration: live servers with unified routing for RPC, pub/sub, queue.
- E2E: multi-hop routing with failure/hop-budget paths.
- All tests use port allocator; no fixed ports.

TEST PARITY AUDIT (IN PROGRESS)

- Canonical mapping lives in `docs/TEST_PARITY_MATRIX.md`.
- Federated queue suites removed -> replaced by fabric queue unit + live tests (`tests/test_fabric_queue_*`, `tests/integration/test_fabric_queue_federation.py`).
- Federated topic exchange suites removed -> replaced by fabric pubsub routing/forwarding tests (`tests/test_fabric_pubsub_*`, `tests/integration/test_fabric_pubsub_forwarding.py`).
- Mesh routing suites removed -> replaced by fabric planner + policy + hop budget tests (`tests/test_fabric_federation_planner.py`, `tests/test_fabric_catalog_policy.py`).
- Unified router impl suite removed -> replaced by `tests/test_fabric_router_impl.py`.
- Global cache federation suite removed -> replaced by fabric cache transport + protocol tests (`tests/test_cache_federation_transport.py`, `tests/test_global_cache.py`).
- Missing parity to add: fabric transport circuit breaker coverage (replaces FederationBridge circuit breaker tests). (DONE: ServerEnvelopeTransport circuit breaker + tests)
- Missing parity to add: geo/latency/capacity-aware cache federation metadata in fabric catalog + selection tests and examples. (DONE: CacheNodeProfile + selector + tests + examples)
- Example parity to add: replace removed legacy federation demos with fabric-native equivalents (tier2/tier3 + showcase).

PROGRESS LOG

- 2025-12-29: Completed system audit and overlap analysis.
- 2025-12-29: Documented target unified fabric architecture.
- 2025-12-29: Added mpreg/fabric with UnifiedMessage + RoutingCatalog (typed keys).
- 2025-12-29: Added RoutingIndex + RoutingEngine with unit tests.
- 2025-12-29: Added RoutingCatalogDelta + applier + dict serialization with tests.
- 2025-12-29: Added CATALOG_UPDATE gossip handling + convergence tests.
- 2025-12-29: Added catalog delta publisher + broadcaster + announcers.
- 2025-12-29: Added local function registry + catalog adapter for function announcements.
- 2025-12-29: Added integration test for function registry delta -> gossip -> remote catalog.
- 2025-12-29: Added integration test for queue advertisement -> gossip -> remote catalog.
- 2025-12-29: Removed federated_queue_gossip; refactored queue manager to use RoutingIndex.
- 2025-12-29: Updated queue tests to use QueueEndpoint registrations.
- 2025-12-29: Added cache gossip transport abstraction + unit tests; refactored CacheGossipProtocol to use transport and updated server/examples/tests.
- 2025-12-29: Added federation gossip transport abstraction; refactored GossipProtocol to use transport and updated tests.
- 2025-12-29: Added gossip message serialization helpers (to_dict/from_dict) and unit tests.
- 2025-12-29: Wired server-side fabric gossip transport + control plane startup; added integration test for catalog propagation across live servers.
- 2025-12-29: Updated advanced fabric topology guide to use dynamic ports + fabric naming; refreshed topology matrix + testing summary note.
- 2025-12-31: Soak + churn fabric tests pass; recorded baseline in production deployment docs; demo system key updated to `fabric`.
- 2025-12-30: Added periodic fabric catalog refresh to keep function/node announcements alive under long-running discovery tests.
- 2025-12-31: Centralized loguru configuration + scoped debug filters; fixed catalog observer filtering to prevent departed nodes being re-added; GOODBYE storm prevention test verified.
- 2025-12-30: Added `docs/TEST_PARITY_MATRIX.md` and catalog refresh integration test coverage.
- 2025-12-30: Fixed tier3 fabric demo cleanup and updated example/docs text for fabric federation.
- 2025-12-30: Removed status-based catalog snapshot fallback; added catalog snapshot sync on peer connect.
- 2025-12-30: Ran fabric cross-cluster E2E suites (pubsub/RPC/queue/cache + unified RPC federation).
- 2025-12-29: Added cache gossip serialization helpers (operation/digest).
- 2025-12-29: Added server cache gossip transport and cache gossip envelope.
- 2025-12-29: Added cache gossip server transport integration test.
- 2025-12-29: Added topic exchange catalog adapter + announcer and gossip propagation tests.
- 2025-12-29: Wired pubsub subscriptions to fabric topic announcements in server lifecycle.
- 2025-12-29: Added pubsub forwarding metadata + unit tests.
- 2025-12-29: Wired server pubsub publish to fabric routing + forwarding.
- 2025-12-29: Added integration test for cross-server pubsub forwarding via fabric catalog.
- 2025-12-29: Added UnifiedMessage codec + fabric-message envelope + server transport.
- 2025-12-29: Routed pubsub forwarding over unified fabric transport.
- 2025-12-29: Removed TopicExchange federation bridge references; trimmed FederatedTopicExchange usage in cache/queue paths.
- 2025-12-29: Scoped membership gossip to local cluster only; cross-cluster routing stays on fabric gossip/transport.
- 2025-12-29: Fixed fabric gossip multi-hop propagation by avoiding pre-marking seen_by in gossip sends.
- 2025-12-29: Adjusted fabric RPC routing to enforce federation policy at next-hop selection (not target cluster).
- 2025-12-29: Returned forward-failure error payloads from RPC runners instead of raising, preserving client-visible error semantics.
- 2025-12-29: Added catalog-propagation waits in unified RPC federation tests to remove race flakiness.
- 2025-12-30: Removed legacy unified routing modules/config; server no longer initializes unified routing.
- 2025-12-30: Rewrote unified routing tests (unit/integration/property) to target FabricRouter + fabric messages.
- 2025-12-30: Fixed FabricRPCRequest kind detection for fabric RPC routing.
- 2025-12-30: Removed unified_federation + system_federation_bridge (legacy topic federation plane).
- 2025-12-30: Added fabric federation-equivalent tests and live workflow integration test.
- 2025-12-30: Added ClusterMessenger + refactored FabricQueueFederationManager forwarding.
- 2025-12-30: Added FabricQueueDeliveryCoordinator with queue consensus control messages.
- 2025-12-30: Added unit tests for cluster messenger, queue federation, and queue delivery.
- 2025-12-30: Added live integration tests for queue delivery, subscriptions, and quorum.
- 2025-12-30: Removed federated_message_queue + federated_delivery_guarantees and queue/consensus handling in federation_bridge.
- 2025-12-30: Updated federation documentation + protocol spec for fabric queue APIs.
- 2025-12-30: Cleaned server logs and removed obsolete federation debug scripts.
- 2025-12-30: Updated README federation examples to use fabric-backed configuration.
- 2025-12-30: Removed mesh routing data structures; rewrote mesh routing tests for fabric catalog/policy/engine.
- 2025-12-30: Updated integration tests for fabric RPC forwarding, resource filtering, hop budgets.
- 2025-12-30: Updated architecture + protocol docs to remove mesh routing references.
- 2025-12-30: Replaced cache_gossip with fabric cache transport + cache sync topics; updated tests/examples/docs.
- 2025-12-30: Moved federation graph + graph monitor into fabric; replaced FederatedRPCRouter with FabricFederationPlanner and updated settings/tests/docs.
- 2025-12-30: Allowed explicit-bridging catalogs to propagate transitively (fabric allowed_clusters now unrestricted outside strict isolation).
- 2025-12-30: Added fabric RPC response forwarding via request routing_path for multi-hop replies.
- 2025-12-30: Added route control plane data structures + unit tests.
- 2025-12-30: Added route announcements (publisher/processor/announcer) and gossip message type.
- 2025-12-30: Added route gossip propagation test and planner route table integration.
- 2025-12-30: Filtered disconnected peers during fabric send to enable reply reroute when a hop fails.
- 2025-12-30: Consolidated server envelope transport for fabric + gossip delivery.
- 2025-12-30: Fixed PeerDirectory subscription to connection event bus (hashable subscriber).
- 2025-12-30: Ran fabric integration suites for queue/pubsub/cache to confirm parity (5 tests).
- 2025-12-30: Removed CacheFederationBridge + GlobalCacheFederation; rewired L4 cache to use FabricCacheProtocol with allowed cluster gating.
- 2025-12-30: Updated cache docs/examples + CLI help for fabric cache federation.
- 2025-12-30: Renamed unified router tests to fabric router tests.
- 2025-12-30: Moved monitoring endpoints, performance metrics, and connection manager under `mpreg/fabric`; updated imports/tests.
- 2025-12-30: Added `PeerNeighbor` typed neighbor references in peer directory; updated server accessor and tests.
- 2025-12-30: Moved federation config + alerting into `mpreg/fabric`; updated imports, docs, and tests.
- 2025-12-30: Replaced tuple-based edge/header helpers with typed structures (message headers, graph edges, topology edges, test helpers).
- 2025-12-30: Monitoring endpoints now stop the unified monitor to prevent leaked-task warnings in tests.
- 2025-12-30: Moved gossip/consensus/membership modules to `mpreg/fabric` and updated imports/docs.
- 2025-12-30: Added event-driven gossip wake and prioritized catalog updates for faster convergence.
- 2025-12-30: Added small-cluster full-fanout for catalog/route gossip to fix star-hub discovery.
- 2025-12-30: Ran fabric integration suites for RPC + gossip (fabric RPC forwarding + unified live workflow + catalog/server gossip).
- 2025-12-30: Removed RPCServerHello data structures and HELLO handling; updated server connection auth, tests, docs, and debug scripts to catalog-only discovery.
- 2025-12-30: Applied STATUS function snapshots into the routing catalog and handled fabric-gossip on outbound peer connections; updated federated RPC tests to wait for catalog convergence.
- 2025-12-30: Added CacheNodeProfile catalog entries + adapter + selector; cache transport selects peers using geo/latency/capacity metadata.
- 2025-12-30: Added circuit breaker handling to ServerEnvelopeTransport with unit coverage.
- 2025-12-30: Removed HELLO propagation in server connections; status-driven discovery + fabric catalog only.
- 2025-12-30: Moved hub/registry/auto-discovery/leader-election/blockchain federation modules into `mpreg/fabric`; updated imports/docs/tests and revalidated suites.
- 2025-12-30: Ran monitoring endpoints + unified system integration suites (basic/integration/comprehensive/live workflow).
- 2025-12-30: Re-ran fabric cross-cluster parity suites (RPC/pubsub/queue/cache) after fabric module moves.
- 2025-12-31: Updated protocol, architecture, examples, client guide, and production docs to reflect unified fabric routing.
- 2025-12-31: Added observability troubleshooting doc, CLI/quick-start/roadmap doc updates, and fabric soak/churn tests.

OPEN QUESTIONS

- Cache propagation semantics under unified routing (eventual vs quorum).
