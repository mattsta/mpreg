# Mesh + Federated RPC Routing Plan

NOTE
This document is a historical log. The current unified source of truth is
`docs/UNIFIED_UNIFICATION_PLAN.md`.

## Vision

- A single, coherent fabric-based routing/control plane for RPC, queue, cache, pubsub, and federation.
- Multi-hop federated RPC is optional, off by default, and fast by design.
- No legacy compatibility layers; build clean, long-term interfaces.

## Scope

- Multi-hop RPC routing across federated clusters.
- Catalog-driven function discovery (no ad-hoc HELLO/ACK fallbacks).
- Consistent hop budgeting, trust-on-join policy, and observability.

## Constraints

- Use existing graph/path utilities and federation modules (no reinvention).
- Add routing data structures before integrating into server logic.
- Unit tests first, then integration, then full E2E.
- No fixed ports in tests; always use the port allocator.
- No opaque tuple keys (e.g., `tuple[str, str]`); use typed, named structures.

## Reuse Inventory (current)

- `mpreg/datastructures/graph_algorithms.py` (pathfinding helpers)
- `mpreg/datastructures/federated_types.py` (federation metadata)
- `mpreg/datastructures/type_aliases.py` (typed IDs, metrics units)
- `mpreg/fabric/federation_graph.py` (fabric federation graph)
- `mpreg/fabric/federation_graph_monitor.py` (graph monitoring)
- `mpreg/fabric/federation_planner.py` (path planning)
- `mpreg/fabric/route_control.py` + `mpreg/fabric/route_announcer.py` (path-vector routing)
- `mpreg/fabric/router.py` + `mpreg/fabric/engine.py` (routing policies + orchestration)
- `mpreg/fabric/catalog.py` + `mpreg/fabric/index.py` (catalog + discovery)

## Completed (current codebase)

- Fabric routing control plane data structures (path-vector routing, typed routes).
- Fabric catalog + index for function discovery.
- RPC protocol fields for `function_id`, version constraints, `target_cluster`.
- Internal RPC forwarding supports `federation_path` + hop budgets.
- Connection management improvements to honor catalog departures.
- Port hygiene: allocator everywhere; no fixed ports.
- Gossip/consensus/membership modules moved to `mpreg/fabric/*` with imports + docs updated.
- Gossip propagation upgraded to event-driven wake + fast catalog updates.
- Small-cluster catalog/route gossip uses full fanout for convergence.
- Monitoring endpoints + performance metrics + connection manager moved under `mpreg/fabric` (imports updated).
- Peer directory neighbors now use `PeerNeighbor` instead of tuple pairs.
- Federation config + alerting moved under `mpreg/fabric` (imports/docs/tests updated).
- Tuple-based edge/header helpers replaced with typed structures (message headers, graph edges, topology edges).
- Monitoring endpoints stop the unified monitor to avoid leaked task warnings in tests.
- Periodic fabric catalog refresh loop keeps function/node announcements alive under long-running tests.
- Removed status-based catalog snapshot fallback; added direct catalog snapshot sync on peer connect.

## Reported Regressions (re-run status)

- Auto-discovery (10/30/50 node star + multi-hub): PASS.
- Federated RPC hop-limit enforcement: PASS.
- Resource-based routing in integration examples: PASS.
- Consensus gossip integration live test: PASS.
- Performance/scalability research test: PASS.
- Advanced topological research (dynamic reconfiguration + self-healing): PASS.
- Production examples (bulk operations): PASS.
- Production raft concurrent safety: PASS.
- Real-world workflows (order processing): PASS.

## Active Workstreams (ordered)

1. **Regression triage and test conversion**
   - Re-run reported failures with `-vs` to refresh status.
   - Update legacy tests to use fabric catalog/routing APIs.
   - Replace timing sleeps with `wait_for_condition` when propagation is eventual.
2. **Naming + module consolidation**
   - Rename federation-era modules used by fabric (gossip/consensus/membership) into `mpreg/fabric/*`. (DONE)
   - Update imports, docs, and tests to use fabric naming exclusively. (DONE)
   - Remaining: move hub/registry modules into `mpreg/fabric` once dependencies are untangled.
3. **Discovery + propagation reliability**
   - Ensure catalog deltas propagate across topologies (star/multi-hub).
   - Fix peer discovery gaps and validate in 8/10/20/50 node tests.
   - Added full-fanout catalog/route gossip for small clusters (<=20 peers).
4. **Federated RPC hop limits**
   - Ensure within-limit routes succeed; beyond-limit cleanly fails.
   - Add unit coverage for hop budget enforcement in routing paths.
5. **Performance and scale**
   - Restore coverage removed during unification (parity tests).
   - Add larger-topology soak tests for routing/catalog under load.
6. **Documentation + examples**
   - Update examples to the unified fabric APIs.
   - Add routing/observability guide for federation path-vector behavior.
7. **Legacy component retirement**
   - Remove old gossip/fallback paths only after parity coverage is restored.

## Validation Log

- `uv run pytest tests/test_dual_registration_scenarios.py -n 0 -vs`
- `uv run pytest tests/test_goodbye_protocol.py -n 0 -vs`
- `uv run pytest tests/integration/test_gossip_auto_discovery.py::TestCatalogAutoDiscovery::test_three_node_catalog_mesh_discovery -n 0 -vs`
- `uv run pytest tests/test_comprehensive_auto_discovery.py::TestAutoDiscoverySmallClusters::test_8_node_star_hub_auto_discovery -n 0 -vs`
- `uv run pytest tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryMediumClusters::test_10_node_star_hub_auto_discovery -n 0 -vs`
- `uv run pytest tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryMediumClusters::test_10_node_star_hub_auto_discovery -n 5 -vs`
- `uv run pytest tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryMediumClusters::test_13_node_multi_hub_auto_discovery -n 5 -vs`
- `uv run pytest tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryMediumClusters::test_20_node_star_hub_auto_discovery -n 5 -vs`
- `uv run pytest tests/integration/test_gossip_auto_discovery.py::TestCatalogAutoDiscovery::test_three_node_catalog_mesh_discovery -n 5 -vs`
- `uv run pytest tests/integration/test_gossip_auto_discovery.py::TestCatalogAutoDiscovery::test_catalog_refresh_keeps_nodes_alive -n 5 -vs`
- `uv run pytest tests/integration/test_gossip_auto_discovery.py::TestCatalogAutoDiscovery::test_three_node_catalog_mesh_discovery -n 5 -vs`
- `uv run pytest tests/integration/test_fabric_pubsub_forwarding.py tests/integration/test_fabric_rpc_forwarding.py tests/integration/test_fabric_queue_federation.py tests/integration/test_cache_federation_transport.py -n 0 -vs`
- `uv run pytest tests/integration/test_unified_rpc_federation.py -n 0 -vs`
- `uv run pytest tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryLargeClusters::test_30_node_multi_hub_auto_discovery -n 0 -vs`
- `uv run pytest tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryLargeClusters::test_50_node_multi_hub_auto_discovery -n 0 -vs`
- `uv run pytest tests/test_federated_rpc_routing.py -n 0 -vs`
- `uv run pytest tests/test_federated_rpc_integration.py::TestFederatedRPCIntegration::test_federated_rpc_hop_count_limiting -n 0 -vs`
- `uv run pytest tests/test_advanced_cluster_scenarios.py::TestAdvancedClusterScenarios::test_fault_tolerant_routing -n 0 -vs`
- `uv run pytest tests/test_advanced_cluster_scenarios.py::TestAdvancedClusterScenarios::test_dynamic_resource_discovery -n 0 -vs`
- `uv run pytest tests/test_dependency_resolution.py::TestComplexDependencyChains::test_four_step_dependency_chain -n 0 -vs`
- `uv run pytest tests/test_integration_examples.py::TestDistributedExamples::test_resource_based_routing -n 0 -vs`
- `uv run pytest tests/test_production_examples.py::TestHighThroughputScenarios::test_bulk_operations_processing -n 0 -vs`
- `uv run pytest tests/test_production_raft_integration.py::TestProductionRaftIntegration::test_concurrent_operations_safety -n 0 -vs`
- `uv run pytest tests/test_real_world_workflows.py::TestBusinessWorkflows::test_order_processing_workflow -n 0 -vs`
- `uv run pytest tests/test_clean_performance_research.py::TestCleanPerformanceResearch::test_scalability_boundary_research -n 0 -vs`
- `uv run pytest tests/test_advanced_topological_research.py::TestAdvancedTopologicalResearch::test_dynamic_topology_reconfiguration -n 0 -vs`
- `uv run pytest tests/test_advanced_topological_research.py::TestAdvancedTopologicalResearch::test_self_healing_network_partitions_with_automatic_recovery -n 0 -vs`
- `uv run pytest tests/test_consensus_gossip_integration_live.py::TestConsensusGossipLiveIntegration::test_live_multi_node_consensus_completion -n 0 -vs`
- `uv run pytest tests/integration/test_mesh_routing_forwarding.py::test_mesh_routing_gossip_propagation -n 0 -vs`
- `uv run pytest tests/integration/test_unified_rpc_federation.py -n 0 -vs`
- `uv run pytest tests/test_fabric_router_impl.py -n 0 -vs`
- `uv run pytest tests/integration/test_fabric_queue_federation.py -n 0 -vs`
- `uv run pytest tests/integration/test_fabric_pubsub_forwarding.py -n 0 -vs`
- `uv run pytest tests/integration/test_cache_federation_transport.py -n 0 -vs`
- `uv run pytest tests/test_federation_monitoring_endpoints.py -n 0 -vs`
- `uv run pytest tests/test_performance_metrics.py -n 0 -vs`
- `uv run pytest tests/test_fabric_peer_directory.py -n 0 -vs`
- `uv run pytest tests/test_federation_config_system.py -n 0 -vs`
- `uv run pytest tests/test_federation_alerting.py -n 0 -vs`
- `uv run pytest tests/test_federated_rpc_routing.py -n 0 -vs`
- `uv run pytest tests/test_graph_algorithms.py -n 0 -vs`
- `uv run pytest tests/test_federation_monitoring_endpoints.py::TestFederationMonitoringEndpoints::test_graph_topology_snapshot -n 0 -vs`
- `uv run pytest tests/test_hub_discovery.py -n 0 -vs`
- `uv run pytest tests/test_federation_resilience.py -n 0 -vs`
- `uv run pytest tests/test_auto_discovery.py -n 0 -vs`
- `uv run pytest tests/test_gossip_protocol.py -n 0 -vs`
- `uv run pytest tests/test_consensus_gossip_integration.py -n 0 -vs`
- `uv run pytest tests/test_consensus_gossip_integration_live.py -n 0 -vs`
- `uv run pytest tests/test_leader_election_network_adapter.py -n 0 -vs`
- `uv run pytest tests/test_blockchain_federation_integration.py -n 0 -vs`
- `uv run pytest tests/test_clean_performance_research.py -n 0 -vs`
- `uv run pytest tests/test_federation_monitoring_endpoints.py -n 0 -vs`
- `uv run pytest tests/integration/test_unified_system_basic.py -n 0 -vs`
- `uv run pytest tests/integration/test_unified_system_integration.py -n 0 -vs`
- `uv run pytest tests/integration/test_unified_system_comprehensive.py -n 0 -vs`
- `uv run pytest tests/integration/test_fabric_unified_live_workflow.py -n 0 -vs`
- `uv run pytest tests/integration/test_fabric_queue_federation.py -n 0 -vs`
- `uv run pytest tests/integration/test_fabric_pubsub_forwarding.py -n 0 -vs`
- `uv run pytest tests/integration/test_cache_federation_transport.py -n 0 -vs`
- `uv run pytest tests/integration/test_unified_rpc_federation.py -n 0 -vs`

## Notes

- Default multi-hop routing is off; trust policy is cluster-only.
- Streaming replies deferred; buffer logging required for later.
- Some tests are legacy; convert rather than delete, and re-add parity coverage.
- Port audit: `rg "127\\.0\\.0\\.1:\\d" tests` and `rg "port\\s*=\\s*\\d{2,5}" tests` found no fixed port literals.
- Full test suite now passes; remaining work tracked in `docs/UNIFIED_UNIFICATION_PLAN.md`.
