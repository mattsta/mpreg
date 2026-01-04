Mesh/Federation Audit Status (2025-12-30)

Audit complete. Legacy mesh/unified routing/federation components are retired or
migrated into the fabric. The fabric is now the canonical routing + discovery
plane for RPC, pub/sub, queue, cache, and federation.

Canonical modules (fabric)

- mpreg/fabric/catalog.py + mpreg/fabric/index.py (routing catalog + selectors)
- mpreg/fabric/router.py + mpreg/fabric/engine.py (routing engine)
- mpreg/fabric/federation_planner.py (multi-hop path planning)
- mpreg/fabric/route_control.py + mpreg/fabric/route_announcer.py (path-vector)
- mpreg/fabric/gossip.py + mpreg/fabric/consensus.py + mpreg/fabric/membership.py
- mpreg/fabric/federation_hubs.py + mpreg/fabric/federation_registry.py
- mpreg/fabric/monitoring_endpoints.py + mpreg/fabric/performance_metrics.py

Remaining work is tracked in docs/archive/UNIFIED_DISTRIBUTED_FABRIC_PLAN.md
(test parity + example parity + any final doc cleanup).
