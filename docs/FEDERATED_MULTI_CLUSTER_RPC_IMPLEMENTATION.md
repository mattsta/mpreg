# Federated Multi-Cluster RPC Implementation

**STATUS: IMPLEMENTED AND OPERATIONAL**

## Executive Summary

Federated RPC runs entirely on the unified Routing Fabric. Function discovery is
handled by fabric catalog deltas (CATALOG_UPDATE), and routing decisions are made
by the fabric router using the routing engine plus the federation planner. There
is no separate HELLO or legacy function broadcast path.

## Architecture Overview

### 1) Type-Safe Function Identity

The fabric uses explicit identity metadata to prevent ambiguity and to support
versioned routing:

- `FunctionIdentity`: name + function_id + semantic version
- `VersionConstraint`: range or exact version matching
- `FunctionSelector`: name/function_id/version constraint filter

### 2) Catalog-Based Discovery

Each node contributes function endpoints to the routing catalog:

- `RpcRegistry` tracks local registrations and RPC specs.
- `LocalFunctionCatalogAdapter` builds `RoutingCatalogDelta` updates.
- The control plane gossips deltas via `CATALOG_UPDATE`.
- `RoutingCatalog` stores `FunctionEndpoint` entries with TTL and resources.

This replaces all legacy HELLO/broadcast discovery.

Direct peers exchange a catalog snapshot on connect for fast convergence.
STATUS messages are diagnostic only; catalog gossip remains the canonical
source for multi-hop propagation.

### 3) Unified Routing + Federation Planning

RPC routing is performed by the fabric router:

- Local match: `RoutingEngine` chooses the best local candidate.
- Cross-cluster: `FabricFederationPlanner` chooses the next hop using the route
  table (path-vector) with graph fallback when needed.
- Load-aware preference: when node load metrics are available, the routing
  engine prefers lower-load targets for the same function identity.
- Reply routing uses the recorded `routing_path` for multi-hop responses.

Routing metadata used in flight:

- `target_cluster`
- `routing_topic` (unified fabric namespace)
- `federation_path` + `federation_remaining_hops`

### 4) Trust + Policy

Connections are authorized by `FederationConnectionManager` using cluster-level
trust policy. Function routing respects:

- Allowed cluster lists and bridge policy
- Resource filters and function identity constraints

## Discovery Flow (High Level)

1. Node registers a function with `function_id` and semantic version.
2. Catalog adapter emits a delta (`FunctionEndpoint` entries with TTL).
3. Fabric gossip distributes the delta to peers.
4. Routing engine indexes the entry and makes it available for selection.

## Routing Flow (High Level)

1. Client issues RPC with optional `function_id` + `version_constraint`.
2. Router selects a matching endpoint via `FunctionSelector` + resource filters.
3. If remote, a multi-hop route is planned and enforced via hop budgets.
4. Responses follow the recorded path; failures can re-route when links drop.

## Observability

- Route decisions include hop budgets and path metadata.
- Routing failures return structured error payloads (no implicit retries).
- Status messages carry node resource metadata for policy and diagnostics.

## Notes

- All discovery and routing are fabric-native.
- No backwards-compatibility paths remain for HELLO-based announcements.
- Tests are built around catalog propagation, routing, and live multi-hop RPC.
