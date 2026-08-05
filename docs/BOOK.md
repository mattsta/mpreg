# MPREG Documentation Book

A single narrative path through the platform. Prefer this over the flat docs
index when learning the system.

## 1. Getting started

1. [GETTING_STARTED.md](GETTING_STARTED.md) — install, first server, first RPC  
2. [examples-curriculum/](examples-curriculum/) — **product-shaped example apps** (preferred)  
3. [EXAMPLES.md](EXAMPLES.md) / `mpreg/examples/` — capability demos + catalog  
4. [ops/SETTINGS_GROUPS.md](ops/SETTINGS_GROUPS.md) — profiles and config groups  

```bash
uv sync
uv run mpreg profile list
uv run mpreg server start-config $(uv run mpreg profile path dev)

# Curriculum (entrypoints only — never python -m / uv run python)
uv run mpreg-example list          # 70 apps (L0–L4 curriculum)
uv run mpreg-example smoke         # 8 apps
uv run mpreg-example suite         # full suite (~70)
uv run mpreg-example demo tier1    # unified former tier demos
```

Curriculum home: [examples-curriculum/](examples-curriculum/) · catalog
[APP_CATALOG.md](examples-curriculum/APP_CATALOG.md) · living plan
[PROJECT_PLAN.md](examples-curriculum/PROJECT_PLAN.md) · API friction
[API_FRICTION.md](examples-curriculum/API_FRICTION.md).

Flagship L4 tours: `global_edge_control_plane` (hub+US/EU) and
`multi_pop_edge_mesh` (hub+US/EU/AP).

Operate examples + platform day-2: [examples-curriculum/OPERATE.md](examples-curriculum/OPERATE.md).
CI: `.github/workflows/ci.yml` runs smoke + full suite via
`scripts/run_demo_smoke.sh` / `scripts/run_demo_suite.sh`.

## 2. Core concepts

1. [ARCHITECTURE.md](ARCHITECTURE.md) — fabric layers (catalog, gossip, routes, envelope)  
2. [MPREG_PROTOCOL_SPECIFICATION.md](MPREG_PROTOCOL_SPECIFICATION.md) — messages, errors, trace  
3. [MPREG_CLIENT_GUIDE.md](MPREG_CLIENT_GUIDE.md) — clients, retries, HA  

**Big idea:** register functions (and optionally datasets/resources); clients
call by name/identity; the fabric routes and resolves dependency graphs.

### Structured errors and traces

- Stable codes: `1000`–`1010`, `1099`, discovery `1101`/`1102` (`mpreg.core.errors.MpregErrorCode`, `mpreg/core/error_codes.json`)
- OpenAPI: `GET /openapi.json` on the monitoring port
- HA clients: `MPREGClusterClient` defaults to `default_ha_policy()`
- Clients raise `MpregError` with `retryable` for HA policies
- Fabric hops carry W3C `traceparent` in header metadata; correlate with
  `/routing/decisions?correlation_id=...`

## 3. Application systems

| System | Guide |
|--------|--------|
| RPC + dependency resolution | GETTING_STARTED, protocol spec, intermediate results |
| Pub/Sub | topic exchange sections in architecture + examples |
| Queues | [SQS_MESSAGE_QUEUE_SYSTEM.md](SQS_MESSAGE_QUEUE_SYSTEM.md) |
| Cache | [CACHING_SYSTEM.md](CACHING_SYSTEM.md), cache federation guides |

## 4. Fabric & multi-cluster

1. Path-vector routes: [FABRIC_ROUTE_POLICIES.md](FABRIC_ROUTE_POLICIES.md)  
2. Link-state (optional): [FABRIC_LINK_STATE_ROUTING.md](FABRIC_LINK_STATE_ROUTING.md)  
3. Route security: [FABRIC_ROUTE_SECURITY.md](FABRIC_ROUTE_SECURITY.md)  
4. Federation topology docs remain under `FEDERATION_*` names for history;
   **runtime plane is the fabric** (`mpreg/fabric/`).

## 5. Discovery & DNS

1. [DISCOVERY_PLATFORM_ROADMAP.md](DISCOVERY_PLATFORM_ROADMAP.md) — status + phases  
2. [DISCOVERY_RUNBOOKS.md](DISCOVERY_RUNBOOKS.md)  
3. [DNS_INTEROP_GUIDE.md](DNS_INTEROP_GUIDE.md) / [DNS_RUNBOOKS.md](DNS_RUNBOOKS.md)  

## 6. Operations

1. [PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md)  
2. [OBSERVABILITY_TROUBLESHOOTING.md](OBSERVABILITY_TROUBLESHOOTING.md)  
3. [ops/SLO_GOLDEN_SIGNALS.md](ops/SLO_GOLDEN_SIGNALS.md)  
4. [ops/SETTINGS_GROUPS.md](ops/SETTINGS_GROUPS.md)  
5. CLI: `mpreg doctor`, `monitor *`, `config-check`, `profile`, `monitor decisions`, `monitor prometheus`  

### Day-2 operator loop

```bash
mpreg config-check mpreg/profiles/cluster.toml
mpreg profile path federated
export MPREG_MONITORING_URL=http://127.0.0.1:<port>
export MPREG_MONITORING_TOKEN=...
mpreg doctor
mpreg monitor status --url "$MPREG_MONITORING_URL"
mpreg monitor decisions --limit 20 --format table
mpreg monitor prometheus | head
```

## 7. Consensus

Canonical API matrix: `mpreg/server_pkg/consensus.md` and architecture notes.
Prefer **ProductionRaft** over fabric transport.

## 8. Extensions (optional)

Not required for RPC/fabric core:

- Blockchain / immutable ledger guides  
- DAO governance guides  
- Merkle / vector clock deep dives  

## 9. Reference

- [TEST_PARITY_MATRIX.md](TEST_PARITY_MATRIX.md) — fabric unification coverage  
- [archive/](archive/) — historical design notes  

## 10. Management plane

Foundation endpoints: `/mgmt/v1/*`, monitoring HTTP, and CLI (`doctor`, `monitor *`).
Details: [MANAGEMENT_UI_CLI_NEXT_STEPS.md](MANAGEMENT_UI_CLI_NEXT_STEPS.md).
