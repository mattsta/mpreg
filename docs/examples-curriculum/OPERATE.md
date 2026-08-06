# Operate: Configure, Start, Run, Manage (Examples + Platform)

This guide is the **day-0 → day-2** companion for curriculum apps and for MPREG
as a whole. Prefer the BOOK for deep architecture; use this for muscle memory.

---

## 0. Prerequisites

```bash
uv sync
uv run mpreg --help
uv run mpreg-example --help
uv run mpreg examples --help
```

Raise file descriptor limits for dense local clusters (tests and multi-app):

```bash
ulimit -n 1048576   # macOS/Linux shells as permitted
```

---

## 1. Configure

### Profiles (recommended for real servers)

```bash
uv run mpreg profile list
uv run mpreg profile path dev
uv run mpreg config-check $(uv run mpreg profile path dev)
# Human field guide for each settings group (Phase Q ERG):
uv run mpreg config-check $(uv run mpreg profile path dev) --explain
```

### Server metrics fabric hops (Phase Q/S)

`ServerMetricsTracker.snapshot()["fabric"]` exposes route-decision hop stats:
`decisions_total`, `decisions_buffered`, `blackhole_count`, `reachable_ratio`,
`avg_hops`, `max_hops`. Curriculum apps pretty-print via
`format_server_snapshot()` (Phase S).

### Settings in examples

Curriculum apps build `MPREGSettings` in code with **dynamic ports**. For a
long-lived process, copy patterns from:

- `mpreg/examples/persistence_settings.toml`
- `mpreg/profiles/*.toml`

Important fields:

| Field               | Role                             |
| ------------------- | -------------------------------- |
| `port` / auto       | Client WebSocket endpoint        |
| `name`              | Human node id                    |
| `resources`         | Routing `locs` sets              |
| `peers`             | Bootstrap mesh                   |
| `cluster_id`        | Fabric / multi-cluster identity  |
| `federation_config` | Bridging policy between clusters |
| `monitoring_port`   | HTTP ops surface (or auto)       |
| `log_level`         | Prefer INFO for demos            |

### Environment

| Variable                 | Use                               |
| ------------------------ | --------------------------------- |
| `MPREG_MONITORING_URL`   | CLI monitor commands              |
| `MPREG_MONITORING_TOKEN` | If auth enabled                   |
| `MPREG_DEBUG_RAFT`       | Raft diagnostics (off by default) |

---

## 2. Start

### Ephemeral (curriculum default)

Apps start servers in-process and stop them on exit:

```bash
uv run mpreg-example run hello_rpc
```

### Long-lived local server

```bash
uv run mpreg server start-config mpreg/profiles/dev.toml
# or
uv run mpreg server start-config mpreg/examples/persistence_settings.toml
```

Note printed URLs and monitoring base URL.

### Multi-node local mesh

Either:

1. Run a curriculum multi-node app (`hello_cluster`, `multi_region_shop`), or
2. Start multiple `server start-config` processes with distinct ports and `peers`.

Always prefer **allocator-driven ports** when scripting.

---

## 3. Run (clients & apps)

### Curriculum runner

```bash
uv run mpreg-example list                 # 70 apps
uv run mpreg-example list --kind plane
uv run mpreg-example describe order_intake
uv run mpreg-example run order_intake
uv run mpreg-example smoke                 # 8 apps
uv run mpreg-example suite                 # all suite apps (~70)
uv run mpreg-example demo tier1
uv run mpreg-example demo product_vertical
uv run mpreg-example bundles
uv run mpreg-example path hello_rpc

# Same runner via main CLI
uv run mpreg examples list
uv run mpreg examples run order_intake
uv run mpreg demo tier1                    # delegates to mpreg-example

# Pytest (live app mains)
uv run pytest tests/examples_apps -m example_smoke
uv run pytest tests/examples_apps -m example_suite
uv run pytest tests/examples_apps

# Shell wrappers (CI + local)
scripts/run_example_apps_smoke.sh
scripts/run_example_apps_suite.sh
# Aliases used by .github/workflows/ci.yml:
scripts/run_demo_smoke.sh
scripts/run_demo_suite.sh
```

### Nightly / CI suite

GitHub Actions workflow `.github/workflows/ci.yml` runs on every push and PR:

| Job          | Script                      | Scope                      |
| ------------ | --------------------------- | -------------------------- |
| `demo-smoke` | `scripts/run_demo_smoke.sh` | smoke subset (~8 apps)     |
| `demo-suite` | `scripts/run_demo_suite.sh` | full `mpreg-example suite` |

Both scripts are entrypoint-only (`uv run mpreg-example …`). For a local
nightly-equivalent run:

```bash
uv run mpreg-example suite
# or
scripts/run_example_apps_suite.sh
```

**Do not** use `python -m`, `uv run python`, or bare script paths for examples.

### Direct client patterns (best practice)

**Single endpoint:**

```python
async with MPREGClientAPI(f"ws://127.0.0.1:{port}") as client:
    result = await client.call("my_fn", arg, locs=frozenset(["resource"]))
```

**HA multi-seed:**

```python
async with MPREGClusterClient(seed_urls=(url_a, url_b)) as client:
    result = await client.call("my_fn", arg, locs=frozenset(["resource"]))
```

**Dependency graph** — use `RPCCommand` chains (see `hello_rpc` / `hello_cluster`).

**Call policies** — deadlines and retries via `ClientCallPolicy` /
`default_ha_policy()` (cluster client applies HA policy by default).

### Legacy capability demos

```bash
uv run mpreg demo tier1 rpc
uv run mpreg demo all
scripts/run_demo_smoke.sh
scripts/run_example_apps_smoke.sh
```

---

## 4. Manage & observe

### Health / doctor

```bash
export MPREG_MONITORING_URL=http://127.0.0.1:<monitoring-port>
uv run mpreg doctor
uv run mpreg monitor status --url "$MPREG_MONITORING_URL"
uv run mpreg monitor decisions --limit 20 --format table
uv run mpreg monitor prometheus | head
```

### HTTP contract

- OpenAPI: `GET {MPREG_MONITORING_URL}/openapi.json`
- Routing decisions: `GET .../routing/decisions`
- Mgmt mutations: `POST .../mgmt/v1/nodes/drain`, `.../peers/detach`, `.../policy/apply`
- Audit: `GET .../mgmt/v1/audit` (`scope=local` default; `scope=cluster` when
  `mgmt_audit_shared_enabled`)
- Status endpoints as documented in OpenAPI surface

### Admin mutations + shared audit

```bash
uv run mpreg admin drain --url "$MPREG_MONITORING_URL" ...
uv run mpreg admin detach --url "$MPREG_MONITORING_URL" ...
uv run mpreg admin audit --url "$MPREG_MONITORING_URL"
# Cluster forensic view (requires mgmt_audit_shared_enabled on nodes):
# GET $MPREG_MONITORING_URL/mgmt/v1/audit?scope=cluster
```

Curriculum: `shared_audit_mesh` (G-Set + metrics capabilities honesty),
`live_partition_chaos`, `ops_cli_tour`.
Settings: `docs/ops/SETTINGS_GROUPS.md` (mgmt audit + shared flags).
DistLab: `uv run mpreg distlab suite --preset audit-core`.

### Cache STRONG put (flag-gated)

Default **off** (`1012`). Production path: set `cache_strong_enabled=true` with
≥ `cache_strong_min_replicas` live peers; use `ConsistencyLevel.STRONG` on put
only. Curriculum: `cache_strong_quorum`. Docs: `docs/CACHING_SYSTEM.md` §STRONG.

Operator surfaces (monitoring HTTP — not WAN SLA):

```bash
export MPREG_MONITORING_URL=http://127.0.0.1:<mon-port>
uv run mpreg monitor strong --url "$MPREG_MONITORING_URL" --format table
uv run mpreg monitor audit --url "$MPREG_MONITORING_URL" --format table
uv run mpreg doctor --url "$MPREG_MONITORING_URL" --strong --audit
uv run mpreg config-check mpreg/profiles/dev.toml --format json --explain
uv run mpreg distlab suite --preset smoke
uv run mpreg distlab suite --preset ci-core   # smoke ∪ strong-core ∪ audit-core
uv run mpreg distlab suite --track T2 --limit 5
uv run mpreg-example run cache_strong_quorum
uv run mpreg-example run shared_audit_mesh
uv run mpreg-example run ops_cli_tour
```

Prom honesty gauges (process-local, not WAN SLO): `mpreg_strong_cap_*`,
`mpreg_shared_audit_cap_*` (get/delete quorum and SIEM/BFT always 0;
`mpreg_strong_cap_cft_only` / `mpreg_strong_cap_abort_best_effort` always 1;
`mpreg_strong_cap_retry_abort_ops_driven` always 1 — honesty, not auto-heal).
Abort series: `mpreg_strong_aborts_peer_ok_total` /
`mpreg_strong_aborts_peer_fail_total` (CFT best-effort; not residual-free under
partial-commit+lost-abort). Pending TTL is **not** residual GC
(`mpreg_strong_cap_pending_ttl_clears_residual_l1` always 0). CFT DistLab:
`strong.cft_partial_commit_lost_abort`, `strong.cft_residual_healed_by_lww`,
`strong.cft_residual_survives_pending_purge`, `strong.cft_orphan_backup_gc`,
`strong.cft_retry_abort_clears_residual`,
`strong.cft_retry_abort_self_target` (RPC fan-in peers=[self]),
`strong.cft_gcm_retry_abort_clears_residual` (GCM library surface; all in
strong-core / ci-core). Doctor/monitor show `abort_fail_op_id=` and an ops
hint → `cache-strong-retry-abort` when residual candidates present;
JSON `residual_ops_hint` on `/metrics/strong` carries the same string
(empty when none; not auto-heal; may fill ns/key from `recent_abort_fails`).
Doctor JSON (`mpreg doctor --strong --format json`) includes `residual_ops_hint` on `metrics_strong` / `mgmt_strong` rows (empty when none).
DistLab `strong.cft_residual_ops_hint_enriched` proves the guidance surface
without clearing residual. Monitor table shows `cft=` / `abort_be=` /
`abort_fail=` / `abort_fail_peers=` / `retry_abort=` / `retry_cleared=` /
`ttl_gc=` / `visible=` / `backups=` / `pruned=`. `abort_fail_peers` lists CFT
residual candidates (ops only). Prom also exposes `mpreg_strong_visible`,
`mpreg_strong_backups`, `mpreg_strong_backups_pruned_total`,
`mpreg_strong_retry_abort_{calls,cleared,still_fail}_total` (process-local;
not residual-free proof; retry is ops-driven not auto-heal). Prometheus gauge `mpreg_strong_abort_fail_peers` counts residual candidates (info alert `MPREGStrongAbortFailPeersPresent`; not auto-heal). JSON field `abort_fail_peer_count` on `/metrics/strong` mirrors the same count. Doctor detail/monitor table show `abort_fail_peer_count=`; doctor JSON rows include the field (0 when clean).
Ops re-ABORT after recovery: library `strong_retry_abort`, client
`MPREGClient.cache_strong_retry_abort` → platform RPC
`mpreg.cache.strong_retry_abort`, or CLI
`uv run mpreg client cache-strong-retry-abort` (still CFT; not background heal).

Runbook: `docs/ops/STRONG_AND_SHARED_AUDIT_RUNBOOK.md`.
OpenAPI: `GET $MPREG_MONITORING_URL/openapi.json` → `StrongMetricsResponse` /
`SharedAuditMetricsResponse`.

### Correlation

- Fabric hops may carry W3C `traceparent` in metadata.
- Curriculum `hello_trace` shows **in-process** unified monitoring timelines
  (always on, no external collector required).

---

## 5. Operate under failure (teaching stance)

| Scenario           | What to run                     | What to expect                       |
| ------------------ | ------------------------------- | ------------------------------------ |
| Seed down          | `ha_client_failover`            | Other seed serves call               |
| Cross-cluster path | `multi_region_shop`             | Federated RPC with bridging config   |
| Slow mesh          | raise timeouts in client policy | Structured timeout errors, not hangs |
| Shared audit lag   | `shared_audit_mesh`             | Eventual G-Set visibility, not SIEM  |
| STRONG quorum loss | `cache_strong_quorum`           | `1015` when ABORT delivered; CFT residual possible; not WAN SLA |
| Full test pressure | concurrent runner + `ulimit`    | See testing docs                     |

For chaos injection, prefer `mpreg.testing.faults.FaultInjector` in curriculum
apps (`chaos_checkout`, `packet_loss_chaos`, `chaos_crash_recover`,
`live_partition_chaos`) — do not randomize production defaults.

---

## 6. Production exit ramp (checklist)

When promoting an example pattern:

1. **Config** — real profile TOML; `config-check` clean.
2. **Identity** — stable `cluster_id`, resource taxonomy, function names.
3. **Client** — `MPREGClusterClient` + explicit deadlines.
4. **Observability** — monitoring URL, scrape prometheus, alert rules under `mpreg/ops/`.
5. **Data planes** — choose queue delivery guarantees and cache levels deliberately.
   Enable `cache_strong_enabled` only when majority-commit put is required.
6. **Fabric** — route policies and security before exposing clusters.
7. **Consensus** — only if you need it; Raft is not free.
8. **Ops audit** — optional `mgmt_audit_path` + `mgmt_audit_shared_enabled` for
   cluster forensic visibility (bounded window; not a SIEM).
9. **Load & soak** — do not ship on demo-only timings.

---

## 7. Troubleshooting examples

| Symptom         | Check                                                                             |
| --------------- | --------------------------------------------------------------------------------- |
| Port in use     | Another demo still running; wait for cleanup; use curriculum apps (dynamic ports) |
| `ExampleFailed` | Read assertion message; often settle time under load — re-run smoke               |
| HA call fails   | Both seeds dead; discovery interval; see cluster client docs                      |
| Federated miss  | `cluster_id` / bridging config / peer URL; fabric ready wait                      |
| FD exhaustion   | `ulimit -n`; fewer parallel suites                                                |

---

## 8. Related docs

- [GETTING_STARTED.md](../GETTING_STARTED.md)
- [MPREG_CLIENT_GUIDE.md](../MPREG_CLIENT_GUIDE.md)
- [PRODUCTION_DEPLOYMENT.md](../PRODUCTION_DEPLOYMENT.md)
- [OBSERVABILITY_TROUBLESHOOTING.md](../OBSERVABILITY_TROUBLESHOOTING.md)
- [FABRIC_ROUTE_POLICIES.md](../FABRIC_ROUTE_POLICIES.md)
- [ops/SETTINGS_GROUPS.md](../ops/SETTINGS_GROUPS.md)
- [SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md](../SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md)
- [CACHING_SYSTEM.md](../CACHING_SYSTEM.md) — STRONG majority-commit put
- [MANAGEMENT_UI_CLI_NEXT_STEPS.md](../MANAGEMENT_UI_CLI_NEXT_STEPS.md) — mgmt + shared audit
