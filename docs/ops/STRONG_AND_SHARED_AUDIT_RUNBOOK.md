# STRONG cache put + shared audit — operator runbook

Honest scope: process-local / same-host mesh operations. **Not** WAN multi-region
SLA, BFT, fsync durability, or kill-9 cold-restart proofs.

## Prerequisites

- Monitoring HTTP enabled (`monitoring_enabled=true`, `MPREG_MONITORING_URL`).
- Entry points only: `uv run mpreg …` (never `python -m`).

## STRONG majority-commit put

### Enable

```text
cache_strong_enabled=true
cache_strong_replica_factor=<N>
cache_strong_min_replicas=<N>   # typically N for full mesh lab; Q=floor(N/2)+1
cache_strong_prepare_timeout_s=…
cache_strong_commit_timeout_s=…
```

Requires eligible fabric cache peers (or `lab_single_node` path for N=1).

### Observe

```bash
export MPREG_MONITORING_URL=http://127.0.0.1:<mon-port>
uv run mpreg monitor strong --url "$MPREG_MONITORING_URL" --format json
uv run mpreg monitor strong --mgmt --url "$MPREG_MONITORING_URL"
uv run mpreg doctor --url "$MPREG_MONITORING_URL" --strong
```

HTTP:

- `GET /metrics/strong` — counters, pending, latency ring (p50/p99 process-local), **capabilities**
- `GET /mgmt/v1/strong` — same payload (mgmt alias)
- `GET /metrics/prometheus` — `mpreg_strong_*` series including refuse counters
- OpenAPI: `uv run mpreg doctor` → `/openapi.json` schemas `StrongMetricsResponse`

Prometheus series (process-local; **not** WAN SLO):

| Series | Meaning |
| --- | --- |
| `mpreg_strong_enabled` | Coordinator bound (1/0) |
| `mpreg_strong_puts_ok_total` | Successful majority-commit puts |
| `mpreg_strong_puts_fail_total` | Failed puts (quorum/timeout/conflict) |
| `mpreg_strong_refused_disabled_total` | Put refused — flag off / unbound (1012) |
| `mpreg_strong_gets_refused_total` | Get refused — STRONG get not implemented (1012) |
| `mpreg_strong_deletes_refused_total` | Delete refused — STRONG delete not implemented (1012) |
| `mpreg_strong_pending` | Local pending prepare count |
| `mpreg_strong_put_latency_p50_ms` / `_p99_ms` | Lab latency ring |

### Capabilities (always honest in v1)

| Flag | v1 value | Notes |
| --- | --- | --- |
| `put_majority_commit` | true when coordinator bound | Product path |
| `get_quorum` | **false** | Quorum get is v1.1; always 1012 |
| `delete_quorum` | **false** | Quorum delete is v1.1; always 1012 |
| `local_ryw_after_put` | true | Use EVENTUAL/WEAK get after STRONG put |

Doctor fails closed if metrics claim `get_quorum` or `delete_quorum`.

### Health values

| health | meaning |
| --- | --- |
| `disabled` | flag off — expected if unused |
| `misconfigured` | flag on but coordinator unbound |
| `degraded_pending` | pending_count > 64 |
| `ok` | coordinator bound, pending healthy |
| `unwired` | monitoring provider not attached |

### Failure modes

- **1012 UNSUPPORTED_CONSISTENCY** — STRONG disabled / coordinator unbound **or**
  STRONG **get** / **delete** (design refuse; quorum paths are v1.1).
- **1015+ quorum codes** — insufficient prepares/commits; residual-free ABORT path.
- Rising `puts_fail` / `refused_disabled` / `gets_refused` / `deletes_refused`.
- Pending not draining — check peer mesh, timeouts, purge task.

### Config check

```bash
uv run mpreg config-check path/to.toml --format json --explain
# groups.strong_cache.capabilities.get_quorum == false
# warnings when cache_strong_enabled without mon/cache
```

## Shared audit (G-Set epidemic)

### Enable

```text
mgmt_audit_path=<jsonl>
mgmt_audit_shared_enabled=true
mgmt_audit_shared_reconcile_interval_s=…
mgmt_audit_shared_gossip_targets=…
```

### Observe

```bash
uv run mpreg monitor audit --url "$MPREG_MONITORING_URL" --format json
uv run mpreg doctor --url "$MPREG_MONITORING_URL" --audit
# cluster-scope mutations (requires shared enabled)
uv run mpreg admin audit --scope cluster --url "$MPREG_URL"   # if wired
```

HTTP:

- `GET /metrics/shared-audit` — store size, counters, replicator health, **capabilities**
- `GET /mgmt/v1/audit?scope=cluster` — G-Set snapshot
- Prometheus: `mpreg_shared_audit_*`
- OpenAPI: `SharedAuditMetricsResponse` (capability enums false for SIEM/BFT/…)

```bash
uv run mpreg monitor audit --url "$MPREG_MONITORING_URL" --format table
```

### Capabilities (always honest in v1)

| Flag | v1 value | Notes |
| --- | --- | --- |
| `gset_epidemic` | true when flag on + store present | Product path |
| `siem` | **false** | Not a SIEM |
| `bft` | **false** | CFT gossip only |
| `infinite_retention` | **false** | Bounded watermark window |
| `linearizable_cluster_ops` | **false** | Visibility ≠ mutation linearizability |
| `multi_tenant_beyond_cluster_id` | **false** | cluster_id reject only |

Doctor fails closed if metrics claim any dishonest capability above.

### Status values

| status | meaning |
| --- | --- |
| `disabled` | flag off |
| `misconfigured` | flag on, store missing |
| `degraded_drops` | publish_dropped > 0 |
| `ok_no_peers` | single-node or not yet meshed |
| `ok` | healthy |

## DistLab validation (lab only)

```bash
uv run mpreg distlab list
uv run mpreg distlab presets
uv run mpreg distlab suite --preset smoke
uv run mpreg distlab run strong.happy_3
uv run mpreg distlab run strong.refuse_get_delete
uv run mpreg distlab run strong.drop_abort
uv run mpreg distlab run audit.partition_heal
uv run mpreg distlab suite --preset audit-core
```

Live multi-process proofs live under `tests/testing/test_distlab_live.py`.
Lab SLIs (`mpreg.testing.distlab.sli`) are **not** production WAN SLAs.

Curriculum (in-process teaching apps):

```bash
uv run mpreg-example run cache_strong_quorum
uv run mpreg-example run shared_audit_mesh
```

## Non-claims

See `tests/invariants/claims.yaml` `non_claims` and
`docs/plans/DISTLAB_PROOF_LEDGER.md`.
