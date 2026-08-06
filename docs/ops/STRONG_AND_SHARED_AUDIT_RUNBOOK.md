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

- `GET /metrics/strong` — counters, pending, latency ring (p50/p99 process-local)
- `GET /mgmt/v1/strong` — same payload (mgmt alias)
- `GET /metrics/prometheus` — `mpreg_strong_*` series

### Health values

| health | meaning |
| --- | --- |
| `disabled` | flag off — expected if unused |
| `misconfigured` | flag on but coordinator unbound |
| `degraded_pending` | pending_count > 64 |
| `ok` | coordinator bound, pending healthy |

### Failure modes

- **1012 UNSUPPORTED_CONSISTENCY** — STRONG disabled / coordinator unbound.
- **1015+ quorum codes** — insufficient prepares/commits; residual-free ABORT path.
- Rising `puts_fail` / `refused_disabled` on `/metrics/strong`.
- Pending not draining — check peer mesh, timeouts, purge task.

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

- `GET /metrics/shared-audit` — store size, counters, replicator health
- `GET /mgmt/v1/audit?scope=cluster` — G-Set snapshot
- Prometheus: `mpreg_shared_audit_*`

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
uv run mpreg distlab run strong.happy_3
uv run mpreg distlab run strong.drop_abort
uv run mpreg distlab run audit.partition_heal
```

Live multi-process proofs live under `tests/testing/test_distlab_live.py`.
Lab SLIs (`mpreg.testing.distlab.sli`) are **not** production WAN SLAs.

## Non-claims

See `tests/invariants/claims.yaml` `non_claims` and
`docs/plans/DISTLAB_PROOF_LEDGER.md`.
