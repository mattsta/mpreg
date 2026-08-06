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
uv run mpreg monitor strong --url "$MPREG_MONITORING_URL" --format table
uv run mpreg monitor strong --mgmt --url "$MPREG_MONITORING_URL"
uv run mpreg doctor --url "$MPREG_MONITORING_URL" --strong
uv run mpreg distlab run strong.cft_partial_commit_lost_abort
uv run mpreg distlab run strong.cft_residual_healed_by_lww
uv run mpreg distlab run strong.cft_residual_survives_pending_purge
uv run mpreg distlab run strong.cft_orphan_backup_gc
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
| `mpreg_strong_visible` | Local visible L1 strong entries (may include CFT residuals) |
| `mpreg_strong_backups` | Pre-commit backups for live ops |
| `mpreg_strong_backups_pruned_total` | Orphan backups dropped (not residual L1 clear) |
| `mpreg_strong_put_latency_p50_ms` / `_p99_ms` | Lab latency ring |
| `mpreg_strong_cap_put_majority_commit` | 1 when put path available |
| `mpreg_strong_cap_get_quorum` / `_delete_quorum` | **Always 0** in v1 |
| `mpreg_strong_cap_local_ryw_after_put` | 1 when RYW via EVENTUAL |
| `mpreg_strong_aborts_peer_ok_total` | Successful peer ABORT deliveries |
| `mpreg_strong_aborts_peer_fail_total` | Failed peer ABORT (CFT; may leave peer L1) |
| `mpreg_strong_retry_abort_calls_total` | Ops-driven `strong_retry_abort` invocations |
| `mpreg_strong_retry_abort_cleared_total` | retry_abort runs that cleared all targets |
| `mpreg_strong_retry_abort_still_fail_total` | retry_abort runs still failing (CFT) |
| `mpreg_strong_cap_cft_only` | **Always 1** — not BFT |
| `mpreg_strong_cap_abort_best_effort` | **Always 1** — lost ABORT CFT limit |
| `mpreg_strong_cap_pending_ttl_clears_residual_l1` | **Always 0** — purge ≠ residual GC |

### Capabilities (always honest in v1)

| Flag | v1 value | Notes |
| --- | --- | --- |
| `put_majority_commit` | true when coordinator bound | Product path |
| `get_quorum` | **false** | Quorum get is v1.1; always 1012 |
| `delete_quorum` | **false** | Quorum delete is v1.1; always 1012 |
| `local_ryw_after_put` | true | Use EVENTUAL/WEAK get after STRONG put |
| `cft_only` | **true** | Not BFT |
| `abort_best_effort` | **true** | Lost ABORT may leave peer L1 until ABORT/LWW |
| `pending_ttl_clears_residual_l1` | **false** | Purge is not residual GC after COMMIT |
| `retry_abort_ops_driven` | **true** | `strong_retry_abort` is ops-driven, not auto-heal |

Doctor fails closed if metrics claim `get_quorum` or `delete_quorum`, if
`cft_only` / `abort_best_effort` are advertised as false, or if
`pending_ttl_clears_residual_l1` is true.

**CFT limit:** if a peer applies COMMIT but ABORT is lost and the put fails,
that peer may retain L1 for `op_id` indefinitely until a delivered ABORT or a
later successful LWW put. **Pending TTL does not clear residual L1** — after
COMMIT apply the pending slot is already gone; `purge_expired_pending` only
drops uncommitted prepares. DistLab `strong.cft_partial_commit_lost_abort` and
`strong.cft_residual_survives_pending_purge` document this — **not** claimed
residual-free. Watch `mpreg_strong_aborts_peer_fail_total`.

**Abort-fail peers (T36 ops):** JSON `last_abort_fail_peers` /
`last_abort_fail_op_id` / `recent_abort_fails` and failed-put
`quorum_info.abort_fail_peers` list peers that exhausted ABORT retries —
**CFT residual candidates** for targeted repair, not auto-heal and not
residual-free proof. Monitor table prints `abort_fail_peers=…`.

**retry_abort (T37/T42/T43 ops):** after network recovery, re-deliver ABORT via:

* library: `StrongPutCoordinator.retry_abort` /
  `GlobalCacheManager.strong_retry_abort`
* client RPC: `MPREGClient.cache_strong_retry_abort(ns, id, op_id, peers=…)`
  → `mpreg.cache.strong_retry_abort`
* CLI: `uv run mpreg client cache-strong-retry-abort --url … \
  --namespace NS --key ID --op-id OID [--peer PEER…] [--loc cache] [--json]`
  (`--loc` pins resource routing; unpinned may land on any `cache` node)

**Doctor / monitor residual hint (T51/T53):** when `last_abort_fail_peers` is
non-empty, `mpreg doctor --check-strong` detail and `mpreg monitor strong
--format table` print `abort_fail_op_id=` and an ops remediation hint pointing
at `cache-strong-retry-abort` (still CFT; not auto-heal; not doctor-fail).
JSON field `residual_ops_hint` on `/metrics/strong` and GCM `strong_status`

Prometheus gauge `mpreg_strong_abort_fail_peers` = `len(last_abort_fail_peers)` (CFT residual candidates; process-local). Info alert `MPREGStrongAbortFailPeersPresent` (5m) is ops guidance only — not automatic heal. JSON `abort_fail_peer_count` on `/metrics/strong` and doctor JSON rows mirror the same count (via `count_abort_fail_peers`). Monitor table shows `abort_fail_peer_count=`.
carries the same string (empty when no candidates) for automation scrape.
When `recent_abort_fails` records a matching `key` (`namespace/id`), the hint
fills `--namespace` / `--key` (process-local best-effort; not SIEM).
Live e2e: `test_distlab_live_residual_ops_hint_enriched_e2e`. DistLab:
`strong.cft_residual_ops_hint_enriched` (strong-core / ci-core; guidance only).
OpenAPI example on `residual_ops_hint` shows a populated CLI template.

DistLab `strong.cft_retry_abort_clears_residual` proves clear when ABORT can
land; `strong.cft_retry_abort_self_target` proves `peers=[self]` local.abort
when RPC lands on the residual peer; `strong.cft_gcm_retry_abort_clears_residual`
proves the GCM library surface (all in strong-core / ci-core). Live mesh:
`test_live_client_rpc_strong_retry_abort_clears_residual` (client RPC over
`ServerCacheTransport`). Still CFT best-effort — fails while peers drop ABORT;
not background heal. LWW success put remains an alternate overwrite path.

**LWW heal (not reliable ABORT):** a later successful majority put for the same
key can overwrite stale peer L1 (`strong.cft_residual_healed_by_lww`). That is
ordinary LWW, not guaranteed ABORT delivery. Monitor table shows
`cft=` / `abort_be=` / `abort_fail=` / `abort_fail_peers=` on
`monitor strong --format table`.

**OpenAPI platform RPC catalog (T48):** monitoring
`GET $MPREG_MONITORING_URL/openapi.json` →
`components.schemas.PlatformCacheRpcCatalog` lists wire FQNs
(`mpreg.cache.get|put|invalidate|strong_retry_abort`) with honesty flags.
These are RPC-plane names, not HTTP paths.

Presets: `strong-core` and `ci-core` include the CFT honesty scenarios.

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
- **1015+ quorum codes** — insufficient prepares/commits; ABORT path is
  residual-free when delivered (CFT best-effort; see `abort_fail_peers`).
- Rising `puts_fail` / `refused_disabled` / `gets_refused` / `deletes_refused`.
- Pending not draining — check peer mesh, timeouts, purge task.

### Config check

```bash
uv run mpreg config-check path/to.toml --format json --explain
# groups.strong_cache.capabilities.get_quorum == false
# groups.shared_audit.capabilities.siem == false (and bft / infinite_retention / …)
# warnings when cache_strong_enabled without mon/cache
# warnings when mgmt_audit_shared_enabled without path/mon
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
- Prometheus: `mpreg_shared_audit_*` plus capability gauges
  (`mpreg_shared_audit_cap_gset_epidemic`, `_siem`, `_bft`,
  `_infinite_retention`, `_linearizable_cluster_ops` — dishonest caps always 0)
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
uv run mpreg distlab suite --preset ci-core   # smoke ∪ strong-core ∪ audit-core
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
