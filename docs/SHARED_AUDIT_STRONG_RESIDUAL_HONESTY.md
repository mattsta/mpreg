# Residual Honesty: Shared Audit + STRONG Put (Beyond MVP)

| Field | Value |
| --- | --- |
| **Status** | Shipped — scoped residual hardening (still not Jepsen-class) |
| **Parent** | `docs/SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md` |
| **Date** | 2026-08-06 |
| **Claims** | Extends proof depth for `INV-SHARED-AUDIT-01`, `INV-CACHE-STRONG-01` |

## Motivation

MVP shipped flag-gated shared audit + majority-commit STRONG put with Hypothesis
lattice/quorum properties, multi-node **in-process** integration, and residual-free
chaos paths. Honest residuals remained:

| Residual | MVP gap | This track |
| --- | --- | --- |
| Fabric wire STRONG RR | In-process transport stand-in; peer commits not mirrored into GCM L1 | **Fix + live 3-server put** |
| Live partition / peer loss | Drop sets on fake transport only | Live shutdown mid-barrier + residual checks |
| Shared-audit epidemic churn | Boot + local publish; in-process anti-entropy | Live multi-drain + reconcile wait on real gossip |
| Linearizability checker | Global non_claim (full Jepsen) | **Bounded single-key history checker** (local, not WAN) |
| Byzantine peers | Unstated | Adversarial ACKs / cluster mismatch **fail-closed** (not BFT) |
| WAN / process-kill / fsync | Unclaimed | **Remain non_claims** |

## Production fix (blocking live STRONG)

Peer `StrongLocalBackend.commit` previously updated only the backend `_visible`
map. Origin GCM applied L1 after success; **peers did not**, so `gcm.get` on a
committer missed STRONG values.

**Fix:** optional `on_visible_apply` / `on_visible_uncommit` callbacks on
`StrongLocalBackend`, wired in `MPREGServer` to `_put_to_l1` / L1 evict-or-restore.
GCM `get` also promotes from `_strong_backend.get_visible` on L1 miss (defensive).

## In scope (ship)

1. **Live 3-node STRONG put** over `ServerCacheTransport` RR
   (`enable_default_cache` + `cache_strong_enabled`).
2. **Live peer shutdown** during barrier → client failure residual-free on survivors.
3. **Live shared-audit** multi-origin drains + wait for cluster scope on all nodes.
4. **History checker** for concurrent single-key STRONG puts (LWW + no dirty fail).
5. **Adversarial peer** transport: bad ACK shapes, cluster_id mismatch → fail closed.
6. Claims proof list + non_claims honesty update.

## Out of scope (remain non_claims)

- Full Jepsen / Elle linearizability across WAN and process crash-restart.
- Byzantine fault **tolerance** (quorum of liars).
- Disk fsync durability of STRONG values across replicas.
- Kernel-level / iptables network partitions (we use process stop + drop injectors).
- STRONG get/delete quorum (still v1.1).

## Test map

| File | Proof |
| --- | --- |
| `tests/integration/test_cache_strong_live_mesh.py` | Live wire STRONG + peer kill residual |
| `tests/integration/test_shared_audit_live_churn.py` | Live gossip multi-drain convergence |
| `tests/invariants/test_cache_strong_history.py` | Bounded single-key history / LWW |
| `tests/core/test_cache_strong_adversarial.py` | Adversarial ACK / mismatch fail-closed |
| `tests/core/test_cache_strong_gcm_bridge.py` | Peer apply/uncommit → GCM L1 |

## Success criteria

- Live STRONG put: value readable via GCM get on ≥ Q committers.
- Failed live put after peer stop: no `op_id` visible residual on remaining backends/GCM.
- Live audit: N drains from multiple origins appear in every node’s `scope=cluster`.
- History checker: no successful put lost under LWW; failed ops leave no residual.
- Docs/claims updated; full related suite green.

## Shipped (2026-08-06)

| Item | Result |
| --- | --- |
| Peer → GCM L1 bridge | `StrongLocalBackend.on_visible_apply/uncommit` + GCM get promote |
| Live STRONG 3-node RR | `test_cache_strong_live_mesh.py` green |
| Live peer-loss residual | Pre-put shutdown + concurrent mid-put kill paths |
| Live audit multi-origin + late joiner | `test_shared_audit_live_churn.py` green |
| Gossip slots handler | `GossipProtocol.mgmt_audit_handler` declared field (slots-safe) |
| Epidemic connected-only peers | `_peers()` filters `is_connected`; re-queue delta if `sent==0` |
| History + adversarial | Hypothesis LWW history; lie/mismatch/drop fail-closed |
| Claims | Proof lists + non_claims refined (still not WAN/Jepsen/BFT/fsync) |

## Bottom line

Originally MVP residual honesty said “not sufficient” for live wire / churn /
history. **Now** those scoped residuals have tests and one production bug
(peer L1 blind spot + dead audit handler on slots) was fixed because of them.
Still **not** claimed: WAN/process-kill-9/disk durability, BFT, full Jepsen/Elle,
kernel partitions.

## Phase 2 — Hardening plan (2026-08-06)

Full ~120-point plan: `docs/SHARED_AUDIT_STRONG_HARDENING_PLAN.md`.

| Item | Result |
| --- | --- |
| Chaos harness | `tests/chaos/harness_strong_audit.py` — partition/delay/drop/dup/malice |
| STRONG stress | 5-node majority, soak, multi-key concurrent, Hypothesis drops/history |
| Expired commit reject | `StrongLocalBackend.commit` → `reason=expired` |
| Server pending purge | `_start_strong_pending_purge_loop` best-effort wall-clock GC |
| Multi-origin LWW | Coordinator bumps `logical_ts` above local visible version |
| Handler fuzz | Bad payloads / cluster mismatch never raise; BFT non_claim documented |
| Audit chaos | Partition/heal, dup DELTA, ineligible, outbound drops, watermark |
| Live expand | Multi-origin concurrent keys + STRONG∥audit coexistence |
| Architecture bugs fixed by tests | LWW same-ms multi-origin soak failure → ts bump |

Still **not** claimed: WAN, kill-9 cold restart, fsync, BFT, Jepsen/Elle, kernel partitions.

## Phase 3 — DistLab first-party product (2026-08-06)

Elevated distributed testing into **`mpreg.testing.distlab`**: history +
pluggable checkers + nemesis + scenario runner + STRONG/audit SUTs. The
platform now tests itself through a reusable lab rather than only ad-hoc
test helpers.

| Surface | Path |
| --- | --- |
| Product | `mpreg/testing/distlab/` |
| Plans (7×25 summary) | `docs/DISTLAB_AND_SEVEN_TRACKS.md` |
| Master plan (7×~60 pts) | `docs/plans/DISTLAB_SEVEN_TRACK_MASTER_PLAN.md` |
| Core self-tests | `tests/testing/test_distlab_core.py` |
| Registry/CLI/generators | `tests/testing/test_distlab_registry.py` |
| STRONG scenarios | `tests/testing/test_distlab_strong_scenarios.py` |
| Audit scenarios | `tests/testing/test_distlab_audit_scenarios.py` |
| Live mesh DistLab | `tests/testing/test_distlab_live.py` |
| CLI | `uv run mpreg distlab list\|run` |
| Re-export | `tests/harness` |

## Phase 4 — Seven-track expansion complete (2026-08-06)

Official master plan executed: generator + registry + live helpers + CLI,
full in-process STRONG/audit scenario catalog, live same-host DistLab suite,
claims/docs gate. See `docs/plans/DISTLAB_SEVEN_TRACK_MASTER_PLAN.md` status
dashboard (T1–T7 complete).

## Phase 4b — Entry points + gap closure (2026-08-06)

Architecture rule enforced: **never `python -m`**. DistLab and concurrent
test runner are top-level CLI groups only:

```bash
uv run mpreg distlab list
uv run mpreg distlab catalog
uv run mpreg distlab run strong.happy_3
uv run mpreg test concurrent --help
```

`python -m mpreg.testing.distlab` exits 2 with a pointer to the entry point.
Gap-closure plan: `docs/plans/DISTLAB_ENTRYPOINT_AND_GAP_CLOSURE_PLAN.md`
(T8–T10). Expanded registry includes single-node, sequential LWW, partition-one,
delay-beyond-timeout, crash-recover, pending-full recover, expired-commit
regression, lie-commit single/both (not_bft).

DistLab is **Jepsen-inspired, not Jepsen**. See claims.yaml non_claims.

## Phase 5 — Ops e2e + residual non-committer fix + suite CLI (2026-08-06)

T11–T16 shipped operator metrics/CLI/live/SLI/proof. T17 adds:

* **Product fix (T16 residual):** after successful majority commit, coordinator
  ABORTs prepared non-committers so minority drop-commit leaves zero pending
  (`aborted_non_committers` in `quorum_info`).
* **History taxonomy:** `History.error_code_counts()` / `outcome_counts()` attached
  to `ScenarioResult.meta`.
* **Suite runner:** `uv run mpreg distlab suite --track T2 --limit N` (excludes
  `not_bft` by default).
* **Live e2e:** STRONG put → scrape `/metrics/strong` + prometheus counters.
* **RYW local:** multi-GCM mesh get after majority put via peer bridge (not quorum get).

Plans: `docs/plans/DISTLAB_T11_OPS_OBS_PERF_CAPABILITY_PLAN.md`,
`docs/plans/DISTLAB_T17_E2E_RESIDUAL_CAPABILITY_PLAN.md`,
`docs/plans/DISTLAB_PROOF_LEDGER.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete.

## Phase 6 — Design-correct refuse + audit metrics e2e + smoke suite (2026-08-06)

T18 closes product honesty on STRONG get/delete and operator smoke:

* **Product:** `ConsistencyLevel.STRONG` **get** and **delete** always refuse with
  `1012 UNSUPPORTED_CONSISTENCY` (quorum get/delete remain v1.1). Counters
  `gets_refused` / `deletes_refused`; `strong_status.capabilities` advertises
  `put_majority_commit` / `local_ryw_after_put` and denies `get_quorum` /
  `delete_quorum`.
* **Ops:** `build_strong_metrics` surfaces capabilities; Prometheus series
  `mpreg_strong_gets_refused_total` / `mpreg_strong_deletes_refused_total`.
* **Smoke suite:** `uv run mpreg distlab suite --preset smoke` (and
  `strong-core` / `audit-core`); `mpreg distlab presets`.
* **Live e2e:** shared-audit multi-origin drain → scrape
  `/metrics/shared-audit` + prom; STRONG refuse counters on live metrics path.
* **Hypothesis:** full commit-drop + abort-drop residual-free after pending GC;
  minority commit-drop success leaves zero pending after GC; property that
  STRONG get/delete always 1012 with EVENTUAL RYW intact.
* **Honest CFT limit:** partial peer COMMIT apply + lost ABORT can leave peer
  L1 until delivered ABORT or LWW success put — not claimed residual-free
  (not BFT, not fsync recovery; pending TTL is not residual GC).

Plans: `docs/plans/DISTLAB_T18_REFUSE_AUDIT_METRICS_SMOKE_PLAN.md`,
`docs/plans/DISTLAB_PROOF_LEDGER.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete.

## Phase 7 — Doctor honesty + refuse scenario + smoke expand (2026-08-06)

T19 operator/ops honesty on top of T18:

* **Doctor:** `evaluate_strong_doctor_payload` fails closed if metrics claim
  `get_quorum` / `delete_quorum`; detail includes refuse counters and capability
  flags when healthy.
* **Monitor:** `mpreg monitor strong --format table` prints a capabilities +
  refuse-counter summary line (not WAN SLA).
* **DistLab:** builtin `strong.refuse_get_delete` (GCM STRONG get/delete 1012 +
  EVENTUAL RYW); included in `smoke` / `strong-core` presets.
* **Registration:** `register_builtins` is additive (new names fill in without
  process restart).

Plan: `docs/plans/DISTLAB_T19_OPS_HONESTY_REFUSE_SCENARIO_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete.

## Phase 8 — Config-check honesty + live doctor e2e (2026-08-06)

T20 operator config + live doctor:

* **config-check:** `strong_cache` and `shared_audit` groups with honest
  capability flags; warnings when STRONG/audit enabled without mon/path/cache;
  always-on put-only / non-SIEM honesty strings when flags are on.
* **Live:** doctor `--strong --audit` against real monitoring HTTP after put +
  refuse + drain; `evaluate_strong_doctor_payload` on live counters.

Plan: `docs/plans/DISTLAB_T20_CONFIG_DOCTOR_E2E_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete.

## Phase 9 — OpenAPI honesty + curriculum refuse + runbook (2026-08-06)

T21 machine-readable contract + teaching:

* **OpenAPI:** `StrongMetricsResponse` / `SharedAuditMetricsResponse` schemas;
  `capabilities.get_quorum` / `delete_quorum` enum `[false]`; path descriptions
  name 1012 refuse + non-WAN / non-SIEM honesty; tags `strong` / `audit`.
* **Curriculum:** `cache_strong_quorum` teaches STRONG get/delete 1012 + EVENTUAL
  RYW + `strong_status.capabilities`.
* **Runbook / SLO / OPERATE:** refuse counter series, presets, config-check,
  curriculum entry points.

Plan: `docs/plans/DISTLAB_T21_OPENAPI_CURRICULUM_RUNBOOK_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete.

## Phase 10 — Shared-audit capability honesty + curriculum parity (2026-08-06)

T22 ops contract parity with STRONG honesty:

* **Metrics:** `build_shared_audit_metrics` always exposes `capabilities`
  (`gset_epidemic` when on; `siem`/`bft`/`infinite_retention`/
  `linearizable_cluster_ops`/`multi_tenant_beyond_cluster_id` always false).
* **Doctor:** `evaluate_shared_audit_doctor_payload` fails closed on dishonest
  capability claims; wired into `mpreg doctor --audit`.
* **Monitor:** `mpreg monitor audit --format table` prints capability summary.
* **OpenAPI:** `SharedAuditMetricsResponse.capabilities` enums false.
* **Curriculum:** `shared_audit_mesh` teaches metrics capability honesty.
* **Preset:** `audit-core` adds digest_repair / duplicate / ineligible.

Plan: `docs/plans/DISTLAB_T22_AUDIT_CAPABILITY_HONESTY_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete, SIEM.

## Phase 11 — Live audit caps e2e + config-check parity (2026-08-06)

T23 closes the honesty loop across config → live HTTP → doctor:

* **config-check:** `shared_audit.capabilities` mirrors metrics (gset when on;
  siem/bft/infinite_retention/linearizable_cluster_ops/
  multi_tenant_beyond_cluster_id always false).
* **Live e2e:** `/metrics/shared-audit` after multi-origin drain asserts
  capability flags; doctor e2e runs `evaluate_shared_audit_doctor_payload`.
* **claims.yaml:** INV-SHARED-AUDIT-01 documents ops honesty contract.

Plan: `docs/plans/DISTLAB_T23_LIVE_AUDIT_CAPS_CONFIG_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete, SIEM.

## Phase 12 — Prometheus capability gauges + ops CLI honesty (2026-08-06)

T24 scrapable honesty + operator teaching:

* **Prom:** `mpreg_strong_cap_*` and `mpreg_shared_audit_cap_*` 0/1 gauges;
  get/delete quorum and SIEM/BFT/… always 0.
* **Alerts:** `mpreg/ops/prometheus_alerts.yml` group `mpreg_strong_shared_audit`
  (pending/drops lab warnings + honesty fail-closed criticals); mirrored in
  `prometheus_alert_rules_yaml()`.
* **Curriculum:** `ops_cli_tour` teaches `monitor strong|audit --format table`
  and `doctor --strong --audit`.

Plan: `docs/plans/DISTLAB_T24_PROM_CAPS_OPS_CLI_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete, SIEM.

## Phase 13 — Live prom caps scrape + ci-core preset (2026-08-06)

T25 closes scrape + CI operator path:

* **Live e2e:** after STRONG put/refuse and audit drain, Prometheus text includes
  `mpreg_strong_cap_*` / `mpreg_shared_audit_cap_*` with honest 0/1 values.
* **Preset:** `ci-core` = ordered deduped union of smoke ∪ strong-core ∪
  audit-core (`uv run mpreg distlab suite --preset ci-core`).

Plan: `docs/plans/DISTLAB_T25_LIVE_PROM_CAPS_CI_PRESET_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete, SIEM.

## Phase 14 — Coexistence live prom caps + OPERATE polish (2026-08-06)

T26 same-process STRONG+audit honesty scrape:

* **Live doctor e2e:** after put + refuse + drain, Prometheus text includes
  both `mpreg_strong_cap_*` and `mpreg_shared_audit_cap_*` with honest 0/1.
* **OPERATE:** ci-core preset, monitor audit table, curriculum trio, OpenAPI
  SharedAuditMetricsResponse pointer.

Plan: `docs/plans/DISTLAB_T26_COEXISTENCE_PROM_CAPS_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete, SIEM.

## Phase 15 — CFT residual honesty + abort metrics (2026-08-06)

T27 measures and documents the CFT ABORT best-effort boundary:

* **Product:** coordinator multi-attempt ABORT (`abort_attempts=3`) with
  `aborts_peer_ok` / `aborts_peer_fail` counters; GCM/metrics/prom surface them.
* **Capabilities:** `cft_only=true`, `abort_best_effort=true` on strong_status /
  OpenAPI / config-check.
* **Doctor / alerts:** fails closed if `cft_only` or `abort_best_effort` is false;
  prom honesty alerts `MPREGStrongCapCftOnlyMissing` /
  `MPREGStrongCapAbortBestEffortMissing`.
* **DistLab:** `strong.cft_partial_commit_lost_abort` proves peer L1 can remain
  after partial COMMIT + lost ABORT (not residual-free; not a product bug).
* **Hypothesis:** property that documents the same CFT residual on a commit peer.
* **Curriculum:** focused live runs of `cache_strong_quorum` + `shared_audit_mesh`.

Plan: `docs/plans/DISTLAB_T27_CFT_RESIDUAL_ABORT_METRICS_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete, SIEM;
residual-free under partial-commit+lost-abort.

## Phase 16 — CFT ops live + LWW heal honesty (2026-08-06)

T28 closes the operator/live loop for T27 CFT surfaces:

* **Monitor:** `monitor strong --format table` shows `cft=` / `abort_be=` /
  `abort_fail=` plus ABORT best-effort dim note.
* **Live e2e:** `/metrics/strong` + prom assert CFT caps and abort counter series
  after put + refuse.
* **Presets:** `strong.cft_partial_commit_lost_abort` and
  `strong.cft_residual_healed_by_lww` in `strong-core` → `ci-core`.
* **DistLab LWW heal:** later successful put overwrites CFT residual peer L1
  (not reliable ABORT).
* **Curriculum:** `ops_cli_tour` requires CFT fields on monitor table.

Plan: `docs/plans/DISTLAB_T28_CFT_OPS_LIVE_PRESET_HEAL_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete, SIEM;
residual-free under partial-commit+lost-abort; automatic ABORT delivery.

## Phase 17 — Pending TTL is not residual L1 GC (2026-08-06)

T29 corrects a dishonest implication that pending TTL clears residual peer L1:

* **Fact:** after COMMIT apply, pending is already gone; `purge_expired_pending`
  only drops uncommitted prepares. Residual L1 survives purge.
* **Cap:** `pending_ttl_clears_residual_l1=false` on status/OpenAPI/config-check;
  doctor fails closed if true; prom gauge always 0 + honesty alert.
* **DistLab:** `strong.cft_residual_survives_pending_purge`.
* **Hypothesis:** `test_cft_residual_survives_pending_purge`.
* **Ops:** `visible_count` / `backups_count` on strong metrics snapshot.

Plan: `docs/plans/DISTLAB_T29_CFT_RESIDUAL_TTL_HONESTY_PLAN.md`.

Still **not** claimed: WAN SLA, Elle, BFT, fsync, STRONG quorum get/delete, SIEM;
residual-free under partial-commit+lost-abort; pending TTL as residual GC.

## Phase 18 — Orphan pre-commit backup GC (2026-08-06)

T30 product fix discovered by CFT residual soak:

* **Bug:** COMMIT apply stored `pre_commit_backup` under `op_id`; lost ABORT never
  popped it → unbounded `_backups` growth across repeated residuals / LWW heal.
* **Fix:** `_prune_orphan_backups` on commit/abort keeps only backups for live
  visible or pending op_ids. Residual L1 itself is unchanged (still CFT).
* **Proof:** DistLab `strong.cft_orphan_backup_gc` + unit uncommit-still-works.

Plan: `docs/plans/DISTLAB_T30_ORPHAN_BACKUP_GC_PLAN.md`.

Still **not** claimed: residual-free under lost ABORT; automatic ABORT delivery.

## Phase 19 — CFT ops surface polish (2026-08-06)

T31 closes operator visibility for T29/T30:

* **Monitor table:** `visible=` / `backups=` / `ttl_gc=` plus dim note that
  pending TTL is not residual GC.
* **Live e2e:** prom `mpreg_strong_cap_pending_ttl_clears_residual_l1` is 0;
  metrics JSON includes `visible_count` / `backups_count`.
* **Curriculum:** `ops_cli_tour` requires TTL/visible honesty tokens.
* **Wording:** remaining "until repair" strings → ABORT/LWW (not TTL).

Plan: `docs/plans/DISTLAB_T31_CFT_OPS_SURFACE_POLISH_PLAN.md`.

Still **not** claimed: residual-free under lost ABORT; WAN/Elle/BFT/fsync.

## Phase 20 — Visible/backups prom + prune counter (2026-08-06)

T32 makes T29/T30 ops signals scrapable:

* **Backend:** `backups_pruned_total` cumulative on orphan GC.
* **GCM/metrics:** `visible_count`, `backups_count`, `backups_pruned_total` on
  snapshot/status/metrics JSON.
* **Prom:** `mpreg_strong_visible`, `mpreg_strong_backups`,
  `mpreg_strong_backups_pruned_total` (process-local; not residual-free proof).
* **config-check:** CFT/TTL caps asserted (`cft_only`, `abort_best_effort`,
  `pending_ttl_clears_residual_l1=false`).
* **Monitor:** table shows `pruned=`.

Plan: `docs/plans/DISTLAB_T32_BACKUP_VISIBLE_PROM_CONFIG_PLAN.md`.

Still **not** claimed: residual-free under lost ABORT; WAN/Elle/BFT/fsync.

## Phase 21 — Curriculum + claims CFT honesty (2026-08-06)

T33 teaches the CFT boundary in the primary STRONG curriculum and claims map:

* **Curriculum:** `cache_strong_quorum` CFT residual + LWW heal scenario; caps
  assert; README non-claims for lost ABORT / TTL / LWW≠ABORT.
* **claims.yaml:** INV-CACHE-STRONG-01 text + non_claims for CFT residual, TTL,
  LWW heal, orphan backup GC; proof tests T27–T33 + curriculum.
* **Doctor:** detail includes `ttl_gc=` / `visible=` / `backups=` / `pruned=`.

Plan: `docs/plans/DISTLAB_T33_CURRICULUM_CLAIMS_CFT_PLAN.md`.

Still **not** claimed: residual-free under lost ABORT; WAN/Elle/BFT/fsync.

## Phase 22 — CACHING_SYSTEM CFT honesty + purge prune (2026-08-06)

T34 product-doc + belt-and-suspenders GC:

* **Docs:** `docs/CACHING_SYSTEM.md` banner + STRONG section state CFT residual,
  TTL ≠ residual GC, LWW ≠ ABORT; ops/DistLab pointers.
* **Purge:** `purge_expired_pending` also runs `_prune_orphan_backups` so the
  server purge loop cannot leave unbounded backups.
* **Hypothesis:** `test_cft_orphan_backups_bounded_under_repeated_residual`.

Plan: `docs/plans/DISTLAB_T34_CACHING_DOC_PURGE_PRUNE_PLAN.md`.

Still **not** claimed: residual-free under lost ABORT; WAN/Elle/BFT/fsync.

## Phase 23 — Design doc CFT residual honesty (2026-08-06)

T35 corrects the design-doc residual-free invariant to CFT best-effort wording
and documents the partial-COMMIT+lost-ABORT exception (aligned with product +
DistLab + claims.yaml).

Plan: `docs/plans/DISTLAB_T35_DESIGN_DOC_CFT_HONESTY_PLAN.md`.

Still **not** claimed: residual-free under lost ABORT; WAN/Elle/BFT/fsync.

## Phase 24 — Abort-fail peer tracking + client honesty (2026-08-06)

T36 surfaces **which peers** exhausted ABORT retries so operators can target
LWW repair without overclaiming residual-free:

* **Product:** `_abort_all` returns fail peers; `last_abort_fail_peers` /
  `last_abort_fail_op_id` / bounded `recent_abort_fails` on coordinator;
  failed put `quorum_info.abort_fail_peers` +
  `abort_best_effort_residual_candidates`.
* **Ops:** `strong_status` / metrics / doctor / `monitor strong` table expose
  peer list; OpenAPI documents fields.
* **DistLab:** `strong.cft_partial_commit_lost_abort` asserts residual peer ∈
  `abort_fail_peers`.
* **Docs:** client guide, APP/FEATURE catalogs, registry blurb, design
  alternatives table, ARCHITECTURE — CFT-qualified residual wording (no
  unqualified "residual-free failures").

Plan: `docs/plans/DISTLAB_T36_ABORT_FAIL_PEER_TRACKING_PLAN.md`.

Still **not** claimed: residual-free under lost ABORT; automatic residual
heal; WAN/Elle/BFT/fsync.

## Phase 25 — retry_abort residual candidates (2026-08-06)

T37 adds best-effort **re-ABORT** for peers listed after exhausted ABORT:

* **Product:** `StrongPutCoordinator.retry_abort(key, op_id, peers=…)` —
  multi-attempt peer ABORT; updates `last_abort_fail_peers` / counters.
* **DistLab:** `strong.cft_retry_abort_clears_residual` (network recovers →
  residual cleared); in strong-core / ci-core.
* **Curriculum:** `cache_strong_quorum` teaches abort_fail_peers + retry_abort
  then LWW heal path; ops_cli asserts `abort_fail_peers=` on monitor table.
* **Hypothesis:** `test_cft_retry_abort_clears_residual_after_heal`.

Still **not** claimed: automatic background heal; residual-free while ABORT
still lost; BFT; WAN/Elle/fsync. Retry is ops-driven CFT best-effort only.

Plan: `docs/plans/DISTLAB_T37_RETRY_ABORT_RESIDUAL_PLAN.md`.

## Phase 26 — GCM strong_retry_abort + product docs (2026-08-06)

T38 exposes retry on the product cache manager and documents the ops path:

* **Product:** `GlobalCacheManager.strong_retry_abort(key, op_id, peers=…)`
  with process-local `retry_abort_calls` / `_cleared` / `_still_fail` counters
  on `strong_status`.
* **Docs:** `CACHING_SYSTEM.md`, client guide, curriculum README teach
  `abort_fail_peers` + `retry_abort` (ops-driven, not automatic heal).

Still **not** claimed: automatic background heal; HTTP mgmt mutation for
retry; residual-free under lost ABORT; WAN/Elle/BFT/fsync.

Plan: `docs/plans/DISTLAB_T38_GCM_RETRY_ABORT_DOCS_PLAN.md`.

## Phase 27 — retry_abort prom/doctor/OpenAPI (2026-08-06)

T39 closes the ops loop for retry counters:

* Prom: `mpreg_strong_retry_abort_{calls,cleared,still_fail}_total`
* Doctor / `monitor strong`: `retry_abort=` / `retry_cleared=`
* OpenAPI schema + runbook metric table
* Design doc CFT exception documents ops-driven retry path

Still **not** claimed: automatic heal; residual-free under lost ABORT;
WAN/Elle/BFT/fsync.

Plan: `docs/plans/DISTLAB_T39_RETRY_ABORT_OPS_SURFACE_PLAN.md`.

## Phase 28 — Live e2e retry_abort counters (2026-08-06)

T40 extends live same-host STRONG metrics e2e:

* `/metrics/strong` includes `retry_abort_*` + `last_abort_fail_peers`
* Live `strong_retry_abort` noop increments `retry_abort_calls`
* Prometheus scrape includes `mpreg_strong_retry_abort_*_total`
* OPERATE curriculum documents retry prom series + monitor fields

Still **not** claimed: WAN residual injection; automatic heal; residual-free
under lost ABORT; Elle/BFT/fsync.

Plan: `docs/plans/DISTLAB_T40_LIVE_RETRY_ABORT_E2E_PLAN.md`.

## Phase 29 — retry_abort_ops_driven honesty cap (2026-08-06)

T41 adds capability flag `retry_abort_ops_driven=true` (always) so metrics never
imply automatic residual heal:

* Caps on `strong_status` / config-check / OpenAPI
* Prom gauge `mpreg_strong_cap_retry_abort_ops_driven` (always 1)
* Doctor fails closed if advertised false; monitor shows `retry_ops=`
* Alert `MPREGStrongCapRetryAbortOpsDrivenMissing`

Still **not** claimed: automatic heal; residual-free under lost ABORT;
WAN/Elle/BFT/fsync.

Plan: `docs/plans/DISTLAB_T41_RETRY_OPS_DRIVEN_CAP_PLAN.md`.

## Phase 30 — Client/RPC strong_retry_abort (2026-08-06)

T42 exposes ops-driven retry on the platform RPC + client façade:

* `PlatformRpc.CACHE_STRONG_RETRY_ABORT` (`mpreg.cache.strong_retry_abort`)
* Plane handler → `GlobalCacheManager.strong_retry_abort`
* `MPREGClient.cache_strong_retry_abort` + `StrongRetryAbortResult`
  (`ops_driven=True`, `automatic_heal=False`, `cft_best_effort` on wire)
* `CacheOpResult` promotes `operation_id` / `quorum_info` (abort_fail_peers)
* `cache_put` RPC emits `operation_id` + `quorum_info` for STRONG failures
* Docs: client guide, CACHING_SYSTEM, runbook, curriculum, OPERATE
* Claims non_claim: RPC path is ops-driven CFT — not auto-heal / BFT / WAN

Still **not** claimed: automatic heal; residual-free under lost ABORT;
WAN/Elle/BFT/fsync. RPC path is CFT best-effort ops only.

Plan: `docs/plans/DISTLAB_T42_CLIENT_RPC_RETRY_ABORT_PLAN.md`.

## Phase 31 — CLI cache-strong-retry-abort (2026-08-06)

T43 adds operator entry-point for the T42 RPC:

* `uv run mpreg client cache-strong-retry-abort --namespace … --key … --op-id …`
* Optional `--peer` (repeatable), `--version`, `--timeout`, `--json`
* Non-zero exit when residual peers still fail; honesty banner on human output
* Docs: runbook, client guide, curriculum OPERATE

Still **not** claimed: automatic heal; residual-free under lost ABORT;
WAN/Elle/BFT/fsync. CLI is thin client of ops-driven CFT RPC only.

Plan: `docs/plans/DISTLAB_T43_CLI_RETRY_ABORT_PLAN.md`.

## Phase 32 — Live client RPC retry_abort e2e (2026-08-06)

T44 proves the T42 client/RPC path on a live 3-node STRONG mesh:

* Seed peer residual via backend prepare+commit (lost-ABORT stand-in)
* `MPREGClient.cache_strong_retry_abort` over live wire clears residual
* GCM `retry_abort_calls` / `retry_abort_cleared` increment (any handler node)
* Product fix: `retry_abort` always `local.abort` + treats self-targeted peers
  (RPC may land on residual peer advertising `cache` resource)
* Test: `tests/integration/test_cache_strong_live_mesh.py::test_live_client_rpc_strong_retry_abort_clears_residual`

Still **not** claimed: automatic heal; residual-free under lost ABORT;
WAN/Elle/BFT/fsync; kernel partition / kill -9 durability.

Plan: `docs/plans/DISTLAB_T44_LIVE_CLIENT_RPC_RETRY_PLAN.md`.

## Phase 33 — Product honesty scan + CLI curriculum (2026-08-06)

T45 locks product-facing docs against reintroducing unqualified residual-free /
auto-heal marketing, and teaches the T43 CLI in `ops_cli_tour`:

* Residual scanner on client guide, CACHING_SYSTEM, runbook, design, curriculum
* Banned phrases: `residual-free failures`, `automatic residual heal`, …
* `ops_cli_tour`: `mpreg client cache-strong-retry-abort --help` honesty

Still **not** claimed: scanner is full-corpus NLP; CLI help is live residual
clear; automatic heal; residual-free under lost ABORT; WAN/Elle/BFT/fsync.

Plan: `docs/plans/DISTLAB_T45_HONESTY_SCAN_CLI_CURRICULUM_PLAN.md`.

## Phase 34 — Client locs pin for retry_abort (2026-08-06)

T46 adds optional routing pin for ops retry:

* `MPREGClient.cache_strong_retry_abort(..., locs=frozenset({"cache"}))`
* CLI `--loc` (repeatable)
* Unpinned still correct after T44 self-target local abort; pin is for
  coordinator affinity / counter scrape, not residual-free guarantee

Still **not** claimed: locs = quorum membership; automatic heal; BFT/WAN.

Plan: `docs/plans/DISTLAB_T46_CLIENT_LOCS_RETRY_PLAN.md`.

## Phase 35 — Live prom cap + client RPC metrics (2026-08-06)

T47 extends live metrics e2e:

* Scrape asserts `mpreg_strong_cap_retry_abort_ops_driven{…} 1`
* Client `cache_strong_retry_abort` noop on live mesh after put
* `/metrics/strong` capabilities.retry_abort_ops_driven remains true
* Test: `tests/testing/test_distlab_live.py::test_distlab_live_strong_metrics_e2e`

Still **not** claimed: cap gauge is a heal toggle; empty-peers noop is residual
clear (see T44); WAN/Elle/BFT/fsync.

Plan: `docs/plans/DISTLAB_T47_LIVE_CAP_CLIENT_RPC_METRICS_PLAN.md`.

## Phase 36 — OpenAPI platform cache RPC catalog (2026-08-06)

T48 documents platform cache FQNs in monitoring OpenAPI components:

* Schema `PlatformCacheRpcCatalog` (get/put/invalidate/strong_retry_abort)
* Honesty enums: `ops_driven=true`, `automatic_heal=false`, `cft_best_effort=true`
* Tag `platform-rpc`; `components.x-mpreg-platform-rpc.cache`
* Wire FQNs over RPC plane — **not** HTTP invoke paths

Still **not** claimed: OpenAPI catalog is an invoke API; auto-heal; BFT/WAN.

Plan: `docs/plans/DISTLAB_T48_OPENAPI_PLATFORM_CACHE_RPC_PLAN.md`.

## Phase 37 — GCM curriculum + DistLab self-target (2026-08-06)

T49 closes the teachable ops stack and DistLab coverage for RPC fan-in:

* Curriculum `cache_strong_quorum`: after coordinator `retry_abort`, a second
  residual is cleared via `GlobalCacheManager.strong_retry_abort` (counters
  `retry_abort_calls` / `retry_abort_cleared`)
* DistLab `strong.cft_retry_abort_self_target` — residual on n1, coordinate as
  n1, `peers=["n1"]` → local.abort clears (simulates client RPC landing on
  residual peer)
* Scenario in `strong-core` / `ci-core`
* `ops_surfaces` meta on `strong.cft_retry_abort_clears_residual` lists full
  stack: coordinator → GCM → platform FQN → client → CLI

Still **not** claimed: self-target is automatic heal; BFT; WAN; locs = quorum.

Plan: `docs/plans/DISTLAB_T49_GCM_CURRICULUM_SELF_TARGET_PLAN.md`.

## Phase 38 — Hypothesis self-target property (2026-08-06)

T50 property-tests the self-target path:

* `test_cft_retry_abort_self_target_clears_local` (n∈[3,7]): residual seeded on
  non-origin peer; coordinator origin_id = residual peer; `peers=[self]` clears
* Residual gate: `tests/chaos/test_t49_residuals.py`,
  `tests/chaos/test_t50_residuals.py`
* Ledger / runbook / OPERATE / claims honesty

Still **not** claimed: in-process property is kernel partition / kill -9 / WAN;
automatic background heal; residual-free under continued ABORT loss.

Plan: `docs/plans/DISTLAB_T50_HYPOTHESIS_SELF_TARGET_PLAN.md`.

## Phase 39 — Doctor op_id + residual ops hint (2026-08-06)

T51 closes the operator loop from metrics → remediation:

* `evaluate_strong_doctor_payload` prints `abort_fail_op_id=`
* When `abort_fail_peers` non-empty, appends ops hint:
  `uv run mpreg client cache-strong-retry-abort … --op-id … --peer …`
  with explicit “not auto-heal” / CFT best-effort wording
* `monitor strong` table/plain shows op_id + same yellow hint
* Helpers: `strong_residual_ops_hint`, `_strong_abort_fail_peers/op_id`
  (resolve top-level or nested coordinator)

Still **not** claimed: hint is automatic heal; doctor fail on residual
candidates (still ok=True — CFT honesty); SIEM; BFT/WAN.

Plan: `docs/plans/DISTLAB_T51_DOCTOR_OP_ID_HINT_PLAN.md`.

## Phase 40 — DistLab GCM.strong_retry_abort (2026-08-06)

T52 first-class DistLab coverage of the product library surface:

* `strong.cft_gcm_retry_abort_clears_residual` — CFT residual → clear drops →
  `GlobalCacheManager.strong_retry_abort` → residual cleared +
  `retry_abort_calls` / `retry_abort_cleared` ≥ 1
* In `strong-core` / `ci-core`
* Complements curriculum GCM path (T49) with registry-runnable scenario

Still **not** claimed: GCM path is auto-heal; BFT; WAN; SIEM.

Plan: `docs/plans/DISTLAB_T52_DISTLAB_GCM_RETRY_PLAN.md`.

## Phase 41 — residual_ops_hint metrics field (2026-08-06)

T53 closes the machine-readable ops loop:

* `format_residual_ops_hint(peers, op_id)` shared helper in `cache_strong`
* `GlobalCacheManager.strong_status()["residual_ops_hint"]`
* `build_strong_metrics` → `/metrics/strong` top-level `residual_ops_hint`
* OpenAPI documents field with honesty (empty when no candidates; not auto-heal)
* CLI doctor/monitor reuse the same formatter (T51)

Still **not** claimed: hint is automatic heal; SIEM; BFT/WAN; residual-free.

Plan: `docs/plans/DISTLAB_T53_RESIDUAL_OPS_HINT_METRICS_PLAN.md`.

## Phase 42 — Hypothesis GCM.strong_retry_abort (2026-08-06)

T54 property-tests the product library surface:

* `test_cft_gcm_retry_abort_clears_residual_after_heal` (n∈[5,7])
* After clear: residual gone, `retry_abort_calls`/`cleared` ≥ 1,
  `residual_ops_hint` empty

Still **not** claimed: in-process property is kernel partition / WAN; auto-heal.

Plan: `docs/plans/DISTLAB_T54_HYPOTHESIS_GCM_RETRY_PLAN.md`.

## Phase 43 — Curriculum ops loop polish (2026-08-06)

T55 teaches the full operator path in curriculum + product docs:

* `ops_cli_tour`: step after retry-abort `--help` documents
  `/metrics/strong` → `residual_ops_hint` / `abort_fail_op_id` → CLI
* `CACHING_SYSTEM.md` ops section lists doctor/monitor/JSON hint fields
* Design doc CFT exception lists GCM + self-target DistLab scenarios and
  `residual_ops_hint`

Still **not** claimed: curriculum help smoke is live residual clear; auto-heal.

Plan: `docs/plans/DISTLAB_T55_CURRICULUM_OPS_LOOP_PLAN.md`.

## Phase 44 — strong-core membership gate (2026-08-06)

T56 locks DistLab preset membership for the full CFT retry_abort surface:

* `test_strong_core_includes_retry_abort_ops_scenarios` requires clears /
  self-target / GCM + prior CFT honesty scenarios in strong-core and ci-core
* Residual: `tests/chaos/test_t56_residuals.py`

Still **not** claimed: preset gate is Jepsen/WAN; residual-free product.

Plan: `docs/plans/DISTLAB_T56_STRONG_CORE_MEMBERSHIP_PLAN.md`.

## Phase 45 — Live residual_ops_hint scrape (2026-08-06)

T57 extends live metrics e2e:

* `/metrics/strong` always includes `residual_ops_hint` (string)
* Empty after clean successful put (no residual candidates)
* Still present after client RPC retry_abort path
* Test: `test_distlab_live_strong_metrics_e2e`

Still **not** claimed: same-host live is WAN; empty hint is residual-free under
lost ABORT elsewhere; automatic heal.

Plan: `docs/plans/DISTLAB_T57_LIVE_RESIDUAL_OPS_HINT_PLAN.md`.

## Phase 46 — Doctor prefers server residual_ops_hint (2026-08-06)

T58: `strong_residual_ops_hint` uses non-empty `body["residual_ops_hint"]`
before rebuilding from peers/op_id — presentation consistency with metrics JSON.

Still **not** claimed: preference is SIEM; auto-heal.

Plan: `docs/plans/DISTLAB_T58_DOCTOR_PREFER_HINT_PLAN.md`.

## Phase 47 — residual_ops_hint key enrichment (2026-08-06)

T59 fills `--namespace` / `--key` in the ops hint when
`recent_abort_fails[].key` matches `last_abort_fail_op_id` (`ns/id` form):

* `format_residual_ops_hint(..., recent_abort_fails=…)`
* GCM `strong_status`, `build_strong_metrics`, doctor rebuild path
* Prefer enriched string over placeholder server hints

Still **not** claimed: process-local recent ring is durable audit/SIEM;
automatic heal; multi-tenant isolation beyond cluster_id.

Plan: `docs/plans/DISTLAB_T59_HINT_KEY_ENRICH_PLAN.md`.

## Phase 48 — Live enriched residual_ops_hint e2e (2026-08-06)

T60 live mesh proves non-empty enriched hint on scrape:

* Seed peer residual (prepare+commit) + origin coordinator abort-fail diagnostics
* `/metrics/strong` and `/mgmt/v1/strong` include
  `residual_ops_hint` with ns/key/op_id/peer + not auto-heal
* Doctor evaluation consumes the same payload
* Test: `test_distlab_live_residual_ops_hint_enriched_e2e`

Still **not** claimed: coordinator field seed is kernel drop/WAN; automatic
heal; residual-free under lost ABORT without ops action.

Plan: `docs/plans/DISTLAB_T60_LIVE_ENRICHED_HINT_PLAN.md`.

## Phase 49 — OpenAPI residual_ops_hint example (2026-08-06)

T61 documents a populated hint example and `recent_abort_fails` item shape
(`key: namespace/identifier`) in monitoring OpenAPI.

Still **not** claimed: OpenAPI example is an invoke API; SIEM; auto-heal.

Plan: `docs/plans/DISTLAB_T61_OPENAPI_HINT_EXAMPLE_PLAN.md`.

## Phase 50 — DistLab residual_ops_hint enrichment (2026-08-06)

T62 first-class DistLab scenario:

* `strong.cft_residual_ops_hint_enriched` — CFT residual remains; GCM status
  hint fills `--namespace distlab --key hint-key --peer n1`
* In `strong-core` / `ci-core`
* Guidance only — does not call retry_abort

Still **not** claimed: scenario clears residual; auto-heal; BFT/WAN.

Plan: `docs/plans/DISTLAB_T62_DISTLAB_HINT_ENRICHED_PLAN.md`.

## Phase 51 — Hypothesis residual_ops_hint enrichment (2026-08-06)

T63 property-tests pure formatter enrichment:

* `test_format_residual_ops_hint_enriches_ns_key`
* Empty peers → `""`; matching `recent_abort_fails` fills ns/key; explicit wins

Still **not** claimed: string property is live mesh; auto-heal; SIEM.

Plan: `docs/plans/DISTLAB_T63_HYPOTHESIS_HINT_ENRICH_PLAN.md`.

## Phase 52 — Catalog + product docs residual_ops_hint (2026-08-06)

T64 documents residual_ops_hint on teachable surfaces:

* FEATURE_CATALOG `cache.strong`
* MPREG_CLIENT_GUIDE helper import + DistLab scenario
* CACHING_SYSTEM ops section

Still **not** claimed: docs are residual-free product claim.

Plan: `docs/plans/DISTLAB_T64_CATALOG_HINT_DOCS_PLAN.md`.

## Phase 53 — config-check explain residual ops loop (2026-08-06)

T65: `mpreg config-check --explain` `strong_cache` guide documents
metrics → residual_ops_hint → `cache-strong-retry-abort` (not auto-heal).

Still **not** claimed: explain text is auto-heal; SIEM.

Plan: `docs/plans/DISTLAB_T65_CONFIG_CHECK_HINT_PLAN.md`.

## Phase 54 — Curriculum config-check residual_ops_hint assert (2026-08-06)

T66: `ops_cli_tour` config-check `--explain` scenario asserts explain output
includes `residual_ops_hint` / `cache-strong-retry-abort` and CFT honesty
(not auto-heal / ops-driven).

Still **not** claimed: curriculum assert is live residual clear; auto-heal; SIEM.

Plan: `docs/plans/DISTLAB_T66_CURRICULUM_EXPLAIN_ASSERT_PLAN.md`.

## Phase 55 — Design-doc residual_ops_hint polish (2026-08-06)

T67: `SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md` documents
`format_residual_ops_hint`, process-local `recent_abort_fails` ns/key fill,
and DistLab `strong.cft_residual_ops_hint_enriched` (guidance only).

Still **not** claimed: design polish is residual-free product claim; BFT/WAN.

Plan: `docs/plans/DISTLAB_T67_DESIGN_HINT_POLISH_PLAN.md`.

## Phase 56 — Ledger + live doctor residual polish (2026-08-06)

T68: proof ledger maps T66–T71; live doctor e2e + enriched residual_ops_hint
e2e remain the multi-process runtime proof path for doctor/hint surfaces.

Still **not** claimed: ledger inventory is Jepsen/WAN; live seed ≠ kernel drop.

Plan: `docs/plans/DISTLAB_T68_LEDGER_LIVE_DOCTOR_PLAN.md`.

## Phase 57 — config-check pytest residual_ops_hint guide (2026-08-06)

T69: `test_config_check_explain_includes_guide` parses JSON `guide.strong_cache`
for `residual_ops_hint` + `cache-strong-retry-abort` + not auto-heal/ops-driven.

Still **not** claimed: pytest coverage is auto-heal; SIEM orchestration.

Plan: `docs/plans/DISTLAB_T69_CONFIG_CHECK_PYTEST_HINT_PLAN.md`.

## Phase 58 — APP_CATALOG ops_cli_tour residual_ops_hint (2026-08-06)

T70: APP_CATALOG elevates `ops_cli_tour` to product tier and documents
STRONG residual_ops_hint ops loop in the app summary.

Still **not** claimed: catalog row is residual-free product claim.

Plan: `docs/plans/DISTLAB_T70_APP_CATALOG_HINT_PLAN.md`.

## Phase 59 — Doctor JSON residual_ops_hint field (2026-08-06)

T71: `mpreg doctor --strong` JSON rows for `metrics_strong` / `mgmt_strong`
always include `residual_ops_hint` (empty when no residual candidates). Same
guidance string as metrics/doctor detail — not a heal toggle.

Still **not** claimed: doctor JSON field is auto-heal; SIEM; residual-free proof.

Plan: `docs/plans/DISTLAB_T71_DOCTOR_JSON_HINT_PLAN.md`.

## Phase 60 — Live doctor residual_ops_hint present (2026-08-06)

T72: live doctor e2e asserts `/metrics/strong` always includes
`residual_ops_hint` (empty after clean put) and Prometheus
`mpreg_strong_abort_fail_peers` is 0 on the happy path.

Still **not** claimed: empty hint is residual-free under lost ABORT elsewhere;
WAN; auto-heal.

Plan: `docs/plans/DISTLAB_T72_LIVE_DOCTOR_HINT_PLAN.md`.

## Phase 61 — Prometheus abort_fail_peers gauge (2026-08-06)

T73: `mpreg_strong_abort_fail_peers` gauge = `len(last_abort_fail_peers)`.
HELP documents CFT residual candidates — not residual-free proof, not auto-heal.

Still **not** claimed: gauge is residual-free proof; auto-heal; WAN SLO.

Plan: `docs/plans/DISTLAB_T73_PROM_ABORT_FAIL_PEERS_PLAN.md`.

## Phase 62 — Hypothesis doctor residual hint (2026-08-06)

T74: property tests — residual peers ⇒ hint with CLI template; empty peers ⇒
no template; dishonest get/delete/cft/abort/ttl/retry caps fail closed.

Still **not** claimed: pure unit properties are live mesh; auto-heal; SIEM.

Plan: `docs/plans/DISTLAB_T74_HYPOTHESIS_DOCTOR_HINT_PLAN.md`.

## Phase 63 — Prometheus residual-candidate info alert (2026-08-06)

T75: `MPREGStrongAbortFailPeersPresent` (severity info, lab_process_local)
when `mpreg_strong_abort_fail_peers > 0` for 5m. Annotations point operators
at residual_ops_hint / cache-strong-retry-abort.

Still **not** claimed: alert is automatic heal; residual-free proof; WAN/BFT/SIEM.

Plan: `docs/plans/DISTLAB_T75_PROM_RESIDUAL_ALERT_PLAN.md`.

## Phase 64 — OpenAPI prom residual gauge (2026-08-06)

T76: `/metrics/strong` OpenAPI description documents
`mpreg_strong_abort_fail_peers` and `residual_ops_hint`.

Still **not** claimed: OpenAPI is auto-heal; SIEM.

Plan: `docs/plans/DISTLAB_T76_OPENAPI_PROM_RESIDUAL_PLAN.md`.

## Phase 65 — Docs prom residual gauge (2026-08-06)

T77: CACHING_SYSTEM + FEATURE_CATALOG document
`mpreg_strong_abort_fail_peers` and `MPREGStrongAbortFailPeersPresent`.

Still **not** claimed: docs are residual-free product claim.

Plan: `docs/plans/DISTLAB_T77_DOCS_PROM_RESIDUAL_PLAN.md`.

## Phase 66 — Curriculum residual_ops_hint assert (2026-08-06)

T78: `cache_strong_quorum` after CFT residual asserts GCM `residual_ops_hint`
includes CLI template + not auto-heal + peer.

Still **not** claimed: curriculum is live auto-heal.

Plan: `docs/plans/DISTLAB_T78_CURRICULUM_HINT_ASSERT_PLAN.md`.

## Phase 67 — Live enriched hint + prom gauge (2026-08-06)

T79: live enriched residual_ops_hint e2e asserts
`mpreg_strong_abort_fail_peers >= 1` while residual present.

Still **not** claimed: live seed is kernel drop; WAN; auto-heal.

Plan: `docs/plans/DISTLAB_T79_LIVE_ENRICHED_PROM_PLAN.md`.

