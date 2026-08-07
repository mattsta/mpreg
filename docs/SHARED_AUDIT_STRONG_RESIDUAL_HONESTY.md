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
  L1 until repair — not claimed residual-free (not BFT, not fsync recovery).

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
