# DistLab + STRONG + shared audit — proof ledger (T16)

Point → test → claim mapping for residual honesty. Scope is **in-process and
same-host multi-process**, not WAN / Elle / BFT / fsync.

| Point / track | Proof path | Claim |
| --- | --- | --- |
| T2 happy 3/5/7 | `tests/testing/test_distlab_strong_scenarios.py`, builtins `strong.happy_*` | INV-CACHE-STRONG-01 |
| T2 soak + lab SLI | `strong.soak_20/50`, `tests/testing/test_distlab_sli.py` | support; non_claim WAN SLA |
| T2 drop prepare/commit | builtins + chaos stress | INV-CACHE-STRONG-01 residual-free |
| T13 drop_abort | `strong.drop_abort` + purge TTL | residual-free after GC |
| T13 live 4-node | `test_distlab_live_strong_happy_4` | INV-CACHE-STRONG-01 live |
| T13 mid-put kill | `test_distlab_live_strong_mid_put_peer_kill` | residual-free survivors |
| T13 audit late joiner | `test_distlab_live_audit_late_joiner` | INV-SHARED-AUDIT-01 live |
| T4 audit scenarios | `test_distlab_audit_scenarios.py` | INV-SHARED-AUDIT-01 |
| T15 Hypothesis drops | `test_random_commit_drop_subset_residual_free` | INV-CACHE-STRONG-01 |
| T15 Hypothesis audit heal | `test_partition_pair_heal_converges` | INV-SHARED-AUDIT-01 |
| T11 metrics JSON | `tests/test_strong_audit_monitoring_endpoints.py`, `tests/server_pkg/test_strong_audit_metrics.py` | support_only ops |
| T11 Prometheus | same + `mpreg_strong_*` / `mpreg_shared_audit_*` | support_only ops |
| T12 CLI | `mpreg monitor strong\|audit`, `mpreg doctor --strong/--audit` | support_only ops |
| Coexistence | `test_strong_audit_coexistence`, live both | both INV-* |

| T17 suite CLI | `mpreg distlab suite`, `test_registry_run_suite_*` | support |
| T17 residual non-committer | `test_success_aborts_prepared_non_committers` | INV-CACHE-STRONG-01 |
| T17 live metrics e2e | `test_distlab_live_strong_metrics_e2e` | support ops |
| T17 multi-GCM RYW | `test_mesh_ryw_all_gcms_after_strong_put` | INV-CACHE-STRONG-01 bridge |
| T17 history taxonomy | `test_history_error_code_and_outcome_counts` | support |
| T18 STRONG get/delete 1012 | `test_strong_get_always_refuses_1012`, `test_strong_delete_always_refuses_1012`, property | INV-CACHE-STRONG-01 refuse |
| T18 capabilities + counters | `strong_status`, `build_strong_metrics`, prom refuse series | support ops |
| T18 smoke preset | `mpreg distlab suite --preset smoke`, `test_registry_run_suite_smoke_preset` | support |
| T18 live audit metrics | `test_distlab_live_audit_metrics_e2e` | support ops INV-SHARED-AUDIT-01 |
| T18 commit+abort drop hyp | `test_full_commit_drop_plus_abort_drop_residual_free_after_gc`, minority success GC | INV-CACHE-STRONG-01 |
| T19 doctor honesty | `evaluate_strong_doctor_payload`, `test_doctor_strong_evaluate_payload_honesty` | support ops |
| T19 refuse scenario | `strong.refuse_get_delete`, smoke preset | INV-CACHE-STRONG-01 refuse |
| T19 monitor summary | `mpreg monitor strong --format table` capabilities line | support ops |
| T20 config-check groups | `test_config_check_*_honesty_warnings`, strong_cache/shared_audit groups | support ops |
| T20 live doctor e2e | `test_distlab_live_doctor_strong_audit_e2e` | support ops |
| T21 OpenAPI schemas | `test_t21_openapi_strong_schema_honesty` | support ops |
| T21 curriculum refuse | `cache_strong_quorum` get/delete 1012 + RYW scenario | INV-CACHE-STRONG-01 teach |
| T22 audit capabilities | `test_t22_shared_audit_metrics_capabilities_honesty` | support ops |
| T22 audit doctor | `evaluate_shared_audit_doctor_payload`, CLI honesty test | support ops |
| T22 OpenAPI audit caps | `test_t22_openapi_shared_audit_schema_capabilities` | support ops |
| T22 curriculum audit | `shared_audit_mesh` metrics capabilities scenario | INV-SHARED-AUDIT-01 teach |
| T22 audit-core preset | `test_registry_run_suite_audit_core_preset` | support |
| T23 config-check audit caps | `test_t23_config_check_shared_audit_capabilities_parity` | support ops |
| T23 live audit caps | `test_distlab_live_audit_metrics_e2e` capabilities asserts | support ops |
| T23 live doctor audit eval | `test_distlab_live_doctor_strong_audit_e2e` evaluate_shared_audit | support ops |
| T24 prom cap gauges | `mpreg_strong_cap_*` / `mpreg_shared_audit_cap_*` endpoint test | support ops |
| T24 honesty alerts | `test_t24_prometheus_alerts_include_honesty_rules` | support ops |
| T24 ops_cli monitor | `ops_cli_tour` monitor strong/audit + doctor scenario | support teach |
| T25 live prom strong caps | `test_distlab_live_strong_metrics_e2e` cap gauges 0/1 | support ops |
| T25 live prom audit caps | `test_distlab_live_audit_metrics_e2e` cap gauges 0/1 | support ops |
| T25 ci-core preset | `test_registry_run_suite_ci_core_preset`, `resolve_preset` | support |
| T26 coexistence prom caps | `test_distlab_live_doctor_strong_audit_e2e` dual cap gauges | support ops |
| T27 abort counters | `aborts_peer_ok/fail`, prom series, GCM status | support ops CFT |
| T27 CFT DistLab | `strong.cft_partial_commit_lost_abort` | honesty (not residual-free) |
| T27 CFT Hypothesis | `test_cft_partial_commit_plus_lost_abort_leaves_peer_l1` | honesty CFT limit |
| T27 curriculum honesty | `test_t27_curriculum_honesty_apps_main` | teach |
| T27 doctor CFT | `evaluate_strong_doctor_payload` fails closed on `cft_only`/`abort_best_effort` false | support ops |
| T27 prom CFT alerts | `MPREGStrongCapCftOnlyMissing`, `MPREGStrongCapAbortBestEffortMissing` | support ops |
| T28 monitor CFT | `monitor strong --format table` cft/abort_be/abort_fail | support ops |
| T28 live CFT prom | `test_distlab_live_strong_metrics_e2e` CFT caps + abort series | support ops |
| T28 LWW heal DistLab | `strong.cft_residual_healed_by_lww` | honesty (not reliable ABORT) |
| T28 presets | strong-core/ci-core include CFT scenarios | support |
| T28 ops curriculum | `ops_cli_tour` CFT monitor fields | teach |
| T29 TTL honesty | `pending_ttl_clears_residual_l1=false`, doctor, prom alert | support ops |
| T29 DistLab TTL | `strong.cft_residual_survives_pending_purge` | honesty (not residual GC) |
| T29 Hypothesis TTL | `test_cft_residual_survives_pending_purge` | honesty |
| T29 visible/backups | GCM snapshot `visible_count` / `backups_count` | support ops |

## Non-claims (do not market)

- Lab SLI p99 ≠ production WAN SLA
- DistLab ≠ Jepsen/Elle/WAN geo
- STRONG put-only MVP (get/delete not majority)
- Shared audit ≠ SIEM / infinite retention / BFT
- CFT only — Byzantine COMMIT lies are `not_bft`
- Partial peer COMMIT apply + lost ABORT may leave peer L1 (ABORT best-effort)

## Gate commands

```bash
uv run mpreg distlab list
uv run mpreg distlab suite --preset smoke --json
uv run pytest tests/testing/ tests/server_pkg/test_shared_audit*.py \
  tests/server_pkg/test_strong_audit_metrics.py \
  tests/core/test_cache_strong*.py tests/integration/test_cache_strong*.py \
  tests/integration/test_shared_audit*.py tests/integration/test_strong_audit_coexistence.py \
  tests/chaos/test_strong_chaos_stress.py tests/chaos/test_shared_audit_chaos.py \
  tests/invariants/test_cache_strong*.py tests/invariants/test_shared_audit*.py \
  tests/test_strong_audit_monitoring_endpoints.py \
  tests/test_cli_strong_audit_monitor.py \
  tests/chaos/test_t14_residuals.py::test_erg_t14_01_openapi_matches_route_table \
  tests/chaos/test_t21_residuals.py tests/chaos/test_t22_residuals.py \
  tests/chaos/test_t23_residuals.py tests/chaos/test_t24_residuals.py \
  tests/chaos/test_t25_residuals.py tests/chaos/test_t26_residuals.py \
  tests/chaos/test_t27_residuals.py tests/chaos/test_t28_residuals.py \
  tests/chaos/test_t29_residuals.py tests/test_config_check_cli.py -q
```
