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

## Non-claims (do not market)

- Lab SLI p99 ≠ production WAN SLA
- DistLab ≠ Jepsen/Elle/WAN geo
- STRONG put-only MVP (get/delete not majority)
- Shared audit ≠ SIEM / infinite retention / BFT
- CFT only — Byzantine COMMIT lies are `not_bft`

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
  tests/chaos/test_t14_residuals.py::test_erg_t14_01_openapi_matches_route_table -q
```
