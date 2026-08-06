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

## Non-claims (do not market)

- Lab SLI p99 ≠ production WAN SLA
- DistLab ≠ Jepsen/Elle/WAN geo
- STRONG put-only MVP (get/delete not majority)
- Shared audit ≠ SIEM / infinite retention / BFT
- CFT only — Byzantine COMMIT lies are `not_bft`

## Gate commands

```bash
uv run mpreg distlab list
uv run pytest tests/testing/ tests/server_pkg/test_shared_audit*.py \
  tests/server_pkg/test_strong_audit_metrics.py \
  tests/core/test_cache_strong*.py tests/integration/test_cache_strong*.py \
  tests/integration/test_shared_audit*.py tests/integration/test_strong_audit_coexistence.py \
  tests/chaos/test_strong_chaos_stress.py tests/chaos/test_shared_audit_chaos.py \
  tests/invariants/test_cache_strong*.py tests/invariants/test_shared_audit*.py \
  tests/test_strong_audit_monitoring_endpoints.py \
  tests/chaos/test_t14_residuals.py::test_erg_t14_01_openapi_matches_route_table -q
```
