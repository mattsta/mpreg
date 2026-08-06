# DistLab T17 — E2E Ops, Residual Proof, Suite CLI, RYW (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete (gated 214)** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T11–T16 complete (`b5f7833`) |
| **Scope** | STRONG + shared audit + DistLab — not whole-platform |
| **Point budget** | **~120 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …` |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA.
3. STRONG get/delete **quorum** remains v1.1 non-goal; local RYW after put is in-scope.
4. Never `python -m`.

## Stages

| Stage | Exit |
| --- | --- |
| T17-S0 | Official plan |
| T17-S1 | History error taxonomy + ScenarioResult meta |
| T17-S2 | `mpreg distlab suite` multi-scenario runner |
| T17-S3 | Unit proof: success-path abort non-committers |
| T17-S4 | Multi-GCM RYW + peer bridge after STRONG put |
| T17-S5 | Live e2e: put → scrape `/metrics/strong` |
| T17-S6 | Docs, claims, honesty, full related gate, commit |

## Points (summary)

1–15. `History.error_code_counts()` / fail taxonomy helper.
16–30. Scenario runner attaches `meta["error_codes"]` + duration already present.
31–50. Registry `run_suite(prefix|track|tags)` + CLI `distlab suite`.
51–65. Explicit residual test for minority drop-commit success path.
66–85. Multi-node GCM RYW: origin + peer get after majority put.
86–100. Live mesh monitoring scrape counters after real put.
101–120. Proof ledger + residual honesty Phase 5 + claims + gate.

## Gate

```bash
uv run mpreg distlab list
uv run mpreg distlab suite --track T2 --limit 5
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

## Status dashboard

| Item | Status |
| --- | --- |
| S0 plan | complete |
| S1–S2 taxonomy + suite | complete |
| S3 residual proof | complete |
| S4 RYW | complete |
| S5 live metrics e2e | complete |
| S6 gate/docs | complete |
