# DistLab T18 — STRONG Refuse Correctness, Audit Metrics E2E, Smoke Suite (Official)

| Field                 | Value                                                |
| --------------------- | ---------------------------------------------------- |
| **Status**            | **Complete (gated 222)**                             |
| **Date**              | 2026-08-06                                           |
| **Authority**         | Continuation after T17 complete (`805dc06`)          |
| **Scope**             | STRONG + shared audit + DistLab — not whole-platform |
| **Point budget**      | **~110 pts**                                         |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …`                 |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA.
3. STRONG get/delete **quorum** remains v1.1 non-goal; design-correct refuse is 1012 always.
4. Local RYW after STRONG put uses EVENTUAL/WEAK get (L1 + peer bridge).
5. Never `python -m`.

## Stages

| Stage  | Exit                                                                   |
| ------ | ---------------------------------------------------------------------- |
| T18-S0 | Official plan                                                          |
| T18-S1 | Product: GCM STRONG get/delete always 1012 + counters + capabilities   |
| T18-S2 | Ops: `build_strong_metrics` capabilities; prom refuse counters         |
| T18-S3 | DistLab: `smoke` suite preset (`mpreg distlab suite --preset smoke`)   |
| T18-S4 | Unit: get/delete refuse + status capabilities + metrics keys           |
| T18-S5 | Live e2e: audit publish → scrape `/metrics/shared-audit` + prom        |
| T18-S6 | Residual Hypothesis expand (drop_commit + drop_abort pairs)            |
| T18-S7 | Docs, claims, honesty Phase 6, proof ledger, full related gate, commit |

## Points (summary)

1–10. Plan + residual honesty Phase 6 outline.
11–25. GCM get/delete refuse 1012; `gets_refused` / `deletes_refused`; `strong_status.capabilities`.
26–40. Monitoring builder + Prometheus refuse series.
41–55. Registry/CLI smoke preset (fast in-process subset).
56–70. Unit + CLI tests for refuse paths and smoke suite.
71–90. Live audit metrics scrape e2e (multi-node gossip eligible).
91–100. Hypothesis residual expand (commit-drop + abort-drop).
101–110. Ledger, claims, gate, commit.

## Smoke preset (default names)

Fast in-process subset (excludes live/not_bft/soak):

- `strong.happy_3`
- `strong.drop_prepare`
- `strong.drop_abort` (if registered)
- `audit.multi_origin`

## Gate

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

## Status dashboard

| Item              | Status   |
| ----------------- | -------- |
| S0 plan           | complete |
| S1 product refuse | complete |
| S2 ops metrics    | complete |
| S3 smoke preset   | complete |
| S4–S6 tests       | complete |
| S7 gate/docs      | complete |

## Non-claims (unchanged)

- Not WAN / Elle / Jepsen / BFT / fsync
- Not STRONG quorum get/delete (v1.1)
- Lab SLI ≠ production SLA
