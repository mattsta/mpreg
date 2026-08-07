# DistLab T27 — CFT Residual Honesty + Abort Metrics + Curriculum (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T26 complete (`09d5077`) |
| **Scope** | Document + measure CFT abort best-effort limits; curriculum honesty CI |
| **Point budget** | **~95 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …` |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Do **not** claim residual-free under partial-commit + lost-abort (CFT limit).
3. Lab SLI / suite runners are not WAN SLA.
4. Never `python -m`.

## Stages

| Stage | Exit | Status |
| --- | --- | --- |
| T27-S0 | Official plan | done |
| T27-S1 | Coordinator tracks `aborts_peer_ok` / `aborts_peer_fail`; more abort retries | done |
| T27-S2 | GCM/metrics/prom surface abort counters; caps `cft_only` + `abort_best_effort` | done |
| T27-S3 | DistLab `strong.cft_partial_commit_lost_abort` (honesty demo, not residual-free) | done |
| T27-S4 | Hypothesis property documents CFT residual when abort lost after peer commit | done |
| T27-S5 | Curriculum honesty apps timeouts + focused live test | done |
| T27-S6 | Full related gate + docs Phase 15 + commit | done |

## Capability contract (additive)

| Flag | v1 value | Notes |
| --- | --- | --- |
| `cft_only` | **true** | Not BFT |
| `abort_best_effort` | **true** | Lost ABORT may leave peer L1 until repair/GC |
| existing put/get/delete/ryw | unchanged | |

## Delivered surfaces

| Surface | What |
| --- | --- |
| `StrongPutCoordinator` | `abort_attempts=3`, `aborts_peer_ok` / `aborts_peer_fail` |
| GCM `strong_status` / snapshot | CFT caps + abort counters |
| Prom | `mpreg_strong_aborts_peer_{ok,fail}_total`, `mpreg_strong_cap_cft_only`, `mpreg_strong_cap_abort_best_effort` |
| Alerts | `MPREGStrongCapCftOnlyMissing`, `MPREGStrongCapAbortBestEffortMissing` |
| Doctor | fails closed if `cft_only` or `abort_best_effort` is false |
| DistLab | `strong.cft_partial_commit_lost_abort` |
| Hypothesis | `test_cft_partial_commit_plus_lost_abort_leaves_peer_l1` |
| OpenAPI / config-check | CFT enums + counters description |
| Docs | Phase 15 honesty + runbook |

## Non-claims

Unchanged + explicit: partial peer COMMIT apply + lost ABORT is **not** residual-free.
Still not claimed: WAN/Elle/Jepsen/BFT/fsync/STRONG quorum get-delete/SIEM.

## Gate (T27-S6)

```bash
uv run pytest tests/chaos/test_t27_residuals.py \
  tests/invariants/test_cache_strong_properties.py -k "cft or drop_abort or refuse or t27" -q
uv run mpreg distlab run strong.cft_partial_commit_lost_abort
uv run mpreg distlab suite --preset smoke --json
# Full related gate (~440 + residual CLI/config) — see DISTLAB_PROOF_LEDGER.md
```
