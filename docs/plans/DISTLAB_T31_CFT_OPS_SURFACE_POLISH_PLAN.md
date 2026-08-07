# DistLab T31 — CFT Ops Surface Polish (T29/T30 closeout) (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T30 (`091cfde`) |
| **Scope** | Operator/live surface for pending-TTL honesty + backup counts; wording fix |
| **Point budget** | **~45 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …` |

## Stages

| Stage | Exit | Status |
| --- | --- | --- |
| T31-S0 | Plan | done |
| T31-S1 | monitor strong table: visible/backups + ttl_gc | done |
| T31-S2 | Live e2e: pending_ttl gauge 0; visible/backups keys | done |
| T31-S3 | OPERATE + residual honesty wording fix | done |
| T31-S4 | Gate + commit | done |

## Delivered

| Surface | What |
| --- | --- |
| `monitor strong --format table` | `visible=` `backups=` `ttl_gc=` |
| Live e2e | TTL cap gauge 0 + JSON counts |
| Curriculum | ops_cli_tour TTL/visible tokens |
| Docs | OPERATE, runbook, Phase 19 |

## Non-claims

Unchanged CFT limits.

## Gate

```bash
uv run pytest tests/chaos/test_t31_residuals.py -q
uv run pytest tests/testing/test_distlab_live.py::test_distlab_live_strong_metrics_e2e -q
```
