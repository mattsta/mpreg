# DistLab T29 — CFT Residual Survives Pending TTL Honesty (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T28 complete (`920f5b3`) |
| **Scope** | Correct dishonest "pending TTL clears residual L1" wording; prove residual survives purge; ops surface |
| **Point budget** | **~70 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …` |

## Problem

After a peer applies COMMIT, pending is removed. `purge_expired_pending` only
drops **pending** prepares — it does **not** uncommit residual L1. Docs that
said "until pending TTL / later repair" overclaimed TTL as a residual fix.

## Stages

| Stage | Exit | Status |
| --- | --- | --- |
| T29-S0 | Official plan | done |
| T29-S1 | Fix product/docs wording (TTL ≠ residual GC) | done |
| T29-S2 | DistLab `strong.cft_residual_survives_pending_purge` | done |
| T29-S3 | Hypothesis property residual survives purge | done |
| T29-S4 | Backend counts + cap + doctor + prom honesty | done |
| T29-S5 | Gate + Phase 17 + commit | done |

## Delivered surfaces

| Surface | What |
| --- | --- |
| Cap | `pending_ttl_clears_residual_l1=false` |
| Doctor | fails closed if true |
| Prom | gauge always 0 + `MPREGStrongCapPendingTtlClearsResidualClaimed` |
| DistLab | `strong.cft_residual_survives_pending_purge` |
| GCM | `visible_count` / `backups_count` |
| Docs | runbook + Phase 17 |

## Non-claims

Unchanged. Explicit: pending TTL does **not** clear applied residual L1.
Heal paths remain: delivered ABORT, or later LWW success put (T28).

## Gate

```bash
uv run pytest tests/chaos/test_t29_residuals.py -q
uv run mpreg distlab run strong.cft_residual_survives_pending_purge
```
