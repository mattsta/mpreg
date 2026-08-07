# DistLab T30 — Orphan Pre-Commit Backup GC (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T29 (`e7347a0`); product bug from CFT residual path |
| **Scope** | Drop orphan `_backups` when op_id is no longer live (visible or pending) |
| **Point budget** | **~55 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …` |

## Problem

On COMMIT apply, backend stores `pre_commit_backup` under `op_id` for later
ABORT uncommit. If ABORT is lost (CFT), that backup is never popped. Repeated
failed puts + LWW heal leave unbounded `_backups` growth — resource leak, not
a consistency claim change.

## Fix

`StrongLocalBackend._prune_orphan_backups` after commit apply and abort: keep
only backups whose `op_id` is in current `_key_op.values()` or `_pending`.
Late ABORT for a non-visible op already no-ops uncommit — pruning is safe.
Current residual op still retains backup for successful uncommit.

## Stages

| Stage | Exit | Status |
| --- | --- | --- |
| T30-S0 | Plan | done |
| T30-S1 | `_prune_orphan_backups` + call sites | done |
| T30-S2 | DistLab + unit proofs | done |
| T30-S3 | Gate + Phase 18 + commit | done |

## Non-claims

Still not residual-free under lost ABORT. GC only drops **orphaned backups**,
not residual L1 itself.

## Gate

```bash
uv run pytest tests/chaos/test_t30_residuals.py -q
uv run mpreg distlab run strong.cft_orphan_backup_gc
uv run mpreg distlab suite --preset strong-core --json
```
