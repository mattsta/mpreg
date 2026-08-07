# DistLab T37 — Retry ABORT Residual Candidates (Official)

| Field                 | Value                                                                           |
| --------------------- | ------------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                                    |
| **Date**              | 2026-08-06                                                                      |
| **Authority**         | Continuation after T36 (`13c5fb7`)                                              |
| **Scope**             | Best-effort re-ABORT for peers listed in abort_fail_peers; curriculum + DistLab |
| **Point budget**      | **~35 pts**                                                                     |
| **Entry points only** | `uv run pytest …` / `uv run mpreg …`                                            |

## Problem

T36 surfaces residual _candidates_ but operators have no product API to
**re-deliver ABORT** once the network recovers — only LWW overwrite or waiting
for a later put.

## Goals

1. `StrongPutCoordinator.retry_abort(key, op_id, peers=…)` best-effort re-ABORT.
2. Updates `last_abort_fail_peers` / counters / recent ring.
3. DistLab `strong.cft_retry_abort_clears_residual` (heal path when ABORT can land).
4. Curriculum teaches abort_fail_peers + retry_abort (still CFT; not BFT).
5. Hypothesis: after drop_abort cleared, retry clears residual.

## Non-claims

Retry is still CFT best-effort. Does not guarantee residual clear if peer is
down / still dropping. Not automatic background heal. Not BFT. Not fsync.
