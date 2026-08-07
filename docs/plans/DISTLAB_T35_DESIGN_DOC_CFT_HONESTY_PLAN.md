# DistLab T35 — Design Doc CFT Residual Honesty (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T34 (`5a375e9`) |
| **Scope** | Correct overclaimed residual-free invariant in design doc for CFT ABORT limit |
| **Point budget** | **~25 pts** |
| **Entry points only** | `uv run pytest …` |

## Problem

`docs/SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md` still states failed puts leave
**no** replica with visible `op_id`. That is the **target under delivered ABORT**,
not the CFT guarantee when ABORT is lost after peer COMMIT apply.

## Fix

Qualify the invariant: residual-free when ABORT is delivered / CFT best-effort;
explicit exception for partial COMMIT + lost ABORT; TTL not residual GC; LWW heal.
