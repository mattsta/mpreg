# DistLab T74 — Hypothesis doctor residual hint (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T51/T63 |
| **Scope** | Property tests: peers⇒hint; empty⇒no template; dishonest caps fail closed |
| **Point budget** | **~10 pts** |
| **Entry points only** | `uv run pytest tests/test_cli_strong_audit_monitor.py` |

## Goals

1. residual peers always produce cache-strong-retry-abort hint
2. dishonest capabilities always fail doctor
3. Residuals + Phase 62

## Non-claims

Pure unit property tests — not live mesh, not auto-heal, not SIEM.
