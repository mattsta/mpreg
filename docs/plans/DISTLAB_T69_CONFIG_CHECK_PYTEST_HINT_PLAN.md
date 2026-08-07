# DistLab T69 — config-check pytest residual_ops_hint guide (Official)

| Field                 | Value                                                                       |
| --------------------- | --------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                                |
| **Date**              | 2026-08-06                                                                  |
| **Authority**         | Continuation after T65/T66                                                  |
| **Scope**             | test_config_check_explain_includes_guide asserts strong_cache residual loop |
| **Point budget**      | **~6 pts**                                                                  |
| **Entry points only** | `uv run pytest tests/test_config_check_cli.py`                              |

## Goals

1. Pytest parses --explain JSON guide.strong_cache for residual_ops_hint +
   cache-strong-retry-abort + not auto-heal / ops-driven
2. Residuals + Phase 57

## Non-claims

Pytest guide assert is operator guidance coverage — not auto-heal, not SIEM.
