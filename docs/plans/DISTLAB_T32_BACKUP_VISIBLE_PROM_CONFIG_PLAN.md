# DistLab T32 — Backup/Visible Prom Gauges + Prune Counter + Config-Check (Official)

| Field                 | Value                                                                             |
| --------------------- | --------------------------------------------------------------------------------- |
| **Status**            | **Complete**                                                                      |
| **Date**              | 2026-08-06                                                                        |
| **Authority**         | Continuation after T31 (`6252839`)                                                |
| **Scope**             | Scrapable visible/backups gauges; `backups_pruned` counter; config-check CFT caps |
| **Point budget**      | **~60 pts**                                                                       |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …`                                              |

## Stages

| Stage  | Exit                                                | Status |
| ------ | --------------------------------------------------- | ------ |
| T32-S0 | Official plan                                       | done   |
| T32-S1 | Backend `backups_pruned_total`; GCM/metrics surface | done   |
| T32-S2 | Prom visible/backups/prune series                   | done   |
| T32-S3 | config-check CFT/TTL caps parity tests              | done   |
| T32-S4 | Live e2e + residuals + docs Phase 20 + commit       | done   |

## Delivered

| Surface                                   | What                                                                                |
| ----------------------------------------- | ----------------------------------------------------------------------------------- |
| `StrongLocalBackend.backups_pruned_total` | Cumulative orphan GC                                                                |
| GCM snapshot/status                       | `visible_count`, `backups_count`, `backups_pruned_total`                            |
| Prom                                      | `mpreg_strong_visible`, `mpreg_strong_backups`, `mpreg_strong_backups_pruned_total` |
| config-check                              | CFT/TTL caps asserted                                                               |
| Monitor table                             | `pruned=`                                                                           |
| OpenAPI                                   | `backups_pruned_total` field                                                        |

## Non-claims

Unchanged CFT limits. Gauges are process-local ops signals, not residual-free proof.

## Gate

```bash
uv run pytest tests/chaos/test_t32_residuals.py tests/test_config_check_cli.py -q
uv run pytest tests/testing/test_distlab_live.py::test_distlab_live_strong_metrics_e2e -q
```
