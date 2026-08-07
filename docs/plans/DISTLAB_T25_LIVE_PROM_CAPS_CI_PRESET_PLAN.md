# DistLab T25 — Live Prom Cap Scrape + CI-Core Preset (Official)

| Field                 | Value                                                           |
| --------------------- | --------------------------------------------------------------- |
| **Status**            | **Complete (gated 249+)**                                       |
| **Date**              | 2026-08-06                                                      |
| **Authority**         | Continuation after T24 complete (`9b7ce5b`)                     |
| **Scope**             | Live scrape of honesty gauges + operator/CI suite preset        |
| **Point budget**      | **~65 pts**                                                     |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …` |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA.
3. Capability gauges are honesty advertisements (0/1), not SLIs.
4. Never `python -m`.

## Stages

| Stage  | Exit                                                                          |
| ------ | ----------------------------------------------------------------------------- |
| T25-S0 | Official plan                                                                 |
| T25-S1 | Live e2e: scrape `mpreg_strong_cap_*` / `mpreg_shared_audit_cap_*` after work |
| T25-S2 | Suite preset `ci-core` = smoke ∪ strong-core ∪ audit-core (deduped)           |
| T25-S3 | Tests + full related gate + docs Phase 13 + commit                            |

## Non-claims

Unchanged: not WAN / Elle / Jepsen / BFT / fsync / STRONG quorum get-delete / SIEM.
