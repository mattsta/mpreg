# DistLab T20 — Config-Check Honesty + Live Doctor E2E (Official)

| Field                 | Value                                        |
| --------------------- | -------------------------------------------- |
| **Status**            | **Complete (gated 230)**                     |
| **Date**              | 2026-08-06                                   |
| **Authority**         | Continuation after T19 complete (`6ce4f6a`)  |
| **Scope**             | STRONG + shared audit operator config/doctor |
| **Point budget**      | **~80 pts**                                  |
| **Entry points only** | `uv run mpreg …` / `uv run pytest …`         |

## Stages

| Stage  | Exit                                                       |
| ------ | ---------------------------------------------------------- |
| T20-S0 | Official plan                                              |
| T20-S1 | `config-check` groups + warnings for STRONG / shared audit |
| T20-S2 | Live doctor `--strong --audit` against real mon HTTP       |
| T20-S3 | Tests + docs + ledger + gate + commit                      |

## Non-claims

Unchanged: not WAN SLA, not quorum get/delete, not BFT/fsync.
