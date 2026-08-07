# DistLab T28 — CFT Ops Live + Preset + LWW Heal Honesty (Official)

| Field                 | Value                                                                   |
| --------------------- | ----------------------------------------------------------------------- |
| **Status**            | **Complete**                                                            |
| **Date**              | 2026-08-06                                                              |
| **Authority**         | Continuation after T27 complete (`041f314`)                             |
| **Scope**             | Close operator/live loop for T27 CFT surfaces; LWW heal of CFT residual |
| **Point budget**      | **~90 pts**                                                             |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …`         |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Do **not** claim residual-free under partial-commit + lost-abort (CFT limit).
3. LWW heal is **not** reliable ABORT — it is a later successful put overwriting stale L1.
4. Lab SLI / suite runners are not WAN SLA.
5. Never `python -m`.

## Stages

| Stage  | Exit                                                     | Status |
| ------ | -------------------------------------------------------- | ------ |
| T28-S0 | Official plan                                            | done   |
| T28-S1 | `monitor strong` table shows cft / abort_be / abort_fail | done   |
| T28-S2 | Live metrics e2e asserts CFT caps + prom gauges          | done   |
| T28-S3 | Presets: CFT scenarios in strong-core → ci-core          | done   |
| T28-S4 | DistLab `strong.cft_residual_healed_by_lww`              | done   |
| T28-S5 | ops_cli_tour + runbook teach CFT monitor line            | done   |
| T28-S6 | Full related gate + docs Phase 16 + commit               | done   |

## Delivered surfaces

| Surface                         | What                                                          |
| ------------------------------- | ------------------------------------------------------------- |
| `monitor strong --format table` | `cft=` / `abort_be=` / `abort_fail=` + ABORT best-effort note |
| GCM snapshot                    | always includes `aborts_peer_*` keys (0 default)              |
| Live e2e                        | CFT caps + prom abort/CFT series                              |
| DistLab                         | `strong.cft_residual_healed_by_lww`                           |
| Presets                         | strong-core / ci-core include both CFT scenarios              |
| Curriculum                      | ops_cli_tour requires CFT monitor fields                      |

## Capability / honesty contract

Unchanged from T27. Additive proof only:

| Path         | Claim                                                                         |
| ------------ | ----------------------------------------------------------------------------- |
| CFT residual | peer L1 after partial COMMIT + lost ABORT (not residual-free)                 |
| LWW heal     | later successful majority put for same key can overwrite stale peer L1        |
| Not claimed  | automatic ABORT delivery, BFT, fsync recovery, residual-free under lost abort |

## Non-claims

WAN/Elle/Jepsen/BFT/fsync/STRONG quorum get-delete/SIEM/residual-free under
partial-commit+lost-abort; automatic ABORT delivery.

## Gate (T28-S6)

```bash
uv run pytest tests/chaos/test_t28_residuals.py -q
uv run mpreg distlab run strong.cft_residual_healed_by_lww
uv run mpreg distlab suite --preset strong-core --json
# Full related gate — see DISTLAB_PROOF_LEDGER.md
```
