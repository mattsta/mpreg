# Architecture Index — Unified Correctness Program

**Status:** Active  
**Date:** 2026-08-07  
**Master plan:** [`docs/plans/UNIFIED_CORRECTNESS_MASTER_PLAN.md`](../plans/UNIFIED_CORRECTNESS_MASTER_PLAN.md)  
**Burndown:** [`docs/plans/UNIFIED_CORRECTNESS_BURNDOWN.md`](../plans/UNIFIED_CORRECTNESS_BURNDOWN.md)  
**Proof ledger:** [`docs/plans/UNIFIED_CORRECTNESS_PROOF_LEDGER.md`](../plans/UNIFIED_CORRECTNESS_PROOF_LEDGER.md)

These documents are the **source-pointed system maps** for the unified
correctness program. They supersede ad-hoc session notes for Raft, exception
logging, validation gates, and operator observability. Prefer these over
microfixing without reading the relevant map.

## Documents

| Doc                                          | Scope                                                                          |
| -------------------------------------------- | ------------------------------------------------------------------------------ |
| [SYSTEM_MAP.md](SYSTEM_MAP.md)               | Whole product: packages, boot, planes, where Raft sits                         |
| [RAFT.md](RAFT.md)                           | ProductionRaft modules, state machine, election, contact time, config, harness |
| [EXCEPTION_LOGGING.md](EXCEPTION_LOGGING.md) | Error taxonomy, dual-catch, adoption gaps                                      |
| [VALIDATION.md](VALIDATION.md)               | CI gates, test taxonomy, what is / is not proven                               |
| [OBSERVABILITY.md](OBSERVABILITY.md)         | Operator + test/debug surfaces (status, diag, metrics)                         |

## Related canonical docs (do not duplicate)

| Doc                                                                                                          | Role                                    |
| ------------------------------------------------------------------------------------------------------------ | --------------------------------------- |
| [`docs/ARCHITECTURE.md`](../ARCHITECTURE.md)                                                                 | Fabric / envelope product architecture  |
| [`mpreg/server_pkg/consensus.md`](../../mpreg/server_pkg/consensus.md)                                       | Consensus API matrix (must stay honest) |
| [`docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md`](../RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md) | 0.3 CI philosophy                       |
| [`tests/invariants/claims.yaml`](../../tests/invariants/claims.yaml)                                         | Proof claims + non_claims               |

## Rules for implementers

1. **No microfixes without a map.** If confused, re-read the subsystem doc.
2. **Source cites win.** If docs and code disagree, fix the code _or_ the doc
   in the same change; never leave a known lie (e.g. dead pre-vote).
3. **Transitions are methods.** Tests must not assign `current_state` to fake
   role changes (see RAFT.md § encapsulation).
4. **Catch + log together.** `OPERATIONAL_EXCEPTIONS` without
   `log_caught_exception` is incomplete (see EXCEPTION_LOGGING.md).
5. **CI green ≠ fully validated.** See VALIDATION.md scorecard.
6. **Gate after each track.** `bash scripts/release_gate.sh` + track-specific
   Raft/logging suites listed in the master plan.
