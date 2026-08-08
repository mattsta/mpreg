# DistLab ↔ Raft — first-class coverage

**Status:** Closed (U7.3) — DistLab owns a first-class Raft track.

DistLab core CI (`scripts/ci_distlab_core.sh`) and the Raft gate
(`scripts/ci_raft.sh`) both exercise **ProductionRaft** via DistLab scenarios.

## Surface matrix

| Surface                 | Raft coverage                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------- |
| DistLab scenarios       | `raft.elect_3`, `raft.elect_5`, `raft.partition_majority`, `raft.partition_heal`, `raft.leader_stepdown_reelect` |
| Adapter                 | `mpreg.testing.distlab.adapters.raft.RaftSUT` + `RaftLabNetwork`                                                 |
| Checkers                | `UniqueLeaderChecker`, `RaftSmAgreementChecker`, `default_raft_checkers`                                         |
| Suite presets           | `raft-core`; included in `smoke` + `ci-core`                                                                     |
| `ci_raft.sh`            | unit/invariants **+** DistLab raft pytest **+** CLI `raft.elect_3`                                               |
| `ci_distlab_core.sh`    | list greps `raft.elect_3`; runs CLI + pytest elect_3                                                             |
| Fabric integration      | `tests/integration/test_fabric_raft_integration.py`                                                              |
| Live multi-process      | `tests/test_live_raft_integration.py`                                                                            |
| Algorithm / MockNetwork | `tests/test_production_raft_integration.py`, invariants                                                          |

## Honest non-claims

DistLab Raft is **in-process** memory storage + controllable lab network:

- Not Fabric wire transport (see fabric integration tests).
- Not multi-process live mesh (see live raft tests).
- Not BFT / Byzantine peers.
- Not dynamic membership changes.
- Not WAN geo latency models.

## Operator commands

```bash
uv run mpreg distlab list | grep raft
uv run mpreg distlab run raft.elect_3
uv run mpreg distlab suite --preset raft-core
bash scripts/ci_raft.sh
bash scripts/ci_distlab_core.sh
```
