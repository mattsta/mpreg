# DistLab ↔ Raft gap note

**Status:** Documented gap (U7.3)

DistLab core CI (`scripts/ci_distlab_core.sh`) exercises **STRONG cache** happy path
(`strong.happy_3`), not ProductionRaft groups.

| Surface            | Raft coverage                                                |
| ------------------ | ------------------------------------------------------------ |
| DistLab scenarios  | No first-class Raft election/partition scenario in core gate |
| `ci_raft.sh`       | Algorithm + invariants (MockNetwork)                         |
| Fabric integration | `tests/integration/test_fabric_raft_integration.py`          |
| Live multi-process | `tests/test_live_raft_integration.py`                        |

**Future:** add a DistLab scenario `raft.elect_3` / `raft.partition_heal` that
registers `ProductionRaft` on fabric transports — out of band from STRONG tracks.
