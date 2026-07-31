# Canonical Consensus API

MPREG exposes more than one consensus-related module for historical reasons.
Use this matrix when integrating:

| Need | Canonical API | Module |
|------|---------------|--------|
| Full Raft (log replication, snapshots) over the fabric | `ProductionRaft` | `mpreg.datastructures.production_raft_implementation` |
| Raft types (state, log entries, RPCs) | types in `production_raft` | `mpreg.datastructures.production_raft` |
| Leader election only (wrapper) | `RaftBasedLeaderElection` | `mpreg.datastructures.leader_election` |
| Lightweight fabric key/value consensus signals | `ConsensusManager` | `mpreg.fabric.consensus` |

**Guidance**

1. New features that need strong consensus should depend on **ProductionRaft**
   with **FabricRaftTransport** (see server `register_raft_node`).
2. Do not start a second TCP Raft stack; fabric transport is mandatory.
3. `ConsensusManager` is for gossip-era coordination helpers, not a substitute
   for Raft safety properties.
4. Prefer one Raft group per coordination domain; avoid dual leaders across
   ProductionRaft and ad-hoc election loops.
