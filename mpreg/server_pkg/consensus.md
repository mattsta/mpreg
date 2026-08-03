# Canonical Consensus API

MPREG exposes more than one consensus-related module for historical reasons.
Use this matrix when integrating:

| Need | Canonical API | Module |
|------|---------------|--------|
| **Application import (preferred)** | facade | `mpreg.consensus` |
| Full Raft (log replication, snapshots) over the fabric | `ProductionRaft` | `mpreg.consensus` / `production_raft_implementation` |
| Raft types (state, log entries, RPCs) | types | `mpreg.datastructures.production_raft` |
| Leader election only (wrapper) | `RaftBasedLeaderElection` | deprecated; prefer ProductionRaft |
| Lightweight fabric key/value consensus signals | `LightweightConsensusManager` | `mpreg.fabric.consensus` (alias `ConsensusManager`) |
| Operator status dict | `status_dict(node)` | `mpreg.consensus` |
| Dynamic membership | **not supported** | raises `MembershipChangeNotSupported` |

## Raft state transitions (B0 matrix)

| From \ Event | election timeout | RequestVote (grant) | AppendEntries (valid leader) | step-down / higher term | majority votes |
|--------------|------------------|---------------------|------------------------------|-------------------------|----------------|
| FOLLOWER | → CANDIDATE | stay / update term | stay FOLLOWER, reset timer | → FOLLOWER | n/a |
| CANDIDATE | new election (new term) | stay / vote | → FOLLOWER | → FOLLOWER | → LEADER |
| LEADER | (heartbeat maintains) | n/a (usually) | if higher term → FOLLOWER | → FOLLOWER | n/a |

RPCs: RequestVote, AppendEntries, InstallSnapshot (catch-up).  
Pre-vote (when enabled) runs before incrementing term.

## Guidance

1. New features that need strong consensus should depend on **ProductionRaft**
   with **FabricRaftTransport** (see server `register_raft_node`).
2. Do not start a second TCP Raft stack; fabric transport is mandatory.
3. `ConsensusManager` / `LightweightConsensusManager` is for gossip-era
   coordination helpers, **not** a substitute for Raft safety properties.
4. Prefer one Raft group per coordination domain; avoid dual leaders across
   ProductionRaft and ad-hoc election loops.
5. **Membership is static** until joint consensus ships; API hard-fails
   `submit_configuration_change` (INV-C6).
6. InstallSnapshot is supported for log compaction catch-up (INV-C5).
