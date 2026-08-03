# Raft State Transition Matrix (B0)

Canonical product doc: `mpreg/server_pkg/consensus.md`.

| From \\ Event | election timeout | RequestVote (grant path) | AppendEntries (valid leader) | higher term observed | majority votes |
|--------------|------------------|--------------------------|------------------------------|----------------------|----------------|
| FOLLOWER | → CANDIDATE | stay FOLLOWER | stay FOLLOWER | → FOLLOWER | n/a |
| CANDIDATE | new election | stay / vote | → FOLLOWER | → FOLLOWER | → LEADER |
| LEADER | maintain via heartbeat | rare | higher term → FOLLOWER | → FOLLOWER | n/a |

Coverage: `tests/invariants/test_raft_transitions.py` + existing safety suites.
