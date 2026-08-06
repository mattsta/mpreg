"""Canonical consensus entry points for application code.

Prefer ``import mpreg.consensus`` for new code. This module re-exports the
same surface for backward compatibility.

See also ``mpreg/server_pkg/consensus.md``.
"""

from __future__ import annotations

from mpreg.consensus import (
    ConsensusManager,
    LightweightConsensusManager,
    LogEntry,
    LogEntryType,
    MembershipChangeNotSupported,
    ProductionRaft,
    RaftConfiguration,
    RaftSnapshot,
    RaftState,
    StateType,
    StateValue,
    raft_based_leader_election,
    status_dict,
)

__all__ = [
    "ConsensusManager",
    "LightweightConsensusManager",
    "LogEntry",
    "LogEntryType",
    "MembershipChangeNotSupported",
    "ProductionRaft",
    "RaftConfiguration",
    "RaftSnapshot",
    "RaftState",
    "StateType",
    "StateValue",
    "raft_based_leader_election",
    "status_dict",
]
