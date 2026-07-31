"""Canonical consensus entry points for application code.

See also ``mpreg/server_pkg/consensus.md``.
"""

from __future__ import annotations

import warnings

# Canonical strong consensus
from mpreg.datastructures.production_raft import (
    LogEntry,
    LogEntryType,
    RaftState,
)
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)

# Lightweight fabric coordination (not a Raft substitute)
from mpreg.fabric.consensus import ConsensusManager, StateType, StateValue

def raft_based_leader_election(*args, **kwargs):  # type: ignore[no-untyped-def]
    """Deprecated wrapper accessor — prefer ProductionRaft directly."""
    warnings.warn(
        "RaftBasedLeaderElection is a thin ProductionRaft wrapper; "
        "prefer ProductionRaft with FabricRaftTransport for new code.",
        DeprecationWarning,
        stacklevel=2,
    )
    from mpreg.datastructures.leader_election import RaftBasedLeaderElection

    return RaftBasedLeaderElection(*args, **kwargs)

__all__ = [
    "ConsensusManager",
    "LogEntry",
    "LogEntryType",
    "ProductionRaft",
    "RaftConfiguration",
    "RaftState",
    "StateType",
    "StateValue",
    "raft_based_leader_election",
]
