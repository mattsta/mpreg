"""Canonical strong-consensus facade for application code.

Prefer this package over importing ProductionRaft internals or the lightweight
``ConsensusManager`` when you need linearizable coordination.

See ``mpreg/server_pkg/consensus.md`` for the full capability matrix.
"""

from __future__ import annotations

import contextlib
import warnings
from typing import Any

from mpreg.core.errors import OPERATIONAL_EXCEPTIONS
from mpreg.datastructures.production_raft import (
    LogEntry,
    LogEntryType,
    RaftSnapshot,
    RaftState,
)
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)

# Re-export lightweight helper with an explicit name so greps find footguns.
from mpreg.fabric.consensus import ConsensusManager as LightweightConsensusManager
from mpreg.fabric.consensus import StateType, StateValue

# Back-compat alias (documented as non-Raft).
ConsensusManager = LightweightConsensusManager


class MembershipChangeNotSupported(RuntimeError):
    """Raised when dynamic Raft membership is requested.

    Joint consensus / single-server membership change is not production-ready.
    Keep cluster membership fixed at ``ProductionRaft`` construction time, or
    rebuild the group offline. See INV-C6.
    """


def raft_based_leader_election(*args: Any, **kwargs: Any) -> Any:
    """Deprecated wrapper — prefer :class:`ProductionRaft` directly."""
    warnings.warn(
        "RaftBasedLeaderElection is a thin ProductionRaft wrapper; "
        "prefer ProductionRaft with FabricRaftTransport for new code.",
        DeprecationWarning,
        stacklevel=2,
    )
    from mpreg.datastructures.leader_election import RaftBasedLeaderElection

    return RaftBasedLeaderElection(*args, **kwargs)


def status_dict(node: ProductionRaft) -> dict[str, Any]:
    """Operator-facing snapshot of a Raft node (mgmt / doctor)."""
    role = (
        node.current_state.value
        if hasattr(node.current_state, "value")
        else str(node.current_state)
    )
    term = 0
    commit_index = 0
    last_applied = 0
    voted_for: str | None = None
    last_log_index = 0
    with contextlib.suppress(Exception):
        term = int(node.persistent_state.current_term)
        voted_for = node.persistent_state.voted_for
        last_log_index = (
            node.persistent_state.log_entries[-1].index
            if node.persistent_state.log_entries
            else 0
        )
    with contextlib.suppress(Exception):
        commit_index = int(node.volatile_state.commit_index)
        last_applied = int(node.volatile_state.last_applied)
    members = sorted(getattr(node, "cluster_members", ()) or ())
    metrics: dict[str, Any] = {}
    try:
        m = getattr(node, "metrics", None)
        if m is not None and hasattr(m, "to_dict"):
            raw = m.to_dict()
            if isinstance(raw, dict):
                metrics = raw
        # Absolute log size includes snapshot base when available
        if not metrics.get("log_size"):
            try:
                metrics["log_size"] = int(
                    getattr(node, "_last_log_index", lambda: last_log_index)()
                )
            except OPERATIONAL_EXCEPTIONS:
                metrics.setdefault("log_size", last_log_index)
    except Exception:  # noqa: BLE001
        metrics = {}
    # Prefer full get_status() when available (parity with RaftNodeStatus).
    status_extra: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        gs = getattr(node, "get_status", None)
        if callable(gs):
            st = gs()
            td = st.to_dict() if hasattr(st, "to_dict") else {}
            if isinstance(td, dict):
                status_extra = {
                    "time_since_leader_contact": td.get("time_since_leader_contact"),
                    "elections_skipped_recent_contact": td.get(
                        "elections_skipped_recent_contact"
                    ),
                    "elections_skipped_backoff": td.get("elections_skipped_backoff"),
                    "pre_vote_enabled": td.get("pre_vote_enabled"),
                    "pre_votes_started": td.get("pre_votes_started"),
                    "pre_votes_passed": td.get("pre_votes_passed"),
                    "pre_votes_failed": td.get("pre_votes_failed"),
                    "coordinator_active": td.get("coordinator_active"),
                    "election_in_progress": td.get("election_in_progress"),
                }

    return {
        "node_id": getattr(node, "node_id", ""),
        "role": role,
        "term": term,
        "commit_index": commit_index,
        "last_applied": last_applied,
        "last_log_index": last_log_index,
        "voted_for": voted_for,
        "cluster_members": members,
        "membership_change_supported": False,
        "snapshot_supported": True,
        # OBS-T14-01: internal counters for Prom bridge / mgmt
        "metrics": metrics,
        "log_size": int(metrics.get("log_size") or last_log_index or 0),
        **status_extra,
    }


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
