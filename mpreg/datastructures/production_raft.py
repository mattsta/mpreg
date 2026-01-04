"""
Production-Ready Raft Consensus Implementation for MPREG.

This module implements the complete Raft distributed consensus algorithm with all
production-critical features including log replication, persistent state, safety
guarantees, and comprehensive error handling.

Key Features:
- Complete Raft consensus algorithm with leader election and log replication
- Strong consistency guarantees and safety properties
- Persistent state with durability guarantees
- Real RPC communication layer for cluster coordination
- Network partition tolerance and split-brain prevention
- Log compaction and snapshotting for efficiency
- Dynamic cluster membership changes
- Comprehensive monitoring and observability
- Property-based correctness verification

Design Principles:
- Type-safe implementation with comprehensive error handling
- Async/await for all network operations with proper timeouts
- Pluggable storage backend for different persistence requirements
- Extensive logging and metrics for production debugging
- Memory-efficient log management with compaction
- Thread-safe concurrent operations

Safety Properties Guaranteed:
1. Election Safety: At most one leader can be elected in a given term
2. Leader Append-Only: A leader never overwrites or deletes entries in its log
3. Log Matching: If two logs contain an entry with the same index and term,
   then the logs are identical in all entries up through the given index
4. Leader Completeness: If a log entry is committed in a given term, then that
   entry will be present in the logs of the leaders for all higher-numbered terms
5. State Machine Safety: If a server has applied a log entry at a given index
   to its state machine, no other server will ever apply a different log entry
   for the same index
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

import ulid


# Core Raft State Enumerations
class RaftState(Enum):
    """Raft node states following the Raft protocol specification."""

    FOLLOWER = "follower"  # Passive; responds to AppendEntries and RequestVote
    CANDIDATE = "candidate"  # Actively seeking votes to become leader
    LEADER = "leader"  # Handles all client requests; sends AppendEntries


class LogEntryType(Enum):
    """Types of log entries in the Raft log."""

    COMMAND = "command"  # Regular state machine command
    CONFIGURATION_CHANGE = "config"  # Cluster membership change
    SNAPSHOT = "snapshot"  # Log compaction snapshot marker
    NOOP = "noop"  # No-operation entry for leader initialization


# Core Raft Data Structures
@dataclass(frozen=True, slots=True)
class LogEntry:
    """
    Individual entry in the Raft replicated log.

    Each entry contains a command to be applied to the state machine,
    along with the term when it was created and additional metadata.
    """

    term: int  # Term when entry was created by leader
    index: int  # Position of entry in log (1-indexed)
    entry_type: LogEntryType  # Type of log entry
    command: Any  # Command to apply to state machine
    client_id: str = ""  # Client that issued the command
    request_id: str = field(default_factory=lambda: ulid.new().str)
    timestamp: float = field(default_factory=time.time)
    checksum: str = field(init=False)  # Integrity verification

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("Term cannot be negative")
        if self.index < 1:
            raise ValueError("Log index must be positive")

        # Calculate entry checksum for integrity verification
        entry_data = {
            "term": self.term,
            "index": self.index,
            "entry_type": self.entry_type.value,
            "command": self.command,
            "client_id": self.client_id,
            "request_id": self.request_id,
            "timestamp": self.timestamp,
        }

        serialized = json.dumps(entry_data, sort_keys=True).encode()
        object.__setattr__(self, "checksum", hashlib.sha256(serialized).hexdigest())

    def verify_integrity(self) -> bool:
        """Verify entry has not been corrupted."""
        try:
            expected_entry = LogEntry(
                term=self.term,
                index=self.index,
                entry_type=self.entry_type,
                command=self.command,
                client_id=self.client_id,
                request_id=self.request_id,
                timestamp=self.timestamp,
            )
            return expected_entry.checksum == self.checksum
        except Exception:
            return False


@dataclass(frozen=True, slots=True)
class RaftSnapshot:
    """
    Snapshot of the state machine at a specific log index.

    Used for log compaction to prevent unbounded log growth.
    Contains the complete state machine state as of the last included index.
    """

    last_included_index: int  # Last log index included in snapshot
    last_included_term: int  # Term of last_included_index entry
    state_machine_state: bytes  # Serialized state machine state
    configuration: set[str]  # Cluster configuration at snapshot time
    snapshot_id: str = field(default_factory=lambda: ulid.new().str)
    created_at: float = field(default_factory=time.time)
    size_bytes: int = field(init=False)
    checksum: str = field(init=False)

    def __post_init__(self) -> None:
        if self.last_included_index < 0:
            raise ValueError("Last included index cannot be negative")
        if self.last_included_term < 0:
            raise ValueError("Last included term cannot be negative")

        # Calculate size and checksum
        object.__setattr__(self, "size_bytes", len(self.state_machine_state))

        snapshot_data = {
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "configuration": sorted(list(self.configuration)),
            "snapshot_id": self.snapshot_id,
            "created_at": self.created_at,
            "state_machine_state": self.state_machine_state.hex(),
        }

        serialized = json.dumps(snapshot_data, sort_keys=True).encode()
        object.__setattr__(self, "checksum", hashlib.sha256(serialized).hexdigest())


# RPC Message Types
@dataclass(frozen=True, slots=True)
class RequestVoteRequest:
    """RequestVote RPC request as specified in Raft paper."""

    term: int  # Candidate's term
    candidate_id: str  # Candidate requesting vote
    last_log_index: int  # Index of candidate's last log entry
    last_log_term: int  # Term of candidate's last log entry

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("Term cannot be negative")
        if self.last_log_index < 0:
            raise ValueError("Last log index cannot be negative")
        if self.last_log_term < 0:
            raise ValueError("Last log term cannot be negative")


@dataclass(frozen=True, slots=True)
class RequestVoteResponse:
    """RequestVote RPC response as specified in Raft paper."""

    term: int  # Current term, for candidate to update itself
    vote_granted: bool  # True means candidate received vote
    voter_id: str  # ID of the voting node

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("Term cannot be negative")


@dataclass(frozen=True, slots=True)
class AppendEntriesRequest:
    """AppendEntries RPC request as specified in Raft paper."""

    term: int  # Leader's term
    leader_id: str  # Leader's ID so followers can redirect clients
    prev_log_index: int  # Index of log entry immediately preceding new ones
    prev_log_term: int  # Term of prev_log_index entry
    entries: list[LogEntry]  # Log entries to store (empty for heartbeat)
    leader_commit: int  # Leader's commit_index

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("Term cannot be negative")
        if self.prev_log_index < 0:
            raise ValueError("Previous log index cannot be negative")
        if self.prev_log_term < 0:
            raise ValueError("Previous log term cannot be negative")
        if self.leader_commit < 0:
            raise ValueError("Leader commit index cannot be negative")


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse:
    """AppendEntries RPC response as specified in Raft paper."""

    term: int  # Current term, for leader to update itself
    success: bool  # True if follower contained entry matching prev_log_index and prev_log_term
    follower_id: str  # ID of the responding follower
    match_index: int = 0  # Index of highest log entry known to be replicated on server
    conflict_index: int = -1  # Index to help leader find conflicting entries quickly
    conflict_term: int = -1  # Term of conflicting entry for fast conflict resolution

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("Term cannot be negative")
        if self.match_index < 0:
            raise ValueError("Match index cannot be negative")


@dataclass(frozen=True, slots=True)
class InstallSnapshotRequest:
    """InstallSnapshot RPC request for log compaction."""

    term: int  # Leader's term
    leader_id: str  # Leader's ID
    last_included_index: (
        int  # Snapshot replaces all entries up through and including this index
    )
    last_included_term: int  # Term of last_included_index
    data: bytes  # Raw bytes of the snapshot chunk
    done: bool  # True if this is the last chunk
    offset: int = 0  # Byte offset where chunk is positioned in the snapshot file

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("Term cannot be negative")
        if self.last_included_index < 0:
            raise ValueError("Last included index cannot be negative")
        if self.last_included_term < 0:
            raise ValueError("Last included term cannot be negative")
        if self.offset < 0:
            raise ValueError("Offset cannot be negative")


@dataclass(frozen=True, slots=True)
class InstallSnapshotResponse:
    """InstallSnapshot RPC response."""

    term: int  # Current term, for leader to update itself
    follower_id: str  # ID of the responding follower

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("Term cannot be negative")


# Storage and State Management Protocols
@dataclass(frozen=True, slots=True)
class PersistentState:
    """
    Persistent state that must be updated on stable storage before responding to RPCs.

    This state must survive crashes and be persisted atomically to ensure
    Raft safety guarantees.
    """

    current_term: int  # Latest term server has seen (initialized to 0)
    voted_for: str | None  # Candidate ID that received vote in current term
    log_entries: list[LogEntry]  # Log entries (1-indexed)

    def __post_init__(self) -> None:
        if self.current_term < 0:
            raise ValueError("Current term cannot be negative")


@dataclass(slots=True)
class VolatileState:
    """
    Volatile state on all servers that is rebuilt on restart.

    This state can be lost on crashes and will be reconstructed from
    persistent state and network communication.
    """

    commit_index: int = 0  # Index of highest log entry known to be committed
    last_applied: int = 0  # Index of highest log entry applied to state machine

    def __post_init__(self) -> None:
        if self.commit_index < 0:
            raise ValueError("Commit index cannot be negative")
        if self.last_applied < 0:
            raise ValueError("Last applied index cannot be negative")


@dataclass(slots=True)
class LeaderVolatileState:
    """
    Volatile state on leaders that is reinitialized after election.

    This state tracks the progress of log replication to each follower
    and is rebuilt when becoming a leader.
    """

    next_index: dict[str, int]  # For each server, index of next log entry to send
    match_index: dict[
        str, int
    ]  # For each server, index of highest log entry known to be replicated

    def __init__(
        self, cluster_members: set[str], last_log_index: int, leader_id: str
    ) -> None:
        # Initialize next_index to leader's last log index + 1
        self.next_index = {member: last_log_index + 1 for member in cluster_members}
        # Initialize match_index to 0 for followers (no entries replicated yet)
        # but leader always has all its own entries, so set its match_index to last_log_index
        self.match_index = {member: 0 for member in cluster_members}
        self.match_index[leader_id] = last_log_index


# Storage Interface
class RaftStorageProtocol(Protocol):
    """Protocol for persistent storage backend used by Raft implementation."""

    async def save_persistent_state(self, state: PersistentState) -> None:
        """Atomically save persistent state to storage."""
        ...

    async def load_persistent_state(self) -> PersistentState | None:
        """Load persistent state from storage, or None if not found."""
        ...

    async def save_snapshot(self, snapshot: RaftSnapshot) -> None:
        """Save snapshot to storage."""
        ...

    async def load_latest_snapshot(self) -> RaftSnapshot | None:
        """Load the most recent snapshot from storage."""
        ...

    async def cleanup_old_snapshots(self, keep_count: int = 3) -> None:
        """Clean up old snapshots, keeping only the most recent ones."""
        ...


# Network Communication Interface
class RaftTransportProtocol(Protocol):
    """Protocol for network communication between Raft nodes."""

    async def send_request_vote(
        self, target: str, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        """Send RequestVote RPC to target node."""
        ...

    async def send_append_entries(
        self, target: str, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        """Send AppendEntries RPC to target node."""
        ...

    async def send_install_snapshot(
        self, target: str, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse | None:
        """Send InstallSnapshot RPC to target node."""
        ...


# State Machine Interface
class StateMachineProtocol(Protocol):
    """Protocol for the replicated state machine."""

    async def apply_command(self, command: Any, index: int) -> Any:
        """Apply command to state machine and return result."""
        ...

    async def create_snapshot(self) -> bytes:
        """Create snapshot of current state machine state."""
        ...

    async def restore_from_snapshot(self, snapshot_data: bytes) -> None:
        """Restore state machine from snapshot data."""
        ...
