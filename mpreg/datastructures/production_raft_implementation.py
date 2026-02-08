"""
Production Raft Consensus Implementation - Main Algorithm.

This module contains the core ProductionRaft class that implements the complete
Raft distributed consensus algorithm with all safety and liveness guarantees.
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from .production_raft import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    LeaderVolatileState,
    LogEntry,
    LogEntryType,
    PersistentState,
    RaftSnapshot,
    RaftState,
    RaftStorageProtocol,
    RaftTransportProtocol,
    RequestVoteRequest,
    StateMachineProtocol,
    VolatileState,
)
from .production_raft_rpcs import ProductionRaftRPCs
from .raft_task_manager import RaftTaskManager

raft_log = logger


_DIAG_TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enabled", "debug"})
RAFT_DIAG_ENABLED = (
    os.environ.get("MPREG_DEBUG_RAFT", "").strip().lower() in _DIAG_TRUE_VALUES
)


class ContactStatus(Enum):
    """Follower contact status enum."""

    NEVER_CONTACTED = "never_contacted"
    REACHABLE = "reachable"
    UNREACHABLE = "unreachable"


@dataclass
class FollowerContactInfo:
    """Tracks contact status and timing for a follower node."""

    contact_status: ContactStatus = ContactStatus.NEVER_CONTACTED
    last_successful_contact: float = 0.0
    consecutive_failures: int = 0
    last_attempt_time: float = 0.0

    def mark_successful_contact(self, current_time: float) -> None:
        """Mark a successful contact with this follower."""
        self.contact_status = ContactStatus.REACHABLE
        self.last_successful_contact = current_time
        self.last_attempt_time = current_time
        self.consecutive_failures = 0

    def mark_failed_contact(self, current_time: float) -> None:
        """Mark a failed contact attempt with this follower."""
        self.contact_status = ContactStatus.UNREACHABLE
        self.last_attempt_time = current_time
        self.consecutive_failures += 1

    def should_retry(self, current_time: float, base_interval: float) -> bool:
        """Check if we should retry contacting this follower based on exponential backoff."""
        if self.consecutive_failures == 0:
            return True

        # Exponential backoff: wait longer after repeated failures
        backoff_multiplier = min(8.0, 2.0 ** (self.consecutive_failures - 1))
        backoff_interval = base_interval * backoff_multiplier

        time_since_last_attempt = current_time - self.last_attempt_time
        return time_since_last_attempt >= backoff_interval

    def is_reachable(self, current_time: float, timeout: float) -> bool:
        """Check if this follower is considered reachable within the timeout."""
        if self.contact_status == ContactStatus.NEVER_CONTACTED:
            return False
        if self.contact_status == ContactStatus.UNREACHABLE:
            return False
        # Only reachable if we've had recent successful contact
        return current_time - self.last_successful_contact <= timeout


@dataclass
class ElectionCoordinator:
    """
    Self-managing, well-encapsulated election coordination system.

    Handles all election-related coordination primitives and logic,
    providing a clean interface for event-driven election management.
    """

    # Core coordination primitives
    election_trigger: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event, init=False)

    # Election state tracking
    election_in_progress: bool = field(default=False, init=False)
    election_start_time: float = field(default=0.0, init=False)
    last_election_attempt_time: float = field(default=0.0, init=False)
    consecutive_election_failures: int = field(default=0, init=False)
    election_backoff_multiplier: float = field(default=1.0, init=False)

    # Task management - will be set by ProductionRaft
    task_manager: RaftTaskManager | None = field(default=None, init=False)
    election_timeout_min: float = field(default=0.15, init=False)
    election_timeout_max: float = field(default=0.25, init=False)
    timeout_bias_seconds: float = field(default=0.0, init=False)
    last_wait_timeout: float = field(default=0.0, init=False)

    def configure_timeouts(
        self, *, election_timeout_min: float, election_timeout_max: float
    ) -> None:
        """Configure coordinator timeout window from node config."""
        minimum = max(election_timeout_min, 0.01)
        maximum = max(election_timeout_max, minimum + 0.01)
        self.election_timeout_min = minimum
        self.election_timeout_max = maximum

    def configure_node_bias(self, *, node_id: str) -> None:
        """Derive a deterministic per-node timeout bias to avoid lockstep elections."""
        span = max(self.election_timeout_max - self.election_timeout_min, 0.01)
        checksum = sum(ord(ch) for ch in node_id) % 1000
        fraction = checksum / 1000.0
        self.timeout_bias_seconds = span * 0.5 * fraction

    async def start_coordinator(self, node_id: str, election_callback) -> None:
        """Start the election coordinator with proper task management."""
        if not self.task_manager:
            raise RuntimeError("Task manager not set on ElectionCoordinator")

        # Reset stop event for new coordinator
        self.stop_event.clear()
        self.configure_node_bias(node_id=node_id)

        # Start coordinator task through task manager
        await self.task_manager.create_task(
            "core",
            "election_coordinator",
            self._coordinator_loop,
            node_id,
            election_callback,
        )

    async def stop_coordinator(self) -> None:
        """Stop the election coordinator cleanly."""
        raft_log.debug("Stopping election coordinator")
        self.stop_event.set()

        # If we have a task manager, ask it to stop just the coordinator task
        if self.task_manager:
            await self.task_manager.stop_specific_task(
                "core", "election_coordinator", timeout=1.0
            )

        raft_log.debug("Election coordinator stopped")

    def trigger_election(self) -> None:
        """Trigger an election event."""
        self.election_trigger.set()

    def should_backoff_election(self, current_time: float, config) -> bool:
        """Check if election should be backed off due to recent failures."""
        if self.consecutive_election_failures == 0:
            return False

        backoff_time = config.election_timeout_min * self.election_backoff_multiplier
        time_since_last_attempt = current_time - self.last_election_attempt_time
        return time_since_last_attempt < backoff_time

    def record_election_failure(self) -> None:
        """Record an election failure and update backoff."""
        self.consecutive_election_failures += 1
        # More aggressive backoff for repeated failures - common in production Raft
        if self.consecutive_election_failures >= 5:
            # Very aggressive backoff for persistent failures (minority partitions)
            self.election_backoff_multiplier = min(
                16.0, self.election_backoff_multiplier * 2.0
            )
        else:
            # Normal backoff for occasional failures
            self.election_backoff_multiplier = min(
                4.0, self.election_backoff_multiplier * 1.5
            )

    def record_election_success(self) -> None:
        """Record an election success and reset backoff."""
        self.consecutive_election_failures = 0
        self.election_backoff_multiplier = 1.0

    async def _coordinator_loop(self, node_id: str, election_callback) -> None:
        """Main coordinator loop - simple election timer."""
        raft_log.debug(f"[{node_id}] Election coordinator loop started")

        try:
            while not self.stop_event.is_set():
                # Wait for election trigger or timeout
                timeout = random.uniform(
                    self.election_timeout_min, self.election_timeout_max
                )
                timeout += self.timeout_bias_seconds
                timed_out = False

                try:
                    # Simple wait pattern to avoid circular dependencies
                    await asyncio.wait_for(
                        self.election_trigger.wait(), timeout=timeout
                    )
                    self.election_trigger.clear()
                except TimeoutError:
                    # Timeout occurred - proceed with election check
                    timed_out = True
                except asyncio.CancelledError:
                    # Coordinator is being cancelled
                    raft_log.debug(
                        f"[{node_id}] Election coordinator cancelled during wait"
                    )
                    break

                # Check if we should stop before proceeding
                if self.stop_event.is_set():
                    raft_log.debug(
                        f"[{node_id}] Election coordinator received stop signal"
                    )
                    break

                self.last_wait_timeout = timeout if timed_out else 0.0
                if RAFT_DIAG_ENABLED:
                    raft_log.warning(
                        "[DIAG_RAFT] node={} action=coordinator_wakeup timed_out={} timeout={:.4f}s "
                        "trigger_set={} election_in_progress={} failures={} backoff={:.3f}",
                        node_id,
                        timed_out,
                        timeout,
                        self.election_trigger.is_set(),
                        self.election_in_progress,
                        self.consecutive_election_failures,
                        self.election_backoff_multiplier,
                    )

                # Call the election callback if not already in progress
                # Don't wait for it to complete to avoid circular dependencies
                if not self.election_in_progress:
                    self.election_in_progress = True
                    current_time = time.time()
                    self.election_start_time = current_time

                    try:
                        # Fire and forget - don't wait for the callback to avoid circular deps
                        asyncio.create_task(
                            self._election_callback_wrapper(election_callback)
                        )
                        if RAFT_DIAG_ENABLED:
                            raft_log.warning(
                                "[DIAG_RAFT] node={} action=election_callback_scheduled "
                                "election_start_time={:.6f}",
                                node_id,
                                self.election_start_time,
                            )
                    except Exception as e:
                        raft_log.warning(
                            f"[{node_id}] Election callback creation error: {e}"
                        )
                        self.election_in_progress = False

        except asyncio.CancelledError:
            raft_log.debug(f"[{node_id}] Election coordinator cancelled")
            raise
        except Exception as e:
            raft_log.error(f"[{node_id}] Election coordinator error: {e}")
        finally:
            raft_log.debug(f"[{node_id}] Election coordinator loop ended")
            # Ensure we're not marked as in progress
            self.election_in_progress = False

    async def _election_callback_wrapper(self, election_callback) -> None:
        """Wrapper for election callback to handle cleanup."""
        try:
            if RAFT_DIAG_ENABLED:
                raft_log.warning("[DIAG_RAFT] action=election_callback_begin")
            await election_callback()
        except Exception as e:
            raft_log.warning(f"Election callback error: {e}")
        finally:
            self.election_in_progress = False
            if RAFT_DIAG_ENABLED:
                raft_log.warning("[DIAG_RAFT] action=election_callback_end")


@dataclass(frozen=True, slots=True)
class RaftConfiguration:
    """Configuration parameters for Raft implementation."""

    # Timing parameters (in seconds)
    election_timeout_min: float = 0.15  # Minimum election timeout
    election_timeout_max: float = 0.30  # Maximum election timeout
    heartbeat_interval: float = 0.025  # Leader heartbeat interval
    rpc_timeout: float = 0.10  # RPC call timeout

    # Log management
    max_log_entries_per_request: int = 100  # Maximum entries per AppendEntries
    snapshot_threshold: int = 10000  # Trigger snapshot after this many entries
    max_log_entries_behind: int = 1000  # Use snapshot if follower is this far behind

    # Safety and performance
    max_election_timeout_jitter: float = 0.05  # Randomization to prevent split votes
    pre_vote_enabled: bool = True  # Enable pre-vote optimization
    pipeline_enabled: bool = True  # Enable request pipelining
    batch_size: int = 50  # Batch size for log replication

    # Network resilience
    max_retry_attempts: int = 3  # Maximum RPC retry attempts
    backoff_multiplier: float = 1.5  # Exponential backoff multiplier
    max_backoff_delay: float = 2.0  # Maximum backoff delay

    def __post_init__(self) -> None:
        if self.election_timeout_min <= 0:
            raise ValueError("Election timeout min must be positive")
        if self.election_timeout_max <= self.election_timeout_min:
            raise ValueError("Election timeout max must be greater than min")
        if self.heartbeat_interval <= 0:
            raise ValueError("Heartbeat interval must be positive")
        if self.heartbeat_interval >= self.election_timeout_min / 3:
            raise ValueError(
                "Heartbeat interval should be much smaller than election timeout"
            )


@dataclass(slots=True)
class RaftMetrics:
    """Comprehensive metrics for monitoring Raft node performance."""

    # Election metrics
    elections_started: int = 0
    elections_won: int = 0
    elections_lost: int = 0
    votes_requested: int = 0
    votes_granted: int = 0

    # Log replication metrics
    append_entries_sent: int = 0
    append_entries_received: int = 0
    append_entries_success: int = 0
    append_entries_failure: int = 0
    log_entries_replicated: int = 0

    # State machine metrics
    commands_applied: int = 0
    snapshots_created: int = 0
    snapshots_installed: int = 0

    # Performance metrics
    average_election_duration_ms: float = 0.0
    average_log_replication_latency_ms: float = 0.0
    average_command_commit_latency_ms: float = 0.0

    # State tracking
    current_term: int = 0
    current_state: RaftState = RaftState.FOLLOWER
    commit_index: int = 0
    last_applied: int = 0
    log_size: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary for serialization."""
        return {
            "elections_started": self.elections_started,
            "elections_won": self.elections_won,
            "elections_lost": self.elections_lost,
            "votes_requested": self.votes_requested,
            "votes_granted": self.votes_granted,
            "append_entries_sent": self.append_entries_sent,
            "append_entries_received": self.append_entries_received,
            "append_entries_success": self.append_entries_success,
            "append_entries_failure": self.append_entries_failure,
            "log_entries_replicated": self.log_entries_replicated,
            "commands_applied": self.commands_applied,
            "snapshots_created": self.snapshots_created,
            "snapshots_installed": self.snapshots_installed,
            "average_election_duration_ms": self.average_election_duration_ms,
            "average_log_replication_latency_ms": self.average_log_replication_latency_ms,
            "average_command_commit_latency_ms": self.average_command_commit_latency_ms,
            "current_term": self.current_term,
            "current_state": self.current_state.value,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_size": self.log_size,
        }


@dataclass(slots=True)
class ProductionRaft(ProductionRaftRPCs):
    """
    Production-ready Raft consensus implementation.

    This class implements the complete Raft distributed consensus algorithm
    as specified in the Raft paper with all safety guarantees and optimizations
    needed for production deployment.

    Key Features:
    - Complete leader election with safety guarantees
    - Log replication with consistency verification
    - Persistent state handling with durability
    - Snapshot support for log compaction
    - Network partition tolerance
    - Dynamic cluster membership changes
    - Comprehensive metrics and monitoring
    """

    node_id: str  # Unique identifier for this node
    cluster_members: set[str]  # Set of all cluster member IDs
    storage: RaftStorageProtocol  # Persistent storage backend
    transport: RaftTransportProtocol  # Network transport layer
    state_machine: StateMachineProtocol  # Replicated state machine
    config: RaftConfiguration = field(default_factory=RaftConfiguration)

    # Raft state (managed internally)
    current_state: RaftState = RaftState.FOLLOWER
    persistent_state: PersistentState = field(init=False)
    volatile_state: VolatileState = field(default_factory=VolatileState, init=False)
    leader_volatile_state: LeaderVolatileState | None = None

    # Election state
    current_leader: str | None = None
    last_heartbeat_time: float = 0.0  # Start with 0 to trigger elections
    votes_received: set[str] = field(default_factory=set, init=False)

    # Leader step-down tracking
    follower_contact_info: dict[str, FollowerContactInfo] = field(
        default_factory=dict, init=False
    )
    last_majority_contact_time: float = 0.0
    leader_start_time: float = 0.0
    became_leader_time: float = 0.0

    # Concurrency control
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    # CLEAN ARCHITECTURE: Single well-encapsulated coordination system
    election_coordinator: ElectionCoordinator = field(
        default_factory=ElectionCoordinator, init=False
    )

    # Centralized task management
    task_manager: RaftTaskManager = field(init=False)

    # Metrics and monitoring
    metrics: RaftMetrics = field(default_factory=RaftMetrics, init=False)

    # Snapshot state
    installing_snapshot: bool = field(default=False, init=False)
    snapshot_chunks: dict[str, list[bytes]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        """Initialize Raft node."""
        if not self.node_id:
            raise ValueError("Node ID cannot be empty")
        if self.node_id not in self.cluster_members:
            raise ValueError("Node ID must be in cluster members")
        if len(self.cluster_members) < 1:
            raise ValueError("Cluster must have at least 1 member")

        # Initialize persistent state with defaults
        self.persistent_state = PersistentState(
            current_term=0, voted_for=None, log_entries=[]
        )

        # Initialize centralized task manager
        self.task_manager = RaftTaskManager(self.node_id)

        # Connect election coordinator to task manager
        self.election_coordinator.task_manager = self.task_manager
        effective_timeout_min, effective_timeout_max = (
            self._effective_election_timeout_window()
        )
        self.election_coordinator.configure_timeouts(
            election_timeout_min=effective_timeout_min,
            election_timeout_max=effective_timeout_max,
        )

        raft_log.info(
            f"Initialized Raft node {self.node_id} with cluster {self.cluster_members}"
        )

    def _effective_election_timeout_window(self) -> tuple[float, float]:
        """
        Compute adaptive election timeout bounds based on cluster fanout.

        Larger clusters need more room for vote and heartbeat fanout so nodes do not
        repeatedly self-timeout while a leader is actively replicating.
        """
        cluster_size = max(len(self.cluster_members), 1)
        base_min = max(
            self.config.election_timeout_min, self.config.heartbeat_interval * 4.0
        )
        base_max = max(self.config.election_timeout_max, base_min + 0.01)
        if cluster_size <= 3:
            return base_min, base_max

        scale = max(1.0, math.sqrt(cluster_size / 3.0))
        base_span = base_max - base_min

        adaptive_min = base_min * scale
        adaptive_span = max(
            base_span * scale,
            self.config.heartbeat_interval * (cluster_size / 2.0),
        )
        adaptive_max = adaptive_min + adaptive_span

        # Keep failover bounded while still widening windows for larger clusters.
        bounded_max = max(
            base_max * 4.0,
            self.config.heartbeat_interval * cluster_size * 2.0,
        )
        adaptive_max = min(adaptive_max, bounded_max)
        if adaptive_max <= adaptive_min:
            adaptive_max = adaptive_min + 0.01
        return adaptive_min, adaptive_max

    async def start(self) -> None:
        """
        Start the Raft node and begin participating in consensus.

        This method loads persistent state, starts background tasks,
        and begins the election timer.
        """
        async with self.state_lock:
            try:
                # Load persistent state from storage
                stored_state = await self.storage.load_persistent_state()
                if stored_state:
                    self.persistent_state = stored_state
                    raft_log.info(
                        f"Loaded persistent state: term={stored_state.current_term}, "
                        f"log_entries={len(stored_state.log_entries)}"
                    )

                # Load latest snapshot if available
                snapshot = await self.storage.load_latest_snapshot()
                if snapshot:
                    await self._apply_snapshot(snapshot)
                    raft_log.info(
                        f"Loaded snapshot up to index {snapshot.last_included_index}"
                    )

                # Start background tasks
                await self._start_background_tasks()
                if RAFT_DIAG_ENABLED:
                    raft_log.warning(
                        "[DIAG_RAFT] node={} action=start_complete state={} term={} "
                        "log_entries={} election_timeout_min={:.4f}s election_timeout_max={:.4f}s",
                        self.node_id,
                        self.current_state.value,
                        self.persistent_state.current_term,
                        len(self.persistent_state.log_entries),
                        self.election_coordinator.election_timeout_min,
                        self.election_coordinator.election_timeout_max,
                    )

                # Update metrics
                self.metrics.current_term = self.persistent_state.current_term
                self.metrics.current_state = self.current_state
                self.metrics.log_size = len(self.persistent_state.log_entries)

                raft_log.info(
                    f"Raft node {self.node_id} started in {self.current_state.value} state"
                )

            except Exception as e:
                raft_log.error(f"Failed to start Raft node {self.node_id}: {e}")
                raise

    async def stop(self) -> None:
        """
        Gracefully stop the Raft node.

        This method cancels background tasks, saves persistent state,
        and performs cleanup.
        """
        raft_log.debug(f"[{self.node_id}] stop() called")
        async with self.state_lock:
            try:
                raft_log.info(f"Stopping Raft node {self.node_id}")

                # Cancel background tasks
                raft_log.debug(f"[{self.node_id}] Stopping background tasks")
                await self._stop_background_tasks()

                # Save final state
                raft_log.debug(f"[{self.node_id}] Saving final state")
                await self.storage.save_persistent_state(self.persistent_state)

                # Reset state
                raft_log.debug(f"[{self.node_id}] Resetting state")
                self.current_state = RaftState.FOLLOWER
                self.current_leader = None
                self.leader_volatile_state = None
                self.election_coordinator.election_in_progress = False
                self.votes_received.clear()

                raft_log.info(f"Raft node {self.node_id} stopped")

            except Exception as e:
                raft_log.error(f"[{self.node_id}] Error stopping Raft node: {e}")
                import traceback

                raft_log.error(
                    f"[{self.node_id}] Stop traceback: {traceback.format_exc()}"
                )
                raise

    async def step_down(self) -> None:
        """Request this node to step down to follower state."""
        async with self.state_lock:
            await self._convert_to_follower(restart_timer=True, reset_backoff=True)

    async def submit_command(self, command: Any, client_id: str = "") -> Any | None:
        """
        Submit a command to be replicated via Raft consensus.

        Args:
            command: Command to be applied to state machine
            client_id: Optional client identifier

        Returns:
            Result of applying command, or None if not leader or replication failed
        """
        # Create log entry first (with lock protection)
        async with self.state_lock:
            if self.current_state != RaftState.LEADER:
                raft_log.warning(
                    f"Node {self.node_id} is not leader, cannot submit command"
                )
                return None

            # Create log entry for command
            entry = LogEntry(
                term=self.persistent_state.current_term,
                index=self._last_log_index() + 1,
                entry_type=LogEntryType.COMMAND,
                command=command,
                client_id=client_id,
            )

            # Append to local log
            new_log = list(self.persistent_state.log_entries) + [entry]
            self.persistent_state = PersistentState(
                current_term=self.persistent_state.current_term,
                voted_for=self.persistent_state.voted_for,
                log_entries=new_log,
            )

            # Persist state
            await self.storage.save_persistent_state(self.persistent_state)

            # CRITICAL: Update leader's own match_index to reflect new entry
            if self.leader_volatile_state:
                self.leader_volatile_state.match_index[self.node_id] = entry.index

        # Trigger immediate replication WITHOUT holding the lock
        try:
            await self._replicate_log_entries()

            # Wait for entry to be committed
            commit_start_time = time.time()
            while self.volatile_state.commit_index < entry.index:
                if time.time() - commit_start_time > 5.0:  # 5 second timeout
                    raft_log.warning(f"Command commit timeout for entry {entry.index}")
                    return None
                await asyncio.sleep(0.01)

            # Apply to state machine and return result
            result = await self.state_machine.apply_command(command, entry.index)

            # Update metrics
            commit_latency = (time.time() - commit_start_time) * 1000
            self._update_command_commit_latency(commit_latency)

            raft_log.info(f"Successfully committed command at index {entry.index}")
            return result

        except Exception as e:
            raft_log.error(f"Error submitting command: {e}")
            return None

    # Leader Election Implementation

    def _reset_election_backoff(self) -> None:
        """Reset election backoff after successful election or receiving valid leader."""
        self.election_coordinator.record_election_success()
        raft_log.debug(f"[{self.node_id}] Election backoff reset")

    def _record_election_failure(self) -> None:
        """Record an election failure and increase backoff."""
        self.election_coordinator.record_election_failure()
        raft_log.debug(
            f"[{self.node_id}] Election failure recorded "
            f"(failures: {self.election_coordinator.consecutive_election_failures}, "
            f"backoff: {self.election_coordinator.election_backoff_multiplier:.2f}x)"
        )

    async def _start_election_with_semaphore(self) -> None:
        """Direct election starter - no semaphore needed with new architecture."""
        await self._start_election()

    async def _start_election(self) -> None:
        """
        Start leader election process.

        Implements the candidate state behavior from Raft paper:
        1. Check if election is warranted (heartbeat timeout)
        2. Increment current term
        3. Vote for self
        4. Reset election timer
        5. Send RequestVote RPCs to all other servers

        Includes election backoff to prevent infinite election storms.
        """
        raft_log.debug(
            f"[{self.node_id}] _start_election called, current_state={self.current_state}"
        )

        current_time = time.time()

        # CRITICAL: Check if we should actually start an election
        # Only start if we're not a leader and haven't received recent heartbeats
        if self.current_state == RaftState.LEADER:
            raft_log.debug(f"[{self.node_id}] Skipping election - already leader")
            if RAFT_DIAG_ENABLED:
                raft_log.warning(
                    "[DIAG_RAFT] node={} action=start_election_skip reason=already_leader "
                    "state={} term={}",
                    self.node_id,
                    self.current_state.value,
                    self.persistent_state.current_term,
                )
            return

        time_since_heartbeat = current_time - self.last_heartbeat_time
        required_quiet_period = self.election_coordinator.last_wait_timeout
        if required_quiet_period <= 0:
            required_quiet_period = self.config.election_timeout_min
        if time_since_heartbeat < required_quiet_period:
            raft_log.debug(
                f"[{self.node_id}] Skipping election - recent heartbeat "
                f"({time_since_heartbeat:.3f}s ago, required={required_quiet_period:.3f}s)"
            )
            if RAFT_DIAG_ENABLED:
                raft_log.warning(
                    "[DIAG_RAFT] node={} action=start_election_skip reason=recent_heartbeat "
                    "time_since_heartbeat={:.4f}s required_quiet_period={:.4f}s",
                    self.node_id,
                    time_since_heartbeat,
                    required_quiet_period,
                )
            return

        # Respect adaptive election backoff after repeated failures.
        if self.election_coordinator.should_backoff_election(current_time, self.config):
            raft_log.debug(
                f"[{self.node_id}] Backing off election "
                f"(failures={self.election_coordinator.consecutive_election_failures}, "
                f"multiplier={self.election_coordinator.election_backoff_multiplier:.2f}x)"
            )
            if RAFT_DIAG_ENABLED:
                raft_log.warning(
                    "[DIAG_RAFT] node={} action=start_election_skip reason=backoff "
                    "failures={} multiplier={:.3f} since_last_attempt={:.4f}s",
                    self.node_id,
                    self.election_coordinator.consecutive_election_failures,
                    self.election_coordinator.election_backoff_multiplier,
                    current_time - self.election_coordinator.last_election_attempt_time,
                )
            return

        raft_log.info(
            f"[{self.node_id}] Starting election - heartbeat timeout exceeded ({time_since_heartbeat:.3f}s)"
        )

        try:
            self.election_coordinator.last_election_attempt_time = current_time

            # RAFT behavior: reset election timer when starting an election round.
            self.last_heartbeat_time = current_time

            # Increment term and vote for self
            new_term = self.persistent_state.current_term + 1

            self.persistent_state = PersistentState(
                current_term=new_term,
                voted_for=self.node_id,
                log_entries=self.persistent_state.log_entries,
            )

            await self.storage.save_persistent_state(self.persistent_state)
            self.metrics.current_term = new_term

            # Convert to candidate
            self.current_state = RaftState.CANDIDATE
            self.votes_received = {self.node_id}  # Vote for self
            self.current_leader = None

            # Check if we're a single-node cluster (immediate leader)
            majority_threshold = len(self.cluster_members) // 2 + 1

            self.metrics.elections_started += 1
            raft_log.info(f"Starting election for term {new_term}")
            if RAFT_DIAG_ENABLED:
                raft_log.warning(
                    "[DIAG_RAFT] node={} action=start_election term={} members={} majority={} "
                    "last_heartbeat={:.6f}",
                    self.node_id,
                    new_term,
                    len(self.cluster_members),
                    majority_threshold,
                    self.last_heartbeat_time,
                )

            if len(self.cluster_members) == 1:
                # Single node cluster - immediately become leader
                raft_log.info(
                    f"Single node cluster, becoming leader for term {new_term}"
                )
                await self._become_leader()
            else:
                # Send RequestVote RPCs to all other nodes
                vote_tasks = []
                for member_id in self.cluster_members:
                    if member_id != self.node_id:
                        task = asyncio.create_task(
                            self._request_vote_from_node(member_id)
                        )
                        vote_tasks.append(task)

                # Wait for responses with proper timeout handling
                # Use a longer timeout to allow for all vote responses
                hard_timeout = self.config.election_timeout_max * 2

                try:
                    # Wait for all vote tasks to complete
                    await asyncio.wait_for(
                        asyncio.gather(*vote_tasks, return_exceptions=True),
                        timeout=hard_timeout,
                    )

                    # Process results - some may be exceptions but that's OK
                    raft_log.info(
                        f"Vote collection completed with {len(self.votes_received)} votes"
                    )

                except TimeoutError:
                    raft_log.warning(
                        f"Hard election timeout in term {new_term} with {len(self.votes_received)} votes"
                    )

                # Check if we won the election (this could have been set by vote responses)
                if self.current_state == RaftState.LEADER:
                    # Already became leader during vote collection
                    raft_log.info("Already became leader during vote collection")
                    if RAFT_DIAG_ENABLED:
                        raft_log.warning(
                            "[DIAG_RAFT] node={} action=election_result result=leader_during_collection "
                            "term={} votes={} majority={}",
                            self.node_id,
                            new_term,
                            len(self.votes_received),
                            majority_threshold,
                        )
                elif len(self.votes_received) >= majority_threshold:
                    raft_log.info(
                        f"Achieved majority after vote collection: {len(self.votes_received)}/{len(self.cluster_members)}"
                    )
                    await self._become_leader()
                else:
                    raft_log.info(
                        f"Election failed in term {new_term}: got {len(self.votes_received)} votes, needed {majority_threshold}"
                    )
                    self.metrics.elections_lost += 1
                    self._record_election_failure()
                    if RAFT_DIAG_ENABLED:
                        raft_log.warning(
                            "[DIAG_RAFT] node={} action=election_result result=failed term={} votes={} majority={}",
                            self.node_id,
                            new_term,
                            len(self.votes_received),
                            majority_threshold,
                        )
                    await self._convert_to_follower()

        except Exception as e:
            raft_log.error(f"Error during election: {e}")
            self.metrics.elections_lost += 1
            self._record_election_failure()
            if RAFT_DIAG_ENABLED:
                raft_log.warning(
                    "[DIAG_RAFT] node={} action=election_exception error={} term={}",
                    self.node_id,
                    e,
                    self.persistent_state.current_term,
                )
            await self._convert_to_follower()
        finally:
            # ElectionCoordinator will reset election_in_progress flag
            pass

        # Record election duration
        if self.election_coordinator.election_start_time:
            duration = (
                time.time() - self.election_coordinator.election_start_time
            ) * 1000
            self._update_election_duration(duration)

    async def _request_vote_from_node(self, target_node: str) -> None:
        """Send RequestVote RPC to a specific node."""
        try:
            request = RequestVoteRequest(
                term=self.persistent_state.current_term,
                candidate_id=self.node_id,
                last_log_index=self._last_log_index(),
                last_log_term=self._last_log_term(),
            )

            response = await self.transport.send_request_vote(target_node, request)

            if response:
                async with self.state_lock:
                    raft_log.debug(
                        f"Vote response from {target_node}: vote_granted={response.vote_granted}, response_term={response.term}, our_state={self.current_state}, our_term={self.persistent_state.current_term}"
                    )

                    # If response has higher term, step down
                    if response.term > self.persistent_state.current_term:
                        await self._update_term(response.term)
                        await self._convert_to_follower(restart_timer=False)
                        return

                    # If vote granted and we're still candidate in same term
                    if (
                        response.vote_granted
                        and self.current_state == RaftState.CANDIDATE
                        and response.term == self.persistent_state.current_term
                    ):
                        self.votes_received.add(response.voter_id)
                        raft_log.info(
                            f"[{self.node_id}] COUNTED vote from {response.voter_id} for term {response.term}, total votes: {len(self.votes_received)}"
                        )

                        # Check if we now have enough votes to become leader
                        majority_threshold = len(self.cluster_members) // 2 + 1
                        if len(self.votes_received) >= majority_threshold:
                            raft_log.info(
                                f"[{self.node_id}] ACHIEVED MAJORITY ({len(self.votes_received)}/{len(self.cluster_members)}), becoming leader"
                            )
                            await self._become_leader()
                            return
                    else:
                        raft_log.debug(
                            f"[{self.node_id}] Vote NOT counted from {target_node}: vote_granted={response.vote_granted}, state={self.current_state}, response_term={response.term}, our_term={self.persistent_state.current_term}"
                        )

        except Exception as e:
            raft_log.error(f"Exception in _request_vote_from_node({target_node}): {e}")
            import traceback

            raft_log.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def _become_leader(self) -> None:
        """Convert to leader state and initialize leader state."""
        raft_log.info(f"Became leader for term {self.persistent_state.current_term}")
        if RAFT_DIAG_ENABLED:
            raft_log.warning(
                "[DIAG_RAFT] node={} action=become_leader term={} members={}",
                self.node_id,
                self.persistent_state.current_term,
                len(self.cluster_members),
            )

        # Reset election backoff on successful election
        self._reset_election_backoff()

        self.current_state = RaftState.LEADER
        self.current_leader = self.node_id
        self.leader_volatile_state = LeaderVolatileState(
            self.cluster_members, self._last_log_index(), self.node_id
        )

        # Track when we became leader for smart step-down logic
        import time

        self.became_leader_time = time.time()

        self.metrics.elections_won += 1
        self.metrics.current_state = self.current_state

        # CRITICAL: Stop election coordinator when becoming leader to prevent election storms
        await self.election_coordinator.stop_coordinator()
        self.election_coordinator.election_in_progress = False
        if RAFT_DIAG_ENABLED:
            raft_log.warning(
                "[DIAG_RAFT] node={} action=coordinator_stopped_for_leader term={}",
                self.node_id,
                self.persistent_state.current_term,
            )

        # Add no-op entry to establish leadership
        noop_entry = LogEntry(
            term=self.persistent_state.current_term,
            index=self._last_log_index() + 1,
            entry_type=LogEntryType.NOOP,
            command=None,
            client_id=self.node_id,
        )

        new_log = list(self.persistent_state.log_entries) + [noop_entry]
        self.persistent_state = PersistentState(
            current_term=self.persistent_state.current_term,
            voted_for=self.persistent_state.voted_for,
            log_entries=new_log,
        )

        await self.storage.save_persistent_state(self.persistent_state)

        # CRITICAL: Update leader's match_index to reflect noop entry
        if self.leader_volatile_state:
            self.leader_volatile_state.match_index[self.node_id] = noop_entry.index

        # Update last heartbeat time as leader
        self.last_heartbeat_time = time.time()

        # Initialize follower contact tracking
        current_time = time.time()
        self.follower_contact_info.clear()
        self.last_majority_contact_time = current_time
        self.leader_start_time = current_time

        # Initialize contact info for all followers (start with never contacted)
        for member_id in self.cluster_members:
            if member_id != self.node_id:
                self.follower_contact_info[member_id] = FollowerContactInfo()

        # Start heartbeat/replication
        await self._start_heartbeat()
        if RAFT_DIAG_ENABLED:
            raft_log.warning(
                "[DIAG_RAFT] node={} action=heartbeat_started term={} noop_index={}",
                self.node_id,
                self.persistent_state.current_term,
                noop_entry.index,
            )

        raft_log.info(
            f"Leader initialization complete for term {self.persistent_state.current_term}"
        )

    # Log Replication Implementation
    async def _replicate_log_entries(self) -> None:
        """
        Replicate log entries to all followers using RAFT's original serial approach.

        RAFT was designed for serial communication, not concurrent task management.
        This prevents race conditions in match_index updates and commit_index calculations.
        """
        if self.current_state != RaftState.LEADER or not self.leader_volatile_state:
            return

        # OPTIMIZED: Collect replication coroutines and execute concurrently
        # This allows network I/O to happen in parallel while maintaining deterministic processing
        replication_coros = []
        for member_id in self.cluster_members:
            if member_id != self.node_id:
                replication_coros.append(self._replicate_to_follower(member_id))

        if replication_coros:
            try:
                # Execute all replication coroutines concurrently
                await asyncio.gather(*replication_coros)
            except asyncio.CancelledError:
                # Heartbeat cancelled - stop replication immediately
                raft_log.debug(
                    f"[{self.node_id}] Heartbeat cancelled during concurrent replication"
                )
                raise
            except Exception as e:
                # Log error - asyncio.gather will propagate the first exception
                raft_log.warning(f"Failed during concurrent replication: {e}")

        # Update commit index after all replications complete
        # This ensures match_index values are consistent
        old_commit_index = self.volatile_state.commit_index
        await self._update_commit_index()

        # CRITICAL: If commit_index advanced, immediately send another round of AppendEntries
        # to propagate the new commit_index to followers (RAFT SPEC requirement)
        if self.volatile_state.commit_index > old_commit_index:
            raft_log.debug(
                f"Commit index advanced to {self.volatile_state.commit_index}, sending immediate commit propagation"
            )

            # OPTIMIZED: Collect commit propagation coroutines and execute concurrently
            commit_propagation_coros = []
            for member_id in self.cluster_members:
                if member_id != self.node_id:
                    commit_propagation_coros.append(
                        self._replicate_to_follower(member_id)
                    )

            if commit_propagation_coros:
                try:
                    await asyncio.gather(*commit_propagation_coros)
                except Exception as e:
                    raft_log.warning(f"Failed during commit_index propagation: {e}")

    async def _replicate_to_follower(self, follower_id: str) -> None:
        """Replicate log entries to a specific follower."""
        if not self.leader_volatile_state:
            return

        try:
            next_index = self.leader_volatile_state.next_index[follower_id]

            # Check if we need to send snapshot
            if next_index <= 0 or (
                next_index <= len(self.persistent_state.log_entries)
                and len(self.persistent_state.log_entries) - next_index
                > self.config.max_log_entries_behind
            ):
                await self._send_snapshot_to_follower(follower_id)
                return

            # Prepare AppendEntries request
            prev_log_index = max(0, next_index - 1)
            prev_log_term = 0

            if prev_log_index > 0 and prev_log_index <= len(
                self.persistent_state.log_entries
            ):
                prev_log_term = self.persistent_state.log_entries[
                    prev_log_index - 1
                ].term

            # Get entries to send
            entries = []
            if next_index <= len(self.persistent_state.log_entries):
                max_entries = min(
                    self.config.max_log_entries_per_request,
                    len(self.persistent_state.log_entries) - next_index + 1,
                )
                entries = self.persistent_state.log_entries[
                    next_index - 1 : next_index - 1 + max_entries
                ]

            request = AppendEntriesRequest(
                term=self.persistent_state.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.volatile_state.commit_index,
            )

            # Send request with retry logic
            response = await self._send_append_entries_with_retry(follower_id, request)

            if response:
                async with self.state_lock:
                    await self._handle_append_entries_response(
                        follower_id, request, response
                    )
            else:
                # No response received - this indicates communication failure
                raft_log.warning(
                    f"[{self.node_id}] No response from {follower_id} - communication failed (PARTITION?)"
                )
                # Track failed contact with this follower
                contact_info = self.follower_contact_info.get(follower_id)
                if contact_info:
                    contact_info.mark_failed_contact(time.time())

        except Exception as e:
            raft_log.warning(f"Failed to replicate to {follower_id}: {e}")

    async def _send_append_entries_with_retry(
        self, follower_id: str, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        """Send AppendEntries with exponential backoff retry."""
        backoff_delay = 0.001
        attempt_limit = max(1, self.config.max_retry_attempts)
        if not request.entries:
            # Heartbeats should stay low-latency; retry on the next cycle instead.
            attempt_limit = 1
        rpc_timeout = self._append_entries_rpc_timeout(request)

        for attempt in range(attempt_limit):
            try:
                response = await asyncio.wait_for(
                    self.transport.send_append_entries(follower_id, request),
                    timeout=rpc_timeout,
                )

                if response:
                    self.metrics.append_entries_sent += 1
                    return response

            except TimeoutError:
                raft_log.debug(
                    f"AppendEntries timeout to {follower_id}, attempt {attempt + 1}"
                )
            except Exception as e:
                raft_log.warning(f"AppendEntries error to {follower_id}: {e}")

            if attempt < attempt_limit - 1:
                await asyncio.sleep(backoff_delay)
                backoff_delay = min(
                    backoff_delay * self.config.backoff_multiplier,
                    self.config.max_backoff_delay,
                )

        return None

    def _append_entries_rpc_timeout(self, request: AppendEntriesRequest) -> float:
        """Compute an adaptive RPC timeout for AppendEntries requests."""
        base_timeout = max(
            self.config.rpc_timeout, self.config.heartbeat_interval * 2.0
        )
        if not request.entries:
            return base_timeout
        max_entries = max(self.config.max_log_entries_per_request, 1)
        entry_factor = 1.0 + (len(request.entries) / max(max_entries / 4.0, 1.0))
        cluster_factor = max(1.0, (len(self.cluster_members) ** 0.5) / 2.0)
        timeout_multiplier = min(3.0, max(entry_factor, cluster_factor))
        adaptive_timeout = base_timeout * timeout_multiplier
        return min(adaptive_timeout, self.config.election_timeout_max * 2.5)

    async def _handle_append_entries_response(
        self,
        follower_id: str,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
    ) -> None:
        """Handle AppendEntries response from follower."""
        if not self.leader_volatile_state:
            return

        # If response has higher term, step down
        if response.term > self.persistent_state.current_term:
            await self._update_term(response.term)
            await self._convert_to_follower()
            return

        if response.success:
            # Track successful contact with this follower
            current_time = time.time()
            contact_info = self.follower_contact_info.get(follower_id)
            if contact_info:
                contact_info.mark_successful_contact(current_time)

            # Update next_index and match_index for successful replication
            if request.entries:
                self.leader_volatile_state.next_index[follower_id] = (
                    request.entries[-1].index + 1
                )
                self.leader_volatile_state.match_index[follower_id] = request.entries[
                    -1
                ].index
            else:
                # Heartbeat success - follower is caught up to prev_log_index
                self.leader_volatile_state.match_index[follower_id] = max(
                    self.leader_volatile_state.match_index[follower_id],
                    request.prev_log_index,
                )

            raft_log.debug(
                f"Successful replication to {follower_id}, match_index: "
                f"{self.leader_volatile_state.match_index[follower_id]}"
            )

        else:
            # Handle replication failure - decrement next_index
            if response.conflict_index > 0 and response.conflict_term >= 0:
                # Use conflict optimization
                conflict_index = response.conflict_index

                # Find last entry in our log with conflict_term
                last_term_index = 0
                for i, entry in enumerate(self.persistent_state.log_entries):
                    if entry.term == response.conflict_term:
                        last_term_index = i + 1

                if last_term_index > 0:
                    self.leader_volatile_state.next_index[follower_id] = (
                        last_term_index + 1
                    )
                else:
                    self.leader_volatile_state.next_index[follower_id] = conflict_index
            else:
                # Fallback: decrement next_index
                self.leader_volatile_state.next_index[follower_id] = max(
                    1, self.leader_volatile_state.next_index[follower_id] - 1
                )

            raft_log.debug(
                f"Replication failed to {follower_id}, next_index: "
                f"{self.leader_volatile_state.next_index[follower_id]}"
            )

    async def _update_commit_index(self) -> None:
        """Update commit index based on successful replications."""
        if not self.leader_volatile_state:
            return

        # Find the highest index that has been replicated to majority of servers
        # Include all cluster members (leader's match_index is already set correctly)
        match_indices = [
            self.leader_volatile_state.match_index[node]
            for node in self.cluster_members
        ]
        match_indices.sort(reverse=True)

        # Majority threshold
        majority_threshold = len(self.cluster_members) // 2 + 1

        if len(match_indices) >= majority_threshold:
            new_commit_index = match_indices[majority_threshold - 1]

            # Only commit entries from current term (safety requirement)
            if (
                new_commit_index > self.volatile_state.commit_index
                and new_commit_index <= len(self.persistent_state.log_entries)
                and self.persistent_state.log_entries[new_commit_index - 1].term
                == self.persistent_state.current_term
            ):
                old_commit_index = self.volatile_state.commit_index
                self.volatile_state.commit_index = new_commit_index
                self.metrics.commit_index = new_commit_index

                raft_log.debug(
                    f"Updated commit index from {old_commit_index} to {new_commit_index}"
                )

                # RAFT SPEC: Apply committed entries immediately when commit_index advances
                await self._apply_committed_entries()

    # Background Tasks
    async def _start_background_tasks(self) -> None:
        """Start background tasks for Raft operation."""
        # CLEAN ARCHITECTURE: Use centralized election coordinator
        await self.election_coordinator.start_coordinator(
            self.node_id, self._start_election_with_semaphore
        )
        if RAFT_DIAG_ENABLED:
            raft_log.warning(
                "[DIAG_RAFT] node={} action=coordinator_started election_timeout_min={:.4f}s "
                "election_timeout_max={:.4f}s timeout_bias={:.4f}s",
                self.node_id,
                self.election_coordinator.election_timeout_min,
                self.election_coordinator.election_timeout_max,
                self.election_coordinator.timeout_bias_seconds,
            )

    async def _stop_background_tasks(self) -> None:
        """Stop all background tasks using centralized task manager."""
        raft_log.debug(f"[{self.node_id}] _stop_background_tasks called")

        # CLEAN ARCHITECTURE: Stop centralized coordinator
        await self.election_coordinator.stop_coordinator()

        # IMPROVED: Use centralized task manager for proper shutdown control
        await self.task_manager.stop_all_tasks(timeout=2.0)

    async def _start_heartbeat(self) -> None:
        """Start heartbeat for leader using task manager."""
        # Check if heartbeat task is already running
        if "core" in self.task_manager.task_groups:
            existing_task = self.task_manager.task_groups["core"].tasks.get("heartbeat")
            if existing_task and existing_task.is_active():
                raft_log.debug(
                    f"[{self.node_id}] Heartbeat task already running, not restarting"
                )
                return

        # Start new heartbeat task
        await self.task_manager.create_task("core", "heartbeat", self._heartbeat_loop)

    async def _heartbeat_loop(self) -> None:
        """Main heartbeat loop for leader."""
        try:
            while self.current_state == RaftState.LEADER:
                # Send heartbeats/replicate entries
                await self._replicate_log_entries()

                # Check majority contact and potentially step down
                await self._check_majority_contact()
                try:
                    await self._check_leader_step_down()
                except Exception as e:
                    raft_log.error(
                        f"[{self.node_id}] Error in _check_leader_step_down: {e}"
                    )
                    import traceback

                    raft_log.error(
                        f"[{self.node_id}] Step-down traceback: {traceback.format_exc()}"
                    )

                # Wait for next heartbeat interval
                await asyncio.sleep(self.config.heartbeat_interval)

        except asyncio.CancelledError:
            raft_log.debug("Heartbeat task cancelled")
        except Exception as e:
            raft_log.error(f"Error in heartbeat loop: {e}")

    async def _apply_committed_entries(self) -> None:
        """Apply committed entries to state machine."""
        try:
            while self.volatile_state.last_applied < self.volatile_state.commit_index:
                next_index = self.volatile_state.last_applied + 1

                if next_index <= len(self.persistent_state.log_entries):
                    entry = self.persistent_state.log_entries[next_index - 1]

                    # Only apply command entries to state machine
                    if (
                        entry.entry_type == LogEntryType.COMMAND
                        and entry.command is not None
                    ):
                        try:
                            await self.state_machine.apply_command(
                                entry.command, entry.index
                            )
                            self.metrics.commands_applied += 1
                            raft_log.debug(f"Applied command at index {entry.index}")
                        except Exception as e:
                            raft_log.error(
                                f"Error applying command at index {entry.index}: {e}"
                            )

                    self.volatile_state.last_applied = next_index
                    self.metrics.last_applied = next_index
                else:
                    # Gap in log entries - this shouldn't happen
                    raft_log.error(f"Gap in log entries at index {next_index}")
                    break

        except Exception as e:
            raft_log.error(f"Error applying committed entries: {e}")

    async def _check_majority_contact(self) -> None:
        """Check if we have majority contact and update last_majority_contact_time."""
        if self.current_state != RaftState.LEADER:
            return

        current_time = time.time()
        majority_threshold = len(self.cluster_members) // 2 + 1

        # Count nodes we can contact (including ourselves)
        reachable_count = 1  # Count ourselves

        contact_details = []
        contact_timeout = self.config.election_timeout_max * 2
        for member_id in self.cluster_members:
            if member_id != self.node_id:
                contact_info = self.follower_contact_info.get(member_id)
                if contact_info:
                    is_reachable = contact_info.is_reachable(
                        current_time, contact_timeout
                    )
                    if is_reachable:
                        reachable_count += 1
                    time_since_contact = (
                        current_time - contact_info.last_successful_contact
                        if contact_info.contact_status != ContactStatus.NEVER_CONTACTED
                        else float("inf")
                    )
                    status_symbol = "✓" if is_reachable else "✗"
                    contact_details.append(
                        f"{member_id}:{contact_info.contact_status.value}:{time_since_contact:.3f}s({status_symbol})"
                    )
                else:
                    contact_details.append(f"{member_id}:missing(✗)")

        raft_log.debug(
            f"[{self.node_id}] Contact status: {','.join(contact_details)}, timeout={contact_timeout:.3f}s"
        )

        if reachable_count >= majority_threshold:
            self.last_majority_contact_time = current_time
            raft_log.debug(
                f"[{self.node_id}] Majority contact maintained: {reachable_count}/{len(self.cluster_members)}"
            )
        else:
            raft_log.warning(
                f"[{self.node_id}] Lost majority contact: only {reachable_count}/{len(self.cluster_members)} reachable (need {majority_threshold}) - "
                f"last_majority_contact_time={self.last_majority_contact_time:.3f}, current_time={current_time:.3f}"
            )
            # Do NOT update last_majority_contact_time when we lose majority contact

    def _calculate_step_down_timeout(self, current_time: float) -> float:
        """
        Calculate smart step-down timeout to distinguish between network partitions and normal failover.

        This timeout must stay above normal heartbeat/RPC jitter under load, while still allowing
        minority leaders to step down promptly during true partitions.
        """
        follower_count = max(len(self.cluster_members) - 1, 1)
        cluster_scale = max(1.0, follower_count**0.5)
        heartbeat_window = self.config.heartbeat_interval * max(
            6.0, cluster_scale * 3.0
        )
        rpc_window = self.config.rpc_timeout * max(3.0, cluster_scale * 1.75)
        election_window = self.config.election_timeout_max * max(
            1.2, cluster_scale * 0.75
        )
        floor_timeout = max(self.config.election_timeout_min, self.config.rpc_timeout)
        cap_timeout = self.config.election_timeout_max * 4.0
        base_timeout = min(
            cap_timeout,
            max(floor_timeout, heartbeat_window, rpc_window, election_window),
        )

        time_since_became_leader = current_time - self.became_leader_time
        stabilization_window = max(base_timeout, self.config.election_timeout_max)
        if time_since_became_leader < stabilization_window:
            return min(self.config.election_timeout_max * 6.0, base_timeout * 1.5)
        return base_timeout

    async def _check_leader_step_down(self) -> None:
        """Check if leader should step down due to lack of majority contact."""
        if self.current_state != RaftState.LEADER:
            return

        current_time = time.time()
        time_since_majority_contact = current_time - self.last_majority_contact_time

        # Use smart step-down timeout calculation
        step_down_timeout = self._calculate_step_down_timeout(current_time)

        raft_log.debug(
            f"[{self.node_id}] Step-down check: {time_since_majority_contact:.3f}s since majority contact "
            f"(timeout: {step_down_timeout:.3f}s)"
        )

        if time_since_majority_contact > step_down_timeout:
            raft_log.warning(
                f"[{self.node_id}] Stepping down as leader: no majority contact for {time_since_majority_contact:.3f}s "
                f"(timeout: {step_down_timeout:.3f}s)"
            )
            await self._convert_to_follower(restart_timer=True)
            # Add backoff delay to prevent immediate re-election cycle
            backoff_delay = min(2.0, step_down_timeout)
            raft_log.debug(
                f"[{self.node_id}] Adding {backoff_delay:.1f}s backoff after step-down"
            )
            await asyncio.sleep(backoff_delay)

    # Snapshot Management
    async def _send_snapshot_to_follower(self, follower_id: str) -> None:
        """Send snapshot to follower that is too far behind."""
        try:
            # Create snapshot
            snapshot_data = await self.state_machine.create_snapshot()

            snapshot = RaftSnapshot(
                last_included_index=self.volatile_state.last_applied,
                last_included_term=self._get_term_at_index(
                    self.volatile_state.last_applied
                ),
                state_machine_state=snapshot_data,
                configuration=set(self.cluster_members),
            )

            # Send snapshot in chunks
            chunk_size = 8192  # 8KB chunks
            offset = 0

            while offset < len(snapshot_data):
                chunk_end = min(offset + chunk_size, len(snapshot_data))
                chunk = snapshot_data[offset:chunk_end]
                is_last_chunk = chunk_end >= len(snapshot_data)

                request = InstallSnapshotRequest(
                    term=self.persistent_state.current_term,
                    leader_id=self.node_id,
                    last_included_index=snapshot.last_included_index,
                    last_included_term=snapshot.last_included_term,
                    data=chunk,
                    done=is_last_chunk,
                    offset=offset,
                )

                response = await self.transport.send_install_snapshot(
                    follower_id, request
                )

                if response:
                    if response.term > self.persistent_state.current_term:
                        await self._update_term(response.term)
                        await self._convert_to_follower()
                        return
                else:
                    raft_log.warning(f"Failed to send snapshot chunk to {follower_id}")
                    return

                offset = chunk_end

            # Update next_index for this follower
            if self.leader_volatile_state:
                self.leader_volatile_state.next_index[follower_id] = (
                    snapshot.last_included_index + 1
                )
                self.leader_volatile_state.match_index[follower_id] = (
                    snapshot.last_included_index
                )

            self.metrics.snapshots_created += 1
            raft_log.info(f"Successfully sent snapshot to {follower_id}")

        except Exception as e:
            raft_log.error(f"Error sending snapshot to {follower_id}: {e}")

    # Utility Methods
    def _last_log_index(self) -> int:
        """Get index of last log entry (0 if log is empty)."""
        return len(self.persistent_state.log_entries)

    def _last_log_term(self) -> int:
        """Get term of last log entry (0 if log is empty)."""
        if not self.persistent_state.log_entries:
            return 0
        return self.persistent_state.log_entries[-1].term

    def _get_term_at_index(self, index: int) -> int:
        """Get term of entry at given index (0 if index is 0 or out of bounds)."""
        if index <= 0 or index > len(self.persistent_state.log_entries):
            return 0
        return self.persistent_state.log_entries[index - 1].term

    async def _update_term(self, new_term: int) -> None:
        """Update current term and clear voted_for."""
        if new_term > self.persistent_state.current_term:
            raft_log.info(
                f"Updating term from {self.persistent_state.current_term} to {new_term}"
            )

            self.persistent_state = PersistentState(
                current_term=new_term,
                voted_for=None,  # Clear vote when term changes
                log_entries=self.persistent_state.log_entries,
            )

            await self.storage.save_persistent_state(self.persistent_state)
            self.metrics.current_term = new_term

    async def _convert_to_follower(
        self, restart_timer: bool = True, reset_backoff: bool = False
    ) -> None:
        """Convert to follower state."""
        raft_log.debug(
            f"[{self.node_id}] _convert_to_follower called, restart_timer={restart_timer}, current_state={self.current_state}"
        )
        if self.current_state != RaftState.FOLLOWER:
            raft_log.info(
                f"[{self.node_id}] Converting from {self.current_state.value} to follower"
            )

            old_state = self.current_state
            self.current_state = RaftState.FOLLOWER
            self.leader_volatile_state = None
            self.election_coordinator.election_in_progress = False
            self.votes_received.clear()
            self.current_leader = None

            # Stop leader-only tasks when stepping down from leader.
            # Keep election coordinator running for candidate/follower transitions.
            if old_state == RaftState.LEADER:
                await self.task_manager.stop_specific_task(
                    "core", "heartbeat", timeout=0.5
                )
                await self.task_manager.stop_task_group("replication", timeout=0.5)
                if RAFT_DIAG_ENABLED:
                    raft_log.warning(
                        "[DIAG_RAFT] node={} action=leader_tasks_stopped old_state={} term={}",
                        self.node_id,
                        old_state.value,
                        self.persistent_state.current_term,
                    )

            # CRITICAL: Restart election coordinator when stepping down from leader
            if (
                old_state == RaftState.LEADER
                and self.current_state == RaftState.FOLLOWER
            ):
                # Leader stepping down - restart election coordinator
                await self.election_coordinator.start_coordinator(
                    self.node_id, self._start_election_with_semaphore
                )
                raft_log.info(
                    f"[{self.node_id}] Restarted election coordinator after stepping down from leader"
                )
                if RAFT_DIAG_ENABLED:
                    raft_log.warning(
                        "[DIAG_RAFT] node={} action=coordinator_restarted_after_stepdown "
                        "term={} restart_timer={}",
                        self.node_id,
                        self.persistent_state.current_term,
                        restart_timer,
                    )

            # Avoid immediate trigger loops; coordinator timeout cadence drives retries.
            if restart_timer and self.last_heartbeat_time <= 0:
                self.last_heartbeat_time = time.time()
            # Note: Don't reset last_heartbeat_time here - it should only be set when we actually receive heartbeats

            # Only reset election backoff when explicitly requested (accepting valid leader)
            if reset_backoff:
                self._reset_election_backoff()

            self.metrics.current_state = self.current_state
            if RAFT_DIAG_ENABLED:
                raft_log.warning(
                    "[DIAG_RAFT] node={} action=converted_to_follower term={} reset_backoff={} "
                    "restart_timer={} last_heartbeat_time={:.6f}",
                    self.node_id,
                    self.persistent_state.current_term,
                    reset_backoff,
                    restart_timer,
                    self.last_heartbeat_time,
                )

    def _update_exponential_average(
        self, current_value: float, new_value: float, alpha: float = 0.1
    ) -> float:
        """Calculate exponential moving average."""
        return current_value * (1.0 - alpha) + new_value * alpha

    def _update_command_commit_latency(self, latency_ms: float) -> None:
        """Update average command commit latency with exponential decay."""
        self.metrics.average_command_commit_latency_ms = (
            self._update_exponential_average(
                self.metrics.average_command_commit_latency_ms, latency_ms
            )
        )

    def _update_election_duration(self, duration_ms: float) -> None:
        """Update average election duration with exponential decay."""
        self.metrics.average_election_duration_ms = self._update_exponential_average(
            self.metrics.average_election_duration_ms, duration_ms
        )

    async def _apply_snapshot(self, snapshot: RaftSnapshot) -> None:
        """Apply snapshot to state machine and update state."""
        try:
            # Restore state machine from snapshot
            await self.state_machine.restore_from_snapshot(snapshot.state_machine_state)

            # Update volatile state
            self.volatile_state.last_applied = snapshot.last_included_index
            self.volatile_state.commit_index = max(
                self.volatile_state.commit_index, snapshot.last_included_index
            )

            # Trim log entries that are included in snapshot
            remaining_entries = []
            for entry in self.persistent_state.log_entries:
                if entry.index > snapshot.last_included_index:
                    remaining_entries.append(entry)

            # Update persistent state
            self.persistent_state = PersistentState(
                current_term=self.persistent_state.current_term,
                voted_for=self.persistent_state.voted_for,
                log_entries=remaining_entries,
            )

            await self.storage.save_persistent_state(self.persistent_state)

            raft_log.info(
                f"Applied snapshot up to index {snapshot.last_included_index}, "
                f"trimmed log to {len(remaining_entries)} entries"
            )

        except Exception as e:
            raft_log.error(f"Error applying snapshot: {e}")
            raise
