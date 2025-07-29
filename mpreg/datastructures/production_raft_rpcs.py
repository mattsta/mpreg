"""
Production Raft RPC Handlers and Core Algorithm Implementation.

This module contains the RPC handlers for RequestVote, AppendEntries, and
InstallSnapshot, as well as the core state transition logic.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from .production_raft import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    PersistentState,
    RaftSnapshot,
    RaftState,
    RequestVoteRequest,
    RequestVoteResponse,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# Extension of ProductionRaft class with RPC handlers
class ProductionRaftRPCs:
    """Mixin class containing Raft RPC handlers and state transition logic."""

    # Type hints for attributes that will be provided by ProductionRaft
    if TYPE_CHECKING:
        node_id: str
        state_lock: asyncio.Lock
        persistent_state: PersistentState
        current_state: RaftState
        last_heartbeat_time: float
        storage: Any
        metrics: Any
        volatile_state: Any
        votes_received: set[str]
        heartbeat_task: Any
        apply_entries_task: Any
        snapshot_chunks: dict[str, list[bytes]]
        cluster_members: set[str]
        state_machine: Any
        current_leader: str | None
        leader_volatile_state: Any
        election_in_progress: bool
        installing_snapshot: bool

        # Methods that will be provided by ProductionRaft
        async def _update_term(self, new_term: int) -> None: ...
        async def _convert_to_follower(
            self, restart_timer: bool = True, reset_backoff: bool = False
        ) -> None: ...
        def _last_log_index(self) -> int: ...
        def _last_log_term(self) -> int: ...
        async def _start_election_timer(self) -> None: ...
        async def _apply_committed_entries(self) -> None: ...

    # RequestVote RPC Handler
    async def handle_request_vote(
        self, request: RequestVoteRequest
    ) -> RequestVoteResponse:
        """
        Handle RequestVote RPC as specified in Raft paper.

        Receiver implementation:
        1. Reply false if term < currentTerm
        2. If votedFor is null or candidateId, and candidate's log is at least
           as up-to-date as receiver's log, grant vote
        """
        async with self.state_lock:
            self.metrics.votes_requested += 1

            # Rule 1: Reply false if term < currentTerm
            if request.term < self.persistent_state.current_term:
                logger.debug(
                    f"Rejecting vote for {request.candidate_id}: stale term "
                    f"{request.term} < {self.persistent_state.current_term}"
                )
                return RequestVoteResponse(
                    term=self.persistent_state.current_term,
                    vote_granted=False,
                    voter_id=self.node_id,
                )

            # If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
            if request.term > self.persistent_state.current_term:
                await self._update_term(request.term)
                await self._convert_to_follower()

            vote_granted = False

            # Rule 2: Grant vote if haven't voted or voted for this candidate, and candidate's log is up-to-date
            if (
                self.persistent_state.voted_for is None
                or self.persistent_state.voted_for == request.candidate_id
            ):
                # Check if candidate's log is at least as up-to-date as ours
                last_log_term = self._last_log_term()
                last_log_index = self._last_log_index()

                candidate_log_up_to_date = request.last_log_term > last_log_term or (
                    request.last_log_term == last_log_term
                    and request.last_log_index >= last_log_index
                )

                if candidate_log_up_to_date:
                    vote_granted = True

                    # Record vote
                    self.persistent_state = PersistentState(
                        current_term=self.persistent_state.current_term,
                        voted_for=request.candidate_id,
                        log_entries=self.persistent_state.log_entries,
                    )

                    # Persist vote
                    await self.storage.save_persistent_state(self.persistent_state)

                    # Reset election timer since we granted a vote
                    self.last_heartbeat_time = time.time()

                    logger.info(
                        f"Granted vote to {request.candidate_id} for term {request.term}"
                    )
                else:
                    logger.debug(
                        f"Rejecting vote for {request.candidate_id}: log not up-to-date. "
                        f"Candidate: term={request.last_log_term}, index={request.last_log_index}. "
                        f"Our: term={last_log_term}, index={last_log_index}"
                    )
            else:
                logger.debug(
                    f"Rejecting vote for {request.candidate_id}: already voted for "
                    f"{self.persistent_state.voted_for}"
                )

            if vote_granted:
                self.metrics.votes_granted += 1

            return RequestVoteResponse(
                term=self.persistent_state.current_term,
                vote_granted=vote_granted,
                voter_id=self.node_id,
            )

    # AppendEntries RPC Handler
    async def handle_append_entries(
        self, request: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        """
        Handle AppendEntries RPC as specified in Raft paper.

        Receiver implementation:
        1. Reply false if term < currentTerm
        2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        3. If an existing entry conflicts with a new one, delete the existing entry and all that follow it
        4. Append any new entries not already in the log
        5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        """
        async with self.state_lock:
            self.metrics.append_entries_received += 1

            # Rule 1: Reply false if term < currentTerm
            if request.term < self.persistent_state.current_term:
                logger.debug(
                    f"Rejecting AppendEntries from {request.leader_id}: stale term "
                    f"{request.term} < {self.persistent_state.current_term}"
                )
                return AppendEntriesResponse(
                    term=self.persistent_state.current_term,
                    success=False,
                    follower_id=self.node_id,
                )

            # If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
            if request.term > self.persistent_state.current_term:
                await self._update_term(request.term)

            # Always convert to follower when receiving valid AppendEntries
            # Reset election backoff since we're receiving from a valid leader
            if self.current_state != RaftState.FOLLOWER:
                await self._convert_to_follower(reset_backoff=True)

            # Update current leader and reset election timer
            self.current_leader = request.leader_id
            self.last_heartbeat_time = time.time()

            # Rule 2: Reply false if log doesn't contain entry at prevLogIndex with matching term
            if request.prev_log_index > 0:
                if (
                    request.prev_log_index > len(self.persistent_state.log_entries)
                    or self.persistent_state.log_entries[
                        request.prev_log_index - 1
                    ].term
                    != request.prev_log_term
                ):
                    # Find conflict information to help leader optimize
                    conflict_index = min(
                        request.prev_log_index, len(self.persistent_state.log_entries)
                    )
                    conflict_term = -1

                    if conflict_index > 0:
                        conflict_term = self.persistent_state.log_entries[
                            conflict_index - 1
                        ].term

                    logger.debug(
                        f"Log consistency check failed for AppendEntries from {request.leader_id}. "
                        f"prevLogIndex={request.prev_log_index}, prevLogTerm={request.prev_log_term}"
                    )

                    self.metrics.append_entries_failure += 1
                    return AppendEntriesResponse(
                        term=self.persistent_state.current_term,
                        success=False,
                        follower_id=self.node_id,
                        conflict_index=conflict_index,
                        conflict_term=conflict_term,
                    )

            # Rules 3 & 4: Handle log entries
            success = True
            match_index = request.prev_log_index

            if request.entries:
                try:
                    # Create new log by taking entries up to prev_log_index and appending new entries
                    new_log = list(
                        self.persistent_state.log_entries[: request.prev_log_index]
                    )

                    for entry in request.entries:
                        # Verify entry integrity
                        if not entry.verify_integrity():
                            logger.error(
                                f"Log entry {entry.index} failed integrity check"
                            )
                            success = False
                            break

                        new_log.append(entry)
                        match_index = entry.index

                    if success:
                        # Update persistent state with new log
                        self.persistent_state = PersistentState(
                            current_term=self.persistent_state.current_term,
                            voted_for=self.persistent_state.voted_for,
                            log_entries=new_log,
                        )

                        # Persist state
                        await self.storage.save_persistent_state(self.persistent_state)

                        self.metrics.log_entries_replicated += len(request.entries)
                        logger.debug(
                            f"Appended {len(request.entries)} entries from {request.leader_id}"
                        )

                except Exception as e:
                    logger.error(f"Error processing log entries: {e}")
                    success = False

            # Rule 5: Update commit index
            if success and request.leader_commit > self.volatile_state.commit_index:
                old_commit_index = self.volatile_state.commit_index
                self.volatile_state.commit_index = min(
                    request.leader_commit, len(self.persistent_state.log_entries)
                )

                if self.volatile_state.commit_index > old_commit_index:
                    logger.debug(
                        f"Updated commit index from {old_commit_index} to {self.volatile_state.commit_index}"
                    )

                    # Trigger state machine application (RAFT SPEC: apply when commit_index advances)
                    await self._apply_committed_entries()

            if success:
                self.metrics.append_entries_success += 1
            else:
                self.metrics.append_entries_failure += 1

            return AppendEntriesResponse(
                term=self.persistent_state.current_term,
                success=success,
                follower_id=self.node_id,
                match_index=match_index if success else 0,
            )

    # InstallSnapshot RPC Handler
    async def handle_install_snapshot(
        self, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse:
        """
        Handle InstallSnapshot RPC for log compaction.

        Receiver implementation:
        1. Reply immediately if term < currentTerm
        2. Create new snapshot file if first chunk (offset is 0)
        3. Write data into snapshot file at given offset
        4. Reply and wait for more data chunks if done is false
        5. Save snapshot file, discard any existing or partial snapshot with a smaller index
        6. If existing log entry has same index and term as snapshot's last included entry,
           retain log entries following it and reply
        7. Discard the entire log
        8. Reset state machine using snapshot contents
        """
        async with self.state_lock:
            # Rule 1: Reply immediately if term < currentTerm
            if request.term < self.persistent_state.current_term:
                return InstallSnapshotResponse(
                    term=self.persistent_state.current_term, follower_id=self.node_id
                )

            # Update term and convert to follower if necessary
            if request.term > self.persistent_state.current_term:
                await self._update_term(request.term)
                await self._convert_to_follower()

            # Update leader and reset election timer
            self.current_leader = request.leader_id
            self.last_heartbeat_time = time.time()

            # Handle snapshot chunks
            snapshot_id = f"{request.leader_id}:{request.last_included_index}:{request.last_included_term}"

            # Rules 2 & 3: Handle snapshot data
            if request.offset == 0:
                # First chunk - initialize chunk storage
                self.snapshot_chunks[snapshot_id] = []
                self.installing_snapshot = True
                logger.info(
                    f"Starting snapshot installation from {request.leader_id}, "
                    f"last_included_index={request.last_included_index}"
                )

            # Store chunk data
            if snapshot_id in self.snapshot_chunks:
                expected_offset = sum(
                    len(chunk) for chunk in self.snapshot_chunks[snapshot_id]
                )
                if request.offset == expected_offset:
                    self.snapshot_chunks[snapshot_id].append(request.data)
                else:
                    logger.error(
                        f"Unexpected snapshot chunk offset: expected {expected_offset}, got {request.offset}"
                    )
                    # Reset snapshot installation
                    self.snapshot_chunks.pop(snapshot_id, None)
                    self.installing_snapshot = False
                    return InstallSnapshotResponse(
                        term=self.persistent_state.current_term,
                        follower_id=self.node_id,
                    )

            # Rule 4: Wait for more chunks if not done
            if not request.done:
                return InstallSnapshotResponse(
                    term=self.persistent_state.current_term, follower_id=self.node_id
                )

            # Rules 5-8: Complete snapshot installation
            try:
                # Combine all chunks
                complete_snapshot_data = b"".join(self.snapshot_chunks[snapshot_id])

                # Create snapshot object
                snapshot = RaftSnapshot(
                    last_included_index=request.last_included_index,
                    last_included_term=request.last_included_term,
                    state_machine_state=complete_snapshot_data,
                    configuration=set(self.cluster_members),
                )

                # Apply snapshot
                await self._apply_snapshot(snapshot)

                # Save snapshot to storage
                await self.storage.save_snapshot(snapshot)

                # Clean up
                self.snapshot_chunks.pop(snapshot_id, None)
                self.installing_snapshot = False

                self.metrics.snapshots_installed += 1
                logger.info(
                    f"Successfully installed snapshot up to index {request.last_included_index}"
                )

            except Exception as e:
                logger.error(f"Error installing snapshot: {e}")
                self.snapshot_chunks.pop(snapshot_id, None)
                self.installing_snapshot = False

            return InstallSnapshotResponse(
                term=self.persistent_state.current_term, follower_id=self.node_id
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

            logger.info(
                f"Applied snapshot up to index {snapshot.last_included_index}, "
                f"trimmed log to {len(remaining_entries)} entries"
            )

        except Exception as e:
            logger.error(f"Error applying snapshot: {e}")
            raise

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
