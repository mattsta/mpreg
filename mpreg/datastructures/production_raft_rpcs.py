"""
Production Raft RPC Handlers and Core Algorithm Implementation.

This module contains the RPC handlers for RequestVote, AppendEntries, and
InstallSnapshot, as well as the core state transition logic.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

from loguru import logger

from mpreg.core.errors import OPERATIONAL_EXCEPTIONS, log_caught_exception

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

rpc_log = logger


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

        def _note_leader_contact(self, *, source: str = "") -> None: ...

        storage: Any
        metrics: Any
        volatile_state: Any
        votes_received: set[str]
        heartbeat_task: Any
        apply_entries_task: Any
        snapshot_chunks: dict[str, list[bytes]]
        _snapshot_chunk_started_at: dict[str, float]
        _snapshot_chunk_bytes: int
        _snapshot_chunk_max_ids: int
        _snapshot_chunk_max_bytes: int
        _snapshot_chunk_ttl_seconds: float
        snapshot_chunk_aborts: int
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
        async def _run_pending_task_ops(self) -> None: ...
        def _last_log_index(self) -> int: ...
        def _last_log_term(self) -> int: ...
        def _get_term_at_index(self, index: int) -> int: ...
        def _get_entry_at_index(self, raft_index: int) -> Any: ...
        def _truncate_log_through(self, prev_log_index: int) -> list: ...

        _snapshot_last_index: int
        _snapshot_last_term: int

        async def _start_election_timer(self) -> None: ...
        async def _apply_committed_entries(self) -> None: ...

    # COR-T13-02 / PERF-T13-01: bound partial InstallSnapshot buffers
    def _snapshot_chunk_drop(self, snapshot_id: str, *, reason: str = "") -> None:
        chunks = self.snapshot_chunks.pop(snapshot_id, None)
        self._snapshot_chunk_started_at.pop(snapshot_id, None)
        if chunks:
            freed = sum(len(c) for c in chunks)
            self._snapshot_chunk_bytes = max(
                0, int(getattr(self, "_snapshot_chunk_bytes", 0)) - freed
            )
        self.snapshot_chunk_aborts = int(getattr(self, "snapshot_chunk_aborts", 0)) + 1
        # OBS-T13-02: optional Prom / metrics hook (server wires ServerMetricsTracker)
        cb = getattr(self, "on_snapshot_chunk_abort", None)
        if callable(cb):
            with contextlib.suppress(Exception):
                cb(1, int(getattr(self, "_snapshot_chunk_bytes", 0) or 0))
        if reason:
            rpc_log.warning(f"Dropped snapshot chunks id={snapshot_id} reason={reason}")

    def _snapshot_chunk_prune_stale(self, now: float | None = None) -> None:
        now = float(now if now is not None else time.time())
        ttl = float(getattr(self, "_snapshot_chunk_ttl_seconds", 120.0) or 120.0)
        started = getattr(self, "_snapshot_chunk_started_at", None)
        if not isinstance(started, dict):
            return
        stale = [sid for sid, ts in list(started.items()) if (now - float(ts)) > ttl]
        for sid in stale:
            self._snapshot_chunk_drop(sid, reason="ttl_expired")

    def _snapshot_chunk_can_accept(self, snapshot_id: str, chunk_len: int) -> bool:
        """Return False if accepting this chunk would exceed bounds."""
        self._snapshot_chunk_prune_stale()
        max_ids = int(getattr(self, "_snapshot_chunk_max_ids", 4) or 4)
        max_bytes = int(
            getattr(self, "_snapshot_chunk_max_bytes", 64 * 1024 * 1024)
            or (64 * 1024 * 1024)
        )
        if snapshot_id not in self.snapshot_chunks:
            # Evict oldest incomplete install if at id cap
            while len(self.snapshot_chunks) >= max_ids:
                # Prefer non-active ids; drop arbitrary oldest by start time
                started = getattr(self, "_snapshot_chunk_started_at", {}) or {}
                if not started:
                    victim = next(iter(self.snapshot_chunks))
                else:
                    victim = min(started.items(), key=lambda kv: kv[1])[0]
                self._snapshot_chunk_drop(victim, reason="max_concurrent_installs")
        cur_bytes = int(getattr(self, "_snapshot_chunk_bytes", 0) or 0)
        return not cur_bytes + max(0, int(chunk_len)) > max_bytes

    def _snapshot_chunk_note_append(self, snapshot_id: str, data: bytes) -> None:
        if snapshot_id not in self.snapshot_chunks:
            self.snapshot_chunks[snapshot_id] = []
            self._snapshot_chunk_started_at[snapshot_id] = time.time()
        self.snapshot_chunks[snapshot_id].append(data)
        self._snapshot_chunk_bytes = int(
            getattr(self, "_snapshot_chunk_bytes", 0) or 0
        ) + len(data)
        cb = getattr(self, "on_snapshot_chunk_bytes", None)
        if callable(cb):
            with contextlib.suppress(Exception):
                cb(int(self._snapshot_chunk_bytes))

    # RequestVote RPC Handler
    async def handle_request_vote(
        self, request: RequestVoteRequest
    ) -> RequestVoteResponse:
        """
        Handle RequestVote RPC as specified in Raft paper (+ pre-vote).

        Receiver implementation:
        1. Reply false if term < currentTerm (real vote) or if pre-vote term
           is not strictly greater than currentTerm when leader contact is fresh
        2. If votedFor is null or candidateId, and candidate's log is at least
           as up-to-date as receiver's log, grant vote
        3. Pre-vote (request.pre_vote): evaluate grant without advancing term,
           persisting voted_for, or stamping leader contact.
        """
        response: RequestVoteResponse | None = None
        try:
            async with self.state_lock:
                self.metrics.votes_requested += 1
                is_pre_vote = bool(getattr(request, "pre_vote", False))

                # --- Pre-vote path: no persistent side effects ---
                if is_pre_vote:
                    current_term = self.persistent_state.current_term
                    # Reject pre-votes for terms that cannot win a real election
                    # against our current term (must be > currentTerm to campaign).
                    if request.term <= current_term:
                        rpc_log.debug(
                            f"Rejecting pre-vote for {request.candidate_id}: "
                            f"term {request.term} <= current {current_term}"
                        )
                        return RequestVoteResponse(
                            term=current_term,
                            vote_granted=False,
                            voter_id=self.node_id,
                        )

                    # If we recently heard from a leader, refuse to encourage
                    # disruptive candidates (core pre-vote safety property).
                    import time as _time

                    quiet = self.config.election_timeout_min
                    last_hb = float(getattr(self, "last_heartbeat_time", 0.0) or 0.0)
                    if last_hb > 0.0 and (_time.time() - last_hb) < quiet:
                        rpc_log.debug(
                            f"Rejecting pre-vote for {request.candidate_id}: "
                            f"recent leader contact"
                        )
                        return RequestVoteResponse(
                            term=current_term,
                            vote_granted=False,
                            voter_id=self.node_id,
                        )

                    last_log_term = self._last_log_term()
                    last_log_index = self._last_log_index()
                    candidate_log_up_to_date = (
                        request.last_log_term > last_log_term
                        or (
                            request.last_log_term == last_log_term
                            and request.last_log_index >= last_log_index
                        )
                    )
                    # Pre-vote ignores voted_for (no real vote committed yet).
                    vote_granted = bool(candidate_log_up_to_date)
                    if vote_granted:
                        rpc_log.debug(
                            f"Granted pre-vote to {request.candidate_id} for "
                            f"prospective term {request.term}"
                        )
                    else:
                        rpc_log.debug(
                            f"Rejecting pre-vote for {request.candidate_id}: "
                            f"log not up-to-date"
                        )
                    return RequestVoteResponse(
                        term=current_term,
                        vote_granted=vote_granted,
                        voter_id=self.node_id,
                    )

                # Rule 1: Reply false if term < currentTerm
                if request.term < self.persistent_state.current_term:
                    rpc_log.debug(
                        f"Rejecting vote for {request.candidate_id}: stale term "
                        f"{request.term} < {self.persistent_state.current_term}"
                    )
                    response = RequestVoteResponse(
                        term=self.persistent_state.current_term,
                        vote_granted=False,
                        voter_id=self.node_id,
                    )
                    return response

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

                    candidate_log_up_to_date = (
                        request.last_log_term > last_log_term
                        or (
                            request.last_log_term == last_log_term
                            and request.last_log_index >= last_log_index
                        )
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
                        self._note_leader_contact(source="grant_vote")

                        rpc_log.info(
                            f"Granted vote to {request.candidate_id} for term {request.term}"
                        )
                    else:
                        rpc_log.debug(
                            f"Rejecting vote for {request.candidate_id}: log not up-to-date. "
                            f"Candidate: term={request.last_log_term}, index={request.last_log_index}. "
                            f"Our: term={last_log_term}, index={last_log_index}"
                        )
                else:
                    rpc_log.debug(
                        f"Rejecting vote for {request.candidate_id}: already voted for "
                        f"{self.persistent_state.voted_for}"
                    )

                if vote_granted:
                    self.metrics.votes_granted += 1

                response = RequestVoteResponse(
                    term=self.persistent_state.current_term,
                    vote_granted=vote_granted,
                    voter_id=self.node_id,
                )
                return response
        finally:
            await self._run_pending_task_ops()

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
        response: AppendEntriesResponse | None = None
        try:
            async with self.state_lock:
                self.metrics.append_entries_received += 1

                # Rule 1: Reply false if term < currentTerm
                if request.term < self.persistent_state.current_term:
                    rpc_log.debug(
                        f"Rejecting AppendEntries from {request.leader_id}: stale term "
                        f"{request.term} < {self.persistent_state.current_term}"
                    )
                    response = AppendEntriesResponse(
                        term=self.persistent_state.current_term,
                        success=False,
                        follower_id=self.node_id,
                    )
                    return response

                # If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
                if request.term > self.persistent_state.current_term:
                    await self._update_term(request.term)

                # Always convert to follower when receiving valid AppendEntries
                # Reset election backoff since we're receiving from a valid leader
                if self.current_state != RaftState.FOLLOWER:
                    await self._convert_to_follower(reset_backoff=True)

                # Update current leader and reset election timer
                self.current_leader = request.leader_id
                self._note_leader_contact(source="append_entries")

                # Rule 2: COR-T11-01 absolute index consistency (snapshot-aware)
                if request.prev_log_index > 0:
                    base = int(getattr(self, "_snapshot_last_index", 0) or 0)
                    local_term = -1
                    if request.prev_log_index == base:
                        local_term = int(getattr(self, "_snapshot_last_term", 0) or 0)
                    elif request.prev_log_index < base:
                        local_term = -1  # compacted away; reject
                    else:
                        entry_prev = self._get_entry_at_index(request.prev_log_index)
                        if entry_prev is not None:
                            local_term = int(entry_prev.term)

                    if local_term < 0 or local_term != request.prev_log_term:
                        conflict_index = request.prev_log_index
                        conflict_term = local_term if local_term >= 0 else -1
                        last = self._last_log_index()
                        if last < request.prev_log_index:
                            conflict_index = last + 1
                            conflict_term = -1
                        elif local_term >= 0:
                            conflict_term = local_term

                        rpc_log.debug(
                            f"Log consistency check failed for AppendEntries from {request.leader_id}. "
                            f"prevLogIndex={request.prev_log_index}, prevLogTerm={request.prev_log_term}"
                        )

                        self.metrics.append_entries_failure += 1
                        response = AppendEntriesResponse(
                            term=self.persistent_state.current_term,
                            success=False,
                            follower_id=self.node_id,
                            conflict_index=conflict_index,
                            conflict_term=conflict_term,
                        )
                        return response

                # Rules 3 & 4: Handle log entries (truncate by absolute entry.index)
                success = True
                match_index = request.prev_log_index

                if request.entries:
                    try:
                        new_log = self._truncate_log_through(request.prev_log_index)

                        for entry in request.entries:
                            if not entry.verify_integrity():
                                rpc_log.error(
                                    f"Log entry {entry.index} failed integrity check"
                                )
                                success = False
                                break

                            new_log.append(entry)
                            match_index = entry.index

                        if success:
                            self.persistent_state = PersistentState(
                                current_term=self.persistent_state.current_term,
                                voted_for=self.persistent_state.voted_for,
                                log_entries=new_log,
                            )

                            await self.storage.save_persistent_state(
                                self.persistent_state
                            )

                            self.metrics.log_entries_replicated += len(request.entries)
                            rpc_log.debug(
                                f"Appended {len(request.entries)} entries from {request.leader_id}"
                            )

                    except OPERATIONAL_EXCEPTIONS as e:
                        log_caught_exception(rpc_log, "Error processing log entries", e)
                        success = False

                # Rule 5: Update commit index (absolute last index, never bare len)
                if success and request.leader_commit > self.volatile_state.commit_index:
                    old_commit_index = self.volatile_state.commit_index
                    self.volatile_state.commit_index = min(
                        request.leader_commit, self._last_log_index()
                    )

                    if self.volatile_state.commit_index > old_commit_index:
                        rpc_log.debug(
                            f"Updated commit index from {old_commit_index} to {self.volatile_state.commit_index}"
                        )

                        # Trigger state machine application (RAFT SPEC: apply when commit_index advances)
                        await self._apply_committed_entries()

                if success:
                    self.metrics.append_entries_success += 1
                else:
                    self.metrics.append_entries_failure += 1

                response = AppendEntriesResponse(
                    term=self.persistent_state.current_term,
                    success=success,
                    follower_id=self.node_id,
                    match_index=match_index if success else 0,
                )
                return response
        finally:
            # Always flush deferred task ops after releasing state_lock.
            await self._run_pending_task_ops()

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
        try:
            async with self.state_lock:
                return await self._handle_install_snapshot_locked(request)
        finally:
            await self._run_pending_task_ops()

    async def _handle_install_snapshot_locked(
        self, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse:
        """InstallSnapshot body; caller holds state_lock and flushes task ops."""
        # Rule 1: Reply immediately if term < currentTerm
        if request.term < self.persistent_state.current_term:
            return InstallSnapshotResponse(
                term=self.persistent_state.current_term,
                follower_id=self.node_id,
                success=False,
            )

        # Update term and convert to follower if necessary
        if request.term > self.persistent_state.current_term:
            await self._update_term(request.term)
            await self._convert_to_follower()

        # Update leader and reset election timer
        self.current_leader = request.leader_id
        self._note_leader_contact(source="install_snapshot")

        # Handle snapshot chunks
        snapshot_id = (
            f"{request.leader_id}:{request.last_included_index}:"
            f"{request.last_included_term}"
        )
        chunk_data = (
            request.data if isinstance(request.data, (bytes, bytearray)) else b""
        )

        # Rules 2 & 3: Handle snapshot data
        if request.offset == 0:
            # First chunk — drop any prior buffer for this id, start fresh
            if snapshot_id in self.snapshot_chunks:
                self._snapshot_chunk_drop(snapshot_id, reason="restart_offset_0")
            if not self._snapshot_chunk_can_accept(snapshot_id, len(chunk_data)):
                self.installing_snapshot = False
                return InstallSnapshotResponse(
                    term=self.persistent_state.current_term,
                    follower_id=self.node_id,
                    success=False,
                )
            self._snapshot_chunk_note_append(snapshot_id, bytes(chunk_data))
            self.installing_snapshot = True
            rpc_log.info(
                f"Starting snapshot installation from {request.leader_id}, "
                f"last_included_index={request.last_included_index}"
            )
        elif snapshot_id in self.snapshot_chunks:
            expected_offset = sum(
                len(chunk) for chunk in self.snapshot_chunks[snapshot_id]
            )
            if request.offset != expected_offset:
                rpc_log.error(
                    f"Unexpected snapshot chunk offset: expected {expected_offset}, "
                    f"got {request.offset}"
                )
                self._snapshot_chunk_drop(snapshot_id, reason="offset_mismatch")
                self.installing_snapshot = False
                return InstallSnapshotResponse(
                    term=self.persistent_state.current_term,
                    follower_id=self.node_id,
                    success=False,
                )
            if not self._snapshot_chunk_can_accept(snapshot_id, len(chunk_data)):
                self._snapshot_chunk_drop(snapshot_id, reason="max_bytes")
                self.installing_snapshot = False
                return InstallSnapshotResponse(
                    term=self.persistent_state.current_term,
                    follower_id=self.node_id,
                    success=False,
                )
            self._snapshot_chunk_note_append(snapshot_id, bytes(chunk_data))
        else:
            # Mid-stream chunk without prior offset=0 — reject
            return InstallSnapshotResponse(
                term=self.persistent_state.current_term,
                follower_id=self.node_id,
                success=False,
            )

        # Rule 4: Wait for more chunks if not done
        if not request.done:
            return InstallSnapshotResponse(
                term=self.persistent_state.current_term,
                follower_id=self.node_id,
                success=True,
            )

        # Rules 5-8: Complete snapshot installation
        # COR-T10-01: never ACK success after apply/persist failure.
        try:
            # Combine all chunks
            complete_snapshot_data = b"".join(self.snapshot_chunks.get(snapshot_id, []))

            # Create snapshot object
            cfg = set(getattr(request, "configuration", ()) or ())
            if not cfg:
                cfg = set(self.cluster_members)
            snapshot = RaftSnapshot(
                last_included_index=request.last_included_index,
                last_included_term=request.last_included_term,
                state_machine_state=complete_snapshot_data,
                configuration=cfg,
            )

            # Apply snapshot
            await self._apply_snapshot(snapshot)

            # Save snapshot to storage
            await self.storage.save_snapshot(snapshot)

            # Clean up without counting as abort
            chunks = self.snapshot_chunks.pop(snapshot_id, None)
            self._snapshot_chunk_started_at.pop(snapshot_id, None)
            if chunks:
                freed = sum(len(c) for c in chunks)
                self._snapshot_chunk_bytes = max(
                    0, int(getattr(self, "_snapshot_chunk_bytes", 0)) - freed
                )
            self.installing_snapshot = False

            self.metrics.snapshots_installed += 1
            rpc_log.info(
                f"Successfully installed snapshot up to index "
                f"{request.last_included_index}"
            )
            return InstallSnapshotResponse(
                term=self.persistent_state.current_term,
                follower_id=self.node_id,
                success=True,
            )

        except OPERATIONAL_EXCEPTIONS as e:
            log_caught_exception(rpc_log, "Error installing snapshot", e)
            self._snapshot_chunk_drop(snapshot_id, reason="apply_or_persist_failed")
            self.installing_snapshot = False
            return InstallSnapshotResponse(
                term=self.persistent_state.current_term,
                follower_id=self.node_id,
                success=False,
            )

    async def _apply_snapshot(self, snapshot: RaftSnapshot) -> None:
        """Apply snapshot (COR-T11-01/05). Overridden by ProductionRaft when both exist."""
        try:
            await self.state_machine.restore_from_snapshot(snapshot.state_machine_state)

            self.volatile_state.last_applied = snapshot.last_included_index
            self.volatile_state.commit_index = max(
                self.volatile_state.commit_index, snapshot.last_included_index
            )

            self._snapshot_last_index = int(snapshot.last_included_index)
            self._snapshot_last_term = int(snapshot.last_included_term)

            remaining_entries = [
                entry
                for entry in self.persistent_state.log_entries
                if int(entry.index) > snapshot.last_included_index
            ]

            self.persistent_state = PersistentState(
                current_term=self.persistent_state.current_term,
                voted_for=self.persistent_state.voted_for,
                log_entries=remaining_entries,
            )

            await self.storage.save_persistent_state(self.persistent_state)

            cfg = getattr(snapshot, "configuration", None) or set()
            if cfg:
                self.cluster_members = {str(m) for m in cfg}

            rpc_log.info(
                f"Applied snapshot up to index {snapshot.last_included_index}, "
                f"trimmed log to {len(remaining_entries)} entries"
            )

        except OPERATIONAL_EXCEPTIONS as e:
            log_caught_exception(rpc_log, "Error applying snapshot", e)
            raise
        except Exception as e:
            log_caught_exception(rpc_log, "Error applying snapshot", e, expected=False)
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
