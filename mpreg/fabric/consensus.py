"""
Distributed State Management for the MPREG Fabric.

This module implements consensus mechanisms and conflict resolution for distributed
coordination in the unified fabric control plane. It provides eventual consistency
guarantees and handles state merge conflicts through sophisticated algorithms.

Key Features:
- Vector clock-based conflict resolution with causal ordering
- Multi-value state management with automatic merging
- Consensus protocols for critical state updates
- Event sourcing for state history and rollback capabilities
- Distributed state synchronization with eventual consistency
- Conflict-free replicated data types (CRDTs) support

This is Phase 3.2 of the Planet-Scale Fabric Roadmap.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Any

from loguru import logger

from ..core.statistics import (
    ConflictSummary,
    ConsensusInfo,
    ConsensusStatistics,
    ProposalSummary,
    ResolutionStatistics,
)
from ..datastructures.vector_clock import VectorClock as DataVectorClock
from .gossip import GossipMessage, GossipMessageType, GossipProtocol, VectorClock


class ConflictResolutionStrategy(Enum):
    """Conflict resolution strategies."""

    LAST_WRITE_WINS = "last_write_wins"
    FIRST_WRITE_WINS = "first_write_wins"
    MERGE_VALUES = "merge_values"
    CUSTOM_RESOLVER = "custom_resolver"
    VECTOR_CLOCK_DOMINANCE = "vector_clock_dominance"
    CONSENSUS_REQUIRED = "consensus_required"


class StateType(Enum):
    """Types of distributed state."""

    SIMPLE_VALUE = "simple_value"
    SET_STATE = "set_state"
    COUNTER_STATE = "counter_state"
    MAP_STATE = "map_state"
    SEQUENCE_STATE = "sequence_state"
    CUSTOM_CRDT = "custom_crdt"


class ConsensusStatus(Enum):
    """Consensus operation status."""

    PENDING = "pending"
    PROPOSED = "proposed"
    ACCEPTED = "accepted"
    COMMITTED = "committed"
    REJECTED = "rejected"
    TIMEOUT = "timeout"


@dataclass(slots=True)
class StateValue:
    """
    Versioned state value with metadata.

    Represents a single version of state with causality information
    and conflict resolution metadata.
    """

    value: Any
    vector_clock: VectorClock
    node_id: str
    timestamp: float = field(default_factory=time.time)
    version: int = 0
    state_type: StateType = StateType.SIMPLE_VALUE

    # Conflict resolution metadata
    merge_policy: ConflictResolutionStrategy = (
        ConflictResolutionStrategy.LAST_WRITE_WINS
    )
    content_hash: str = ""

    def __post_init__(self) -> None:
        """Initialize computed fields."""
        if not self.content_hash:
            self.content_hash = self._compute_content_hash()

    def _compute_content_hash(self) -> str:
        """Compute content hash for value comparison."""
        content = f"{self.value}:{self.vector_clock.to_dict()}:{self.timestamp}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def is_causally_after(self, other: StateValue) -> bool:
        """Check if this state value is causally after another."""
        comparison = self.vector_clock.compare(other.vector_clock)
        return comparison == "after"

    def is_causally_before(self, other: StateValue) -> bool:
        """Check if this state value is causally before another."""
        comparison = self.vector_clock.compare(other.vector_clock)
        return comparison == "before"

    def is_concurrent_with(self, other: StateValue) -> bool:
        """Check if this state value is concurrent with another."""
        comparison = self.vector_clock.compare(other.vector_clock)
        return comparison == "concurrent"

    def copy(self) -> StateValue:
        """Create a copy of this state value."""
        return StateValue(
            value=self.value,
            vector_clock=self.vector_clock.copy(),
            node_id=self.node_id,
            timestamp=self.timestamp,
            version=self.version,
            state_type=self.state_type,
            merge_policy=self.merge_policy,
            content_hash=self.content_hash,
        )


@dataclass(slots=True)
class StateConflict:
    """
    Represents a conflict between state values.

    Contains information about conflicting state values and
    provides context for conflict resolution.
    """

    conflict_id: str
    state_key: str
    conflicting_values: list[StateValue]
    detected_at: float = field(default_factory=time.time)
    resolution_strategy: ConflictResolutionStrategy = (
        ConflictResolutionStrategy.LAST_WRITE_WINS
    )
    resolved: bool = False
    resolved_value: StateValue | None = None
    resolved_at: float | None = None

    def get_conflict_summary(self) -> ConflictSummary:
        """Get summary of the conflict."""
        return ConflictSummary(
            conflict_id=self.conflict_id,
            state_key=self.state_key,
            num_conflicting_values=len(self.conflicting_values),
            value_sources=[v.node_id for v in self.conflicting_values],
            value_timestamps=[v.timestamp for v in self.conflicting_values],
            resolution_strategy=self.resolution_strategy.value,
            resolved=self.resolved,
            age_seconds=time.time() - self.detected_at,
        )


@dataclass(slots=True)
class ConsensusProposal:
    """
    Consensus proposal for distributed state changes.

    Represents a proposed state change that requires consensus
    from multiple nodes in the federation.
    """

    proposal_id: str
    proposer_node_id: str
    state_key: str
    proposed_value: StateValue
    proposal_timestamp: float = field(default_factory=time.time)

    # Consensus tracking
    required_votes: int = 1
    received_votes: dict[str, bool] = field(default_factory=dict)
    status: ConsensusStatus = ConsensusStatus.PENDING

    # Timeouts and deadlines
    proposal_timeout: float = 30.0
    consensus_deadline: float = field(default_factory=lambda: time.time() + 30.0)

    def add_vote(self, node_id: str, vote: bool) -> None:
        """Add a vote for this proposal."""
        self.received_votes[node_id] = vote

        # Update status based on votes
        if self.status == ConsensusStatus.PENDING:
            self.status = ConsensusStatus.PROPOSED

    def get_vote_count(self) -> tuple[int, int]:
        """Get (positive_votes, total_votes) count."""
        positive_votes = sum(1 for vote in self.received_votes.values() if vote)
        total_votes = len(self.received_votes)
        return positive_votes, total_votes

    def has_consensus(self) -> bool:
        """Check if proposal has reached consensus."""
        positive_votes, total_votes = self.get_vote_count()
        return (
            positive_votes >= self.required_votes and total_votes >= self.required_votes
        )

    def is_expired(self) -> bool:
        """Check if proposal has expired."""
        return time.time() > self.consensus_deadline

    def get_proposal_summary(self) -> ProposalSummary:
        """Get summary of the proposal."""
        positive_votes, total_votes = self.get_vote_count()
        return ProposalSummary(
            proposal_id=self.proposal_id,
            proposer=self.proposer_node_id,
            state_key=self.state_key,
            status=self.status.value,
            votes=f"{positive_votes}/{total_votes}",
            required_votes=self.required_votes,
            has_consensus=self.has_consensus(),
            is_expired=self.is_expired(),
            age_seconds=time.time() - self.proposal_timestamp,
        )


@dataclass(slots=True)
class ConflictResolver:
    """
    Advanced conflict resolution engine for distributed state.

    Implements sophisticated algorithms for resolving conflicts between
    concurrent state updates using vector clocks and custom strategies.
    """

    resolution_strategies: dict[ConflictResolutionStrategy, Callable] = field(
        init=False
    )
    custom_resolvers: dict[str, Callable] = field(default_factory=dict)
    resolution_stats: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    resolution_history: deque[Any] = field(default_factory=lambda: deque(maxlen=1000))
    consensus_manager: ConsensusManager | None = None
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize conflict resolver."""
        self.resolution_strategies = {
            ConflictResolutionStrategy.LAST_WRITE_WINS: self._resolve_last_write_wins,
            ConflictResolutionStrategy.FIRST_WRITE_WINS: self._resolve_first_write_wins,
            ConflictResolutionStrategy.MERGE_VALUES: self._resolve_merge_values,
            ConflictResolutionStrategy.VECTOR_CLOCK_DOMINANCE: self._resolve_vector_clock_dominance,
            ConflictResolutionStrategy.CONSENSUS_REQUIRED: self._resolve_consensus_required,
        }

        logger.info("Initialized ConflictResolver with built-in strategies")

    def register_custom_resolver(
        self, state_key: str, resolver: Callable[[list[StateValue]], StateValue]
    ) -> None:
        """Register a custom conflict resolver for a specific state key."""
        with self._lock:
            self.custom_resolvers[state_key] = resolver
            logger.debug(f"Registered custom resolver for state key: {state_key}")

    def resolve_conflict(self, conflict: StateConflict) -> StateValue:
        """
        Resolve a conflict between state values.

        Args:
            conflict: StateConflict containing conflicting values

        Returns:
            Resolved StateValue
        """
        start_time = time.time()

        with self._lock:
            # Check for custom resolver first
            if conflict.state_key in self.custom_resolvers:
                try:
                    resolved_value = self.custom_resolvers[conflict.state_key](
                        conflict.conflicting_values
                    )
                    strategy_used = ConflictResolutionStrategy.CUSTOM_RESOLVER
                except Exception as e:
                    logger.warning(
                        f"Custom resolver failed for {conflict.state_key}: {e}, falling back to default"
                    )
                    resolved_value = self._resolve_with_strategy(
                        conflict.conflicting_values,
                        conflict.resolution_strategy,
                        conflict.state_key,
                    )
                    strategy_used = conflict.resolution_strategy
            else:
                resolved_value = self._resolve_with_strategy(
                    conflict.conflicting_values,
                    conflict.resolution_strategy,
                    conflict.state_key,
                )
                strategy_used = conflict.resolution_strategy

            # Update conflict with resolution
            conflict.resolved = True
            conflict.resolved_value = resolved_value
            conflict.resolved_at = time.time()

            # Track statistics
            self.resolution_stats[strategy_used.value] += 1
            self.resolution_stats["total_resolutions"] += 1

            # Record resolution history
            resolution_record = {
                "conflict_id": conflict.conflict_id,
                "state_key": conflict.state_key,
                "strategy": strategy_used.value,
                "num_values": len(conflict.conflicting_values),
                "resolution_time_ms": (time.time() - start_time) * 1000,
                "timestamp": time.time(),
            }
            self.resolution_history.append(resolution_record)

            logger.debug(
                f"Resolved conflict {conflict.conflict_id} using {strategy_used.value}"
            )
            return resolved_value

    def _resolve_with_strategy(
        self,
        values: list[StateValue],
        strategy: ConflictResolutionStrategy,
        state_key: str | None = None,
    ) -> StateValue:
        """Resolve conflict using specified strategy."""
        if strategy in self.resolution_strategies:
            if strategy == ConflictResolutionStrategy.CONSENSUS_REQUIRED:
                return self._resolve_consensus_required(values, state_key)
            return self.resolution_strategies[strategy](values)
        else:
            logger.warning(
                f"Unknown strategy {strategy}, falling back to last_write_wins"
            )
            return self._resolve_last_write_wins(values)

    def _resolve_last_write_wins(self, values: list[StateValue]) -> StateValue:
        """Resolve conflict by selecting the value with the latest timestamp."""
        return max(values, key=lambda v: v.timestamp)

    def _resolve_first_write_wins(self, values: list[StateValue]) -> StateValue:
        """Resolve conflict by selecting the value with the earliest timestamp."""
        return min(values, key=lambda v: v.timestamp)

    def _resolve_vector_clock_dominance(self, values: list[StateValue]) -> StateValue:
        """Resolve conflict using vector clock dominance."""
        # Find values that are not dominated by any other value
        non_dominated = []

        for value in values:
            is_dominated = False
            for other in values:
                if value != other and value.is_causally_before(other):
                    is_dominated = True
                    break

            if not is_dominated:
                non_dominated.append(value)

        # If we have a clear winner, return it
        if len(non_dominated) == 1:
            return non_dominated[0]

        # If multiple non-dominated values, fall back to last write wins
        return self._resolve_last_write_wins(non_dominated)

    def _resolve_merge_values(self, values: list[StateValue]) -> StateValue:
        """Resolve conflict by merging values based on their type."""
        if not values:
            raise ValueError("Cannot merge empty values list")

        # Get the base value to build upon
        base_value = values[0]

        # Handle different state types
        if base_value.state_type == StateType.SET_STATE:
            return self._merge_set_values(values)
        elif base_value.state_type == StateType.COUNTER_STATE:
            return self._merge_counter_values(values)
        elif base_value.state_type == StateType.MAP_STATE:
            return self._merge_map_values(values)
        else:
            # For simple values, fall back to last write wins
            return self._resolve_last_write_wins(values)

    def _merge_set_values(self, values: list[StateValue]) -> StateValue:
        """Merge set-type state values."""
        merged_set: set[Any] = set()
        latest_clock = VectorClock()
        latest_timestamp: float = 0.0
        latest_node = values[0].node_id

        for value in values:
            if isinstance(value.value, set | list):
                merged_set.update(value.value)
            latest_clock.update(value.vector_clock)
            if value.timestamp > latest_timestamp:
                latest_timestamp = value.timestamp
                latest_node = value.node_id

        return StateValue(
            value=merged_set,
            vector_clock=latest_clock,
            node_id=latest_node,
            timestamp=latest_timestamp,
            state_type=StateType.SET_STATE,
        )

    def _merge_counter_values(self, values: list[StateValue]) -> StateValue:
        """Merge counter-type state values."""
        total_value: int | float = 0
        latest_clock = VectorClock()
        latest_timestamp: float = 0.0
        latest_node = values[0].node_id

        for value in values:
            if isinstance(value.value, int | float):
                total_value += value.value
            latest_clock.update(value.vector_clock)
            if value.timestamp > latest_timestamp:
                latest_timestamp = value.timestamp
                latest_node = value.node_id

        return StateValue(
            value=total_value,
            vector_clock=latest_clock,
            node_id=latest_node,
            timestamp=latest_timestamp,
            state_type=StateType.COUNTER_STATE,
        )

    def _merge_map_values(self, values: list[StateValue]) -> StateValue:
        """Merge map-type state values."""
        merged_map = {}
        latest_clock = VectorClock()
        latest_timestamp: float = 0.0
        latest_node = values[0].node_id

        for value in values:
            if isinstance(value.value, dict):
                merged_map.update(value.value)
            latest_clock.update(value.vector_clock)
            if value.timestamp > latest_timestamp:
                latest_timestamp = value.timestamp
                latest_node = value.node_id

        return StateValue(
            value=merged_map,
            vector_clock=latest_clock,
            node_id=latest_node,
            timestamp=latest_timestamp,
            state_type=StateType.MAP_STATE,
        )

    def _resolve_consensus_required(
        self, values: list[StateValue], state_key: str | None
    ) -> StateValue:
        """Mark conflict as requiring consensus (handled by ConsensusManager)."""
        candidate = self._select_consensus_candidate(values)

        if self.consensus_manager:
            try:
                loop = asyncio.get_running_loop()

                async def _propose() -> None:
                    await self.consensus_manager.propose_state_change(
                        state_key=state_key or candidate.node_id,
                        proposed_value=candidate,
                    )

                loop.create_task(_propose())
            except RuntimeError:
                logger.debug(
                    "Consensus manager available but no running loop to schedule proposal"
                )

        return candidate

    def _select_consensus_candidate(self, values: list[StateValue]) -> StateValue:
        """Select deterministic candidate to propose for consensus."""

        def _score(value: StateValue) -> tuple[float, str]:
            return (value.timestamp, value.node_id)

        return max(values, key=_score)

    def detect_conflicts(
        self, state_key: str, existing_value: StateValue | None, new_value: StateValue
    ) -> StateConflict | None:
        """
        Detect conflicts between existing and new state values.

        Args:
            state_key: Key of the state being updated
            existing_value: Current state value (if any)
            new_value: New state value being applied

        Returns:
            StateConflict if conflict detected, None otherwise
        """
        if existing_value is None:
            return None

        # Check if values are causally related
        if new_value.is_causally_after(existing_value):
            # New value supersedes existing - no conflict
            return None
        elif new_value.is_causally_before(existing_value):
            # New value is older - no conflict (ignore)
            return None
        elif new_value.is_concurrent_with(existing_value):
            # Concurrent updates - conflict detected
            conflict_id = f"conflict_{state_key}_{int(time.time())}_{hash((existing_value.content_hash, new_value.content_hash)) % 10000}"

            return StateConflict(
                conflict_id=conflict_id,
                state_key=state_key,
                conflicting_values=[existing_value, new_value],
                resolution_strategy=new_value.merge_policy,
            )

        return None

    def get_resolution_statistics(self) -> ResolutionStatistics:
        """Get conflict resolution statistics."""
        with self._lock:
            recent_resolutions = [
                r
                for r in self.resolution_history
                if time.time() - r["timestamp"] < 3600
            ]

            avg_resolution_time = sum(
                r["resolution_time_ms"] for r in recent_resolutions
            ) / max(1, len(recent_resolutions))

            return ResolutionStatistics(
                resolution_counts=dict(self.resolution_stats),
                recent_resolutions=len(recent_resolutions),
                avg_resolution_time_ms=avg_resolution_time,
                custom_resolvers=len(self.custom_resolvers),
                resolution_history_size=len(self.resolution_history),
            )


@dataclass(slots=True)
class ConsensusManager:
    """
    Distributed consensus manager for critical state updates.

    Implements consensus protocols for state changes that require
    agreement from multiple nodes in the federation.
    """

    node_id: str
    gossip_protocol: GossipProtocol | None = None
    message_broadcast_callback: Callable[[dict[str, Any]], Awaitable[None]] | None = (
        None
    )
    node_count_provider: Callable[[], int] | None = None
    default_consensus_threshold: float = 0.5
    proposal_timeout: float = 30.0
    active_proposals: dict[str, ConsensusProposal] = field(default_factory=dict)
    proposal_history: deque[Any] = field(default_factory=lambda: deque(maxlen=1000))
    known_nodes: set[str] = field(default_factory=set)
    node_weights: dict[str, float] = field(default_factory=dict)
    consensus_stats: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _background_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize consensus manager."""
        logger.debug(f"Initialized ConsensusManager for node {self.node_id}")

    async def start(self) -> None:
        """Start the consensus manager."""
        logger.debug(f"Starting ConsensusManager for node {self.node_id}")

        # Start background tasks
        cleanup_task = asyncio.create_task(self._proposal_cleanup_loop())
        self._background_tasks.add(cleanup_task)

        self.consensus_stats["manager_started"] = int(time.time())

    async def stop(self) -> None:
        """Stop the consensus manager."""
        logger.debug(f"Stopping ConsensusManager for node {self.node_id}")

        if self._shutdown_event.is_set() and not self._background_tasks:
            return

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()
        self.consensus_stats["manager_stopped"] = int(time.time())

    async def propose_state_change(
        self,
        state_key: str,
        proposed_value: StateValue,
        required_consensus: float | None = None,
    ) -> str:
        """
        Propose a state change that requires consensus.

        Args:
            state_key: Key of the state to change
            proposed_value: Proposed new state value
            required_consensus: Required consensus threshold (0.0-1.0)

        Returns:
            Proposal ID
        """
        proposal_id = f"proposal_{self.node_id}_{int(time.time())}_{hash((state_key, proposed_value.content_hash)) % 10000}"

        # Calculate required votes
        consensus_threshold = required_consensus or self.default_consensus_threshold
        total_nodes = max(1, len(self.known_nodes))
        if self.node_count_provider:
            try:
                total_nodes = max(total_nodes, int(self.node_count_provider()))
            except Exception as e:
                logger.warning(f"Failed to get node count for consensus: {e}")
        required_votes = max(1, int(total_nodes * consensus_threshold))

        # Create proposal
        proposal = ConsensusProposal(
            proposal_id=proposal_id,
            proposer_node_id=self.node_id,
            state_key=state_key,
            proposed_value=proposed_value,
            required_votes=required_votes,
            proposal_timeout=self.proposal_timeout,
        )

        with self._lock:
            self.active_proposals[proposal_id] = proposal

        # Broadcast proposal via gossip or callback if available
        if self.message_broadcast_callback:
            await self._broadcast_proposal_via_callback(proposal)
        elif self.gossip_protocol:
            await self._broadcast_proposal(proposal)

        self.consensus_stats["proposals_created"] += 1
        logger.debug(f"Created consensus proposal {proposal_id} for state {state_key}")

        return proposal_id

    async def vote_on_proposal(
        self, proposal_id: str, vote: bool, voter_node_id: str | None = None
    ) -> bool:
        """
        Cast a vote on a consensus proposal.

        Args:
            proposal_id: ID of the proposal to vote on
            vote: True for accept, False for reject
            voter_node_id: ID of the voting node (defaults to self.node_id)

        Returns:
            True if vote was recorded, False otherwise
        """
        voter_id = voter_node_id or self.node_id

        with self._lock:
            if proposal_id not in self.active_proposals:
                logger.warning(f"Vote received for unknown proposal: {proposal_id}")
                return False

            proposal = self.active_proposals[proposal_id]

            # Check if proposal is still active
            if proposal.is_expired() or proposal.status in [
                ConsensusStatus.COMMITTED,
                ConsensusStatus.REJECTED,
            ]:
                logger.warning(f"Vote received for inactive proposal: {proposal_id}")
                return False

            # Record vote
            proposal.add_vote(voter_id, vote)

            # Broadcast the vote to other nodes
            await self._broadcast_vote(proposal_id, vote, voter_id)

            # Check if consensus is reached
            if proposal.has_consensus():
                proposal.status = ConsensusStatus.ACCEPTED
                logger.info(f"Consensus reached for proposal {proposal_id}")

                # Apply the state change
                await self._apply_consensus_decision(proposal)

                # Move to committed status
                proposal.status = ConsensusStatus.COMMITTED
                self.consensus_stats["proposals_committed"] += 1

            self.consensus_stats["votes_received"] += 1
            return True

    async def handle_proposal_message(self, proposal_data: dict[str, Any]) -> None:
        """
        Handle a received consensus proposal message.

        Args:
            proposal_data: Serialized proposal data
        """
        try:
            proposal_id = proposal_data["proposal_id"]
            proposer_id = proposal_data["proposer_node_id"]
            state_key = proposal_data["state_key"]

            # Add proposer to known nodes
            self.known_nodes.add(proposer_id)

            # Reconstruct the proposal from the message and add to active proposals
            # Reconstruct the StateValue from the proposal data
            proposed_value_data = proposal_data["proposed_value"]
            vector_clock = DataVectorClock.from_dict(
                proposed_value_data["vector_clock"]
            )

            state_value = StateValue(
                value=proposed_value_data["value"],
                vector_clock=vector_clock,
                node_id=proposed_value_data["node_id"],
                timestamp=proposed_value_data["timestamp"],
            )

            # Create and store the proposal
            proposal = ConsensusProposal(
                proposal_id=proposal_id,
                proposer_node_id=proposer_id,
                state_key=state_key,
                proposed_value=state_value,
                required_votes=proposal_data["required_votes"],
                consensus_deadline=proposal_data["consensus_deadline"],
            )

            with self._lock:
                self.active_proposals[proposal_id] = proposal

            # NOTE: Do NOT auto-vote on received proposals - let external code decide when to vote
            # This allows tests and applications to control the voting process
            logger.debug(
                f"Stored proposal {proposal_id} in active_proposals for manual voting"
            )

            self.consensus_stats["proposals_evaluated"] += 1

        except Exception as e:
            logger.error(f"Error handling proposal message: {e}")

    async def handle_vote_message(self, vote_data: dict[str, Any]) -> None:
        """
        Handle a received consensus vote message.

        Args:
            vote_data: Serialized vote data
        """
        try:
            proposal_id = vote_data["proposal_id"]
            vote = vote_data["vote"]
            voter_id = vote_data["voter_id"]

            logger.debug(
                f"Handling vote message for proposal {proposal_id} from {voter_id}: {vote}"
            )

            # Add voter to known nodes
            self.known_nodes.add(voter_id)

            # Apply the vote to the proposal if it exists and is still active
            with self._lock:
                if proposal_id in self.active_proposals:
                    proposal = self.active_proposals[proposal_id]

                    # Only accept votes for active proposals
                    if not proposal.is_expired() and proposal.status not in [
                        ConsensusStatus.COMMITTED,
                        ConsensusStatus.REJECTED,
                    ]:
                        # Record the vote
                        proposal.add_vote(voter_id, vote)
                        logger.debug(
                            f"Recorded vote for proposal {proposal_id} from {voter_id}: {vote}"
                        )

                        # Check if consensus is reached after this vote
                        if proposal.has_consensus():
                            proposal.status = ConsensusStatus.ACCEPTED
                            logger.info(
                                f"Consensus reached for proposal {proposal_id} after receiving vote from {voter_id}"
                            )

                            # Apply the state change
                            await self._apply_consensus_decision(proposal)

                            # Move to committed status
                            proposal.status = ConsensusStatus.COMMITTED
                            self.consensus_stats["proposals_committed"] += 1

                        self.consensus_stats["votes_processed"] += 1
                    else:
                        logger.debug(
                            f"Ignoring vote for inactive/expired proposal {proposal_id}"
                        )
                else:
                    logger.debug(f"Received vote for unknown proposal: {proposal_id}")

        except Exception as e:
            logger.error(f"Error handling vote message: {e}")

    async def _evaluate_proposal(self, proposal_data: dict[str, Any]) -> bool:
        """
        Evaluate a consensus proposal and decide how to vote.

        Args:
            proposal_data: Proposal data to evaluate

        Returns:
            True to accept, False to reject
        """
        # Basic evaluation logic - can be customized
        try:
            state_key = proposal_data["state_key"]
            proposed_value = proposal_data["proposed_value"]

            # Simple validation - accept if proposal seems valid
            if state_key and proposed_value:
                return True

        except Exception as e:
            logger.warning(f"Error evaluating proposal: {e}")

        return False

    async def _apply_consensus_decision(self, proposal: ConsensusProposal) -> None:
        """
        Apply a consensus decision by updating state.

        Args:
            proposal: Consensus proposal that was accepted
        """
        # This would integrate with the actual state management system
        # For now, just log the decision
        logger.info(
            f"Applying consensus decision for {proposal.state_key}: {proposal.proposed_value.value}"
        )

    async def _broadcast_proposal(self, proposal: ConsensusProposal) -> None:
        """Broadcast a consensus proposal via gossip."""
        if not self.gossip_protocol:
            return

        proposal_message = GossipMessage(
            message_id=f"consensus_proposal_{proposal.proposal_id}",
            message_type=GossipMessageType.CONSENSUS_PROPOSAL,
            sender_id=self.node_id,
            payload={
                "proposal_id": proposal.proposal_id,
                "proposer_node_id": proposal.proposer_node_id,
                "state_key": proposal.state_key,
                "proposed_value": {
                    "value": proposal.proposed_value.value,
                    "vector_clock": proposal.proposed_value.vector_clock.to_dict(),
                    "node_id": proposal.proposed_value.node_id,
                    "timestamp": proposal.proposed_value.timestamp,
                },
                "required_votes": proposal.required_votes,
                "consensus_deadline": proposal.consensus_deadline,
            },
        )

        await self.gossip_protocol.add_message(proposal_message)

    async def _broadcast_proposal_via_callback(
        self, proposal: ConsensusProposal
    ) -> None:
        """Broadcast a consensus proposal via callback to server gossip system."""
        if not self.message_broadcast_callback:
            return

        proposal_data = {
            "proposal_id": proposal.proposal_id,
            "proposer_node_id": proposal.proposer_node_id,
            "state_key": proposal.state_key,
            "proposed_value": {
                "value": proposal.proposed_value.value,
                "vector_clock": proposal.proposed_value.vector_clock.to_dict(),
                "node_id": proposal.proposed_value.node_id,
                "timestamp": proposal.proposed_value.timestamp,
            },
            "required_votes": proposal.required_votes,
            "consensus_deadline": proposal.consensus_deadline,
        }

        await self.message_broadcast_callback(proposal_data)

    async def _broadcast_vote(
        self, proposal_id: str, vote: bool, voter_id: str
    ) -> None:
        """Broadcast a vote via gossip or callback."""
        if self.message_broadcast_callback:
            await self._broadcast_vote_via_callback(proposal_id, vote, voter_id)
        elif self.gossip_protocol:
            await self._broadcast_vote_via_gossip(proposal_id, vote, voter_id)

    async def _broadcast_vote_via_gossip(
        self, proposal_id: str, vote: bool, voter_id: str
    ) -> None:
        """Broadcast a vote via standalone gossip protocol."""
        if not self.gossip_protocol:
            return

        vote_message = GossipMessage(
            message_id=f"consensus_vote_{proposal_id}_{voter_id}_{int(time.time())}",
            message_type=GossipMessageType.CONSENSUS_VOTE,
            sender_id=self.node_id,
            payload={
                "proposal_id": proposal_id,
                "voter_node_id": voter_id,
                "vote": vote,
                "timestamp": time.time(),
            },
        )

        await self.gossip_protocol.add_message(vote_message)

    async def _broadcast_vote_via_callback(
        self, proposal_id: str, vote: bool, voter_id: str
    ) -> None:
        """Broadcast a vote via callback to server gossip system."""
        if not self.message_broadcast_callback:
            logger.warning(
                "No message broadcast callback available for vote broadcasting"
            )
            return

        # Create vote data dictionary for the callback
        vote_data = {
            "role": "consensus-vote",
            "proposal_id": proposal_id,
            "vote": vote,
            "voter_id": voter_id,
            "timestamp": time.time(),
        }

        try:
            # Use the callback to broadcast the vote data
            await self.message_broadcast_callback(vote_data)
        except Exception as e:
            logger.error(f"Error broadcasting vote via callback: {e}")

    async def _proposal_cleanup_loop(self) -> None:
        """Background task to clean up expired proposals."""
        while not self._shutdown_event.is_set():
            try:
                await self._cleanup_expired_proposals()
                await asyncio.sleep(10.0)  # Cleanup every 10 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in proposal cleanup loop: {e}")
                await asyncio.sleep(10.0)

    async def _cleanup_expired_proposals(self) -> None:
        """Clean up expired proposals."""
        expired_proposals = []

        with self._lock:
            for proposal_id, proposal in list(self.active_proposals.items()):
                if proposal.is_expired() and proposal.status not in [
                    ConsensusStatus.COMMITTED,
                    ConsensusStatus.REJECTED,
                ]:
                    proposal.status = ConsensusStatus.TIMEOUT
                    expired_proposals.append(proposal_id)

            # Move expired proposals to history
            for proposal_id in expired_proposals:
                proposal = self.active_proposals.pop(proposal_id)
                self.proposal_history.append(
                    {
                        "proposal_id": proposal_id,
                        "status": proposal.status.value,
                        "expired_at": time.time(),
                    }
                )
                self.consensus_stats["proposals_expired"] += 1

        if expired_proposals:
            logger.debug(f"Cleaned up {len(expired_proposals)} expired proposals")

    def get_proposal_status(self, proposal_id: str) -> ProposalSummary | None:
        """Get status of a specific proposal."""
        with self._lock:
            if proposal_id in self.active_proposals:
                return self.active_proposals[proposal_id].get_proposal_summary()
        return None

    def get_consensus_statistics(self) -> ConsensusStatistics:
        """Get comprehensive consensus statistics."""
        with self._lock:
            consensus_info = ConsensusInfo(
                node_id=self.node_id,
                known_nodes=len(self.known_nodes),
                default_threshold=self.default_consensus_threshold,
                proposal_timeout=self.proposal_timeout,
            )

            active_proposal_summaries = [
                proposal.get_proposal_summary()
                for proposal in self.active_proposals.values()
            ]

            return ConsensusStatistics(
                consensus_info=consensus_info,
                consensus_stats=dict(self.consensus_stats),
                active_proposals=len(self.active_proposals),
                proposal_history_size=len(self.proposal_history),
                active_proposal_summaries=active_proposal_summaries,
            )


# Note: CONSENSUS_PROPOSAL and CONSENSUS_VOTE are now defined in GossipMessageType enum
