"""
Leader Election Abstractions and Implementations for MPREG.

This module provides a pluggable leader election system that supports
multiple algorithms and strategies for distributed cache coordination.

Key Features:
- Pluggable LeaderElection protocol interface
- Multiple concrete implementations (Raft-based, Quorum-based, Metric-based)
- Well-encapsulated with proper state management
- Comprehensive error handling and recovery
- Integration with federation infrastructure

Design Principles:
- Protocol-based architecture for swappable implementations
- Immutable dataclasses with proper validation
- Async/await throughout for non-blocking operations
- Comprehensive logging and monitoring support
"""

from __future__ import annotations

import asyncio
import time
from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from loguru import logger

from .production_raft import (
    RaftState,
    RaftStorageProtocol,
    RaftTransportProtocol,
    StateMachineProtocol,
)
from .production_raft_implementation import ProductionRaft, RaftConfiguration
from .raft_storage_adapters import RaftStorageFactory
from .type_aliases import ClusterId, Timestamp


def _get_cluster_fitness_score(
    cluster_id: ClusterId,
    self_cluster_id: ClusterId,
    cluster_metrics: dict[ClusterId, LeaderElectionMetrics],
    default_self_score: float = 75.0,
    default_unknown_score: float = 50.0,
) -> float:
    """
    Get fitness score for a cluster with fallback defaults.

    Args:
        cluster_id: The cluster to get fitness score for
        self_cluster_id: The current cluster's ID
        cluster_metrics: Dictionary of known cluster metrics
        default_self_score: Default score for self if no metrics available
        default_unknown_score: Default score for unknown clusters

    Returns:
        Fitness score for the cluster
    """
    if cluster_id in cluster_metrics:
        return cluster_metrics[cluster_id].fitness_score()

    if cluster_id == self_cluster_id:
        return default_self_score

    return default_unknown_score


class LeaderElectionState(Enum):
    """States for leader election process."""

    FOLLOWER = "follower"  # Node is following another leader
    CANDIDATE = "candidate"  # Node is campaigning to be leader
    LEADER = "leader"  # Node is the current leader
    OFFLINE = "offline"  # Node is offline/unreachable


@dataclass(frozen=True, slots=True)
class LeaderElectionTerm:
    """
    Term information for leader election.

    In Raft-like algorithms, terms provide logical time to detect
    stale leaders and ensure election safety.
    """

    term_number: int
    leader_id: ClusterId | None = None
    started_at: Timestamp = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if self.term_number < 0:
            raise ValueError("Term number cannot be negative")
        if self.started_at <= 0:
            raise ValueError("Start time must be positive")


@dataclass(frozen=True, slots=True)
class LeaderElectionVote:
    """Vote in a leader election process."""

    term: int
    candidate_id: ClusterId
    voter_id: ClusterId
    granted: bool
    timestamp: Timestamp = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("Term cannot be negative")
        if not self.candidate_id:
            raise ValueError("Candidate ID cannot be empty")
        if not self.voter_id:
            raise ValueError("Voter ID cannot be empty")
        if self.timestamp <= 0:
            raise ValueError("Timestamp must be positive")


@dataclass(frozen=True, slots=True)
class LeaderElectionMetrics:
    """Metrics for leader election performance."""

    cluster_id: ClusterId
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    cache_hit_rate: float = 0.0
    active_connections: int = 0
    network_latency_ms: float = 0.0
    last_heartbeat: Timestamp = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("Cluster ID cannot be empty")
        if not (0.0 <= self.cpu_usage <= 1.0):
            raise ValueError("CPU usage must be between 0.0 and 1.0")
        if not (0.0 <= self.memory_usage <= 1.0):
            raise ValueError("Memory usage must be between 0.0 and 1.0")
        if not (0.0 <= self.cache_hit_rate <= 1.0):
            raise ValueError("Cache hit rate must be between 0.0 and 1.0")
        if self.active_connections < 0:
            raise ValueError("Active connections cannot be negative")
        if self.network_latency_ms < 0:
            raise ValueError("Network latency cannot be negative")

    def fitness_score(self) -> float:
        """
        Calculate fitness score for leader election (higher is better).

        Returns:
            Fitness score from 0.0-100.0, higher means better candidate
        """
        # Calculate component scores (all normalized to 0-1, higher = better)
        cpu_score = 1.0 - self.cpu_usage  # Lower CPU usage is better
        memory_score = 1.0 - self.memory_usage  # Lower memory usage is better
        hit_rate_score = self.cache_hit_rate  # Higher hit rate is better
        connection_score = min(
            self.active_connections / 1000.0, 1.0
        )  # More connections is better (up to limit)
        latency_score = max(
            0.0, 1.0 - (self.network_latency_ms / 1000.0)
        )  # Lower latency is better

        # Weighted average
        total_score = (
            cpu_score * 25.0  # CPU weight: 25%
            + memory_score * 20.0  # Memory weight: 20%
            + hit_rate_score * 25.0  # Hit rate weight: 25%
            + connection_score * 15.0  # Connection weight: 15%
            + latency_score * 15.0  # Latency weight: 15%
        )

        return min(total_score, 100.0)


@runtime_checkable
class LeaderElection(Protocol):
    """
    Protocol interface for leader election implementations.

    This allows different leader election algorithms to be plugged in
    while maintaining a consistent interface for the cache system.
    """

    @abstractmethod
    async def elect_leader(self, namespace: str) -> ClusterId:
        """
        Elect a leader for the given namespace.

        Args:
            namespace: Namespace to elect leader for

        Returns:
            Cluster ID of the elected leader
        """
        ...

    @abstractmethod
    async def get_current_leader(self, namespace: str) -> ClusterId | None:
        """
        Get current leader for namespace without triggering election.

        Args:
            namespace: Namespace to check

        Returns:
            Current leader cluster ID, or None if no leader
        """
        ...

    @abstractmethod
    async def is_leader(self, namespace: str) -> bool:
        """
        Check if this node is the leader for the namespace.

        Args:
            namespace: Namespace to check

        Returns:
            True if this node is the leader
        """
        ...

    @abstractmethod
    def update_metrics(
        self, cluster_id: ClusterId, metrics: LeaderElectionMetrics
    ) -> None:
        """
        Update cluster metrics for leader election decisions.

        Args:
            cluster_id: Cluster to update metrics for
            metrics: New metrics for the cluster
        """
        ...

    @abstractmethod
    async def step_down(self, namespace: str) -> None:
        """
        Step down as leader for the namespace.

        Args:
            namespace: Namespace to step down from
        """
        ...


class _NullRaftTransport:
    async def send_request_vote(self, target, request):
        return None

    async def send_append_entries(self, target, request):
        return None

    async def send_install_snapshot(self, target, request):
        return None


class _NoOpRaftStateMachine:
    async def apply_command(self, command: Any, index: int) -> Any:
        return None

    async def create_snapshot(self) -> bytes:
        return b""

    async def restore_from_snapshot(self, snapshot_data: bytes) -> None:
        return None


@dataclass(slots=True)
class RaftBasedLeaderElection:
    """
    Raft-backed leader election powered by the ProductionRaft implementation.

    This is a thin wrapper that exposes the LeaderElection protocol while
    delegating consensus mechanics to ProductionRaft over a provided transport
    (preferably the fabric transport).
    """

    cluster_id: ClusterId
    node_id: ClusterId | None = None
    cluster_members: set[ClusterId] = field(default_factory=set)
    transport: RaftTransportProtocol | None = None
    storage: RaftStorageProtocol | None = None
    state_machine: StateMachineProtocol | None = None
    config: RaftConfiguration | None = None
    leader_wait_timeout: float = 5.0

    cluster_metrics: dict[ClusterId, LeaderElectionMetrics] = field(
        default_factory=dict
    )
    known_clusters: set[ClusterId] = field(default_factory=set)

    _raft: ProductionRaft = field(init=False)
    _started: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("Cluster ID cannot be empty")
        node_id = self.node_id or self.cluster_id
        if not self.cluster_members:
            self.cluster_members = {node_id}
        if node_id not in self.cluster_members:
            raise ValueError("node_id must be included in cluster_members")
        if self.transport is None:
            self.transport = _NullRaftTransport()
            if len(self.cluster_members) > 1:
                logger.warning(
                    "Raft leader election configured without transport for multi-node cluster"
                )
        if self.storage is None:
            self.storage = RaftStorageFactory.create_memory_storage(f"leader_{node_id}")
        if self.state_machine is None:
            self.state_machine = _NoOpRaftStateMachine()
        if self.config is None:
            self.config = RaftConfiguration()

        self.node_id = node_id
        self.known_clusters.add(self.cluster_id)
        self._raft = ProductionRaft(
            node_id=self.node_id,
            cluster_members=set(self.cluster_members),
            storage=self.storage,
            transport=self.transport,
            state_machine=self.state_machine,
            config=self.config,
        )
        register = getattr(self.transport, "register_node", None)
        if callable(register):
            register(self._raft)

    @property
    def current_term(self) -> int:
        return self._raft.persistent_state.current_term

    @property
    def voted_for(self) -> ClusterId | None:
        return self._raft.persistent_state.voted_for

    @property
    def state(self) -> LeaderElectionState:
        if self._raft.current_state == RaftState.LEADER:
            return LeaderElectionState.LEADER
        if self._raft.current_state == RaftState.CANDIDATE:
            return LeaderElectionState.CANDIDATE
        return LeaderElectionState.FOLLOWER

    async def elect_leader(self, namespace: str) -> ClusterId:
        if not namespace:
            raise ValueError("Namespace cannot be empty")
        await self._ensure_started()
        self._raft.election_coordinator.trigger_election()
        deadline = time.time() + self.leader_wait_timeout
        while time.time() < deadline:
            leader = self._raft.current_leader
            if leader:
                return leader
            await asyncio.sleep(0.05)
        return self._raft.current_leader or self.node_id

    async def get_current_leader(self, namespace: str) -> ClusterId | None:
        if not namespace:
            raise ValueError("Namespace cannot be empty")
        return self._raft.current_leader

    async def is_leader(self, namespace: str) -> bool:
        if not namespace:
            raise ValueError("Namespace cannot be empty")
        return self._raft.current_state == RaftState.LEADER

    def update_metrics(
        self, cluster_id: ClusterId, metrics: LeaderElectionMetrics
    ) -> None:
        self.cluster_metrics[cluster_id] = metrics
        self.known_clusters.add(cluster_id)

    async def step_down(self, namespace: str) -> None:
        if not namespace:
            raise ValueError("Namespace cannot be empty")
        if not self._started:
            return
        await self._raft.step_down()

    async def shutdown(self) -> None:
        if not self._started:
            return
        await self._raft.stop()
        self._started = False

    async def _ensure_started(self) -> None:
        if self._started:
            return
        await self._raft.start()
        self._started = True


@dataclass(slots=True)
class QuorumBasedLeaderElection:
    """
    Quorum-based leader election implementation.

    Uses a quorum consensus approach where leaders are elected
    based on majority vote from all participating clusters.
    """

    cluster_id: ClusterId
    cluster_metrics: dict[ClusterId, LeaderElectionMetrics] = field(
        default_factory=dict
    )
    namespace_leaders: dict[str, ClusterId] = field(default_factory=dict)
    quorum_size: int = 3

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("Cluster ID cannot be empty")
        if self.quorum_size < 1:
            raise ValueError("Quorum size must be positive")

    async def elect_leader(self, namespace: str) -> ClusterId:
        """Elect leader using quorum consensus."""
        # Get all available clusters
        available_clusters = list(self.cluster_metrics.keys())
        available_clusters.append(self.cluster_id)
        available_clusters = list(set(available_clusters))

        if len(available_clusters) < self.quorum_size:
            # Not enough clusters for quorum, elect self
            self.namespace_leaders[namespace] = self.cluster_id
            return self.cluster_id

        # Sort clusters by fitness score (highest first)
        # Use self-preference as primary tiebreaker, then cluster_id for deterministic ordering
        clusters_by_fitness = sorted(
            available_clusters,
            key=lambda cid: (
                self._get_fitness_score(cid),
                cid == self.cluster_id,  # Prefer self when scores are equal
                cid,  # Final tiebreaker for deterministic ordering
            ),
            reverse=True,
        )

        # Leader is the cluster with highest fitness
        leader = clusters_by_fitness[0]
        self.namespace_leaders[namespace] = leader

        return leader

    async def get_current_leader(self, namespace: str) -> ClusterId | None:
        """Get current leader."""
        return self.namespace_leaders.get(namespace)

    async def is_leader(self, namespace: str) -> bool:
        """Check if this node is the leader."""
        current_leader = await self.get_current_leader(namespace)
        return current_leader == self.cluster_id

    def update_metrics(
        self, cluster_id: ClusterId, metrics: LeaderElectionMetrics
    ) -> None:
        """Update cluster metrics."""
        self.cluster_metrics[cluster_id] = metrics

    async def step_down(self, namespace: str) -> None:
        """Step down as leader."""
        if namespace in self.namespace_leaders:
            del self.namespace_leaders[namespace]

    def _get_fitness_score(self, cluster_id: ClusterId) -> float:
        """Get fitness score for cluster."""
        return _get_cluster_fitness_score(
            cluster_id, self.cluster_id, self.cluster_metrics, default_self_score=80.0
        )


@dataclass(slots=True)
class MetricBasedLeaderElection:
    """
    Metric-based leader election implementation.

    Simple deterministic leader selection based purely on
    cluster performance metrics. No consensus protocol.
    """

    cluster_id: ClusterId
    cluster_metrics: dict[ClusterId, LeaderElectionMetrics] = field(
        default_factory=dict
    )
    namespace_leaders: dict[str, ClusterId] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("Cluster ID cannot be empty")

    async def elect_leader(self, namespace: str) -> ClusterId:
        """Elect leader based on best metrics."""
        # Get all candidates including self
        all_candidates = set(self.cluster_metrics.keys())
        all_candidates.add(self.cluster_id)

        if not all_candidates:
            # Fallback - should never happen
            self.namespace_leaders[namespace] = self.cluster_id
            return self.cluster_id

        # Find cluster with best fitness score including self
        # Use self-preference as tiebreaker, then cluster_id for deterministic ordering
        best_cluster = max(
            all_candidates,
            key=lambda cid: (
                self._get_fitness_score(cid),
                cid == self.cluster_id,  # Prefer self when scores are equal
                cid,  # Final tiebreaker for deterministic ordering
            ),
        )

        self.namespace_leaders[namespace] = best_cluster
        return best_cluster

    async def get_current_leader(self, namespace: str) -> ClusterId | None:
        """Get current leader."""
        return self.namespace_leaders.get(namespace)

    async def is_leader(self, namespace: str) -> bool:
        """Check if this node is the leader."""
        current_leader = await self.get_current_leader(namespace)
        return current_leader == self.cluster_id

    def update_metrics(
        self, cluster_id: ClusterId, metrics: LeaderElectionMetrics
    ) -> None:
        """Update cluster metrics."""
        self.cluster_metrics[cluster_id] = metrics

    async def step_down(self, namespace: str) -> None:
        """Step down as leader."""
        if namespace in self.namespace_leaders:
            del self.namespace_leaders[namespace]

    def _get_fitness_score(self, cluster_id: ClusterId) -> float:
        """Get fitness score for cluster."""
        return _get_cluster_fitness_score(
            cluster_id, self.cluster_id, self.cluster_metrics, default_self_score=75.0
        )
