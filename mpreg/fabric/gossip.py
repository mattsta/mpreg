"""
Fabric Gossip Protocol for MPREG.

This module implements epidemic information propagation for distributed coordination
in the unified fabric control plane. The gossip protocol enables efficient
dissemination of state updates, membership changes, and configuration updates
across thousands of nodes with eventual consistency guarantees.

Key Features:
- Epidemic information dissemination with O(log N) convergence
- Anti-entropy mechanisms to prevent message loops
- Bandwidth-efficient filtering and aggregation
- Versioned messages with vector clocks
- Configurable gossip strategies and scheduling
- Failure detection and membership management

This is Phase 3.1 of the Planet-Scale Fabric Roadmap.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import math
import random
import time
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from threading import RLock
from typing import TYPE_CHECKING, Any

from loguru import logger

from mpreg.fabric.catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from mpreg.fabric.link_state import LinkStateUpdate
from mpreg.fabric.route_control import RouteAnnouncement, RouteWithdrawal
from mpreg.fabric.route_keys import RouteKeyAnnouncement

from ..core.statistics import (
    ConnectionInfo,
    GossipConvergenceTracking,
    GossipProtocolStatistics,
    LoadMetrics,
    NodeCapabilities,
)
from ..datastructures.type_aliases import NodeId
from ..datastructures.vector_clock import VectorClock
from .gossip_transport import GossipTransport

if TYPE_CHECKING:
    from mpreg.fabric.hub_registry import HubRegistry
    from mpreg.fabric.link_state import LinkStateProcessor
    from mpreg.fabric.route_announcer import RouteAnnouncementProcessor

    from .consensus import ConsensusManager


# New dataclasses for type safety
@dataclass(slots=True)
class StateUpdatePayload:
    """Payload for state update messages."""

    key: str
    value: Any
    version: int = 0
    timestamp: float = field(default_factory=time.time)
    source_node: str = ""
    ttl: int = 300


@dataclass(slots=True)
class MembershipUpdatePayload:
    """Payload for membership update messages."""

    node_id: NodeId
    node_info: dict[str, Any]  # Keep as dict for flexibility
    event_type: str = "update"
    timestamp: float = field(default_factory=time.time)
    incarnation: int = 0


@dataclass(slots=True)
class ConfigUpdatePayload:
    """Payload for configuration update messages."""

    config_key: str
    config_value: Any
    version: int = 0
    timestamp: float = field(default_factory=time.time)
    source_node: str = ""
    scope: str = "global"  # global, regional, local


@dataclass(slots=True)
class HeartbeatPayload:
    """Payload for heartbeat messages."""

    sender_id: str
    timestamp: float = field(default_factory=time.time)
    load_average: float = 0.0
    memory_usage: float = 0.0
    cpu_usage: float = 0.0
    connection_count: int = 0
    health_status: str = "healthy"


@dataclass(slots=True)
class FilterStatistics:
    """Filter statistics information."""

    seen_messages: int
    recent_digests: int
    filtered_count: int
    duplicate_count: int
    expired_count: int
    filter_ratio: float


@dataclass(slots=True)
class SchedulerStatistics:
    """Scheduler statistics information."""

    gossip_interval: float
    adaptive_interval: float
    fanout: int
    strategy: str
    gossip_cycles: int
    pending_messages: int
    avg_bandwidth_usage: float
    last_gossip_time: float


@dataclass(slots=True)
class NodeMetadata:
    """Metadata about a node."""

    node_id: str
    region: str = ""
    distance: float = 0.0
    last_heartbeat: float = 0.0
    capabilities: NodeCapabilities = field(default_factory=NodeCapabilities)
    health_status: str = "unknown"
    load_metrics: LoadMetrics = field(default_factory=LoadMetrics)
    connection_info: ConnectionInfo = field(default_factory=ConnectionInfo)


@dataclass(slots=True)
class ConvergenceStatus:
    """Convergence status information."""

    node_id: str
    known_nodes: int
    pending_messages: int
    recent_messages: int
    state_cache_size: int
    vector_clock: dict[str, int]
    last_gossip_time: float
    gossip_cycles: int


@dataclass(slots=True)
class ProtocolInfo:
    """Protocol configuration information."""

    node_id: str
    gossip_strategy: str
    gossip_interval: float
    fanout: int


@dataclass(slots=True)
class StateInfo:
    """State information."""

    known_nodes: int
    pending_messages: int
    recent_messages: int
    state_cache_size: int


@dataclass(slots=True)
class ComprehensiveStatistics:
    """Comprehensive protocol statistics."""

    protocol_info: ProtocolInfo
    protocol_stats: GossipProtocolStatistics
    scheduler_stats: SchedulerStatistics
    filter_stats: FilterStatistics
    state_info: StateInfo
    convergence_status: ConvergenceStatus


class GossipMessageType(Enum):
    """Types of gossip messages."""

    STATE_UPDATE = "state_update"
    MEMBERSHIP_UPDATE = "membership_update"
    CONFIG_UPDATE = "config_update"
    CATALOG_UPDATE = "catalog_update"
    ROUTE_ADVERTISEMENT = "route_advertisement"
    ROUTE_WITHDRAWAL = "route_withdrawal"
    LINK_STATE_UPDATE = "link_state_update"
    ROUTE_KEY_ANNOUNCEMENT = "route_key_announcement"
    HEARTBEAT = "heartbeat"
    ANTI_ENTROPY = "anti_entropy"
    RUMOR = "rumor"
    CONSENSUS_PROPOSAL = "consensus_proposal"
    CONSENSUS_VOTE = "consensus_vote"
    MEMBERSHIP_PROBE = "membership_probe"
    MEMBERSHIP_ACK = "membership_ack"
    MEMBERSHIP_INDIRECT_PROBE = "membership_indirect_probe"


class GossipStrategy(Enum):
    """Gossip propagation strategies."""

    RANDOM = "random"
    PROXIMITY = "proximity"
    TOPOLOGY_AWARE = "topology_aware"
    HYBRID = "hybrid"


@dataclass(slots=True)
class GossipMessage:
    """
    Versioned gossip message with TTL and anti-entropy information.

    Represents a single unit of information propagated through the gossip protocol.
    """

    message_id: str
    message_type: GossipMessageType
    sender_id: str
    payload: (
        StateUpdatePayload
        | MembershipUpdatePayload
        | ConfigUpdatePayload
        | HeartbeatPayload
        | RoutingCatalogDelta
        | RouteAnnouncement
        | RouteWithdrawal
        | LinkStateUpdate
        | RouteKeyAnnouncement
        | dict[str, Any]
    )

    # Versioning and causality
    vector_clock: VectorClock = field(default_factory=VectorClock.empty)
    sequence_number: int = 0

    # TTL and propagation control
    ttl: int = 10
    hop_count: int = 0
    max_hops: int = 5

    # Anti-entropy information
    digest: str = ""
    checksum: str = ""
    created_at: float = field(default_factory=time.time)
    expires_at: float = field(default_factory=lambda: time.time() + 300)  # 5 minutes

    # Propagation metadata
    propagation_path: list[str] = field(default_factory=list)
    seen_by: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        """Initialize computed fields."""
        if not self.digest:
            self.digest = self._compute_digest()
        if not self.checksum:
            self.checksum = self._compute_checksum()

    def _compute_digest(self) -> str:
        """Compute digest for anti-entropy."""
        content = f"{self.message_type.value}:{self.sender_id}:{self.sequence_number}"
        return hashlib.md5(content.encode()).hexdigest()[:8]

    def _compute_checksum(self) -> str:
        """Compute checksum for integrity."""
        content = f"{self.payload}:{self.vector_clock.to_dict()}:{self.sequence_number}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def is_expired(self) -> bool:
        """Check if message is expired."""
        return time.time() > self.expires_at

    def can_propagate(self) -> bool:
        """Check if message can still be propagated."""
        return not self.is_expired() and self.hop_count < self.max_hops and self.ttl > 0

    def prepare_for_propagation(self, sender_id: str) -> GossipMessage:
        """Prepare message for propagation to next hop."""
        new_message = GossipMessage(
            message_id=self.message_id,
            message_type=self.message_type,
            sender_id=sender_id,  # Update sender
            payload=self.payload,
            vector_clock=self.vector_clock.copy(),
            sequence_number=self.sequence_number,
            ttl=self.ttl - 1,
            hop_count=self.hop_count + 1,
            max_hops=self.max_hops,
            digest=self.digest,
            checksum=self.checksum,
            created_at=self.created_at,
            expires_at=self.expires_at,
            propagation_path=self.propagation_path + [sender_id],
            seen_by=self.seen_by.copy(),
        )

        # Update vector clock
        new_message.vector_clock = new_message.vector_clock.increment(sender_id)
        return new_message

    def add_seen_by(self, node_id: str) -> None:
        """Add node to seen_by set."""
        self.seen_by.add(node_id)

    def has_been_seen_by(self, node_id: str) -> bool:
        """Check if message has been seen by a node."""
        return node_id in self.seen_by

    def get_message_age(self) -> float:
        """Get age of message in seconds."""
        return time.time() - self.created_at

    def to_dict(self) -> dict[str, Any]:
        """Serialize gossip message for transport."""
        payload = self.payload
        if isinstance(payload, RoutingCatalogDelta):
            payload_data: Any = payload.to_dict()
        elif isinstance(payload, (RouteAnnouncement, RouteWithdrawal)):
            payload_data = payload.to_dict()
        elif is_dataclass(payload):
            payload_data = asdict(payload)
        else:
            payload_data = payload

        return {
            "message_id": self.message_id,
            "message_type": self.message_type.value,
            "sender_id": self.sender_id,
            "payload": payload_data,
            "vector_clock": self.vector_clock.to_dict(),
            "sequence_number": self.sequence_number,
            "ttl": self.ttl,
            "hop_count": self.hop_count,
            "max_hops": self.max_hops,
            "digest": self.digest,
            "checksum": self.checksum,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "propagation_path": list(self.propagation_path),
            "seen_by": sorted(self.seen_by),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> GossipMessage:
        """Deserialize a gossip message from transport payload."""
        message_type = GossipMessageType(payload["message_type"])
        vector_clock = VectorClock.from_dict(payload.get("vector_clock", {}))
        raw_payload = payload.get("payload")

        if message_type == GossipMessageType.CATALOG_UPDATE and isinstance(
            raw_payload, dict
        ):
            parsed_payload: Any = RoutingCatalogDelta.from_dict(raw_payload)
        elif message_type == GossipMessageType.ROUTE_ADVERTISEMENT and isinstance(
            raw_payload, dict
        ):
            parsed_payload = RouteAnnouncement.from_dict(raw_payload)
        elif message_type == GossipMessageType.ROUTE_WITHDRAWAL and isinstance(
            raw_payload, dict
        ):
            parsed_payload = RouteWithdrawal.from_dict(raw_payload)
        elif message_type == GossipMessageType.LINK_STATE_UPDATE and isinstance(
            raw_payload, dict
        ):
            parsed_payload = LinkStateUpdate.from_dict(raw_payload)
        elif message_type == GossipMessageType.ROUTE_KEY_ANNOUNCEMENT and isinstance(
            raw_payload, dict
        ):
            parsed_payload = RouteKeyAnnouncement.from_dict(raw_payload)
        elif message_type == GossipMessageType.STATE_UPDATE and isinstance(
            raw_payload, dict
        ):
            parsed_payload = StateUpdatePayload(**raw_payload)
        elif message_type == GossipMessageType.MEMBERSHIP_UPDATE and isinstance(
            raw_payload, dict
        ):
            parsed_payload = MembershipUpdatePayload(**raw_payload)
        elif message_type == GossipMessageType.CONFIG_UPDATE and isinstance(
            raw_payload, dict
        ):
            parsed_payload = ConfigUpdatePayload(**raw_payload)
        elif message_type == GossipMessageType.HEARTBEAT and isinstance(
            raw_payload, dict
        ):
            parsed_payload = HeartbeatPayload(**raw_payload)
        else:
            parsed_payload = raw_payload

        return cls(
            message_id=str(payload["message_id"]),
            message_type=message_type,
            sender_id=str(payload["sender_id"]),
            payload=parsed_payload,
            vector_clock=vector_clock,
            sequence_number=int(payload.get("sequence_number", 0)),
            ttl=int(payload.get("ttl", 10)),
            hop_count=int(payload.get("hop_count", 0)),
            max_hops=int(payload.get("max_hops", 5)),
            digest=str(payload.get("digest", "")),
            checksum=str(payload.get("checksum", "")),
            created_at=float(payload.get("created_at", time.time())),
            expires_at=float(payload.get("expires_at", time.time() + 300)),
            propagation_path=list(payload.get("propagation_path", [])),
            seen_by=set(payload.get("seen_by", [])),
        )


@dataclass(slots=True)
class GossipFilter:
    """
    Gossip filter to prevent message loops and reduce bandwidth.

    Implements various filtering strategies to prevent redundant
    message propagation and reduce network overhead.
    """

    # Message tracking
    seen_messages: set[str] = field(default_factory=set)
    recent_digests: deque[Any] = field(default_factory=lambda: deque(maxlen=1000))

    # Filter configuration
    max_seen_messages: int = 10000
    digest_cache_size: int = 1000
    duplicate_threshold: int = 3

    # Statistics
    filtered_count: int = 0
    duplicate_count: int = 0
    expired_count: int = 0

    def should_propagate(self, message: GossipMessage, node_id: str) -> bool:
        """
        Determine if message should be propagated.

        Args:
            message: Gossip message to check
            node_id: ID of the node making the decision

        Returns:
            True if message should be propagated
        """
        # Check if message can still propagate
        if not message.can_propagate():
            self.expired_count += 1
            return False

        # Check if we've seen this message before
        if message.message_id in self.seen_messages:
            self.duplicate_count += 1
            return False

        # Check if message has been seen by this node
        if message.has_been_seen_by(node_id):
            self.duplicate_count += 1
            return False

        # Check if we've seen this digest recently
        if message.digest in self.recent_digests:
            self.duplicate_count += 1
            return False

        # Check if message has looped back
        if node_id in message.propagation_path:
            self.duplicate_count += 1
            return False

        # Message should be propagated
        self._record_message(message)
        return True

    def _record_message(self, message: GossipMessage) -> None:
        """Record message to prevent future duplicates."""
        self.seen_messages.add(message.message_id)
        self.recent_digests.append(message.digest)

        # Cleanup old messages
        if len(self.seen_messages) > self.max_seen_messages:
            # Remove oldest 10% of messages
            to_remove = list(self.seen_messages)[: self.max_seen_messages // 10]
            for msg_id in to_remove:
                self.seen_messages.discard(msg_id)

    def get_filter_statistics(self) -> FilterStatistics:
        """Get filter statistics."""
        return FilterStatistics(
            seen_messages=len(self.seen_messages),
            recent_digests=len(self.recent_digests),
            filtered_count=self.filtered_count,
            duplicate_count=self.duplicate_count,
            expired_count=self.expired_count,
            filter_ratio=self.filtered_count
            / max(1, self.filtered_count + self.duplicate_count),
        )


@dataclass(slots=True)
class GossipScheduler:
    """
    Gossip scheduler for periodic and event-driven gossip cycles.

    Manages the timing and frequency of gossip operations to balance
    convergence speed with bandwidth efficiency.
    """

    gossip_interval: float = 1.0
    fanout: int = 3
    strategy: GossipStrategy = GossipStrategy.RANDOM
    last_gossip_time: float = 0.0
    gossip_cycles: int = 0
    pending_messages: int = 0
    convergence_times: list[float] = field(default_factory=list)
    bandwidth_usage: list[float] = field(default_factory=list)

    adaptive_interval: float = field(init=False)
    min_interval: float = field(init=False)
    max_interval: float = field(init=False)

    def __post_init__(self) -> None:
        """Initialize computed intervals."""
        self.adaptive_interval = self.gossip_interval
        self.min_interval = self.gossip_interval * 0.1
        self.max_interval = self.gossip_interval * 10.0

    def should_gossip(self) -> bool:
        """Determine if it's time to gossip."""
        current_time = time.time()

        # Check if enough time has passed
        if current_time - self.last_gossip_time < self.adaptive_interval:
            return False

        # Check if we have messages to propagate
        return self.pending_messages != 0

    def get_next_gossip_interval(self) -> float:
        """Get the next gossip interval."""
        # Adaptive interval based on pending messages
        if self.pending_messages > 10:
            # Increase frequency if many pending messages
            self.adaptive_interval = max(
                self.min_interval, self.adaptive_interval * 0.8
            )
        elif self.pending_messages < 3:
            # Decrease frequency if few pending messages
            self.adaptive_interval = min(
                self.max_interval, self.adaptive_interval * 1.2
            )

        return self.adaptive_interval

    def select_gossip_targets(
        self,
        available_nodes: list[str],
        node_metadata: dict[str, NodeMetadata] | None = None,
        *,
        fanout_override: int | None = None,
    ) -> list[str]:
        """
        Select nodes to gossip to based on strategy.

        Args:
            available_nodes: List of available node IDs
            node_metadata: Optional metadata about nodes

        Returns:
            List of selected node IDs
        """
        if not available_nodes:
            return []

        # Limit by fanout
        target_count = min(
            fanout_override if fanout_override is not None else self.fanout,
            len(available_nodes),
        )

        if self.strategy == GossipStrategy.RANDOM:
            return random.sample(available_nodes, target_count)

        elif self.strategy == GossipStrategy.PROXIMITY:
            # Select nodes based on proximity (if metadata available)
            if node_metadata:
                # Sort by proximity and select closest nodes
                sorted_nodes = sorted(
                    available_nodes,
                    key=lambda n: node_metadata.get(
                        n, NodeMetadata(node_id=n)
                    ).distance,
                )
                return sorted_nodes[:target_count]
            else:
                return random.sample(available_nodes, target_count)

        elif self.strategy == GossipStrategy.TOPOLOGY_AWARE:
            # Select nodes based on topology (if metadata available)
            if node_metadata:
                # Prefer nodes in different regions/tiers
                selected: list[str] = []
                seen_regions = set()

                for node_id in available_nodes:
                    node_info = node_metadata.get(
                        node_id, NodeMetadata(node_id=node_id)
                    )
                    region = node_info.region or "unknown"

                    if region not in seen_regions or len(selected) < target_count:
                        selected.append(node_id)
                        seen_regions.add(region)

                        if len(selected) >= target_count:
                            break

                return selected
            else:
                return random.sample(available_nodes, target_count)

        elif self.strategy == GossipStrategy.HYBRID:
            # Mix of random and proximity-based selection
            half_count = target_count // 2

            # Random selection
            random_targets = random.sample(
                available_nodes, min(half_count, len(available_nodes))
            )

            # Proximity-based selection
            remaining_nodes = [n for n in available_nodes if n not in random_targets]
            if remaining_nodes and node_metadata:
                proximity_count = target_count - len(random_targets)
                sorted_nodes = sorted(
                    remaining_nodes,
                    key=lambda n: node_metadata.get(
                        n, NodeMetadata(node_id=n)
                    ).distance,
                )
                proximity_targets = sorted_nodes[:proximity_count]
            else:
                proximity_targets = random.sample(
                    remaining_nodes,
                    min(target_count - len(random_targets), len(remaining_nodes)),
                )

            return random_targets + proximity_targets

        return random.sample(available_nodes, target_count)

    def record_gossip_cycle(self, messages_sent: int, bandwidth_used: float) -> None:
        """Record completion of a gossip cycle."""
        self.last_gossip_time = time.time()
        self.gossip_cycles += 1
        self.pending_messages = max(0, self.pending_messages - messages_sent)

        # Track bandwidth usage
        self.bandwidth_usage.append(bandwidth_used)
        if len(self.bandwidth_usage) > 100:
            self.bandwidth_usage = self.bandwidth_usage[-100:]

    def update_pending_messages(self, count: int) -> None:
        """Update count of pending messages."""
        self.pending_messages = max(0, count)

    def get_scheduler_statistics(self) -> SchedulerStatistics:
        """Get scheduler statistics."""
        return SchedulerStatistics(
            gossip_interval=self.gossip_interval,
            adaptive_interval=self.adaptive_interval,
            fanout=self.fanout,
            strategy=self.strategy.value,
            gossip_cycles=self.gossip_cycles,
            pending_messages=self.pending_messages,
            avg_bandwidth_usage=sum(self.bandwidth_usage)
            / max(1, len(self.bandwidth_usage)),
            last_gossip_time=self.last_gossip_time,
        )


@dataclass(slots=True)
class GossipProtocol:
    """
    Core epidemic dissemination algorithm for distributed coordination.

    Implements the gossip protocol for reliable, efficient propagation
    of information across the planet-scale federation network.
    """

    node_id: str
    transport: GossipTransport
    hub_registry: HubRegistry | None = None
    consensus_manager: ConsensusManager | None = None
    gossip_interval: float = 1.0
    fanout: int = 3
    strategy: GossipStrategy = GossipStrategy.RANDOM
    scheduler: GossipScheduler = field(init=False)
    filter: GossipFilter = field(default_factory=GossipFilter)
    message_target_filter: Callable[[GossipMessage, NodeId], bool] | None = None
    vector_clock: VectorClock = field(default_factory=VectorClock.empty)
    pending_messages: deque[Any] = field(default_factory=deque)
    recent_messages: dict[str, GossipMessage] = field(default_factory=dict)
    state_cache: dict[str, Any] = field(default_factory=dict)
    known_nodes: dict[NodeId, NodeMetadata] = field(default_factory=dict)
    node_metadata: dict[NodeId, NodeMetadata] = field(default_factory=dict)
    state_providers: list[Callable] = field(default_factory=list)
    catalog_applier: RoutingCatalogApplier | None = None
    route_applier: RouteAnnouncementProcessor | None = None
    link_state_applier: LinkStateProcessor | None = None
    route_key_applier: Any | None = None
    _background_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)
    _gossip_signal: asyncio.Event = field(default_factory=asyncio.Event)
    protocol_stats: GossipProtocolStatistics = field(
        default_factory=GossipProtocolStatistics
    )
    convergence_tracking: GossipConvergenceTracking = field(
        default_factory=GossipConvergenceTracking
    )
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """
        Initialize scheduler after other fields are set.
        """
        self.scheduler = GossipScheduler(
            self.gossip_interval, self.fanout, self.strategy
        )
        logger.debug(f"Initialized GossipProtocol for node {self.node_id}")

        # Thread safety
        self._lock = RLock()

    async def start(self) -> None:
        """Start the gossip protocol."""
        logger.debug(f"Starting GossipProtocol for node {self.node_id}")
        self.transport.register(self)

        # Start background tasks
        self._start_background_tasks()

        self.protocol_stats.protocol_started = int(time.time())

    async def stop(self) -> None:
        """Stop the gossip protocol."""
        logger.debug(f"Stopping GossipProtocol for node {self.node_id}")
        self.transport.unregister(self.node_id)

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()
        self.protocol_stats.protocol_stopped = int(time.time())

    def _start_background_tasks(self) -> None:
        """Start background tasks for gossip protocol."""
        # Main gossip cycle task
        gossip_task = asyncio.create_task(self._gossip_cycle_loop())
        self._background_tasks.add(gossip_task)

        # Message cleanup task
        cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._background_tasks.add(cleanup_task)

        # State synchronization task
        sync_task = asyncio.create_task(self._state_sync_loop())
        self._background_tasks.add(sync_task)

    async def _gossip_cycle_loop(self) -> None:
        """Main gossip cycle loop."""
        while not self._shutdown_event.is_set():
            try:
                if self.scheduler.should_gossip():
                    await self._perform_gossip_cycle()

                # Wait for next cycle or an explicit signal.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._gossip_signal.wait(),
                        timeout=self.scheduler.get_next_gossip_interval(),
                    )
                self._gossip_signal.clear()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in gossip cycle: {e}")
                await asyncio.sleep(1.0)

    async def _cleanup_loop(self) -> None:
        """Background task for message cleanup."""
        while not self._shutdown_event.is_set():
            try:
                await self._cleanup_expired_messages()
                await asyncio.sleep(30.0)  # Cleanup every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(30.0)

    async def _state_sync_loop(self) -> None:
        """Background task for state synchronization."""
        while not self._shutdown_event.is_set():
            try:
                await self._synchronize_state()
                await asyncio.sleep(60.0)  # Sync every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in state sync loop: {e}")
                await asyncio.sleep(60.0)

    async def _perform_gossip_cycle(self) -> None:
        """Perform a single gossip cycle."""
        start_time = time.time()
        messages_sent = 0
        bandwidth_used = 0.0
        has_test_function = False

        with self._lock:
            # Get available nodes
            available_nodes = self._get_available_nodes()

            if not available_nodes:
                return

            # Get messages to propagate
            messages_to_send = self._get_messages_to_propagate()
            if not messages_to_send:
                return
            # Use full fanout for catalog/route updates in small clusters.
            full_fanout_threshold = 20
            force_full_fanout = any(
                message.message_type
                in {
                    GossipMessageType.CATALOG_UPDATE,
                    GossipMessageType.ROUTE_ADVERTISEMENT,
                    GossipMessageType.ROUTE_WITHDRAWAL,
                    GossipMessageType.LINK_STATE_UPDATE,
                    GossipMessageType.ROUTE_KEY_ANNOUNCEMENT,
                }
                for message in messages_to_send
            )
            if force_full_fanout and len(available_nodes) <= full_fanout_threshold:
                targets = list(available_nodes)
            else:
                # Select gossip targets
                fanout_override = None
                if force_full_fanout and len(available_nodes) > full_fanout_threshold:
                    fanout_override = max(
                        self.fanout,
                        int(math.sqrt(len(available_nodes))) + 1,
                    )
                targets = self.scheduler.select_gossip_targets(
                    available_nodes,
                    self.node_metadata,
                    fanout_override=fanout_override,
                )

        # Send messages to targets
        for target_id in targets:
            try:
                sent_count = await self._send_messages_to_node(
                    target_id, messages_to_send
                )
                messages_sent += sent_count
                bandwidth_used += sent_count * 1024  # Estimate 1KB per message
            except Exception as e:
                logger.warning(f"Failed to send messages to {target_id}: {e}")

        # Update statistics
        cycle_time = time.time() - start_time
        self.scheduler.record_gossip_cycle(messages_sent, bandwidth_used)

        self.protocol_stats.gossip_cycles += 1
        self.protocol_stats.total_messages_sent += messages_sent

        logger.debug(
            f"Gossip cycle completed: {messages_sent} messages sent to {len(targets)} nodes in {cycle_time:.3f}s"
        )

    def _get_available_nodes(self) -> list[str]:
        """Get list of available nodes for gossiping."""
        return list(self.transport.peer_ids(exclude=self.node_id))

    def _get_messages_to_propagate(self) -> list[GossipMessage]:
        """Get messages that should be propagated."""
        messages_to_send = []

        # Get pending messages
        while self.pending_messages:
            message = self.pending_messages.popleft()

            # Check if message should be propagated
            if self.filter.should_propagate(message, self.node_id):
                # Prepare message for propagation
                propagated_message = message.prepare_for_propagation(self.node_id)
                messages_to_send.append(propagated_message)

            # Limit number of messages per cycle
            if len(messages_to_send) >= 10:
                break

        return messages_to_send

    async def _send_messages_to_node(
        self, target_id: str, messages: list[GossipMessage]
    ) -> int:
        """Send messages to a target node."""
        if not messages:
            return 0

        # Deliver messages to target gossip protocol instance

        # Check if target exists in known nodes or active transport peers
        if (
            target_id not in self.known_nodes
            and target_id not in self.transport.peer_ids(exclude=self.node_id)
        ):
            logger.warning(
                f"Target node {target_id} not found in known_nodes or transport peers"
            )
            return 0

        sent_count = 0
        for message in messages:
            try:
                if self.message_target_filter and not self.message_target_filter(
                    message, target_id
                ):
                    continue
                # Simulate network delay
                await asyncio.sleep(0.001)  # 1ms delay
                # Avoid pre-marking seen_by so receivers can forward multi-hop.

                # Store in recent messages for inspection
                self.recent_messages[message.message_id] = message
                delivered = await self.transport.send_message(target_id, message)
                if delivered:
                    sent_count += 1

            except Exception as e:
                logger.warning(
                    f"Failed to send message {message.message_id} to {target_id}: {e}"
                )

        return sent_count

    async def _cleanup_expired_messages(self) -> None:
        """Clean up expired messages."""
        current_time = time.time()

        with self._lock:
            # Clean up recent messages
            expired_messages = [
                msg_id
                for msg_id, message in self.recent_messages.items()
                if message.is_expired()
            ]

            for msg_id in expired_messages:
                del self.recent_messages[msg_id]

            # Clean up pending messages
            valid_pending: deque[GossipMessage] = deque()
            for message in self.pending_messages:
                if not message.is_expired():
                    valid_pending.append(message)

            self.pending_messages = valid_pending

            # Update scheduler with pending count
            self.scheduler.update_pending_messages(len(self.pending_messages))

        if expired_messages:
            logger.debug(f"Cleaned up {len(expired_messages)} expired messages")

    async def _synchronize_state(self) -> None:
        """Synchronize state with state providers."""
        for provider in self.state_providers:
            try:
                state_update = await provider()
                if state_update:
                    await self._create_state_update_message(state_update)
            except Exception as e:
                logger.warning(f"Failed to get state update from provider: {e}")

    async def _create_state_update_message(
        self, state_update: StateUpdatePayload | dict[str, Any]
    ) -> None:
        """Create a state update message."""
        # Increment vector clock
        self.vector_clock = self.vector_clock.increment(self.node_id)

        # Ensure payload is properly typed
        if isinstance(state_update, dict):
            typed_payload = StateUpdatePayload(
                key=state_update.get("key", ""),
                value=state_update.get("value"),
                version=state_update.get("version", 0),
                timestamp=state_update.get("timestamp", time.time()),
                source_node=self.node_id,
                ttl=state_update.get("ttl", 300),
            )
        else:
            typed_payload = state_update

        # Create message
        message = GossipMessage(
            message_id=f"{self.node_id}_{int(time.time())}_{random.randint(1000, 9999)}",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id=self.node_id,
            payload=typed_payload,
            vector_clock=self.vector_clock.copy(),
            sequence_number=self.protocol_stats.messages_created,
            ttl=10,
            max_hops=5,
        )

        # Add to pending messages
        await self.add_message(message)

        self.protocol_stats.messages_created += 1

    async def add_message(self, message: GossipMessage) -> None:
        """Add a message to be propagated."""
        broadcast_local_catalog = (
            message.message_type == GossipMessageType.CATALOG_UPDATE
            and message.hop_count == 0
        )
        with self._lock:
            # Always add locally created messages to pending queue
            # The filter check will happen later during propagation
            if message.message_type in {
                GossipMessageType.CATALOG_UPDATE,
                GossipMessageType.ROUTE_ADVERTISEMENT,
                GossipMessageType.ROUTE_WITHDRAWAL,
                GossipMessageType.LINK_STATE_UPDATE,
                GossipMessageType.ROUTE_KEY_ANNOUNCEMENT,
            }:
                if message.hop_count == 0:
                    self.pending_messages.appendleft(message)
                else:
                    self.pending_messages.append(message)
            else:
                self.pending_messages.append(message)
            self.scheduler.update_pending_messages(len(self.pending_messages))

            # Also add to recent_messages so tests can access locally created messages
            # This ensures both locally created and received messages are available for inspection
            self.recent_messages[message.message_id] = message

            logger.debug(f"Added message {message.message_id} to pending queue")
            if message.message_type == GossipMessageType.CATALOG_UPDATE:
                # Prioritize catalog updates to minimize propagation latency.
                self.scheduler.last_gossip_time = 0.0
            self._gossip_signal.set()
        if broadcast_local_catalog:
            await self._broadcast_local_catalog_update(message)

    async def _broadcast_local_catalog_update(self, message: GossipMessage) -> None:
        peer_ids = self.transport.peer_ids(exclude=self.node_id)
        if not peer_ids:
            return
        propagated_message = message.prepare_for_propagation(self.node_id)
        for peer_id in peer_ids:
            try:
                await self.transport.send_message(peer_id, propagated_message)
            except Exception as e:
                logger.warning(f"Failed to broadcast catalog update to {peer_id}: {e}")

    async def handle_received_message(self, message: GossipMessage) -> None:
        """Handle a received gossip message."""
        with self._lock:
            # Update vector clock
            self.vector_clock = self.vector_clock.update(message.vector_clock)

            # Process message based on type
            if message.message_type == GossipMessageType.STATE_UPDATE:
                await self._handle_state_update(message)
            elif message.message_type == GossipMessageType.MEMBERSHIP_UPDATE:
                await self._handle_membership_update(message)
            elif message.message_type == GossipMessageType.CONFIG_UPDATE:
                await self._handle_config_update(message)
            elif message.message_type == GossipMessageType.HEARTBEAT:
                await self._handle_heartbeat(message)
            elif message.message_type == GossipMessageType.CONSENSUS_PROPOSAL:
                await self._handle_consensus_proposal(message)
            elif message.message_type == GossipMessageType.CONSENSUS_VOTE:
                await self._handle_consensus_vote(message)
            elif message.message_type == GossipMessageType.CATALOG_UPDATE:
                await self._handle_catalog_update(message)
            elif message.message_type == GossipMessageType.ROUTE_ADVERTISEMENT:
                await self._handle_route_advertisement(message)
            elif message.message_type == GossipMessageType.ROUTE_WITHDRAWAL:
                await self._handle_route_withdrawal(message)
            elif message.message_type == GossipMessageType.LINK_STATE_UPDATE:
                await self._handle_link_state_update(message)
            elif message.message_type == GossipMessageType.ROUTE_KEY_ANNOUNCEMENT:
                await self._handle_route_key_announcement(message)

            # Add to recent messages
            self.recent_messages[message.message_id] = message

            # Propagate if needed
            if message.can_propagate():
                await self.add_message(message)

        self.protocol_stats.messages_received += 1

    async def _handle_state_update(self, message: GossipMessage) -> None:
        """Handle a state update message."""
        if isinstance(message.payload, StateUpdatePayload):
            state_key = message.payload.key
            state_value = message.payload.value
        elif isinstance(message.payload, dict):
            state_key = message.payload.get("key") or ""
            state_value = message.payload.get("value")
        else:
            logger.warning(
                f"Invalid state update payload type: {type(message.payload)}"
            )
            return

        if state_key:
            self.state_cache[state_key] = state_value
            logger.debug(f"Updated state: {state_key} = {state_value}")

    async def _handle_membership_update(self, message: GossipMessage) -> None:
        """Handle a membership update message."""
        if isinstance(message.payload, MembershipUpdatePayload):
            node_id = message.payload.node_id
            node_info = message.payload.node_info
        elif isinstance(message.payload, dict):
            node_id = message.payload.get("node_id") or ""
            node_info = message.payload.get("node_info") or {}
        else:
            logger.warning(
                f"Invalid membership update payload type: {type(message.payload)}"
            )
            return

        if node_id and node_info:
            # Convert node_info dict to NodeMetadata if needed
            if isinstance(node_info, dict):
                metadata = NodeMetadata(
                    node_id=node_id,
                    region=node_info.get("region", ""),
                    distance=node_info.get("distance", 0.0),
                    last_heartbeat=node_info.get("last_heartbeat", 0.0),
                    capabilities=node_info.get("capabilities", {}),
                    health_status=node_info.get("health_status", "unknown"),
                    load_metrics=node_info.get("load_metrics", {}),
                    connection_info=node_info.get("connection_info", {}),
                )
            else:
                metadata = node_info

            self.known_nodes[node_id] = metadata
            self.node_metadata[node_id] = metadata
            logger.debug(f"Updated membership: {node_id}")

    async def _handle_config_update(self, message: GossipMessage) -> None:
        """Handle a configuration update message."""
        if isinstance(message.payload, ConfigUpdatePayload):
            config_key = message.payload.config_key
            config_value = message.payload.config_value
        elif isinstance(message.payload, dict):
            config_key = message.payload.get("config_key") or ""
            config_value = message.payload.get("config_value")
        else:
            logger.warning(
                f"Invalid config update payload type: {type(message.payload)}"
            )
            return

        if config_key:
            # Apply configuration update
            logger.debug(f"Config update: {config_key} = {config_value}")

    async def _handle_heartbeat(self, message: GossipMessage) -> None:
        """Handle a heartbeat message."""
        sender_id = message.sender_id

        # Update node metadata
        if sender_id in self.known_nodes:
            self.known_nodes[sender_id].last_heartbeat = time.time()

    async def _handle_consensus_proposal(self, message: GossipMessage) -> None:
        """Handle a consensus proposal message."""
        if not self.consensus_manager:
            logger.warning(
                "Received consensus proposal but no consensus manager available"
            )
            return

        try:
            if isinstance(message.payload, dict):
                await self.consensus_manager.handle_proposal_message(message.payload)
                logger.debug(
                    f"Forwarded consensus proposal {message.payload.get('proposal_id')} to consensus manager"
                )
            else:
                logger.warning(
                    f"Invalid consensus proposal payload type: {type(message.payload)}"
                )
        except Exception as e:
            logger.error(f"Error handling consensus proposal: {e}")

    async def _handle_consensus_vote(self, message: GossipMessage) -> None:
        """Handle a consensus vote message."""
        if not self.consensus_manager:
            logger.warning("Received consensus vote but no consensus manager available")
            return

        try:
            if isinstance(message.payload, dict):
                proposal_id = message.payload.get("proposal_id")
                vote = message.payload.get("vote")
                voter_id = message.payload.get(
                    "voter_node_id"
                )  # Fixed: use voter_node_id

                if proposal_id and vote is not None and voter_id:
                    await self.consensus_manager.vote_on_proposal(
                        proposal_id, vote, voter_id
                    )
                    logger.debug(
                        f"Forwarded consensus vote from {voter_id} on proposal {proposal_id} to consensus manager"
                    )
                else:
                    logger.warning(
                        f"Incomplete consensus vote payload: {message.payload}"
                    )
            else:
                logger.warning(
                    f"Invalid consensus vote payload type: {type(message.payload)}"
                )
        except Exception as e:
            logger.error(f"Error handling consensus vote: {e}")

    async def _handle_catalog_update(self, message: GossipMessage) -> None:
        """Handle routing catalog updates."""
        if not self.catalog_applier:
            logger.debug("Catalog update received but no catalog applier configured.")
            return
        if isinstance(message.payload, RoutingCatalogDelta):
            delta = message.payload
        elif isinstance(message.payload, dict):
            delta = RoutingCatalogDelta.from_dict(message.payload)
        else:
            logger.warning(
                f"Invalid catalog update payload type: {type(message.payload)}"
            )
            return
        self.catalog_applier.apply(delta, now=delta.sent_at)
        return

    async def _handle_route_advertisement(self, message: GossipMessage) -> None:
        """Handle path-vector route advertisements."""
        if not self.route_applier:
            logger.debug(
                "Route advertisement received but no route applier configured."
            )
            return
        if isinstance(message.payload, RouteAnnouncement):
            await self.route_applier.handle_announcement(
                message.payload, sender_id=message.sender_id
            )
            return
        if isinstance(message.payload, dict):
            announcement = RouteAnnouncement.from_dict(message.payload)
            await self.route_applier.handle_announcement(
                announcement, sender_id=message.sender_id
            )
            return
        logger.warning(
            f"Invalid route advertisement payload type: {type(message.payload)}"
        )

    async def _handle_route_withdrawal(self, message: GossipMessage) -> None:
        """Handle path-vector route withdrawals."""
        if not self.route_applier:
            logger.debug("Route withdrawal received but no route applier configured.")
            return
        if isinstance(message.payload, RouteWithdrawal):
            await self.route_applier.handle_withdrawal(
                message.payload, sender_id=message.sender_id
            )
            return
        if isinstance(message.payload, dict):
            withdrawal = RouteWithdrawal.from_dict(message.payload)
            await self.route_applier.handle_withdrawal(
                withdrawal, sender_id=message.sender_id
            )
            return
        logger.warning(
            f"Invalid route withdrawal payload type: {type(message.payload)}"
        )

    async def _handle_link_state_update(self, message: GossipMessage) -> None:
        """Handle link-state updates."""
        if not self.link_state_applier:
            logger.debug(
                "Link-state update received but no link-state applier configured."
            )
            return
        if isinstance(message.payload, LinkStateUpdate):
            await self.link_state_applier.handle_update(
                message.payload, sender_id=message.sender_id
            )
            return
        if isinstance(message.payload, dict):
            update = LinkStateUpdate.from_dict(message.payload)
            await self.link_state_applier.handle_update(
                update, sender_id=message.sender_id
            )
            return
        logger.warning(f"Invalid link-state payload type: {type(message.payload)}")

    async def _handle_route_key_announcement(self, message: GossipMessage) -> None:
        """Handle route key announcements."""
        if not self.route_key_applier:
            logger.debug(
                "Route key announcement received but no key applier configured."
            )
            return
        if isinstance(message.payload, RouteKeyAnnouncement):
            await self.route_key_applier.handle_announcement(
                message.payload, sender_id=message.sender_id
            )
            return
        if isinstance(message.payload, dict):
            announcement = RouteKeyAnnouncement.from_dict(message.payload)
            await self.route_key_applier.handle_announcement(
                announcement, sender_id=message.sender_id
            )
            return
        logger.warning(f"Invalid route key payload type: {type(message.payload)}")

    def register_state_provider(self, provider: Callable) -> None:
        """Register a state provider function."""
        self.state_providers.append(provider)
        logger.debug(f"Registered state provider: {provider}")

    def get_convergence_status(self) -> ConvergenceStatus:
        """Get convergence status information."""
        with self._lock:
            return ConvergenceStatus(
                node_id=self.node_id,
                known_nodes=len(self.known_nodes),
                pending_messages=len(self.pending_messages),
                recent_messages=len(self.recent_messages),
                state_cache_size=len(self.state_cache),
                vector_clock=self.vector_clock.to_dict(),
                last_gossip_time=self.scheduler.last_gossip_time,
                gossip_cycles=self.scheduler.gossip_cycles,
            )

    def get_comprehensive_statistics(self) -> ComprehensiveStatistics:
        """Get comprehensive protocol statistics."""
        with self._lock:
            return ComprehensiveStatistics(
                protocol_info=ProtocolInfo(
                    node_id=self.node_id,
                    gossip_strategy=self.scheduler.strategy.value,
                    gossip_interval=self.scheduler.gossip_interval,
                    fanout=self.scheduler.fanout,
                ),
                protocol_stats=self.protocol_stats,
                scheduler_stats=self.scheduler.get_scheduler_statistics(),
                filter_stats=self.filter.get_filter_statistics(),
                state_info=StateInfo(
                    known_nodes=len(self.known_nodes),
                    pending_messages=len(self.pending_messages),
                    recent_messages=len(self.recent_messages),
                    state_cache_size=len(self.state_cache),
                ),
                convergence_status=self.get_convergence_status(),
            )
