"""
Membership and Failure Detection for the MPREG Fabric.

This module implements advanced membership management and failure detection
for the planet-scale fabric control plane. It provides SWIM-based failure detection
with configurable suspicion levels and automatic recovery mechanisms.

Key Features:
- SWIM-based failure detection with suspicion mechanisms
- Gradual suspicion escalation with configurable thresholds
- Automatic node recovery and reintegration
- Membership change propagation via gossip protocol
- Configurable failure detection timing and sensitivity
- Comprehensive health monitoring and statistics
- Integration with existing gossip and consensus systems

This is Phase 3.3 of the Planet-Scale Fabric Roadmap.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Any

from loguru import logger

from mpreg.fabric.federation_graph import GeographicCoordinate

from .consensus import ConsensusManager
from .gossip import GossipMessage, GossipMessageType, GossipProtocol


class MembershipState(Enum):
    """Membership states for nodes in the federation."""

    ALIVE = "alive"
    SUSPECT = "suspect"
    FAILED = "failed"
    LEFT = "left"
    JOINING = "joining"


class ProbeResult(Enum):
    """Results of membership probes."""

    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    INDIRECT_SUCCESS = "indirect_success"
    INDIRECT_FAILURE = "indirect_failure"


class MembershipEventType(Enum):
    """Types of membership events."""

    NODE_JOINED = "node_joined"
    NODE_LEFT = "node_left"
    NODE_FAILED = "node_failed"
    NODE_RECOVERED = "node_recovered"
    NODE_SUSPECTED = "node_suspected"
    NODE_CONFIRMED = "node_confirmed"


# New dataclasses for type safety
@dataclass(slots=True)
class NodeCapabilities:
    """Node capabilities information."""

    cpu_cores: int = 0
    memory_gb: int = 0
    storage_gb: int = 0
    network_bandwidth_mbps: int = 0
    supports_persistence: bool = False
    supports_clustering: bool = False
    max_connections: int = 100
    protocols: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    custom_attributes: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class MembershipSummary:
    """Summary of node membership information."""

    node_id: str
    state: str
    incarnation: int
    region: str
    last_seen: float
    staleness_seconds: float
    probe_failures: int
    suspicion_level: float
    suspected_by_count: int
    is_available: bool


@dataclass(slots=True)
class EventMetadata:
    """Metadata for membership events."""

    priority: str = "normal"
    source_region: str = ""
    correlation_id: str = ""
    sequence_id: int = 0
    retry_count: int = 0
    additional_info: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class EventSummary:
    """Summary of a membership event."""

    event_id: str
    event_type: str
    node_id: str
    incarnation: int
    timestamp: float
    source_node: str
    reason: str
    age_seconds: float


@dataclass(slots=True)
class ProbeMessagePayload:
    """Payload for probe messages."""

    probe_id: str
    target_node: str
    source_node: str
    probe_type: str = "direct"
    sequence_number: int = 0
    timestamp: float = field(default_factory=time.time)
    indirect_node: str = ""  # For indirect probes


@dataclass(slots=True)
class MembershipUpdatePayload:
    """Payload for membership update messages."""

    node_id: str
    last_seen: float
    state: MembershipState
    incarnation: int = 0
    region: str = ""
    capabilities: NodeCapabilities | None = None


@dataclass(slots=True)
class MembershipStatistics:
    """Comprehensive membership statistics."""

    membership_info: dict[str, Any]  # Keep as dict for protocol info
    membership_counts: dict[str, int]
    membership_stats: dict[str, int]
    probe_stats: dict[str, int]
    active_probes: int
    recent_events: int
    probe_history_size: int


@dataclass(slots=True)
class MembershipInfo:
    """
    Membership information for a federation node.

    Tracks the current state, health, and metadata for a node
    in the federation membership system.
    """

    node_id: str
    state: MembershipState
    incarnation: int = 0
    coordinates: GeographicCoordinate | None = None
    region: str = ""

    # Health and timing information
    last_seen: float = field(default_factory=time.time)
    last_probe: float = 0.0
    last_ack: float = 0.0
    probe_failures: int = 0

    # Suspicion tracking
    suspicion_level: float = 0.0
    suspicion_timeout: float = 30.0
    suspected_at: float = 0.0
    suspected_by: set[str] = field(default_factory=set)

    # Metadata
    capabilities: NodeCapabilities = field(default_factory=NodeCapabilities)
    version: str = ""
    created_at: float = field(default_factory=time.time)

    def is_alive(self) -> bool:
        """Check if node is considered alive."""
        return self.state == MembershipState.ALIVE

    def is_suspect(self) -> bool:
        """Check if node is under suspicion."""
        return self.state == MembershipState.SUSPECT

    def is_failed(self) -> bool:
        """Check if node is considered failed."""
        return self.state == MembershipState.FAILED

    def is_available(self) -> bool:
        """Check if node is available for communication."""
        return self.state in [MembershipState.ALIVE, MembershipState.SUSPECT]

    def get_staleness(self) -> float:
        """Get time since last contact."""
        return time.time() - self.last_seen

    def update_seen(self) -> None:
        """Update last seen timestamp."""
        self.last_seen = time.time()

    def add_suspicion(self, suspecting_node: str) -> None:
        """Add a node to the suspicion set."""
        self.suspected_by.add(suspecting_node)
        self.suspicion_level = min(
            1.0, len(self.suspected_by) / 3.0
        )  # Max at 3 suspicions

        if self.suspected_at == 0.0:
            self.suspected_at = time.time()

    def clear_suspicion(self) -> None:
        """Clear all suspicion information."""
        self.suspected_by.clear()
        self.suspicion_level = 0.0
        self.suspected_at = 0.0

    def is_suspicion_expired(self) -> bool:
        """Check if suspicion has expired."""
        if self.suspected_at == 0.0:
            return False
        return time.time() - self.suspected_at > self.suspicion_timeout

    def get_membership_summary(self) -> MembershipSummary:
        """Get summary of membership information."""
        return MembershipSummary(
            node_id=self.node_id,
            state=self.state.value,
            incarnation=self.incarnation,
            region=self.region,
            last_seen=self.last_seen,
            staleness_seconds=self.get_staleness(),
            probe_failures=self.probe_failures,
            suspicion_level=self.suspicion_level,
            suspected_by_count=len(self.suspected_by),
            is_available=self.is_available(),
        )


@dataclass(slots=True)
class MembershipEvent:
    """
    Represents a membership change event.

    Used to track and propagate membership changes throughout
    the federation system.
    """

    event_id: str
    event_type: MembershipEventType
    node_id: str
    incarnation: int
    timestamp: float = field(default_factory=time.time)

    # Event metadata
    source_node: str = ""
    reason: str = ""
    metadata: EventMetadata = field(default_factory=EventMetadata)

    def get_event_summary(self) -> EventSummary:
        """Get summary of the membership event."""
        return EventSummary(
            event_id=self.event_id,
            event_type=self.event_type.value,
            node_id=self.node_id,
            incarnation=self.incarnation,
            timestamp=self.timestamp,
            source_node=self.source_node,
            reason=self.reason,
            age_seconds=time.time() - self.timestamp,
        )


@dataclass(slots=True)
class ProbeRequest:
    """
    Represents a membership probe request.

    Used for direct and indirect probing of node health
    in the SWIM failure detection protocol.
    """

    probe_id: str
    target_node: str
    source_node: str
    probe_type: str = "direct"  # direct, indirect, ack
    sequence_number: int = 0
    timestamp: float = field(default_factory=time.time)
    timeout: float = 5.0

    # Indirect probing
    indirect_nodes: list[str] = field(default_factory=list)

    def is_expired(self) -> bool:
        """Check if probe request has expired."""
        return time.time() - self.timestamp > self.timeout

    def get_age(self) -> float:
        """Get age of probe request in seconds."""
        return time.time() - self.timestamp


@dataclass(slots=True)
class MembershipProtocol:
    """
    SWIM-based membership protocol for distributed failure detection.

    Implements the SWIM (Scalable Weakly-consistent Infection-style
    Process Group Membership) protocol for reliable failure detection
    and membership management in large-scale distributed systems.
    """

    node_id: str
    gossip_protocol: GossipProtocol | None = None
    consensus_manager: ConsensusManager | None = None
    probe_interval: float = 2.0
    probe_timeout: float = 5.0
    suspicion_timeout: float = 30.0
    indirect_probe_count: int = 3
    membership: dict[str, MembershipInfo] = field(init=False)
    membership_events: deque[Any] = field(default_factory=lambda: deque(maxlen=1000))
    pending_probes: dict[str, ProbeRequest] = field(default_factory=dict)
    own_info: MembershipInfo = field(init=False)
    probe_sequence: int = 0
    current_probe_target: str | None = None
    probe_history: deque[Any] = field(default_factory=lambda: deque(maxlen=100))
    membership_stats: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    probe_stats: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _background_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """
        Initialize own membership info and add to membership dict.
        """
        # Own membership info
        self.own_info = MembershipInfo(
            node_id=self.node_id, state=MembershipState.ALIVE, incarnation=0
        )
        self.membership = {self.node_id: self.own_info}

        logger.info(f"Initialized MembershipProtocol for node {self.node_id}")

    async def start(self) -> None:
        """Start the membership protocol."""
        logger.info(f"Starting MembershipProtocol for node {self.node_id}")

        # Start background tasks
        self._start_background_tasks()

        # Announce our presence
        await self._announce_membership()

        self.membership_stats["protocol_started"] = int(time.time())

    async def stop(self) -> None:
        """Stop the membership protocol."""
        logger.info(f"Stopping MembershipProtocol for node {self.node_id}")

        # Announce departure
        await self._announce_departure()

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()
        self.membership_stats["protocol_stopped"] = int(time.time())

    def _start_background_tasks(self) -> None:
        """Start background tasks for membership protocol."""
        # Main probe task
        probe_task = asyncio.create_task(self._probe_loop())
        self._background_tasks.add(probe_task)

        # Suspicion management task
        suspicion_task = asyncio.create_task(self._suspicion_loop())
        self._background_tasks.add(suspicion_task)

        # Cleanup task
        cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._background_tasks.add(cleanup_task)

    async def _probe_loop(self) -> None:
        """Main probe loop for failure detection."""
        while not self._shutdown_event.is_set():
            try:
                await self._perform_probe_cycle()
                await asyncio.sleep(self.probe_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in probe loop: {e}")
                await asyncio.sleep(self.probe_interval)

    async def _suspicion_loop(self) -> None:
        """Background task for managing suspicions."""
        while not self._shutdown_event.is_set():
            try:
                await self._process_suspicions()
                await asyncio.sleep(5.0)  # Check suspicions every 5 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in suspicion loop: {e}")
                await asyncio.sleep(5.0)

    async def _cleanup_loop(self) -> None:
        """Background task for cleanup operations."""
        while not self._shutdown_event.is_set():
            try:
                await self._cleanup_expired_probes()
                await asyncio.sleep(30.0)  # Cleanup every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(30.0)

    async def _perform_probe_cycle(self) -> None:
        """Perform a single probe cycle."""
        with self._lock:
            # Select target for probing
            target = self._select_probe_target()

            if not target:
                return

            self.current_probe_target = target
            self.probe_sequence += 1

        # Perform probe
        result = await self._probe_node(target)

        # Process result
        await self._process_probe_result(target, result)

        self.probe_stats["probe_cycles"] += 1

    def _select_probe_target(self) -> str | None:
        """Select next node to probe."""
        # Get all alive or suspect nodes (excluding self)
        candidates = [
            node_id
            for node_id, info in self.membership.items()
            if node_id != self.node_id and info.is_available()
        ]

        if not candidates:
            return None

        # Prefer nodes that haven't been probed recently
        candidates.sort(key=lambda n: self.membership[n].last_probe)

        # Select randomly from top candidates
        top_count = min(3, len(candidates))
        return random.choice(candidates[:top_count])

    async def _probe_node(self, target_node: str) -> ProbeResult:
        """Probe a specific node."""
        probe_id = f"probe_{self.node_id}_{target_node}_{self.probe_sequence}"

        probe_request = ProbeRequest(
            probe_id=probe_id,
            target_node=target_node,
            source_node=self.node_id,
            probe_type="direct",
            sequence_number=self.probe_sequence,
            timeout=self.probe_timeout,
        )

        # Store pending probe
        self.pending_probes[probe_id] = probe_request

        # Send probe via gossip if available
        if self.gossip_protocol:
            await self._send_probe_message(probe_request)

        # Wait for response or timeout
        start_time = time.time()
        while time.time() - start_time < self.probe_timeout:
            if probe_id not in self.pending_probes:
                # Response received
                return ProbeResult.SUCCESS

            await asyncio.sleep(0.1)

        # Timeout - try indirect probing
        if probe_id in self.pending_probes:
            del self.pending_probes[probe_id]
            return await self._indirect_probe_node(target_node)

        return ProbeResult.FAILURE

    async def _indirect_probe_node(self, target_node: str) -> ProbeResult:
        """Perform indirect probing of a node."""
        # Select nodes for indirect probing
        indirect_candidates = [
            node_id
            for node_id, info in self.membership.items()
            if node_id != self.node_id and node_id != target_node and info.is_alive()
        ]

        if not indirect_candidates:
            return ProbeResult.FAILURE

        # Use up to indirect_probe_count nodes
        indirect_nodes = random.sample(
            indirect_candidates,
            min(self.indirect_probe_count, len(indirect_candidates)),
        )

        probe_id = f"indirect_probe_{self.node_id}_{target_node}_{self.probe_sequence}"

        indirect_request = ProbeRequest(
            probe_id=probe_id,
            target_node=target_node,
            source_node=self.node_id,
            probe_type="indirect",
            sequence_number=self.probe_sequence,
            timeout=self.probe_timeout,
            indirect_nodes=indirect_nodes,
        )

        # Store pending probe
        self.pending_probes[probe_id] = indirect_request

        # Send indirect probe requests
        if self.gossip_protocol:
            await self._send_indirect_probe_messages(indirect_request)

        # Wait for responses
        start_time = time.time()
        while time.time() - start_time < self.probe_timeout:
            if probe_id not in self.pending_probes:
                # Response received
                return ProbeResult.INDIRECT_SUCCESS

            await asyncio.sleep(0.1)

        # Cleanup and return failure
        if probe_id in self.pending_probes:
            del self.pending_probes[probe_id]

        return ProbeResult.INDIRECT_FAILURE

    async def _process_probe_result(
        self, target_node: str, result: ProbeResult
    ) -> None:
        """Process the result of a probe."""
        with self._lock:
            if target_node not in self.membership:
                return

            member_info = self.membership[target_node]
            member_info.last_probe = time.time()

            if result in [ProbeResult.SUCCESS, ProbeResult.INDIRECT_SUCCESS]:
                # Node is alive
                if member_info.is_suspect():
                    # Node recovered from suspicion
                    await self._recover_node(target_node)

                member_info.probe_failures = 0
                member_info.update_seen()

                self.probe_stats["probe_successes"] += 1

            else:
                # Probe failed
                member_info.probe_failures += 1

                if member_info.is_alive():
                    # Mark as suspect
                    await self._suspect_node(target_node, "probe_failure")
                elif member_info.is_suspect():
                    # Increase suspicion
                    member_info.add_suspicion(self.node_id)

                self.probe_stats["probe_failures"] += 1

        # Record probe history
        self.probe_history.append(
            {"target": target_node, "result": result.value, "timestamp": time.time()}
        )

    async def _suspect_node(self, node_id: str, reason: str) -> None:
        """Mark a node as suspect."""
        with self._lock:
            if node_id not in self.membership:
                return

            member_info = self.membership[node_id]

            if member_info.is_alive():
                member_info.state = MembershipState.SUSPECT
                member_info.add_suspicion(self.node_id)

                # Create suspicion event
                event = MembershipEvent(
                    event_id=f"suspect_{node_id}_{int(time.time())}",
                    event_type=MembershipEventType.NODE_SUSPECTED,
                    node_id=node_id,
                    incarnation=member_info.incarnation,
                    source_node=self.node_id,
                    reason=reason,
                )

                self.membership_events.append(event)

                # Propagate suspicion via gossip
                if self.gossip_protocol:
                    await self._propagate_membership_event(event)

                self.membership_stats["nodes_suspected"] += 1
                logger.info(f"Node {node_id} marked as suspect: {reason}")

    async def _recover_node(self, node_id: str) -> None:
        """Recover a node from suspicion."""
        with self._lock:
            if node_id not in self.membership:
                return

            member_info = self.membership[node_id]

            if member_info.is_suspect():
                member_info.state = MembershipState.ALIVE
                member_info.clear_suspicion()

                # Create recovery event
                event = MembershipEvent(
                    event_id=f"recover_{node_id}_{int(time.time())}",
                    event_type=MembershipEventType.NODE_RECOVERED,
                    node_id=node_id,
                    incarnation=member_info.incarnation,
                    source_node=self.node_id,
                    reason="probe_success",
                )

                self.membership_events.append(event)

                # Propagate recovery via gossip
                if self.gossip_protocol:
                    await self._propagate_membership_event(event)

                self.membership_stats["nodes_recovered"] += 1
                logger.info(f"Node {node_id} recovered from suspicion")

    async def _fail_node(self, node_id: str, reason: str) -> None:
        """Mark a node as failed."""
        with self._lock:
            if node_id not in self.membership:
                return

            member_info = self.membership[node_id]

            if not member_info.is_failed():
                member_info.state = MembershipState.FAILED
                member_info.clear_suspicion()

                # Create failure event
                event = MembershipEvent(
                    event_id=f"fail_{node_id}_{int(time.time())}",
                    event_type=MembershipEventType.NODE_FAILED,
                    node_id=node_id,
                    incarnation=member_info.incarnation,
                    source_node=self.node_id,
                    reason=reason,
                )

                self.membership_events.append(event)

                # Propagate failure via gossip
                if self.gossip_protocol:
                    await self._propagate_membership_event(event)

                self.membership_stats["nodes_failed"] += 1
                logger.warning(f"Node {node_id} marked as failed: {reason}")

    async def _process_suspicions(self) -> None:
        """Process pending suspicions and timeouts."""
        current_time = time.time()

        with self._lock:
            for node_id, member_info in list(self.membership.items()):
                if member_info.is_suspect():
                    # Check if suspicion has expired
                    if member_info.is_suspicion_expired():
                        await self._fail_node(node_id, "suspicion_timeout")

                    # Check if enough nodes suspect this node
                    elif member_info.suspicion_level >= 0.5:  # 50% threshold
                        await self._fail_node(node_id, "suspicion_consensus")

    async def _cleanup_expired_probes(self) -> None:
        """Clean up expired probe requests."""
        current_time = time.time()

        expired_probes = [
            probe_id
            for probe_id, probe in self.pending_probes.items()
            if probe.is_expired()
        ]

        for probe_id in expired_probes:
            del self.pending_probes[probe_id]

        if expired_probes:
            logger.debug(f"Cleaned up {len(expired_probes)} expired probes")

    async def _send_probe_message(self, probe_request: ProbeRequest) -> None:
        """Send a probe message via gossip."""
        if not self.gossip_protocol:
            return

        probe_message = GossipMessage(
            message_id=f"probe_{probe_request.probe_id}",
            message_type=GossipMessageType.MEMBERSHIP_PROBE,
            sender_id=self.node_id,
            payload={
                "probe_id": probe_request.probe_id,
                "target_node": probe_request.target_node,
                "source_node": probe_request.source_node,
                "probe_type": probe_request.probe_type,
                "sequence_number": probe_request.sequence_number,
                "timestamp": probe_request.timestamp,
            },
        )

        await self.gossip_protocol.add_message(probe_message)

    async def _send_indirect_probe_messages(self, probe_request: ProbeRequest) -> None:
        """Send indirect probe messages."""
        if not self.gossip_protocol:
            return

        for indirect_node in probe_request.indirect_nodes:
            indirect_message = GossipMessage(
                message_id=f"indirect_probe_{probe_request.probe_id}_{indirect_node}",
                message_type=GossipMessageType.MEMBERSHIP_INDIRECT_PROBE,
                sender_id=self.node_id,
                payload={
                    "probe_id": probe_request.probe_id,
                    "target_node": probe_request.target_node,
                    "source_node": probe_request.source_node,
                    "indirect_node": indirect_node,
                    "sequence_number": probe_request.sequence_number,
                    "timestamp": probe_request.timestamp,
                },
            )

            await self.gossip_protocol.add_message(indirect_message)

    async def _propagate_membership_event(self, event: MembershipEvent) -> None:
        """Propagate a membership event via gossip."""
        if not self.gossip_protocol:
            return

        event_message = GossipMessage(
            message_id=f"membership_event_{event.event_id}",
            message_type=GossipMessageType.MEMBERSHIP_UPDATE,
            sender_id=self.node_id,
            payload={
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "node_id": event.node_id,
                "incarnation": event.incarnation,
                "timestamp": event.timestamp,
                "source_node": event.source_node,
                "reason": event.reason,
                "metadata": event.metadata,
            },
        )

        await self.gossip_protocol.add_message(event_message)

    async def _announce_membership(self) -> None:
        """Announce our membership to the federation."""
        join_event = MembershipEvent(
            event_id=f"join_{self.node_id}_{int(time.time())}",
            event_type=MembershipEventType.NODE_JOINED,
            node_id=self.node_id,
            incarnation=self.own_info.incarnation,
            source_node=self.node_id,
            reason="startup",
        )

        self.membership_events.append(join_event)

        if self.gossip_protocol:
            await self._propagate_membership_event(join_event)

        self.membership_stats["announcements_sent"] += 1
        logger.info(f"Announced membership for node {self.node_id}")

    async def _announce_departure(self) -> None:
        """Announce our departure from the federation."""
        leave_event = MembershipEvent(
            event_id=f"leave_{self.node_id}_{int(time.time())}",
            event_type=MembershipEventType.NODE_LEFT,
            node_id=self.node_id,
            incarnation=self.own_info.incarnation,
            source_node=self.node_id,
            reason="shutdown",
        )

        self.membership_events.append(leave_event)

        if self.gossip_protocol:
            await self._propagate_membership_event(leave_event)

        self.membership_stats["departures_sent"] += 1
        logger.info(f"Announced departure for node {self.node_id}")

    async def handle_probe_message(
        self, message_payload: ProbeMessagePayload | dict[str, Any]
    ) -> None:
        """Handle a received probe message."""
        if isinstance(message_payload, ProbeMessagePayload):
            probe_id = message_payload.probe_id
            target_node = message_payload.target_node
            source_node = message_payload.source_node
        elif isinstance(message_payload, dict):
            probe_id = message_payload.get("probe_id") or ""
            target_node = message_payload.get("target_node") or ""
            source_node = message_payload.get("source_node") or ""
        else:
            logger.warning(
                f"Invalid probe message payload type: {type(message_payload)}"
            )
            return

        if (
            target_node == self.node_id
            and isinstance(probe_id, str)
            and isinstance(source_node, str)
        ):
            # This is a probe for us - send ACK
            await self._send_probe_ack(probe_id, source_node)

        # Update membership info for source node
        if source_node and source_node != self.node_id:
            await self._update_node_membership(
                source_node,
                MembershipUpdatePayload(
                    node_id=source_node,
                    last_seen=time.time(),
                    state=MembershipState.ALIVE,
                ),
            )

    async def handle_probe_ack(
        self, message_payload: ProbeMessagePayload | dict[str, Any]
    ) -> None:
        """Handle a received probe ACK."""
        if isinstance(message_payload, ProbeMessagePayload):
            probe_id = message_payload.probe_id
            source_node = message_payload.source_node
        elif isinstance(message_payload, dict):
            probe_id = message_payload.get("probe_id") or ""
            source_node = message_payload.get("source_node") or ""
        else:
            logger.warning(f"Invalid probe ack payload type: {type(message_payload)}")
            return

        # Mark probe as successful
        if probe_id in self.pending_probes:
            del self.pending_probes[probe_id]

        # Update membership info for responding node
        if source_node and source_node != self.node_id:
            await self._update_node_membership(
                source_node,
                MembershipUpdatePayload(
                    node_id=source_node,
                    last_seen=time.time(),
                    state=MembershipState.ALIVE,
                ),
            )

    async def handle_membership_event(self, event_payload: dict[str, Any]) -> None:
        """Handle a received membership event."""
        event_type = MembershipEventType(event_payload.get("event_type"))
        node_id = event_payload.get("node_id")
        incarnation = event_payload.get("incarnation", 0)
        source_node = event_payload.get("source_node")

        if node_id == self.node_id:
            # Event about ourselves - check incarnation
            if incarnation <= self.own_info.incarnation:
                # Refute if stale
                await self._refute_membership_event(event_payload)
            return

        # Process event for other nodes
        if not isinstance(node_id, str):
            logger.warning("Invalid node_id in membership event")
            return

        with self._lock:
            if node_id not in self.membership:
                # Unknown node
                if event_type == MembershipEventType.NODE_JOINED:
                    self.membership[node_id] = MembershipInfo(
                        node_id=node_id,
                        state=MembershipState.ALIVE,
                        incarnation=incarnation,
                    )
                    self.membership_stats["nodes_joined"] += 1
            else:
                # Known node - update state
                member_info = self.membership[node_id]

                if incarnation > member_info.incarnation:
                    member_info.incarnation = incarnation

                    if event_type == MembershipEventType.NODE_SUSPECTED:
                        member_info.state = MembershipState.SUSPECT
                        if isinstance(source_node, str):
                            member_info.add_suspicion(source_node)
                    elif event_type == MembershipEventType.NODE_FAILED:
                        member_info.state = MembershipState.FAILED
                        member_info.clear_suspicion()
                    elif event_type == MembershipEventType.NODE_RECOVERED:
                        member_info.state = MembershipState.ALIVE
                        member_info.clear_suspicion()
                    elif event_type == MembershipEventType.NODE_LEFT:
                        member_info.state = MembershipState.LEFT

    async def _send_probe_ack(self, probe_id: str, source_node: str) -> None:
        """Send a probe ACK message."""
        if not self.gossip_protocol:
            return

        ack_message = GossipMessage(
            message_id=f"probe_ack_{probe_id}",
            message_type=GossipMessageType.MEMBERSHIP_ACK,
            sender_id=self.node_id,
            payload={
                "probe_id": probe_id,
                "source_node": self.node_id,
                "target_node": source_node,
                "timestamp": time.time(),
            },
        )

        await self.gossip_protocol.add_message(ack_message)

    async def _update_node_membership(
        self, node_id: str, updates: MembershipUpdatePayload
    ) -> None:
        """Update membership information for a node."""
        with self._lock:
            if node_id not in self.membership:
                self.membership[node_id] = MembershipInfo(
                    node_id=node_id, state=MembershipState.ALIVE, incarnation=0
                )

            member_info = self.membership[node_id]

            if updates.last_seen > 0:
                member_info.last_seen = updates.last_seen
            if updates.state:
                member_info.state = updates.state
            if updates.incarnation > 0:
                member_info.incarnation = updates.incarnation
            if updates.region:
                member_info.region = updates.region
            if updates.capabilities:
                member_info.capabilities = updates.capabilities

            member_info.update_seen()

    async def _refute_membership_event(self, event_payload: dict[str, Any]) -> None:
        """Refute a membership event about ourselves."""
        # Increment our incarnation and announce
        self.own_info.incarnation += 1

        refute_event = MembershipEvent(
            event_id=f"refute_{self.node_id}_{int(time.time())}",
            event_type=MembershipEventType.NODE_CONFIRMED,
            node_id=self.node_id,
            incarnation=self.own_info.incarnation,
            source_node=self.node_id,
            reason="refutation",
        )

        self.membership_events.append(refute_event)

        if self.gossip_protocol:
            await self._propagate_membership_event(refute_event)

        self.membership_stats["refutations_sent"] += 1
        logger.info(f"Refuted membership event for node {self.node_id}")

    def get_membership_list(self) -> list[MembershipInfo]:
        """Get current membership list."""
        with self._lock:
            return list(self.membership.values())

    def get_alive_nodes(self) -> list[str]:
        """Get list of alive node IDs."""
        with self._lock:
            return [
                node_id for node_id, info in self.membership.items() if info.is_alive()
            ]

    def get_suspect_nodes(self) -> list[str]:
        """Get list of suspect node IDs."""
        with self._lock:
            return [
                node_id
                for node_id, info in self.membership.items()
                if info.is_suspect()
            ]

    def get_failed_nodes(self) -> list[str]:
        """Get list of failed node IDs."""
        with self._lock:
            return [
                node_id for node_id, info in self.membership.items() if info.is_failed()
            ]

    def get_membership_statistics(self) -> MembershipStatistics:
        """Get comprehensive membership statistics."""
        with self._lock:
            alive_nodes = self.get_alive_nodes()
            suspect_nodes = self.get_suspect_nodes()
            failed_nodes = self.get_failed_nodes()

            return MembershipStatistics(
                membership_info={
                    "node_id": self.node_id,
                    "incarnation": self.own_info.incarnation,
                    "probe_interval": self.probe_interval,
                    "probe_timeout": self.probe_timeout,
                    "suspicion_timeout": self.suspicion_timeout,
                },
                membership_counts={
                    "total_nodes": len(self.membership),
                    "alive_nodes": len(alive_nodes),
                    "suspect_nodes": len(suspect_nodes),
                    "failed_nodes": len(failed_nodes),
                },
                membership_stats=dict(self.membership_stats),
                probe_stats=dict(self.probe_stats),
                active_probes=len(self.pending_probes),
                recent_events=len(self.membership_events),
                probe_history_size=len(self.probe_history),
            )


# Note: MEMBERSHIP_PROBE, MEMBERSHIP_ACK, and MEMBERSHIP_INDIRECT_PROBE are now defined in GossipMessageType enum
