"""Consensus-aware queue delivery over the unified fabric."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from mpreg.core.message_queue import DeliveryGuarantee as QueueDeliveryGuarantee
from mpreg.core.task_manager import ManagedObject
from mpreg.datastructures.type_aliases import ClusterId, QueueName, Timestamp
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.fabric.cluster_messenger import ClusterMessenger
from mpreg.fabric.message import (
    DeliveryGuarantee as FabricDeliveryGuarantee,
)
from mpreg.fabric.message import (
    MessageHeaders,
    MessageType,
    UnifiedMessage,
)
from mpreg.fabric.queue_messages import QueueMessageOptions

from .queue_federation import FabricQueueFederationManager

fabric_queue_log = logger

QueueConsensusId = str
QueueDispatchId = str
ClusterWeightValue = float
ByzantineFaultThreshold = int

QUEUE_CONSENSUS_TOPIC_PREFIX = "mpreg.queue.consensus."
QUEUE_CONSENSUS_REQUEST_TOPIC = "mpreg.queue.consensus.request"
QUEUE_CONSENSUS_VOTE_TOPIC = "mpreg.queue.consensus.vote"


class QueueConsensusKind(Enum):
    REQUEST = "queue_consensus_request"
    VOTE = "queue_consensus_vote"


class QueueConsensusStatus(Enum):
    INITIATED = "initiated"
    IN_PROGRESS = "in_progress"
    QUORUM_REACHED = "quorum_reached"
    CONSENSUS_ACHIEVED = "consensus_achieved"
    CONSENSUS_FAILED = "consensus_failed"
    BYZANTINE_DETECTED = "byzantine_detected"
    TIMEOUT_EXCEEDED = "timeout_exceeded"


@dataclass(frozen=True, slots=True)
class QueueDispatchRequest:
    dispatch_id: QueueDispatchId = field(default_factory=lambda: str(uuid.uuid4()))
    queue_name: QueueName = ""
    topic: str = ""
    payload: Any = None
    delivery_guarantee: QueueDeliveryGuarantee = QueueDeliveryGuarantee.AT_LEAST_ONCE
    options: QueueMessageOptions = field(default_factory=QueueMessageOptions)
    created_at: Timestamp = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dispatch_id": self.dispatch_id,
            "queue_name": self.queue_name,
            "topic": self.topic,
            "payload": self.payload,
            "delivery_guarantee": self.delivery_guarantee.value,
            "options": self.options.to_dict(),
            "created_at": float(self.created_at),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> QueueDispatchRequest:
        return cls(
            dispatch_id=str(payload.get("dispatch_id", "")),
            queue_name=str(payload.get("queue_name", "")),
            topic=str(payload.get("topic", "")),
            payload=payload.get("payload"),
            delivery_guarantee=QueueDeliveryGuarantee(
                payload.get(
                    "delivery_guarantee", QueueDeliveryGuarantee.AT_LEAST_ONCE.value
                )
            ),
            options=QueueMessageOptions.from_dict(
                payload.get("options", {})
                if isinstance(payload.get("options"), dict)
                else {}
            ),
            created_at=float(payload.get("created_at", time.time())),
        )


@dataclass(frozen=True, slots=True)
class QueueConsensusRequest:
    consensus_id: QueueConsensusId
    dispatch_id: QueueDispatchId
    queue_name: QueueName
    topic: str
    delivery_guarantee: QueueDeliveryGuarantee
    source_cluster: ClusterId
    target_cluster: ClusterId
    required_weight_threshold: ClusterWeightValue
    byzantine_fault_threshold: ByzantineFaultThreshold
    timestamp: Timestamp = field(default_factory=time.time)
    payload_digest: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": QueueConsensusKind.REQUEST.value,
            "consensus_id": self.consensus_id,
            "dispatch_id": self.dispatch_id,
            "queue_name": self.queue_name,
            "topic": self.topic,
            "delivery_guarantee": self.delivery_guarantee.value,
            "source_cluster": self.source_cluster,
            "target_cluster": self.target_cluster,
            "required_weight_threshold": self.required_weight_threshold,
            "byzantine_fault_threshold": self.byzantine_fault_threshold,
            "timestamp": float(self.timestamp),
            "payload_digest": self.payload_digest,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> QueueConsensusRequest:
        return cls(
            consensus_id=str(payload.get("consensus_id", "")),
            dispatch_id=str(payload.get("dispatch_id", "")),
            queue_name=str(payload.get("queue_name", "")),
            topic=str(payload.get("topic", "")),
            delivery_guarantee=QueueDeliveryGuarantee(
                payload.get(
                    "delivery_guarantee", QueueDeliveryGuarantee.AT_LEAST_ONCE.value
                )
            ),
            source_cluster=str(payload.get("source_cluster", "")),
            target_cluster=str(payload.get("target_cluster", "")),
            required_weight_threshold=float(
                payload.get("required_weight_threshold", 0.5)
            ),
            byzantine_fault_threshold=int(payload.get("byzantine_fault_threshold", 1)),
            timestamp=float(payload.get("timestamp", time.time())),
            payload_digest=payload.get("payload_digest"),
        )


@dataclass(frozen=True, slots=True)
class QueueConsensusVote:
    consensus_id: QueueConsensusId
    voter_cluster: ClusterId
    vote: bool
    response_payload: Any = None
    timestamp: Timestamp = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": QueueConsensusKind.VOTE.value,
            "consensus_id": self.consensus_id,
            "voter_cluster": self.voter_cluster,
            "vote": self.vote,
            "response_payload": self.response_payload,
            "timestamp": float(self.timestamp),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> QueueConsensusVote:
        return cls(
            consensus_id=str(payload.get("consensus_id", "")),
            voter_cluster=str(payload.get("voter_cluster", "")),
            vote=bool(payload.get("vote", False)),
            response_payload=payload.get("response_payload"),
            timestamp=float(payload.get("timestamp", time.time())),
        )


QueueConsensusMessage = QueueConsensusRequest | QueueConsensusVote


def queue_consensus_message_from_dict(payload: dict[str, Any]) -> QueueConsensusMessage:
    kind = payload.get("kind")
    if kind == QueueConsensusKind.REQUEST.value:
        return QueueConsensusRequest.from_dict(payload)
    if kind == QueueConsensusKind.VOTE.value:
        return QueueConsensusVote.from_dict(payload)
    raise ValueError(f"Unknown queue consensus kind: {kind}")


@dataclass(slots=True)
class ConsensusClusterWeight:
    cluster_id: ClusterId
    weight: ClusterWeightValue = 1.0
    reliability_score: float = 1.0
    latency_ms: float = 0.0
    last_successful_ack: Timestamp = field(default_factory=time.time)
    byzantine_fault_count: int = 0
    is_trusted: bool = True

    def effective_weight(self) -> ClusterWeightValue:
        if not self.is_trusted:
            return 0.0
        return self.weight * self.reliability_score


@dataclass(slots=True)
class QueueConsensusRound:
    consensus_id: QueueConsensusId = field(default_factory=lambda: str(uuid.uuid4()))
    dispatch: QueueDispatchRequest = field(default_factory=QueueDispatchRequest)
    initiating_cluster: ClusterId = ""
    target_clusters: set[ClusterId] = field(default_factory=set)
    cluster_weights: dict[ClusterId, ConsensusClusterWeight] = field(
        default_factory=dict
    )
    required_weight_threshold: ClusterWeightValue = 0.5
    byzantine_fault_threshold: ByzantineFaultThreshold = 1

    votes_received: dict[ClusterId, bool] = field(default_factory=dict)
    vector_clocks: dict[ClusterId, VectorClock] = field(default_factory=dict)
    consensus_status: QueueConsensusStatus = QueueConsensusStatus.INITIATED
    start_time: Timestamp = field(default_factory=time.time)
    timeout_seconds: float = 120.0

    conflicting_responses: dict[ClusterId, list[Any]] = field(
        default_factory=lambda: defaultdict(list)
    )
    suspected_byzantine_clusters: set[ClusterId] = field(default_factory=set)

    def total_weight(self) -> ClusterWeightValue:
        return sum(
            weight.effective_weight() for weight in self.cluster_weights.values()
        )

    def current_weight(self) -> ClusterWeightValue:
        return sum(
            self.cluster_weights[cluster].effective_weight()
            for cluster, vote in self.votes_received.items()
            if vote
        )

    def has_quorum(self) -> bool:
        total = self.total_weight()
        if total == 0:
            return False
        current_percentage = self.current_weight() / total
        epsilon = 0.01
        return current_percentage >= (self.required_weight_threshold - epsilon)

    def is_timeout_exceeded(self) -> bool:
        return (time.time() - self.start_time) > self.timeout_seconds

    def record_vote(
        self,
        *,
        voter: ClusterId,
        vote: bool,
        response_payload: Any = None,
        vector_clock: VectorClock | None = None,
    ) -> None:
        self.votes_received[voter] = vote
        self.vector_clocks[voter] = (
            vector_clock.copy() if vector_clock else VectorClock.empty()
        )

        if response_payload is None:
            return

        responses = self.conflicting_responses.setdefault(voter, [])
        if response_payload not in responses:
            responses.append(response_payload)

    def detect_byzantine_faults(self) -> set[ClusterId]:
        byzantine_clusters = set()
        for cluster_id, responses in self.conflicting_responses.items():
            if len(responses) > 1:
                byzantine_clusters.add(cluster_id)
                fabric_queue_log.warning(
                    "Byzantine fault detected: cluster {} sent {} conflicting responses",
                    cluster_id,
                    len(responses),
                )
        return byzantine_clusters


@dataclass(frozen=True, slots=True)
class QueueCausalOrder:
    dispatch_id: QueueDispatchId
    vector_clock: VectorClock
    causal_dependencies: set[str] = field(default_factory=set)
    delivery_timestamp: Timestamp = field(default_factory=time.time)
    cluster_id: ClusterId = ""

    def can_deliver_after(self, other: QueueCausalOrder) -> bool:
        for cluster in other.vector_clock.node_ids():
            clock = other.vector_clock.get_timestamp(cluster)
            if cluster in self.vector_clock:
                if self.vector_clock.get_timestamp(cluster) < clock:
                    return False
        if other.dispatch_id in self.causal_dependencies:
            return True
        return True


@dataclass(slots=True)
class QueueDeliveryStatistics:
    global_consensus_rounds: int = 0
    successful_global_consensus: int = 0
    failed_global_consensus: int = 0
    byzantine_faults_detected: int = 0
    average_consensus_time_ms: float = 0.0
    federated_broadcast_deliveries: int = 0
    causal_order_violations: int = 0
    last_consensus_activity: Timestamp = field(default_factory=time.time)

    def consensus_success_rate(self) -> float:
        total = self.successful_global_consensus + self.failed_global_consensus
        return self.successful_global_consensus / total if total > 0 else 0.0


@dataclass(frozen=True, slots=True)
class FabricQueueDeliveryResult:
    success: bool
    dispatch_id: QueueDispatchId
    consensus_id: QueueConsensusId | None = None
    delivered_clusters: set[ClusterId] = field(default_factory=set)
    failed_clusters: set[ClusterId] = field(default_factory=set)
    error_message: str | None = None
    delivery_timestamp: Timestamp = field(default_factory=time.time)


@dataclass(frozen=True, slots=True)
class QueueConsensusOutcome:
    success: bool
    approved_clusters: set[ClusterId] = field(default_factory=set)
    byzantine_clusters: set[ClusterId] = field(default_factory=set)
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class QueueCausalDelivery:
    dispatch: QueueDispatchRequest
    causal_order: QueueCausalOrder
    target_clusters: set[ClusterId]


@dataclass(slots=True)
class FabricQueueDeliveryCoordinator(ManagedObject):
    """Consensus-aware coordinator for fabric queue delivery."""

    cluster_id: ClusterId
    queue_federation: FabricQueueFederationManager
    messenger: ClusterMessenger | None = None
    allowed_clusters: frozenset[ClusterId] | None = None
    vote_decider: Callable[[QueueConsensusRequest], bool] | None = None
    default_quorum_threshold: ClusterWeightValue = 0.67
    default_byzantine_threshold: ByzantineFaultThreshold = 1
    default_timeout_seconds: float = 120.0

    cluster_weights: dict[ClusterId, ConsensusClusterWeight] = field(
        default_factory=dict
    )
    active_consensus_rounds: dict[QueueConsensusId, QueueConsensusRound] = field(
        default_factory=dict
    )
    delivery_stats: QueueDeliveryStatistics = field(
        default_factory=QueueDeliveryStatistics
    )
    causal_buffer: dict[QueueDispatchId, QueueCausalDelivery] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        ManagedObject.__init__(self, name="FabricQueueDeliveryCoordinator")
        if self.messenger is None:
            self.messenger = self.queue_federation.messenger
        if self.vote_decider is None:
            self.vote_decider = self._default_vote_decider

    async def deliver_with_global_quorum(
        self,
        *,
        queue_name: QueueName,
        topic: str,
        payload: Any,
        target_clusters: set[ClusterId],
        delivery_guarantee: QueueDeliveryGuarantee = QueueDeliveryGuarantee.AT_LEAST_ONCE,
        options: QueueMessageOptions | None = None,
        required_weight_threshold: ClusterWeightValue | None = None,
        byzantine_fault_threshold: ByzantineFaultThreshold | None = None,
        timeout_seconds: float | None = None,
    ) -> FabricQueueDeliveryResult:
        dispatch = QueueDispatchRequest(
            queue_name=queue_name,
            topic=topic,
            payload=payload,
            delivery_guarantee=delivery_guarantee,
            options=options or QueueMessageOptions(),
        )
        if not target_clusters:
            return FabricQueueDeliveryResult(
                success=False,
                dispatch_id=dispatch.dispatch_id,
                delivered_clusters=set(),
                failed_clusters=set(),
                error_message="no_target_clusters",
            )
        if self.allowed_clusters is not None:
            target_clusters = {
                cluster_id
                for cluster_id in target_clusters
                if cluster_id in self.allowed_clusters
            }
        if not target_clusters:
            return FabricQueueDeliveryResult(
                success=False,
                dispatch_id=dispatch.dispatch_id,
                consensus_id=None,
                delivered_clusters=set(),
                failed_clusters=set(),
                error_message="no_target_clusters",
            )
        await self._initialize_cluster_weights(target_clusters)

        consensus_round = QueueConsensusRound(
            dispatch=dispatch,
            initiating_cluster=self.cluster_id,
            target_clusters=set(target_clusters),
            cluster_weights={
                cluster_id: self.cluster_weights.get(
                    cluster_id, ConsensusClusterWeight(cluster_id=cluster_id)
                )
                for cluster_id in target_clusters
            },
            required_weight_threshold=required_weight_threshold
            if required_weight_threshold is not None
            else self.default_quorum_threshold,
            byzantine_fault_threshold=byzantine_fault_threshold
            if byzantine_fault_threshold is not None
            else self.default_byzantine_threshold,
            timeout_seconds=timeout_seconds
            if timeout_seconds is not None
            else self.default_timeout_seconds,
        )
        self.active_consensus_rounds[consensus_round.consensus_id] = consensus_round

        try:
            await self._initiate_global_consensus(consensus_round)
            outcome = await self._wait_for_consensus(consensus_round)

            self.delivery_stats.global_consensus_rounds += 1
            if outcome.success:
                self.delivery_stats.successful_global_consensus += 1
            else:
                self.delivery_stats.failed_global_consensus += 1

            if not outcome.success:
                return FabricQueueDeliveryResult(
                    success=False,
                    dispatch_id=dispatch.dispatch_id,
                    consensus_id=consensus_round.consensus_id,
                    delivered_clusters=set(),
                    failed_clusters=set(target_clusters),
                    error_message=outcome.error_message or "consensus_failed",
                )

            delivered, failed = await self._deliver_dispatch_to_clusters(
                dispatch, outcome.approved_clusters
            )
            success = len(failed) == 0 and len(delivered) > 0
            return FabricQueueDeliveryResult(
                success=success,
                dispatch_id=dispatch.dispatch_id,
                consensus_id=consensus_round.consensus_id,
                delivered_clusters=delivered,
                failed_clusters=failed,
                error_message=None if success else "delivery_failed",
            )
        finally:
            self.active_consensus_rounds.pop(consensus_round.consensus_id, None)

    async def deliver_with_causal_consistency(
        self,
        *,
        queue_name: QueueName,
        topic: str,
        payload: Any,
        vector_clock: VectorClock,
        causal_dependencies: set[str],
        target_clusters: set[ClusterId],
        delivery_guarantee: QueueDeliveryGuarantee = QueueDeliveryGuarantee.AT_LEAST_ONCE,
        options: QueueMessageOptions | None = None,
    ) -> FabricQueueDeliveryResult:
        if self.allowed_clusters is not None:
            target_clusters = {
                cluster_id
                for cluster_id in target_clusters
                if cluster_id in self.allowed_clusters
            }
        dispatch = QueueDispatchRequest(
            queue_name=queue_name,
            topic=topic,
            payload=payload,
            delivery_guarantee=delivery_guarantee,
            options=options or QueueMessageOptions(),
        )
        if not target_clusters:
            return FabricQueueDeliveryResult(
                success=False,
                dispatch_id=dispatch.dispatch_id,
                delivered_clusters=set(),
                failed_clusters=set(),
                error_message="no_target_clusters",
            )
        causal_order = QueueCausalOrder(
            dispatch_id=dispatch.dispatch_id,
            vector_clock=vector_clock.copy(),
            causal_dependencies=causal_dependencies.copy(),
            cluster_id=self.cluster_id,
        )
        if await self._can_deliver_causally(causal_order):
            delivered, failed = await self._deliver_dispatch_to_clusters(
                dispatch, target_clusters
            )
            return FabricQueueDeliveryResult(
                success=len(delivered) > 0 and len(failed) == 0,
                dispatch_id=dispatch.dispatch_id,
                delivered_clusters=delivered,
                failed_clusters=failed,
            )

        self.causal_buffer[dispatch.dispatch_id] = QueueCausalDelivery(
            dispatch=dispatch,
            causal_order=causal_order,
            target_clusters=set(target_clusters),
        )
        return FabricQueueDeliveryResult(
            success=True,
            dispatch_id=dispatch.dispatch_id,
            delivered_clusters=set(),
            failed_clusters=set(),
        )

    async def deliver_with_federated_broadcast(
        self,
        *,
        queue_name: QueueName,
        topic: str,
        payload: Any,
        target_clusters: set[ClusterId],
        delivery_guarantee: QueueDeliveryGuarantee = QueueDeliveryGuarantee.BROADCAST,
        options: QueueMessageOptions | None = None,
        require_all_clusters: bool = False,
    ) -> FabricQueueDeliveryResult:
        if self.allowed_clusters is not None:
            target_clusters = {
                cluster_id
                for cluster_id in target_clusters
                if cluster_id in self.allowed_clusters
            }
        dispatch = QueueDispatchRequest(
            queue_name=queue_name,
            topic=topic,
            payload=payload,
            delivery_guarantee=delivery_guarantee,
            options=options or QueueMessageOptions(),
        )
        delivered, failed = await self._deliver_dispatch_to_clusters(
            dispatch, target_clusters
        )
        if require_all_clusters:
            success = len(failed) == 0
        else:
            success = len(delivered) >= max(1, len(target_clusters) // 2)

        self.delivery_stats.federated_broadcast_deliveries += 1

        return FabricQueueDeliveryResult(
            success=success,
            dispatch_id=dispatch.dispatch_id,
            delivered_clusters=delivered,
            failed_clusters=failed,
        )

    async def handle_control_message(
        self,
        message: UnifiedMessage,
        *,
        source_peer_url: str | None = None,
    ) -> None:
        if message.message_type is not MessageType.CONTROL:
            return
        if not message.topic.startswith(QUEUE_CONSENSUS_TOPIC_PREFIX):
            return

        if (
            self.allowed_clusters is not None
            and message.headers.source_cluster
            and message.headers.source_cluster not in self.allowed_clusters
        ):
            fabric_queue_log.warning(
                "[{}] Rejecting queue consensus message from disallowed cluster {}",
                self.cluster_id,
                message.headers.source_cluster,
            )
            return

        target_cluster = message.headers.target_cluster
        if target_cluster and target_cluster != self.cluster_id:
            await self._forward_control_message(
                message, target_cluster, source_peer_url
            )
            return

        if not isinstance(message.payload, dict):
            fabric_queue_log.warning(
                "[{}] Invalid queue consensus payload type: {}",
                self.cluster_id,
                type(message.payload),
            )
            return

        try:
            payload = queue_consensus_message_from_dict(message.payload)
        except Exception as exc:
            fabric_queue_log.warning(
                "[{}] Invalid queue consensus payload: {}",
                self.cluster_id,
                exc,
            )
            return

        if isinstance(payload, QueueConsensusRequest):
            await self._handle_consensus_request(payload)
            return

        if isinstance(payload, QueueConsensusVote):
            await self._handle_consensus_vote(payload)
            return

    async def _handle_consensus_request(self, request: QueueConsensusRequest) -> None:
        if request.target_cluster != self.cluster_id:
            return

        vote = True if self.vote_decider is None else self.vote_decider(request)

        vote_payload = QueueConsensusVote(
            consensus_id=request.consensus_id,
            voter_cluster=self.cluster_id,
            vote=vote,
            response_payload={"queue": request.queue_name, "vote": vote},
        )
        await self._send_consensus_vote(
            vote_payload, target_cluster=request.source_cluster
        )

    async def _handle_consensus_vote(self, vote: QueueConsensusVote) -> None:
        consensus_round = self.active_consensus_rounds.get(vote.consensus_id)
        if not consensus_round:
            fabric_queue_log.debug(
                "[{}] Ignoring consensus vote for unknown round {}",
                self.cluster_id,
                vote.consensus_id,
            )
            return

        consensus_round.record_vote(
            voter=vote.voter_cluster,
            vote=vote.vote,
            response_payload=vote.response_payload,
        )

        if consensus_round.has_quorum():
            consensus_round.consensus_status = QueueConsensusStatus.QUORUM_REACHED

    async def _initialize_cluster_weights(
        self, target_clusters: set[ClusterId]
    ) -> None:
        for cluster_id in target_clusters:
            if cluster_id not in self.cluster_weights:
                self.cluster_weights[cluster_id] = ConsensusClusterWeight(
                    cluster_id=cluster_id,
                    weight=1.0,
                    reliability_score=1.0,
                    is_trusted=True,
                )

    async def _initiate_global_consensus(
        self, consensus_round: QueueConsensusRound
    ) -> None:
        try:
            consensus_round.consensus_status = QueueConsensusStatus.IN_PROGRESS

            if self.cluster_id in consensus_round.target_clusters:
                consensus_round.record_vote(
                    voter=self.cluster_id,
                    vote=True,
                    response_payload={"queue": consensus_round.dispatch.queue_name},
                )
                if consensus_round.has_quorum():
                    consensus_round.consensus_status = (
                        QueueConsensusStatus.QUORUM_REACHED
                    )

            for cluster_id in consensus_round.target_clusters:
                if cluster_id == self.cluster_id:
                    continue
                request = QueueConsensusRequest(
                    consensus_id=consensus_round.consensus_id,
                    dispatch_id=consensus_round.dispatch.dispatch_id,
                    queue_name=consensus_round.dispatch.queue_name,
                    topic=consensus_round.dispatch.topic,
                    delivery_guarantee=consensus_round.dispatch.delivery_guarantee,
                    source_cluster=self.cluster_id,
                    target_cluster=cluster_id,
                    required_weight_threshold=consensus_round.required_weight_threshold,
                    byzantine_fault_threshold=consensus_round.byzantine_fault_threshold,
                )
                await self._send_consensus_request(request)
        except Exception as exc:
            fabric_queue_log.error(
                "[{}] Failed to initiate queue consensus: {}",
                self.cluster_id,
                exc,
            )
            consensus_round.consensus_status = QueueConsensusStatus.CONSENSUS_FAILED

    async def _wait_for_consensus(
        self, consensus_round: QueueConsensusRound
    ) -> QueueConsensusOutcome:
        start_time = time.time()
        while not consensus_round.is_timeout_exceeded():
            if consensus_round.consensus_status == QueueConsensusStatus.QUORUM_REACHED:
                byzantine_clusters = consensus_round.detect_byzantine_faults()
                if byzantine_clusters:
                    consensus_round.suspected_byzantine_clusters.update(
                        byzantine_clusters
                    )
                    self.delivery_stats.byzantine_faults_detected += len(
                        byzantine_clusters
                    )
                    await self._handle_byzantine_faults(
                        consensus_round, byzantine_clusters
                    )
                if (
                    len(byzantine_clusters) > consensus_round.byzantine_fault_threshold
                    or not consensus_round.has_quorum()
                ):
                    consensus_round.consensus_status = (
                        QueueConsensusStatus.CONSENSUS_FAILED
                    )
                    return QueueConsensusOutcome(
                        success=False,
                        byzantine_clusters=byzantine_clusters,
                        error_message="byzantine_faults_detected",
                    )

                consensus_round.consensus_status = (
                    QueueConsensusStatus.CONSENSUS_ACHIEVED
                )
                consensus_time = (time.time() - start_time) * 1000
                self.delivery_stats.average_consensus_time_ms = (
                    self.delivery_stats.average_consensus_time_ms + consensus_time
                ) / 2
                approved_clusters = {
                    cluster
                    for cluster, vote in consensus_round.votes_received.items()
                    if vote
                }
                return QueueConsensusOutcome(
                    success=True,
                    approved_clusters=approved_clusters,
                    byzantine_clusters=byzantine_clusters,
                )

            if (
                consensus_round.consensus_status
                == QueueConsensusStatus.CONSENSUS_FAILED
            ):
                return QueueConsensusOutcome(
                    success=False,
                    error_message="consensus_failed",
                )

            await asyncio.sleep(0.05)

        consensus_round.consensus_status = QueueConsensusStatus.TIMEOUT_EXCEEDED
        return QueueConsensusOutcome(
            success=False,
            error_message="consensus_timeout",
        )

    async def _send_consensus_request(self, request: QueueConsensusRequest) -> None:
        headers = self._next_headers(
            request.consensus_id,
            None,
            target_cluster=request.target_cluster,
        )
        if headers is None:
            return
        message = UnifiedMessage(
            message_id=request.consensus_id,
            topic=QUEUE_CONSENSUS_REQUEST_TOPIC,
            message_type=MessageType.CONTROL,
            delivery=FabricDeliveryGuarantee.AT_LEAST_ONCE,
            payload=request.to_dict(),
            headers=headers,
            timestamp=time.time(),
        )
        await self.messenger.send_to_cluster(message, request.target_cluster, None)

    async def _send_consensus_vote(
        self, vote: QueueConsensusVote, *, target_cluster: ClusterId
    ) -> None:
        headers = self._next_headers(
            vote.consensus_id,
            None,
            target_cluster=target_cluster,
        )
        if headers is None:
            return
        message = UnifiedMessage(
            message_id=vote.consensus_id,
            topic=QUEUE_CONSENSUS_VOTE_TOPIC,
            message_type=MessageType.CONTROL,
            delivery=FabricDeliveryGuarantee.AT_LEAST_ONCE,
            payload=vote.to_dict(),
            headers=headers,
            timestamp=time.time(),
        )
        await self.messenger.send_to_cluster(message, target_cluster, None)

    async def _forward_control_message(
        self,
        message: UnifiedMessage,
        target_cluster: ClusterId,
        source_peer_url: str | None,
    ) -> None:
        headers = self._next_headers(
            message.headers.correlation_id,
            message.headers,
            target_cluster=target_cluster,
        )
        if headers is None:
            return
        forward_message = UnifiedMessage(
            message_id=message.message_id,
            topic=message.topic,
            message_type=MessageType.CONTROL,
            delivery=message.delivery,
            payload=message.payload,
            headers=headers,
            timestamp=message.timestamp,
        )
        await self.messenger.send_to_cluster(
            forward_message, target_cluster, source_peer_url
        )

    def _next_headers(
        self,
        correlation_id: str,
        headers: MessageHeaders | None,
        *,
        target_cluster: ClusterId | None,
    ) -> MessageHeaders | None:
        return self.messenger.next_headers(
            correlation_id,
            headers,
            target_cluster=target_cluster,
        )

    async def _handle_byzantine_faults(
        self,
        consensus_round: QueueConsensusRound,
        byzantine_clusters: set[ClusterId],
    ) -> None:
        for cluster_id in byzantine_clusters:
            if cluster_id in self.cluster_weights:
                self.cluster_weights[cluster_id].is_trusted = False
                self.cluster_weights[cluster_id].byzantine_fault_count += 1
                if cluster_id in consensus_round.votes_received:
                    del consensus_round.votes_received[cluster_id]

    async def _can_deliver_causally(self, causal_order: QueueCausalOrder) -> bool:
        for dependency_id in causal_order.causal_dependencies:
            if dependency_id in self.causal_buffer:
                return False
        return True

    async def _deliver_dispatch_to_clusters(
        self, dispatch: QueueDispatchRequest, target_clusters: set[ClusterId]
    ) -> tuple[set[ClusterId], set[ClusterId]]:
        delivered: set[ClusterId] = set()
        failed: set[ClusterId] = set()

        async def _deliver(cluster_id: ClusterId) -> tuple[ClusterId, bool]:
            result = await self.queue_federation.send_message_globally(
                queue_name=dispatch.queue_name,
                topic=dispatch.topic,
                payload=dispatch.payload,
                delivery_guarantee=dispatch.delivery_guarantee,
                target_cluster=cluster_id,
                options=dispatch.options,
            )
            return cluster_id, result.success

        tasks = [
            asyncio.create_task(_deliver(cluster_id)) for cluster_id in target_clusters
        ]
        if tasks:
            for task in asyncio.as_completed(tasks):
                cluster_id, success = await task
                if success:
                    delivered.add(cluster_id)
                else:
                    failed.add(cluster_id)

        return delivered, failed

    def _default_vote_decider(self, request: QueueConsensusRequest) -> bool:
        if (
            self.allowed_clusters is not None
            and request.source_cluster not in self.allowed_clusters
        ):
            return False
        queue_names = self.queue_federation.queue_manager.list_queues()
        return request.queue_name in queue_names


def is_queue_consensus_message(message: UnifiedMessage) -> bool:
    return message.message_type is MessageType.CONTROL and message.topic.startswith(
        QUEUE_CONSENSUS_TOPIC_PREFIX
    )
