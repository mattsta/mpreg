from unittest.mock import AsyncMock, patch

import pytest

from mpreg.core.message_queue import DeliveryResult
from mpreg.datastructures.message_structures import MessageId
from mpreg.fabric.cluster_messenger import ClusterMessenger
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    UnifiedMessage,
)
from mpreg.fabric.queue_delivery import (
    ConsensusClusterWeight,
    FabricQueueDeliveryCoordinator,
    QueueConsensusOutcome,
    QueueConsensusRound,
    QueueConsensusVote,
)


class DummyTransport:
    async def send_message(self, peer_id: str, message: UnifiedMessage) -> bool:
        return True


class DummyQueueManager:
    def __init__(self, queues: set[str]) -> None:
        self._queues = queues

    def list_queues(self) -> list[str]:
        return list(self._queues)


class DummyQueueFederation:
    def __init__(self) -> None:
        self.queue_manager = DummyQueueManager({"consensus-queue"})
        self.messenger = ClusterMessenger(
            cluster_id="cluster-a",
            node_id="node-a",
            transport=DummyTransport(),
            peer_locator=lambda _: ("peer-1",),
        )
        self.send_message_globally = AsyncMock(
            return_value=DeliveryResult(
                success=True,
                message_id=MessageId.from_string("dispatch"),
            )
        )


def test_cluster_weight_calculations() -> None:
    trusted = ConsensusClusterWeight(
        cluster_id="trusted",
        weight=2.0,
        reliability_score=0.9,
        is_trusted=True,
    )
    assert trusted.effective_weight() == 1.8

    untrusted = ConsensusClusterWeight(
        cluster_id="untrusted",
        weight=2.0,
        reliability_score=0.9,
        is_trusted=False,
    )
    assert untrusted.effective_weight() == 0.0


def test_global_consensus_round_quorum() -> None:
    consensus_round = QueueConsensusRound(
        cluster_weights={
            "cluster-1": ConsensusClusterWeight(cluster_id="cluster-1", weight=1.0),
            "cluster-2": ConsensusClusterWeight(cluster_id="cluster-2", weight=1.0),
            "cluster-3": ConsensusClusterWeight(cluster_id="cluster-3", weight=1.0),
        },
        required_weight_threshold=0.67,
    )
    assert not consensus_round.has_quorum()

    consensus_round.votes_received["cluster-1"] = True
    assert not consensus_round.has_quorum()

    consensus_round.votes_received["cluster-2"] = True
    assert consensus_round.has_quorum()


def test_byzantine_fault_detection() -> None:
    consensus_round = QueueConsensusRound()
    consensus_round.record_vote(
        voter="byzantine",
        vote=True,
        response_payload={"value": "A"},
    )
    consensus_round.record_vote(
        voter="byzantine",
        vote=False,
        response_payload={"value": "B"},
    )
    byzantine = consensus_round.detect_byzantine_faults()
    assert "byzantine" in byzantine


@pytest.mark.asyncio
async def test_deliver_with_global_quorum() -> None:
    federation = DummyQueueFederation()
    coordinator = FabricQueueDeliveryCoordinator(
        cluster_id="cluster-a",
        queue_federation=federation,
    )

    approved = {"cluster-a", "cluster-b"}
    with patch.object(
        coordinator,
        "_wait_for_consensus",
        return_value=QueueConsensusOutcome(success=True, approved_clusters=approved),
    ):
        result = await coordinator.deliver_with_global_quorum(
            queue_name="consensus-queue",
            topic="consensus.test",
            payload={"value": "data"},
            target_clusters=approved,
        )

    assert result.success is True
    assert federation.send_message_globally.await_count == 2
    assert coordinator.delivery_stats.global_consensus_rounds == 1
    assert coordinator.delivery_stats.successful_global_consensus == 1


@pytest.mark.asyncio
async def test_handle_consensus_vote() -> None:
    federation = DummyQueueFederation()
    coordinator = FabricQueueDeliveryCoordinator(
        cluster_id="cluster-a",
        queue_federation=federation,
    )
    consensus_round = QueueConsensusRound(
        consensus_id="consensus-1",
        cluster_weights={"cluster-b": ConsensusClusterWeight(cluster_id="cluster-b")},
    )
    coordinator.active_consensus_rounds["consensus-1"] = consensus_round

    vote = QueueConsensusVote(
        consensus_id="consensus-1",
        voter_cluster="cluster-b",
        vote=True,
    )
    headers = MessageHeaders(
        correlation_id="consensus-1",
        source_cluster="cluster-b",
        target_cluster="cluster-a",
    )
    message = UnifiedMessage(
        message_id="consensus-1",
        topic="mpreg.queue.consensus.vote",
        message_type=MessageType.CONTROL,
        delivery=DeliveryGuarantee.AT_LEAST_ONCE,
        payload=vote.to_dict(),
        headers=headers,
    )

    await coordinator.handle_control_message(message)

    assert consensus_round.votes_received["cluster-b"] is True
    assert consensus_round.has_quorum()
