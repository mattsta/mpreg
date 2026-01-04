import pytest

from mpreg.fabric.cluster_messenger import ClusterMessenger
from mpreg.fabric.federation_planner import (
    FabricForwardingFailureReason,
    FabricForwardingPlan,
)
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    UnifiedMessage,
)


class DummyTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[str, UnifiedMessage]] = []

    async def send_message(self, peer_id: str, message: UnifiedMessage) -> bool:
        self.sent.append((peer_id, message))
        return True


class DummyPlanner:
    def __init__(self, next_peer: str | None) -> None:
        self.next_peer = next_peer

    def plan_next_hop(
        self,
        *,
        target_cluster: str | None,
        visited_clusters: tuple[str, ...] = (),
        remaining_hops: int | None = None,
    ) -> FabricForwardingPlan:
        return FabricForwardingPlan(
            target_cluster=target_cluster or "",
            next_cluster=target_cluster,
            next_peer_url=self.next_peer,
            planned_path=(target_cluster or "",),
            federation_path=visited_clusters,
            remaining_hops=remaining_hops or 0,
            reason=FabricForwardingFailureReason.OK,
        )


def test_next_headers_initial() -> None:
    transport = DummyTransport()
    messenger = ClusterMessenger(
        cluster_id="cluster-a",
        node_id="node-a",
        transport=transport,
        peer_locator=lambda _: (),
        max_hops=3,
    )

    headers = messenger.next_headers("corr-1", None, target_cluster="cluster-b")
    assert headers is not None
    assert headers.correlation_id == "corr-1"
    assert headers.source_cluster == "cluster-a"
    assert headers.target_cluster == "cluster-b"
    assert headers.routing_path == ("node-a",)
    assert headers.federation_path == ("cluster-a",)
    assert headers.hop_budget == 3


def test_next_headers_appends_paths() -> None:
    transport = DummyTransport()
    messenger = ClusterMessenger(
        cluster_id="cluster-a",
        node_id="node-a",
        transport=transport,
        peer_locator=lambda _: (),
        max_hops=2,
    )
    existing = MessageHeaders(
        correlation_id="corr-2",
        source_cluster="cluster-x",
        target_cluster="cluster-y",
        routing_path=("node-x",),
        federation_path=("cluster-x",),
        hop_budget=5,
    )

    headers = messenger.next_headers("corr-2", existing, target_cluster="cluster-b")
    assert headers is not None
    assert headers.routing_path == ("node-x", "node-a")
    assert headers.federation_path == ("cluster-x", "cluster-a")
    assert headers.hop_budget == 2
    assert headers.target_cluster == "cluster-b"


def test_next_headers_loop_detected() -> None:
    transport = DummyTransport()
    messenger = ClusterMessenger(
        cluster_id="cluster-a",
        node_id="node-a",
        transport=transport,
        peer_locator=lambda _: (),
    )
    existing = MessageHeaders(
        correlation_id="corr-loop",
        routing_path=("node-a",),
    )
    assert (
        messenger.next_headers("corr-loop", existing, target_cluster="cluster-b")
        is None
    )


def test_next_headers_hop_budget_exceeded() -> None:
    transport = DummyTransport()
    messenger = ClusterMessenger(
        cluster_id="cluster-a",
        node_id="node-a",
        transport=transport,
        peer_locator=lambda _: (),
        max_hops=1,
    )
    existing = MessageHeaders(
        correlation_id="corr-hop",
        routing_path=("node-1", "node-2", "node-3"),
        hop_budget=1,
    )
    assert (
        messenger.next_headers("corr-hop", existing, target_cluster="cluster-b") is None
    )


@pytest.mark.asyncio
async def test_send_to_cluster_with_planner() -> None:
    transport = DummyTransport()
    planner = DummyPlanner(next_peer="peer-planned")
    messenger = ClusterMessenger(
        cluster_id="cluster-a",
        node_id="node-a",
        transport=transport,
        peer_locator=lambda _: ("peer-fallback",),
        hop_planner=planner,
    )
    headers = MessageHeaders(correlation_id="corr", routing_path=(), federation_path=())
    message = UnifiedMessage(
        message_id="msg-1",
        topic="mpreg.control",
        message_type=MessageType.CONTROL,
        delivery=DeliveryGuarantee.AT_LEAST_ONCE,
        payload={"kind": "test"},
        headers=headers,
    )

    sent = await messenger.send_to_cluster(message, "cluster-b", None)
    assert sent is True
    assert transport.sent[0][0] == "peer-planned"


@pytest.mark.asyncio
async def test_send_to_cluster_fallback_peer_locator() -> None:
    transport = DummyTransport()
    messenger = ClusterMessenger(
        cluster_id="cluster-a",
        node_id="node-a",
        transport=transport,
        peer_locator=lambda _: ("peer-1", "peer-2"),
    )
    headers = MessageHeaders(correlation_id="corr", routing_path=(), federation_path=())
    message = UnifiedMessage(
        message_id="msg-2",
        topic="mpreg.control",
        message_type=MessageType.CONTROL,
        delivery=DeliveryGuarantee.AT_LEAST_ONCE,
        payload={"kind": "test"},
        headers=headers,
    )

    sent = await messenger.send_to_cluster(message, "cluster-b", None)
    assert sent is True
    assert transport.sent[0][0] == "peer-1"
