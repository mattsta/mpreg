import pytest

from mpreg.core.message_queue import DeliveryGuarantee, QueueConfiguration
from mpreg.core.message_queue_manager import (
    MessageQueueManager,
    QueueManagerConfiguration,
)
from mpreg.fabric.catalog import QueueEndpoint, QueueHealth
from mpreg.fabric.cluster_messenger import ClusterMessenger
from mpreg.fabric.index import RoutingIndex
from mpreg.fabric.message import DeliveryGuarantee as FabricDeliveryGuarantee
from mpreg.fabric.message import MessageHeaders, MessageType, UnifiedMessage
from mpreg.fabric.queue_federation import FabricQueueFederationManager
from mpreg.fabric.queue_messages import QueueFederationAck


class DummyTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[str, UnifiedMessage]] = []

    async def send_message(self, peer_id: str, message: UnifiedMessage) -> bool:
        self.sent.append((peer_id, message))
        return True


class DummyAnnouncer:
    def __init__(self, index: RoutingIndex) -> None:
        self.index = index

    async def advertise(
        self, endpoint: QueueEndpoint, *, now: float | None = None
    ) -> None:
        self.index.catalog.queues.register(endpoint, now=now or endpoint.advertised_at)


def build_manager() -> tuple[FabricQueueFederationManager, DummyTransport]:
    index = RoutingIndex()
    transport = DummyTransport()
    messenger = ClusterMessenger(
        cluster_id="cluster-a",
        node_id="node-a",
        transport=transport,
        peer_locator=lambda _: ("peer-1",),
        max_hops=3,
    )
    queue_manager = MessageQueueManager(QueueManagerConfiguration())
    announcer = DummyAnnouncer(index)
    manager = FabricQueueFederationManager(
        cluster_id="cluster-a",
        node_id="node-a",
        routing_index=index,
        queue_manager=queue_manager,
        queue_announcer=announcer,
        messenger=messenger,
    )
    return manager, transport


@pytest.fixture
async def queue_federation_manager() -> tuple[
    FabricQueueFederationManager, DummyTransport
]:
    manager, transport = build_manager()
    yield manager, transport
    await manager.queue_manager.shutdown()


@pytest.mark.asyncio
async def test_create_queue_advertises(
    queue_federation_manager: tuple[FabricQueueFederationManager, DummyTransport],
) -> None:
    manager, _ = queue_federation_manager
    created = await manager.create_queue(
        "test-queue", QueueConfiguration(name="test-queue")
    )
    assert created is True
    assert "test-queue" in manager.queue_manager.list_queues()
    assert manager.routing_index.catalog.queues.entry_count() > 0


@pytest.mark.asyncio
async def test_subscribe_globally_registers_subscription(
    queue_federation_manager: tuple[FabricQueueFederationManager, DummyTransport],
) -> None:
    manager, _ = queue_federation_manager
    await manager.create_queue("sub-queue")

    subscription_id = await manager.subscribe_globally(
        subscriber_id="subscriber-1",
        queue_pattern="sub-*",
        topic_pattern="events.*",
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    )

    assert subscription_id in manager.subscriptions
    assert manager.subscriptions[subscription_id].subscriber_id == "subscriber-1"


@pytest.mark.asyncio
async def test_discover_queues(
    queue_federation_manager: tuple[FabricQueueFederationManager, DummyTransport],
) -> None:
    manager, _ = queue_federation_manager
    await manager.create_queue("local-queue")
    manager.routing_index.catalog.queues.register(
        QueueEndpoint(
            queue_name="remote-queue",
            cluster_id="cluster-b",
            node_id="node-b",
            delivery_guarantees=frozenset({FabricDeliveryGuarantee.AT_LEAST_ONCE}),
            health=QueueHealth.HEALTHY,
        ),
        now=100.0,
    )

    discovered = await manager.discover_queues()
    assert "cluster-a" in discovered
    assert "cluster-b" in discovered
    assert any(entry.queue_name == "local-queue" for entry in discovered["cluster-a"])
    assert any(entry.queue_name == "remote-queue" for entry in discovered["cluster-b"])


@pytest.mark.asyncio
async def test_send_message_globally_local(
    queue_federation_manager: tuple[FabricQueueFederationManager, DummyTransport],
) -> None:
    manager, transport = queue_federation_manager
    await manager.create_queue("local-target")

    result = await manager.send_message_globally(
        queue_name="local-target",
        topic="local.test",
        payload={"value": "local"},
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
    )

    assert result.success is True
    assert result.message_id is not None
    assert transport.sent == []


@pytest.mark.asyncio
async def test_send_message_globally_remote(
    queue_federation_manager: tuple[FabricQueueFederationManager, DummyTransport],
) -> None:
    manager, transport = queue_federation_manager
    manager.routing_index.catalog.queues.register(
        QueueEndpoint(
            queue_name="remote-target",
            cluster_id="cluster-b",
            node_id="node-b",
            delivery_guarantees=frozenset({FabricDeliveryGuarantee.AT_LEAST_ONCE}),
            health=QueueHealth.HEALTHY,
        ),
        now=100.0,
    )

    result = await manager.send_message_globally(
        queue_name="remote-target",
        topic="remote.test",
        payload={"value": "remote"},
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        target_cluster="cluster-b",
    )

    assert result.success is True
    assert result.message_id is not None
    assert transport.sent


@pytest.mark.asyncio
async def test_remote_ack_clears_in_flight(
    queue_federation_manager: tuple[FabricQueueFederationManager, DummyTransport],
) -> None:
    manager, _ = queue_federation_manager
    manager.routing_index.catalog.queues.register(
        QueueEndpoint(
            queue_name="remote-target",
            cluster_id="cluster-b",
            node_id="node-b",
            delivery_guarantees=frozenset({FabricDeliveryGuarantee.AT_LEAST_ONCE}),
            health=QueueHealth.HEALTHY,
        ),
        now=100.0,
    )

    result = await manager.send_message_globally(
        queue_name="remote-target",
        topic="ack.test",
        payload={"value": "ack"},
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        target_cluster="cluster-b",
        required_cluster_acks=1,
    )
    assert result.success is True
    assert manager.in_flight
    ack_token = next(iter(manager.in_flight.keys()))

    ack = QueueFederationAck(
        ack_token=ack_token,
        message_id=ack_token,
        acknowledging_cluster="cluster-b",
        acknowledging_subscriber="",
        success=True,
        error_message=None,
    )
    headers = MessageHeaders(
        correlation_id=ack_token,
        source_cluster="cluster-b",
        target_cluster="cluster-a",
    )
    message = UnifiedMessage(
        message_id=ack_token,
        topic="mpreg.queue.ack.remote-target",
        message_type=MessageType.QUEUE,
        delivery=FabricDeliveryGuarantee.AT_LEAST_ONCE,
        payload=ack.to_dict(),
        headers=headers,
    )

    await manager.handle_fabric_message(message)
    assert not manager.in_flight
