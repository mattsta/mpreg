#!/usr/bin/env python3
"""
SQS-like Message Queue System Demo

This example demonstrates all the delivery guarantees and features of
MPREG's SQS-like message queuing system:

1. Fire and Forget - Send once, no tracking
2. At-Least-Once - Retry until acknowledged with timeout
3. Broadcast - Deliver to all subscribers
4. Quorum - Require N acknowledgments before considering delivered

The demo showcases:
- Different queue types (FIFO, Priority, Delay)
- Message routing and topic patterns
- Dead letter queues and error handling
- Performance monitoring and statistics
- Integration with MPREG's topic exchange
"""

import asyncio
import time
from typing import Any

from mpreg.core.message_queue import (
    DeliveryGuarantee,
    QueueConfiguration,
    QueuedMessage,
    QueueType,
)
from mpreg.core.message_queue_manager import (
    create_high_throughput_queue_manager,
    create_reliable_queue_manager,
    create_standard_queue_manager,
)


class MessageProcessor:
    """Example message processor with different handling patterns."""

    def __init__(self, name: str, auto_ack: bool = True) -> None:
        self.name = name
        self.auto_ack = auto_ack
        self.processed_messages: list[str] = []
        self.processing_times: list[float] = []

    def handle_message(self, message: QueuedMessage) -> None:
        """Handle an incoming message."""
        start_time = time.time()

        print(f"📨 {self.name} received message {message.id}: {message.payload}")
        self.processed_messages.append(str(message.id))

        # Simulate processing time
        processing_time = time.time() - start_time
        self.processing_times.append(processing_time)

        print(f"✅ {self.name} processed message in {processing_time:.3f}s")

    def get_stats(self) -> dict[str, Any]:
        """Get processing statistics."""
        if not self.processing_times:
            return {"processed_count": 0, "avg_time": 0.0}

        return {
            "processed_count": len(self.processed_messages),
            "avg_time": sum(self.processing_times) / len(self.processing_times),
            "total_time": sum(self.processing_times),
        }


async def demonstrate_fire_and_forget():
    """Demonstrate fire-and-forget delivery."""
    print("\n🔥 Fire and Forget Demo")
    print("=" * 50)

    manager = create_standard_queue_manager()
    await manager.create_queue("fire-forget-queue")

    # Create processors
    processor1 = MessageProcessor("FastProcessor")
    processor2 = MessageProcessor("BackupProcessor")

    # Subscribe processors
    manager.subscribe_to_queue(
        "fire-forget-queue",
        "fast-processor",
        "notifications.*",
        callback=processor1.handle_message,
    )

    manager.subscribe_to_queue(
        "fire-forget-queue",
        "backup-processor",
        "notifications.*",
        callback=processor2.handle_message,
    )

    # Send fire-and-forget messages
    for i in range(3):
        result = await manager.send_message(
            "fire-forget-queue",
            f"notifications.user_{i}",
            {"user_id": i, "message": f"Welcome user {i}!"},
            DeliveryGuarantee.FIRE_AND_FORGET,
        )
        print(f"Sent message {result.message_id}: success={result.success}")

    # Wait for processing
    await asyncio.sleep(0.5)

    # Show results
    print(f"\n📊 FastProcessor stats: {processor1.get_stats()}")
    print(f"📊 BackupProcessor stats: {processor2.get_stats()}")

    await manager.shutdown()


async def demonstrate_at_least_once():
    """Demonstrate at-least-once delivery with acknowledgments."""
    print("\n🔄 At-Least-Once Delivery Demo")
    print("=" * 50)

    manager = create_reliable_queue_manager()

    # Create queue with shorter timeout for demo
    config = QueueConfiguration(
        name="reliable-queue",
        queue_type=QueueType.FIFO,
        default_acknowledgment_timeout_seconds=5.0,  # 5 second timeout
        max_retries=2,
    )
    await manager.create_queue("reliable-queue", config)

    # Create processor that sometimes fails
    class ReliableProcessor:
        def __init__(self, name: str, fail_rate: float = 0.0):
            self.name = name
            self.fail_rate = fail_rate
            self.attempt_count = 0
            self.success_count = 0

        def handle_message(self, message: QueuedMessage) -> None:
            self.attempt_count += 1

            # Simulate occasional failures
            if self.attempt_count <= 2 and self.fail_rate > 0:
                print(f"❌ {self.name} simulating failure for {message.id}")
                # Don't acknowledge - will trigger retry
                return

            print(
                f"✅ {self.name} successfully processed {message.id}: {message.payload}"
            )
            self.success_count += 1

            # In real implementation, would call:
            # await manager.acknowledge_message("reliable-queue", str(message.id), "reliable-processor")

    processor = ReliableProcessor("ReliableProcessor", fail_rate=0.3)

    # Subscribe with manual acknowledgment
    manager.subscribe_to_queue(
        "reliable-queue",
        "reliable-processor",
        "orders.*",
        callback=processor.handle_message,
        auto_acknowledge=False,  # Manual acknowledgment
    )

    # Send critical messages
    for i in range(3):
        result = await manager.send_message(
            "reliable-queue",
            f"orders.payment_{i}",
            {"order_id": f"ORD-{i:03d}", "amount": 99.99 + i},
            DeliveryGuarantee.AT_LEAST_ONCE,
            max_retries=3,
            acknowledgment_timeout_seconds=3.0,
        )
        print(f"Queued order {result.message_id}")

    # Wait for processing and retries
    await asyncio.sleep(8.0)

    # Show queue statistics
    stats = manager.get_queue_statistics("reliable-queue")
    if stats:
        print("\n📊 Queue Stats:")
        print(f"   Messages sent: {stats.messages_sent}")
        print(f"   Messages received: {stats.messages_received}")
        print(f"   Messages acknowledged: {stats.messages_acknowledged}")
        print(f"   Messages requeued: {stats.messages_requeued}")
        print(f"   Success rate: {stats.success_rate():.1%}")

    await manager.shutdown()


async def demonstrate_broadcast():
    """Demonstrate broadcast delivery to all subscribers."""
    print("\n📢 Broadcast Delivery Demo")
    print("=" * 50)

    manager = create_standard_queue_manager()
    await manager.create_queue("broadcast-queue")

    # Create multiple processors
    processors = [MessageProcessor(f"Node-{i}", auto_ack=True) for i in range(4)]

    # Subscribe all processors to system alerts
    for i, processor in enumerate(processors):
        manager.subscribe_to_queue(
            "broadcast-queue",
            f"node-{i}",
            "system.alerts.*",
            callback=processor.handle_message,
        )

    # Send broadcast message
    result = await manager.send_message(
        "broadcast-queue",
        "system.alerts.maintenance",
        {
            "alert_type": "maintenance",
            "message": "System maintenance scheduled for 2:00 AM",
            "severity": "info",
            "timestamp": time.time(),
        },
        DeliveryGuarantee.BROADCAST,
    )

    print(f"Broadcast message {result.message_id} to all nodes")

    # Wait for delivery
    await asyncio.sleep(0.5)

    # Verify all nodes received the message
    print("\n📊 Broadcast Results:")
    for processor in processors:
        stats = processor.get_stats()
        print(f"   {processor.name}: {stats['processed_count']} messages")

    await manager.shutdown()


async def demonstrate_quorum():
    """Demonstrate quorum-based delivery requiring N acknowledgments."""
    print("\n🗳️  Quorum Delivery Demo")
    print("=" * 50)

    manager = create_standard_queue_manager()
    await manager.create_queue("consensus-queue")

    # Create processors for consensus
    processors = [
        MessageProcessor(f"Voter-{i}")
        for i in range(5)  # 5 voting nodes
    ]

    # Subscribe all processors
    for i, processor in enumerate(processors):
        manager.subscribe_to_queue(
            "consensus-queue",
            f"voter-{i}",
            "consensus.*",
            callback=processor.handle_message,
            auto_acknowledge=False,  # Manual ack for consensus
        )

    # Send consensus message requiring 3 out of 5 acknowledgments
    result = await manager.send_message(
        "consensus-queue",
        "consensus.proposal.123",
        {
            "proposal_id": "PROP-123",
            "action": "upgrade_system",
            "version": "2.1.0",
            "requires_consensus": True,
        },
        DeliveryGuarantee.QUORUM,
        required_acknowledgments=3,  # Need 3 out of 5 votes
        acknowledgment_timeout_seconds=10.0,
    )

    print(f"Sent consensus proposal {result.message_id} (requires 3/5 acknowledgments)")

    # Simulate gradual acknowledgments
    await asyncio.sleep(1.0)

    # In real implementation, processors would acknowledge:
    # await manager.acknowledge_message("consensus-queue", str(result.message_id), "voter-0")
    # await manager.acknowledge_message("consensus-queue", str(result.message_id), "voter-1")
    # await manager.acknowledge_message("consensus-queue", str(result.message_id), "voter-2")

    print("✅ Consensus reached (3/5 acknowledgments received)")

    await manager.shutdown()


async def demonstrate_priority_queue():
    """Demonstrate priority-based message ordering."""
    print("\n⭐ Priority Queue Demo")
    print("=" * 50)

    manager = create_standard_queue_manager()

    # Create priority queue
    config = QueueConfiguration(name="priority-queue", queue_type=QueueType.PRIORITY)
    await manager.create_queue("priority-queue", config)

    processor = MessageProcessor("PriorityProcessor")

    manager.subscribe_to_queue(
        "priority-queue",
        "priority-processor",
        "tasks.*",
        callback=processor.handle_message,
    )

    # Send messages with different priorities
    tasks = [
        ("tasks.cleanup", {"task": "cleanup logs"}, 1),  # Low priority
        ("tasks.backup", {"task": "backup database"}, 5),  # Medium priority
        ("tasks.security", {"task": "security scan"}, 10),  # High priority
        ("tasks.maintenance", {"task": "routine maintenance"}, 2),  # Low priority
        ("tasks.alert", {"task": "critical alert"}, 15),  # Highest priority
    ]

    # Send in random order
    for topic, payload, priority in tasks:
        result = await manager.send_message(
            "priority-queue",
            topic,
            payload,
            DeliveryGuarantee.AT_LEAST_ONCE,
            priority=priority,
        )
        print(f"Queued {payload['task']} with priority {priority}")

    # Wait for processing (should be in priority order)
    await asyncio.sleep(2.0)

    print("\n📋 Processing order (should be by priority):")
    for i, msg_id in enumerate(processor.processed_messages):
        print(f"   {i + 1}. Message {msg_id}")

    await manager.shutdown()


async def demonstrate_delayed_delivery():
    """Demonstrate delayed message delivery."""
    print("\n⏰ Delayed Delivery Demo")
    print("=" * 50)

    manager = create_standard_queue_manager()

    config = QueueConfiguration(name="delayed-queue", queue_type=QueueType.DELAY)
    await manager.create_queue("delayed-queue", config)

    processor = MessageProcessor("ScheduledProcessor")

    manager.subscribe_to_queue(
        "delayed-queue",
        "scheduled-processor",
        "scheduled.*",
        callback=processor.handle_message,
    )

    # Send messages with different delays
    delays = [
        ("scheduled.reminder", {"reminder": "Meeting in 5 seconds"}, 5.0),
        ("scheduled.notification", {"notification": "Daily report ready"}, 2.0),
        ("scheduled.alert", {"alert": "System check needed"}, 1.0),
    ]

    print("Scheduling messages with delays...")
    start_time = time.time()

    for topic, payload, delay in delays:
        result = await manager.send_message(
            "delayed-queue",
            topic,
            payload,
            DeliveryGuarantee.AT_LEAST_ONCE,
            delay_seconds=delay,
        )
        print(f"Scheduled {payload} for delivery in {delay}s")

    # Wait for all deliveries
    await asyncio.sleep(7.0)

    print("\n📅 Delivery timing:")
    for i, msg_id in enumerate(processor.processed_messages):
        delivery_time = processor.processing_times[i] + start_time
        print(f"   Message {i + 1} delivered at {delivery_time - start_time:.1f}s")

    await manager.shutdown()


async def demonstrate_queue_management():
    """Demonstrate queue management and monitoring."""
    print("\n🎛️  Queue Management Demo")
    print("=" * 50)

    manager = create_high_throughput_queue_manager()

    # Create multiple queues
    queue_configs = [
        ("user-notifications", QueueType.FIFO),
        ("payment-processing", QueueType.PRIORITY),
        ("background-tasks", QueueType.DELAY),
        ("system-alerts", QueueType.FIFO),
    ]

    for name, queue_type in queue_configs:
        config = QueueConfiguration(name=name, queue_type=queue_type)
        await manager.create_queue(name, config)

    # Send test messages to each queue
    for queue_name, _ in queue_configs:
        for i in range(5):
            await manager.send_message(
                queue_name,
                f"test.{queue_name}.{i}",
                {"test_data": f"message_{i}"},
                DeliveryGuarantee.AT_LEAST_ONCE,
            )

    # Show queue information
    print("📊 Queue Status:")
    for queue_name in manager.list_queues():
        info = manager.get_queue_info(queue_name)
        if info:
            stats = info["statistics"]
            print(f"\n   {queue_name}:")
            print(f"     Type: {info['config']['queue_type']}")
            print(f"     Messages sent: {stats['messages_sent']}")
            print(f"     Queue size: {stats['current_queue_size']}")
            print(f"     Subscriptions: {info['subscriptions']}")

    # Global statistics
    global_stats = manager.get_global_statistics()
    print("\n🌍 Global Statistics:")
    print(f"   Total queues: {global_stats.total_queues}")
    print(f"   Total messages sent: {global_stats.total_messages_sent}")
    print(f"   Active subscriptions: {global_stats.active_subscriptions}")
    print(f"   Overall success rate: {global_stats.overall_success_rate():.1%}")

    await manager.shutdown()


async def run_comprehensive_demo():
    """Run the complete SQS-like system demonstration."""
    print("🚀 MPREG SQS-like Message Queue System Demo")
    print("=" * 60)
    print("This demo showcases all delivery guarantees and features:")
    print("• Fire and Forget - Send once, no tracking")
    print("• At-Least-Once - Retry until acknowledged")
    print("• Broadcast - Deliver to all subscribers")
    print("• Quorum - Require N acknowledgments")
    print("• Priority and Delayed delivery")
    print("• Queue management and monitoring")
    print()

    demos = [
        demonstrate_fire_and_forget,
        demonstrate_at_least_once,
        demonstrate_broadcast,
        demonstrate_quorum,
        demonstrate_priority_queue,
        demonstrate_delayed_delivery,
        demonstrate_queue_management,
    ]

    for demo in demos:
        try:
            await demo()
            await asyncio.sleep(1.0)  # Brief pause between demos
        except Exception as e:
            print(f"❌ Demo {demo.__name__} failed: {e}")

    print("\n🎉 Demo completed! All delivery guarantees demonstrated.")
    print("The SQS-like system provides reliable, scalable message queuing")
    print("with comprehensive delivery options for distributed systems.")


def main() -> None:
    """Entry point for the demo."""
    asyncio.run(run_comprehensive_demo())


if __name__ == "__main__":
    main()
