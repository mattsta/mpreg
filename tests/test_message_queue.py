"""
Comprehensive tests for the SQS-like Message Queue System.

Tests cover all delivery guarantees, queue types, and system features:
- Fire and forget delivery
- At-least-once with acknowledgments and retries
- Broadcast to all subscribers
- Quorum-based delivery requiring N acknowledgments
- Priority and delayed delivery
- Dead letter queues and error handling
- Queue management and statistics
"""

import asyncio
import time

import pytest

from mpreg.core.message_queue import (
    DeliveryGuarantee,
    MessageQueue,
    QueueConfiguration,
    QueuedMessage,
    QueueType,
)
from mpreg.core.message_queue_manager import (
    MessageQueueManager,
    QueueManagerConfiguration,
)


class TestMessageQueue:
    """Test core MessageQueue functionality."""

    def test_message_creation(self):
        """Test creating queued messages with proper dataclass structure."""
        config = QueueConfiguration(name="test-queue")
        queue = MessageQueue(config)

        # Test message has all required fields as dataclass
        assert hasattr(queue.statistics, "messages_sent")
        assert hasattr(queue.statistics, "success_rate")

        # Verify no dict[str, Any] usage
        assert isinstance(queue.statistics.messages_sent, int)
        assert callable(queue.statistics.success_rate)

        queue.shutdown_sync()

    @pytest.mark.asyncio
    async def test_fire_and_forget_delivery(self):
        """Test fire-and-forget message delivery."""
        config = QueueConfiguration(name="fire-forget-test")
        queue = MessageQueue(config)

        # Set up mock callback
        received_messages = []

        def callback(message: QueuedMessage) -> None:
            received_messages.append(message)

        # Subscribe
        queue.subscribe("test-subscriber", "test.*", callback=callback)

        # Send fire-and-forget message
        result = await queue.send_message(
            "test.message", {"data": "test_payload"}, DeliveryGuarantee.FIRE_AND_FORGET
        )

        assert result.success
        assert len(received_messages) == 1
        assert received_messages[0].payload == {"data": "test_payload"}
        assert (
            received_messages[0].delivery_guarantee == DeliveryGuarantee.FIRE_AND_FORGET
        )

        await queue.shutdown()

    @pytest.mark.asyncio
    async def test_at_least_once_delivery(self):
        """Test at-least-once delivery with acknowledgments."""
        config = QueueConfiguration(
            name="reliable-test", default_acknowledgment_timeout_seconds=1.0
        )
        queue = MessageQueue(config)

        received_messages = []

        def callback(message: QueuedMessage) -> None:
            received_messages.append(message)

        # Subscribe without auto-acknowledgment
        queue.subscribe(
            "test-subscriber", "reliable.*", callback=callback, auto_acknowledge=False
        )

        # Send message
        result = await queue.send_message(
            "reliable.order", {"order_id": "12345"}, DeliveryGuarantee.AT_LEAST_ONCE
        )

        assert result.success

        # Wait for delivery
        await asyncio.sleep(0.2)
        assert len(received_messages) == 1

        # Acknowledge the message
        ack_result = await queue.acknowledge_message(
            result.message_id.id, "test-subscriber"
        )
        assert ack_result

        # Verify statistics
        stats = queue.get_statistics()
        assert stats.messages_sent == 1
        assert stats.messages_received == 1
        assert stats.messages_acknowledged == 1

        await queue.shutdown()

    @pytest.mark.asyncio
    async def test_broadcast_delivery(self):
        """Test broadcast delivery to all subscribers."""
        config = QueueConfiguration(name="broadcast-test")
        queue = MessageQueue(config)

        # Set up multiple subscribers
        received_by_sub1 = []
        received_by_sub2 = []
        received_by_sub3 = []

        def callback1(message: QueuedMessage) -> None:
            received_by_sub1.append(message)

        def callback2(message: QueuedMessage) -> None:
            received_by_sub2.append(message)

        def callback3(message: QueuedMessage) -> None:
            received_by_sub3.append(message)

        # Subscribe all
        queue.subscribe("subscriber-1", "alerts.*", callback=callback1)
        queue.subscribe("subscriber-2", "alerts.*", callback=callback2)
        queue.subscribe("subscriber-3", "alerts.*", callback=callback3)

        # Send broadcast message
        result = await queue.send_message(
            "alerts.system",
            {"alert": "System maintenance"},
            DeliveryGuarantee.BROADCAST,
        )

        assert result.success

        # Wait for delivery
        await asyncio.sleep(0.2)

        # All subscribers should receive the message
        assert len(received_by_sub1) == 1
        assert len(received_by_sub2) == 1
        assert len(received_by_sub3) == 1

        # All should have same message content
        assert received_by_sub1[0].payload == {"alert": "System maintenance"}
        assert received_by_sub2[0].payload == {"alert": "System maintenance"}
        assert received_by_sub3[0].payload == {"alert": "System maintenance"}

        await queue.shutdown()

    @pytest.mark.asyncio
    async def test_quorum_delivery(self):
        """Test quorum delivery requiring N acknowledgments."""
        config = QueueConfiguration(
            name="quorum-test", default_acknowledgment_timeout_seconds=2.0
        )
        queue = MessageQueue(config)

        received_messages = []

        def callback(message: QueuedMessage) -> None:
            received_messages.append(message)

        # Subscribe multiple nodes
        for i in range(5):
            queue.subscribe(
                f"node-{i}", "consensus.*", callback=callback, auto_acknowledge=False
            )

        # Send quorum message requiring 3 acknowledgments
        result = await queue.send_message(
            "consensus.proposal",
            {"proposal_id": "PROP-123"},
            DeliveryGuarantee.QUORUM,
            required_acknowledgments=3,
        )

        assert result.success

        # Wait for delivery
        await asyncio.sleep(0.2)
        assert len(received_messages) == 5  # Delivered to all

        # Acknowledge from 3 nodes (should complete)
        for i in range(3):
            ack_result = await queue.acknowledge_message(
                result.message_id.id, f"node-{i}"
            )
            assert ack_result

        # Verify completion
        stats = queue.get_statistics()
        assert stats.messages_acknowledged >= 1

        await queue.shutdown()

    @pytest.mark.asyncio
    async def test_priority_queue(self):
        """Test priority-based message ordering."""
        config = QueueConfiguration(name="priority-test", queue_type=QueueType.PRIORITY)
        queue = MessageQueue(config)

        processed_order = []

        def callback(message: QueuedMessage) -> None:
            processed_order.append((message.payload["task"], message.priority))

        queue.subscribe("processor", "tasks.*", callback=callback)

        # Send messages with different priorities (higher number = higher priority)
        tasks = [
            ("low-priority-task", 1),
            ("high-priority-task", 10),
            ("medium-priority-task", 5),
            ("urgent-task", 15),
        ]

        for task_name, priority in tasks:
            await queue.send_message(
                "tasks.work",
                {"task": task_name},
                DeliveryGuarantee.AT_LEAST_ONCE,
                priority=priority,
            )

        # Wait for processing
        await asyncio.sleep(0.5)

        # Should be processed in priority order (high to low)
        assert len(processed_order) == 4
        assert processed_order[0] == ("urgent-task", 15)
        assert processed_order[1] == ("high-priority-task", 10)
        assert processed_order[2] == ("medium-priority-task", 5)
        assert processed_order[3] == ("low-priority-task", 1)

        await queue.shutdown()

    @pytest.mark.asyncio
    async def test_delayed_delivery(self):
        """Test delayed message delivery."""
        config = QueueConfiguration(name="delay-test", queue_type=QueueType.DELAY)
        queue = MessageQueue(config)

        delivery_times = []
        start_time = time.time()

        def callback(message: QueuedMessage) -> None:
            delivery_times.append(time.time() - start_time)

        queue.subscribe("scheduler", "scheduled.*", callback=callback)

        # Send messages with different delays
        delays = [2.0, 1.0, 0.5]  # seconds

        for i, delay in enumerate(delays):
            await queue.send_message(
                "scheduled.task",
                {"task_id": f"task-{i}"},
                DeliveryGuarantee.AT_LEAST_ONCE,
                delay_seconds=delay,
            )

        # Wait for all deliveries
        await asyncio.sleep(3.0)

        # Should have delivered in delay order (shortest delay first)
        assert len(delivery_times) == 3
        assert delivery_times[0] >= 0.5  # First delivery after 0.5s
        assert delivery_times[1] >= 1.0  # Second delivery after 1.0s
        assert delivery_times[2] >= 2.0  # Third delivery after 2.0s

        await queue.shutdown()

    @pytest.mark.asyncio
    async def test_message_retry_logic(self):
        """Test message retry behavior on failures."""
        config = QueueConfiguration(
            name="retry-test", default_acknowledgment_timeout_seconds=0.5, max_retries=2
        )
        queue = MessageQueue(config)

        attempt_count = 0

        def failing_callback(message: QueuedMessage) -> None:
            nonlocal attempt_count
            attempt_count += 1
            # Don't acknowledge - will trigger retry

        queue.subscribe(
            "failing-processor",
            "retry.*",
            callback=failing_callback,
            auto_acknowledge=False,
        )

        # Send message that will fail
        result = await queue.send_message(
            "retry.test",
            {"data": "will_fail"},
            DeliveryGuarantee.AT_LEAST_ONCE,
            max_retries=2,
        )

        assert result.success

        # Wait for retries to complete
        await asyncio.sleep(2.0)

        # Should have attempted delivery 3 times (initial + 2 retries)
        assert attempt_count == 3

        # Check statistics
        stats = queue.get_statistics()
        assert stats.messages_requeued >= 2

        await queue.shutdown()

    def test_topic_pattern_matching(self):
        """Test topic pattern matching functionality."""
        config = QueueConfiguration(name="pattern-test")
        queue = MessageQueue(config)

        # Test exact match
        assert queue._topic_matches_pattern("user.login", "user.login")

        # Test wildcard patterns
        assert queue._topic_matches_pattern("user.login", "user.*")
        assert queue._topic_matches_pattern("user.logout", "user.*")
        assert queue._topic_matches_pattern("system.alert.critical", "*.alert.*")
        assert queue._topic_matches_pattern("system.alert.critical", "system.#")
        assert queue._topic_matches_pattern("system.alert.critical", "system.alert.#")
        assert queue._topic_matches_pattern("system.alert.critical", "#")

        # Test non-matches
        assert not queue._topic_matches_pattern("admin.login", "user.*")
        assert not queue._topic_matches_pattern("user.settings", "user.login")
        assert not queue._topic_matches_pattern("user.login", "system.#")

        queue.shutdown_sync()


class TestMessageQueueManager:
    """Test MessageQueueManager functionality."""

    @pytest.mark.asyncio
    async def test_queue_creation_and_management(self):
        """Test creating and managing multiple queues."""
        config = QueueManagerConfiguration()
        manager = MessageQueueManager(config)

        # Create queues
        assert await manager.create_queue("queue-1")
        assert await manager.create_queue("queue-2")
        assert await manager.create_queue("queue-3")

        # List queues
        queue_names = manager.list_queues()
        assert len(queue_names) == 3
        assert "queue-1" in queue_names
        assert "queue-2" in queue_names
        assert "queue-3" in queue_names

        # Get queue info
        info = manager.get_queue_info("queue-1")
        assert info is not None
        assert info["name"] == "queue-1"
        assert "config" in info
        assert "statistics" in info

        # Delete queue
        assert await manager.delete_queue("queue-2")
        assert len(manager.list_queues()) == 2
        assert "queue-2" not in manager.list_queues()

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_auto_queue_creation(self):
        """Test automatic queue creation when sending messages."""
        config = QueueManagerConfiguration(enable_auto_queue_creation=True)
        manager = MessageQueueManager(config)

        # Send message to non-existent queue (should auto-create)
        result = await manager.send_message(
            "auto-created-queue",
            "test.message",
            {"data": "auto_created"},
            DeliveryGuarantee.FIRE_AND_FORGET,
        )

        assert result.success
        assert "auto-created-queue" in manager.list_queues()

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_subscription_management(self):
        """Test subscription creation and cleanup."""
        config = QueueManagerConfiguration()
        manager = MessageQueueManager(config)

        await manager.create_queue("sub-test-queue")

        received_messages = []

        def callback(message: QueuedMessage) -> None:
            received_messages.append(message)

        # Subscribe
        sub_id = manager.subscribe_to_queue(
            "sub-test-queue", "test-subscriber", "events.*", callback=callback
        )

        assert sub_id is not None

        # Send test message
        result = await manager.send_message(
            "sub-test-queue",
            "events.test",
            {"event": "test_event"},
            DeliveryGuarantee.FIRE_AND_FORGET,
        )

        assert result.success

        # Wait for delivery
        await asyncio.sleep(0.2)
        assert len(received_messages) == 1

        # Unsubscribe
        assert manager.unsubscribe_from_queue("sub-test-queue", sub_id)

        # Send another message (should not be received)
        await manager.send_message(
            "sub-test-queue",
            "events.test2",
            {"event": "test_event2"},
            DeliveryGuarantee.FIRE_AND_FORGET,
        )

        await asyncio.sleep(0.2)
        assert len(received_messages) == 1  # Still only 1 message

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_global_statistics(self):
        """Test global statistics across multiple queues."""
        config = QueueManagerConfiguration()
        manager = MessageQueueManager(config)

        # Create multiple queues
        for i in range(3):
            await manager.create_queue(f"stats-queue-{i}")

        # Send messages to different queues
        for i in range(3):
            for j in range(5):  # 5 messages per queue
                await manager.send_message(
                    f"stats-queue-{i}",
                    f"test.message.{j}",
                    {"queue": i, "message": j},
                    DeliveryGuarantee.FIRE_AND_FORGET,
                )

        # Check global statistics
        stats = manager.get_global_statistics()
        assert stats.total_queues == 3
        assert stats.total_messages_sent == 15  # 3 queues * 5 messages

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_acknowledgment_routing(self):
        """Test message acknowledgment routing through manager."""
        config = QueueManagerConfiguration()
        manager = MessageQueueManager(config)

        await manager.create_queue("ack-test-queue")

        received_messages = []

        def callback(message: QueuedMessage) -> None:
            received_messages.append(message)

        # Subscribe without auto-acknowledgment
        manager.subscribe_to_queue(
            "ack-test-queue",
            "ack-subscriber",
            "ack.*",
            callback=callback,
            auto_acknowledge=False,
        )

        # Send message
        result = await manager.send_message(
            "ack-test-queue",
            "ack.test",
            {"requires_ack": True},
            DeliveryGuarantee.AT_LEAST_ONCE,
        )

        assert result.success

        # Wait for delivery
        await asyncio.sleep(0.2)
        assert len(received_messages) == 1

        # Acknowledge through manager
        ack_result = await manager.acknowledge_message(
            "ack-test-queue", result.message_id.id, "ack-subscriber"
        )
        assert ack_result

        await manager.shutdown()


class TestErrorHandling:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_queue_capacity_limits(self):
        """Test queue capacity enforcement."""
        config = QueueConfiguration(
            name="capacity-test",
            max_size=2,  # Very small queue
        )
        queue = MessageQueue(config)

        # Fill queue to capacity
        result1 = await queue.send_message(
            "test.1", {"data": 1}, DeliveryGuarantee.AT_LEAST_ONCE
        )
        result2 = await queue.send_message(
            "test.2", {"data": 2}, DeliveryGuarantee.AT_LEAST_ONCE
        )

        assert result1.success
        assert result2.success

        # Try to exceed capacity
        result3 = await queue.send_message(
            "test.3", {"data": 3}, DeliveryGuarantee.AT_LEAST_ONCE
        )
        assert not result3.success
        assert result3.error_message and "capacity" in result3.error_message.lower()

        await queue.shutdown()

    @pytest.mark.asyncio
    async def test_non_existent_queue_operations(self):
        """Test operations on non-existent queues."""
        config = QueueManagerConfiguration(enable_auto_queue_creation=False)
        manager = MessageQueueManager(config)

        # Try to send to non-existent queue
        result = await manager.send_message(
            "non-existent-queue",
            "test.topic",
            {"data": "test"},
            DeliveryGuarantee.FIRE_AND_FORGET,
        )

        assert not result.success
        assert result.error_message and "does not exist" in result.error_message

        # Try to subscribe to non-existent queue
        sub_id = manager.subscribe_to_queue(
            "non-existent-queue", "subscriber", "topic.*"
        )
        assert sub_id is None

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_duplicate_queue_creation(self):
        """Test handling of duplicate queue creation."""
        config = QueueManagerConfiguration()
        manager = MessageQueueManager(config)

        # Create queue
        assert await manager.create_queue("duplicate-test")

        # Try to create same queue again
        assert not await manager.create_queue("duplicate-test")

        # Should still have only one queue
        assert len(manager.list_queues()) == 1

        await manager.shutdown()


class TestDeduplication:
    """Test message deduplication functionality."""

    @pytest.mark.asyncio
    async def test_message_deduplication(self):
        """Test message deduplication within time window."""
        config = QueueConfiguration(
            name="dedup-test",
            enable_deduplication=True,
            deduplication_window_seconds=2.0,
        )
        queue = MessageQueue(config)

        received_count = 0

        def callback(message: QueuedMessage) -> None:
            nonlocal received_count
            received_count += 1

        queue.subscribe("dedup-subscriber", "dedup.*", callback=callback)

        # Send identical messages
        payload = {"order_id": "12345", "amount": 100.0}

        result1 = await queue.send_message(
            "dedup.order", payload, DeliveryGuarantee.FIRE_AND_FORGET
        )
        result2 = await queue.send_message(
            "dedup.order", payload, DeliveryGuarantee.FIRE_AND_FORGET
        )
        result3 = await queue.send_message(
            "dedup.order", payload, DeliveryGuarantee.FIRE_AND_FORGET
        )

        assert result1.success
        assert not result2.success  # Duplicate
        assert not result3.success  # Duplicate

        await asyncio.sleep(0.2)

        # Should only receive one message
        assert received_count == 1

        await queue.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
