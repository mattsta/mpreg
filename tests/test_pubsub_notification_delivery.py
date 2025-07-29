"""
Test PubSub notification delivery end-to-end.

This test verifies that notifications are properly delivered from the server
to the client through the WebSocket connection.
"""

import asyncio
import time

import pytest

from mpreg.client.pubsub_client import MPREGPubSubExtendedClient
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


class TestPubSubNotificationDelivery:
    """Test end-to-end notification delivery."""

    @pytest.mark.asyncio
    async def test_basic_notification_delivery(self):
        """Test that notifications are delivered to subscribers."""
        # Start server
        settings = MPREGSettings(
            host="127.0.0.1", port=19100, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        server_task = asyncio.create_task(server.server())
        await asyncio.sleep(0.5)  # Give server time to start

        try:
            # Track received messages
            received_messages = []

            def message_callback(message):
                received_messages.append(message)

            # Create and connect client
            async with MPREGPubSubExtendedClient("ws://127.0.0.1:19100") as client:
                # Subscribe to test topics
                subscription_id = await client.subscribe(
                    patterns=["test.*"], callback=message_callback, get_backlog=False
                )

                # Wait for subscription to be active
                await asyncio.sleep(0.2)

                # Publish a test message
                success = await client.publish(
                    "test.notification",
                    {"data": "Hello PubSub!", "timestamp": time.time()},
                )
                assert success, "Message publish should succeed"

                # Wait for notification delivery
                await asyncio.sleep(0.5)

                # Verify notification was received
                assert len(received_messages) == 1, (
                    f"Expected 1 message, got {len(received_messages)}"
                )

                received_msg = received_messages[0]
                assert received_msg.topic == "test.notification"
                assert received_msg.payload["data"] == "Hello PubSub!"

        finally:
            # Cleanup
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_multiple_subscribers_same_topic(self):
        """Test that multiple subscribers receive the same message."""
        # Start server
        settings = MPREGSettings(
            host="127.0.0.1", port=19101, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        server_task = asyncio.create_task(server.server())
        await asyncio.sleep(0.5)

        try:
            # Track messages for each client
            client1_messages = []
            client2_messages = []

            def client1_callback(message):
                client1_messages.append(message)

            def client2_callback(message):
                client2_messages.append(message)

            # Create two clients
            async with MPREGPubSubExtendedClient("ws://127.0.0.1:19101") as client1:
                async with MPREGPubSubExtendedClient("ws://127.0.0.1:19101") as client2:
                    # Both subscribe to the same topic
                    sub1 = await client1.subscribe(
                        patterns=["broadcast.*"],
                        callback=client1_callback,
                        get_backlog=False,
                    )

                    sub2 = await client2.subscribe(
                        patterns=["broadcast.*"],
                        callback=client2_callback,
                        get_backlog=False,
                    )

                    # Wait for subscriptions to be active
                    await asyncio.sleep(0.2)

                    # Publish a broadcast message
                    success = await client1.publish(
                        "broadcast.announcement",
                        {"message": "Important announcement!", "priority": "high"},
                    )
                    assert success

                    # Wait for delivery
                    await asyncio.sleep(0.5)

                    # Both clients should receive the message
                    assert len(client1_messages) == 1
                    assert len(client2_messages) == 1

                    # Verify message content
                    assert client1_messages[0].topic == "broadcast.announcement"
                    assert client2_messages[0].topic == "broadcast.announcement"
                    assert (
                        client1_messages[0].payload["message"]
                        == "Important announcement!"
                    )
                    assert (
                        client2_messages[0].payload["message"]
                        == "Important announcement!"
                    )

        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_wildcard_pattern_matching(self):
        """Test that wildcard patterns work correctly for notifications."""
        # Start server
        settings = MPREGSettings(
            host="127.0.0.1", port=19102, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        server_task = asyncio.create_task(server.server())
        await asyncio.sleep(0.5)

        try:
            # Track received messages
            single_wildcard_messages = []
            multi_wildcard_messages = []

            def single_wildcard_callback(message):
                single_wildcard_messages.append(message)

            def multi_wildcard_callback(message):
                multi_wildcard_messages.append(message)

            async with MPREGPubSubExtendedClient("ws://127.0.0.1:19102") as client:
                # Subscribe with different wildcard patterns
                sub1 = await client.subscribe(
                    patterns=["user.*.login"],  # Single wildcard
                    callback=single_wildcard_callback,
                    get_backlog=False,
                )

                sub2 = await client.subscribe(
                    patterns=["system.#"],  # Multi wildcard
                    callback=multi_wildcard_callback,
                    get_backlog=False,
                )

                await asyncio.sleep(0.2)

                # Publish messages that should match patterns
                messages_to_publish = [
                    (
                        "user.123.login",
                        {"user_id": "123"},
                    ),  # Should match single wildcard
                    (
                        "user.456.login",
                        {"user_id": "456"},
                    ),  # Should match single wildcard
                    ("system.startup", {"status": "ok"}),  # Should match multi wildcard
                    (
                        "system.config.updated",
                        {"section": "auth"},
                    ),  # Should match multi wildcard
                    (
                        "user.login",
                        {"error": "no user id"},
                    ),  # Should NOT match single wildcard
                    ("admin.123.login", {"admin_id": "123"}),  # Should NOT match either
                ]

                for topic, payload in messages_to_publish:
                    success = await client.publish(topic, payload)
                    assert success

                # Wait for delivery
                await asyncio.sleep(0.5)

                # Verify correct pattern matching
                assert len(single_wildcard_messages) == 2, (
                    "Should receive 2 user.*.login messages"
                )
                assert len(multi_wildcard_messages) == 2, (
                    "Should receive 2 system.# messages"
                )

                # Verify correct topics
                single_topics = [msg.topic for msg in single_wildcard_messages]
                multi_topics = [msg.topic for msg in multi_wildcard_messages]

                assert "user.123.login" in single_topics
                assert "user.456.login" in single_topics
                assert "system.startup" in multi_topics
                assert "system.config.updated" in multi_topics

        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_subscription_cleanup_on_disconnect(self):
        """Test that subscriptions are cleaned up when client disconnects."""
        # Start server
        settings = MPREGSettings(
            host="127.0.0.1", port=19103, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        server_task = asyncio.create_task(server.server())
        await asyncio.sleep(0.5)

        try:
            # Create a client and subscription, then disconnect
            client = MPREGPubSubExtendedClient("ws://127.0.0.1:19103")
            await client.connect()

            received_messages = []

            def callback(message):
                received_messages.append(message)

            subscription_id = await client.subscribe(
                patterns=["cleanup.test"], callback=callback, get_backlog=False
            )

            await asyncio.sleep(0.2)

            # Verify subscription exists in server
            assert len(server.topic_exchange.subscriptions) == 1
            assert subscription_id in server.topic_exchange.subscriptions

            # Disconnect client
            await client.disconnect()
            await asyncio.sleep(0.2)

            # Server should still have the subscription (cleanup happens on actual disconnect)
            # But publishing a message shouldn't crash
            publisher_client = MPREGPubSubExtendedClient("ws://127.0.0.1:19103")
            await publisher_client.connect()

            success = await publisher_client.publish("cleanup.test", {"data": "test"})
            assert success

            await asyncio.sleep(0.2)

            # No messages should be received (client is disconnected)
            assert len(received_messages) == 0

            await publisher_client.disconnect()

        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_unsubscribe_functionality(self):
        """Test that unsubscribing works correctly."""
        # Start server
        settings = MPREGSettings(
            host="127.0.0.1", port=19104, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        server_task = asyncio.create_task(server.server())
        await asyncio.sleep(0.5)

        try:
            received_messages = []

            def callback(message):
                received_messages.append(message)

            async with MPREGPubSubExtendedClient("ws://127.0.0.1:19104") as client:
                # Subscribe to topic
                subscription_id = await client.subscribe(
                    patterns=["unsub.test"], callback=callback, get_backlog=False
                )

                await asyncio.sleep(0.2)

                # Publish message - should be received
                success = await client.publish("unsub.test", {"phase": "before_unsub"})
                assert success
                await asyncio.sleep(0.2)

                assert len(received_messages) == 1
                assert received_messages[0].payload["phase"] == "before_unsub"

                # Unsubscribe
                unsub_success = await client.unsubscribe(subscription_id)
                assert unsub_success
                await asyncio.sleep(0.2)

                # Publish another message - should NOT be received
                success = await client.publish("unsub.test", {"phase": "after_unsub"})
                assert success
                await asyncio.sleep(0.2)

                # Should still only have the first message
                assert len(received_messages) == 1

        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass
