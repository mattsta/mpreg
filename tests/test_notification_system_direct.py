"""
Test PubSub notification system directly without client WebSocket conflicts.

This tests the core notification delivery mechanism by directly calling
the server methods and checking that notifications are created and sent.
"""

import time
from unittest.mock import AsyncMock

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.model import PubSubMessage, PubSubSubscription, TopicPattern
from mpreg.server import MPREGServer


class TestNotificationSystemDirect:
    """Test the notification system directly."""

    @pytest.mark.asyncio
    async def test_notification_creation_and_tracking(self):
        """Test that notifications are created and client tracking works."""
        # Create server
        settings = MPREGSettings(
            host="127.0.0.1", port=19200, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        # Create a mock WebSocket
        mock_websocket = AsyncMock()
        mock_websocket.remote_address = ("127.0.0.1", 12345)

        # Create a subscription directly
        subscription = PubSubSubscription(
            subscription_id="test_sub_1",
            patterns=(TopicPattern(pattern="test.*", exact_match=False),),
            subscriber="test_client",
            created_at=time.time(),
            get_backlog=False,
        )

        # Add subscription to server
        server.topic_exchange.add_subscription(subscription)

        # Track the subscription-client mapping
        client_id = f"client_{id(mock_websocket)}"
        server.pubsub_clients[client_id] = mock_websocket
        server.subscription_to_client["test_sub_1"] = client_id

        # Create a test message
        message = PubSubMessage(
            topic="test.notification",
            payload={"data": "Hello Direct Test!"},
            timestamp=time.time(),
            message_id="msg_1",
            publisher="test_publisher",
        )

        # Publish message and get notifications
        notifications = server.topic_exchange.publish_message(message)

        # Verify notification was created
        assert len(notifications) == 1
        notification = notifications[0]
        assert notification.subscription_id == "test_sub_1"
        assert notification.message.topic == "test.notification"

        # Test sending notification to client
        await server._send_notification_to_client(notification)

        # Verify WebSocket send was called
        mock_websocket.send.assert_called_once()
        sent_data = mock_websocket.send.call_args[0][0]

        # Verify the sent data is properly serialized notification
        deserialized = server.serializer.deserialize(sent_data)
        assert deserialized["role"] == "pubsub-notification"
        assert deserialized["subscription_id"] == "test_sub_1"
        assert deserialized["message"]["topic"] == "test.notification"

    @pytest.mark.asyncio
    async def test_multiple_subscribers_receive_notifications(self):
        """Test that multiple subscribers receive the same notification."""
        settings = MPREGSettings(
            host="127.0.0.1", port=19201, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        # Create mock WebSockets for two clients
        mock_websocket1 = AsyncMock()
        mock_websocket1.remote_address = ("127.0.0.1", 12346)
        mock_websocket2 = AsyncMock()
        mock_websocket2.remote_address = ("127.0.0.1", 12347)

        # Create subscriptions for both clients
        subscription1 = PubSubSubscription(
            subscription_id="sub_1",
            patterns=(TopicPattern(pattern="broadcast.*", exact_match=False),),
            subscriber="client_1",
            created_at=time.time(),
            get_backlog=False,
        )

        subscription2 = PubSubSubscription(
            subscription_id="sub_2",
            patterns=(TopicPattern(pattern="broadcast.*", exact_match=False),),
            subscriber="client_2",
            created_at=time.time(),
            get_backlog=False,
        )

        # Add subscriptions
        server.topic_exchange.add_subscription(subscription1)
        server.topic_exchange.add_subscription(subscription2)

        # Track client mappings
        client_id1 = f"client_{id(mock_websocket1)}"
        client_id2 = f"client_{id(mock_websocket2)}"
        server.pubsub_clients[client_id1] = mock_websocket1
        server.pubsub_clients[client_id2] = mock_websocket2
        server.subscription_to_client["sub_1"] = client_id1
        server.subscription_to_client["sub_2"] = client_id2

        # Publish a broadcast message
        message = PubSubMessage(
            topic="broadcast.announcement",
            payload={"message": "Important update!", "priority": "high"},
            timestamp=time.time(),
            message_id="broadcast_1",
            publisher="admin",
        )

        notifications = server.topic_exchange.publish_message(message)

        # Should create notifications for both subscribers
        assert len(notifications) == 2
        subscription_ids = [n.subscription_id for n in notifications]
        assert "sub_1" in subscription_ids
        assert "sub_2" in subscription_ids

        # Send notifications to both clients
        for notification in notifications:
            await server._send_notification_to_client(notification)

        # Both WebSockets should have been called
        mock_websocket1.send.assert_called_once()
        mock_websocket2.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_wildcard_pattern_notification_routing(self):
        """Test that wildcard patterns route notifications correctly."""
        settings = MPREGSettings(
            host="127.0.0.1", port=19202, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        # Create mock WebSockets
        mock_websocket1 = AsyncMock()  # Single wildcard subscriber
        mock_websocket2 = AsyncMock()  # Multi wildcard subscriber
        mock_websocket3 = AsyncMock()  # Exact match subscriber

        # Create different subscription patterns
        subscriptions = [
            PubSubSubscription(
                subscription_id="single_wildcard",
                patterns=(TopicPattern(pattern="user.*.login", exact_match=False),),
                subscriber="single_wildcard_client",
                created_at=time.time(),
                get_backlog=False,
            ),
            PubSubSubscription(
                subscription_id="multi_wildcard",
                patterns=(TopicPattern(pattern="system.#", exact_match=False),),
                subscriber="multi_wildcard_client",
                created_at=time.time(),
                get_backlog=False,
            ),
            PubSubSubscription(
                subscription_id="exact_match",
                patterns=(TopicPattern(pattern="exact.topic", exact_match=True),),
                subscriber="exact_match_client",
                created_at=time.time(),
                get_backlog=False,
            ),
        ]

        # Add subscriptions and set up client tracking
        websockets = [mock_websocket1, mock_websocket2, mock_websocket3]
        for i, (subscription, websocket) in enumerate(zip(subscriptions, websockets)):
            server.topic_exchange.add_subscription(subscription)
            client_id = f"client_{i}"
            server.pubsub_clients[client_id] = websocket
            server.subscription_to_client[subscription.subscription_id] = client_id

        # Test different message types
        test_messages = [
            ("user.123.login", "single_wildcard"),  # Should match single wildcard
            ("user.456.login", "single_wildcard"),  # Should match single wildcard
            ("system.startup", "multi_wildcard"),  # Should match multi wildcard
            ("system.config.updated", "multi_wildcard"),  # Should match multi wildcard
            ("exact.topic", "exact_match"),  # Should match exact
            (
                "user.login",
                None,
            ),  # Should match nothing (wrong format for single wildcard)
            ("other.topic", None),  # Should match nothing
        ]

        for topic, expected_sub in test_messages:
            message = PubSubMessage(
                topic=topic,
                payload={"test": True},
                timestamp=time.time(),
                message_id=f"msg_{topic}",
                publisher="test",
            )

            notifications = server.topic_exchange.publish_message(message)

            if expected_sub:
                assert len(notifications) == 1
                assert notifications[0].subscription_id == expected_sub

                # Send notification
                await server._send_notification_to_client(notifications[0])
            else:
                assert len(notifications) == 0

        # Verify call counts
        assert mock_websocket1.send.call_count == 2  # 2 user.*.login messages
        assert mock_websocket2.send.call_count == 2  # 2 system.# messages
        assert mock_websocket3.send.call_count == 1  # 1 exact.topic message

    @pytest.mark.asyncio
    async def test_client_cleanup_on_failed_send(self):
        """Test that failed client connections are cleaned up."""
        settings = MPREGSettings(
            host="127.0.0.1", port=19203, name="TestServer", cluster_id="test-cluster"
        )
        server = MPREGServer(settings=settings)

        # Create a mock WebSocket that will fail
        mock_websocket = AsyncMock()
        mock_websocket.send.side_effect = Exception("Connection lost")

        # Set up subscription and client tracking
        subscription = PubSubSubscription(
            subscription_id="test_sub",
            patterns=(TopicPattern(pattern="test.*", exact_match=False),),
            subscriber="test_client",
            created_at=time.time(),
            get_backlog=False,
        )

        server.topic_exchange.add_subscription(subscription)
        client_id = "failed_client"
        server.pubsub_clients[client_id] = mock_websocket
        server.subscription_to_client["test_sub"] = client_id

        # Verify client is tracked
        assert client_id in server.pubsub_clients
        assert "test_sub" in server.subscription_to_client

        # Create and publish message
        message = PubSubMessage(
            topic="test.cleanup",
            payload={"test": "cleanup"},
            timestamp=time.time(),
            message_id="cleanup_msg",
            publisher="test",
        )

        notifications = server.topic_exchange.publish_message(message)
        assert len(notifications) == 1

        # Try to send notification (should fail and cleanup)
        await server._send_notification_to_client(notifications[0])

        # Client should be cleaned up after failed send
        assert client_id not in server.pubsub_clients
        assert "test_sub" not in server.subscription_to_client
