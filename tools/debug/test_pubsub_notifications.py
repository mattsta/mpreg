#!/usr/bin/env python3
"""
Test script to verify PubSub notification delivery works properly.
"""

import asyncio
import contextlib
import time

from mpreg.client.pubsub_client import MPREGPubSubExtendedClient
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_basic_pubsub_notifications():
    """Test that PubSub notifications are properly delivered to clients."""
    print("=" * 60)
    print("🧪 TESTING PUBSUB NOTIFICATION DELIVERY")
    print("=" * 60)

    # Start server
    settings = MPREGSettings(
        host="127.0.0.1", port=19000, name="TestServer", cluster_id="test-cluster"
    )
    server = MPREGServer(settings=settings)

    print("🚀 Starting server...")
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(1.0)  # Give server time to start

    try:
        # Create client
        client = MPREGPubSubExtendedClient("ws://127.0.0.1:19000")

        print("🔗 Connecting client...")
        await client.connect()

        # Set up message tracking
        received_messages = []

        def message_callback(message):
            print(f"📨 Received message: {message.topic} -> {message.payload}")
            received_messages.append(message)

        # Subscribe to test topics
        print("📋 Creating subscription...")
        subscription_id = await client.subscribe(
            patterns=["test.*", "hello.world"],
            callback=message_callback,
            get_backlog=False,
        )
        print(f"✅ Subscription created: {subscription_id}")

        # Wait a moment for subscription to be active
        await asyncio.sleep(0.5)

        # Publish test messages
        print("📤 Publishing test messages...")

        # Message 1
        success1 = await client.publish(
            "test.message1", {"data": "Hello from test 1", "timestamp": time.time()}
        )
        print(f"Message 1 published: {success1}")

        # Message 2
        success2 = await client.publish(
            "hello.world", {"data": "Hello World!", "timestamp": time.time()}
        )
        print(f"Message 2 published: {success2}")

        # Message 3 (should not match subscription)
        success3 = await client.publish(
            "other.topic", {"data": "Should not receive this", "timestamp": time.time()}
        )
        print(f"Message 3 published: {success3}")

        # Wait for notifications to be processed
        print("⏰ Waiting for notifications...")
        await asyncio.sleep(2.0)

        # Check results
        print("\n📊 RESULTS:")
        print(f"Messages received: {len(received_messages)}")
        for i, msg in enumerate(received_messages):
            print(f"  {i + 1}. Topic: {msg.topic}, Payload: {msg.payload}")

        # Validate results
        if len(received_messages) == 2:
            topics = [msg.topic for msg in received_messages]
            if "test.message1" in topics and "hello.world" in topics:
                print("🎉 SUCCESS: All expected notifications received!")
                return True
            else:
                print(
                    f"❌ FAILURE: Wrong topics received. Expected: ['test.message1', 'hello.world'], Got: {topics}"
                )
                return False
        else:
            print(f"❌ FAILURE: Expected 2 messages, got {len(received_messages)}")
            return False

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Cleanup
        try:
            await client.disconnect()
        except (ConnectionError, asyncio.CancelledError):
            # Client disconnection errors during cleanup are expected
            pass
        server_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server_task


if __name__ == "__main__":
    result = asyncio.run(test_basic_pubsub_notifications())
    if result:
        print("\n✅ Test PASSED")
        exit(0)
    else:
        print("\n❌ Test FAILED")
        exit(1)
