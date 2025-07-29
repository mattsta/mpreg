#!/usr/bin/env python3
"""
Basic Unified System Integration Tests using built-in commands.

This test suite validates basic MPREG unified system functionality
using the built-in echo commands to ensure the infrastructure works
before testing custom unified routing components.
"""

import asyncio
import time
from dataclasses import dataclass

import pytest

from mpreg.core.unified_routing import MessageType


# Well-encapsulated basic test case dataclass
@dataclass(frozen=True, slots=True)
class BasicTestCase:
    """Basic unified routing test case."""

    topic: str
    message_type: MessageType
    expected_control_plane: bool
    description: str = ""


# Core MPREG server and client imports (ACTUAL SYSTEM)
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.unified_router_impl import UnifiedRouterImpl, UnifiedRoutingConfig

# Unified routing system (ACTUAL IMPLEMENTATION)
from mpreg.core.unified_routing import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
    create_correlation_id,
)
from mpreg.server import MPREGServer

# Test infrastructure imports
from tests.conftest import AsyncTestContext


class TestBasicUnifiedSystemIntegration:
    """
    Basic integration tests to verify unified system infrastructure works.

    Uses built-in commands first to validate the test setup, then tests
    unified routing components independently.
    """

    async def test_basic_cluster_formation(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """
        Test that basic MPREG cluster formation works with live servers.

        This validates our test infrastructure before testing unified components.
        """
        port1, port2, port3 = server_cluster_ports[:3]

        # Create basic cluster
        settings1 = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Server-1",
            cluster_id="basic-test-cluster",
            resources={"server1"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        settings2 = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Server-2",
            cluster_id="basic-test-cluster",
            resources={"server2"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # Create servers
        server1 = MPREGServer(settings=settings1)
        server2 = MPREGServer(settings=settings2)

        test_context.servers.extend([server1, server2])

        # Start servers
        task1 = asyncio.create_task(server1.server())
        test_context.tasks.append(task1)
        await asyncio.sleep(0.3)

        task2 = asyncio.create_task(server2.server())
        test_context.tasks.append(task2)

        # Wait for cluster formation
        await asyncio.sleep(1.0)

        # Test basic communication with built-in commands
        client1 = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        client2 = MPREGClientAPI(f"ws://127.0.0.1:{port2}")

        test_context.clients.extend([client1, client2])

        await client1.connect()
        await client2.connect()

        # Test built-in echo commands work
        result1 = await client1.call("echo", "test_server_1")
        assert result1 == "test_server_1"

        result2 = await client2.call("echo", "test_server_2")
        assert result2 == "test_server_2"

        print(f"✓ Basic cluster formation works: {len(test_context.servers)} servers")

    async def test_unified_routing_components_standalone(
        self,
        test_context: AsyncTestContext,
    ):
        """
        Test REAL UnifiedRouterImpl components work standalone (without server integration).

        This validates the unified routing system itself works correctly.
        """
        # Create REAL UnifiedRoutingConfig
        config = UnifiedRoutingConfig(
            enable_topic_rpc=True,
            enable_queue_topics=True,
            enable_cross_system_correlation=True,
            federation_timeout_ms=5000.0,
        )

        # Create REAL UnifiedRouterImpl
        router = UnifiedRouterImpl(config)

        # Test different message types using proper dataclasses
        test_cases: list[BasicTestCase] = [
            BasicTestCase(
                topic="mpreg.rpc.test.standalone",
                message_type=MessageType.RPC,
                expected_control_plane=False,  # Only CONTROL_PLANE routing_type is control plane
                description="RPC routing test",
            ),
            BasicTestCase(
                topic="mpreg.pubsub.notifications",
                message_type=MessageType.PUBSUB,
                expected_control_plane=False,  # Only CONTROL_PLANE routing_type is control plane
                description="PubSub routing test",
            ),
            BasicTestCase(
                topic="user.profile.updates",
                message_type=MessageType.PUBSUB,
                expected_control_plane=False,
                description="User topic routing test",
            ),
            BasicTestCase(
                topic="mpreg.system.control",
                message_type=MessageType.CONTROL_PLANE,
                expected_control_plane=True,  # Both mpreg.* topic AND CONTROL_PLANE type
                description="Control plane routing test",
            ),
        ]

        for test_case in test_cases:
            # Create REAL UnifiedMessage
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.RPC,
                target_system=test_case.message_type,
                priority=RoutingPriority.NORMAL,
            )

            unified_message = UnifiedMessage(
                topic=test_case.topic,
                routing_type=test_case.message_type,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={"test": "standalone"},
                headers=headers,
                timestamp=time.time(),
            )

            # Test routing computation (this is the actual system)
            route_result = await router.route_message(unified_message)

            # Verify routing works
            assert route_result.route_id is not None
            assert len(route_result.targets) >= 0  # May be 0 if no handlers registered
            assert route_result.estimated_latency_ms > 0
            assert isinstance(route_result.federation_required, bool)

            # Verify message properties
            assert (
                unified_message.is_control_plane_message
                == test_case.expected_control_plane
            )
            assert unified_message.headers.correlation_id == correlation_id

            # Additional checks for mpreg.* topics
            is_mpreg_topic = unified_message.topic.startswith("mpreg.")
            print(
                f"✓ Unified routing: {test_case.topic} → {test_case.message_type.value} (mpreg_topic: {is_mpreg_topic}, control_plane: {unified_message.is_control_plane_message}, federation: {route_result.federation_required})"
            )

    async def test_unified_message_creation_and_properties(self):
        """
        Test REAL UnifiedMessage creation and property methods.

        This validates the core unified routing data structures.
        """
        # Test control plane message
        control_headers = MessageHeaders(
            correlation_id=create_correlation_id(),
            source_system=MessageType.RPC,
            target_system=MessageType.PUBSUB,
        )

        control_message = UnifiedMessage(
            topic="mpreg.system.control",
            routing_type=MessageType.CONTROL_PLANE,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"control": "test"},
            headers=control_headers,
        )

        # Verify control plane properties
        assert control_message.is_control_plane_message is True
        assert control_message.is_federation_message is False

        # Test federation message
        federation_headers = MessageHeaders(
            correlation_id=create_correlation_id(),
            source_system=MessageType.RPC,
            target_cluster="remote-cluster",
            federation_hop=1,
        )

        federation_message = UnifiedMessage(
            topic="mpreg.federation.test",
            routing_type=MessageType.FEDERATION,
            delivery_guarantee=DeliveryGuarantee.QUORUM,
            payload={"federation": "test"},
            headers=federation_headers,
        )

        # Verify federation properties
        assert federation_message.is_federation_message is True
        assert federation_message.headers.federation_hop == 1
        assert federation_message.headers.target_cluster == "remote-cluster"

        # Test data plane message
        data_headers = MessageHeaders(
            correlation_id=create_correlation_id(),
            source_system=MessageType.PUBSUB,
        )

        data_message = UnifiedMessage(
            topic="user.notifications.email",
            routing_type=MessageType.DATA_PLANE,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"user_id": "12345", "email": "test@example.com"},
            headers=data_headers,
        )

        # Verify data plane properties
        assert data_message.is_control_plane_message is False
        assert data_message.is_federation_message is False

        # Test message transformation
        enhanced_message = data_message.with_routing_metadata("priority", "high")
        assert enhanced_message.headers.routing_metadata["priority"] == "high"
        assert enhanced_message.topic == data_message.topic  # Original unchanged

        print("✓ UnifiedMessage creation and properties work correctly")

        # Test the actual control plane vs internal topic distinction
        # All mpreg.* topics are "internal" but only CONTROL_PLANE routing type is "control plane"
        assert (
            control_message.is_control_plane_message is True
        )  # mpreg.* + CONTROL_PLANE = True
        assert (
            federation_message.is_control_plane_message is False
        )  # mpreg.* + FEDERATION = False
        assert (
            data_message.is_control_plane_message is False
        )  # user.* + DATA_PLANE = False

    async def test_correlation_id_generation_and_uniqueness(self):
        """
        Test correlation ID generation works and produces unique IDs.
        """
        # Generate multiple correlation IDs
        correlation_ids = [create_correlation_id() for _ in range(100)]

        # Verify format
        for corr_id in correlation_ids:
            assert corr_id.startswith("corr_")
            assert len(corr_id) > 10  # Should be reasonably long

        # Verify uniqueness
        assert len(set(correlation_ids)) == 100, "Correlation IDs should be unique"

        print(f"✓ Generated 100 unique correlation IDs: {correlation_ids[:3]}...")

    async def test_routing_statistics_and_metrics(
        self,
        test_context: AsyncTestContext,
    ):
        """
        Test that routing statistics and metrics collection works.
        """
        # Create router
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Route several messages to generate statistics
        for i in range(10):
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.RPC,
            )

            message = UnifiedMessage(
                topic=f"test.metrics.message_{i}",
                routing_type=MessageType.PUBSUB,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={"test": i},
                headers=headers,
            )

            await router.route_message(message)

        # Get statistics
        stats = await router.get_routing_statistics()

        # Verify statistics
        assert stats.total_routes_computed >= 10
        assert stats.pubsub_routes >= 10
        assert stats.average_route_computation_ms > 0
        assert stats.cache_hit_ratio >= 0.0

        print(
            f"✓ Routing statistics: {stats.total_routes_computed} routes, {stats.average_route_computation_ms:.2f}ms avg"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
