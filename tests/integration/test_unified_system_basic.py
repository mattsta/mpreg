#!/usr/bin/env python3
"""
Basic Fabric System Integration Tests using built-in commands.

This test suite validates basic MPREG fabric system functionality
using the built-in echo commands to ensure the infrastructure works
before testing fabric routing components.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, replace

import pytest

from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.fabric.catalog import FunctionEndpoint, NodeDescriptor, TransportEndpoint
from mpreg.fabric.catalog_delta import RoutingCatalogDelta
from mpreg.fabric.index import RoutingIndex
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
)
from mpreg.fabric.router import (
    FabricRouter,
    FabricRoutingConfig,
    create_correlation_id,
    is_control_plane_message,
    is_federation_message,
)


# Well-encapsulated basic test case dataclass
@dataclass(frozen=True, slots=True)
class BasicTestCase:
    """Basic fabric routing test case."""

    topic: str
    message_type: MessageType
    expected_control_plane: bool
    description: str = ""


# Core MPREG server and client imports (ACTUAL SYSTEM)
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer

# Test infrastructure imports
from tests.conftest import AsyncTestContext


class TestBasicUnifiedSystemIntegration:
    """
    Basic integration tests to verify fabric system infrastructure works.

    Uses built-in commands first to validate the test setup, then tests
    fabric routing components independently.
    """

    async def test_basic_cluster_formation(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """
        Test that basic MPREG cluster formation works with live servers.

        This validates our test infrastructure before testing fabric components.
        """
        port1, port2, port3 = server_cluster_ports[:3]

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

        server1 = MPREGServer(settings=settings1)
        server2 = MPREGServer(settings=settings2)

        test_context.servers.extend([server1, server2])

        task1 = asyncio.create_task(server1.server())
        test_context.tasks.append(task1)
        await asyncio.sleep(0.3)

        task2 = asyncio.create_task(server2.server())
        test_context.tasks.append(task2)

        await asyncio.sleep(1.0)

        client1 = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        client2 = MPREGClientAPI(f"ws://127.0.0.1:{port2}")

        test_context.clients.extend([client1, client2])

        await client1.connect()
        await client2.connect()

        result1 = await client1.call("echo", "test_server_1")
        assert result1 == "test_server_1"

        result2 = await client2.call("echo", "test_server_2")
        assert result2 == "test_server_2"

        print(f"✓ Basic cluster formation works: {len(test_context.servers)} servers")

    async def test_fabric_routing_components_standalone(
        self,
        test_context: AsyncTestContext,
    ):
        """
        Test FabricRouter components work standalone (without server integration).
        """
        config = FabricRoutingConfig(
            enable_topic_rpc=True,
            enable_queue_topics=True,
            enable_federation_optimization=True,
        )

        routing_index = RoutingIndex()
        routing_index.catalog.functions.register(
            FunctionEndpoint(
                identity=FunctionIdentity(
                    name="test",
                    function_id="fn-test",
                    version=SemanticVersion(1, 0, 0),
                ),
                resources=frozenset(),
                node_id=config.local_node_id,
                cluster_id=config.local_cluster_id,
            )
        )
        router = FabricRouter(config, routing_index=routing_index)

        test_cases: list[BasicTestCase] = [
            BasicTestCase(
                topic="mpreg.rpc.test.standalone",
                message_type=MessageType.RPC,
                expected_control_plane=False,
                description="RPC routing test",
            ),
            BasicTestCase(
                topic="mpreg.pubsub.notifications",
                message_type=MessageType.PUBSUB,
                expected_control_plane=False,
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
                message_type=MessageType.CONTROL,
                expected_control_plane=True,
                description="Control plane routing test",
            ),
        ]

        for test_case in test_cases:
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
                priority=RoutingPriority.NORMAL,
            )

            message = UnifiedMessage(
                message_id=correlation_id,
                topic=test_case.topic,
                message_type=test_case.message_type,
                delivery=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={"test": "standalone"},
                headers=headers,
                timestamp=time.time(),
            )

            route_result = await router.route_message(message)

            assert route_result.route_id is not None
            assert len(route_result.targets) >= 1
            assert route_result.estimated_latency_ms > 0
            assert isinstance(route_result.federation_required, bool)

            assert is_control_plane_message(message) == test_case.expected_control_plane
            assert message.headers.correlation_id == correlation_id

            is_mpreg_topic = message.topic.startswith("mpreg.")
            print(
                f"✓ Fabric routing: {test_case.topic} → {test_case.message_type.value} (mpreg_topic: {is_mpreg_topic}, control_plane: {is_control_plane_message(message)}, federation: {route_result.federation_required})"
            )

    async def test_peer_connections_use_transport_endpoints(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ) -> None:
        """Ensure peer connections dial advertised transport endpoints."""
        port_a, port_b = server_cluster_ports[:2]

        settings_a = MPREGSettings(
            host="127.0.0.1",
            port=port_a,
            name="Endpoint-A",
            cluster_id="transport-endpoints",
            resources={"server-a"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=0.4,
        )
        settings_b = MPREGSettings(
            host="127.0.0.1",
            port=port_b,
            name="Endpoint-B",
            cluster_id="transport-endpoints",
            resources={"server-b"},
            peers=None,
            connect=f"ws://127.0.0.1:{port_a}",
            advertised_urls=None,
            gossip_interval=0.4,
        )

        server_a = MPREGServer(settings=settings_a)
        server_b = MPREGServer(settings=settings_b)
        test_context.servers.extend([server_a, server_b])

        task_a = asyncio.create_task(server_a.server())
        task_b = asyncio.create_task(server_b.server())
        test_context.tasks.extend([task_a, task_b])

        await asyncio.sleep(1.0)

        if not server_b._fabric_control_plane:
            pytest.fail("Fabric control plane missing for transport endpoint test")

        peer_url = f"ws://127.0.0.1:{port_b}"
        alternate_url = f"ws://localhost:{port_b}"
        now = time.time()
        node = NodeDescriptor(
            node_id=peer_url,
            cluster_id=settings_b.cluster_id,
            resources=frozenset(settings_b.resources or set()),
            capabilities=frozenset({"rpc"}),
            transport_endpoints=(
                TransportEndpoint(
                    connection_type="internal",
                    protocol="ws",
                    host="localhost",
                    port=port_b,
                ),
            ),
            advertised_at=now,
            ttl_seconds=settings_b.fabric_catalog_ttl_seconds,
        )
        delta = RoutingCatalogDelta(
            update_id=str(uuid.uuid4()),
            cluster_id=settings_b.cluster_id,
            sent_at=now,
            nodes=(node,),
        )
        await server_b._fabric_control_plane.broadcaster.broadcast(delta, now=now)

        deadline = time.time() + 5.0
        while time.time() < deadline:
            directory = server_a._peer_directory
            entry = directory.node_for_id(peer_url) if directory else None
            if entry and entry.transport_endpoints:
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Timed out waiting for transport endpoints to propagate")

        existing = server_a.peer_connections.pop(peer_url, None)
        if existing:
            await existing.disconnect()

        deadline = time.time() + 5.0
        while time.time() < deadline:
            connection = server_a.peer_connections.get(peer_url)
            if connection and connection.is_connected:
                assert connection.url == alternate_url
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Peer connection did not re-establish via transport endpoint")

    async def test_fabric_message_creation_and_properties(self):
        """
        Test UnifiedMessage creation and property checks for fabric routing.
        """
        control_headers = MessageHeaders(
            correlation_id=create_correlation_id(),
        )

        control_message = UnifiedMessage(
            message_id="msg_control",
            topic="mpreg.system.control",
            message_type=MessageType.CONTROL,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"control": "test"},
            headers=control_headers,
        )

        assert is_control_plane_message(control_message) is True
        assert is_federation_message(control_message) is False

        federation_headers = MessageHeaders(
            correlation_id=create_correlation_id(),
            target_cluster="remote-cluster",
            hop_budget=1,
        )

        federation_message = UnifiedMessage(
            message_id="msg_fed",
            topic="mpreg.fabric.test",
            message_type=MessageType.CONTROL,
            delivery=DeliveryGuarantee.QUORUM,
            payload={"federation": "test"},
            headers=federation_headers,
        )

        assert is_federation_message(federation_message) is True
        assert federation_message.headers.hop_budget == 1
        assert federation_message.headers.target_cluster == "remote-cluster"

        data_headers = MessageHeaders(
            correlation_id=create_correlation_id(),
        )

        data_message = UnifiedMessage(
            message_id="msg_data",
            topic="user.notifications.email",
            message_type=MessageType.DATA,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"user_id": "12345", "email": "test@example.com"},
            headers=data_headers,
        )

        assert is_control_plane_message(data_message) is False
        assert is_federation_message(data_message) is False

        enhanced_message = replace(
            data_message,
            headers=replace(
                data_message.headers,
                metadata={**data_message.headers.metadata, "priority": "high"},
            ),
        )
        assert enhanced_message.headers.metadata["priority"] == "high"
        assert enhanced_message.topic == data_message.topic

        print("✓ UnifiedMessage creation and properties work correctly")

        assert is_control_plane_message(control_message) is True
        assert is_control_plane_message(federation_message) is True
        assert is_control_plane_message(data_message) is False

    async def test_correlation_id_generation_and_uniqueness(self):
        """
        Test correlation ID generation works and produces unique IDs.
        """
        correlation_ids = [create_correlation_id() for _ in range(100)]

        for corr_id in correlation_ids:
            assert corr_id.startswith("corr_")
            assert len(corr_id) > 10

        assert len(set(correlation_ids)) == 100

        print(f"✓ Generated 100 unique correlation IDs: {correlation_ids[:3]}...")

    async def test_routing_statistics_and_metrics(
        self,
        test_context: AsyncTestContext,
    ):
        """
        Test that routing statistics and metrics collection works.
        """
        config = FabricRoutingConfig()
        router = FabricRouter(config)

        for i in range(10):
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
            )

            message = UnifiedMessage(
                message_id=f"msg_metrics_{i}",
                topic=f"test.metrics.message_{i}",
                message_type=MessageType.PUBSUB,
                delivery=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={"test": i},
                headers=headers,
            )

            await router.route_message(message)

        stats = await router.get_routing_statistics()

        assert stats.total_routes_computed >= 10
        assert stats.pubsub_routes >= 10
        assert stats.average_route_computation_ms > 0
        assert stats.cache_hit_ratio >= 0.0

        print(
            f"✓ Routing statistics: {stats.total_routes_computed} routes, {stats.average_route_computation_ms:.2f}ms avg"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
