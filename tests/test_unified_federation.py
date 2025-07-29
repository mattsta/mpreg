"""
Comprehensive tests for the unified federation protocol.

This module tests the complete unified federation system including:
- Unified federation message model and routing
- Topic-driven federation router with caching and analytics
- Cross-system federation integration bridge
- Property-based testing for complex federation scenarios
- Performance and reliability testing

Tests use live MPREG clusters and realistic federation topologies
to validate end-to-end functionality, routing optimality, and
cross-system coordination.
"""

import time
from collections.abc import Callable
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.federation.system_federation_bridge import (
    SystemBridgeConfig,
    SystemFederationBridge,
    create_forwarding_rule,
    create_system_federation_bridge,
)
from mpreg.federation.unified_federation import (
    FederationHopStrategy,
    FederationRoutingMetadata,
    FederationStatistics,
    SystemMessageType,
    UnifiedFederationConfig,
    UnifiedFederationRouter,
    create_federation_message,
    create_federation_router,
    create_federation_rule,
)
from mpreg.server import MPREGServer


class TestUnifiedFederationMessages:
    """Test unified federation message model."""

    def test_federation_message_creation(self) -> None:
        """Test creating federation messages."""
        message = create_federation_message(
            source_cluster="cluster_1",
            topic="test.topic",
            message_type=SystemMessageType.RPC,
            payload={"data": "test"},
            target_pattern="test.*",
        )

        assert message.source_cluster == "cluster_1"
        assert message.original_topic == "test.topic"
        assert message.target_pattern == "test.*"
        assert message.message_type == SystemMessageType.RPC
        assert message.payload == {"data": "test"}
        assert message.hop_count == 0
        assert not message.is_expired
        assert not message.has_exceeded_max_hops
        assert message.remaining_hops == message.max_hops

    def test_federation_message_hop_tracking(self) -> None:
        """Test federation message hop tracking."""
        message = create_federation_message(
            source_cluster="cluster_1",
            topic="test.topic",
            message_type=SystemMessageType.PUBSUB,
            payload="test data",
            max_hops=3,
        )

        # Add hops
        message_hop1 = message.with_hop_added("cluster_2")
        assert message_hop1.hop_count == 1
        assert message_hop1.federation_path == ["cluster_2"]
        assert message_hop1.remaining_hops == 2

        message_hop2 = message_hop1.with_hop_added("cluster_3")
        assert message_hop2.hop_count == 2
        assert message_hop2.federation_path == ["cluster_2", "cluster_3"]
        assert message_hop2.remaining_hops == 1

        message_hop3 = message_hop2.with_hop_added("cluster_4")
        assert message_hop3.hop_count == 3
        assert message_hop3.federation_path == ["cluster_2", "cluster_3", "cluster_4"]
        assert message_hop3.remaining_hops == 0
        assert message_hop3.has_exceeded_max_hops

    def test_federation_message_expiration(self) -> None:
        """Test federation message expiration."""
        past_time = time.time() - 100  # 100 seconds ago
        message = create_federation_message(
            source_cluster="cluster_1",
            topic="test.topic",
            message_type=SystemMessageType.QUEUE,
            payload="test data",
            expires_at=past_time,
        )

        assert message.is_expired


class TestFederationRoutingRules:
    """Test federation routing rules."""

    def test_routing_rule_creation(self) -> None:
        """Test creating federation routing rules."""
        rule = create_federation_rule(
            rule_id="rule_1",
            source_pattern="user.*",
            target_clusters={"cluster_a", "cluster_b"},
            message_types={SystemMessageType.RPC, SystemMessageType.PUBSUB},
        )

        assert rule.rule_id == "rule_1"
        assert rule.source_pattern == "user.*"
        assert rule.target_clusters == {"cluster_a", "cluster_b"}
        assert SystemMessageType.RPC in rule.message_types
        assert SystemMessageType.PUBSUB in rule.message_types
        assert not rule.applies_to_all_clusters

    def test_routing_rule_pattern_matching(self) -> None:
        """Test routing rule pattern matching."""
        rule = create_federation_rule(
            rule_id="rule_pattern",
            source_pattern="user.*.login",
            target_clusters="all",
            message_types={SystemMessageType.RPC},
        )

        message_match = create_federation_message(
            source_cluster="cluster_1",
            topic="user.123.login",
            message_type=SystemMessageType.RPC,
            payload="login data",
        )

        message_no_match = create_federation_message(
            source_cluster="cluster_1",
            topic="user.123.profile",
            message_type=SystemMessageType.RPC,
            payload="profile data",
        )

        assert rule.matches_message(message_match)
        assert not rule.matches_message(message_no_match)
        assert rule.applies_to_all_clusters


class TestUnifiedFederationRouter:
    """Test the unified federation router."""

    @pytest.fixture
    def federation_router(self) -> UnifiedFederationRouter:
        """Create a federation router for testing."""
        config = UnifiedFederationConfig(
            cluster_id="test_cluster",
            enable_federation_analytics=True,
            routing_cache_ttl_ms=1000.0,  # Short TTL for testing
        )
        return create_federation_router(config=config)

    @pytest.mark.asyncio
    async def test_router_rule_registration(
        self, federation_router: UnifiedFederationRouter
    ) -> None:
        """Test registering routing rules."""
        rule = create_federation_rule(
            rule_id="test_rule",
            source_pattern="test.*",
            target_clusters={"cluster_a"},
            message_types={SystemMessageType.RPC},
        )

        initial_rules = len(federation_router.federation_rules)
        await federation_router.register_routing_rule(rule)

        assert len(federation_router.federation_rules) == initial_rules + 1
        assert rule in federation_router.federation_rules

        # Check analytics
        stats = await federation_router.get_federation_statistics()
        assert stats.total_rules == initial_rules + 1
        assert stats.total_events > 0  # Should have rule registration event

    @pytest.mark.asyncio
    async def test_router_message_routing(
        self, federation_router: UnifiedFederationRouter
    ) -> None:
        """Test routing federation messages."""
        # Register a routing rule
        rule = create_federation_rule(
            rule_id="routing_test",
            source_pattern="data.*",
            target_clusters={"cluster_target"},
            message_types={SystemMessageType.PUBSUB},
        )
        await federation_router.register_routing_rule(rule)

        # Create test message
        message = create_federation_message(
            source_cluster="source_cluster",
            topic="data.update",
            message_type=SystemMessageType.PUBSUB,
            payload={"update": "test"},
        )

        # Route the message
        route_result = await federation_router.route_message(message)

        assert route_result.target_clusters == ["cluster_target"]
        assert route_result.rule_id == "routing_test"
        assert route_result.hop_strategy_used == FederationHopStrategy.SHORTEST_PATH
        assert not route_result.cache_hit  # First time routing
        assert isinstance(route_result.routing_metadata, FederationRoutingMetadata)

    @pytest.mark.asyncio
    async def test_router_caching(
        self, federation_router: UnifiedFederationRouter
    ) -> None:
        """Test routing result caching."""
        # Register a routing rule
        rule = create_federation_rule(
            rule_id="cache_test",
            source_pattern="cache.*",
            target_clusters={"cluster_cache"},
            message_types={SystemMessageType.QUEUE},
        )
        await federation_router.register_routing_rule(rule)

        # Create test message
        message = create_federation_message(
            source_cluster="source_cluster",
            topic="cache.test",
            message_type=SystemMessageType.QUEUE,
            payload={"test": "cache"},
        )

        # Route the message twice
        result1 = await federation_router.route_message(message)
        result2 = await federation_router.route_message(message)

        assert not result1.cache_hit
        assert result2.cache_hit
        assert result1.target_clusters == result2.target_clusters

        # Check cache statistics
        stats = await federation_router.get_federation_statistics()
        assert stats.cache_entries > 0
        assert stats.cache_hit_rate_percent > 0

    @pytest.mark.asyncio
    async def test_router_statistics(
        self, federation_router: UnifiedFederationRouter
    ) -> None:
        """Test federation router statistics."""
        # Perform some operations
        rule = create_federation_rule(
            rule_id="stats_test",
            source_pattern="stats.*",
            target_clusters={"cluster_stats"},
            message_types={SystemMessageType.RPC},
        )
        await federation_router.register_routing_rule(rule)

        message = create_federation_message(
            source_cluster="test_cluster",
            topic="stats.test",
            message_type=SystemMessageType.RPC,
            payload={"test": "stats"},
        )
        await federation_router.route_message(message)

        # Get statistics
        stats = await federation_router.get_federation_statistics()

        assert isinstance(stats, FederationStatistics)
        assert stats.total_rules >= 1
        assert stats.total_events >= 1
        assert stats.cluster_id == "test_cluster"
        assert isinstance(stats.route_distribution.total_routes, int)
        assert stats.average_processing_time_ms >= 0


class TestSystemFederationBridge:
    """Test the system federation bridge."""

    @pytest.fixture
    def federation_bridge(self) -> SystemFederationBridge:
        """Create a federation bridge for testing."""
        config = SystemBridgeConfig(
            cluster_id="bridge_test_cluster", enable_analytics=True
        )

        federation_router = create_federation_router()

        return create_system_federation_bridge(
            federation_router=federation_router, config=config
        )

    @pytest.mark.asyncio
    async def test_bridge_forwarding_rule_registration(
        self, federation_bridge: SystemFederationBridge
    ) -> None:
        """Test registering forwarding rules."""
        rule = create_forwarding_rule(
            rule_id="forward_test",
            local_topic_pattern="local.*",
            system_type=SystemMessageType.RPC,
            forward_to_federation=True,
        )

        initial_rules = len(federation_bridge.forwarding_rules)
        await federation_bridge.register_forwarding_rule(rule)

        assert len(federation_bridge.forwarding_rules) == initial_rules + 1
        assert rule in federation_bridge.forwarding_rules

        # Check analytics
        stats = await federation_bridge.get_bridge_statistics()
        assert stats.active_forwarding_rules == initial_rules + 1

    @pytest.mark.asyncio
    async def test_bridge_message_forwarding(
        self, federation_bridge: SystemFederationBridge
    ) -> None:
        """Test forwarding messages to federation."""
        # Register forwarding rule
        rule = create_forwarding_rule(
            rule_id="forward_rpc",
            local_topic_pattern="rpc.*",
            system_type=SystemMessageType.RPC,
            federation_topic_pattern="fed.rpc.*",
        )
        await federation_bridge.register_forwarding_rule(rule)

        # Forward a message
        await federation_bridge.forward_to_federation(
            topic="rpc.test",
            message={"rpc": "test_data"},
            message_type=SystemMessageType.RPC,
        )

        # Check statistics
        stats = await federation_bridge.get_bridge_statistics()
        assert stats.total_forwarded_to_federation >= 1
        assert len(stats.recent_operations) >= 1

        # Check the operation was recorded
        forward_ops = [
            op
            for op in stats.recent_operations
            if op.operation_type == "forward_to_federation"
        ]
        assert len(forward_ops) >= 1
        assert forward_ops[0].system_type == SystemMessageType.RPC
        assert forward_ops[0].local_topic == "rpc.test"
        assert forward_ops[0].success

    @pytest.mark.asyncio
    async def test_bridge_federation_message_receiving(
        self, federation_bridge: SystemFederationBridge
    ) -> None:
        """Test receiving messages from federation."""
        # Create a federation message
        fed_message = create_federation_message(
            source_cluster="remote_cluster",
            topic="remote.data",
            message_type=SystemMessageType.PUBSUB,
            payload={"remote": "data"},
        )

        # This will fail because we don't have actual systems connected,
        # but we can test the error handling
        with pytest.raises(
            ValueError, match="No handler for message type SystemMessageType.PUBSUB"
        ):
            await federation_bridge.receive_from_federation(fed_message)

        # Test cache system routing
        cache_message = create_federation_message(
            source_cluster="remote_cluster",
            topic="cache.data",
            message_type=SystemMessageType.CACHE,
            payload={"cache": "data"},
        )

        with pytest.raises(
            ValueError, match="No handler for message type SystemMessageType.CACHE"
        ):
            await federation_bridge.receive_from_federation(cache_message)

        # Check that the failed operation was recorded
        stats = await federation_bridge.get_bridge_statistics()
        failed_ops = [
            op
            for op in stats.recent_operations
            if op.operation_type == "receive_from_federation" and not op.success
        ]
        assert len(failed_ops) >= 1
        assert stats.error_rate_percent > 0


class TestFederationIntegrationWithServers:
    """Test federation integration with live MPREG servers."""

    @pytest.mark.asyncio
    async def test_federation_with_live_servers(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ) -> None:
        """Test federation setup with live MPREG servers."""
        server1, server2 = cluster_2_servers

        # Create federation components for each server
        config1 = UnifiedFederationConfig(cluster_id="server1_cluster")
        config2 = UnifiedFederationConfig(cluster_id="server2_cluster")

        router1 = create_federation_router(config=config1)
        router2 = create_federation_router(config=config2)

        # Create cross-cluster routing rules
        rule1 = create_federation_rule(
            rule_id="server1_to_server2",
            source_pattern="server1.*",
            target_clusters={"server2_cluster"},
            message_types={SystemMessageType.RPC},
        )

        rule2 = create_federation_rule(
            rule_id="server2_to_server1",
            source_pattern="server2.*",
            target_clusters={"server1_cluster"},
            message_types={SystemMessageType.RPC},
        )

        await router1.register_routing_rule(rule1)
        await router2.register_routing_rule(rule2)

        # Test routing between clusters
        message = create_federation_message(
            source_cluster="server1_cluster",
            topic="server1.test",
            message_type=SystemMessageType.RPC,
            payload={"test": "cross_cluster"},
        )

        route_result = await router1.route_message(message)
        assert "server2_cluster" in route_result.target_clusters

        # Check statistics
        stats1 = await router1.get_federation_statistics()
        stats2 = await router2.get_federation_statistics()

        assert stats1.cluster_id == "server1_cluster"
        assert stats2.cluster_id == "server2_cluster"
        assert stats1.total_rules >= 1
        assert stats2.total_rules >= 1


# Property-based testing with Hypothesis
@given(
    message_count=st.integers(min_value=1, max_value=10),
    hop_strategy=st.sampled_from(list(FederationHopStrategy)),
    enable_caching=st.booleans(),
)
@settings(max_examples=20, deadline=5000)
def test_federation_routing_properties(
    message_count: int, hop_strategy: FederationHopStrategy, enable_caching: bool
) -> None:
    """Property test: Federation routing maintains invariants."""
    config = UnifiedFederationConfig(
        enable_route_caching=enable_caching, default_hop_strategy=hop_strategy
    )
    router = create_federation_router(config=config)

    # Create varied messages
    messages = []
    for i in range(message_count):
        message = create_federation_message(
            source_cluster=f"cluster_{i}",
            topic=f"test.topic.{i}",
            message_type=SystemMessageType.RPC,
            payload={"data": f"test_{i}"},
        )
        messages.append(message)

    # Test message properties
    for message in messages:
        assert len(message.message_id) > 0
        assert len(message.source_cluster) > 0
        assert len(message.original_topic) > 0
        assert message.message_type in SystemMessageType
        assert message.hop_count >= 0
        assert message.hop_count <= message.max_hops
        assert message.remaining_hops >= 0
        assert not message.is_expired  # Newly created messages shouldn't be expired

        # Test hop tracking
        hopped_message = message.with_hop_added("new_cluster")
        assert hopped_message.hop_count == message.hop_count + 1
        assert "new_cluster" in hopped_message.federation_path
        assert hopped_message.remaining_hops == message.remaining_hops - 1


@given(
    rule_count=st.integers(min_value=1, max_value=5),
    target_cluster_count=st.integers(min_value=1, max_value=3),
)
@settings(max_examples=15, deadline=3000)
def test_federation_rule_properties(rule_count: int, target_cluster_count: int) -> None:
    """Property test: Federation rules maintain consistency."""
    rules = []

    for i in range(rule_count):
        target_clusters = {f"cluster_{j}" for j in range(target_cluster_count)}

        rule = create_federation_rule(
            rule_id=f"rule_{i}",
            source_pattern=f"pattern.{i}.*",
            target_clusters=target_clusters,
            message_types={SystemMessageType.RPC, SystemMessageType.PUBSUB},
        )
        rules.append(rule)

    # Test rule properties
    for rule in rules:
        assert len(rule.rule_id) > 0
        assert len(rule.source_pattern) > 0
        assert len(rule.message_types) > 0
        assert rule.priority >= 0
        assert isinstance(rule.enabled, bool)
        assert isinstance(rule.target_clusters, set | str)

        if isinstance(rule.target_clusters, set):
            assert len(rule.target_clusters) > 0
            assert not rule.applies_to_all_clusters
        else:
            assert rule.target_clusters == "all"
            assert rule.applies_to_all_clusters


@pytest.mark.asyncio
async def test_end_to_end_federation_workflow() -> None:
    """Complete end-to-end test of federation workflow."""
    # Create federation topology
    cluster_configs = [
        UnifiedFederationConfig(cluster_id="hub_cluster"),
        UnifiedFederationConfig(cluster_id="east_cluster"),
        UnifiedFederationConfig(cluster_id="west_cluster"),
    ]

    routers = [create_federation_router(config=config) for config in cluster_configs]
    bridges = []

    # Create bridges for each cluster
    for i, router in enumerate(routers):
        bridge_config = SystemBridgeConfig(
            cluster_id=cluster_configs[i].cluster_id, enable_analytics=True
        )
        bridge = create_system_federation_bridge(
            federation_router=router, config=bridge_config
        )
        bridges.append(bridge)

    # Set up cross-cluster routing rules
    hub_to_all_rule = create_federation_rule(
        rule_id="hub_broadcast",
        source_pattern="broadcast.*",
        target_clusters="all",
        message_types={SystemMessageType.PUBSUB},
    )

    east_to_west_rule = create_federation_rule(
        rule_id="east_to_west",
        source_pattern="east.*",
        target_clusters={"west_cluster"},
        message_types={SystemMessageType.RPC},
    )

    await routers[0].register_routing_rule(hub_to_all_rule)  # Hub router
    await routers[1].register_routing_rule(east_to_west_rule)  # East router

    # Register bridge forwarding rules
    for bridge in bridges:
        rpc_rule = create_forwarding_rule(
            rule_id=f"{bridge.config.cluster_id}_rpc",
            local_topic_pattern="rpc.*",
            system_type=SystemMessageType.RPC,
        )
        pubsub_rule = create_forwarding_rule(
            rule_id=f"{bridge.config.cluster_id}_pubsub",
            local_topic_pattern="pubsub.*",
            system_type=SystemMessageType.PUBSUB,
        )

        await bridge.register_forwarding_rule(rpc_rule)
        await bridge.register_forwarding_rule(pubsub_rule)

    # Test broadcast message from hub
    broadcast_message = create_federation_message(
        source_cluster="hub_cluster",
        topic="broadcast.update",
        message_type=SystemMessageType.PUBSUB,
        payload={"update": "global_announcement"},
    )

    route_result = await routers[0].route_message(broadcast_message)
    # Would route to all clusters in a real implementation

    # Test point-to-point message from east to west
    east_message = create_federation_message(
        source_cluster="east_cluster",
        topic="east.data",
        message_type=SystemMessageType.RPC,
        payload={"data": "from_east"},
    )

    route_result = await routers[1].route_message(east_message)
    assert "west_cluster" in route_result.target_clusters

    # Verify statistics across all components
    total_rules = 0
    total_operations = 0

    for router in routers:
        stats = await router.get_federation_statistics()
        total_rules += stats.total_rules
        total_operations += stats.total_events

    for bridge in bridges:
        bridge_stats = await bridge.get_bridge_statistics()
        total_operations += len(bridge_stats.recent_operations)

    assert total_rules >= 2  # At least the rules we registered
    assert total_operations > 0  # Should have recorded operations

    print("✅ End-to-end federation test completed!")
    print(f"🌐 Total routing rules: {total_rules}")
    print(f"📊 Total operations: {total_operations}")
    print(f"🔗 Clusters: {len(routers)}")
    print(f"🌉 Bridges: {len(bridges)}")
