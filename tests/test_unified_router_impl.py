"""
Comprehensive tests for the UnifiedRouter implementation.

This test suite verifies the concrete UnifiedRouter implementation's integration
with MPREG's existing federation infrastructure while maintaining the unified
routing interface across RPC, Topic Pub/Sub, and Message Queue systems.

Test categories:
1. Basic Routing Functionality: Core routing operations and message handling
2. Federation Integration: Integration with GraphBasedFederationRouter
3. Cache Performance: Route caching and cache invalidation
4. Policy Application: Routing policy enforcement and filtering
5. System Integration: Integration with TopicExchange and MessageQueue
6. Error Handling: Graceful degradation and error recovery
"""

import time
from unittest.mock import AsyncMock, Mock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.unified_router_impl import (
    RouteHandlerRegistry,
    RoutingMetrics,
    UnifiedRouterImpl,
    create_unified_router,
)
from mpreg.core.unified_routing import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RouteResult,
    RouteTarget,
    RoutingPriority,
    UnifiedMessage,
    UnifiedRoutingConfig,
    create_correlation_id,
)

# Import strategies from the unified routing tests
from .test_unified_routing import (
    routing_configs,
    unified_messages,
)


class TestRouteHandlerRegistry:
    """Test the route handler registry functionality."""

    def test_handler_registration(self):
        """Test basic handler registration."""
        registry = RouteHandlerRegistry()

        # Register a handler
        handler = Mock()
        patterns = ["user.*.events", "system.#"]
        asyncio_mock = AsyncMock()

        # Since register_handler is async, we need to run it
        import asyncio

        asyncio.run(
            registry.register_handler(
                "test_handler", handler, patterns, MessageType.PUBSUB
            )
        )

        assert "test_handler" in registry.handlers
        assert registry.handlers["test_handler"] == handler
        assert "user.*.events" in registry.pattern_mappings
        assert "test_handler" in registry.pattern_mappings["user.*.events"]
        assert MessageType.PUBSUB in registry.system_mappings
        assert "test_handler" in registry.system_mappings[MessageType.PUBSUB]

    def test_pattern_matching(self):
        """Test pattern matching functionality."""
        registry = RouteHandlerRegistry()

        # Test pattern matching directly
        assert registry._pattern_matches("user.*.events", "user.123.events")
        assert registry._pattern_matches("user.#", "user.123.login.success")
        assert not registry._pattern_matches("user.*.events", "admin.123.events")

    @pytest.mark.asyncio
    async def test_get_handlers_for_message(self):
        """Test getting handlers for a message."""
        registry = RouteHandlerRegistry()

        # Register handlers
        handler1 = Mock()
        handler2 = Mock()

        await registry.register_handler(
            "handler1", handler1, ["user.*.events"], MessageType.PUBSUB
        )
        await registry.register_handler(
            "handler2", handler2, ["user.#"], MessageType.PUBSUB
        )

        # Create test message
        message = UnifiedMessage(
            topic="user.123.events",
            routing_type=MessageType.PUBSUB,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id=create_correlation_id(), source_system=MessageType.PUBSUB
            ),
        )

        # Get matching handlers
        handlers = await registry.get_handlers_for_message(message)

        # Both handlers should match
        handler_ids = [handler_id for handler_id, _ in handlers]
        assert "handler1" in handler_ids
        assert "handler2" in handler_ids


class TestRoutingMetrics:
    """Test routing metrics collection and reporting."""

    def test_metrics_recording(self):
        """Test basic metrics recording."""
        metrics = RoutingMetrics()

        # Create test route result
        route = RouteResult(
            route_id="test_route",
            targets=[RouteTarget(MessageType.RPC, "test_function")],
            routing_path=["node1"],
            federation_path=[],
            estimated_latency_ms=50.0,
            route_cost=10.0,
            federation_required=False,
            hops_required=1,
        )

        # Record metrics
        metrics.record_route_computation(route, 15.0, MessageType.RPC, used_cache=False)

        assert metrics.total_routes_computed == 1
        assert metrics.local_routes == 1
        assert metrics.federation_routes == 0
        assert metrics.rpc_routes == 1
        assert metrics.cache_misses == 1
        assert metrics.total_route_computation_time_ms == 15.0

    def test_statistics_snapshot(self):
        """Test statistics snapshot generation."""
        metrics = RoutingMetrics()

        # Record some metrics
        route = RouteResult(
            route_id="test_route",
            targets=[RouteTarget(MessageType.PUBSUB, "subscription")],
            routing_path=["node1", "node2"],
            federation_path=["cluster1", "cluster2"],
            estimated_latency_ms=100.0,
            route_cost=20.0,
            federation_required=True,
            hops_required=2,
        )

        metrics.record_route_computation(
            route, 25.0, MessageType.PUBSUB, used_cache=True
        )

        # Get snapshot
        snapshot = metrics.get_statistics_snapshot()

        assert snapshot.total_routes_computed == 1
        assert snapshot.federation_routes == 1
        assert snapshot.pubsub_routes == 1
        assert snapshot.cache_hit_ratio == 1.0
        assert snapshot.average_route_computation_ms == 25.0
        assert snapshot.federation_ratio == 1.0


class TestUnifiedRouterImpl:
    """Test the concrete UnifiedRouter implementation."""

    def test_router_initialization(self):
        """Test router initialization with default config."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        assert router.config == config
        assert isinstance(router.handler_registry, RouteHandlerRegistry)
        assert isinstance(router.metrics, RoutingMetrics)
        assert router.route_cache == {}

    def test_cache_key_generation(self):
        """Test cache key generation."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        message = UnifiedMessage(
            topic="test.topic",
            routing_type=MessageType.RPC,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id="test_corr", source_system=MessageType.RPC
            ),
        )

        cache_key = router._get_cache_key(message)
        expected = "test.topic:rpc:at_least_once"
        assert cache_key == expected

    def test_route_caching(self):
        """Test route caching and retrieval."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Create test route
        route = RouteResult(
            route_id="test_route",
            targets=[RouteTarget(MessageType.RPC, "test_function")],
            routing_path=["local"],
            federation_path=[],
            estimated_latency_ms=5.0,
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
        )

        cache_key = "test:rpc:at_least_once"

        # Cache the route
        router._cache_route(cache_key, route)

        # Retrieve from cache
        cached_route = router._get_cached_route(cache_key)
        assert cached_route == route

        # Test cache expiration
        router.config.routing_cache_ttl_ms = 1.0  # 1ms TTL
        time.sleep(0.002)  # Wait for expiration

        expired_route = router._get_cached_route(cache_key)
        assert expired_route is None

    @pytest.mark.asyncio
    async def test_local_route_computation(self):
        """Test local route computation."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        message = UnifiedMessage(
            topic="local.test",
            routing_type=MessageType.DATA_PLANE,
            delivery_guarantee=DeliveryGuarantee.FIRE_AND_FORGET,
            payload={"test": "data"},
            headers=MessageHeaders(
                correlation_id="test_corr", source_system=MessageType.RPC
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_local_route(message, policy, "test_route")

        assert route is not None
        assert route.route_id == "test_route"
        assert not route.federation_required
        assert route.is_local_route
        assert len(route.targets) == 1
        assert route.targets[0].system_type == MessageType.DATA_PLANE

    @pytest.mark.asyncio
    async def test_control_plane_priority_routing(self):
        """Test that control plane messages get priority routing."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        message = UnifiedMessage(
            topic="mpreg.rpc.command.123.started",
            routing_type=MessageType.CONTROL_PLANE,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id="test_corr",
                source_system=MessageType.RPC,
                priority=RoutingPriority.CRITICAL,
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_control_plane_route(message, policy, "test_route")

        assert route is not None
        assert route.estimated_latency_ms == 1.0  # Priority boost
        assert route.targets[0].priority_weight == 2.0  # Priority boost

    @pytest.mark.asyncio
    async def test_pattern_registration(self):
        """Test route pattern registration."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Register valid pattern
        await router.register_route_pattern("user.*.events", "test_handler")

        assert "user.*.events" in router.handler_registry.pattern_mappings
        assert (
            "test_handler" in router.handler_registry.pattern_mappings["user.*.events"]
        )

        # Test invalid pattern registration
        with pytest.raises(ValueError, match="Invalid topic pattern"):
            await router.register_route_pattern("invalid.**.pattern", "bad_handler")

    @pytest.mark.asyncio
    async def test_cache_invalidation(self):
        """Test route cache invalidation."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Add some cached routes
        route1 = RouteResult(
            route_id="route1",
            targets=[],
            routing_path=[],
            federation_path=[],
            estimated_latency_ms=5.0,
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
        )
        route2 = RouteResult(
            route_id="route2",
            targets=[],
            routing_path=[],
            federation_path=[],
            estimated_latency_ms=5.0,
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
        )

        router._cache_route("user.123.events:pubsub:at_least_once", route1)
        router._cache_route("admin.456.events:pubsub:at_least_once", route2)

        assert len(router.route_cache) == 2

        # Invalidate specific pattern
        await router.invalidate_routing_cache("user")

        assert len(router.route_cache) == 1
        assert "admin.456.events:pubsub:at_least_once" in router.route_cache

        # Invalidate all
        await router.invalidate_routing_cache()
        assert len(router.route_cache) == 0

    @pytest.mark.asyncio
    @given(unified_messages(), routing_configs())
    @settings(max_examples=50, deadline=5000)
    async def test_route_message_property(
        self, message: UnifiedMessage, config: UnifiedRoutingConfig
    ):
        """Property test for route_message method."""
        router = UnifiedRouterImpl(config)

        # Route the message
        route_result = await router.route_message(message)

        # Should always return a result for valid messages
        assert route_result is not None
        assert isinstance(route_result, RouteResult)
        assert route_result.route_id is not None
        assert len(route_result.targets) >= 1

        # Verify routing metrics were updated
        assert router.metrics.total_routes_computed >= 1

    @pytest.mark.asyncio
    async def test_statistics_collection(self):
        """Test comprehensive statistics collection."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Route some messages to generate statistics
        messages = [
            UnifiedMessage(
                topic="test.rpc.call",
                routing_type=MessageType.RPC,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={},
                headers=MessageHeaders(
                    correlation_id="rpc_corr", source_system=MessageType.RPC
                ),
            ),
            UnifiedMessage(
                topic="test.pubsub.event",
                routing_type=MessageType.PUBSUB,
                delivery_guarantee=DeliveryGuarantee.BROADCAST,
                payload={},
                headers=MessageHeaders(
                    correlation_id="pubsub_corr", source_system=MessageType.PUBSUB
                ),
            ),
        ]

        for message in messages:
            await router.route_message(message)

        # Get statistics
        stats = await router.get_routing_statistics()

        assert stats.total_routes_computed == 2
        assert stats.rpc_routes == 1
        assert stats.pubsub_routes == 1
        assert stats.cache_routes == 0  # No cache routes in this test
        assert stats.cache_hit_ratio >= 0.0
        assert stats.local_efficiency >= 0.0


class TestCacheRouting:
    """Test cache-specific routing functionality."""

    @pytest.mark.asyncio
    async def test_cache_invalidation_routing(self):
        """Test cache invalidation message routing."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        message = UnifiedMessage(
            topic="mpreg.cache.invalidation.user_data.user_123",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.BROADCAST,
            payload={"namespace": "user_data", "key": "user_123"},
            headers=MessageHeaders(
                correlation_id="cache_inv_corr",
                source_system=MessageType.CACHE,
                priority=RoutingPriority.HIGH,
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_cache_invalidation_route(
            message, policy, "cache_inv_route"
        )

        assert route is not None
        assert route.route_id == "cache_inv_route"
        assert len(route.targets) >= 1
        assert route.targets[0].system_type == MessageType.CACHE
        assert route.targets[0].target_id == "cache_manager"
        assert (
            route.targets[0].priority_weight == 2.0
        )  # High priority for cache consistency
        assert route.estimated_latency_ms == 2.0  # Very fast for cache operations
        assert not route.federation_required
        assert route.is_local_route

    @pytest.mark.asyncio
    async def test_cache_coordination_routing(self):
        """Test cache coordination message routing."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        message = UnifiedMessage(
            topic="mpreg.cache.coordination.replication.global",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"operation": "replication", "namespace": "global"},
            headers=MessageHeaders(
                correlation_id="cache_coord_corr", source_system=MessageType.CACHE
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_cache_coordination_route(
            message, policy, "cache_coord_route"
        )

        assert route is not None
        assert route.targets[0].target_id == "cache_coordinator"
        assert route.targets[0].priority_weight == 1.5  # High priority for coordination
        assert route.estimated_latency_ms == 3.0  # Fast coordination

    @pytest.mark.asyncio
    async def test_cache_gossip_routing(self):
        """Test cache gossip message routing."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        message = UnifiedMessage(
            topic="mpreg.cache.gossip.state.node_west_1",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.FIRE_AND_FORGET,
            payload={"node_id": "node_west_1", "state": "healthy"},
            headers=MessageHeaders(
                correlation_id="cache_gossip_corr", source_system=MessageType.CACHE
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_cache_gossip_route(
            message, policy, "cache_gossip_route"
        )

        assert route is not None
        assert route.targets[0].target_id == "cache_gossip"
        assert route.targets[0].priority_weight == 1.0  # Normal priority for gossip
        assert route.estimated_latency_ms == 5.0  # Relaxed timing for gossip

    @pytest.mark.asyncio
    async def test_cache_federation_routing(self):
        """Test cache federation routing with mock federation router."""
        config = UnifiedRoutingConfig(enable_federation_optimization=True)

        # Mock federation router
        mock_federation_router = Mock()
        mock_federation_router.find_optimal_path.return_value = ["cluster1", "cluster2"]
        mock_federation_router.get_comprehensive_statistics.return_value = Mock(
            routing_performance=Mock(average_computation_time_ms=15.0)
        )

        router = UnifiedRouterImpl(config, federation_router=mock_federation_router)

        message = UnifiedMessage(
            topic="mpreg.cache.federation.sync.west_coast.east_coast",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.QUORUM,
            payload={"source_cluster": "west_coast", "target_cluster": "east_coast"},
            headers=MessageHeaders(
                correlation_id="cache_fed_corr",
                source_system=MessageType.CACHE,
                source_cluster="west_coast",
                target_cluster="east_coast",
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_cache_federation_route(
            message, policy, "cache_fed_route"
        )

        # Should use federation routing for cross-cluster cache sync
        assert route is not None
        assert route.federation_required
        mock_federation_router.find_optimal_path.assert_called_once()

    @pytest.mark.asyncio
    async def test_cache_federation_fallback(self):
        """Test cache federation fallback to local routing."""
        config = UnifiedRoutingConfig()  # Federation disabled
        router = UnifiedRouterImpl(config)  # No federation router

        message = UnifiedMessage(
            topic="mpreg.cache.federation.sync.west_coast.east_coast",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id="cache_fed_fallback_corr",
                source_system=MessageType.CACHE,
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_cache_federation_route(
            message, policy, "cache_fed_fallback_route"
        )

        # Should fallback to local routing with priority boost
        assert route is not None
        assert not route.federation_required
        assert route.is_local_route
        assert route.targets[0].priority_weight == 2.0  # Priority boost

    @pytest.mark.asyncio
    async def test_cache_monitoring_routing(self):
        """Test cache monitoring and analytics routing."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Test cache events
        events_message = UnifiedMessage(
            topic="mpreg.cache.events.user_data.eviction",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.FIRE_AND_FORGET,
            payload={"namespace": "user_data", "event_type": "eviction"},
            headers=MessageHeaders(
                correlation_id="cache_events_corr", source_system=MessageType.CACHE
            ),
        )

        policy = config.get_policy_for_message(events_message)
        events_route = await router._compute_cache_monitoring_route(
            events_message, policy, "cache_events_route"
        )

        assert events_route is not None
        assert events_route.targets[0].target_id == "cache_monitor"
        assert (
            events_route.targets[0].priority_weight == 0.5
        )  # Lower priority for monitoring
        assert events_route.estimated_latency_ms == 10.0  # Relaxed timing for analytics

        # Test cache analytics
        analytics_message = UnifiedMessage(
            topic="mpreg.cache.analytics.hit_rate.global",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.FIRE_AND_FORGET,
            payload={"metric_type": "hit_rate", "namespace": "global"},
            headers=MessageHeaders(
                correlation_id="cache_analytics_corr", source_system=MessageType.CACHE
            ),
        )

        analytics_route = await router._compute_cache_monitoring_route(
            analytics_message, policy, "cache_analytics_route"
        )

        assert analytics_route is not None
        assert analytics_route.targets[0].target_id == "cache_monitor"

    @pytest.mark.asyncio
    async def test_cache_unknown_topic_routing(self):
        """Test cache routing for unknown topic patterns."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        message = UnifiedMessage(
            topic="mpreg.cache.unknown.topic.pattern",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id="cache_unknown_corr", source_system=MessageType.CACHE
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_cache_route(
            message, policy, "cache_unknown_route"
        )

        # Should fallback to local routing with cache priority
        assert route is not None
        assert not route.federation_required
        assert route.targets[0].priority_weight == 2.0  # Priority boost for cache

    @pytest.mark.asyncio
    async def test_cache_routing_integration(self):
        """Test complete cache routing integration."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Test routing different cache message types
        cache_messages = [
            UnifiedMessage(
                topic="mpreg.cache.invalidation.test.key123",
                routing_type=MessageType.CACHE,
                delivery_guarantee=DeliveryGuarantee.BROADCAST,
                payload={},
                headers=MessageHeaders(
                    correlation_id="cache_1", source_system=MessageType.CACHE
                ),
            ),
            UnifiedMessage(
                topic="mpreg.cache.coordination.sync.global",
                routing_type=MessageType.CACHE,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={},
                headers=MessageHeaders(
                    correlation_id="cache_2", source_system=MessageType.CACHE
                ),
            ),
            UnifiedMessage(
                topic="mpreg.cache.analytics.memory_usage.local",
                routing_type=MessageType.CACHE,
                delivery_guarantee=DeliveryGuarantee.FIRE_AND_FORGET,
                payload={},
                headers=MessageHeaders(
                    correlation_id="cache_3", source_system=MessageType.CACHE
                ),
            ),
        ]

        for message in cache_messages:
            route = await router.route_message(message)
            assert route is not None
            assert isinstance(route, RouteResult)
            assert len(route.targets) >= 1
            assert route.targets[0].system_type == MessageType.CACHE

        # Verify cache routing metrics were updated
        stats = await router.get_routing_statistics()
        assert stats.cache_routes == 3
        assert stats.total_routes_computed >= 3

    @pytest.mark.asyncio
    @given(
        st.sampled_from(
            [
                "mpreg.cache.invalidation.namespace.key",
                "mpreg.cache.coordination.operation.scope",
                "mpreg.cache.gossip.state.node_id",
                "mpreg.cache.federation.sync.source.target",
                "mpreg.cache.events.namespace.event_type",
                "mpreg.cache.analytics.metric.scope",
            ]
        )
    )
    @settings(max_examples=20, deadline=3000)
    async def test_cache_routing_property(self, topic_pattern: str):
        """Property test for cache routing correctness."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        message = UnifiedMessage(
            topic=topic_pattern,
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id=create_correlation_id(), source_system=MessageType.CACHE
            ),
        )

        route = await router.route_message(message)

        # All cache messages should route successfully
        assert route is not None
        assert isinstance(route, RouteResult)
        assert len(route.targets) >= 1
        assert all(target.system_type == MessageType.CACHE for target in route.targets)

        # Cache routes should be fast (local operations)
        assert route.estimated_latency_ms <= 10.0  # Maximum observed in implementation
        assert route.is_local_route  # Cache operations are local by default

    @pytest.mark.asyncio
    async def test_cache_metrics_tracking(self):
        """Test that cache routing metrics are properly tracked."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Route a cache message
        message = UnifiedMessage(
            topic="mpreg.cache.invalidation.test.metrics",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.BROADCAST,
            payload={},
            headers=MessageHeaders(
                correlation_id="cache_metrics_test", source_system=MessageType.CACHE
            ),
        )

        initial_stats = await router.get_routing_statistics()
        initial_cache_routes = initial_stats.cache_routes

        await router.route_message(message)

        final_stats = await router.get_routing_statistics()

        # Cache routes should have increased by 1
        assert final_stats.cache_routes == initial_cache_routes + 1
        assert (
            final_stats.total_routes_computed == initial_stats.total_routes_computed + 1
        )


class TestUnifiedRouterFactory:
    """Test the unified router factory function."""

    def test_factory_creation(self):
        """Test router creation via factory function."""
        config = UnifiedRoutingConfig()

        # Mock federation components
        mock_federation_router = Mock()
        mock_federation_bridge = Mock()
        mock_topic_exchange = Mock()
        mock_message_queue = Mock()

        router = create_unified_router(
            config=config,
            federation_router=mock_federation_router,
            federation_bridge=mock_federation_bridge,
            topic_exchange=mock_topic_exchange,
            message_queue=mock_message_queue,
        )

        assert isinstance(router, UnifiedRouterImpl)
        assert router.config == config
        assert router.federation_router == mock_federation_router
        assert router.federation_bridge == mock_federation_bridge
        assert router.topic_exchange == mock_topic_exchange
        assert router.message_queue == mock_message_queue

    def test_factory_with_minimal_config(self):
        """Test factory with minimal configuration."""
        config = UnifiedRoutingConfig()

        router = create_unified_router(config)

        assert isinstance(router, UnifiedRouterImpl)
        assert router.config == config
        assert router.federation_router is None
        assert router.federation_bridge is None


class TestFederationIntegration:
    """Test integration with existing federation infrastructure."""

    @pytest.mark.asyncio
    async def test_federation_route_computation_with_mock(self):
        """Test federation route computation with mocked federation router."""
        config = UnifiedRoutingConfig(enable_federation_optimization=True)

        # Mock federation router
        mock_federation_router = Mock()
        mock_federation_router.find_optimal_path.return_value = [
            "cluster1",
            "cluster2",
            "cluster3",
        ]
        mock_federation_router.get_comprehensive_statistics.return_value = Mock(
            routing_performance=Mock(average_computation_time_ms=25.0)
        )

        router = UnifiedRouterImpl(config, federation_router=mock_federation_router)

        # Create federation message
        message = UnifiedMessage(
            topic="test.federation.message",
            routing_type=MessageType.QUEUE,
            delivery_guarantee=DeliveryGuarantee.QUORUM,
            payload={},
            headers=MessageHeaders(
                correlation_id="fed_corr",
                source_system=MessageType.QUEUE,
                source_cluster="cluster1",
                target_cluster="cluster3",
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_federation_route(message, policy, "fed_route")

        assert route is not None
        assert route.federation_required
        assert route.routing_path == ["cluster1", "cluster2", "cluster3"]
        assert route.federation_path == ["cluster1", "cluster2", "cluster3"]
        assert route.hops_required == 2

        # Verify federation router was called
        mock_federation_router.find_optimal_path.assert_called_once_with(
            "cluster1",
            "cluster3",
            max_hops=5,  # Default policy max_federation_hops
        )

    @pytest.mark.asyncio
    async def test_federation_fallback_to_local(self):
        """Test fallback to local routing when federation fails."""
        config = UnifiedRoutingConfig(enable_federation_optimization=True)

        # Mock federation router that returns None (no path)
        mock_federation_router = Mock()
        mock_federation_router.find_optimal_path.return_value = None

        router = UnifiedRouterImpl(config, federation_router=mock_federation_router)

        message = UnifiedMessage(
            topic="test.federation.message",
            routing_type=MessageType.RPC,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id="fed_corr",
                source_system=MessageType.RPC,
                source_cluster="cluster1",
                target_cluster="unreachable_cluster",
            ),
        )

        policy = config.get_policy_for_message(message)
        route = await router._compute_federation_route(message, policy, "fed_route")

        # Should fallback to local routing
        assert route is not None
        assert not route.federation_required
        assert route.is_local_route


if __name__ == "__main__":
    pytest.main([__file__])
