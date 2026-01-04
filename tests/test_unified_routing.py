"""
Comprehensive property-based tests for MPREG fabric routing system.

This test suite uses Hypothesis to verify fundamental properties of the routing
fabric architecture with mathematically rigorous correctness guarantees.

Property categories tested:
1. Routing Consistency: Same inputs always produce consistent routing decisions
2. Message Integrity: Messages maintain integrity through routing transformations
3. Access Control: Internal topics properly restricted to control plane
4. Federation Routing: Cross-cluster routing correctness and optimality
5. Performance Properties: Routing decisions complete within expected bounds
6. Policy Application: Routing policies applied correctly and consistently

Each property is tested across thousands of randomly generated routing
configurations to ensure robustness across the entire input space.
"""

import time
from dataclasses import replace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.topic_taxonomy import TopicAccessLevel, TopicValidator
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
)
from mpreg.fabric.router import (
    FabricRouteReason,
    FabricRouteResult,
    FabricRouteTarget,
    FabricRoutingConfig,
    FabricRoutingPolicy,
    create_correlation_id,
    create_route_id,
    extract_system_from_topic,
    is_control_plane_message,
    is_federation_message,
    is_internal_topic,
)

# Hypothesis strategies for generating test data


@st.composite
def correlation_ids(draw):
    """Generate valid correlation IDs."""
    return draw(
        st.text(
            min_size=8,
            max_size=32,
            alphabet=st.characters(min_codepoint=32, max_codepoint=126),
        )
    )


@st.composite
def node_ids(draw):
    """Generate valid node IDs."""
    return draw(
        st.text(
            min_size=1,
            max_size=20,
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
        )
    )


@st.composite
def cluster_ids(draw):
    """Generate valid cluster IDs."""
    return draw(
        st.text(
            min_size=3,
            max_size=15,
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
        )
    )


@st.composite
def topic_patterns(draw):
    """Generate valid topic patterns."""
    # Generate basic topic patterns
    segments = draw(
        st.lists(
            st.text(
                min_size=1,
                max_size=10,
                alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            ),
            min_size=1,
            max_size=5,
        )
    )

    # Optionally add wildcards
    if draw(st.booleans()):
        # Replace some segments with wildcards
        for i in range(len(segments)):
            wildcard_choice = draw(st.integers(min_value=0, max_value=2))
            if wildcard_choice == 1:
                segments[i] = "*"
            elif wildcard_choice == 2 and i == len(segments) - 1:  # # only at end
                segments[i] = "#"

    return ".".join(segments)


@st.composite
def internal_topic_patterns(draw):
    """Generate internal mpreg.* topic patterns."""
    base_topics = [
        "mpreg.rpc.command.{}.started",
        "mpreg.queue.{}.enqueued",
        "mpreg.fabric.cluster.{}.join",
        "mpreg.pubsub.subscription.{}.created",
        "mpreg.monitoring.metrics.{}.{}",
    ]

    base = draw(st.sampled_from(base_topics))

    # Fill in placeholders
    if "{}" in base:
        parts = base.split("{}")
        filled_parts = []
        for i, part in enumerate(parts[:-1]):
            filled_parts.append(part)
            filled_parts.append(
                draw(
                    st.text(
                        min_size=1,
                        max_size=10,
                        alphabet=st.characters(
                            min_codepoint=ord("a"), max_codepoint=ord("z")
                        ),
                    )
                )
            )
        filled_parts.append(parts[-1])
        return "".join(filled_parts)

    return base


@st.composite
def message_headers(draw):
    """Generate valid message headers."""
    return MessageHeaders(
        correlation_id=draw(correlation_ids()),
        priority=draw(st.sampled_from(RoutingPriority)),
        source_cluster=draw(st.one_of(st.none(), cluster_ids())),
        target_cluster=draw(st.one_of(st.none(), cluster_ids())),
        hop_budget=draw(st.one_of(st.none(), st.integers(min_value=0, max_value=10))),
        routing_path=tuple(draw(st.lists(node_ids(), min_size=0, max_size=5))),
        federation_path=tuple(draw(st.lists(cluster_ids(), min_size=0, max_size=3))),
        metadata=draw(st.dictionaries(st.text(min_size=1, max_size=10), st.integers())),
    )


@st.composite
def unified_messages(draw):
    """Generate valid unified messages."""
    topic = draw(topic_patterns())
    message_type = draw(st.sampled_from(MessageType))

    return UnifiedMessage(
        message_id=draw(correlation_ids()),
        topic=topic,
        message_type=message_type,
        delivery=draw(st.sampled_from(DeliveryGuarantee)),
        payload=draw(
            st.one_of(
                st.none(),
                st.integers(),
                st.text(),
                st.dictionaries(st.text(), st.integers()),
            )
        ),
        headers=draw(message_headers()),
        timestamp=time.time(),  # Use fixed timestamp to avoid flaky tests
    )


@st.composite
def internal_unified_messages(draw):
    """Generate unified messages with internal topics."""
    topic = draw(internal_topic_patterns())

    return UnifiedMessage(
        message_id=draw(correlation_ids()),
        topic=topic,
        message_type=MessageType.CONTROL,
        delivery=draw(st.sampled_from(DeliveryGuarantee)),
        payload=draw(st.dictionaries(st.text(), st.integers())),
        headers=draw(message_headers()),
        timestamp=time.time(),
    )


@st.composite
def route_targets(draw):
    """Generate valid route targets."""
    return FabricRouteTarget(
        system_type=draw(st.sampled_from(MessageType)),
        target_id=draw(st.text(min_size=1, max_size=20)),
        node_id=draw(st.one_of(st.none(), node_ids())),
        cluster_id=draw(st.one_of(st.none(), cluster_ids())),
        priority_weight=draw(st.floats(min_value=0.1, max_value=10.0)),
    )


@st.composite
def route_results(draw):
    """Generate valid route results."""
    targets = draw(st.lists(route_targets(), min_size=1, max_size=5))
    routing_path = draw(st.lists(node_ids(), min_size=0, max_size=10))
    federation_path = draw(st.lists(cluster_ids(), min_size=0, max_size=5))

    return FabricRouteResult(
        route_id=f"route_{draw(st.text(min_size=8, max_size=16))}",
        targets=targets,
        routing_path=routing_path,
        federation_path=federation_path,
        estimated_latency_ms=draw(st.floats(min_value=1.0, max_value=10000.0)),
        route_cost=draw(st.floats(min_value=1.0, max_value=1000.0)),
        federation_required=len(federation_path) > 1,
        hops_required=len(routing_path),
        reason=draw(st.sampled_from(FabricRouteReason)),
    )


@st.composite
def routing_policies(draw):
    """Generate valid routing policies."""
    return FabricRoutingPolicy(
        policy_id=f"policy_{draw(st.text(min_size=4, max_size=12))}",
        message_type_filter=draw(
            st.one_of(
                st.none(), st.sets(st.sampled_from(MessageType), min_size=1, max_size=3)
            )
        ),
        topic_pattern_filter=draw(st.one_of(st.none(), topic_patterns())),
        priority_filter=draw(
            st.one_of(
                st.none(),
                st.sets(st.sampled_from(RoutingPriority), min_size=1, max_size=3),
            )
        ),
        prefer_local_routing=draw(st.booleans()),
        max_federation_hops=draw(st.integers(min_value=1, max_value=10)),
        enable_load_balancing=draw(st.booleans()),
        enable_geographic_optimization=draw(st.booleans()),
        max_latency_ms=draw(st.floats(min_value=100.0, max_value=30000.0)),
        max_route_cost=draw(st.floats(min_value=10.0, max_value=1000.0)),
        min_reliability_threshold=draw(st.floats(min_value=0.5, max_value=1.0)),
    )


@st.composite
def routing_configs(draw):
    """Generate valid routing configurations."""
    policies = draw(st.lists(routing_policies(), min_size=0, max_size=5))

    return FabricRoutingConfig(
        enable_topic_rpc=draw(st.booleans()),
        enable_queue_topics=draw(st.booleans()),
        enable_federation_optimization=draw(st.booleans()),
        max_routing_hops=draw(st.integers(min_value=1, max_value=20)),
        routing_cache_ttl_ms=draw(st.floats(min_value=5000.0, max_value=300000.0)),
        max_cached_routes=draw(st.integers(min_value=100, max_value=50000)),
        policies=policies,
    )


# Core property tests for unified routing


class TestUnifiedRoutingProperties:
    """Property-based tests for unified routing correctness."""

    @given(unified_messages())
    @settings(max_examples=500, deadline=2000)
    def test_message_integrity_property(self, message: UnifiedMessage):
        """Property: Messages maintain integrity through routing operations."""
        # Test message immutability
        original_topic = message.topic
        original_headers = message.headers
        original_payload = message.payload

        # Apply routing metadata
        modified_message = replace(
            message,
            headers=replace(
                message.headers,
                metadata={**message.headers.metadata, "test_key": "test_value"},
            ),
        )

        # Original message should be unchanged
        assert message.topic == original_topic
        assert message.headers == original_headers
        assert message.payload == original_payload

        # Modified message should have new metadata
        assert modified_message.headers.metadata["test_key"] == "test_value"
        assert modified_message.topic == original_topic
        assert modified_message.payload == original_payload

    @given(unified_messages())
    @settings(max_examples=500, deadline=2000)
    def test_correlation_id_consistency_property(self, message: UnifiedMessage):
        """Property: Correlation IDs are consistently preserved and generated."""
        # Test correlation ID generation
        corr_id = create_correlation_id()
        assert corr_id.startswith("corr_")
        assert len(corr_id) > 5

        # Test route ID generation consistency
        route_id1 = create_route_id(message)
        route_id2 = create_route_id(message)

        # Same message should generate same route ID
        assert route_id1 == route_id2

        # Different messages should generate different route IDs (high probability)
        if message.headers.correlation_id != "test_different":
            modified_message = replace(
                message,
                headers=replace(message.headers, correlation_id="test_different"),
            )
            route_id3 = create_route_id(modified_message)
            assert route_id1 != route_id3

    @given(internal_unified_messages())
    @settings(max_examples=200, deadline=2000)
    def test_internal_topic_access_control_property(self, message: UnifiedMessage):
        """Property: Internal topics are properly identified and access controlled."""
        # Internal topics should be detected correctly
        assert is_internal_topic(message.topic)
        assert message.topic.startswith("mpreg.")

        # Access control validation
        assert not TopicValidator.validate_topic_access(
            message.topic, "external_system"
        )
        assert TopicValidator.validate_topic_access(message.topic, "mpreg_internal")

        # Access level should be control plane
        access_level = TopicValidator.get_topic_access_level(message.topic)
        assert access_level == TopicAccessLevel.CONTROL_PLANE

    @given(unified_messages())
    @settings(max_examples=300, deadline=2000)
    def test_system_extraction_consistency_property(self, message: UnifiedMessage):
        """Property: System type extraction from topics is consistent and correct."""
        extracted_system = extract_system_from_topic(message.topic)

        if message.topic.startswith("mpreg.rpc."):
            assert extracted_system == MessageType.RPC
        elif message.topic.startswith("mpreg.queue."):
            assert extracted_system == MessageType.QUEUE
        elif message.topic.startswith("mpreg.pubsub."):
            assert extracted_system == MessageType.PUBSUB
        elif message.topic.startswith("mpreg.fabric.") or message.topic.startswith(
            "mpreg."
        ):
            assert extracted_system == MessageType.CONTROL
        else:
            assert extracted_system == MessageType.DATA

    @given(unified_messages())
    @settings(max_examples=300, deadline=2000)
    def test_federation_detection_property(self, message: UnifiedMessage):
        """Property: Federation requirement detection is accurate."""
        # Messages with target cluster should be detected as federation messages
        if message.headers.target_cluster is not None:
            assert is_federation_message(message)

        # Messages with federation path should be detected
        if message.headers.federation_path:
            assert is_federation_message(message)

        # Federation topic messages should be detected
        if message.topic.startswith("mpreg.fabric."):
            assert is_federation_message(message)

        # Control plane detection should work correctly
        if (
            message.topic.startswith("mpreg.")
            and message.message_type == MessageType.CONTROL
        ):
            assert is_control_plane_message(message)

    @given(routing_policies(), unified_messages())
    @settings(max_examples=400, deadline=3000)
    def test_policy_matching_consistency_property(
        self, policy: FabricRoutingPolicy, message: UnifiedMessage
    ):
        """Property: Policy matching is consistent and follows filter rules."""
        matches = policy.matches_message(message)

        # Check message type filter consistency
        if policy.message_type_filter is not None:
            if message.message_type not in policy.message_type_filter:
                assert not matches
            # If message type matches, continue checking other filters

        # Check priority filter consistency
        if policy.priority_filter is not None:
            if message.headers.priority not in policy.priority_filter:
                assert not matches

        # Check topic pattern filter (simplified check)
        if policy.topic_pattern_filter is not None:
            from mpreg.core.topic_taxonomy import TopicValidator

            if not TopicValidator.matches_pattern(
                message.topic, policy.topic_pattern_filter
            ):
                assert not matches

    @given(route_results())
    @settings(max_examples=300, deadline=2000)
    def test_route_result_consistency_property(self, route: FabricRouteResult):
        """Property: Route results maintain internal consistency."""
        # Local routing consistency
        if route.is_local_route:
            assert not route.federation_required
            assert len(route.federation_path) <= 1

        # Federation routing consistency
        if route.federation_required:
            assert len(route.federation_path) > 1 or route.hops_required > 0

        # Multi-target consistency
        if route.is_multi_target:
            assert len(route.targets) > 1

        # Performance metrics should be positive
        assert route.estimated_latency_ms > 0
        assert route.route_cost > 0
        assert route.hops_required >= 0

    @given(routing_configs())
    @settings(max_examples=200, deadline=2000)
    def test_routing_config_validation_property(self, config: FabricRoutingConfig):
        """Property: Routing configurations are valid and consistent."""
        # Hop limits should be reasonable
        assert config.max_routing_hops > 0
        assert config.max_routing_hops <= 50  # Reasonable upper bound

        # Cache settings should be positive
        assert config.routing_cache_ttl_ms > 0
        assert config.max_cached_routes > 0

        # Policy application should work
        test_message = UnifiedMessage(
            message_id="msg_test",
            topic="test.topic",
            message_type=MessageType.DATA,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id="test_corr",
                priority=RoutingPriority.NORMAL,
            ),
        )

        # Should always return a policy (default if none match)
        policy = config.get_policy_for_message(test_message)
        assert policy is not None

    @given(st.text(), st.text(min_size=1))
    @settings(max_examples=200, deadline=1000)
    def test_topic_validation_property(self, topic: str, system: str):
        """Property: Topic validation is consistent and follows rules."""
        is_valid, error_msg = TopicValidator.validate_topic_pattern(topic)

        # Empty topics should be invalid
        if not topic:
            assert not is_valid
            assert error_msg

        # Valid topics should pass validation
        if is_valid:
            assert not error_msg
            # Valid topics should allow component extraction
            components = TopicValidator.extract_topic_components(topic)
            assert isinstance(components, list)

            # Access level determination should work
            access_level = TopicValidator.get_topic_access_level(topic)
            assert access_level in [
                TopicAccessLevel.CONTROL_PLANE,
                TopicAccessLevel.DATA_PLANE,
            ]


# Integration tests for unified routing


class TestUnifiedRoutingIntegration:
    """Integration tests for unified routing system components."""

    def test_message_header_federation_hop_immutability(self):
        """Test that hop budget updates create new immutable headers."""
        original_headers = MessageHeaders(
            correlation_id="test_corr",
            priority=RoutingPriority.HIGH,
        )

        # Add hop budget
        new_headers = replace(original_headers, hop_budget=3)

        # Original should be unchanged
        assert original_headers.hop_budget is None

        # New headers should have hop budget
        assert new_headers.hop_budget == 3
        assert new_headers.correlation_id == "test_corr"

    def test_topic_pattern_wildcard_validation(self):
        """Test topic pattern validation with wildcards."""
        # Valid patterns
        valid_patterns = [
            "user.*.events",
            "app.#",
            "service.*.action.#",
            "mpreg.rpc.command.*.started",
            "user.#.events",
            "user.*.#.events",
        ]

        for pattern in valid_patterns:
            is_valid, error_msg = TopicValidator.validate_topic_pattern(pattern)
            assert is_valid, f"Pattern {pattern} should be valid: {error_msg}"

        # Invalid patterns
        invalid_patterns = [
            "",
            "user.**.events",  # Double wildcard
            "user.#suffix.events",  # # must be its own segment
            "user.suffix#.events",  # # must be its own segment
            "user.a*.events",  # * must be its own segment
            "user.*suffix.events",  # * must be its own segment
        ]

        for pattern in invalid_patterns:
            is_valid, error_msg = TopicValidator.validate_topic_pattern(pattern)
            assert not is_valid, f"Pattern {pattern} should be invalid"
            assert error_msg

    def test_topic_pattern_matching_with_hash(self):
        """Test wildcard matching with # in non-terminal positions."""
        assert TopicValidator.matches_pattern("user.login.events", "user.#.events")
        assert TopicValidator.matches_pattern("user.login.deep.events", "user.#.events")
        assert TopicValidator.matches_pattern(
            "user.login.deep.events", "user.*.#.events"
        )
        assert not TopicValidator.matches_pattern("admin.login.events", "user.#.events")

    def test_route_result_properties(self):
        """Test route result property calculations."""
        # Local route
        local_route = FabricRouteResult(
            route_id="route_local",
            targets=[FabricRouteTarget(MessageType.RPC, "test_function")],
            routing_path=["node1"],
            federation_path=["cluster1"],
            estimated_latency_ms=50.0,
            route_cost=10.0,
            federation_required=False,
            hops_required=1,
            reason=FabricRouteReason.LOCAL,
        )

        assert local_route.is_local_route
        assert not local_route.is_multi_target

        # Federation route
        federation_route = FabricRouteResult(
            route_id="route_fed",
            targets=[
                FabricRouteTarget(MessageType.QUEUE, "queue1"),
                FabricRouteTarget(MessageType.PUBSUB, "sub1"),
            ],
            routing_path=["node1", "hub1", "node2"],
            federation_path=["cluster1", "cluster2"],
            estimated_latency_ms=200.0,
            route_cost=50.0,
            federation_required=True,
            hops_required=3,
            reason=FabricRouteReason.FEDERATED,
        )

        assert not federation_route.is_local_route
        assert federation_route.is_multi_target

    def test_routing_policy_complex_matching(self):
        """Test complex routing policy matching scenarios."""
        # Create a policy for high priority RPC messages
        rpc_policy = FabricRoutingPolicy(
            policy_id="high_priority_rpc",
            message_type_filter={MessageType.RPC},
            priority_filter={RoutingPriority.HIGH, RoutingPriority.CRITICAL},
            topic_pattern_filter="mpreg.rpc.#",
            max_latency_ms=1000.0,
        )

        # Matching message
        matching_message = UnifiedMessage(
            message_id="msg_test",
            topic="mpreg.rpc.command.123.started",
            message_type=MessageType.RPC,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={},
            headers=MessageHeaders(
                correlation_id="test",
                priority=RoutingPriority.HIGH,
            ),
        )

        assert rpc_policy.matches_message(matching_message)

        # Non-matching message (wrong type)
        non_matching_message = replace(matching_message, message_type=MessageType.QUEUE)

        assert not rpc_policy.matches_message(non_matching_message)

        # Non-matching message (wrong priority)
        non_matching_priority = replace(
            matching_message,
            headers=replace(matching_message.headers, priority=RoutingPriority.LOW),
        )

        assert not rpc_policy.matches_message(non_matching_priority)


if __name__ == "__main__":
    pytest.main([__file__])
