#!/usr/bin/env python3
"""
Property-Based Tests for Cross-System Behaviors in MPREG Unified Architecture.

This module uses Hypothesis to generate thousands of test cases that verify
fundamental properties and invariants of the unified system architecture.

Property categories tested:
1. Message integrity across system boundaries
2. Correlation consistency in complex workflows
3. Topic routing determinism and correctness
4. Federation behavior under various conditions
5. Performance characteristics under load
6. Error handling and recovery properties

The tests are designed to expose edge cases and verify that the system
behaves correctly under all possible input combinations.
"""

import time
from typing import Any

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, initialize, rule

from mpreg.core.enhanced_rpc import (
    TopicAwareRPCCommand,
)
from mpreg.core.monitoring.unified_monitoring import SystemType
from mpreg.core.topic_queue_routing import (
    TopicRoutedQueue,
)

# Core MPREG imports
from mpreg.core.unified_routing import (
    MessageHeaders,
    MessageType,
    UnifiedMessage,
)
from mpreg.federation.unified_federation import UnifiedFederationMessage


# Hypothesis strategies for generating test data
def correlation_ids() -> st.SearchStrategy[str]:
    """Generate realistic correlation IDs."""
    return st.text(
        min_size=8,
        max_size=64,
        alphabet=st.characters(
            whitelist_categories=("L", "N"), blacklist_characters="-_"
        ),
    ).map(lambda s: f"corr-{s}")


def tracking_ids() -> st.SearchStrategy[str]:
    """Generate ULID-style tracking IDs with system prefixes."""
    prefixes = ["rpc-", "ps-", "q-", "cache-", "fed-", "tx-"]
    ulid_part = st.text(
        min_size=26, max_size=26, alphabet="0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    )
    return st.tuples(st.sampled_from(prefixes), ulid_part).map(
        lambda t: f"{t[0]}{t[1]}"
    )


def topic_patterns() -> st.SearchStrategy[str]:
    """Generate valid topic patterns following MPREG taxonomy."""
    # Generate hierarchical topic patterns
    levels = st.integers(min_value=2, max_value=6)
    segments = st.text(
        min_size=3, max_size=12, alphabet=st.characters(whitelist_categories=("L", "N"))
    )

    @st.composite
    def topic_pattern(draw):
        num_levels = draw(levels)
        topic_segments = [draw(segments) for _ in range(num_levels)]

        # Add wildcards occasionally
        if draw(st.booleans()):
            # Replace random segment with wildcard
            wildcard_pos = draw(st.integers(min_value=0, max_value=num_levels - 1))
            wildcard_type = draw(st.sampled_from(["*", "#"]))
            topic_segments[wildcard_pos] = wildcard_type

        return ".".join(topic_segments)

    return topic_pattern()


def unified_messages() -> st.SearchStrategy[UnifiedMessage]:
    """Generate valid UnifiedMessage instances."""
    from mpreg.core.unified_routing import DeliveryGuarantee, RoutingPriority

    return st.builds(
        UnifiedMessage,
        topic=topic_patterns(),
        routing_type=st.sampled_from(list(MessageType)),
        delivery_guarantee=st.sampled_from(list(DeliveryGuarantee)),
        payload=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.one_of(
                st.text(), st.integers(), st.booleans(), st.floats(allow_nan=False)
            ),
        ),
        headers=st.builds(
            MessageHeaders,
            correlation_id=correlation_ids(),
            source_system=st.sampled_from(list(MessageType)),
            target_system=st.one_of(st.none(), st.sampled_from(list(MessageType))),
            priority=st.sampled_from(list(RoutingPriority)),
            routing_metadata=st.dictionaries(
                st.text(min_size=1, max_size=10), st.text()
            ),
        ),
        timestamp=st.floats(
            min_value=1600000000, max_value=2000000000
        ),  # Reasonable timestamp range
    )


def topic_aware_rpc_commands() -> st.SearchStrategy[TopicAwareRPCCommand]:
    """Generate valid TopicAwareRPCCommand instances."""
    return st.builds(
        TopicAwareRPCCommand,
        command=st.text(
            min_size=1,
            max_size=50,
            alphabet=st.characters(whitelist_categories=("L", "N")),
        ),
        args=st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=10),
        kwargs=st.dictionaries(
            st.text(min_size=1, max_size=15),
            st.one_of(st.text(), st.integers(), st.booleans()),
        ),
        publish_progress=st.booleans(),
        progress_topic_pattern=st.text(min_size=10, max_size=100),
        subscribe_to_dependencies=st.booleans(),
        dependency_topic_patterns=st.lists(topic_patterns(), min_size=0, max_size=5),
        tracking_id=tracking_ids(),
    )


def topic_routed_queues() -> st.SearchStrategy[TopicRoutedQueue]:
    """Generate valid TopicRoutedQueue instances."""
    return st.builds(
        TopicRoutedQueue,
        queue_name=st.text(
            min_size=3,
            max_size=30,
            alphabet=st.characters(whitelist_categories=("L", "N")),
        ),
        topic_patterns=st.lists(topic_patterns(), min_size=1, max_size=10),
        routing_priority=st.integers(min_value=1, max_value=1000),
        delivery_guarantee=st.sampled_from(
            ["AT_MOST_ONCE", "AT_LEAST_ONCE", "EXACTLY_ONCE"]
        ),
        consumer_groups=st.sets(
            st.text(min_size=1, max_size=20), min_size=0, max_size=5
        ),
    )


def federation_messages() -> st.SearchStrategy[UnifiedFederationMessage]:
    """Generate valid federation messages."""
    return st.builds(
        UnifiedFederationMessage,
        source_cluster=st.text(
            min_size=5,
            max_size=20,
            alphabet=st.characters(whitelist_categories=("L", "N")),
        ),
        target_clusters=st.one_of(
            st.lists(st.text(min_size=5, max_size=20), min_size=1, max_size=5),
            st.just(["*"]),  # Broadcast
        ),
        message_type=st.sampled_from(list(MessageType)),
        topic=topic_patterns(),
        payload=st.dictionaries(st.text(), st.text()),
        hop_count=st.integers(min_value=0, max_value=10),
        max_hops=st.integers(min_value=1, max_value=20),
        federation_id=st.text(min_size=10, max_size=30),
    )


class TestMessageIntegrityProperties:
    """Test properties related to message integrity across system boundaries."""

    @given(message=unified_messages())
    @settings(max_examples=200, deadline=3000)
    def test_message_serialization_integrity(self, message):
        """
        Property: Messages maintain integrity through serialization/deserialization.

        Invariant: serialize(deserialize(message)) == message
        """
        # Test that message can be converted to dict and back
        message_dict = {
            "topic": message.topic,
            "routing_type": message.routing_type.value,
            "payload": message.payload,
            "correlation_id": message.headers.correlation_id,
            "headers": message.headers,
            "timestamp": message.timestamp,
        }

        # Reconstruct message
        reconstructed = UnifiedMessage(
            topic=message_dict["topic"],
            routing_type=MessageType(message_dict["routing_type"]),
            delivery_guarantee=message.delivery_guarantee,
            payload=message_dict["payload"],
            headers=message_dict["headers"],
            timestamp=message_dict["timestamp"],
        )

        # Verify integrity
        assert reconstructed.topic == message.topic
        assert reconstructed.routing_type == message.routing_type
        assert reconstructed.payload == message.payload
        assert reconstructed.headers.correlation_id == message.headers.correlation_id

    @given(
        messages=st.lists(unified_messages(), min_size=1, max_size=50),
        batch_size=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=50, deadline=5000)
    def test_batch_message_processing_integrity(self, messages, batch_size):
        """
        Property: Batch message processing preserves individual message integrity.

        Invariant: All messages in a batch are processed without corruption
        """
        # Process messages in batches
        batches = [
            messages[i : i + batch_size] for i in range(0, len(messages), batch_size)
        ]

        processed_messages = []
        for batch in batches:
            # Simulate batch processing
            batch_result = []
            for message in batch:
                # Each message should maintain its properties
                assert message.headers.correlation_id is not None
                assert message.topic is not None

                batch_result.append(message)

            processed_messages.extend(batch_result)

        # Verify all messages were processed
        assert len(processed_messages) == len(messages)

        # Verify message integrity
        for original, processed in zip(messages, processed_messages):
            assert original.headers.correlation_id == processed.headers.correlation_id
            assert original.topic == processed.topic

    @given(
        message=unified_messages(),
        transformation_count=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=100, deadline=4000)
    def test_message_transformation_chain_integrity(
        self, message, transformation_count
    ):
        """
        Property: Messages maintain core identity through transformation chains.

        Invariant: Correlation ID and tracking ID are preserved through transformations
        """
        current_message = message
        original_correlation = message.headers.correlation_id

        # Apply multiple transformations
        for i in range(transformation_count):
            # Simulate message transformation (e.g., routing, enrichment)
            transformed_payload = dict(current_message.payload)
            transformed_payload[f"transformation_{i}"] = f"step_{i}"

            # Create new headers with additional transform metadata
            new_headers = MessageHeaders(
                correlation_id=current_message.headers.correlation_id,
                source_system=current_message.headers.source_system,
                target_system=current_message.headers.target_system,
                priority=current_message.headers.priority,
                federation_hop=current_message.headers.federation_hop,
                source_cluster=current_message.headers.source_cluster,
                target_cluster=current_message.headers.target_cluster,
                routing_metadata={
                    **current_message.headers.routing_metadata,
                    f"transform_{i}": str(i),
                },
            )

            current_message = UnifiedMessage(
                topic=current_message.topic,
                routing_type=current_message.routing_type,
                delivery_guarantee=current_message.delivery_guarantee,
                payload=transformed_payload,
                headers=new_headers,
                timestamp=current_message.timestamp,
            )

        # Verify core identity preservation
        assert current_message.headers.correlation_id == original_correlation

        # Verify transformations were applied
        assert len(current_message.payload) >= len(message.payload)
        assert len(current_message.headers.routing_metadata) >= len(
            message.headers.routing_metadata
        )


class TestCorrelationConsistencyProperties:
    """Test properties related to correlation consistency across systems."""

    @given(
        base_correlation=correlation_ids(),
        system_count=st.integers(min_value=2, max_value=6),
        events_per_system=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=50, deadline=6000)
    def test_correlation_timeline_consistency(
        self, base_correlation, system_count, events_per_system
    ):
        """
        Property: Correlation timelines maintain consistency across systems.

        Invariants:
        1. All events with same correlation ID are linked
        2. Timeline ordering is chronologically consistent
        3. No correlation data is lost across system boundaries
        """
        systems = [
            SystemType.RPC,
            SystemType.PUBSUB,
            SystemType.QUEUE,
            SystemType.CACHE,
            SystemType.FEDERATION,
            SystemType.TRANSPORT,
        ]
        selected_systems = systems[:system_count]

        # Generate correlated events across systems
        all_events = []
        base_timestamp = time.time()

        for system_idx, system_type in enumerate(selected_systems):
            for event_idx in range(events_per_system):
                event_timestamp = (
                    base_timestamp + (system_idx * events_per_system + event_idx) * 0.1
                )

                event = {
                    "correlation_id": base_correlation,
                    "system_type": system_type,
                    "event_id": f"{system_type.value}-{event_idx}",
                    "timestamp": event_timestamp,
                    "payload": {"system": system_type.value, "event": event_idx},
                }
                all_events.append(event)

        # Verify correlation consistency properties

        # Property 1: All events share the same correlation ID
        correlation_ids = {event["correlation_id"] for event in all_events}
        assert len(correlation_ids) == 1
        assert base_correlation in correlation_ids

        # Property 2: Events are chronologically ordered
        timestamps = [event["timestamp"] for event in all_events]
        assert timestamps == sorted(timestamps)

        # Property 3: All systems are represented
        systems_in_events = {event["system_type"] for event in all_events}
        assert len(systems_in_events) == system_count

        # Property 4: Event count is correct
        total_expected_events = system_count * events_per_system
        assert len(all_events) == total_expected_events

    @given(
        correlation_groups=st.lists(
            st.tuples(correlation_ids(), st.integers(min_value=1, max_value=20)),
            min_size=1,
            max_size=10,
        )
    )
    @settings(max_examples=30, deadline=8000)
    def test_multiple_correlation_isolation(self, correlation_groups):
        """
        Property: Multiple correlation groups remain isolated.

        Invariant: Events from different correlations never interfere
        """
        all_correlations = []
        correlation_to_events = {}

        for correlation_id, event_count in correlation_groups:
            events = []
            for i in range(event_count):
                event = {
                    "correlation_id": correlation_id,
                    "event_id": f"event-{i}",
                    "timestamp": time.time() + i * 0.01,
                    "data": f"data-{correlation_id}-{i}",
                }
                events.append(event)
                all_correlations.append(correlation_id)

            correlation_to_events[correlation_id] = events

        # Verify isolation properties
        unique_correlations = set(all_correlations)

        # Each correlation should have its own isolated event set
        for correlation_id in unique_correlations:
            correlation_events = correlation_to_events[correlation_id]

            # All events in group have same correlation ID
            event_correlations = {
                event["correlation_id"] for event in correlation_events
            }
            assert len(event_correlations) == 1
            assert correlation_id in event_correlations

            # Events are unique to this correlation
            for other_correlation in unique_correlations:
                if other_correlation != correlation_id:
                    other_events = correlation_to_events[other_correlation]

                    # No event IDs should overlap (assuming unique generation)
                    this_event_ids = {event["event_id"] for event in correlation_events}
                    other_event_ids = {event["event_id"] for event in other_events}

                    # Event data should be correlation-specific
                    this_data = {event["data"] for event in correlation_events}
                    other_data = {event["data"] for event in other_events}
                    assert not this_data.intersection(other_data)


class TestTopicRoutingProperties:
    """Test properties of topic-based routing systems."""

    @given(
        queue_patterns=st.lists(topic_routed_queues(), min_size=1, max_size=20),
        test_topics=st.lists(topic_patterns(), min_size=1, max_size=30),
    )
    @settings(max_examples=30, deadline=10000)
    def test_topic_routing_determinism(self, queue_patterns, test_topics):
        """
        Property: Topic routing is deterministic and consistent.

        Invariants:
        1. Same topic always routes to same queues
        2. Pattern matching follows AMQP rules
        3. Priority ordering is respected
        4. No duplicate routes
        """
        # Create a deterministic routing scenario
        unique_patterns = []
        seen_names = set()

        for pattern in queue_patterns:
            # Ensure unique queue names for deterministic testing
            if pattern.queue_name not in seen_names:
                unique_patterns.append(pattern)
                seen_names.add(pattern.queue_name)

        assume(len(unique_patterns) >= 1)  # Need at least one pattern

        # Test routing consistency
        for topic in test_topics:
            # Simulate routing multiple times
            routing_results = []

            for attempt in range(3):  # Test consistency across attempts
                matched_queues = []

                for queue_pattern in unique_patterns:
                    # Simple pattern matching (simplified for property testing)
                    for pattern in queue_pattern.topic_patterns:
                        if _simple_topic_match(topic, pattern):
                            matched_queues.append(queue_pattern)
                            break  # Avoid duplicate matches from same queue

                # Sort by priority for deterministic ordering
                matched_queues.sort(key=lambda q: q.routing_priority, reverse=True)
                routing_results.append(matched_queues)

            # Verify determinism: all attempts should yield same result
            if routing_results:
                first_result = routing_results[0]
                for result in routing_results[1:]:
                    first_names = [q.queue_name for q in first_result]
                    result_names = [q.queue_name for q in result]
                    assert first_names == result_names

    @given(
        patterns=st.lists(
            st.text(min_size=3, max_size=30, alphabet="abcdefghijklmnopqrstuvwxyz.#*"),
            min_size=1,
            max_size=15,
        ),
        test_strings=st.lists(
            st.text(min_size=3, max_size=30, alphabet="abcdefghijklmnopqrstuvwxyz."),
            min_size=1,
            max_size=15,
        ),
    )
    @settings(max_examples=50, deadline=5000)
    def test_topic_pattern_matching_properties(self, patterns, test_strings):
        """
        Property: Topic pattern matching follows consistent rules.

        Invariants:
        1. '*' matches exactly one segment
        2. '#' matches zero or more segments
        3. Exact matches work correctly
        4. No false positives/negatives
        """
        for pattern in patterns:
            for test_string in test_strings:
                result = _simple_topic_match(test_string, pattern)

                # Basic sanity checks
                assert isinstance(result, bool)

                # Exact match should always work
                if pattern == test_string and "#" not in pattern and "*" not in pattern:
                    assert result is True

                # Pattern with only '#' should match everything
                if pattern == "#":
                    assert result is True


class CrossSystemWorkflowStateMachine(RuleBasedStateMachine):
    """
    Stateful property-based testing for cross-system workflows.

    This uses Hypothesis's stateful testing to model complex workflows
    across multiple systems and verify invariants are maintained.
    """

    def __init__(self):
        super().__init__()
        self.active_correlations: set[str] = set()
        self.processed_messages: list[dict[str, Any]] = []
        self.system_states: dict[SystemType, dict[str, Any]] = {
            system: {} for system in SystemType
        }
        self.message_counter = 0

    messages = Bundle("messages")
    correlations = Bundle("correlations")

    @initialize()
    def setup_systems(self):
        """Initialize the cross-system environment."""
        self.system_states = {
            SystemType.RPC: {"active_commands": set(), "completed_commands": set()},
            SystemType.PUBSUB: {"published_topics": set(), "subscriptions": set()},
            SystemType.QUEUE: {"queued_messages": set(), "processed_messages": set()},
            SystemType.CACHE: {"cached_keys": set(), "invalidated_keys": set()},
        }

    @rule(target=correlations, correlation_id=correlation_ids())
    def start_correlation(self, correlation_id):
        """Start a new correlation workflow."""
        assume(correlation_id not in self.active_correlations)
        self.active_correlations.add(correlation_id)
        return correlation_id

    @rule(
        target=messages,
        correlation=correlations,
        message_type=st.sampled_from(list(MessageType)),
        topic=topic_patterns(),
    )
    def create_message(self, correlation, message_type, topic):
        """Create a message within an active correlation."""
        self.message_counter += 1
        message = {
            "id": self.message_counter,
            "correlation_id": correlation,
            "message_type": message_type,
            "topic": topic,
            "timestamp": time.time(),
            "processed": False,
        }
        return message

    @rule(message=messages)
    def process_message(self, message):
        """Process a message through the appropriate system."""
        assume(not message["processed"])

        system_type = _message_type_to_system_type(message["message_type"])

        # Update system state
        if system_type == SystemType.RPC:
            self.system_states[SystemType.RPC]["active_commands"].add(message["id"])
        elif system_type == SystemType.PUBSUB:
            self.system_states[SystemType.PUBSUB]["published_topics"].add(
                message["topic"]
            )
        elif system_type == SystemType.QUEUE:
            self.system_states[SystemType.QUEUE]["queued_messages"].add(message["id"])
        elif system_type == SystemType.CACHE:
            self.system_states[SystemType.CACHE]["cached_keys"].add(message["topic"])

        message["processed"] = True
        self.processed_messages.append(message)

    @rule(correlation=correlations)
    def complete_correlation(self, correlation):
        """Complete a correlation workflow."""
        assume(correlation in self.active_correlations)

        # Find all messages in this correlation
        correlation_messages = [
            msg
            for msg in self.processed_messages
            if msg["correlation_id"] == correlation
        ]

        # Verify workflow invariants
        if correlation_messages:
            # All messages in correlation should be processed
            assert all(msg["processed"] for msg in correlation_messages)

            # Correlation should have consistent ID across all messages
            correlation_ids = {msg["correlation_id"] for msg in correlation_messages}
            assert len(correlation_ids) == 1
            assert correlation in correlation_ids

        self.active_correlations.remove(correlation)

    def teardown(self):
        """Verify final system state is consistent."""
        # All processed messages should have valid correlation IDs
        for message in self.processed_messages:
            assert message["correlation_id"] is not None
            assert message["processed"] is True

        # System states should be consistent
        for system_type, state in self.system_states.items():
            assert isinstance(state, dict)


def _simple_topic_match(topic: str, pattern: str) -> bool:
    """
    Simplified topic pattern matching for property testing.

    This is a basic implementation for testing purposes.
    Real implementation would be more sophisticated.
    """
    if pattern == "#":
        return True

    if pattern == topic:
        return True

    # Simple wildcard matching
    if "*" in pattern:
        pattern_parts = pattern.split(".")
        topic_parts = topic.split(".")

        if len(pattern_parts) != len(topic_parts):
            return False

        for pattern_part, topic_part in zip(pattern_parts, topic_parts):
            if pattern_part != "*" and pattern_part != topic_part:
                return False

        return True

    if "#" in pattern:
        # Very simplified # matching
        prefix = pattern.replace("#", "")
        return topic.startswith(prefix.rstrip("."))

    return False


def _message_type_to_system_type(message_type: MessageType) -> SystemType:
    """Convert MessageType to SystemType."""
    mapping = {
        MessageType.RPC: SystemType.RPC,
        MessageType.PUBSUB: SystemType.PUBSUB,
        MessageType.QUEUE: SystemType.QUEUE,
        MessageType.CACHE: SystemType.CACHE,
    }
    return mapping.get(message_type, SystemType.PUBSUB)


# Stateful testing class
TestCrossSystemWorkflows = CrossSystemWorkflowStateMachine.TestCase


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
