"""
Comprehensive tests for gossip protocol system.

This module tests the gossip protocol components including:
- GossipProtocol epidemic dissemination algorithm
- VectorClock causal ordering and conflict resolution
- GossipMessage versioned messaging with TTL
- GossipFilter anti-entropy and loop prevention
- GossipScheduler adaptive timing and target selection
- End-to-end gossip convergence and performance

Test Coverage:
- Vector clock operations and causality
- Message creation and propagation
- Filter mechanisms and bandwidth efficiency
- Scheduler strategies and adaptive timing
- Protocol lifecycle and background tasks
- Convergence testing and performance validation
"""

import random
import time

import pytest

from mpreg.datastructures.vector_clock import VectorClock
from mpreg.federation.federation_gossip import (
    GossipFilter,
    GossipMessage,
    GossipMessageType,
    GossipProtocol,
    GossipScheduler,
    GossipStrategy,
    NodeMetadata,
)
from mpreg.federation.federation_registry import HubRegistry


@pytest.fixture
def sample_vector_clock():
    """Create a sample vector clock for testing."""
    clock = VectorClock.empty()
    clock.increment("node_1")
    clock.increment("node_2")
    clock.increment("node_1")  # node_1 should be at 2, node_2 at 1
    return clock


@pytest.fixture
def sample_gossip_message():
    """Create a sample gossip message for testing."""
    vector_clock = VectorClock.empty()
    vector_clock = vector_clock.increment("test_node")

    return GossipMessage(
        message_id="test_msg_001",
        message_type=GossipMessageType.STATE_UPDATE,
        sender_id="test_node",
        payload={"key": "test_key", "value": "test_value"},
        vector_clock=vector_clock,
        sequence_number=1,
        ttl=10,
        max_hops=5,
    )


@pytest.fixture
def sample_gossip_filter():
    """Create a sample gossip filter for testing."""
    return GossipFilter(
        max_seen_messages=100, digest_cache_size=50, duplicate_threshold=3
    )


@pytest.fixture
def sample_gossip_scheduler():
    """Create a sample gossip scheduler for testing."""
    return GossipScheduler(
        gossip_interval=1.0, fanout=3, strategy=GossipStrategy.RANDOM
    )


@pytest.fixture
def sample_hub_registry():
    """Create a sample hub registry for testing."""
    return HubRegistry(
        registry_id="test_registry",
        discovery_methods=[],
        heartbeat_interval=10.0,
        cleanup_interval=30.0,
    )


@pytest.fixture
def sample_gossip_protocol(sample_hub_registry):
    """Create a sample gossip protocol for testing."""
    return GossipProtocol(
        node_id="test_node_001",
        hub_registry=sample_hub_registry,
        gossip_interval=0.1,  # Fast for testing
        fanout=2,
        strategy=GossipStrategy.RANDOM,
    )


class TestVectorClock:
    """Test suite for vector clock implementation."""

    def test_vector_clock_creation(self):
        """Test vector clock creation."""
        clock = VectorClock.empty()
        assert len(clock) == 0

    def test_vector_clock_increment(self):
        """Test vector clock increment operations."""
        clock = VectorClock.empty()

        # Increment new node
        clock = clock.increment("node_1")
        assert clock.get_timestamp("node_1") == 1

        # Increment existing node
        clock = clock.increment("node_1")
        assert clock.get_timestamp("node_1") == 2

        # Increment different node
        clock = clock.increment("node_2")
        assert clock.get_timestamp("node_2") == 1
        assert clock.get_timestamp("node_1") == 2

    def test_vector_clock_update(self):
        """Test vector clock update with another clock."""
        clock1 = VectorClock.empty()
        clock1 = clock1.increment("node_1")
        clock1 = clock1.increment("node_1")
        clock1 = clock1.increment("node_2")

        clock2 = VectorClock.empty()
        clock2 = clock2.increment("node_2")
        clock2 = clock2.increment("node_2")
        clock2 = clock2.increment("node_3")

        # Update clock1 with clock2
        clock1 = clock1.update(clock2)

        assert clock1.get_timestamp("node_1") == 2  # Unchanged
        assert clock1.get_timestamp("node_2") == 2  # Max of 1 and 2
        assert clock1.get_timestamp("node_3") == 1  # New from clock2

    def test_vector_clock_comparison(self):
        """Test vector clock comparison operations."""
        clock1 = VectorClock.empty()
        clock1 = clock1.increment("node_1")
        clock1 = clock1.increment("node_2")

        clock2 = VectorClock.empty()
        clock2 = clock2.increment("node_1")
        clock2 = clock2.increment("node_2")

        # Equal clocks
        assert clock1.compare(clock2) == "equal"

        # Clock1 after clock2
        clock1 = clock1.increment("node_1")
        assert clock1.compare(clock2) == "after"
        assert clock2.compare(clock1) == "before"

        # Concurrent clocks
        clock2 = clock2.increment("node_2")
        clock2 = clock2.increment("node_3")
        assert clock1.compare(clock2) == "concurrent"

    def test_vector_clock_copy(self, sample_vector_clock):
        """Test vector clock copy operation."""
        clock_copy = sample_vector_clock.copy()

        assert clock_copy.to_dict() == sample_vector_clock.to_dict()
        # For immutable objects, copy() returns the same object
        assert clock_copy is sample_vector_clock

        # Modify copy creates new object, doesn't affect original
        modified_copy = clock_copy.increment("node_3")
        assert "node_3" not in sample_vector_clock.to_dict()
        assert "node_3" in modified_copy.to_dict()


class TestGossipMessage:
    """Test suite for gossip message implementation."""

    def test_gossip_message_creation(self, sample_gossip_message):
        """Test gossip message creation."""
        message = sample_gossip_message

        assert message.message_id == "test_msg_001"
        assert message.message_type == GossipMessageType.STATE_UPDATE
        assert message.sender_id == "test_node"
        assert message.payload["key"] == "test_key"
        assert message.sequence_number == 1
        assert message.ttl == 10
        assert message.max_hops == 5
        assert message.hop_count == 0

    def test_gossip_message_digest_computation(self, sample_gossip_message):
        """Test gossip message digest computation."""
        message = sample_gossip_message

        assert len(message.digest) == 8  # MD5 truncated to 8 chars
        assert len(message.checksum) == 16  # SHA256 truncated to 16 chars

        # Same message should have same digest
        message2 = GossipMessage(
            message_id="test_msg_001",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="test_node",
            payload={"key": "test_key", "value": "test_value"},
            sequence_number=1,
        )
        assert message.digest == message2.digest

    def test_gossip_message_expiration(self):
        """Test gossip message expiration."""
        message = GossipMessage(
            message_id="test_msg",
            message_type=GossipMessageType.HEARTBEAT,
            sender_id="test_node",
            payload={},
            expires_at=time.time() + 0.1,  # 100ms from now
        )

        # Initially not expired
        assert not message.is_expired()

        # Wait and check expiration
        time.sleep(0.2)
        assert message.is_expired()

    def test_gossip_message_propagation_check(self, sample_gossip_message):
        """Test gossip message propagation checks."""
        message = sample_gossip_message

        # Initially can propagate
        assert message.can_propagate()

        # Exceed hop count
        message.hop_count = message.max_hops
        assert not message.can_propagate()

        # Reset hop count, zero TTL
        message.hop_count = 0
        message.ttl = 0
        assert not message.can_propagate()

        # Reset TTL, make expired
        message.ttl = 5
        message.expires_at = time.time() - 1
        assert not message.can_propagate()

    def test_gossip_message_propagation_preparation(self, sample_gossip_message):
        """Test gossip message preparation for propagation."""
        original_message = sample_gossip_message
        original_ttl = original_message.ttl
        original_hop_count = original_message.hop_count

        # Prepare for propagation
        propagated_message = original_message.prepare_for_propagation("new_sender")

        assert propagated_message.sender_id == "new_sender"
        assert propagated_message.ttl == original_ttl - 1
        assert propagated_message.hop_count == original_hop_count + 1
        assert "new_sender" in propagated_message.propagation_path
        assert propagated_message.message_id == original_message.message_id

        # Vector clock should be updated
        assert propagated_message.vector_clock.to_dict()["new_sender"] == 1

    def test_gossip_message_seen_tracking(self, sample_gossip_message):
        """Test gossip message seen tracking."""
        message = sample_gossip_message

        # Initially not seen by anyone
        assert not message.has_been_seen_by("node_1")

        # Add seen by
        message.add_seen_by("node_1")
        assert message.has_been_seen_by("node_1")
        assert not message.has_been_seen_by("node_2")

        # Add multiple nodes
        message.add_seen_by("node_2")
        message.add_seen_by("node_3")
        assert len(message.seen_by) == 3

    def test_gossip_message_age_calculation(self, sample_gossip_message):
        """Test gossip message age calculation."""
        message = sample_gossip_message

        # Should have some age (small but positive)
        age = message.get_message_age()
        assert age >= 0
        assert age < 1.0  # Should be very small for new message

        # Wait and check age increases
        time.sleep(0.1)
        new_age = message.get_message_age()
        assert new_age > age


class TestGossipFilter:
    """Test suite for gossip filter implementation."""

    def test_gossip_filter_creation(self, sample_gossip_filter):
        """Test gossip filter creation."""
        filter_obj = sample_gossip_filter

        assert filter_obj.max_seen_messages == 100
        assert filter_obj.digest_cache_size == 50
        assert filter_obj.duplicate_threshold == 3
        assert len(filter_obj.seen_messages) == 0
        assert len(filter_obj.recent_digests) == 0

    def test_gossip_filter_message_propagation_decision(
        self, sample_gossip_filter, sample_gossip_message
    ):
        """Test gossip filter propagation decisions."""
        filter_obj = sample_gossip_filter
        message = sample_gossip_message

        # First time seeing message - should propagate
        should_propagate = filter_obj.should_propagate(message, "test_node")
        assert should_propagate

        # Second time seeing same message - should not propagate
        should_propagate = filter_obj.should_propagate(message, "test_node")
        assert not should_propagate
        assert filter_obj.duplicate_count == 1

    def test_gossip_filter_expired_message_filtering(self, sample_gossip_filter):
        """Test gossip filter handling of expired messages."""
        filter_obj = sample_gossip_filter

        # Create expired message
        expired_message = GossipMessage(
            message_id="expired_msg",
            message_type=GossipMessageType.HEARTBEAT,
            sender_id="test_node",
            payload={},
            expires_at=time.time() - 1,  # Already expired
        )

        should_propagate = filter_obj.should_propagate(expired_message, "test_node")
        assert not should_propagate
        assert filter_obj.expired_count == 1

    def test_gossip_filter_loop_detection(self, sample_gossip_filter):
        """Test gossip filter loop detection."""
        filter_obj = sample_gossip_filter

        # Create message that has already been through this node
        message = GossipMessage(
            message_id="loop_msg",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="sender_node",
            payload={"test": "data"},
            propagation_path=[
                "node_1",
                "test_node",
                "node_2",
            ],  # test_node already in path
        )

        should_propagate = filter_obj.should_propagate(message, "test_node")
        assert not should_propagate
        assert filter_obj.duplicate_count == 1

    def test_gossip_filter_seen_by_detection(self, sample_gossip_filter):
        """Test gossip filter seen_by detection."""
        filter_obj = sample_gossip_filter

        # Create message that has been seen by this node
        message = GossipMessage(
            message_id="seen_msg",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="sender_node",
            payload={"test": "data"},
        )
        message.add_seen_by("test_node")

        should_propagate = filter_obj.should_propagate(message, "test_node")
        assert not should_propagate
        assert filter_obj.duplicate_count == 1

    def test_gossip_filter_digest_caching(self, sample_gossip_filter):
        """Test gossip filter digest caching."""
        filter_obj = sample_gossip_filter

        # Create first message
        message1 = GossipMessage(
            message_id="msg_1",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="sender_node",
            payload={"test": "data"},
            sequence_number=1,
        )

        # Process first message
        filter_obj.should_propagate(message1, "test_node")

        # Create second message with same digest
        message2 = GossipMessage(
            message_id="msg_2",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="sender_node",
            payload={"test": "data"},
            sequence_number=1,  # Same content = same digest
        )

        # Should be filtered due to duplicate digest
        should_propagate = filter_obj.should_propagate(message2, "test_node")
        assert not should_propagate
        assert filter_obj.duplicate_count == 1

    def test_gossip_filter_statistics(
        self, sample_gossip_filter, sample_gossip_message
    ):
        """Test gossip filter statistics collection."""
        filter_obj = sample_gossip_filter

        # Process some messages
        filter_obj.should_propagate(sample_gossip_message, "test_node")
        filter_obj.should_propagate(sample_gossip_message, "test_node")  # Duplicate

        stats = filter_obj.get_filter_statistics()

        assert hasattr(stats, "seen_messages")
        assert hasattr(stats, "recent_digests")
        assert hasattr(stats, "filtered_count")
        assert hasattr(stats, "duplicate_count")
        assert hasattr(stats, "duplicate_count")
        assert hasattr(stats, "expired_count")
        assert hasattr(stats, "filter_ratio")

        assert stats.duplicate_count == 1
        assert stats.seen_messages == 1


class TestGossipScheduler:
    """Test suite for gossip scheduler implementation."""

    def test_gossip_scheduler_creation(self, sample_gossip_scheduler):
        """Test gossip scheduler creation."""
        scheduler = sample_gossip_scheduler

        assert scheduler.gossip_interval == 1.0
        assert scheduler.fanout == 3
        assert scheduler.strategy == GossipStrategy.RANDOM
        assert scheduler.adaptive_interval == 1.0
        assert scheduler.pending_messages == 0

    def test_gossip_scheduler_timing(self, sample_gossip_scheduler):
        """Test gossip scheduler timing decisions."""
        scheduler = sample_gossip_scheduler

        # No pending messages - should not gossip
        assert not scheduler.should_gossip()

        # Add pending messages but set recent gossip time
        scheduler.update_pending_messages(5)
        scheduler.last_gossip_time = time.time()  # Just gossiped
        assert scheduler.pending_messages == 5

        # Still shouldn't gossip immediately (no time elapsed)
        assert not scheduler.should_gossip()

        # Simulate time passage
        scheduler.last_gossip_time = time.time() - 2.0  # 2 seconds ago
        assert scheduler.should_gossip()

    def test_gossip_scheduler_adaptive_interval(self, sample_gossip_scheduler):
        """Test gossip scheduler adaptive interval adjustment."""
        scheduler = sample_gossip_scheduler
        original_interval = scheduler.adaptive_interval

        # Many pending messages should decrease interval
        scheduler.update_pending_messages(15)
        new_interval = scheduler.get_next_gossip_interval()
        assert new_interval < original_interval

        # Reset and test few pending messages should increase interval
        scheduler.adaptive_interval = original_interval  # Reset
        scheduler.update_pending_messages(1)
        new_interval = scheduler.get_next_gossip_interval()
        assert new_interval > original_interval

    def test_gossip_scheduler_target_selection_random(self, sample_gossip_scheduler):
        """Test random gossip target selection."""
        scheduler = sample_gossip_scheduler
        available_nodes = ["node_1", "node_2", "node_3", "node_4", "node_5"]

        targets = scheduler.select_gossip_targets(available_nodes)

        assert len(targets) == min(scheduler.fanout, len(available_nodes))
        assert all(target in available_nodes for target in targets)
        assert len(set(targets)) == len(targets)  # No duplicates

    def test_gossip_scheduler_target_selection_proximity(self):
        """Test proximity-based gossip target selection."""
        scheduler = GossipScheduler(
            gossip_interval=1.0, fanout=2, strategy=GossipStrategy.PROXIMITY
        )

        available_nodes = ["node_1", "node_2", "node_3", "node_4"]
        node_metadata = {
            "node_1": NodeMetadata(node_id="node_1", distance=100.0),
            "node_2": NodeMetadata(node_id="node_2", distance=50.0),
            "node_3": NodeMetadata(node_id="node_3", distance=200.0),
            "node_4": NodeMetadata(node_id="node_4", distance=75.0),
        }

        targets = scheduler.select_gossip_targets(available_nodes, node_metadata)

        assert len(targets) == 2
        # Should select closest nodes
        assert "node_2" in targets  # distance 50
        assert "node_4" in targets  # distance 75

    def test_gossip_scheduler_target_selection_topology_aware(self):
        """Test topology-aware gossip target selection."""
        scheduler = GossipScheduler(
            gossip_interval=1.0, fanout=3, strategy=GossipStrategy.TOPOLOGY_AWARE
        )

        available_nodes = ["node_1", "node_2", "node_3", "node_4"]
        node_metadata = {
            "node_1": NodeMetadata(node_id="node_1", region="us-east"),
            "node_2": NodeMetadata(node_id="node_2", region="us-west"),
            "node_3": NodeMetadata(node_id="node_3", region="us-east"),
            "node_4": NodeMetadata(node_id="node_4", region="eu"),
        }

        targets = scheduler.select_gossip_targets(available_nodes, node_metadata)

        assert len(targets) <= 3
        # Should prefer different regions
        selected_regions = [node_metadata[target].region for target in targets]
        assert len(set(selected_regions)) >= 2  # At least 2 different regions

    def test_gossip_scheduler_target_selection_hybrid(self):
        """Test hybrid gossip target selection."""
        scheduler = GossipScheduler(
            gossip_interval=1.0, fanout=4, strategy=GossipStrategy.HYBRID
        )

        available_nodes = ["node_1", "node_2", "node_3", "node_4", "node_5", "node_6"]
        node_metadata = {
            "node_1": NodeMetadata(node_id="node_1", distance=100.0),
            "node_2": NodeMetadata(node_id="node_2", distance=50.0),
            "node_3": NodeMetadata(node_id="node_3", distance=200.0),
            "node_4": NodeMetadata(node_id="node_4", distance=75.0),
            "node_5": NodeMetadata(node_id="node_5", distance=300.0),
            "node_6": NodeMetadata(node_id="node_6", distance=25.0),
        }

        targets = scheduler.select_gossip_targets(available_nodes, node_metadata)

        assert len(targets) == 4
        # Should be a mix of random and proximity-based
        assert all(target in available_nodes for target in targets)

    def test_gossip_scheduler_cycle_recording(self, sample_gossip_scheduler):
        """Test gossip scheduler cycle recording."""
        scheduler = sample_gossip_scheduler
        scheduler.update_pending_messages(10)

        initial_cycles = scheduler.gossip_cycles
        initial_pending = scheduler.pending_messages

        # Record a gossip cycle
        scheduler.record_gossip_cycle(messages_sent=5, bandwidth_used=5120.0)

        assert scheduler.gossip_cycles == initial_cycles + 1
        assert scheduler.pending_messages == initial_pending - 5
        assert len(scheduler.bandwidth_usage) == 1
        assert scheduler.bandwidth_usage[0] == 5120.0

    def test_gossip_scheduler_statistics(self, sample_gossip_scheduler):
        """Test gossip scheduler statistics collection."""
        scheduler = sample_gossip_scheduler

        # Perform some operations
        scheduler.update_pending_messages(5)
        scheduler.record_gossip_cycle(3, 1024.0)

        stats = scheduler.get_scheduler_statistics()

        assert hasattr(stats, "gossip_interval")
        assert hasattr(stats, "adaptive_interval")
        assert hasattr(stats, "fanout")
        assert hasattr(stats, "strategy")
        assert hasattr(stats, "gossip_cycles")
        assert hasattr(stats, "pending_messages")
        assert hasattr(stats, "pending_messages")
        assert hasattr(stats, "avg_bandwidth_usage")

        assert stats.strategy == GossipStrategy.RANDOM.value
        assert stats.gossip_cycles == 1
        assert stats.pending_messages == 2  # 5 - 3


class TestGossipProtocol:
    """Test suite for gossip protocol implementation."""

    def test_gossip_protocol_creation(self, sample_gossip_protocol):
        """Test gossip protocol creation."""
        protocol = sample_gossip_protocol

        assert protocol.node_id == "test_node_001"
        assert protocol.hub_registry is not None
        assert protocol.scheduler is not None
        assert protocol.filter is not None
        assert protocol.vector_clock is not None
        assert len(protocol.pending_messages) == 0

    @pytest.mark.asyncio
    async def test_gossip_protocol_lifecycle(self, sample_gossip_protocol):
        """Test gossip protocol start and stop."""
        protocol = sample_gossip_protocol

        # Start protocol
        await protocol.start()
        assert len(protocol._background_tasks) > 0
        assert protocol.protocol_stats.protocol_started > 0

        # Stop protocol
        await protocol.stop()
        assert len(protocol._background_tasks) == 0
        assert protocol.protocol_stats.protocol_stopped > 0

    @pytest.mark.asyncio
    async def test_gossip_protocol_message_addition(
        self, sample_gossip_protocol, sample_gossip_message
    ):
        """Test adding messages to gossip protocol."""
        protocol = sample_gossip_protocol

        initial_count = len(protocol.pending_messages)

        # Add message
        await protocol.add_message(sample_gossip_message)

        assert len(protocol.pending_messages) == initial_count + 1
        assert protocol.scheduler.pending_messages == initial_count + 1

    @pytest.mark.asyncio
    async def test_gossip_protocol_message_handling(
        self, sample_gossip_protocol, sample_gossip_message
    ):
        """Test handling received gossip messages."""
        protocol = sample_gossip_protocol

        initial_stats = protocol.protocol_stats.messages_received

        # Handle message
        await protocol.handle_received_message(sample_gossip_message)

        assert protocol.protocol_stats.messages_received == initial_stats + 1
        assert sample_gossip_message.message_id in protocol.recent_messages

        # Vector clock should be updated
        assert "test_node" in protocol.vector_clock.to_dict()

    @pytest.mark.asyncio
    async def test_gossip_protocol_state_update_handling(self, sample_gossip_protocol):
        """Test handling state update messages."""
        protocol = sample_gossip_protocol

        # Create state update message
        state_message = GossipMessage(
            message_id="state_update_001",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="sender_node",
            payload={"key": "test_state", "value": "test_value"},
        )

        # Handle state update
        await protocol.handle_received_message(state_message)

        # Check that state was updated
        assert "test_state" in protocol.state_cache
        assert protocol.state_cache["test_state"] == "test_value"

    @pytest.mark.asyncio
    async def test_gossip_protocol_membership_update_handling(
        self, sample_gossip_protocol
    ):
        """Test handling membership update messages."""
        protocol = sample_gossip_protocol

        # Create membership update message
        from mpreg.federation.federation_gossip import MembershipUpdatePayload

        membership_message = GossipMessage(
            message_id="membership_update_001",
            message_type=GossipMessageType.MEMBERSHIP_UPDATE,
            sender_id="sender_node",
            payload=MembershipUpdatePayload(
                node_id="new_node_001",
                node_info={
                    "region": "us-east",
                    "capabilities": {"max_connections": 100},
                },
            ),
        )

        # Handle membership update
        await protocol.handle_received_message(membership_message)

        # Check that membership was updated
        assert "new_node_001" in protocol.known_nodes
        assert protocol.known_nodes["new_node_001"].region == "us-east"
        assert "new_node_001" in protocol.node_metadata

    @pytest.mark.asyncio
    async def test_gossip_protocol_heartbeat_handling(self, sample_gossip_protocol):
        """Test handling heartbeat messages."""
        protocol = sample_gossip_protocol

        # Add a known node first
        from mpreg.federation.federation_gossip import HeartbeatPayload

        protocol.known_nodes["heartbeat_node"] = NodeMetadata(
            node_id="heartbeat_node", last_heartbeat=0
        )

        # Create heartbeat message
        heartbeat_message = GossipMessage(
            message_id="heartbeat_001",
            message_type=GossipMessageType.HEARTBEAT,
            sender_id="heartbeat_node",
            payload=HeartbeatPayload(sender_id="heartbeat_node"),
        )

        # Handle heartbeat
        await protocol.handle_received_message(heartbeat_message)

        # Check that heartbeat was recorded
        assert protocol.known_nodes["heartbeat_node"].last_heartbeat > 0

    @pytest.mark.asyncio
    async def test_gossip_protocol_cleanup(self, sample_gossip_protocol):
        """Test gossip protocol message cleanup."""
        protocol = sample_gossip_protocol

        # Add expired message
        expired_message = GossipMessage(
            message_id="expired_msg",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="test_node",
            payload={},
            expires_at=time.time() - 1,  # Already expired
        )

        protocol.recent_messages["expired_msg"] = expired_message
        protocol.pending_messages.append(expired_message)

        # Perform cleanup
        await protocol._cleanup_expired_messages()

        # Check that expired message was removed
        assert "expired_msg" not in protocol.recent_messages
        assert expired_message not in protocol.pending_messages

    def test_gossip_protocol_state_provider_registration(self, sample_gossip_protocol):
        """Test state provider registration."""
        protocol = sample_gossip_protocol

        def mock_state_provider():
            return {"test": "state"}

        initial_count = len(protocol.state_providers)

        # Register state provider
        protocol.register_state_provider(mock_state_provider)

        assert len(protocol.state_providers) == initial_count + 1
        assert mock_state_provider in protocol.state_providers

    def test_gossip_protocol_convergence_status(self, sample_gossip_protocol):
        """Test gossip protocol convergence status."""
        protocol = sample_gossip_protocol

        # Add some test data
        protocol.known_nodes["node_1"] = NodeMetadata(
            node_id="node_1", region="us-east"
        )
        protocol.state_cache["key_1"] = "value_1"

        status = protocol.get_convergence_status()

        assert hasattr(status, "node_id")
        assert hasattr(status, "known_nodes")
        assert hasattr(status, "pending_messages")
        assert hasattr(status, "recent_messages")
        assert hasattr(status, "state_cache_size")
        assert hasattr(status, "vector_clock")
        assert hasattr(status, "vector_clock")

        assert status.node_id == "test_node_001"
        assert status.known_nodes == 1
        assert status.state_cache_size == 1

    def test_gossip_protocol_comprehensive_statistics(self, sample_gossip_protocol):
        """Test gossip protocol comprehensive statistics."""
        protocol = sample_gossip_protocol

        stats = protocol.get_comprehensive_statistics()

        assert hasattr(stats, "protocol_info")
        assert hasattr(stats, "protocol_stats")
        assert hasattr(stats, "scheduler_stats")
        assert hasattr(stats, "filter_stats")
        assert hasattr(stats, "state_info")
        assert hasattr(stats, "convergence_status")

        # Check protocol info
        protocol_info = stats.protocol_info
        assert protocol_info.node_id == "test_node_001"
        assert protocol_info.gossip_strategy == GossipStrategy.RANDOM.value
        assert protocol_info.gossip_interval == 0.1
        assert protocol_info.fanout == 2


class TestEndToEndGossipScenarios:
    """Test suite for end-to-end gossip scenarios."""

    @pytest.mark.asyncio
    async def test_two_node_gossip_convergence(self):
        """Test gossip convergence between two nodes."""
        # Create two gossip protocol instances
        protocol1 = GossipProtocol("node_1", gossip_interval=0.1, fanout=1)
        protocol2 = GossipProtocol("node_2", gossip_interval=0.1, fanout=1)

        # Add each other as known nodes
        protocol1.known_nodes["node_2"] = NodeMetadata(node_id="node_2", region="test")
        protocol2.known_nodes["node_1"] = NodeMetadata(node_id="node_1", region="test")

        # Create a state update message
        state_message = GossipMessage(
            message_id="state_001",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="node_1",
            payload={"key": "shared_state", "value": "test_value"},
        )

        # Add message to first protocol
        await protocol1.add_message(state_message)

        # Simulate message propagation
        # In a real implementation, this would happen over network
        await protocol2.handle_received_message(state_message)

        # Check convergence
        assert "shared_state" in protocol2.state_cache
        assert protocol2.state_cache["shared_state"] == "test_value"
        assert protocol2.protocol_stats.messages_received == 1

    @pytest.mark.asyncio
    async def test_multiple_node_gossip_propagation(self):
        """Test gossip propagation across multiple nodes."""
        # Create multiple gossip protocol instances
        protocols = {}
        for i in range(5):
            protocols[f"node_{i}"] = GossipProtocol(
                f"node_{i}", gossip_interval=0.1, fanout=2
            )

        # Set up topology (each node knows about two others)
        for i in range(5):
            node_id = f"node_{i}"
            # Each node knows about next two nodes (circular)
            protocols[node_id].known_nodes[f"node_{(i + 1) % 5}"] = NodeMetadata(
                node_id=f"node_{(i + 1) % 5}", region="test"
            )
            protocols[node_id].known_nodes[f"node_{(i + 2) % 5}"] = NodeMetadata(
                node_id=f"node_{(i + 2) % 5}", region="test"
            )

        # Create a rumor message
        rumor_message = GossipMessage(
            message_id="rumor_001",
            message_type=GossipMessageType.RUMOR,
            sender_id="node_0",
            payload={"rumor": "important_news", "data": "test_data"},
        )

        # Start rumor at node_0
        await protocols["node_0"].add_message(rumor_message)

        # Simulate propagation by having each node process the message
        # In reality, this would happen through network communication
        for target_node in ["node_1", "node_2"]:  # Direct neighbors of node_0
            propagated_msg = rumor_message.prepare_for_propagation("node_0")
            await protocols[target_node].handle_received_message(propagated_msg)

        # Check that message reached other nodes
        assert protocols["node_1"].protocol_stats.messages_received == 1
        assert protocols["node_2"].protocol_stats.messages_received == 1

        # Check that vector clocks were updated
        assert "node_0" in protocols["node_1"].vector_clock.to_dict()
        assert "node_0" in protocols["node_2"].vector_clock.to_dict()

    @pytest.mark.asyncio
    async def test_gossip_loop_prevention(self):
        """Test gossip loop prevention mechanisms."""
        # Create three nodes in a triangle
        protocols = {
            "node_a": GossipProtocol("node_a", gossip_interval=0.1),
            "node_b": GossipProtocol("node_b", gossip_interval=0.1),
            "node_c": GossipProtocol("node_c", gossip_interval=0.1),
        }

        # Set up fully connected topology
        for node_id, protocol in protocols.items():
            for other_id in protocols.keys():
                if other_id != node_id:
                    protocol.known_nodes[other_id] = NodeMetadata(
                        node_id=other_id, region="test"
                    )

        # Create message that has already traveled in a loop
        loop_message = GossipMessage(
            message_id="loop_test_001",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="node_a",
            payload={"test": "loop_data"},
            propagation_path=["node_a", "node_b", "node_c"],  # Already been around
        )

        # Try to send back to node_a (should be filtered)
        should_propagate = protocols["node_a"].filter.should_propagate(
            loop_message, "node_a"
        )
        assert not should_propagate

        # Check that loop was detected
        assert protocols["node_a"].filter.duplicate_count > 0


class TestGossipPerformanceAndScalability:
    """Test suite for gossip performance and scalability."""

    @pytest.mark.asyncio
    async def test_gossip_protocol_performance(self):
        """Test gossip protocol performance with many messages."""
        protocol = GossipProtocol("perf_test_node", gossip_interval=0.01)

        # Create many unique messages (different message IDs and content)
        messages = []
        for i in range(100):
            message = GossipMessage(
                message_id=f"perf_msg_{i}_{time.time()}_{random.randint(1000, 9999)}",  # Unique ID
                message_type=GossipMessageType.STATE_UPDATE,
                sender_id=f"sender_node_{i}",  # Different senders
                payload={"key": f"key_{i}", "value": f"value_{i}", "index": i},
            )
            messages.append(message)

        # Time message processing
        start_time = time.time()

        for message in messages:
            await protocol.add_message(message)

        processing_time = time.time() - start_time

        # Should process messages quickly
        assert processing_time < 1.0  # Should complete within 1 second
        # Note: Some messages might be filtered if they're seen as duplicates by digest
        assert (
            len(protocol.pending_messages) >= 50
        )  # Should have at least half the messages

    @pytest.mark.asyncio
    async def test_gossip_filter_efficiency(self):
        """Test gossip filter efficiency with many duplicates."""
        filter_obj = GossipFilter(max_seen_messages=1000)

        # Create base message
        base_message = GossipMessage(
            message_id="efficiency_test",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id="sender_node",
            payload={"test": "data"},
        )

        # Test first propagation (should succeed)
        should_propagate = filter_obj.should_propagate(base_message, "test_node")
        assert should_propagate

        # Test many duplicate attempts
        start_time = time.time()

        for i in range(1000):
            should_propagate = filter_obj.should_propagate(base_message, "test_node")
            assert not should_propagate  # All should be filtered

        filtering_time = time.time() - start_time

        # Should filter quickly
        assert filtering_time < 0.5  # Should complete within 500ms
        assert filter_obj.duplicate_count == 1000

    def test_vector_clock_performance(self):
        """Test vector clock performance with many operations."""
        clock = VectorClock.empty()

        # Time many increment operations
        start_time = time.time()

        for i in range(1000):
            clock = clock.increment(f"node_{i % 10}")  # 10 different nodes

        increment_time = time.time() - start_time

        # Should be very fast
        assert increment_time < 0.1  # Should complete within 100ms
        assert len(clock.to_dict()) == 10  # Should have 10 nodes

        # Test comparison performance
        other_clock = VectorClock.empty()
        for i in range(10):
            other_clock = other_clock.increment(f"node_{i}")

        start_time = time.time()

        for i in range(1000):
            result = clock.compare(other_clock)
            assert result in ["before", "after", "concurrent", "equal"]

        comparison_time = time.time() - start_time

        # Should be very fast
        assert comparison_time < 0.1  # Should complete within 100ms

    @pytest.mark.asyncio
    async def test_gossip_scheduler_scalability(self):
        """Test gossip scheduler scalability with many nodes."""
        scheduler = GossipScheduler(gossip_interval=0.1, fanout=10)

        # Create large node list
        available_nodes = [f"node_{i}" for i in range(1000)]

        # Time target selection
        start_time = time.time()

        for i in range(100):
            targets = scheduler.select_gossip_targets(available_nodes)
            assert len(targets) == 10  # Should select fanout number of targets
            assert all(target in available_nodes for target in targets)

        selection_time = time.time() - start_time

        # Should select targets quickly even with many nodes
        assert selection_time < 1.0  # Should complete within 1 second


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
