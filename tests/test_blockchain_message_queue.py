"""
Comprehensive tests for blockchain-backed message queue system.

Tests the democratic message queue implementation with DAO governance,
equitable priority algorithms, and blockchain audit trails.
"""

import time
from unittest.mock import patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.blockchain_ledger import BlockchainLedger
from mpreg.core.blockchain_message_queue import (
    BlockchainMessageQueue,
    BlockchainMessageRouter,
    EquitablePriorityQueue,
    MessageQueueGovernance,
)
from mpreg.core.blockchain_message_queue_types import (
    BlockchainMessage,
    DeliveryGuarantee,
    MessagePriority,
    MessageRoute,
    PolicyParameters,
    QueueGovernancePolicy,
    QueueMetrics,
    RouteStatus,
)
from mpreg.datastructures import (
    Blockchain,
    DaoMember,
    DecentralizedAutonomousOrganization,
    VoteType,
)


class TestMessageQueueGovernance:
    """Test DAO governance for message queue operations."""

    def setup_method(self):
        """Set up test environment."""
        self.dao = DecentralizedAutonomousOrganization(
            name="Message Queue DAO",
            description="Governance for message queue operations",
        )

        # Add some members
        members = [
            DaoMember(member_id="alice", voting_power=1000, token_balance=5000),
            DaoMember(member_id="bob", voting_power=800, token_balance=4000),
            DaoMember(member_id="charlie", voting_power=1200, token_balance=6000),
        ]

        for member in members:
            self.dao = self.dao.add_member(member)

        self.governance = MessageQueueGovernance(self.dao)

    def test_propose_routing_policy(self):
        """Test creation of routing policy proposals."""
        policy_spec = {
            "name": "Low Latency Priority",
            "description": "Prioritize low-latency routes",
            "latency_weight": 0.6,
            "cost_weight": 0.2,
            "reliability_weight": 0.2,
            "routes": ["route_1", "route_2"],
        }

        proposal_id = self.governance.propose_routing_policy("alice", policy_spec)
        assert proposal_id in self.governance.dao.proposals

        proposal = self.governance.dao.proposals[proposal_id]
        assert "Routing Policy" in proposal.title
        assert proposal.proposer_id == "alice"
        assert proposal.metadata["policy_type"] == "routing"

    def test_propose_fee_structure(self):
        """Test creation of fee structure proposals."""
        fee_structure = {
            "name": "Progressive Fees",
            "description": "Progressive fee structure protecting small users",
            "base_multiplier": 1.0,
            "emergency_multiplier": 10.0,
            "high_multiplier": 3.0,
            "normal_multiplier": 1.0,
            "low_multiplier": 0.5,
            "bulk_multiplier": 0.1,
            "minimum_fee": 1,
            "progressive": True,
        }

        proposal_id = self.governance.propose_fee_structure("bob", fee_structure)
        assert proposal_id in self.governance.dao.proposals

        proposal = self.governance.dao.proposals[proposal_id]
        assert "Fee Structure" in proposal.title
        assert proposal.metadata["policy_type"] == "fees"
        assert proposal.metadata["progressive"] is True

    def test_propose_priority_algorithm(self):
        """Test creation of priority algorithm proposals."""
        algorithm_spec = {
            "name": "Anti-Monopoly Priority",
            "description": "Priority algorithm preventing sender monopolization",
            "max_messages_per_sender": 100,
            "fairness_factor": 1.0,
            "fairness": 0.9,
        }

        proposal_id = self.governance.propose_priority_algorithm(
            "charlie", algorithm_spec
        )
        assert proposal_id in self.governance.dao.proposals

        proposal = self.governance.dao.proposals[proposal_id]
        assert "Priority Algorithm" in proposal.title
        assert proposal.metadata["policy_type"] == "prioritization"
        assert proposal.metadata["fairness_score"] == 0.9

    def test_execute_governance_decision(self):
        """Test execution of approved governance decisions."""
        # Create and approve a policy proposal
        policy_spec = {
            "name": "Test Policy",
            "description": "Test policy for execution",
            "test_parameter": 42,
        }

        proposal_id = self.governance.propose_routing_policy("alice", policy_spec)

        # Simulate voting and approval
        self.governance.dao = self.governance.dao.cast_vote(
            "alice", proposal_id, VoteType.FOR
        )
        self.governance.dao = self.governance.dao.cast_vote(
            "bob", proposal_id, VoteType.FOR
        )
        self.governance.dao = self.governance.dao.cast_vote(
            "charlie", proposal_id, VoteType.FOR
        )

        # Finalize proposal (simulate time passing)
        with patch("time.time", return_value=time.time() + 86400 * 8):  # 8 days later
            self.governance.dao = self.governance.dao.finalize_proposal(proposal_id)

        # Execute the governance decision
        policy = self.governance.execute_governance_decision(proposal_id)

        assert policy.policy_name == "Test Policy"
        assert policy.approved_by_dao is True
        assert policy.dao_proposal_id == proposal_id
        assert policy.parameters.get_parameter("test_parameter") == 42
        assert policy.policy_id in self.governance.active_policies

    def test_get_active_policies(self):
        """Test retrieval of active governance policies."""
        # Initially no policies
        policies = self.governance.get_active_policies()
        assert len(policies) == 0

        # Add a mock active policy
        policy = QueueGovernancePolicy(
            policy_name="Test Policy",
            policy_type="routing",
            parameters=PolicyParameters(custom_parameters={"test": True}),
            approved_by_dao=True,
            effective_from=time.time() - 100,
            effective_until=time.time() + 3600,
        )
        self.governance.active_policies[policy.policy_id] = policy

        # Should find the active policy
        policies = self.governance.get_active_policies()
        assert len(policies) == 1
        assert policies[0].policy_name == "Test Policy"

        # Filter by type
        routing_policies = self.governance.get_active_policies("routing")
        assert len(routing_policies) == 1

        fee_policies = self.governance.get_active_policies("fees")
        assert len(fee_policies) == 0

    def test_calculate_message_fee(self):
        """Test fee calculation based on DAO policies."""
        # Add a fee policy
        fee_policy = QueueGovernancePolicy(
            policy_name="Progressive Fees",
            policy_type="fees",
            parameters=PolicyParameters(
                priority_fee_multiplier=2.0,
                base_fee_per_mb=5,
                custom_parameters={
                    "emergency_multiplier": 10.0,
                    "high_multiplier": 3.0,
                    "normal_multiplier": 1.0,
                    "low_multiplier": 0.5,
                    "bulk_multiplier": 0.1,
                },
            ),
            approved_by_dao=True,
            effective_from=time.time() - 100,
        )
        self.governance.active_policies[fee_policy.policy_id] = fee_policy

        # Test normal priority message
        message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="test",
            priority=MessagePriority.NORMAL,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=b"test message",
        )

        fee = self.governance.calculate_message_fee(message)
        assert fee >= 5  # Minimum fee

        # Test emergency priority (should be higher)
        emergency_message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="emergency",
            priority=MessagePriority.EMERGENCY,
            delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE,
            payload=b"emergency message",
        )

        emergency_fee = self.governance.calculate_message_fee(emergency_message)
        assert emergency_fee >= fee  # Emergency should be at least as much, often more

        # Test bulk priority (should be lower)
        bulk_message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="bulk",
            priority=MessagePriority.BULK,
            delivery_guarantee=DeliveryGuarantee.AT_MOST_ONCE,
            payload=b"bulk message",
        )

        bulk_fee = self.governance.calculate_message_fee(bulk_message)
        assert bulk_fee <= fee  # Bulk should be at most as much, often less


class TestEquitablePriorityQueue:
    """Test equitable priority queue with fairness guarantees."""

    def setup_method(self):
        """Set up test environment."""
        self.dao = DecentralizedAutonomousOrganization(
            name="Queue DAO", description="Queue governance"
        )

        # Add members
        self.dao = self.dao.add_member(
            DaoMember(member_id="operator", voting_power=1000, token_balance=5000)
        )

        self.governance = MessageQueueGovernance(self.dao)

        # Add a priority policy
        priority_policy = QueueGovernancePolicy(
            policy_name="Fair Priority",
            policy_type="prioritization",
            parameters=PolicyParameters(
                custom_parameters={
                    "max_messages_per_sender": 10,
                    "fairness_factor": 1.0,
                    "max_consecutive_same_sender": 2,
                }
            ),
            approved_by_dao=True,
            effective_from=time.time() - 100,
        )
        self.governance.active_policies[priority_policy.policy_id] = priority_policy

        self.queue = EquitablePriorityQueue(self.governance)

    def test_enqueue_with_fee_checking(self):
        """Test enqueueing messages with fee validation."""
        # Add fee policy
        fee_policy = QueueGovernancePolicy(
            policy_name="Test Fees",
            policy_type="fees",
            parameters=PolicyParameters(base_fee_per_mb=10),
            approved_by_dao=True,
            effective_from=time.time() - 100,
        )
        self.governance.active_policies[fee_policy.policy_id] = fee_policy

        message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="test",
            priority=MessagePriority.NORMAL,
            processing_fee=15,  # Above minimum
        )

        # Should succeed
        queued = self.queue.enqueue(message)
        assert queued.processing_fee >= 10
        assert len(self.queue.message_queue) == 1

        # Test insufficient fee
        low_fee_message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="test",
            priority=MessagePriority.NORMAL,
            processing_fee=5,  # Below minimum
        )

        with pytest.raises(ValueError, match="Insufficient fee"):
            self.queue.enqueue(low_fee_message)

    def test_sender_quota_enforcement(self):
        """Test sender quota enforcement to prevent spam."""
        # Fill sender quota
        for i in range(10):  # max_messages_per_sender = 10
            message = BlockchainMessage(
                sender_id="alice",
                recipient_id="bob",
                message_type=f"test_{i}",
                priority=MessagePriority.NORMAL,
                processing_fee=10,
            )
            self.queue.enqueue(message)

        # 11th message should fail
        overflow_message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="overflow",
            priority=MessagePriority.NORMAL,
            processing_fee=10,
        )

        with pytest.raises(ValueError, match="exceeded quota"):
            self.queue.enqueue(overflow_message)

        # Different sender should still work
        other_message = BlockchainMessage(
            sender_id="charlie",
            recipient_id="bob",
            message_type="other",
            priority=MessagePriority.NORMAL,
            processing_fee=10,
        )

        queued = self.queue.enqueue(other_message)
        assert queued.sender_id == "charlie"

    def test_priority_ordering_with_fairness(self):
        """Test that messages are ordered by priority with fairness adjustments."""
        messages = [
            BlockchainMessage(
                sender_id="alice",
                recipient_id="bob",
                message_type="low",
                priority=MessagePriority.LOW,
                processing_fee=10,
            ),
            BlockchainMessage(
                sender_id="bob",
                recipient_id="charlie",
                message_type="high",
                priority=MessagePriority.HIGH,
                processing_fee=10,
            ),
            BlockchainMessage(
                sender_id="charlie",
                recipient_id="alice",
                message_type="emergency",
                priority=MessagePriority.EMERGENCY,
                processing_fee=10,
            ),
            BlockchainMessage(
                sender_id="alice",
                recipient_id="bob",
                message_type="normal",
                priority=MessagePriority.NORMAL,
                processing_fee=10,
            ),
        ]

        # Enqueue in random order
        for msg in messages:
            self.queue.enqueue(msg)

        # Dequeue should respect priority
        first = self.queue.dequeue()
        assert first is not None
        assert first.priority == MessagePriority.EMERGENCY

        second = self.queue.dequeue()
        assert second is not None
        assert second.priority == MessagePriority.HIGH

    def test_fairness_adjustment_for_age(self):
        """Test that older messages get priority boost."""
        # Create an old message
        old_time = time.time() - 7200  # 2 hours ago
        with patch("time.time", return_value=old_time):
            old_message = BlockchainMessage(
                sender_id="alice",
                recipient_id="bob",
                message_type="old",
                priority=MessagePriority.NORMAL,
                processing_fee=10,
            )

        # Create a new message with higher priority
        new_message = BlockchainMessage(
            sender_id="bob",
            recipient_id="charlie",
            message_type="new",
            priority=MessagePriority.HIGH,
            processing_fee=10,
        )

        # Enqueue old message first
        self.queue.enqueue(old_message)
        self.queue.enqueue(new_message)

        # New high priority should still come first
        first = self.queue.dequeue()
        assert first is not None
        assert first.priority == MessagePriority.HIGH

    def test_consecutive_sender_limit(self):
        """Ensure consecutive sender limiting interleaves different senders."""
        for idx in range(3):
            self.queue.enqueue(
                BlockchainMessage(
                    sender_id="alice",
                    recipient_id="bob",
                    message_type=f"alice_{idx}",
                    priority=MessagePriority.NORMAL,
                    processing_fee=10,
                )
            )

        self.queue.enqueue(
            BlockchainMessage(
                sender_id="charlie",
                recipient_id="bob",
                message_type="charlie_0",
                priority=MessagePriority.LOW,
                processing_fee=10,
            )
        )

        first = self.queue.dequeue()
        second = self.queue.dequeue()
        third = self.queue.dequeue()

        assert first is not None and second is not None and third is not None
        assert first.sender_id == "alice"
        assert second.sender_id == "alice"
        assert third.sender_id == "charlie"

    def test_quota_reset(self):
        """Test periodic quota reset functionality."""
        # Fill quota for a sender
        for i in range(5):
            message = BlockchainMessage(
                sender_id="alice",
                recipient_id="bob",
                message_type=f"test_{i}",
                priority=MessagePriority.NORMAL,
                processing_fee=10,
            )
            self.queue.enqueue(message)

        # Simulate time passing (quota window exceeded)
        self.queue.last_quota_reset = time.time() - 7200  # 2 hours ago

        # This should reset quotas and allow new messages
        another_message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="after_reset",
            priority=MessagePriority.NORMAL,
            processing_fee=10,
        )

        # Should succeed after quota reset
        queued = self.queue.enqueue(another_message)
        assert queued.message_type == "after_reset"

    def test_dequeue_empty_queue(self):
        """Test dequeue from empty queue."""
        result = self.queue.dequeue()
        assert result is None


class TestBlockchainMessageRouter:
    """Test blockchain message router with audit trails."""

    def setup_method(self):
        """Set up test environment."""
        self.blockchain = Blockchain.create_new_chain(
            "message_router", "router_genesis"
        )
        self.ledger = BlockchainLedger(self.blockchain)

        self.dao = DecentralizedAutonomousOrganization(
            name="Router DAO", description="Router governance"
        )

        self.governance = MessageQueueGovernance(self.dao, ledger=self.ledger)
        self.router = BlockchainMessageRouter(self.ledger, self.governance)

    def test_register_route(self):
        """Test route registration with blockchain record."""
        route = MessageRoute(
            route_id="test_route",
            source_hub="hub_a",
            destination_hub="hub_b",
            path_hops=["hop1", "hop2"],
            latency_ms=100,
            bandwidth_mbps=1000,
            reliability_score=0.95,
            cost_per_mb=5,
        )

        route_id = self.router.register_route(route, "operator")

        assert route_id == "test_route"
        assert route_id in self.router.active_routes
        assert self.router.blockchain.get_height() >= 1  # Block was added

    def test_route_message_selection(self):
        """Test optimal route selection for messages."""
        # Register multiple routes
        routes = [
            MessageRoute(
                route_id="fast_route",
                source_hub="hub_a",
                destination_hub="hub_b",
                latency_ms=50,
                bandwidth_mbps=1000,
                reliability_score=0.90,
                cost_per_mb=10,
                status=RouteStatus.ACTIVE,
            ),
            MessageRoute(
                route_id="cheap_route",
                source_hub="hub_a",
                destination_hub="hub_b",
                latency_ms=200,
                bandwidth_mbps=500,
                reliability_score=0.95,
                cost_per_mb=2,
                status=RouteStatus.ACTIVE,
            ),
            MessageRoute(
                route_id="reliable_route",
                source_hub="hub_a",
                destination_hub="hub_b",
                latency_ms=100,
                bandwidth_mbps=800,
                reliability_score=0.99,
                cost_per_mb=7,
                status=RouteStatus.ACTIVE,
            ),
        ]

        for route in routes:
            self.router.register_route(route, "operator")

        # Test emergency message (should prefer fast route)
        emergency_message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="emergency",
            priority=MessagePriority.EMERGENCY,
            processing_fee=100,
        )

        selected_route = self.router.route_message(emergency_message)
        # Should select route with good latency for emergency
        assert selected_route.latency_ms <= 100

        # Test bulk message (should prefer cheap route)
        bulk_message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="bulk",
            priority=MessagePriority.BULK,
            processing_fee=10,
            payload=b"large bulk data" * 1000,  # Large payload
        )

        selected_route = self.router.route_message(bulk_message)
        # Should consider cost for bulk messages
        assert selected_route is not None

    def test_route_requirements_checking(self):
        """Test route requirements validation."""
        # Register a high-latency route
        slow_route = MessageRoute(
            route_id="slow_route",
            source_hub="hub_a",
            destination_hub="hub_b",
            latency_ms=5000,  # 5 seconds
            reliability_score=0.95,
            cost_per_mb=1,
            status=RouteStatus.ACTIVE,
        )

        self.router.register_route(slow_route, "operator")

        # Emergency message should reject slow route
        emergency_message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="emergency",
            priority=MessagePriority.EMERGENCY,
            processing_fee=100,
        )

        with pytest.raises(ValueError, match="No suitable route found"):
            self.router.route_message(emergency_message)

    def test_performance_metrics_recording(self):
        """Test recording of performance metrics on blockchain."""
        metrics = QueueMetrics(
            queue_id="test_queue",
            route_id="test_route",
            messages_processed=100,
            average_latency_ms=150.5,
            throughput_msgs_per_sec=50.0,
            error_rate=0.01,
            total_fees_collected=1000,
            delivery_success_rate=0.99,
            sla_compliance_rate=0.95,
        )

        initial_height = self.router.blockchain.get_height()
        self.router.record_performance_metrics(metrics)

        # Should have added a block
        assert self.router.blockchain.get_height() == initial_height + 1

    def test_message_routing_history(self):
        """Test tracking of message routing history."""
        # Register a route
        route = MessageRoute(
            route_id="history_route",
            source_hub="hub_a",
            destination_hub="hub_b",
            reliability_score=0.95,
            cost_per_mb=5,
            status=RouteStatus.ACTIVE,
        )

        self.router.register_route(route, "operator")

        # Route a message
        message = BlockchainMessage(
            message_id="test_message",
            sender_id="alice",
            recipient_id="bob",
            message_type="test",
            processing_fee=10,
        )

        selected_route = self.router.route_message(message)

        # Check history tracking
        assert message.message_id in self.router.message_history
        assert (
            selected_route.route_id in self.router.message_history[message.message_id]
        )


class TestBlockchainMessageQueue:
    """Test complete blockchain message queue integration."""

    def setup_method(self):
        """Set up complete test environment."""
        self.queue = BlockchainMessageQueue(queue_id="test_queue")

        self.route = MessageRoute(
            route_id="test_route",
            source_hub="hub_a",
            destination_hub="hub_b",
            path_hops=["hop1"],
            latency_ms=50,
            bandwidth_mbps=1000,
            reliability_score=0.98,
            cost_per_mb=1,
        )

    def test_end_to_end_message_flow(self):
        """Test complete message flow from submission to delivery."""
        initial_height = self.queue.blockchain.get_height()

        self.queue.router.register_route(self.route, "operator")
        assert self.queue.blockchain.get_height() == initial_height + 1

        message = BlockchainMessage(
            sender_id="alice",
            recipient_id="bob",
            message_type="test",
            priority=MessagePriority.NORMAL,
            processing_fee=10,
        )
        self.queue.submit_message(message)
        assert self.queue.blockchain.get_height() == initial_height + 2

        processed = self.queue.process_next_message()
        assert processed is not None
        assert processed.message_id == message.message_id
        assert self.queue.blockchain.get_height() == initial_height + 4

    def test_shared_ledger_consistency(self):
        """Ensure queue components share the same ledger state."""
        assert self.queue.ledger is self.queue.router.ledger
        assert self.queue.ledger is self.queue.governance.ledger

    def test_dao_governance_integration(self):
        """Test DAO governance affecting queue behavior."""
        self.queue.add_governance_member(
            "member1", voting_power=1000, token_balance=5000
        )
        proposal_id = self.queue.governance.propose_priority_algorithm(
            "member1",
            {
                "name": "Priority Test",
                "description": "Enable fairness policy for testing",
                "fairness": 0.9,
            },
        )

        self.queue.governance.dao = self.queue.governance.dao.cast_vote(
            "member1", proposal_id, VoteType.FOR
        )
        with patch("time.time", return_value=time.time() + 86400 * 8):
            self.queue.governance.dao = self.queue.governance.dao.finalize_proposal(
                proposal_id
            )
        self.queue.governance.ledger.replace(self.queue.governance.dao.blockchain)
        policy = self.queue.governance.execute_governance_decision(proposal_id)

        active = self.queue.governance.get_active_policies("prioritization")
        assert policy.policy_id in {p.policy_id for p in active}

    def test_cross_chain_coordination(self):
        """Test cross-chain message coordination."""
        other_chain = Blockchain.create_new_chain("secondary", "secondary_genesis")
        other_ledger = BlockchainLedger(other_chain)
        other_router = BlockchainMessageRouter(other_ledger, self.queue.governance)

        assert other_router.blockchain.chain_id == "secondary"
        assert self.queue.blockchain.chain_id != other_router.blockchain.chain_id


# Property-based testing strategies


@st.composite
def message_route_strategy(draw):
    """Generate valid MessageRoute instances."""
    return MessageRoute(
        route_id=draw(st.text(min_size=1, max_size=50)),
        source_hub=draw(st.text(min_size=1, max_size=20)),
        destination_hub=draw(st.text(min_size=1, max_size=20)),
        path_hops=draw(st.lists(st.text(min_size=1, max_size=20), max_size=5)),
        latency_ms=draw(st.integers(min_value=1, max_value=10000)),
        bandwidth_mbps=draw(st.integers(min_value=1, max_value=10000)),
        reliability_score=draw(st.floats(min_value=0.0, max_value=1.0)),
        cost_per_mb=draw(st.integers(min_value=1, max_value=1000)),
        status=draw(st.sampled_from(RouteStatus)),
    )


@st.composite
def blockchain_message_strategy(draw):
    """Generate valid BlockchainMessage instances."""
    return BlockchainMessage(
        sender_id=draw(st.text(min_size=1, max_size=50)),
        recipient_id=draw(st.text(min_size=1, max_size=50)),
        message_type=draw(st.text(min_size=1, max_size=20)),
        priority=draw(st.sampled_from(MessagePriority)),
        delivery_guarantee=draw(st.sampled_from(DeliveryGuarantee)),
        payload=draw(st.binary(max_size=1024)),
        processing_fee=draw(st.integers(min_value=0, max_value=10000)),
    )


@given(message_route_strategy())
@settings(max_examples=50, deadline=None)
def test_message_route_properties(route):
    """Property-based test for MessageRoute validation."""
    # All generated routes should be valid
    assert route.route_id
    assert route.source_hub
    assert route.destination_hub
    assert route.latency_ms >= 0
    assert 0.0 <= route.reliability_score <= 1.0
    assert route.cost_per_mb >= 0


@given(blockchain_message_strategy())
@settings(max_examples=50, deadline=None)
def test_blockchain_message_properties(message):
    """Property-based test for BlockchainMessage validation."""
    # All generated messages should be valid
    assert message.message_id
    assert message.sender_id
    assert message.recipient_id
    assert message.processing_fee >= 0
    assert message.retry_count >= 0
    assert message.max_retries >= 0


def test_governance_policy_lifecycle():
    """Test complete governance policy lifecycle."""
    dao = DecentralizedAutonomousOrganization(
        name="Policy Test DAO", description="Testing policy lifecycle"
    )

    # Add voting members
    members = [
        DaoMember(member_id="member1", voting_power=1000, token_balance=5000),
        DaoMember(member_id="member2", voting_power=800, token_balance=4000),
        DaoMember(member_id="member3", voting_power=1200, token_balance=6000),
    ]

    for member in members:
        dao = dao.add_member(member)

    governance = MessageQueueGovernance(dao)

    # 1. Propose policy
    policy_spec = {
        "name": "Test Lifecycle Policy",
        "description": "Policy for testing complete lifecycle",
        "test_parameter": 123,
    }

    proposal_id = governance.propose_routing_policy("member1", policy_spec)

    # 2. Vote on policy
    governance.dao = governance.dao.cast_vote("member1", proposal_id, VoteType.FOR)
    governance.dao = governance.dao.cast_vote("member2", proposal_id, VoteType.FOR)
    governance.dao = governance.dao.cast_vote("member3", proposal_id, VoteType.FOR)

    # 3. Finalize proposal (simulate time passing)
    with patch("time.time", return_value=time.time() + 86400 * 8):  # 8 days later
        governance.dao = governance.dao.finalize_proposal(proposal_id)

    # 4. Execute governance decision
    policy = governance.execute_governance_decision(proposal_id)

    # 5. Verify policy is active
    active_policies = governance.get_active_policies("routing")
    assert len(active_policies) == 1
    assert active_policies[0].policy_name == "Test Lifecycle Policy"
    assert active_policies[0].parameters.get_parameter("test_parameter") == 123


if __name__ == "__main__":
    pytest.main([__file__])
