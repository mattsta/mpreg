"""
Comprehensive tests for distributed state management system.

This module tests the distributed state management components including:
- StateValue versioned state with vector clocks
- ConflictResolver advanced conflict resolution algorithms
- ConsensusManager distributed consensus protocols
- StateConflict detection and resolution
- End-to-end state synchronization scenarios

Test Coverage:
- Vector clock-based state versioning
- Conflict detection and resolution strategies
- Consensus proposal and voting mechanisms
- CRDT-like state merging operations
- Performance and scalability validation
"""

import time
from unittest.mock import Mock

import pytest

from mpreg.datastructures.vector_clock import VectorClock
from mpreg.fabric.consensus import (
    ConflictResolutionStrategy,
    ConflictResolver,
    ConsensusManager,
    ConsensusProposal,
    ConsensusStatus,
    StateConflict,
    StateType,
    StateValue,
)
from mpreg.fabric.gossip import GossipProtocol


@pytest.fixture
def sample_vector_clock():
    """Create a sample vector clock for testing."""
    clock = VectorClock.empty()
    clock = clock.increment("node_1")
    clock = clock.increment("node_2")
    return clock


@pytest.fixture
def sample_state_value(sample_vector_clock):
    """Create a sample state value for testing."""
    return StateValue(
        value="test_value",
        vector_clock=sample_vector_clock,
        node_id="test_node",
        timestamp=time.time(),
        version=1,
        state_type=StateType.SIMPLE_VALUE,
    )


@pytest.fixture
def sample_conflict_resolver():
    """Create a sample conflict resolver for testing."""
    return ConflictResolver()


@pytest.fixture
def sample_gossip_protocol():
    """Create a mock gossip protocol for testing."""
    return Mock(spec=GossipProtocol)


@pytest.fixture
def sample_consensus_manager(sample_gossip_protocol):
    """Create a sample consensus manager for testing."""
    return ConsensusManager(
        node_id="test_consensus_node",
        gossip_protocol=sample_gossip_protocol,
        default_consensus_threshold=0.5,
        proposal_timeout=5.0,  # Short timeout for testing
    )


class TestStateValue:
    """Test suite for state value implementation."""

    def test_state_value_creation(self, sample_state_value):
        """Test state value creation."""
        state = sample_state_value

        assert state.value == "test_value"
        assert state.node_id == "test_node"
        assert state.version == 1
        assert state.state_type == StateType.SIMPLE_VALUE
        assert len(state.content_hash) == 16  # SHA256 truncated to 16 chars

    def test_state_value_content_hash(self):
        """Test state value content hash computation."""
        clock1 = VectorClock.empty()
        clock1 = clock1.increment("node_1")

        state1 = StateValue(
            value="same_value", vector_clock=clock1, node_id="node_1", timestamp=1000.0
        )

        state2 = StateValue(
            value="same_value",
            vector_clock=clock1.copy(),
            node_id="node_1",
            timestamp=1000.0,
        )

        # Same content should have same hash
        assert state1.content_hash == state2.content_hash

        # Different content should have different hash
        state3 = StateValue(
            value="different_value",
            vector_clock=clock1.copy(),
            node_id="node_1",
            timestamp=1000.0,
        )

        assert state1.content_hash != state3.content_hash

    def test_state_value_causal_relationships(self):
        """Test state value causal relationship detection."""
        # Create base vector clock
        base_clock = VectorClock.empty()
        base_clock = base_clock.increment("node_1")

        state1 = StateValue(
            value="value1", vector_clock=base_clock.copy(), node_id="node_1"
        )

        # Create causally later state
        later_clock = base_clock.copy()
        later_clock = later_clock.increment("node_1")

        state2 = StateValue(value="value2", vector_clock=later_clock, node_id="node_1")

        # Test causal relationships
        assert state2.is_causally_after(state1)
        assert state1.is_causally_before(state2)
        assert not state1.is_concurrent_with(state2)

        # Create concurrent state with different base
        concurrent_clock = VectorClock.empty()
        concurrent_clock = concurrent_clock.increment("node_2")

        state3 = StateValue(
            value="value3", vector_clock=concurrent_clock, node_id="node_2"
        )

        assert state1.is_concurrent_with(state3)
        assert state3.is_concurrent_with(state1)

    def test_state_value_copy(self, sample_state_value):
        """Test state value copy operation."""
        original = sample_state_value
        copied = original.copy()

        assert copied.value == original.value
        assert copied.node_id == original.node_id
        assert copied.content_hash == original.content_hash
        assert copied is not original
        # For immutable VectorClock, copy() returns the same object
        assert copied.vector_clock is original.vector_clock


class TestStateConflict:
    """Test suite for state conflict representation."""

    def test_state_conflict_creation(self):
        """Test state conflict creation."""
        clock1 = VectorClock.empty()
        clock1 = clock1.increment("node_1")

        clock2 = VectorClock.empty()
        clock2 = clock2.increment("node_2")

        value1 = StateValue(value="v1", vector_clock=clock1, node_id="node_1")
        value2 = StateValue(value="v2", vector_clock=clock2, node_id="node_2")

        conflict = StateConflict(
            conflict_id="test_conflict",
            state_key="test_key",
            conflicting_values=[value1, value2],
            resolution_strategy=ConflictResolutionStrategy.LAST_WRITE_WINS,
        )

        assert conflict.conflict_id == "test_conflict"
        assert conflict.state_key == "test_key"
        assert len(conflict.conflicting_values) == 2
        assert not conflict.resolved
        assert conflict.resolved_value is None

    def test_state_conflict_summary(self):
        """Test state conflict summary generation."""
        clock1 = VectorClock.empty()
        clock1 = clock1.increment("node_1")

        value1 = StateValue(value="v1", vector_clock=clock1, node_id="node_1")
        value2 = StateValue(value="v2", vector_clock=clock1.copy(), node_id="node_2")

        conflict = StateConflict(
            conflict_id="summary_test",
            state_key="summary_key",
            conflicting_values=[value1, value2],
        )

        summary = conflict.get_conflict_summary()

        assert summary.conflict_id == "summary_test"
        assert summary.state_key == "summary_key"
        assert summary.num_conflicting_values == 2
        assert "node_1" in summary.value_sources
        assert "node_2" in summary.value_sources
        assert not summary.resolved


class TestConflictResolver:
    """Test suite for conflict resolver implementation."""

    def test_conflict_resolver_initialization(self, sample_conflict_resolver):
        """Test conflict resolver initialization."""
        resolver = sample_conflict_resolver

        assert len(resolver.resolution_strategies) >= 4
        assert (
            ConflictResolutionStrategy.LAST_WRITE_WINS in resolver.resolution_strategies
        )
        assert (
            ConflictResolutionStrategy.VECTOR_CLOCK_DOMINANCE
            in resolver.resolution_strategies
        )
        assert len(resolver.custom_resolvers) == 0

    def test_last_write_wins_resolution(self, sample_conflict_resolver):
        """Test last write wins conflict resolution."""
        resolver = sample_conflict_resolver

        # Create values with different timestamps
        early_value = StateValue(
            value="early",
            vector_clock=VectorClock.empty(),
            node_id="node_1",
            timestamp=1000.0,
        )

        late_value = StateValue(
            value="late",
            vector_clock=VectorClock.empty(),
            node_id="node_2",
            timestamp=2000.0,
        )

        conflict = StateConflict(
            conflict_id="lww_test",
            state_key="test_key",
            conflicting_values=[early_value, late_value],
            resolution_strategy=ConflictResolutionStrategy.LAST_WRITE_WINS,
        )

        resolved = resolver.resolve_conflict(conflict)

        assert resolved.value == "late"
        assert resolved.timestamp == 2000.0
        assert conflict.resolved
        assert conflict.resolved_value == resolved

    def test_first_write_wins_resolution(self, sample_conflict_resolver):
        """Test first write wins conflict resolution."""
        resolver = sample_conflict_resolver

        early_value = StateValue(
            value="early",
            vector_clock=VectorClock.empty(),
            node_id="node_1",
            timestamp=1000.0,
        )

        late_value = StateValue(
            value="late",
            vector_clock=VectorClock.empty(),
            node_id="node_2",
            timestamp=2000.0,
        )

        conflict = StateConflict(
            conflict_id="fww_test",
            state_key="test_key",
            conflicting_values=[late_value, early_value],  # Order shouldn't matter
            resolution_strategy=ConflictResolutionStrategy.FIRST_WRITE_WINS,
        )

        resolved = resolver.resolve_conflict(conflict)

        assert resolved.value == "early"
        assert resolved.timestamp == 1000.0

    def test_vector_clock_dominance_resolution(self, sample_conflict_resolver):
        """Test vector clock dominance conflict resolution."""
        resolver = sample_conflict_resolver

        # Create causally ordered states
        clock1 = VectorClock.empty()
        clock1 = clock1.increment("node_1")

        clock2 = clock1.copy()
        clock2 = clock2.increment("node_1")
        clock2 = clock2.increment("node_2")

        early_value = StateValue(value="early", vector_clock=clock1, node_id="node_1")

        later_value = StateValue(value="later", vector_clock=clock2, node_id="node_2")

        conflict = StateConflict(
            conflict_id="vc_test",
            state_key="test_key",
            conflicting_values=[early_value, later_value],
            resolution_strategy=ConflictResolutionStrategy.VECTOR_CLOCK_DOMINANCE,
        )

        resolved = resolver.resolve_conflict(conflict)

        # Later value should win due to vector clock dominance
        assert resolved.value == "later"

    def test_set_merge_resolution(self, sample_conflict_resolver):
        """Test set state merge resolution."""
        resolver = sample_conflict_resolver

        value1 = StateValue(
            value={1, 2, 3},
            vector_clock=VectorClock.empty(),
            node_id="node_1",
            state_type=StateType.SET_STATE,
        )

        value2 = StateValue(
            value={3, 4, 5},
            vector_clock=VectorClock.empty(),
            node_id="node_2",
            state_type=StateType.SET_STATE,
        )

        conflict = StateConflict(
            conflict_id="set_merge_test",
            state_key="test_set",
            conflicting_values=[value1, value2],
            resolution_strategy=ConflictResolutionStrategy.MERGE_VALUES,
        )

        resolved = resolver.resolve_conflict(conflict)

        # Should merge both sets
        assert resolved.value == {1, 2, 3, 4, 5}
        assert resolved.state_type == StateType.SET_STATE

    def test_counter_merge_resolution(self, sample_conflict_resolver):
        """Test counter state merge resolution."""
        resolver = sample_conflict_resolver

        value1 = StateValue(
            value=10,
            vector_clock=VectorClock.empty(),
            node_id="node_1",
            state_type=StateType.COUNTER_STATE,
        )

        value2 = StateValue(
            value=15,
            vector_clock=VectorClock.empty(),
            node_id="node_2",
            state_type=StateType.COUNTER_STATE,
        )

        conflict = StateConflict(
            conflict_id="counter_merge_test",
            state_key="test_counter",
            conflicting_values=[value1, value2],
            resolution_strategy=ConflictResolutionStrategy.MERGE_VALUES,
        )

        resolved = resolver.resolve_conflict(conflict)

        # Should sum the counters
        assert resolved.value == 25
        assert resolved.state_type == StateType.COUNTER_STATE

    def test_map_merge_resolution(self, sample_conflict_resolver):
        """Test map state merge resolution."""
        resolver = sample_conflict_resolver

        value1 = StateValue(
            value={"a": 1, "b": 2},
            vector_clock=VectorClock.empty(),
            node_id="node_1",
            state_type=StateType.MAP_STATE,
        )

        value2 = StateValue(
            value={"b": 3, "c": 4},
            vector_clock=VectorClock.empty(),
            node_id="node_2",
            state_type=StateType.MAP_STATE,
        )

        conflict = StateConflict(
            conflict_id="map_merge_test",
            state_key="test_map",
            conflicting_values=[value1, value2],
            resolution_strategy=ConflictResolutionStrategy.MERGE_VALUES,
        )

        resolved = resolver.resolve_conflict(conflict)

        # Should merge maps (later values overwrite)
        expected = {"a": 1, "b": 3, "c": 4}
        assert resolved.value == expected

    def test_consensus_required_resolution(self, sample_conflict_resolver):
        """Test consensus-required resolution selects deterministic candidate."""
        resolver = sample_conflict_resolver

        older = StateValue(
            value="old",
            vector_clock=VectorClock.empty(),
            node_id="node_a",
            timestamp=time.time() - 10,
        )
        newer = StateValue(
            value="new",
            vector_clock=VectorClock.empty(),
            node_id="node_b",
            timestamp=time.time(),
        )

        conflict = StateConflict(
            conflict_id="consensus_required_test",
            state_key="consensus_key",
            conflicting_values=[older, newer],
            resolution_strategy=ConflictResolutionStrategy.CONSENSUS_REQUIRED,
        )

        resolved = resolver.resolve_conflict(conflict)
        assert resolved.value == "new"

    def test_custom_resolver_registration(self, sample_conflict_resolver):
        """Test custom conflict resolver registration."""
        resolver = sample_conflict_resolver

        def custom_resolver(values):
            # Always return the first value
            return values[0]

        resolver.register_custom_resolver("custom_key", custom_resolver)

        assert "custom_key" in resolver.custom_resolvers
        assert resolver.custom_resolvers["custom_key"] == custom_resolver

    def test_custom_resolver_usage(self, sample_conflict_resolver):
        """Test custom conflict resolver usage."""
        resolver = sample_conflict_resolver

        # Register custom resolver that prefers specific node
        def prefer_node_1(values):
            for value in values:
                if value.node_id == "node_1":
                    return value
            return values[0]

        resolver.register_custom_resolver("preferred_key", prefer_node_1)

        value1 = StateValue(
            value="from_node_1", vector_clock=VectorClock.empty(), node_id="node_1"
        )
        value2 = StateValue(
            value="from_node_2", vector_clock=VectorClock.empty(), node_id="node_2"
        )

        conflict = StateConflict(
            conflict_id="custom_test",
            state_key="preferred_key",
            conflicting_values=[value2, value1],  # Order shouldn't matter
        )

        resolved = resolver.resolve_conflict(conflict)

        assert resolved.value == "from_node_1"
        assert resolved.node_id == "node_1"

    def test_conflict_detection(self, sample_conflict_resolver):
        """Test conflict detection between state values."""
        resolver = sample_conflict_resolver

        # Create concurrent states (should conflict)
        clock1 = VectorClock.empty()
        clock1 = clock1.increment("node_1")

        clock2 = VectorClock.empty()
        clock2 = clock2.increment("node_2")

        existing_value = StateValue(
            value="existing", vector_clock=clock1, node_id="node_1"
        )
        new_value = StateValue(value="new", vector_clock=clock2, node_id="node_2")

        conflict = resolver.detect_conflicts("test_key", existing_value, new_value)

        assert conflict is not None
        assert conflict.state_key == "test_key"
        assert len(conflict.conflicting_values) == 2

        # Test no conflict for causally ordered states
        later_clock = clock1.copy()
        later_clock = later_clock.increment("node_1")
        later_value = StateValue(
            value="later", vector_clock=later_clock, node_id="node_1"
        )

        no_conflict = resolver.detect_conflicts("test_key", existing_value, later_value)
        assert no_conflict is None

    def test_resolver_statistics(self, sample_conflict_resolver):
        """Test conflict resolver statistics tracking."""
        resolver = sample_conflict_resolver

        # Perform some resolutions
        value1 = StateValue(
            value="v1",
            vector_clock=VectorClock.empty(),
            node_id="node_1",
            timestamp=1000.0,
        )
        value2 = StateValue(
            value="v2",
            vector_clock=VectorClock.empty(),
            node_id="node_2",
            timestamp=2000.0,
        )

        conflict = StateConflict(
            conflict_id="stats_test",
            state_key="stats_key",
            conflicting_values=[value1, value2],
            resolution_strategy=ConflictResolutionStrategy.LAST_WRITE_WINS,
        )

        resolver.resolve_conflict(conflict)

        stats = resolver.get_resolution_statistics()

        assert hasattr(stats, "resolution_counts")
        assert hasattr(stats, "recent_resolutions")
        assert hasattr(stats, "avg_resolution_time_ms")
        assert stats.resolution_counts["total_resolutions"] >= 1
        assert stats.resolution_counts["last_write_wins"] >= 1


class TestConsensusProposal:
    """Test suite for consensus proposal implementation."""

    def test_consensus_proposal_creation(self):
        """Test consensus proposal creation."""
        state_value = StateValue(
            value="consensus_value",
            vector_clock=VectorClock.empty(),
            node_id="proposer",
        )

        proposal = ConsensusProposal(
            proposal_id="test_proposal",
            proposer_node_id="proposer",
            state_key="consensus_key",
            proposed_value=state_value,
            required_votes=3,
        )

        assert proposal.proposal_id == "test_proposal"
        assert proposal.proposer_node_id == "proposer"
        assert proposal.state_key == "consensus_key"
        assert proposal.required_votes == 3
        assert proposal.status == ConsensusStatus.PENDING
        assert len(proposal.received_votes) == 0

    def test_consensus_proposal_voting(self):
        """Test consensus proposal voting."""
        state_value = StateValue(
            value="test", vector_clock=VectorClock.empty(), node_id="proposer"
        )

        proposal = ConsensusProposal(
            proposal_id="vote_test",
            proposer_node_id="proposer",
            state_key="vote_key",
            proposed_value=state_value,
            required_votes=2,
        )

        # Add votes
        proposal.add_vote("node_1", True)
        proposal.add_vote("node_2", False)
        proposal.add_vote("node_3", True)

        positive_votes, total_votes = proposal.get_vote_count()
        assert positive_votes == 2
        assert total_votes == 3

        # Should have consensus (2 positive votes >= 2 required)
        assert proposal.has_consensus()
        assert proposal.status == ConsensusStatus.PROPOSED

    def test_consensus_proposal_expiration(self):
        """Test consensus proposal expiration."""
        state_value = StateValue(
            value="test", vector_clock=VectorClock.empty(), node_id="proposer"
        )

        proposal = ConsensusProposal(
            proposal_id="expire_test",
            proposer_node_id="proposer",
            state_key="expire_key",
            proposed_value=state_value,
            consensus_deadline=time.time() - 1,  # Already expired
        )

        assert proposal.is_expired()

    def test_consensus_proposal_summary(self):
        """Test consensus proposal summary generation."""
        state_value = StateValue(
            value="test", vector_clock=VectorClock.empty(), node_id="proposer"
        )

        proposal = ConsensusProposal(
            proposal_id="summary_test",
            proposer_node_id="proposer",
            state_key="summary_key",
            proposed_value=state_value,
            required_votes=2,
        )

        proposal.add_vote("node_1", True)

        summary = proposal.get_proposal_summary()

        assert summary.proposal_id == "summary_test"
        assert summary.proposer == "proposer"
        assert summary.state_key == "summary_key"
        assert summary.votes == "1/1"
        assert summary.required_votes == 2
        assert not summary.has_consensus


class TestConsensusManager:
    """Test suite for consensus manager implementation."""

    def test_consensus_manager_initialization(self, sample_consensus_manager):
        """Test consensus manager initialization."""
        manager = sample_consensus_manager

        assert manager.node_id == "test_consensus_node"
        assert manager.default_consensus_threshold == 0.5
        assert manager.proposal_timeout == 5.0
        assert len(manager.active_proposals) == 0
        assert len(manager.known_nodes) == 0

    @pytest.mark.asyncio
    async def test_consensus_manager_lifecycle(self, sample_consensus_manager):
        """Test consensus manager start and stop."""
        manager = sample_consensus_manager

        # Start manager
        await manager.start()
        assert len(manager._background_tasks) > 0
        assert manager.consensus_stats["manager_started"] > 0

        # Stop manager
        await manager.stop()
        assert len(manager._background_tasks) == 0
        assert manager.consensus_stats["manager_stopped"] > 0

    @pytest.mark.asyncio
    async def test_consensus_proposal_creation(self):
        """Test consensus proposal creation."""
        # Create manager without gossip protocol to avoid message issues
        manager = ConsensusManager(
            node_id="test_consensus_node",
            gossip_protocol=None,  # No gossip protocol
            default_consensus_threshold=0.5,
            proposal_timeout=5.0,
        )

        # Add some known nodes for consensus calculation
        manager.known_nodes.update(["node_1", "node_2", "node_3"])

        state_value = StateValue(
            value="proposal_test",
            vector_clock=VectorClock.empty(),
            node_id=manager.node_id,
        )

        proposal_id = await manager.propose_state_change("test_key", state_value)

        assert proposal_id is not None
        assert proposal_id in manager.active_proposals

        proposal = manager.active_proposals[proposal_id]
        assert proposal.state_key == "test_key"
        assert proposal.proposed_value.value == "proposal_test"
        assert proposal.required_votes >= 1  # At least one vote required
        assert manager.consensus_stats["proposals_created"] == 1

    @pytest.mark.asyncio
    async def test_consensus_voting(self):
        """Test consensus voting process."""
        manager = ConsensusManager(
            node_id="test_consensus_node",
            gossip_protocol=None,  # No gossip protocol
            default_consensus_threshold=0.5,
            proposal_timeout=5.0,
        )

        # Create proposal
        state_value = StateValue(
            value="vote_test", vector_clock=VectorClock.empty(), node_id=manager.node_id
        )
        proposal_id = await manager.propose_state_change(
            "vote_key", state_value, required_consensus=1.0
        )

        # Vote on proposal
        result = await manager.vote_on_proposal(proposal_id, True, "voter_1")
        assert result

        proposal = manager.active_proposals[proposal_id]
        assert "voter_1" in proposal.received_votes
        assert proposal.received_votes["voter_1"]
        assert manager.consensus_stats["votes_received"] == 1

    @pytest.mark.asyncio
    async def test_consensus_achievement(self):
        """Test consensus achievement and state application."""
        manager = ConsensusManager(
            node_id="test_consensus_node",
            gossip_protocol=None,  # No gossip protocol
            default_consensus_threshold=0.5,
            proposal_timeout=5.0,
        )

        # Set single node for easy consensus
        manager.known_nodes = {"test_node"}

        state_value = StateValue(
            value="consensus_test",
            vector_clock=VectorClock.empty(),
            node_id=manager.node_id,
        )
        proposal_id = await manager.propose_state_change("consensus_key", state_value)

        # Vote to achieve consensus
        await manager.vote_on_proposal(proposal_id, True, "test_node")

        proposal = manager.active_proposals[proposal_id]
        assert proposal.has_consensus()
        assert proposal.status == ConsensusStatus.COMMITTED
        assert manager.consensus_stats["proposals_committed"] == 1

    @pytest.mark.asyncio
    async def test_consensus_proposal_evaluation(self, sample_consensus_manager):
        """Test consensus proposal evaluation."""
        manager = sample_consensus_manager

        # Test valid proposal
        valid_proposal_data = {
            "proposal_id": "eval_test",
            "proposer_node_id": "proposer",
            "state_key": "eval_key",
            "proposed_value": "eval_value",
        }

        vote = await manager._evaluate_proposal(valid_proposal_data)
        assert vote

        # Test invalid proposal
        invalid_proposal_data = {
            "proposal_id": "invalid_test"
            # Missing required fields
        }

        vote = await manager._evaluate_proposal(invalid_proposal_data)
        assert not vote

    @pytest.mark.asyncio
    async def test_consensus_proposal_cleanup(self):
        """Test consensus proposal cleanup."""
        manager = ConsensusManager(
            node_id="test_consensus_node",
            gossip_protocol=None,  # No gossip protocol
            default_consensus_threshold=0.5,
            proposal_timeout=5.0,
        )

        # Create expired proposal
        state_value = StateValue(
            value="cleanup_test",
            vector_clock=VectorClock.empty(),
            node_id=manager.node_id,
        )
        proposal_id = await manager.propose_state_change("cleanup_key", state_value)

        # Make proposal expire
        proposal = manager.active_proposals[proposal_id]
        proposal.consensus_deadline = time.time() - 1  # Already expired

        # Trigger cleanup
        await manager._cleanup_expired_proposals()

        # Proposal should be removed from active and moved to history
        assert proposal_id not in manager.active_proposals
        assert len(manager.proposal_history) > 0
        assert manager.consensus_stats["proposals_expired"] == 1

    def test_consensus_proposal_status(self, sample_consensus_manager):
        """Test consensus proposal status retrieval."""
        manager = sample_consensus_manager

        # Test non-existent proposal
        status = manager.get_proposal_status("non_existent")
        assert status is None

        # Create proposal and check status
        state_value = StateValue(
            value="status_test",
            vector_clock=VectorClock.empty(),
            node_id=manager.node_id,
        )
        proposal = ConsensusProposal(
            proposal_id="status_test",
            proposer_node_id=manager.node_id,
            state_key="status_key",
            proposed_value=state_value,
        )

        manager.active_proposals["status_test"] = proposal

        status = manager.get_proposal_status("status_test")
        assert status is not None
        assert status.proposal_id == "status_test"
        assert status.state_key == "status_key"

    def test_consensus_manager_statistics(self, sample_consensus_manager):
        """Test consensus manager statistics collection."""
        manager = sample_consensus_manager

        # Add some test data
        manager.known_nodes.update(["node_1", "node_2"])
        manager.consensus_stats["test_stat"] = 42

        stats = manager.get_consensus_statistics()

        assert hasattr(stats, "consensus_info")
        assert hasattr(stats, "consensus_stats")
        assert hasattr(stats, "active_proposals")
        assert hasattr(stats, "proposal_history_size")

        consensus_info = stats.consensus_info
        assert consensus_info.node_id == "test_consensus_node"
        assert consensus_info.known_nodes == 2
        assert consensus_info.default_threshold == 0.5

        assert stats.consensus_stats["test_stat"] == 42


class TestEndToEndDistributedState:
    """Test suite for end-to-end distributed state scenarios."""

    @pytest.mark.asyncio
    async def test_conflict_resolution_workflow(self):
        """Test complete conflict resolution workflow."""
        resolver = ConflictResolver()

        # Create concurrent state updates
        clock1 = VectorClock.empty()
        clock1 = clock1.increment("node_1")

        clock2 = VectorClock.empty()
        clock2 = clock2.increment("node_2")

        value1 = StateValue(
            value="concurrent_value_1",
            vector_clock=clock1,
            node_id="node_1",
            timestamp=1000.0,
        )

        value2 = StateValue(
            value="concurrent_value_2",
            vector_clock=clock2,
            node_id="node_2",
            timestamp=1500.0,
        )

        # Detect conflict
        conflict = resolver.detect_conflicts("workflow_key", value1, value2)
        assert conflict is not None

        # Resolve conflict
        resolved = resolver.resolve_conflict(conflict)

        # Should use last write wins (value2 has later timestamp)
        assert resolved.value == "concurrent_value_2"
        assert conflict.resolved
        assert resolved == conflict.resolved_value

    @pytest.mark.asyncio
    async def test_consensus_workflow(self):
        """Test complete consensus workflow."""
        # Create multiple consensus managers
        managers = {}
        for i in range(3):
            managers[f"node_{i}"] = ConsensusManager(
                node_id=f"node_{i}",
                default_consensus_threshold=0.67,  # Need 2/3 consensus
                proposal_timeout=1.0,
            )

            # Set up known nodes
            managers[f"node_{i}"].known_nodes = {"node_0", "node_1", "node_2"}

        # Start all managers
        for manager in managers.values():
            await manager.start()

        try:
            # Create state change proposal from node_0
            proposer = managers["node_0"]
            state_value = StateValue(
                value="consensus_workflow_value",
                vector_clock=VectorClock.empty(),
                node_id="node_0",
            )

            proposal_id = await proposer.propose_state_change(
                "workflow_key", state_value
            )

            # Share the proposal with other managers (simulate gossip distribution)
            proposal = proposer.active_proposals[proposal_id]
            for manager_id, manager in managers.items():
                if manager_id != "node_0":
                    manager.active_proposals[proposal_id] = proposal

            # Simulate voting from other nodes
            await managers["node_1"].vote_on_proposal(proposal_id, True, "node_1")
            await managers["node_2"].vote_on_proposal(proposal_id, True, "node_2")

            # Check that consensus was reached
            proposal = proposer.active_proposals[proposal_id]
            assert proposal.has_consensus()
            assert proposal.status == ConsensusStatus.COMMITTED

        finally:
            # Stop all managers
            for manager in managers.values():
                await manager.stop()

    @pytest.mark.asyncio
    async def test_crdt_like_merging(self):
        """Test CRDT-like state merging scenarios."""
        resolver = ConflictResolver()

        # Test G-Set (grow-only set) merging
        set1 = StateValue(
            value={1, 2, 3},
            vector_clock=VectorClock.empty(),
            node_id="node_1",
            state_type=StateType.SET_STATE,
        )

        set2 = StateValue(
            value={3, 4, 5},
            vector_clock=VectorClock.empty(),
            node_id="node_2",
            state_type=StateType.SET_STATE,
        )

        conflict = StateConflict(
            conflict_id="crdt_set_test",
            state_key="crdt_set",
            conflicting_values=[set1, set2],
            resolution_strategy=ConflictResolutionStrategy.MERGE_VALUES,
        )

        resolved = resolver.resolve_conflict(conflict)
        assert resolved.value == {1, 2, 3, 4, 5}

        # Test G-Counter (grow-only counter) merging
        counter1 = StateValue(
            value=10,
            vector_clock=VectorClock.empty(),
            node_id="node_1",
            state_type=StateType.COUNTER_STATE,
        )

        counter2 = StateValue(
            value=15,
            vector_clock=VectorClock.empty(),
            node_id="node_2",
            state_type=StateType.COUNTER_STATE,
        )

        counter_conflict = StateConflict(
            conflict_id="crdt_counter_test",
            state_key="crdt_counter",
            conflicting_values=[counter1, counter2],
            resolution_strategy=ConflictResolutionStrategy.MERGE_VALUES,
        )

        counter_resolved = resolver.resolve_conflict(counter_conflict)
        assert counter_resolved.value == 25  # Sum of counters

    @pytest.mark.asyncio
    async def test_performance_under_load(self):
        """Test performance under high conflict load."""
        resolver = ConflictResolver()

        # Create many concurrent conflicts
        conflicts = []
        for i in range(100):
            clock1 = VectorClock.empty()
            clock1 = clock1.increment(f"node_{i % 5}")

            clock2 = VectorClock.empty()
            clock2 = clock2.increment(f"node_{(i + 1) % 5}")

            value1 = StateValue(
                value=f"value_{i}_1",
                vector_clock=clock1,
                node_id=f"node_{i % 5}",
                timestamp=i * 10,
            )

            value2 = StateValue(
                value=f"value_{i}_2",
                vector_clock=clock2,
                node_id=f"node_{(i + 1) % 5}",
                timestamp=i * 10 + 5,
            )

            conflict = StateConflict(
                conflict_id=f"perf_conflict_{i}",
                state_key=f"perf_key_{i}",
                conflicting_values=[value1, value2],
                resolution_strategy=ConflictResolutionStrategy.LAST_WRITE_WINS,
            )

            conflicts.append(conflict)

        # Resolve all conflicts
        start_time = time.time()

        for conflict in conflicts:
            resolved = resolver.resolve_conflict(conflict)
            assert resolved is not None
            assert conflict.resolved

        resolution_time = time.time() - start_time

        # Should resolve quickly
        assert resolution_time < 1.0  # Should complete within 1 second

        # Check statistics
        stats = resolver.get_resolution_statistics()
        assert stats.resolution_counts["total_resolutions"] == 100
        assert stats.avg_resolution_time_ms < 10  # Should be very fast per resolution


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
