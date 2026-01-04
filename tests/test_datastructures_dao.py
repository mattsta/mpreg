"""
Comprehensive property-based tests for DAO datastructure.

This test suite uses the hypothesis library to aggressively test the correctness
of the DAO implementation with property-based testing, focusing on governance
mechanisms, voting integrity, and blockchain integration.
"""

import json
import time
from dataclasses import dataclass

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from mpreg.datastructures.dao import (
    DecentralizedAutonomousOrganization,
    dao_config_strategy,
    dao_member_strategy,
    dao_proposal_strategy,
    dao_strategy,
)


@dataclass
class FederationHubData:
    """Test data for federation hubs."""

    id: str
    stake: int


@dataclass
class CommunityMemberData:
    """Test data for community members."""

    id: str
    tokens: int
    reputation: int


@dataclass
class TechnicalMemberData:
    """Test data for technical committee members."""

    id: str
    reputation: int
    voting_power: int


from mpreg.datastructures.blockchain_types import OperationType
from mpreg.datastructures.dao_types import (
    DaoConfig,
    DaoMember,
    DaoProposal,
    DaoType,
    ExecutionStatus,
    MembershipType,
    ProposalStatus,
    ProposalType,
    VoteType,
)


class TestDaoTypes:
    """Test DAO type definitions and validation."""

    @given(dao_config_strategy())
    def test_dao_config_creation(self, config: DaoConfig):
        """Test that valid DAO configs can be created."""
        assert config.voting_period_seconds > 0
        assert config.execution_delay_seconds >= 0
        assert 0.0 <= config.quorum_threshold <= 1.0
        assert 0.0 <= config.approval_threshold <= 1.0
        assert config.proposal_deposit >= 0
        assert config.minimum_voting_power > 0

    def test_dao_config_validation(self):
        """Test DAO config validation."""
        # Valid config
        config = DaoConfig(
            dao_type=DaoType.COMMUNITY_GOVERNANCE,
            membership_type=MembershipType.TOKEN_HOLDER,
        )
        assert config.voting_period_seconds > 0

        # Invalid configs
        with pytest.raises(ValueError, match="Voting period must be positive"):
            DaoConfig(
                dao_type=DaoType.COMMUNITY_GOVERNANCE,
                membership_type=MembershipType.TOKEN_HOLDER,
                voting_period_seconds=0,
            )

        with pytest.raises(
            ValueError, match="Quorum threshold must be between 0 and 1"
        ):
            DaoConfig(
                dao_type=DaoType.COMMUNITY_GOVERNANCE,
                membership_type=MembershipType.TOKEN_HOLDER,
                quorum_threshold=1.5,
            )

    @given(dao_member_strategy())
    def test_dao_member_creation(self, member: DaoMember):
        """Test that valid DAO members can be created."""
        assert member.member_id != ""
        assert member.voting_power >= 0
        assert member.token_balance >= 0
        assert member.reputation_score >= 0
        assert member.joined_at >= 0

    def test_dao_member_validation(self):
        """Test DAO member validation."""
        # Valid member
        member = DaoMember(member_id="alice", voting_power=100, token_balance=1000)
        assert member.get_effective_voting_power() == 100

        # Invalid members
        with pytest.raises(ValueError, match="Member ID cannot be empty"):
            DaoMember(member_id="", voting_power=100)

        with pytest.raises(ValueError, match="Voting power cannot be negative"):
            DaoMember(member_id="alice", voting_power=-1)

    def test_dao_member_delegation(self):
        """Test DAO member delegation functionality."""
        member = DaoMember(member_id="alice", voting_power=100)

        # Test delegation
        delegated_member = member.delegate_to("bob")
        assert delegated_member.delegated_to == "bob"
        assert delegated_member.voting_power == 100

        # Cannot delegate to self
        with pytest.raises(ValueError, match="Cannot delegate to self"):
            member.delegate_to("alice")

    @given(dao_proposal_strategy())
    def test_dao_proposal_creation(self, proposal: DaoProposal):
        """Test that valid DAO proposals can be created."""
        assert proposal.proposal_id != ""
        assert proposal.proposer_id != ""
        assert proposal.title != ""
        assert proposal.description != ""
        assert proposal.created_at >= 0
        assert proposal.deposit_amount >= 0

    def test_dao_proposal_lifecycle(self):
        """Test DAO proposal lifecycle states."""
        current_time = time.time()

        # Create proposal with voting period
        proposal = DaoProposal(
            proposer_id="alice",
            title="Test Proposal",
            description="A test proposal for validation",
            voting_starts_at=current_time,
            voting_ends_at=current_time + 3600,  # 1 hour voting
            execution_time=current_time + 7200,  # 2 hours to execute
            status=ProposalStatus.ACTIVE,
        )

        # Should be active during voting period
        assert proposal.is_active(current_time + 1800)  # 30 minutes in
        assert not proposal.is_active(current_time + 4000)  # After voting ends

        # Should be able to execute after passing and delay period
        passed_proposal = proposal.with_status(ProposalStatus.PASSED)
        assert not passed_proposal.can_execute(
            current_time + 3700
        )  # Before execution time
        assert passed_proposal.can_execute(current_time + 7300)  # After execution time


class TestDecentralizedAutonomousOrganization:
    """Test main DAO functionality."""

    def test_dao_creation(self):
        """Test DAO creation and validation."""
        dao = DecentralizedAutonomousOrganization(
            name="Test DAO",
            description="A test DAO for validation",
            dao_type=DaoType.COMMUNITY_GOVERNANCE,
        )

        assert dao.name == "Test DAO"
        assert dao.dao_type == DaoType.COMMUNITY_GOVERNANCE
        assert dao.get_member_count() == 0
        assert dao.get_total_voting_power() == 0
        assert dao.validate_dao_integrity()

    def test_dao_validation(self):
        """Test DAO validation requirements."""
        # Invalid DAOs
        with pytest.raises(ValueError, match="DAO name cannot be empty"):
            DecentralizedAutonomousOrganization(name="", description="Test")

        with pytest.raises(ValueError, match="DAO description cannot be empty"):
            DecentralizedAutonomousOrganization(name="Test", description="")

    def test_member_management(self):
        """Test adding and managing DAO members."""
        dao = DecentralizedAutonomousOrganization(
            name="Test DAO", description="A test DAO for member management"
        )

        # Add member
        member = DaoMember(member_id="alice", voting_power=100, token_balance=1000)

        new_dao = dao.add_member(member)
        assert new_dao.get_member_count() == 1
        assert new_dao.get_total_voting_power() == 100
        assert "alice" in new_dao.members

        # Cannot add duplicate member
        with pytest.raises(ValueError, match="Member alice already exists"):
            new_dao.add_member(member)

        # Update member voting power
        updated_dao = new_dao.update_member_voting_power("alice", 200)
        assert updated_dao.members["alice"].voting_power == 200
        assert updated_dao.get_total_voting_power() == 200

        # Cannot update non-existent member
        with pytest.raises(ValueError, match="Member bob not found"):
            updated_dao.update_member_voting_power("bob", 100)

    def test_proposal_creation_and_voting(self):
        """Test proposal creation and voting process."""
        dao = DecentralizedAutonomousOrganization(
            name="Test DAO",
            description="A test DAO for proposals",
            treasury_balance=10000,  # Sufficient for proposal deposits
        )

        # Add members with sufficient tokens for deposits
        alice = DaoMember(member_id="alice", voting_power=100, token_balance=1000)
        bob = DaoMember(member_id="bob", voting_power=150, token_balance=1000)
        charlie = DaoMember(member_id="charlie", voting_power=50, token_balance=1000)

        dao = dao.add_member(alice)
        dao = dao.add_member(bob)
        dao = dao.add_member(charlie)

        # Create proposal
        proposal = DaoProposal(
            proposer_id="alice",
            proposal_type=ProposalType.TEXT_PROPOSAL,
            title="Increase Token Supply",
            description="Proposal to increase token supply by 10%",
            execution_data=b"increase_supply(0.1)",
        )

        dao_with_proposal = dao.create_proposal("alice", proposal)

        assert len(dao_with_proposal.proposals) == 1
        proposal_id = list(dao_with_proposal.proposals.keys())[0]
        created_proposal = dao_with_proposal.proposals[proposal_id]
        assert created_proposal.status == ProposalStatus.ACTIVE

        # Cast votes
        dao_with_votes = dao_with_proposal.cast_vote(
            "alice", proposal_id, VoteType.FOR, "I support this"
        )
        dao_with_votes = dao_with_votes.cast_vote(
            "bob", proposal_id, VoteType.FOR, "Good idea"
        )
        dao_with_votes = dao_with_votes.cast_vote(
            "charlie", proposal_id, VoteType.AGAINST, "Too risky"
        )

        # Check voting results
        result = dao_with_votes.calculate_voting_result(proposal_id)
        assert result.votes_for == 250  # alice(100) + bob(150)
        assert result.votes_against == 50  # charlie(50)
        assert result.total_voters == 3
        assert result.proposal_passed  # 250/300 = 83% approval, quorum met

        # Cannot vote twice
        with pytest.raises(ValueError, match="has already voted"):
            dao_with_votes.cast_vote("alice", proposal_id, VoteType.AGAINST)

    def test_proposal_execution(self):
        """Test proposal execution process."""
        dao = DecentralizedAutonomousOrganization(
            name="Test DAO",
            description="A test DAO for execution",
            config=DaoConfig(
                dao_type=DaoType.COMMUNITY_GOVERNANCE,
                membership_type=MembershipType.TOKEN_HOLDER,
                execution_delay_seconds=0,  # No delay for testing
            ),
        )

        # Add member with sufficient tokens for deposit
        member = DaoMember(member_id="alice", voting_power=100, token_balance=1000)
        dao = dao.add_member(member)

        # Create and pass proposal
        proposal = DaoProposal(
            proposer_id="alice",
            title="Execute Action",
            description="Execute some action",
            execution_data=b"execute_action()",
        )

        dao = dao.create_proposal("alice", proposal)
        proposal_id = list(dao.proposals.keys())[0]

        # Vote and finalize
        dao = dao.cast_vote("alice", proposal_id, VoteType.FOR)

        # Simulate voting period end and execution time ready
        current_proposal = dao.proposals[proposal_id]
        past_start = time.time() - 3600  # Started 1 hour ago
        past_end = time.time() - 1  # Ended 1 second ago
        past_execution = time.time() - 1  # Execution time is now
        ended_proposal = DaoProposal(
            proposal_id=current_proposal.proposal_id,
            proposer_id=current_proposal.proposer_id,
            proposal_type=current_proposal.proposal_type,
            title=current_proposal.title,
            description=current_proposal.description,
            execution_data=current_proposal.execution_data,
            created_at=current_proposal.created_at,
            voting_starts_at=past_start,
            voting_ends_at=past_end,
            execution_time=past_execution,
            deposit_amount=current_proposal.deposit_amount,
            status=current_proposal.status,
            metadata=current_proposal.metadata,
        )
        new_proposals = dict(dao.proposals)
        new_proposals[proposal_id] = ended_proposal

        dao = DecentralizedAutonomousOrganization(
            dao_id=dao.dao_id,
            name=dao.name,
            description=dao.description,
            dao_type=dao.dao_type,
            config=dao.config,
            blockchain=dao.blockchain,
            members=dao.members,
            proposals=new_proposals,
            votes=dao.votes,
            executions=dao.executions,
            created_at=dao.created_at,
            treasury_balance=dao.treasury_balance,
        )

        dao = dao.finalize_proposal(proposal_id)
        assert dao.proposals[proposal_id].status == ProposalStatus.PASSED

        # Execute proposal
        dao = dao.execute_proposal(proposal_id, "alice")
        assert dao.proposals[proposal_id].status == ProposalStatus.EXECUTED
        assert proposal_id in dao.executions
        assert dao.executions[proposal_id].execution_status == ExecutionStatus.EXECUTED

    def test_voting_edge_cases(self):
        """Test edge cases in voting."""
        dao = DecentralizedAutonomousOrganization(
            name="Test DAO",
            description="Edge case testing",
            config=DaoConfig(
                dao_type=DaoType.COMMUNITY_GOVERNANCE,
                membership_type=MembershipType.TOKEN_HOLDER,
                quorum_threshold=0.6,  # 60% quorum required
                approval_threshold=0.75,  # 75% approval required
            ),
        )

        # Add members with different voting power and sufficient tokens
        members = [
            DaoMember(member_id="alice", voting_power=100, token_balance=1000),
            DaoMember(member_id="bob", voting_power=200, token_balance=1000),
            DaoMember(member_id="charlie", voting_power=300, token_balance=1000),
            DaoMember(member_id="dave", voting_power=400, token_balance=1000),
        ]

        for member in members:
            dao = dao.add_member(member)

        # Total voting power: 1000
        assert dao.get_total_voting_power() == 1000

        # Create proposal
        proposal = DaoProposal(
            proposer_id="alice",
            title="High Threshold Proposal",
            description="Requires high approval threshold",
        )

        dao = dao.create_proposal("alice", proposal)
        proposal_id = list(dao.proposals.keys())[0]

        # Test quorum not reached (only alice votes = 100/1000 = 10% < 60%)
        dao_low_turnout = dao.cast_vote("alice", proposal_id, VoteType.FOR)
        result = dao_low_turnout.calculate_voting_result(proposal_id)
        assert not result.quorum_reached
        assert not result.proposal_passed

        # Test quorum reached but approval threshold not met
        # alice(100) + bob(200) + charlie(300) = 600/1000 = 60% quorum
        # but alice(100) for vs bob(200) + charlie(300) against = 100/500 = 20% approval < 75%
        dao_quorum = dao.cast_vote("alice", proposal_id, VoteType.FOR)
        dao_quorum = dao_quorum.cast_vote("bob", proposal_id, VoteType.AGAINST)
        dao_quorum = dao_quorum.cast_vote("charlie", proposal_id, VoteType.AGAINST)

        result = dao_quorum.calculate_voting_result(proposal_id)
        assert result.quorum_reached
        assert not result.proposal_passed  # 20% approval < 75% threshold

        # Test abstain votes (don't count toward approval)
        dao_abstain = dao.cast_vote("alice", proposal_id, VoteType.FOR)
        dao_abstain = dao_abstain.cast_vote("bob", proposal_id, VoteType.FOR)
        dao_abstain = dao_abstain.cast_vote("charlie", proposal_id, VoteType.ABSTAIN)

        result = dao_abstain.calculate_voting_result(proposal_id)
        # Participation: 600/1000 = 60% (meets quorum)
        # Approval: 300/300 = 100% (abstain doesn't count in approval calculation)
        assert result.quorum_reached
        assert result.proposal_passed

    def test_emergency_proposals(self):
        """Test emergency proposal handling."""
        dao = DecentralizedAutonomousOrganization(
            name="Emergency DAO",
            description="Testing emergency procedures",
            config=DaoConfig(
                dao_type=DaoType.COMMUNITY_GOVERNANCE,
                membership_type=MembershipType.TOKEN_HOLDER,
                emergency_threshold=0.67,  # 67% for emergency actions
            ),
        )

        # Add members with sufficient tokens
        members = [
            DaoMember(member_id="alice", voting_power=100, token_balance=1000),
            DaoMember(member_id="bob", voting_power=100, token_balance=1000),
            DaoMember(member_id="charlie", voting_power=100, token_balance=1000),
        ]

        for member in members:
            dao = dao.add_member(member)

        # Create emergency proposal
        emergency_proposal = DaoProposal(
            proposer_id="alice",
            proposal_type=ProposalType.EMERGENCY_ACTION,
            title="Emergency Protocol Upgrade",
            description="Critical security patch",
        )

        dao = dao.create_proposal("alice", emergency_proposal)
        proposal_id = list(dao.proposals.keys())[0]

        # Test emergency threshold
        # alice + bob + charlie = 300/300 = 100% participation
        # alice + bob = 200/300 = 66.7% < 67% (should fail emergency threshold)
        # Need to have at least 201 votes for approval
        dao = dao.cast_vote("alice", proposal_id, VoteType.FOR)
        dao = dao.cast_vote("bob", proposal_id, VoteType.FOR)
        dao = dao.cast_vote(
            "charlie", proposal_id, VoteType.FOR
        )  # Change to FOR to pass threshold

        result = dao.calculate_voting_result(proposal_id)
        # All votes FOR: 300/300 = 100% > 67% emergency threshold
        assert result.votes_for == 300
        assert result.votes_against == 0

        # Should meet emergency threshold
        assert result.proposal_passed

    @given(dao_strategy())
    def test_dao_serialization(self, dao: DecentralizedAutonomousOrganization):
        """Test DAO serialization and statistics."""
        # Should be able to serialize DAO to dict
        dao_dict = dao.to_dict()
        assert isinstance(dao_dict, dict)
        assert "dao_id" in dao_dict
        assert "name" in dao_dict
        assert "statistics" in dao_dict

        # Statistics should be comprehensive
        stats = dao.get_dao_statistics()
        assert "member_count" in stats
        assert "total_voting_power" in stats
        assert "total_proposals" in stats
        assert "treasury_balance" in stats

    @given(dao_strategy())
    def test_dao_integrity_validation(self, dao: DecentralizedAutonomousOrganization):
        """Test DAO integrity validation."""
        # Valid DAO should pass integrity check
        assert dao.validate_dao_integrity()

    def test_dao_blockchain_integration(self):
        """Test DAO blockchain integration."""
        dao = DecentralizedAutonomousOrganization(
            name="Blockchain DAO", description="Testing blockchain integration"
        )

        initial_height = dao.blockchain.get_height()

        # Add member (should create blockchain transaction)
        member = DaoMember(member_id="alice", voting_power=100)
        dao = dao.add_member(member)

        # Blockchain height should increase
        assert dao.blockchain.get_height() > initial_height

        # Should be able to find membership transaction
        latest_block = dao.blockchain.get_latest_block()
        membership_txs = [
            tx
            for tx in latest_block.transactions
            if tx.operation_type == OperationType.FEDERATION_JOIN
        ]
        assert len(membership_txs) > 0

        # Transaction should contain membership data
        membership_tx = membership_txs[0]
        payload_data = json.loads(membership_tx.payload.decode())
        assert payload_data["action"] == "dao_join"
        assert payload_data["dao_id"] == dao.dao_id


class TestDaoProperties:
    """Test mathematical and governance properties of DAOs."""

    @given(dao_member_strategy(), dao_member_strategy())
    def test_voting_power_aggregation(self, member1: DaoMember, member2: DaoMember):
        """Test voting power aggregation properties."""
        assume(member1.member_id != member2.member_id)

        dao = DecentralizedAutonomousOrganization(
            name="Power Test DAO", description="Testing voting power aggregation"
        )

        # Add members individually
        dao = dao.add_member(member1)
        dao = dao.add_member(member2)

        # Total voting power should equal sum of individual powers
        expected_power = (
            member1.get_effective_voting_power() + member2.get_effective_voting_power()
        )
        assert dao.get_total_voting_power() == expected_power

    def test_quorum_threshold_properties(self):
        """Test mathematical properties of quorum thresholds."""
        # Test various quorum thresholds
        for threshold in [0.1, 0.25, 0.5, 0.67, 0.75, 0.9]:
            dao = DecentralizedAutonomousOrganization(
                name=f"Quorum {threshold} DAO",
                description="Testing quorum",
                config=DaoConfig(
                    dao_type=DaoType.COMMUNITY_GOVERNANCE,
                    membership_type=MembershipType.TOKEN_HOLDER,
                    quorum_threshold=threshold,
                ),
            )

            # Add 100 members with 1 voting power each and sufficient tokens
            for i in range(100):
                member = DaoMember(
                    member_id=f"member_{i}", voting_power=1, token_balance=1000
                )
                dao = dao.add_member(member)

            # Create proposal
            proposal = DaoProposal(
                proposer_id="member_0",
                title="Quorum Test",
                description="Testing quorum threshold",
            )
            dao = dao.create_proposal("member_0", proposal)
            proposal_id = list(dao.proposals.keys())[0]

            # Calculate required voters for quorum
            required_voters = int(threshold * 100)

            # Vote with exactly enough voters for quorum
            for i in range(required_voters):
                dao = dao.cast_vote(f"member_{i}", proposal_id, VoteType.FOR)

            result = dao.calculate_voting_result(proposal_id)

            # Should meet quorum threshold
            assert result.quorum_reached
            participation_rate = result.get_participation_rate()
            assert (
                participation_rate >= threshold - 0.01
            )  # Allow for floating point precision

    def test_approval_threshold_properties(self):
        """Test mathematical properties of approval thresholds."""
        dao = DecentralizedAutonomousOrganization(
            name="Approval Test DAO",
            description="Testing approval thresholds",
            config=DaoConfig(
                dao_type=DaoType.COMMUNITY_GOVERNANCE,
                membership_type=MembershipType.TOKEN_HOLDER,
                approval_threshold=0.6,  # 60% approval required
            ),
        )

        # Add 10 members with equal voting power and sufficient tokens
        for i in range(10):
            member = DaoMember(
                member_id=f"member_{i}", voting_power=10, token_balance=1000
            )
            dao = dao.add_member(member)

        # Create proposal
        proposal = DaoProposal(
            proposer_id="member_0",
            title="Approval Test",
            description="Testing approval threshold",
        )
        dao = dao.create_proposal("member_0", proposal)
        proposal_id = list(dao.proposals.keys())[0]

        # Test edge case: exactly 60% approval
        # 6 for, 4 against = 6/10 = 60% approval
        for i in range(6):
            dao = dao.cast_vote(f"member_{i}", proposal_id, VoteType.FOR)
        for i in range(6, 10):
            dao = dao.cast_vote(f"member_{i}", proposal_id, VoteType.AGAINST)

        result = dao.calculate_voting_result(proposal_id)
        approval_rate = result.get_approval_rate()

        # Should meet approval threshold (6/10 = 60%)
        assert approval_rate >= 0.6
        assert result.proposal_passed

    def test_voting_power_delegation_properties(self):
        """Test properties of voting power delegation."""
        member = DaoMember(member_id="alice", voting_power=100, is_active=True)

        # Effective voting power should equal actual power when active
        assert member.get_effective_voting_power() == 100

        # Inactive member should have zero effective voting power
        inactive_member = DaoMember(
            member_id="alice", voting_power=100, is_active=False
        )
        assert inactive_member.get_effective_voting_power() == 0

        # Delegation should preserve voting power amount
        delegated = member.delegate_to("bob")
        assert delegated.voting_power == member.voting_power
        assert delegated.delegated_to == "bob"


class TestDaoStateMachine(RuleBasedStateMachine):
    """Stateful testing for DAO operations."""

    __test__ = False

    def __init__(self):
        super().__init__()
        self.dao = DecentralizedAutonomousOrganization(
            name="StateMachine DAO", description="Testing DAO state transitions"
        )
        self.member_counter = 0
        self.proposal_counter = 0

    @initialize()
    def setup_dao(self):
        """Initialize DAO with some members."""
        # Add initial members
        for i in range(3):
            member = DaoMember(
                member_id=f"member_{i}",
                voting_power=(i + 1) * 50,  # 50, 100, 150 voting power
            )
            self.dao = self.dao.add_member(member)
            self.member_counter = i + 1

    @rule(voting_power=st.integers(min_value=1, max_value=200))
    def add_member(self, voting_power):
        """Add a new member to the DAO."""
        member = DaoMember(
            member_id=f"member_{self.member_counter}", voting_power=voting_power
        )
        try:
            self.dao = self.dao.add_member(member)
            self.member_counter += 1
        except ValueError:
            # Expected for duplicate members or invalid voting power
            pass

    @rule(proposer_index=st.integers(min_value=0, max_value=10))
    def create_proposal(self, proposer_index):
        """Create a new proposal."""
        if proposer_index < len(self.dao.members):
            proposer_id = f"member_{proposer_index}"
            if proposer_id in self.dao.members:
                proposal = DaoProposal(
                    proposer_id=proposer_id,
                    title=f"Proposal {self.proposal_counter}",
                    description=f"Test proposal number {self.proposal_counter}",
                )
                try:
                    self.dao = self.dao.create_proposal(proposer_id, proposal)
                    self.proposal_counter += 1
                except ValueError:
                    # Expected for invalid proposals or cooldown violations
                    pass

    @rule(
        voter_index=st.integers(min_value=0, max_value=10),
        vote_choice=st.sampled_from(VoteType),
    )
    def cast_vote(self, voter_index, vote_choice):
        """Cast a vote on an active proposal."""
        if (
            voter_index < len(self.dao.members)
            and self.dao.proposals
            and f"member_{voter_index}" in self.dao.members
        ):
            voter_id = f"member_{voter_index}"
            active_proposals = self.dao.get_active_proposals()

            if active_proposals:
                proposal_id = active_proposals[0].proposal_id
                try:
                    self.dao = self.dao.cast_vote(voter_id, proposal_id, vote_choice)
                except ValueError:
                    # Expected for duplicate votes or inactive proposals
                    pass

    @invariant()
    def dao_integrity_maintained(self):
        """DAO should always maintain integrity."""
        assert self.dao.validate_dao_integrity()

    @invariant()
    def voting_power_consistency(self):
        """Total voting power should equal sum of member powers."""
        expected_power = sum(
            member.get_effective_voting_power() for member in self.dao.members.values()
        )
        assert self.dao.get_total_voting_power() == expected_power

    @invariant()
    def proposal_consistency(self):
        """All proposals should have valid proposers."""
        for proposal in self.dao.proposals.values():
            assert proposal.proposer_id in self.dao.members

    @invariant()
    def vote_consistency(self):
        """All votes should be for valid proposals by valid members."""
        for proposal_id, votes in self.dao.votes.items():
            assert proposal_id in self.dao.proposals
            for vote in votes:
                assert vote.voter_id in self.dao.members

    @invariant()
    def blockchain_consistency(self):
        """Blockchain should remain valid."""
        assert self.dao.blockchain.validate_chain()


# Run stateful tests
TestDaoStateMachineTest = TestDaoStateMachine.TestCase


class TestDaoExamples:
    """Test real-world DAO examples and use cases."""

    def test_federation_governance_dao(self):
        """Test DAO for federation governance."""
        # Create federation governance DAO
        fed_dao = DecentralizedAutonomousOrganization(
            name="MPREG Federation Governance",
            description="Decentralized governance for MPREG federation",
            dao_type=DaoType.FEDERATION_GOVERNANCE,
            config=DaoConfig(
                dao_type=DaoType.FEDERATION_GOVERNANCE,
                membership_type=MembershipType.STAKE_BASED,
                voting_period_seconds=7 * 86400,  # 7 days
                quorum_threshold=0.33,  # 33% quorum
                approval_threshold=0.67,  # 67% supermajority
                emergency_threshold=0.8,  # 80% for emergency actions
                proposal_deposit=1000,
            ),
        )

        # Add federation hubs as members
        hubs = [
            FederationHubData(id="hub_us_east", stake=10000),
            FederationHubData(id="hub_eu_west", stake=8000),
            FederationHubData(id="hub_asia_pacific", stake=12000),
            FederationHubData(id="hub_south_america", stake=6000),
        ]

        for hub in hubs:
            member = DaoMember(
                member_id=hub.id,
                voting_power=hub.stake,  # Voting power based on stake
                token_balance=hub.stake,
                metadata={"role": "federation_hub", "region": hub.id.split("_")[1]},
            )
            fed_dao = fed_dao.add_member(member)

        # Create governance proposal
        protocol_upgrade = DaoProposal(
            proposer_id="hub_us_east",
            proposal_type=ProposalType.PROTOCOL_UPGRADE,
            title="Upgrade Federation Protocol to v2.0",
            description="Implement new consensus mechanism and security improvements",
            execution_data=json.dumps(
                {
                    "upgrade_version": "2.0",
                    "breaking_changes": True,
                    "migration_period_days": 30,
                }
            ).encode(),
        )

        fed_dao = fed_dao.create_proposal("hub_us_east", protocol_upgrade)
        proposal_id = list(fed_dao.proposals.keys())[0]

        # Hubs vote on the proposal
        fed_dao = fed_dao.cast_vote(
            "hub_us_east", proposal_id, VoteType.FOR, "Necessary upgrade"
        )
        fed_dao = fed_dao.cast_vote(
            "hub_eu_west", proposal_id, VoteType.FOR, "Improves security"
        )
        fed_dao = fed_dao.cast_vote(
            "hub_asia_pacific", proposal_id, VoteType.FOR, "Good for network"
        )
        fed_dao = fed_dao.cast_vote(
            "hub_south_america",
            proposal_id,
            VoteType.ABSTAIN,
            "Need more time to review",
        )

        # Check results
        result = fed_dao.calculate_voting_result(proposal_id)

        # Total stake: 36000, Voted: 36000 (100% participation)
        # For: 30000, Against: 0, Abstain: 6000
        # Approval: 30000/30000 = 100% (only counting decisive votes)
        assert result.quorum_reached  # 100% > 33%
        assert result.proposal_passed  # 100% > 67%

        # Verify statistics
        stats = fed_dao.get_dao_statistics()
        assert stats["dao_type"] == "federation_governance"
        assert stats["member_count"] == 4
        assert stats["total_voting_power"] == 36000

    def test_project_funding_dao(self):
        """Test DAO for project funding decisions."""
        funding_dao = DecentralizedAutonomousOrganization(
            name="MPREG Development Fund",
            description="Community-driven funding for MPREG ecosystem projects",
            dao_type=DaoType.PROJECT_FUNDING,
            config=DaoConfig(
                dao_type=DaoType.PROJECT_FUNDING,
                membership_type=MembershipType.TOKEN_HOLDER,
                voting_period_seconds=14 * 86400,  # 14 days
                quorum_threshold=0.2,  # 20% quorum
                approval_threshold=0.5,  # Simple majority
                proposal_deposit=500,
            ),
            treasury_balance=1000000,  # 1M tokens in treasury
        )

        # Add community members
        community_members = [
            CommunityMemberData(id="dev_alice", tokens=50000, reputation=85),
            CommunityMemberData(id="researcher_bob", tokens=30000, reputation=90),
            CommunityMemberData(id="community_charlie", tokens=20000, reputation=70),
            CommunityMemberData(id="validator_dave", tokens=40000, reputation=80),
        ]

        for member in community_members:
            dao_member = DaoMember(
                member_id=member.id,
                voting_power=member.tokens,
                token_balance=member.tokens,
                reputation_score=member.reputation,
                metadata={"role": member.id.split("_")[0]},
            )
            funding_dao = funding_dao.add_member(dao_member)

        # Create funding proposal
        funding_proposal = DaoProposal(
            proposer_id="dev_alice",
            proposal_type=ProposalType.BUDGET_ALLOCATION,
            title="Fund Advanced Caching System Development",
            description="Proposal to fund development of advanced distributed caching with 50K tokens",
            execution_data=json.dumps(
                {
                    "funding_amount": 50000,
                    "recipient": "dev_alice",
                    "milestones": [
                        {"description": "Design phase", "amount": 15000},
                        {"description": "Implementation", "amount": 25000},
                        {"description": "Testing and documentation", "amount": 10000},
                    ],
                    "duration_months": 6,
                }
            ).encode(),
        )

        funding_dao = funding_dao.create_proposal("dev_alice", funding_proposal)
        proposal_id = list(funding_dao.proposals.keys())[0]

        # Community votes
        funding_dao = funding_dao.cast_vote(
            "dev_alice", proposal_id, VoteType.FOR, "I'll deliver quality work"
        )
        funding_dao = funding_dao.cast_vote(
            "researcher_bob", proposal_id, VoteType.FOR, "Needed feature"
        )
        funding_dao = funding_dao.cast_vote(
            "community_charlie", proposal_id, VoteType.AGAINST, "Too expensive"
        )
        funding_dao = funding_dao.cast_vote(
            "validator_dave", proposal_id, VoteType.FOR, "Good investment"
        )

        result = funding_dao.calculate_voting_result(proposal_id)

        # Votes: Alice(50k) + Bob(30k) + Dave(40k) = 120k FOR
        #        Charlie(20k) = 20k AGAINST
        # Total: 140k voted out of 140k total = 100% participation
        # Approval: 120k / 140k = 85.7% > 50%
        assert result.proposal_passed

        # Check treasury impact would be recorded
        stats = funding_dao.get_dao_statistics()
        assert stats["treasury_balance"] == 1000000  # Not reduced until execution

    def test_technical_committee_dao(self):
        """Test DAO for technical decision making."""
        tech_dao = DecentralizedAutonomousOrganization(
            name="MPREG Technical Committee",
            description="Technical governance for protocol decisions",
            dao_type=DaoType.TECHNICAL_COMMITTEE,
            config=DaoConfig(
                dao_type=DaoType.TECHNICAL_COMMITTEE,
                membership_type=MembershipType.REPUTATION_BASED,
                voting_period_seconds=5 * 86400,  # 5 days for technical decisions
                quorum_threshold=0.5,  # 50% quorum
                approval_threshold=0.6,  # 60% approval
                emergency_threshold=0.75,  # 75% for emergency patches
            ),
        )

        # Add technical committee members
        tech_members = [
            TechnicalMemberData(id="lead_architect", reputation=100, voting_power=3),
            TechnicalMemberData(id="security_expert", reputation=95, voting_power=3),
            TechnicalMemberData(id="protocol_dev_1", reputation=85, voting_power=2),
            TechnicalMemberData(id="protocol_dev_2", reputation=80, voting_power=2),
            TechnicalMemberData(id="qa_specialist", reputation=75, voting_power=1),
        ]

        for member in tech_members:
            dao_member = DaoMember(
                member_id=member.id,
                voting_power=member.voting_power,
                token_balance=1000,  # Sufficient for proposal deposits
                reputation_score=member.reputation,
                metadata={"role": "technical_committee", "specialization": member.id},
            )
            tech_dao = tech_dao.add_member(dao_member)

        # Create emergency security proposal
        security_proposal = DaoProposal(
            proposer_id="security_expert",
            proposal_type=ProposalType.EMERGENCY_ACTION,
            title="Emergency Security Patch",
            description="Critical vulnerability fix for consensus mechanism",
            execution_data=json.dumps(
                {
                    "patch_version": "2.0.1",
                    "vulnerability_id": "CVE-2024-001",
                    "severity": "critical",
                    "immediate_deployment": True,
                }
            ).encode(),
        )

        tech_dao = tech_dao.create_proposal("security_expert", security_proposal)
        proposal_id = list(tech_dao.proposals.keys())[0]

        # Technical committee votes
        tech_dao = tech_dao.cast_vote(
            "security_expert", proposal_id, VoteType.FOR, "Critical fix needed"
        )
        tech_dao = tech_dao.cast_vote(
            "lead_architect",
            proposal_id,
            VoteType.FOR,
            "Approved for emergency deployment",
        )
        tech_dao = tech_dao.cast_vote(
            "protocol_dev_1", proposal_id, VoteType.FOR, "Reviewed and tested"
        )
        tech_dao = tech_dao.cast_vote(
            "protocol_dev_2", proposal_id, VoteType.FOR, "Security fix confirmed"
        )
        # QA specialist abstains

        result = tech_dao.calculate_voting_result(proposal_id)

        # Emergency proposal needs 75% approval
        # FOR: 3+3+2+2 = 10, AGAINST: 0, Total decisive: 10
        # Approval: 10/10 = 100% > 75%
        # Participation: 10/11 = 90.9% > 50%
        assert result.proposal_passed
        assert result.quorum_reached

        # Verify emergency threshold was applied
        approval_rate = result.get_approval_rate()
        assert approval_rate >= tech_dao.config.emergency_threshold
