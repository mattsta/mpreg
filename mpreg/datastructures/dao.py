"""
Decentralized Autonomous Organization (DAO) implementation.

This module provides a complete DAO abstraction built on top of our blockchain
infrastructure, enabling democratic governance, proposal management, and
automated execution of organizational decisions.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from hypothesis import strategies as st

from .block import Block
from .blockchain import Blockchain
from .blockchain_types import (
    ConsensusConfig,
    ConsensusType,
    OperationType,
)
from .dao_types import (
    DaoConfig,
    DaoExecution,
    DaoId,
    DaoMember,
    DaoProposal,
    DaoType,
    DaoVote,
    DaoVotingResult,
    ExecutionStatus,
    MemberId,
    MembershipType,
    ProposalId,
    ProposalStatus,
    ProposalType,
    Timestamp,
    TokenAmount,
    VoteType,
    VotingPower,
    current_timestamp,
    generate_dao_id,
)
from .transaction import Transaction


@dataclass(frozen=True, slots=True)
class DecentralizedAutonomousOrganization:
    """
    Complete DAO implementation with blockchain-backed governance.

    Provides democratic decision-making, proposal management, voting mechanisms,
    and automated execution of organizational decisions using blockchain consensus.
    """

    dao_id: DaoId = field(default_factory=generate_dao_id)
    name: str = ""
    description: str = ""
    dao_type: DaoType = DaoType.COMMUNITY_GOVERNANCE
    config: DaoConfig = field(
        default_factory=lambda: DaoConfig(
            dao_type=DaoType.COMMUNITY_GOVERNANCE,
            membership_type=MembershipType.TOKEN_HOLDER,
        )
    )
    blockchain: Blockchain = field(
        default_factory=lambda: Blockchain.create_new_chain(
            chain_id="dao_governance",
            genesis_miner="dao_genesis",
            consensus_config=ConsensusConfig(
                consensus_type=ConsensusType.PROOF_OF_AUTHORITY,
                block_time_target=30,  # 30-second blocks for governance
                difficulty_adjustment_interval=100,
                max_transactions_per_block=1000,
            ),
        )
    )
    members: dict[MemberId, DaoMember] = field(default_factory=dict)
    proposals: dict[ProposalId, DaoProposal] = field(default_factory=dict)
    votes: dict[ProposalId, list[DaoVote]] = field(default_factory=dict)
    executions: dict[ProposalId, DaoExecution] = field(default_factory=dict)
    created_at: Timestamp = field(default_factory=current_timestamp)
    treasury_balance: TokenAmount = 0

    def __post_init__(self) -> None:
        """Validate DAO structure."""
        if not self.dao_id:
            raise ValueError("DAO ID cannot be empty")
        if not self.name:
            raise ValueError("DAO name cannot be empty")
        if not self.description:
            raise ValueError("DAO description cannot be empty")
        if self.created_at < 0:
            raise ValueError("Created timestamp cannot be negative")
        if self.treasury_balance < 0:
            raise ValueError("Treasury balance cannot be negative")

    # Member Management

    def add_member(self, member: DaoMember) -> DecentralizedAutonomousOrganization:
        """Add new member to DAO with blockchain record."""
        if member.member_id in self.members:
            raise ValueError(f"Member {member.member_id} already exists")

        if member.voting_power < self.config.minimum_voting_power:
            raise ValueError(
                f"Voting power below minimum: {self.config.minimum_voting_power}"
            )

        # Create membership transaction
        membership_tx = Transaction(
            sender=member.member_id,
            receiver=self.dao_id,
            operation_type=OperationType.FEDERATION_JOIN,
            payload=json.dumps(
                {
                    "action": "dao_join",
                    "dao_id": self.dao_id,
                    "member_data": {
                        "voting_power": member.voting_power,
                        "token_balance": member.token_balance,
                        "reputation_score": member.reputation_score,
                        "metadata": member.metadata,
                    },
                }
            ).encode(),
            fee=0,  # No fee for membership
        )

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.blockchain.get_latest_block(),
            transactions=(membership_tx,),
            miner="dao_manager",
        )

        new_blockchain = self.blockchain.add_block(new_block)
        new_members = dict(self.members)
        new_members[member.member_id] = member

        return DecentralizedAutonomousOrganization(
            dao_id=self.dao_id,
            name=self.name,
            description=self.description,
            dao_type=self.dao_type,
            config=self.config,
            blockchain=new_blockchain,
            members=new_members,
            proposals=self.proposals,
            votes=self.votes,
            executions=self.executions,
            created_at=self.created_at,
            treasury_balance=self.treasury_balance,
        )

    def update_member_voting_power(
        self, member_id: MemberId, new_voting_power: VotingPower
    ) -> DecentralizedAutonomousOrganization:
        """Update member's voting power with blockchain record."""
        if member_id not in self.members:
            raise ValueError(f"Member {member_id} not found")

        member = self.members[member_id]
        updated_member = member.update_voting_power(new_voting_power)

        # Create update transaction
        update_tx = Transaction(
            sender=member_id,
            receiver=self.dao_id,
            operation_type=OperationType.NODE_UPDATE,
            payload=json.dumps(
                {
                    "action": "update_voting_power",
                    "dao_id": self.dao_id,
                    "old_power": member.voting_power,
                    "new_power": new_voting_power,
                }
            ).encode(),
            fee=1,
        )

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.blockchain.get_latest_block(),
            transactions=(update_tx,),
            miner="dao_manager",
        )

        new_blockchain = self.blockchain.add_block(new_block)
        new_members = dict(self.members)
        new_members[member_id] = updated_member

        return DecentralizedAutonomousOrganization(
            dao_id=self.dao_id,
            name=self.name,
            description=self.description,
            dao_type=self.dao_type,
            config=self.config,
            blockchain=new_blockchain,
            members=new_members,
            proposals=self.proposals,
            votes=self.votes,
            executions=self.executions,
            created_at=self.created_at,
            treasury_balance=self.treasury_balance,
        )

    def get_total_voting_power(self) -> VotingPower:
        """Calculate total voting power in DAO."""
        return sum(
            member.get_effective_voting_power() for member in self.members.values()
        )

    def get_member_count(self) -> int:
        """Get number of active members."""
        return len([m for m in self.members.values() if m.is_active])

    # Proposal Management

    def create_proposal(
        self, proposer_id: MemberId, proposal: DaoProposal
    ) -> DecentralizedAutonomousOrganization:
        """Create new proposal with blockchain record."""
        if proposer_id not in self.members:
            raise ValueError(f"Proposer {proposer_id} is not a member")

        proposer = self.members[proposer_id]
        if proposer.voting_power < self.config.minimum_voting_power:
            raise ValueError("Insufficient voting power to create proposal")

        # Check proposer has enough tokens for deposit
        if proposer.token_balance < self.config.proposal_deposit:
            raise ValueError("Proposer has insufficient tokens for proposal deposit")

        # Check proposal cooldown
        current_time = time.time()
        recent_proposals = [
            p
            for p in self.proposals.values()
            if (
                p.proposer_id == proposer_id
                and current_time - p.created_at < self.config.proposal_cooldown_seconds
            )
        ]
        if len(recent_proposals) >= self.config.max_proposals_per_member:
            raise ValueError("Proposal cooldown period not satisfied")

        # Set voting period
        voting_starts = current_time
        voting_ends = voting_starts + self.config.voting_period_seconds
        execution_time = voting_ends + self.config.execution_delay_seconds

        final_proposal = proposal.with_voting_period(voting_starts, voting_ends)
        final_proposal = DaoProposal(
            proposal_id=final_proposal.proposal_id,
            proposer_id=proposer_id,
            proposal_type=final_proposal.proposal_type,
            title=final_proposal.title,
            description=final_proposal.description,
            execution_data=final_proposal.execution_data,
            created_at=current_time,
            voting_starts_at=voting_starts,
            voting_ends_at=voting_ends,
            execution_time=execution_time,
            deposit_amount=self.config.proposal_deposit,
            status=ProposalStatus.ACTIVE,
            metadata=final_proposal.metadata,
        )

        # Create proposal transaction
        proposal_tx = Transaction(
            sender=proposer_id,
            receiver=self.dao_id,
            operation_type=OperationType.CONSENSUS_VOTE,
            payload=json.dumps(
                {
                    "action": "create_proposal",
                    "dao_id": self.dao_id,
                    "proposal_data": {
                        "proposal_id": final_proposal.proposal_id,
                        "proposal_type": final_proposal.proposal_type.value,
                        "title": final_proposal.title,
                        "description": final_proposal.description,
                        "execution_data": final_proposal.execution_data.hex(),
                        "voting_ends_at": final_proposal.voting_ends_at,
                        "execution_time": final_proposal.execution_time,
                    },
                }
            ).encode(),
            fee=self.config.proposal_deposit,
        )

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.blockchain.get_latest_block(),
            transactions=(proposal_tx,),
            miner="dao_manager",
        )

        new_blockchain = self.blockchain.add_block(new_block)
        new_proposals = dict(self.proposals)
        new_proposals[final_proposal.proposal_id] = final_proposal
        new_votes = dict(self.votes)
        new_votes[final_proposal.proposal_id] = []

        # Deduct deposit from proposer's token balance
        updated_proposer = DaoMember(
            member_id=proposer.member_id,
            voting_power=proposer.voting_power,
            token_balance=proposer.token_balance - self.config.proposal_deposit,
            reputation_score=proposer.reputation_score,
            joined_at=proposer.joined_at,
            delegated_to=proposer.delegated_to,
            is_active=proposer.is_active,
            metadata=proposer.metadata,
        )
        new_members = dict(self.members)
        new_members[proposer_id] = updated_proposer

        return DecentralizedAutonomousOrganization(
            dao_id=self.dao_id,
            name=self.name,
            description=self.description,
            dao_type=self.dao_type,
            config=self.config,
            blockchain=new_blockchain,
            members=new_members,
            proposals=new_proposals,
            votes=new_votes,
            executions=self.executions,
            created_at=self.created_at,
            treasury_balance=self.treasury_balance,  # Treasury unchanged
        )

    def cast_vote(
        self,
        voter_id: MemberId,
        proposal_id: ProposalId,
        vote_choice: VoteType,
        reason: str = "",
    ) -> DecentralizedAutonomousOrganization:
        """Cast vote on proposal with blockchain record."""
        if voter_id not in self.members:
            raise ValueError(f"Voter {voter_id} is not a member")

        if proposal_id not in self.proposals:
            raise ValueError(f"Proposal {proposal_id} not found")

        proposal = self.proposals[proposal_id]
        voter = self.members[voter_id]

        # Check if proposal is active
        current_time = time.time()
        if not proposal.is_active(current_time):
            raise ValueError("Proposal is not in active voting period")

        # Check if already voted
        existing_votes = self.votes.get(proposal_id, [])
        if any(vote.voter_id == voter_id for vote in existing_votes):
            raise ValueError(f"Voter {voter_id} has already voted on this proposal")

        # Create vote
        vote = DaoVote(
            voter_id=voter_id,
            proposal_id=proposal_id,
            vote_choice=vote_choice,
            voting_power_used=voter.get_effective_voting_power(),
            cast_at=current_time,
            reason=reason,
        )

        # Create vote transaction
        vote_tx = Transaction(
            sender=voter_id,
            receiver=self.dao_id,
            operation_type=OperationType.CONSENSUS_VOTE,
            payload=json.dumps(
                {
                    "action": "cast_vote",
                    "dao_id": self.dao_id,
                    "proposal_id": proposal_id,
                    "vote_choice": vote_choice.value,
                    "voting_power": vote.voting_power_used,
                    "reason": reason,
                }
            ).encode(),
            fee=1,
        )

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.blockchain.get_latest_block(),
            transactions=(vote_tx,),
            miner="dao_manager",
        )

        new_blockchain = self.blockchain.add_block(new_block)
        new_votes = dict(self.votes)
        new_votes[proposal_id] = existing_votes + [vote]

        return DecentralizedAutonomousOrganization(
            dao_id=self.dao_id,
            name=self.name,
            description=self.description,
            dao_type=self.dao_type,
            config=self.config,
            blockchain=new_blockchain,
            members=self.members,
            proposals=self.proposals,
            votes=new_votes,
            executions=self.executions,
            created_at=self.created_at,
            treasury_balance=self.treasury_balance,
        )

    def calculate_voting_result(self, proposal_id: ProposalId) -> DaoVotingResult:
        """Calculate voting results for a proposal."""
        if proposal_id not in self.proposals:
            raise ValueError(f"Proposal {proposal_id} not found")

        proposal_votes = self.votes.get(proposal_id, [])
        total_voting_power = self.get_total_voting_power()

        votes_for = sum(
            vote.voting_power_used
            for vote in proposal_votes
            if vote.vote_choice == VoteType.FOR
        )
        votes_against = sum(
            vote.voting_power_used
            for vote in proposal_votes
            if vote.vote_choice == VoteType.AGAINST
        )
        votes_abstain = sum(
            vote.voting_power_used
            for vote in proposal_votes
            if vote.vote_choice == VoteType.ABSTAIN
        )

        total_votes_cast = votes_for + votes_against + votes_abstain
        participation_rate = (
            total_votes_cast / total_voting_power if total_voting_power > 0 else 0
        )

        # Check quorum
        quorum_reached = participation_rate >= self.config.quorum_threshold

        # Check approval (only count decisive votes)
        decisive_votes = votes_for + votes_against
        approval_rate = votes_for / decisive_votes if decisive_votes > 0 else 0

        # Determine if proposal passed
        proposal_passed = (
            quorum_reached and approval_rate >= self.config.approval_threshold
        )

        # Check emergency threshold for emergency proposals
        proposal = self.proposals[proposal_id]
        if proposal.proposal_type == ProposalType.EMERGENCY_ACTION:
            proposal_passed = (
                quorum_reached and approval_rate >= self.config.emergency_threshold
            )

        return DaoVotingResult(
            proposal_id=proposal_id,
            total_voting_power=total_voting_power,
            votes_for=votes_for,
            votes_against=votes_against,
            votes_abstain=votes_abstain,
            total_voters=len(proposal_votes),
            quorum_reached=quorum_reached,
            proposal_passed=proposal_passed,
        )

    def finalize_proposal(
        self, proposal_id: ProposalId
    ) -> DecentralizedAutonomousOrganization:
        """Finalize proposal voting and update status."""
        if proposal_id not in self.proposals:
            raise ValueError(f"Proposal {proposal_id} not found")

        proposal = self.proposals[proposal_id]
        current_time = time.time()

        # Check if voting period has ended
        if current_time <= proposal.voting_ends_at:
            raise ValueError("Voting period has not ended")

        # Calculate results
        result = self.calculate_voting_result(proposal_id)

        # Update proposal status
        new_status = (
            ProposalStatus.PASSED if result.proposal_passed else ProposalStatus.REJECTED
        )
        updated_proposal = proposal.with_status(new_status)

        # Create finalization transaction
        finalize_tx = Transaction(
            sender="dao_manager",
            receiver=self.dao_id,
            operation_type=OperationType.CONSENSUS_VOTE,
            payload=json.dumps(
                {
                    "action": "finalize_proposal",
                    "dao_id": self.dao_id,
                    "proposal_id": proposal_id,
                    "result": {
                        "passed": result.proposal_passed,
                        "votes_for": result.votes_for,
                        "votes_against": result.votes_against,
                        "votes_abstain": result.votes_abstain,
                        "quorum_reached": result.quorum_reached,
                    },
                }
            ).encode(),
            fee=0,
        )

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.blockchain.get_latest_block(),
            transactions=(finalize_tx,),
            miner="dao_manager",
        )

        new_blockchain = self.blockchain.add_block(new_block)
        new_proposals = dict(self.proposals)
        new_proposals[proposal_id] = updated_proposal

        return DecentralizedAutonomousOrganization(
            dao_id=self.dao_id,
            name=self.name,
            description=self.description,
            dao_type=self.dao_type,
            config=self.config,
            blockchain=new_blockchain,
            members=self.members,
            proposals=new_proposals,
            votes=self.votes,
            executions=self.executions,
            created_at=self.created_at,
            treasury_balance=self.treasury_balance,
        )

    def execute_proposal(
        self, proposal_id: ProposalId, executor_id: MemberId
    ) -> DecentralizedAutonomousOrganization:
        """Execute a passed proposal."""
        if proposal_id not in self.proposals:
            raise ValueError(f"Proposal {proposal_id} not found")

        if executor_id not in self.members:
            raise ValueError(f"Executor {executor_id} is not a member")

        proposal = self.proposals[proposal_id]
        current_time = time.time()

        # Check if proposal can be executed
        if not proposal.can_execute(current_time):
            raise ValueError("Proposal cannot be executed yet")

        if proposal.status != ProposalStatus.PASSED:
            raise ValueError("Only passed proposals can be executed")

        # Create execution record
        execution = DaoExecution(
            proposal_id=proposal_id,
            executor_id=executor_id,
            execution_status=ExecutionStatus.EXECUTED,
            executed_at=current_time,
        )

        # Create execution transaction
        execute_tx = Transaction(
            sender=executor_id,
            receiver=self.dao_id,
            operation_type=OperationType.SMART_CONTRACT,
            payload=json.dumps(
                {
                    "action": "execute_proposal",
                    "dao_id": self.dao_id,
                    "proposal_id": proposal_id,
                    "execution_data": proposal.execution_data.hex(),
                    "executor": executor_id,
                }
            ).encode(),
            fee=10,
        )

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.blockchain.get_latest_block(),
            transactions=(execute_tx,),
            miner="dao_manager",
        )

        new_blockchain = self.blockchain.add_block(new_block)

        # Update proposal status
        executed_proposal = proposal.with_status(ProposalStatus.EXECUTED)
        new_proposals = dict(self.proposals)
        new_proposals[proposal_id] = executed_proposal

        new_executions = dict(self.executions)
        new_executions[proposal_id] = execution

        return DecentralizedAutonomousOrganization(
            dao_id=self.dao_id,
            name=self.name,
            description=self.description,
            dao_type=self.dao_type,
            config=self.config,
            blockchain=new_blockchain,
            members=self.members,
            proposals=new_proposals,
            votes=self.votes,
            executions=new_executions,
            created_at=self.created_at,
            treasury_balance=self.treasury_balance,
        )

    # Utility Methods

    def get_active_proposals(
        self, current_time: Timestamp | None = None
    ) -> list[DaoProposal]:
        """Get all currently active proposals."""
        if current_time is None:
            current_time = time.time()

        return [
            proposal
            for proposal in self.proposals.values()
            if proposal.is_active(current_time)
        ]

    def get_member_voting_history(self, member_id: MemberId) -> list[DaoVote]:
        """Get voting history for a member."""
        history: list[DaoVote] = []
        for votes_list in self.votes.values():
            history.extend(vote for vote in votes_list if vote.voter_id == member_id)
        return sorted(history, key=lambda v: v.cast_at, reverse=True)

    def get_proposal_votes(self, proposal_id: ProposalId) -> list[DaoVote]:
        """Get all votes for a specific proposal."""
        return self.votes.get(proposal_id, [])

    def get_dao_statistics(self) -> dict[str, Any]:
        """Get comprehensive DAO statistics."""
        current_time = time.time()

        active_proposals = self.get_active_proposals(current_time)
        passed_proposals = [
            p for p in self.proposals.values() if p.status == ProposalStatus.PASSED
        ]
        executed_proposals = [
            p for p in self.proposals.values() if p.status == ProposalStatus.EXECUTED
        ]

        total_votes_cast = sum(len(votes) for votes in self.votes.values())

        return {
            "dao_id": self.dao_id,
            "name": self.name,
            "dao_type": self.dao_type.value,
            "created_at": self.created_at,
            "member_count": self.get_member_count(),
            "total_voting_power": self.get_total_voting_power(),
            "treasury_balance": self.treasury_balance,
            "total_proposals": len(self.proposals),
            "active_proposals": len(active_proposals),
            "passed_proposals": len(passed_proposals),
            "executed_proposals": len(executed_proposals),
            "total_votes_cast": total_votes_cast,
            "blockchain_height": self.blockchain.get_height(),
            "governance_activity": {
                "proposals_last_30_days": len(
                    [
                        p
                        for p in self.proposals.values()
                        if current_time - p.created_at <= 30 * 86400
                    ]
                ),
                "avg_participation_rate": self._calculate_avg_participation_rate(),
            },
        }

    def _calculate_avg_participation_rate(self) -> float:
        """Calculate average participation rate across all proposals."""
        if not self.proposals:
            return 0.0

        total_rate = 0.0
        count = 0

        for proposal_id in self.proposals:
            if proposal_id in self.votes:
                result = self.calculate_voting_result(proposal_id)
                total_rate += result.get_participation_rate()
                count += 1

        return total_rate / count if count > 0 else 0.0

    def validate_dao_integrity(self) -> bool:
        """Validate DAO state integrity."""
        try:
            # Validate blockchain
            if not self.blockchain.validate_chain():
                return False

            # Validate all members
            for member in self.members.values():
                if member.voting_power < 0:
                    return False

            # Validate all proposals
            for proposal in self.proposals.values():
                if proposal.proposer_id not in self.members:
                    return False

            # Validate all votes
            for proposal_id, votes_list in self.votes.items():
                if proposal_id not in self.proposals:
                    return False
                for vote in votes_list:
                    if vote.voter_id not in self.members:
                        return False

            return True

        except Exception:
            return False

    def to_dict(self) -> dict[str, Any]:
        """Convert DAO to dictionary for serialization."""
        return {
            "dao_id": self.dao_id,
            "name": self.name,
            "description": self.description,
            "dao_type": self.dao_type.value,
            "config": {
                "dao_type": self.config.dao_type.value,
                "membership_type": self.config.membership_type.value,
                "voting_period_seconds": self.config.voting_period_seconds,
                "execution_delay_seconds": self.config.execution_delay_seconds,
                "quorum_threshold": self.config.quorum_threshold,
                "approval_threshold": self.config.approval_threshold,
                "proposal_deposit": self.config.proposal_deposit,
                "minimum_voting_power": self.config.minimum_voting_power,
            },
            "blockchain": self.blockchain.to_dict(),
            "members": {
                member_id: {
                    "member_id": member.member_id,
                    "voting_power": member.voting_power,
                    "token_balance": member.token_balance,
                    "reputation_score": member.reputation_score,
                    "joined_at": member.joined_at,
                    "delegated_to": member.delegated_to,
                    "is_active": member.is_active,
                    "metadata": member.metadata,
                }
                for member_id, member in self.members.items()
            },
            "statistics": self.get_dao_statistics(),
        }

    def __str__(self) -> str:
        """String representation of DAO."""
        return (
            f"DAO(id={self.dao_id}, name='{self.name}', "
            f"type={self.dao_type.value}, members={len(self.members)}, "
            f"proposals={len(self.proposals)})"
        )


# Hypothesis strategies for property-based testing


def dao_member_strategy() -> st.SearchStrategy[DaoMember]:
    """Generate valid DaoMember instances for testing."""
    return st.builds(
        DaoMember,
        member_id=st.text(min_size=1, max_size=50),
        voting_power=st.integers(min_value=1, max_value=10000),
        token_balance=st.integers(min_value=0, max_value=1000000),
        reputation_score=st.integers(min_value=0, max_value=1000),
        joined_at=st.floats(min_value=0, max_value=1700000000),
        delegated_to=st.one_of(st.none(), st.text(min_size=1, max_size=50)),
        is_active=st.booleans(),
        metadata=st.dictionaries(st.text(), st.one_of(st.text(), st.integers())),
    )


def dao_proposal_strategy() -> st.SearchStrategy[DaoProposal]:
    """Generate valid DaoProposal instances for testing."""

    @st.composite
    def generate_valid_proposal(draw):
        proposer_id = draw(st.text(min_size=1, max_size=50))
        proposal_type = draw(st.sampled_from(ProposalType))
        title = draw(st.text(min_size=1, max_size=200))
        description = draw(st.text(min_size=1, max_size=1000))
        execution_data = draw(st.binary(max_size=1024))
        created_at = draw(st.floats(min_value=0, max_value=1700000000))
        voting_starts_at = draw(st.floats(min_value=created_at, max_value=1700000000))
        voting_ends_at = draw(
            st.floats(min_value=voting_starts_at + 1, max_value=1700000001)
        )
        execution_time = draw(st.floats(min_value=voting_ends_at, max_value=1700000001))
        deposit_amount = draw(st.integers(min_value=0, max_value=1000))
        status = draw(st.sampled_from(ProposalStatus))
        metadata = draw(st.dictionaries(st.text(), st.one_of(st.text(), st.integers())))

        return DaoProposal(
            proposer_id=proposer_id,
            proposal_type=proposal_type,
            title=title,
            description=description,
            execution_data=execution_data,
            created_at=created_at,
            voting_starts_at=voting_starts_at,
            voting_ends_at=voting_ends_at,
            execution_time=execution_time,
            deposit_amount=deposit_amount,
            status=status,
            metadata=metadata,
        )

    return generate_valid_proposal()


def dao_config_strategy() -> st.SearchStrategy[DaoConfig]:
    """Generate valid DaoConfig instances for testing."""
    return st.builds(
        DaoConfig,
        dao_type=st.sampled_from(DaoType),
        membership_type=st.sampled_from(MembershipType),
        voting_period_seconds=st.integers(
            min_value=3600, max_value=30 * 86400
        ),  # 1 hour to 30 days
        execution_delay_seconds=st.integers(
            min_value=0, max_value=7 * 86400
        ),  # 0 to 7 days
        quorum_threshold=st.floats(min_value=0.01, max_value=1.0),
        approval_threshold=st.floats(min_value=0.01, max_value=1.0),
        proposal_deposit=st.integers(min_value=0, max_value=10000),
        minimum_voting_power=st.integers(min_value=1, max_value=100),
        max_proposals_per_member=st.integers(min_value=1, max_value=10),
        proposal_cooldown_seconds=st.integers(min_value=0, max_value=86400),
    )


def dao_strategy() -> st.SearchStrategy[DecentralizedAutonomousOrganization]:
    """Generate valid DAO instances for testing."""
    return st.builds(
        DecentralizedAutonomousOrganization,
        name=st.text(min_size=1, max_size=100),
        description=st.text(min_size=1, max_size=500),
        dao_type=st.sampled_from(DaoType),
        config=dao_config_strategy(),
        created_at=st.floats(min_value=0, max_value=1700000000),
        treasury_balance=st.integers(min_value=0, max_value=1000000),
    )
