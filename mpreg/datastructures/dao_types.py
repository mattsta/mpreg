"""
DAO (Decentralized Autonomous Organization) type definitions.

This module provides semantic type aliases and enums for DAO operations,
following the same design principles as our blockchain types with strong
typing and clear separation of concerns.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# DAO Semantic Types
DaoId = str
ProposalId = str
MemberId = str
VotingPower = int
TokenAmount = int
Timestamp = float
DaoName = str
ProposalTitle = str
ProposalDescription = str
VoteChoice = str
QuorumThreshold = float
ApprovalThreshold = float


def generate_dao_id() -> DaoId:
    """Generate unique DAO identifier."""
    return f"dao_{uuid.uuid4()}"


def generate_proposal_id() -> ProposalId:
    """Generate unique proposal identifier."""
    return f"prop_{uuid.uuid4()}"


def current_timestamp() -> Timestamp:
    """Get current timestamp."""
    return time.time()


class DaoType(Enum):
    """Types of DAOs supported."""

    FEDERATION_GOVERNANCE = "federation_governance"
    PROJECT_FUNDING = "project_funding"
    TECHNICAL_COMMITTEE = "technical_committee"
    COMMUNITY_GOVERNANCE = "community_governance"
    RESOURCE_ALLOCATION = "resource_allocation"
    PROTOCOL_UPGRADE = "protocol_upgrade"


class MembershipType(Enum):
    """Types of DAO membership."""

    TOKEN_HOLDER = "token_holder"
    STAKE_BASED = "stake_based"
    INVITE_ONLY = "invite_only"
    REPUTATION_BASED = "reputation_based"
    DELEGATION_BASED = "delegation_based"


class ProposalType(Enum):
    """Types of proposals that can be made."""

    CONSTITUTIONAL_CHANGE = "constitutional_change"
    BUDGET_ALLOCATION = "budget_allocation"
    MEMBER_ADMISSION = "member_admission"
    MEMBER_REMOVAL = "member_removal"
    PARAMETER_CHANGE = "parameter_change"
    CONTRACT_EXECUTION = "contract_execution"
    TEXT_PROPOSAL = "text_proposal"
    EMERGENCY_ACTION = "emergency_action"
    PROTOCOL_UPGRADE = "protocol_upgrade"


class ProposalStatus(Enum):
    """Status of DAO proposals."""

    DRAFT = "draft"
    ACTIVE = "active"
    PASSED = "passed"
    REJECTED = "rejected"
    EXECUTED = "executed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class VoteType(Enum):
    """Types of votes in DAO proposals."""

    FOR = "for"
    AGAINST = "against"
    ABSTAIN = "abstain"


class ExecutionStatus(Enum):
    """Status of proposal execution."""

    PENDING = "pending"
    EXECUTING = "executing"
    EXECUTED = "executed"
    FAILED = "failed"
    REVERTED = "reverted"


@dataclass(frozen=True, slots=True)
class DaoConfig:
    """Configuration parameters for DAO governance."""

    dao_type: DaoType
    membership_type: MembershipType
    voting_period_seconds: int = 86400 * 7  # 7 days default
    execution_delay_seconds: int = 86400 * 2  # 2 days default
    quorum_threshold: QuorumThreshold = 0.1  # 10% quorum
    approval_threshold: ApprovalThreshold = 0.5  # Simple majority
    proposal_deposit: TokenAmount = 100
    minimum_voting_power: VotingPower = 1
    max_proposals_per_member: int = 3
    proposal_cooldown_seconds: int = 86400  # 1 day between proposals
    emergency_threshold: ApprovalThreshold = 0.67  # 2/3 for emergency actions

    def __post_init__(self) -> None:
        """Validate DAO configuration."""
        if self.voting_period_seconds <= 0:
            raise ValueError("Voting period must be positive")
        if self.execution_delay_seconds < 0:
            raise ValueError("Execution delay cannot be negative")
        if not (0.0 <= self.quorum_threshold <= 1.0):
            raise ValueError("Quorum threshold must be between 0 and 1")
        if not (0.0 <= self.approval_threshold <= 1.0):
            raise ValueError("Approval threshold must be between 0 and 1")
        if self.proposal_deposit < 0:
            raise ValueError("Proposal deposit cannot be negative")
        if self.minimum_voting_power <= 0:
            raise ValueError("Minimum voting power must be positive")


@dataclass(frozen=True, slots=True)
class DaoMember:
    """DAO member with voting power and metadata."""

    member_id: MemberId
    voting_power: VotingPower
    token_balance: TokenAmount = 0
    reputation_score: int = 0
    joined_at: Timestamp = field(default_factory=current_timestamp)
    delegated_to: MemberId | None = None
    is_active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate DAO member."""
        if not self.member_id:
            raise ValueError("Member ID cannot be empty")
        if self.voting_power < 0:
            raise ValueError("Voting power cannot be negative")
        if self.token_balance < 0:
            raise ValueError("Token balance cannot be negative")
        if self.reputation_score < 0:
            raise ValueError("Reputation score cannot be negative")
        if self.joined_at < 0:
            raise ValueError("Joined timestamp cannot be negative")

    def get_effective_voting_power(self) -> VotingPower:
        """Calculate effective voting power considering delegation."""
        if not self.is_active:
            return 0
        return self.voting_power

    def delegate_to(self, delegate_id: MemberId) -> DaoMember:
        """Create new member with delegation."""
        if delegate_id == self.member_id:
            raise ValueError("Cannot delegate to self")

        return DaoMember(
            member_id=self.member_id,
            voting_power=self.voting_power,
            token_balance=self.token_balance,
            reputation_score=self.reputation_score,
            joined_at=self.joined_at,
            delegated_to=delegate_id,
            is_active=self.is_active,
            metadata=self.metadata,
        )

    def update_voting_power(self, new_power: VotingPower) -> DaoMember:
        """Create new member with updated voting power."""
        return DaoMember(
            member_id=self.member_id,
            voting_power=new_power,
            token_balance=self.token_balance,
            reputation_score=self.reputation_score,
            joined_at=self.joined_at,
            delegated_to=self.delegated_to,
            is_active=self.is_active,
            metadata=self.metadata,
        )


@dataclass(frozen=True, slots=True)
class DaoProposal:
    """DAO proposal with complete proposal lifecycle."""

    proposal_id: ProposalId = field(default_factory=generate_proposal_id)
    proposer_id: MemberId = ""
    proposal_type: ProposalType = ProposalType.TEXT_PROPOSAL
    title: ProposalTitle = ""
    description: ProposalDescription = ""
    execution_data: bytes = b""  # Smart contract call data or execution parameters
    created_at: Timestamp = field(default_factory=current_timestamp)
    voting_starts_at: Timestamp = field(default_factory=current_timestamp)
    voting_ends_at: Timestamp = 0
    execution_time: Timestamp = 0
    deposit_amount: TokenAmount = 0
    status: ProposalStatus = ProposalStatus.DRAFT
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate DAO proposal."""
        if not self.proposal_id:
            raise ValueError("Proposal ID cannot be empty")
        if not self.proposer_id:
            raise ValueError("Proposer ID cannot be empty")
        if not self.title:
            raise ValueError("Proposal title cannot be empty")
        if not self.description:
            raise ValueError("Proposal description cannot be empty")
        if self.created_at < 0:
            raise ValueError("Created timestamp cannot be negative")
        if self.voting_starts_at < 0:
            raise ValueError("Voting start timestamp cannot be negative")
        if self.voting_ends_at != 0 and self.voting_ends_at <= self.voting_starts_at:
            raise ValueError("Voting end must be after voting start")
        if self.deposit_amount < 0:
            raise ValueError("Deposit amount cannot be negative")

    def is_active(self, current_time: Timestamp | None = None) -> bool:
        """Check if proposal is in active voting period."""
        if current_time is None:
            current_time = time.time()

        return (
            self.status == ProposalStatus.ACTIVE
            and self.voting_starts_at <= current_time <= self.voting_ends_at
        )

    def is_expired(self, current_time: Timestamp | None = None) -> bool:
        """Check if proposal has expired."""
        if current_time is None:
            current_time = time.time()

        return (
            self.voting_ends_at != 0
            and current_time > self.voting_ends_at
            and self.status in [ProposalStatus.ACTIVE, ProposalStatus.DRAFT]
        )

    def can_execute(self, current_time: Timestamp | None = None) -> bool:
        """Check if proposal can be executed."""
        if current_time is None:
            current_time = time.time()

        return (
            self.status == ProposalStatus.PASSED and self.execution_time <= current_time
        )

    def with_status(self, new_status: ProposalStatus) -> DaoProposal:
        """Create new proposal with updated status."""
        return DaoProposal(
            proposal_id=self.proposal_id,
            proposer_id=self.proposer_id,
            proposal_type=self.proposal_type,
            title=self.title,
            description=self.description,
            execution_data=self.execution_data,
            created_at=self.created_at,
            voting_starts_at=self.voting_starts_at,
            voting_ends_at=self.voting_ends_at,
            execution_time=self.execution_time,
            deposit_amount=self.deposit_amount,
            status=new_status,
            metadata=self.metadata,
        )

    def with_voting_period(
        self, starts_at: Timestamp, ends_at: Timestamp
    ) -> DaoProposal:
        """Create new proposal with voting period set."""
        return DaoProposal(
            proposal_id=self.proposal_id,
            proposer_id=self.proposer_id,
            proposal_type=self.proposal_type,
            title=self.title,
            description=self.description,
            execution_data=self.execution_data,
            created_at=self.created_at,
            voting_starts_at=starts_at,
            voting_ends_at=ends_at,
            execution_time=ends_at + 86400,  # Default 1 day execution delay
            deposit_amount=self.deposit_amount,
            status=self.status,
            metadata=self.metadata,
        )


@dataclass(frozen=True, slots=True)
class DaoVote:
    """Individual vote cast by DAO member."""

    voter_id: MemberId
    proposal_id: ProposalId
    vote_choice: VoteType
    voting_power_used: VotingPower
    cast_at: Timestamp = field(default_factory=current_timestamp)
    reason: str = ""
    delegated_from: MemberId | None = None

    def __post_init__(self) -> None:
        """Validate DAO vote."""
        if not self.voter_id:
            raise ValueError("Voter ID cannot be empty")
        if not self.proposal_id:
            raise ValueError("Proposal ID cannot be empty")
        if self.voting_power_used < 0:
            raise ValueError("Voting power used cannot be negative")
        if self.cast_at < 0:
            raise ValueError("Cast timestamp cannot be negative")

    def is_delegated_vote(self) -> bool:
        """Check if this is a delegated vote."""
        return self.delegated_from is not None


@dataclass(frozen=True, slots=True)
class DaoVotingResult:
    """Results of voting on a DAO proposal."""

    proposal_id: ProposalId
    total_voting_power: VotingPower
    votes_for: VotingPower = 0
    votes_against: VotingPower = 0
    votes_abstain: VotingPower = 0
    total_voters: int = 0
    quorum_reached: bool = False
    proposal_passed: bool = False
    calculated_at: Timestamp = field(default_factory=current_timestamp)

    def __post_init__(self) -> None:
        """Validate voting result."""
        if not self.proposal_id:
            raise ValueError("Proposal ID cannot be empty")
        if self.total_voting_power < 0:
            raise ValueError("Total voting power cannot be negative")
        if any(v < 0 for v in [self.votes_for, self.votes_against, self.votes_abstain]):
            raise ValueError("Vote counts cannot be negative")
        if self.total_voters < 0:
            raise ValueError("Total voters cannot be negative")

    def get_participation_rate(self) -> float:
        """Calculate voter participation rate."""
        if self.total_voting_power == 0:
            return 0.0

        total_votes_cast = self.votes_for + self.votes_against + self.votes_abstain
        return total_votes_cast / self.total_voting_power

    def get_approval_rate(self) -> float:
        """Calculate approval rate among voters."""
        total_decisive_votes = self.votes_for + self.votes_against
        if total_decisive_votes == 0:
            return 0.0

        return self.votes_for / total_decisive_votes

    def get_vote_distribution(self) -> dict[str, float]:
        """Get vote distribution percentages."""
        total_votes = self.votes_for + self.votes_against + self.votes_abstain
        if total_votes == 0:
            return {"for": 0.0, "against": 0.0, "abstain": 0.0}

        return {
            "for": self.votes_for / total_votes,
            "against": self.votes_against / total_votes,
            "abstain": self.votes_abstain / total_votes,
        }


@dataclass(frozen=True, slots=True)
class DaoExecution:
    """Execution record for DAO proposal."""

    proposal_id: ProposalId
    executor_id: MemberId
    execution_status: ExecutionStatus
    executed_at: Timestamp = field(default_factory=current_timestamp)
    execution_result: bytes = b""
    gas_used: int = 0
    error_message: str = ""

    def __post_init__(self) -> None:
        """Validate DAO execution."""
        if not self.proposal_id:
            raise ValueError("Proposal ID cannot be empty")
        if not self.executor_id:
            raise ValueError("Executor ID cannot be empty")
        if self.executed_at < 0:
            raise ValueError("Execution timestamp cannot be negative")
        if self.gas_used < 0:
            raise ValueError("Gas used cannot be negative")

    def is_successful(self) -> bool:
        """Check if execution was successful."""
        return self.execution_status == ExecutionStatus.EXECUTED

    def with_result(self, result: bytes, gas_used: int = 0) -> DaoExecution:
        """Create new execution with result data."""
        return DaoExecution(
            proposal_id=self.proposal_id,
            executor_id=self.executor_id,
            execution_status=ExecutionStatus.EXECUTED,
            executed_at=self.executed_at,
            execution_result=result,
            gas_used=gas_used,
            error_message="",
        )

    def with_error(self, error_message: str) -> DaoExecution:
        """Create new execution with error."""
        return DaoExecution(
            proposal_id=self.proposal_id,
            executor_id=self.executor_id,
            execution_status=ExecutionStatus.FAILED,
            executed_at=self.executed_at,
            execution_result=b"",
            gas_used=self.gas_used,
            error_message=error_message,
        )
