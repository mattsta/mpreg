"""
Semantic type aliases for blockchain datastructures.

This module provides well-typed aliases for blockchain operations,
replacing raw strings and primitives with meaningful semantic types.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum

# Core blockchain identifiers
type TransactionId = str
type BlockId = str
type BlockHash = str
type ChainId = str
type NodeId = str

# Cryptographic types
type DigitalSignature = bytes
type PublicKey = bytes
type PrivateKey = bytes
type TransactionHash = str
type MerkleHash = str

# Temporal types
type Timestamp = float
type BlockHeight = int
type Nonce = int

# Consensus types
type Difficulty = int
type StakeAmount = int
type ValidatorWeight = int

# Transaction types
type TransactionPayload = bytes
type TransactionFee = int
type GasLimit = int
type GasPrice = int

# Network types
type NetworkId = str
type PeerId = str


class OperationType(Enum):
    """Types of operations supported in transactions."""

    TRANSFER = "transfer"
    MESSAGE = "message"
    FEDERATION_JOIN = "federation_join"
    FEDERATION_LEAVE = "federation_leave"
    NODE_REGISTER = "node_register"
    NODE_UPDATE = "node_update"
    CONSENSUS_VOTE = "consensus_vote"
    SMART_CONTRACT = "smart_contract"


class ConsensusType(Enum):
    """Supported consensus mechanisms."""

    PROOF_OF_AUTHORITY = "proof_of_authority"
    PROOF_OF_STAKE = "proof_of_stake"
    PROOF_OF_WORK = "proof_of_work"
    FEDERATED_BYZANTINE = "federated_byzantine"


@dataclass(frozen=True, slots=True)
class ConsensusConfig:
    """Configuration for blockchain consensus mechanisms."""

    consensus_type: ConsensusType
    block_time_target: int  # Target seconds between blocks
    difficulty_adjustment_interval: int  # Blocks between difficulty adjustments
    max_transactions_per_block: int
    minimum_stake: StakeAmount = 0  # For proof-of-stake
    authority_threshold: float = 0.67  # Fraction of authorities needed


@dataclass(frozen=True, slots=True)
class SlashingConfig:
    """Configuration for proof-of-stake slashing penalties."""

    double_sign_penalty: float = 0.05  # 5% stake loss
    offline_penalty: float = 0.001  # 0.1% stake loss
    byzantine_penalty: float = 0.30  # 30% stake loss
    evidence_validity_period: int = 100  # Blocks


@dataclass(frozen=True, slots=True)
class NetworkConfig:
    """Configuration for blockchain network parameters."""

    network_id: NetworkId
    chain_id: ChainId
    genesis_timestamp: Timestamp
    minimum_fee: TransactionFee = 1
    maximum_block_size: int = 1024 * 1024  # 1MB
    transaction_timeout: int = 3600  # 1 hour


@dataclass(frozen=True, slots=True)
class CryptoConfig:
    """Configuration for cryptographic parameters."""

    hash_algorithm: str = "sha256"
    signature_algorithm: str = "ed25519"
    key_length: int = 256
    require_signatures: bool = False


# Utility functions for type creation
def generate_transaction_id() -> TransactionId:
    """Generate unique transaction ID."""
    import uuid

    return str(uuid.uuid4())


def generate_block_id() -> BlockId:
    """Generate unique block ID."""
    import uuid

    return str(uuid.uuid4())


def current_timestamp() -> Timestamp:
    """Get current timestamp."""
    return time.time()


def compute_block_hash(
    previous_hash: BlockHash,
    merkle_root: MerkleHash,
    timestamp: Timestamp,
    nonce: Nonce,
) -> BlockHash:
    """Compute hash for a block."""
    import hashlib

    data = f"{previous_hash}:{merkle_root}:{timestamp}:{nonce}"
    return hashlib.sha256(data.encode()).hexdigest()


def compute_transaction_hash(
    sender: NodeId,
    receiver: NodeId,
    operation_type: OperationType,
    payload: TransactionPayload,
    timestamp: Timestamp,
    nonce: Nonce,
) -> TransactionHash:
    """Compute hash for a transaction."""
    import hashlib

    data = f"{sender}:{receiver}:{operation_type.value}:{payload.hex()}:{timestamp}:{nonce}"
    return hashlib.sha256(data.encode()).hexdigest()
