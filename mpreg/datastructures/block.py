"""
Block datastructure for blockchain operations.

This module provides an immutable Block class that combines Merkle trees
for transaction verification with Vector clocks for distributed consensus.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from hypothesis import strategies as st

from .blockchain_types import (
    BlockHash,
    BlockHeight,
    BlockId,
    CryptoConfig,
    Difficulty,
    MerkleHash,
    NodeId,
    Nonce,
    Timestamp,
    compute_block_hash,
    current_timestamp,
    generate_block_id,
)
from .merkle_tree import MerkleProof, MerkleTree
from .transaction import Transaction, transaction_strategy
from .vector_clock import VectorClock


@dataclass(frozen=True, slots=True)
class Block:
    """
    Immutable block containing transactions with Merkle proof and vector clock.

    Combines MPREG's distributed time (Vector Clock) with cryptographic
    verification (Merkle Tree) for federated consensus.
    """

    block_id: BlockId = field(default_factory=generate_block_id)
    height: BlockHeight = 0
    previous_hash: BlockHash = ""
    merkle_root: MerkleHash = ""
    vector_clock: VectorClock = field(default_factory=VectorClock)
    transactions: tuple[Transaction, ...] = field(default_factory=tuple)
    miner: NodeId = ""
    difficulty: Difficulty = 1
    nonce: Nonce = 0
    timestamp: Timestamp = field(default_factory=current_timestamp)

    def __post_init__(self) -> None:
        """Validate block fields and compute derived values."""
        if not self.block_id:
            raise ValueError("Block ID cannot be empty")
        if self.height < 0:
            raise ValueError("Block height cannot be negative")
        if not self.miner:
            raise ValueError("Miner cannot be empty")
        if self.difficulty <= 0:
            raise ValueError("Difficulty must be positive")
        if self.nonce < 0:
            raise ValueError("Nonce cannot be negative")
        if self.timestamp < 0:
            raise ValueError("Timestamp cannot be negative")

        # Validate merkle root matches transactions
        expected_merkle_root = self._compute_merkle_root()
        if self.transactions:
            if self.merkle_root != expected_merkle_root:
                raise ValueError("Merkle root does not match transactions")
        else:
            if self.merkle_root:
                raise ValueError(
                    "Merkle root must be empty when there are no transactions"
                )

    def _compute_merkle_root(self) -> MerkleHash:
        """Compute Merkle root for transactions."""
        if not self.transactions:
            return ""

        transaction_hashes = [tx.to_bytes() for tx in self.transactions]
        merkle_tree = MerkleTree.from_leaves(transaction_hashes)
        return merkle_tree.root_hash()

    def get_transactions_tree(self) -> MerkleTree:
        """Get Merkle tree of transactions."""
        if not self.transactions:
            return MerkleTree.empty()

        transaction_hashes = [tx.to_bytes() for tx in self.transactions]
        return MerkleTree.from_leaves(transaction_hashes)

    def get_block_hash(self) -> BlockHash:
        """Compute deterministic hash of block header."""
        return compute_block_hash(
            self.previous_hash, self.merkle_root, self.timestamp, self.nonce
        )

    def generate_transaction_proof(self, tx_index: int) -> MerkleProof:
        """Generate Merkle proof for transaction at index."""
        if tx_index < 0 or tx_index >= len(self.transactions):
            raise IndexError(f"Transaction index {tx_index} out of range")

        tree = self.get_transactions_tree()
        return tree.generate_proof(tx_index)

    def verify_transaction_proof(self, proof: MerkleProof) -> bool:
        """Verify a transaction Merkle proof against this block."""
        return proof.root_hash == self.merkle_root and proof.verify()

    def verify_proof_of_work(self, target_prefix: str = "0000") -> bool:
        """Verify proof-of-work (simple leading zeros)."""
        block_hash = self.get_block_hash()
        return block_hash.startswith(target_prefix)

    def contains_transaction(self, transaction_id: str) -> bool:
        """Check if block contains transaction with given ID."""
        return any(tx.transaction_id == transaction_id for tx in self.transactions)

    def get_transaction(self, transaction_id: str) -> Transaction | None:
        """Get transaction by ID from this block."""
        for tx in self.transactions:
            if tx.transaction_id == transaction_id:
                return tx
        return None

    def is_valid_successor(self, previous_block: Block | None) -> bool:
        """Check if this block is a valid successor to the previous block."""
        if previous_block is None:
            # Genesis block validation
            return (
                self.height == 0
                and self.previous_hash == ""
                and self.vector_clock.is_empty()
            )

        # Check height increment
        if self.height != previous_block.height + 1:
            return False

        # Check previous hash reference
        if self.previous_hash != previous_block.get_block_hash():
            return False

        # Check vector clock causality
        if not previous_block.vector_clock.happens_before(self.vector_clock):
            return False

        # Check timestamp progression
        return not self.timestamp <= previous_block.timestamp

    def validate_transactions(
        self,
        current_time: Timestamp | None = None,
        *,
        crypto_config: CryptoConfig | None = None,
    ) -> bool:
        """Validate all transactions in the block."""
        if current_time is None:
            current_time = time.time()

        for tx in self.transactions:
            if not tx.is_valid(current_time, crypto_config=crypto_config):
                return False

        return True

    def is_valid(
        self,
        previous_block: Block | None = None,
        current_time: Timestamp | None = None,
        *,
        crypto_config: CryptoConfig | None = None,
    ) -> bool:
        """Comprehensive block validation."""
        if current_time is None:
            current_time = time.time()

        # Basic structure validation
        try:
            self.__post_init__()
        except ValueError:
            return False

        # Successor validation
        if not self.is_valid_successor(previous_block):
            return False

        # Transaction validation
        if not self.validate_transactions(current_time, crypto_config=crypto_config):
            return False

        # Merkle root validation
        return self.merkle_root == self._compute_merkle_root()

    def with_nonce(self, nonce: Nonce) -> Block:
        """Create new block with specified nonce (for mining)."""
        return Block(
            block_id=self.block_id,
            height=self.height,
            previous_hash=self.previous_hash,
            merkle_root=self.merkle_root,
            vector_clock=self.vector_clock,
            transactions=self.transactions,
            miner=self.miner,
            difficulty=self.difficulty,
            nonce=nonce,
            timestamp=self.timestamp,
        )

    def with_transactions(self, transactions: tuple[Transaction, ...]) -> Block:
        """Create new block with specified transactions."""
        new_merkle_root = ""
        if transactions:
            transaction_hashes = [tx.to_bytes() for tx in transactions]
            merkle_tree = MerkleTree.from_leaves(transaction_hashes)
            new_merkle_root = merkle_tree.root_hash()

        return Block(
            block_id=self.block_id,
            height=self.height,
            previous_hash=self.previous_hash,
            merkle_root=new_merkle_root,
            vector_clock=self.vector_clock,
            transactions=transactions,
            miner=self.miner,
            difficulty=self.difficulty,
            nonce=self.nonce,
            timestamp=self.timestamp,
        )

    def with_vector_clock(self, vector_clock: VectorClock) -> Block:
        """Create new block with specified vector clock."""
        return Block(
            block_id=self.block_id,
            height=self.height,
            previous_hash=self.previous_hash,
            merkle_root=self.merkle_root,
            vector_clock=vector_clock,
            transactions=self.transactions,
            miner=self.miner,
            difficulty=self.difficulty,
            nonce=self.nonce,
            timestamp=self.timestamp,
        )

    def age_seconds(self, current_time: Timestamp | None = None) -> float:
        """Get age of block in seconds."""
        if current_time is None:
            current_time = time.time()
        return max(0.0, current_time - self.timestamp)

    def transaction_count(self) -> int:
        """Get number of transactions in block."""
        return len(self.transactions)

    def is_empty(self) -> bool:
        """Check if block has no transactions."""
        return len(self.transactions) == 0

    def total_fees(self) -> int:
        """Calculate total transaction fees in block."""
        return sum(tx.fee for tx in self.transactions)

    def to_dict(self) -> dict[str, Any]:
        """Convert block to dictionary for serialization."""
        return {
            "block_id": self.block_id,
            "height": self.height,
            "previous_hash": self.previous_hash,
            "merkle_root": self.merkle_root,
            "vector_clock": self.vector_clock.to_dict(),
            "transactions": [tx.to_dict() for tx in self.transactions],
            "miner": self.miner,
            "difficulty": self.difficulty,
            "nonce": self.nonce,
            "timestamp": self.timestamp,
            "block_hash": self.get_block_hash(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Block:
        """Create block from dictionary."""
        from .transaction import Transaction

        return cls(
            block_id=data["block_id"],
            height=data["height"],
            previous_hash=data["previous_hash"],
            merkle_root=data["merkle_root"],
            vector_clock=VectorClock.from_dict(data["vector_clock"]),
            transactions=tuple(
                Transaction.from_dict(tx) for tx in data["transactions"]
            ),
            miner=data["miner"],
            difficulty=data["difficulty"],
            nonce=data["nonce"],
            timestamp=data["timestamp"],
        )

    @classmethod
    def create_genesis(
        cls, miner: NodeId, initial_transactions: tuple[Transaction, ...] = ()
    ) -> Block:
        """Create genesis block."""
        merkle_root = ""
        if initial_transactions:
            transaction_hashes = [tx.to_bytes() for tx in initial_transactions]
            merkle_tree = MerkleTree.from_leaves(transaction_hashes)
            merkle_root = merkle_tree.root_hash()

        return cls(
            height=0,
            previous_hash="",
            merkle_root=merkle_root,
            vector_clock=VectorClock(),
            transactions=initial_transactions,
            miner=miner,
            difficulty=1,
            nonce=0,
        )

    @classmethod
    def create_next_block(
        cls,
        previous_block: Block,
        transactions: tuple[Transaction, ...],
        miner: NodeId,
        difficulty: Difficulty | None = None,
    ) -> Block:
        """Create next block in chain."""
        if difficulty is None:
            difficulty = previous_block.difficulty

        # Increment vector clock for miner
        new_vector_clock = previous_block.vector_clock.increment(miner)

        # Compute merkle root
        merkle_root = ""
        if transactions:
            transaction_hashes = [tx.to_bytes() for tx in transactions]
            merkle_tree = MerkleTree.from_leaves(transaction_hashes)
            merkle_root = merkle_tree.root_hash()
        latest_tx_timestamp = max((tx.timestamp for tx in transactions), default=0.0)
        timestamp = max(
            current_timestamp(),
            latest_tx_timestamp,
            previous_block.timestamp + 1e-6,
        )

        return cls(
            height=previous_block.height + 1,
            previous_hash=previous_block.get_block_hash(),
            merkle_root=merkle_root,
            vector_clock=new_vector_clock,
            transactions=transactions,
            miner=miner,
            difficulty=difficulty,
            nonce=0,
            timestamp=timestamp,
        )

    def __str__(self) -> str:
        """String representation of block."""
        return (
            f"Block(height={self.height}, "
            f"transactions={len(self.transactions)}, "
            f"miner={self.miner}, "
            f"hash={self.get_block_hash()[:16]}...)"
        )


# Hypothesis strategies for property-based testing


def block_id_strategy() -> st.SearchStrategy[BlockId]:
    """Generate valid block IDs."""
    return st.text(min_size=1, max_size=100)


def block_hash_strategy() -> st.SearchStrategy[BlockHash]:
    """Generate valid block hashes."""
    return st.text(min_size=64, max_size=64, alphabet="0123456789abcdef")


def merkle_hash_strategy() -> st.SearchStrategy[MerkleHash]:
    """Generate valid merkle hashes."""
    return st.text(min_size=64, max_size=64, alphabet="0123456789abcdef")


def node_id_strategy() -> st.SearchStrategy[NodeId]:
    """Generate valid node IDs."""
    return st.text(
        min_size=1,
        max_size=50,
        alphabet=st.characters(
            whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
        ),
    )


def block_strategy() -> st.SearchStrategy[Block]:
    """Generate valid Block instances for testing."""
    from .vector_clock import vector_clock_strategy

    @st.composite
    def _block(draw):
        transactions = draw(st.lists(transaction_strategy(), max_size=10).map(tuple))
        merkle_root = ""
        if transactions:
            transaction_hashes = [tx.to_bytes() for tx in transactions]
            merkle_tree = MerkleTree.from_leaves(transaction_hashes)
            merkle_root = merkle_tree.root_hash()
            latest_tx_timestamp = max(tx.timestamp for tx in transactions)
        else:
            latest_tx_timestamp = 0.0

        timestamp = draw(st.floats(min_value=0, max_value=1700000000))
        timestamp = max(timestamp, latest_tx_timestamp)

        return Block(
            block_id=draw(block_id_strategy()),
            height=draw(st.integers(min_value=0, max_value=1000)),
            previous_hash=draw(st.one_of(st.just(""), block_hash_strategy())),
            merkle_root=merkle_root,
            vector_clock=draw(vector_clock_strategy()),
            transactions=transactions,
            miner=draw(node_id_strategy()),
            difficulty=draw(st.integers(min_value=1, max_value=10)),
            nonce=draw(st.integers(min_value=0, max_value=1000000)),
            timestamp=timestamp,
        )

    return _block()


def genesis_block_strategy() -> st.SearchStrategy[Block]:
    """Generate valid genesis blocks for testing."""
    return st.builds(
        Block.create_genesis,
        miner=node_id_strategy(),
        initial_transactions=st.lists(transaction_strategy(), max_size=5).map(tuple),
    )
