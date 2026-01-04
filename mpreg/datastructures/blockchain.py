"""
Blockchain datastructure for distributed consensus.

This module provides an immutable Blockchain class that maintains a chain
of blocks with validation, consensus mechanisms, and fork resolution.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from hypothesis import strategies as st

from .block import Block, block_strategy, genesis_block_strategy
from .blockchain_types import (
    BlockHash,
    BlockHeight,
    ChainId,
    ConsensusConfig,
    ConsensusType,
    CryptoConfig,
    Difficulty,
    NodeId,
    Timestamp,
    TransactionId,
)
from .transaction import Transaction, transaction_strategy
from .vector_clock import VectorClock


@dataclass(frozen=True, slots=True)
class Blockchain:
    """
    Immutable blockchain with consensus mechanisms.

    Maintains ordered chain of blocks with validation and
    federation-specific consensus rules.
    """

    chain_id: ChainId = "default"
    genesis_block: Block = field(
        default_factory=lambda: Block.create_genesis("genesis")
    )
    blocks: tuple[Block, ...] = field(default_factory=tuple)
    consensus_config: ConsensusConfig = field(
        default_factory=lambda: ConsensusConfig(
            consensus_type=ConsensusType.PROOF_OF_AUTHORITY,
            block_time_target=10,
            difficulty_adjustment_interval=100,
            max_transactions_per_block=1000,
        )
    )
    crypto_config: CryptoConfig = field(
        default_factory=lambda: CryptoConfig(require_signatures=False)
    )

    def __post_init__(self) -> None:
        """Validate blockchain structure."""
        if not self.chain_id:
            raise ValueError("Chain ID cannot be empty")

        # Validate genesis block
        if not self.genesis_block.is_valid():
            raise ValueError("Invalid genesis block")

        # Validate chain continuity and block integrity
        if self.blocks:
            previous_block = self.genesis_block
            for block in self.blocks:
                if not block.is_valid(
                    previous_block,
                    current_time=block.timestamp,
                    crypto_config=self.crypto_config,
                ):
                    raise ValueError(f"Invalid block at height {block.height}")
                previous_block = block

    def get_height(self) -> BlockHeight:
        """Get current blockchain height."""
        if not self.blocks:
            return 0
        return self.blocks[-1].height

    def get_latest_block(self) -> Block:
        """Get the latest block in the chain."""
        if not self.blocks:
            return self.genesis_block
        return self.blocks[-1]

    def get_block_at_height(self, height: BlockHeight) -> Block | None:
        """Get block at specific height."""
        if height == 0:
            return self.genesis_block
        if height > self.get_height():
            return None

        # Blocks are ordered by height
        block_index = height - 1
        if 0 <= block_index < len(self.blocks):
            return self.blocks[block_index]
        return None

    def get_block_by_hash(self, block_hash: BlockHash) -> Block | None:
        """Find block by hash."""
        if self.genesis_block.get_block_hash() == block_hash:
            return self.genesis_block

        for block in self.blocks:
            if block.get_block_hash() == block_hash:
                return block
        return None

    def add_block(
        self, new_block: Block, current_time: Timestamp | None = None
    ) -> Blockchain:
        """Add new block to chain (if valid)."""
        if not self.can_add_block(new_block, current_time=current_time):
            raise ValueError("Invalid block: cannot be added to chain")

        # Create new chain with added block
        new_blocks = list(self.blocks)
        new_blocks.append(new_block)

        return Blockchain(
            chain_id=self.chain_id,
            genesis_block=self.genesis_block,
            blocks=tuple(new_blocks),
            consensus_config=self.consensus_config,
            crypto_config=self.crypto_config,
        )

    def validate_chain(self) -> bool:
        """Validate entire blockchain."""
        try:
            self.__post_init__()
            return True
        except ValueError:
            return False

    def find_transaction(
        self, transaction_id: TransactionId
    ) -> tuple[Block, Transaction] | None:
        """Find transaction in blockchain."""
        # Check genesis block
        for tx in self.genesis_block.transactions:
            if tx.transaction_id == transaction_id:
                return self.genesis_block, tx

        # Check other blocks
        for block in self.blocks:
            for tx in block.transactions:
                if tx.transaction_id == transaction_id:
                    return block, tx

        return None

    def get_balance(self, node_id: NodeId) -> int:
        """Calculate balance for a node (simple implementation)."""
        balance = 0

        # Process all transactions in chain
        all_blocks = [self.genesis_block] + list(self.blocks)
        for block in all_blocks:
            for tx in block.transactions:
                if tx.receiver == node_id:
                    balance += tx.fee  # Simplified: receiver gets fee
                if tx.sender == node_id:
                    balance -= tx.fee  # Sender pays fee

        return balance

    def get_total_work(self) -> int:
        """Calculate total proof-of-work for chain."""
        total_work = self.genesis_block.difficulty
        for block in self.blocks:
            total_work += block.difficulty
        return total_work

    def fork_at_block(self, block_hash: BlockHash) -> Blockchain:
        """Create fork starting from specified block."""
        fork_block = self.get_block_by_hash(block_hash)
        if fork_block is None:
            raise ValueError(f"Block {block_hash} not found")

        if fork_block == self.genesis_block:
            # Fork from genesis
            return Blockchain(
                chain_id=f"{self.chain_id}_fork",
                genesis_block=self.genesis_block,
                blocks=(),
                consensus_config=self.consensus_config,
            )

        # Find blocks up to fork point
        fork_height = fork_block.height
        fork_blocks = []
        for block in self.blocks:
            if block.height < fork_height:
                fork_blocks.append(block)
            elif block.height == fork_height:
                fork_blocks.append(block)
                break

        return Blockchain(
            chain_id=f"{self.chain_id}_fork",
            genesis_block=self.genesis_block,
            blocks=tuple(fork_blocks),
            consensus_config=self.consensus_config,
        )

    def resolve_fork(self, competing_chain: Blockchain) -> Blockchain:
        """Resolve fork using longest valid chain rule."""
        if not competing_chain.validate_chain():
            return self  # Keep current chain if competitor is invalid

        # Different genesis blocks cannot be resolved
        if (
            self.genesis_block.get_block_hash()
            != competing_chain.genesis_block.get_block_hash()
        ):
            return self

        # Use total work for resolution
        our_work = self.get_total_work()
        their_work = competing_chain.get_total_work()

        if their_work > our_work:
            return competing_chain
        elif their_work == our_work:
            # Tie-breaker: use chain with latest timestamp
            our_latest = self.get_latest_block()
            their_latest = competing_chain.get_latest_block()
            if their_latest.timestamp > our_latest.timestamp:
                return competing_chain

        return self

    def get_current_difficulty(self) -> Difficulty:
        """Get current mining difficulty."""
        # Simple difficulty adjustment
        if len(self.blocks) < self.consensus_config.difficulty_adjustment_interval:
            return self.consensus_config.difficulty_adjustment_interval

        # Calculate average block time over last interval
        interval = self.consensus_config.difficulty_adjustment_interval
        recent_blocks = self.blocks[-interval:]

        if len(recent_blocks) < 2:
            return 1

        time_span = recent_blocks[-1].timestamp - recent_blocks[0].timestamp
        average_time = time_span / (len(recent_blocks) - 1)
        target_time = self.consensus_config.block_time_target

        # Adjust difficulty
        current_difficulty = self.get_latest_block().difficulty
        if average_time < target_time * 0.5:
            return min(current_difficulty * 2, 100)  # Increase difficulty
        elif average_time > target_time * 2:
            return max(current_difficulty // 2, 1)  # Decrease difficulty
        else:
            return current_difficulty

    def can_add_block(
        self, block: Block, current_time: Timestamp | None = None
    ) -> bool:
        """Check if block can be added to chain."""
        if current_time is None:
            current_time = time.time()

        latest_block = self.get_latest_block()

        # Basic validation
        if not block.is_valid(
            latest_block, current_time, crypto_config=self.crypto_config
        ):
            return False

        # Consensus-specific validation
        if self.consensus_config.consensus_type == ConsensusType.PROOF_OF_WORK:
            if not block.verify_proof_of_work():
                return False

        # Check transaction limit
        return (
            not len(block.transactions)
            > self.consensus_config.max_transactions_per_block
        )

    def get_pending_transactions_limit(self) -> int:
        """Get maximum transactions for next block."""
        return self.consensus_config.max_transactions_per_block

    def get_vector_clock_state(self) -> VectorClock:
        """Get current vector clock state from latest block."""
        return self.get_latest_block().vector_clock

    def get_chain_summary(self) -> dict[str, Any]:
        """Get summary information about the chain."""
        latest_block = self.get_latest_block()
        total_transactions = sum(
            len(block.transactions)
            for block in [self.genesis_block] + list(self.blocks)
        )

        return {
            "chain_id": self.chain_id,
            "height": self.get_height(),
            "total_blocks": len(self.blocks) + 1,  # Include genesis
            "total_transactions": total_transactions,
            "total_work": self.get_total_work(),
            "latest_block_hash": latest_block.get_block_hash(),
            "latest_timestamp": latest_block.timestamp,
            "consensus_type": self.consensus_config.consensus_type.value,
        }

    def to_dict(self) -> dict[str, Any]:
        """Convert blockchain to dictionary for serialization."""
        return {
            "chain_id": self.chain_id,
            "genesis_block": self.genesis_block.to_dict(),
            "blocks": [block.to_dict() for block in self.blocks],
            "consensus_config": {
                "consensus_type": self.consensus_config.consensus_type.value,
                "block_time_target": self.consensus_config.block_time_target,
                "difficulty_adjustment_interval": self.consensus_config.difficulty_adjustment_interval,
                "max_transactions_per_block": self.consensus_config.max_transactions_per_block,
                "minimum_stake": self.consensus_config.minimum_stake,
                "authority_threshold": self.consensus_config.authority_threshold,
            },
            "crypto_config": {
                "hash_algorithm": self.crypto_config.hash_algorithm,
                "signature_algorithm": self.crypto_config.signature_algorithm,
                "key_length": self.crypto_config.key_length,
                "require_signatures": self.crypto_config.require_signatures,
            },
            "summary": self.get_chain_summary(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Blockchain:
        """Create blockchain from dictionary."""
        consensus_config = ConsensusConfig(
            consensus_type=ConsensusType(data["consensus_config"]["consensus_type"]),
            block_time_target=data["consensus_config"]["block_time_target"],
            difficulty_adjustment_interval=data["consensus_config"][
                "difficulty_adjustment_interval"
            ],
            max_transactions_per_block=data["consensus_config"][
                "max_transactions_per_block"
            ],
            minimum_stake=data["consensus_config"].get("minimum_stake", 0),
            authority_threshold=data["consensus_config"].get(
                "authority_threshold", 0.67
            ),
        )
        crypto_payload = data.get("crypto_config") or {}
        crypto_config = CryptoConfig(
            hash_algorithm=crypto_payload.get("hash_algorithm", "sha256"),
            signature_algorithm=crypto_payload.get("signature_algorithm", "ed25519"),
            key_length=crypto_payload.get("key_length", 256),
            require_signatures=crypto_payload.get("require_signatures", False),
        )

        return cls(
            chain_id=data["chain_id"],
            genesis_block=Block.from_dict(data["genesis_block"]),
            blocks=tuple(Block.from_dict(block) for block in data["blocks"]),
            consensus_config=consensus_config,
            crypto_config=crypto_config,
        )

    @classmethod
    def create_new_chain(
        cls,
        chain_id: ChainId,
        genesis_miner: NodeId,
        consensus_config: ConsensusConfig | None = None,
        crypto_config: CryptoConfig | None = None,
        initial_transactions: tuple[Transaction, ...] = (),
    ) -> Blockchain:
        """Create new blockchain with genesis block."""
        if consensus_config is None:
            consensus_config = ConsensusConfig(
                consensus_type=ConsensusType.PROOF_OF_AUTHORITY,
                block_time_target=10,
                difficulty_adjustment_interval=100,
                max_transactions_per_block=1000,
            )
        if crypto_config is None:
            crypto_config = CryptoConfig(require_signatures=False)

        genesis_block = Block.create_genesis(genesis_miner, initial_transactions)

        return cls(
            chain_id=chain_id,
            genesis_block=genesis_block,
            blocks=(),
            consensus_config=consensus_config,
            crypto_config=crypto_config,
        )

    def __len__(self) -> int:
        """Get number of blocks in chain (including genesis)."""
        return len(self.blocks) + 1

    def __iter__(self):
        """Iterate over all blocks in chain."""
        yield self.genesis_block
        yield from self.blocks

    def __str__(self) -> str:
        """String representation of blockchain."""
        return (
            f"Blockchain(chain_id={self.chain_id}, "
            f"height={self.get_height()}, "
            f"blocks={len(self)}, "
            f"consensus={self.consensus_config.consensus_type.value})"
        )


# Hypothesis strategies for property-based testing


def chain_id_strategy() -> st.SearchStrategy[ChainId]:
    """Generate valid chain IDs."""
    return st.text(
        min_size=1,
        max_size=50,
        alphabet=st.characters(
            whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
        ),
    )


def consensus_config_strategy() -> st.SearchStrategy[ConsensusConfig]:
    """Generate valid consensus configurations."""
    return st.builds(
        ConsensusConfig,
        consensus_type=st.sampled_from(ConsensusType),
        block_time_target=st.integers(min_value=1, max_value=60),
        difficulty_adjustment_interval=st.integers(min_value=10, max_value=1000),
        max_transactions_per_block=st.integers(min_value=1, max_value=10000),
    )


def blockchain_strategy() -> st.SearchStrategy[Blockchain]:
    """Generate valid Blockchain instances for testing."""
    return st.builds(
        Blockchain,
        chain_id=chain_id_strategy(),
        genesis_block=genesis_block_strategy(),
        blocks=st.lists(block_strategy(), max_size=10).map(tuple),
        consensus_config=consensus_config_strategy(),
    ).filter(lambda chain: chain.validate_chain())


def simple_blockchain_strategy() -> st.SearchStrategy[Blockchain]:
    """Generate simple valid blockchains for testing."""
    return st.builds(
        Blockchain.create_new_chain,
        chain_id=chain_id_strategy(),
        genesis_miner=st.text(min_size=1, max_size=20),
        consensus_config=consensus_config_strategy(),
        initial_transactions=st.lists(transaction_strategy(), max_size=3).map(tuple),
    )
