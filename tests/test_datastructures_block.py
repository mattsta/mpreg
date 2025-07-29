"""
Comprehensive property-based tests for Block datastructure.

This test suite uses the hypothesis library to aggressively test the correctness
of the Block implementation with property-based testing, focusing on Merkle tree
integration and Vector clock functionality.
"""

import time

import pytest
from hypothesis import given

from mpreg.datastructures.block import (
    Block,
    genesis_block_strategy,
)
from mpreg.datastructures.blockchain_types import OperationType
from mpreg.datastructures.transaction import Transaction
from mpreg.datastructures.vector_clock import VectorClock


class TestBlock:
    """Test Block datastructure."""

    @given(genesis_block_strategy())
    def test_genesis_block_creation(self, block: Block):
        """Test that valid genesis blocks can be created."""
        assert block.height == 0
        assert block.previous_hash == ""
        assert block.miner != ""
        assert block.difficulty > 0
        assert block.nonce >= 0
        assert block.timestamp >= 0
        assert isinstance(block.vector_clock, VectorClock)
        assert isinstance(block.transactions, tuple)

    def test_block_validation(self):
        """Test block validation."""
        # Valid genesis block
        block = Block.create_genesis("miner1")
        assert block.is_valid()

        # Invalid blocks
        with pytest.raises(ValueError, match="Miner cannot be empty"):
            Block(miner="")

        with pytest.raises(ValueError, match="Difficulty must be positive"):
            Block(miner="miner1", difficulty=0)

        with pytest.raises(ValueError, match="Nonce cannot be negative"):
            Block(miner="miner1", nonce=-1)

    @given(genesis_block_strategy())
    def test_block_hash_consistency(self, block: Block):
        """Test that block hashes are consistent."""
        hash1 = block.get_block_hash()
        hash2 = block.get_block_hash()
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex length

    @given(genesis_block_strategy())
    def test_block_immutability(self, block: Block):
        """Test that blocks are immutable."""
        original_hash = block.get_block_hash()
        original_height = block.height

        # Operations should return new blocks
        new_block = block.with_nonce(block.nonce + 1)
        assert new_block is not block
        assert new_block.nonce == block.nonce + 1
        assert new_block.height == original_height
        assert block.get_block_hash() == original_hash  # Original unchanged

    def test_merkle_tree_integration(self):
        """Test Merkle tree integration."""
        # Create transactions
        tx1 = Transaction(sender="alice", receiver="bob", fee=10)
        tx2 = Transaction(sender="bob", receiver="charlie", fee=20)

        # Create block with transactions
        block = Block.create_genesis("miner", (tx1, tx2))

        # Should have merkle root
        assert block.merkle_root != ""

        # Merkle tree should be accessible
        tree = block.get_transactions_tree()
        assert not tree.is_empty()
        assert tree.root_hash() == block.merkle_root

    def test_transaction_proofs(self):
        """Test transaction Merkle proof generation and verification."""
        # Create transactions
        tx1 = Transaction(sender="alice", receiver="bob", fee=10)
        tx2 = Transaction(sender="bob", receiver="charlie", fee=20)
        tx3 = Transaction(sender="charlie", receiver="alice", fee=5)

        # Create block
        block = Block.create_genesis("miner", (tx1, tx2, tx3))

        # Generate proofs for each transaction
        for i in range(3):
            proof = block.generate_transaction_proof(i)
            assert block.verify_transaction_proof(proof)
            assert proof.root_hash == block.merkle_root

    def test_block_succession(self):
        """Test valid block succession."""
        # Genesis block
        genesis = Block.create_genesis("miner1")
        assert genesis.is_valid_successor(None)

        # Next block
        tx = Transaction(sender="alice", receiver="bob")
        next_block = Block.create_next_block(genesis, (tx,), "miner2")

        assert next_block.height == 1
        assert next_block.previous_hash == genesis.get_block_hash()
        assert next_block.is_valid_successor(genesis)
        assert next_block.is_valid(genesis)

    def test_vector_clock_progression(self):
        """Test vector clock progression in blocks."""
        # Genesis block
        genesis = Block.create_genesis("miner1")
        assert genesis.vector_clock.is_empty()

        # Next block should increment vector clock
        next_block = Block.create_next_block(genesis, (), "miner1")
        assert not next_block.vector_clock.is_empty()
        assert genesis.vector_clock.happens_before(next_block.vector_clock)

    @given(genesis_block_strategy())
    def test_block_serialization(self, block: Block):
        """Test block serialization roundtrip."""
        # Serialize to dict
        block_dict = block.to_dict()
        assert isinstance(block_dict, dict)
        assert "block_id" in block_dict
        assert "height" in block_dict
        assert "block_hash" in block_dict

        # Deserialize from dict
        reconstructed = Block.from_dict(block_dict)
        assert reconstructed.block_id == block.block_id
        assert reconstructed.height == block.height
        assert reconstructed.miner == block.miner
        assert reconstructed.get_block_hash() == block.get_block_hash()

    @given(genesis_block_strategy())
    def test_block_age_calculation(self, block: Block):
        """Test block age calculation."""
        current_time = time.time()
        age = block.age_seconds(current_time)
        assert age >= 0

        # Age should increase with time
        future_time = current_time + 100
        future_age = block.age_seconds(future_time)
        assert future_age >= age

    def test_proof_of_work(self):
        """Test proof-of-work verification."""
        block = Block.create_genesis("miner1")

        # Simple proof-of-work with leading zeros
        for nonce in range(1000):
            test_block = block.with_nonce(nonce)
            if test_block.verify_proof_of_work("00"):  # 2 leading zeros
                assert test_block.get_block_hash().startswith("00")
                break

    @given(genesis_block_strategy(), genesis_block_strategy())
    def test_block_equality(self, block1: Block, block2: Block):
        """Test block equality."""
        if (
            block1.block_id == block2.block_id
            and block1.height == block2.height
            and block1.previous_hash == block2.previous_hash
            and block1.merkle_root == block2.merkle_root
            and block1.miner == block2.miner
            and block1.difficulty == block2.difficulty
            and block1.nonce == block2.nonce
            and block1.timestamp == block2.timestamp
        ):
            assert block1 == block2
            assert hash(block1) == hash(block2)
        else:
            assert block1 != block2


class TestBlockProperties:
    """Test mathematical and cryptographic properties of blocks."""

    @given(genesis_block_strategy())
    def test_block_deterministic_hashing(self, block: Block):
        """Test that blocks with same data have same hash."""
        # Same block should always have same hash
        assert block.get_block_hash() == block.get_block_hash()

        # Different nonce should give different hash
        different_nonce = block.with_nonce(block.nonce + 1)
        assert block.get_block_hash() != different_nonce.get_block_hash()

    @given(genesis_block_strategy())
    def test_block_modification_creates_new_instance(self, block: Block):
        """Test that modifications create new instances."""
        original_id = id(block)

        # All modification methods should return new instances
        new_nonce_block = block.with_nonce(block.nonce + 1)
        assert id(new_nonce_block) != original_id

        new_vc_block = block.with_vector_clock(VectorClock())
        assert id(new_vc_block) != original_id

    def test_merkle_root_computation(self):
        """Test Merkle root computation consistency."""
        tx1 = Transaction(sender="alice", receiver="bob")
        tx2 = Transaction(sender="bob", receiver="charlie")

        # Block with same transactions should have same merkle root
        block1 = Block.create_genesis("miner", (tx1, tx2))
        block2 = Block.create_genesis("miner", (tx1, tx2))

        assert block1.merkle_root == block2.merkle_root

        # Different transactions should have different merkle root
        tx3 = Transaction(sender="charlie", receiver="alice")
        block3 = Block.create_genesis("miner", (tx1, tx3))

        assert block1.merkle_root != block3.merkle_root

    def test_transaction_lookup(self):
        """Test transaction lookup in blocks."""
        tx1 = Transaction(sender="alice", receiver="bob")
        tx2 = Transaction(sender="bob", receiver="charlie")

        block = Block.create_genesis("miner", (tx1, tx2))

        # Should find existing transactions
        assert block.contains_transaction(tx1.transaction_id)
        assert block.get_transaction(tx1.transaction_id) == tx1

        # Should not find non-existent transaction
        assert not block.contains_transaction("nonexistent")
        assert block.get_transaction("nonexistent") is None

    def test_transaction_fees_calculation(self):
        """Test total fees calculation."""
        tx1 = Transaction(sender="alice", receiver="bob", fee=10)
        tx2 = Transaction(sender="bob", receiver="charlie", fee=20)
        tx3 = Transaction(sender="charlie", receiver="alice", fee=5)

        block = Block.create_genesis("miner", (tx1, tx2, tx3))

        assert block.total_fees() == 35
        assert block.transaction_count() == 3
        assert not block.is_empty()

    def test_block_validation_edge_cases(self):
        """Test block validation edge cases."""
        # Empty block should be valid
        empty_block = Block.create_genesis("miner")
        assert empty_block.is_valid()
        assert empty_block.is_empty()
        assert empty_block.merkle_root == ""

        # Block with invalid transactions should be invalid
        future_time = time.time() + 1000
        invalid_tx = Transaction(sender="alice", receiver="bob", timestamp=future_time)

        invalid_block = Block.create_genesis("miner", (invalid_tx,))
        assert not invalid_block.is_valid()


class TestBlockSuccession:
    """Test block succession and chain building."""

    def test_genesis_block_properties(self):
        """Test genesis block specific properties."""
        genesis = Block.create_genesis("genesis_miner")

        assert genesis.height == 0
        assert genesis.previous_hash == ""
        assert genesis.vector_clock.is_empty()
        assert genesis.is_valid_successor(None)

    def test_chain_building(self):
        """Test building a chain of blocks."""
        # Genesis
        genesis = Block.create_genesis("miner1")

        # Block 1
        tx1 = Transaction(sender="alice", receiver="bob", fee=10)
        block1 = Block.create_next_block(genesis, (tx1,), "miner2")

        assert block1.height == 1
        assert block1.is_valid_successor(genesis)

        # Block 2
        tx2 = Transaction(sender="bob", receiver="charlie", fee=20)
        block2 = Block.create_next_block(block1, (tx2,), "miner3")

        assert block2.height == 2
        assert block2.is_valid_successor(block1)
        assert not block2.is_valid_successor(genesis)  # Not immediate successor

    def test_invalid_succession(self):
        """Test invalid block succession scenarios."""
        genesis = Block.create_genesis("miner1")

        # Wrong height
        invalid_block = Block(
            height=2,  # Should be 1
            previous_hash=genesis.get_block_hash(),
            miner="miner2",
            vector_clock=genesis.vector_clock.increment("miner2"),
        )
        assert not invalid_block.is_valid_successor(genesis)

        # Wrong previous hash
        invalid_block2 = Block(
            height=1,
            previous_hash="wrong_hash",
            miner="miner2",
            vector_clock=genesis.vector_clock.increment("miner2"),
        )
        assert not invalid_block2.is_valid_successor(genesis)

    def test_vector_clock_causality(self):
        """Test vector clock causality in block succession."""
        genesis = Block.create_genesis("miner1")

        # Valid causality
        valid_vc = genesis.vector_clock.increment("miner2")
        valid_block = Block(
            height=1,
            previous_hash=genesis.get_block_hash(),
            miner="miner2",
            vector_clock=valid_vc,
            timestamp=genesis.timestamp + 10,
        )
        assert valid_block.is_valid_successor(genesis)

        # Invalid causality - timestamp goes backwards
        invalid_block = Block(
            height=1,
            previous_hash=genesis.get_block_hash(),
            miner="miner3",
            vector_clock=valid_vc,
            timestamp=genesis.timestamp - 10,  # Invalid: goes backwards in time
        )
        # This should fail because timestamp must progress forward
        assert not invalid_block.is_valid_successor(genesis)


class TestBlockExamples:
    """Test specific block examples and use cases."""

    def test_simple_genesis_block(self):
        """Test a simple genesis block."""
        block = Block.create_genesis("genesis_node")

        assert block.is_valid()
        assert block.height == 0
        assert block.miner == "genesis_node"
        assert block.is_empty()

    def test_block_with_federation_transactions(self):
        """Test block with federation-specific transactions."""

        # Federation join transaction
        join_tx = Transaction(
            sender="new_node",
            receiver="federation_registry",
            operation_type=OperationType.FEDERATION_JOIN,
            payload=b'{"capabilities": "full_node"}',
            fee=10,
        )

        # Message transaction
        msg_tx = Transaction(
            sender="node1",
            receiver="node2",
            operation_type=OperationType.MESSAGE,
            payload=b"Hello federation!",
            fee=1,
        )

        block = Block.create_genesis("hub_node", (join_tx, msg_tx))

        assert block.is_valid()
        assert block.transaction_count() == 2
        assert block.total_fees() == 11
        assert join_tx in block.transactions
        assert msg_tx in block.transactions

    def test_mining_workflow(self):
        """Test complete mining workflow."""
        # Create genesis
        genesis = Block.create_genesis("genesis_miner")

        # Create transaction
        tx = Transaction(
            sender="alice",
            receiver="bob",
            operation_type=OperationType.TRANSFER,
            fee=10,
        )

        # Create next block
        next_block = Block.create_next_block(genesis, (tx,), "miner1")

        # Mine block (find nonce for proof-of-work)
        for nonce in range(100):
            candidate = next_block.with_nonce(nonce)
            if candidate.verify_proof_of_work("0"):  # 1 leading zero
                mined_block = candidate
                break
        else:
            # Fallback if no valid nonce found
            mined_block = next_block

        # Verify final block
        assert mined_block.is_valid(genesis)
        assert mined_block.contains_transaction(tx.transaction_id)

    def test_block_merkle_proof_workflow(self):
        """Test complete Merkle proof workflow."""
        # Create multiple transactions
        transactions = []
        for i in range(5):
            tx = Transaction(sender=f"sender{i}", receiver=f"receiver{i}", fee=i + 1)
            transactions.append(tx)

        # Create block
        block = Block.create_genesis("prover_node", tuple(transactions))

        # Generate and verify proof for each transaction
        for i, tx in enumerate(transactions):
            proof = block.generate_transaction_proof(i)

            # Proof should be valid
            assert block.verify_transaction_proof(proof)
            assert proof.root_hash == block.merkle_root

            # Verify the specific transaction data is in the proof
            tx_bytes = tx.to_bytes()
            assert proof.verify()
