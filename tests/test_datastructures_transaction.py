"""
Comprehensive property-based tests for Transaction datastructure.

This test suite uses the hypothesis library to aggressively test the correctness
of the centralized Transaction implementation with property-based testing.
"""

import time

import pytest
from hypothesis import given

from mpreg.datastructures.transaction import (
    OperationType,
    Transaction,
    TransactionPool,
    transaction_pool_strategy,
    transaction_strategy,
)


class TestTransaction:
    """Test Transaction datastructure."""

    @given(transaction_strategy())
    def test_transaction_creation(self, transaction: Transaction):
        """Test that valid transactions can be created."""
        assert transaction.transaction_id
        assert transaction.sender
        assert transaction.receiver
        assert isinstance(transaction.operation_type, OperationType)
        assert isinstance(transaction.payload, bytes)
        assert transaction.timestamp >= 0
        assert transaction.fee >= 0
        assert transaction.nonce >= 0
        assert isinstance(transaction.signature, bytes)

    def test_transaction_validation(self):
        """Test transaction validation."""
        # Valid transaction
        tx = Transaction(
            sender="alice",
            receiver="bob",
            operation_type=OperationType.TRANSFER,
            payload=b"test_data",
        )
        assert tx.is_valid()

        # Invalid transactions
        with pytest.raises(ValueError, match="Sender cannot be empty"):
            Transaction(sender="", receiver="bob")

        with pytest.raises(ValueError, match="Receiver cannot be empty"):
            Transaction(sender="alice", receiver="")

        with pytest.raises(ValueError, match="Fee cannot be negative"):
            Transaction(sender="alice", receiver="bob", fee=-1)

        with pytest.raises(ValueError, match="Nonce cannot be negative"):
            Transaction(sender="alice", receiver="bob", nonce=-1)

    @given(transaction_strategy())
    def test_transaction_hash_consistency(self, transaction: Transaction):
        """Test that transaction hashes are consistent."""
        hash1 = transaction.get_hash()
        hash2 = transaction.get_hash()
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex length

    @given(transaction_strategy())
    def test_transaction_immutability(self, transaction: Transaction):
        """Test that transactions are immutable."""
        original_hash = transaction.get_hash()
        original_sender = transaction.sender

        # Operations should return new transactions
        new_tx = transaction.with_nonce(transaction.nonce + 1)
        assert new_tx is not transaction
        assert new_tx.nonce == transaction.nonce + 1
        assert new_tx.sender == original_sender
        assert transaction.get_hash() == original_hash  # Original unchanged

    def test_transaction_signing(self):
        """Test transaction signing and verification."""
        tx = Transaction(
            sender="alice",
            receiver="bob",
            operation_type=OperationType.TRANSFER,
            payload=b"test_data",
        )

        # Sign transaction
        private_key = b"private_key_alice"
        signed_tx = tx.sign(private_key)

        assert signed_tx.signature != b""
        assert len(signed_tx.signature) == 32  # SHA-256 digest

        # Verify signature
        public_key = (
            b"private_key_alice"  # In real implementation, this would be derived
        )
        assert signed_tx.verify_signature(public_key)

        # Wrong key should fail
        wrong_key = b"wrong_key"
        assert not signed_tx.verify_signature(wrong_key)

    @given(transaction_strategy())
    def test_transaction_serialization(self, transaction: Transaction):
        """Test transaction serialization roundtrip."""
        # Serialize to dict
        tx_dict = transaction.to_dict()
        assert isinstance(tx_dict, dict)
        assert "transaction_id" in tx_dict
        assert "sender" in tx_dict
        assert "hash" in tx_dict

        # Deserialize from dict
        reconstructed = Transaction.from_dict(tx_dict)
        assert reconstructed.transaction_id == transaction.transaction_id
        assert reconstructed.sender == transaction.sender
        assert reconstructed.receiver == transaction.receiver
        assert reconstructed.operation_type == transaction.operation_type
        assert reconstructed.payload == transaction.payload
        assert reconstructed.get_hash() == transaction.get_hash()

    @given(transaction_strategy())
    def test_transaction_age_calculation(self, transaction: Transaction):
        """Test transaction age calculation."""
        current_time = time.time()
        age = transaction.age_seconds(current_time)
        assert age >= 0

        # Age should increase with time
        future_time = current_time + 100
        future_age = transaction.age_seconds(future_time)
        assert future_age >= age

    def test_transaction_expiry(self):
        """Test transaction expiry logic."""
        current_time = time.time()

        # Recent transaction should not be expired
        recent_tx = Transaction(
            sender="alice",
            receiver="bob",
            timestamp=current_time - 10,  # 10 seconds ago
        )
        assert not recent_tx.is_expired(60, current_time)  # 60 second timeout

        # Old transaction should be expired
        old_tx = Transaction(
            sender="alice",
            receiver="bob",
            timestamp=current_time - 120,  # 2 minutes ago
        )
        assert old_tx.is_expired(60, current_time)  # 60 second timeout

    @given(transaction_strategy(), transaction_strategy())
    def test_transaction_equality(self, tx1: Transaction, tx2: Transaction):
        """Test transaction equality."""
        if (
            tx1.transaction_id == tx2.transaction_id
            and tx1.sender == tx2.sender
            and tx1.receiver == tx2.receiver
            and tx1.operation_type == tx2.operation_type
            and tx1.payload == tx2.payload
            and tx1.timestamp == tx2.timestamp
            and tx1.fee == tx2.fee
            and tx1.nonce == tx2.nonce
            and tx1.signature == tx2.signature
        ):
            assert tx1 == tx2
            assert hash(tx1) == hash(tx2)
        else:
            assert tx1 != tx2


class TestTransactionPool:
    """Test TransactionPool datastructure."""

    @given(transaction_pool_strategy())
    def test_transaction_pool_creation(self, pool: TransactionPool):
        """Test that valid transaction pools can be created."""
        assert pool.max_size > 0
        assert pool.size() <= pool.max_size
        assert isinstance(pool.transactions, tuple)

    def test_transaction_pool_validation(self):
        """Test transaction pool validation."""
        # Valid pool
        pool = TransactionPool(max_size=100)
        assert pool.size() == 0
        assert pool.is_empty()

        # Invalid max size
        with pytest.raises(ValueError, match="Max size must be positive"):
            TransactionPool(max_size=0)

        with pytest.raises(ValueError, match="Max size must be positive"):
            TransactionPool(max_size=-1)

    @given(transaction_pool_strategy(), transaction_strategy())
    def test_transaction_pool_add_remove(self, pool: TransactionPool, tx: Transaction):
        """Test adding and removing transactions."""
        initial_size = pool.size()

        # Add transaction
        new_pool = pool.add_transaction(tx)

        # Check if transaction was already in pool or if pool hit max_size
        if tx not in pool.transactions:
            # Pool may hit size limit
            if initial_size < pool.max_size:
                assert new_pool.size() == initial_size + 1
                assert tx in new_pool.transactions
            else:
                # Pool is at max size, may drop oldest
                assert new_pool.size() <= pool.max_size
                assert tx in new_pool.transactions  # New tx should be added
        else:
            assert new_pool.size() == initial_size  # Already in pool

        # Remove transaction (only if it's actually in the pool)
        if tx in new_pool.transactions:
            # Count how many transactions have this ID before removal
            transactions_with_id = sum(
                1
                for t in new_pool.transactions
                if t.transaction_id == tx.transaction_id
            )
            removed_pool = new_pool.remove_transaction(tx.transaction_id)

            # All transactions with this ID should be removed
            assert removed_pool.size() == new_pool.size() - transactions_with_id
            assert not removed_pool.contains(tx.transaction_id)

    def test_transaction_pool_ordering(self):
        """Test transaction pool fee-based ordering."""
        # Create transactions with different fees
        tx1 = Transaction(sender="alice", receiver="bob", fee=10)
        tx2 = Transaction(sender="bob", receiver="charlie", fee=20)
        tx3 = Transaction(sender="charlie", receiver="alice", fee=5)

        pool = TransactionPool()
        pool = pool.add_transaction(tx1)
        pool = pool.add_transaction(tx2)
        pool = pool.add_transaction(tx3)

        # Get transactions by fee (highest first)
        ordered_txs = pool.get_transactions_by_fee(3)
        assert len(ordered_txs) == 3
        assert ordered_txs[0].fee == 20  # tx2
        assert ordered_txs[1].fee == 10  # tx1
        assert ordered_txs[2].fee == 5  # tx3

    def test_transaction_pool_size_limit(self):
        """Test transaction pool size limits."""
        max_size = 3
        pool = TransactionPool(max_size=max_size)

        # Add transactions up to limit
        txs = [
            Transaction(sender=f"sender{i}", receiver="receiver", fee=i)
            for i in range(5)
        ]

        for tx in txs:
            pool = pool.add_transaction(tx)

        # Pool should be limited to max_size
        assert pool.size() <= max_size

        # Should contain the most recent transactions
        final_txs = pool.transactions
        assert len(final_txs) <= max_size

    @given(transaction_pool_strategy())
    def test_transaction_pool_immutability(self, pool: TransactionPool):
        """Test transaction pool immutability."""
        original_size = pool.size()

        # Create a new transaction
        tx = Transaction(sender="test", receiver="test")

        # Adding should return new pool
        new_pool = pool.add_transaction(tx)

        # Original pool should be unchanged
        assert pool.size() == original_size
        assert new_pool is not pool

    def test_transaction_pool_validation_filtering(self):
        """Test filtering valid transactions."""
        current_time = time.time()

        # Create mix of valid and invalid transactions
        valid_tx = Transaction(
            sender="alice", receiver="bob", timestamp=current_time - 10
        )

        future_tx = Transaction(
            sender="bob",
            receiver="charlie",
            timestamp=current_time + 1000,  # Far future
        )

        pool = TransactionPool()
        pool = pool.add_transaction(valid_tx)
        pool = pool.add_transaction(future_tx)

        # Get only valid transactions
        valid_txs = pool.get_valid_transactions(current_time)

        assert valid_tx in valid_txs
        assert future_tx not in valid_txs


class TestTransactionProperties:
    """Test mathematical and cryptographic properties of transactions."""

    @given(transaction_strategy())
    def test_transaction_deterministic_hashing(self, transaction: Transaction):
        """Test that transactions with same data have same hash."""
        # Same transaction should always have same hash
        assert transaction.get_hash() == transaction.get_hash()

        # Different nonce should give different hash
        different_nonce = transaction.with_nonce(transaction.nonce + 1)
        assert transaction.get_hash() != different_nonce.get_hash()

    @given(transaction_strategy())
    def test_transaction_to_bytes_consistency(self, transaction: Transaction):
        """Test that to_bytes() is consistent."""
        bytes1 = transaction.to_bytes()
        bytes2 = transaction.to_bytes()
        assert bytes1 == bytes2
        assert isinstance(bytes1, bytes)

    @given(transaction_strategy())
    def test_transaction_modification_creates_new_instance(
        self, transaction: Transaction
    ):
        """Test that modifications create new instances."""
        original_id = id(transaction)

        # All modification methods should return new instances
        new_nonce_tx = transaction.with_nonce(transaction.nonce + 1)
        assert id(new_nonce_tx) != original_id

        new_sig_tx = transaction.with_signature(b"new_signature")
        assert id(new_sig_tx) != original_id

    def test_transaction_validation_edge_cases(self):
        """Test transaction validation edge cases."""
        current_time = time.time()

        # Transaction slightly in future should be valid
        near_future_tx = Transaction(
            sender="alice",
            receiver="bob",
            timestamp=current_time + 100,  # 100 seconds in future
        )
        assert near_future_tx.is_valid(current_time)

        # Transaction far in future should be invalid
        far_future_tx = Transaction(
            sender="alice",
            receiver="bob",
            timestamp=current_time + 1000,  # 1000 seconds in future
        )
        assert not far_future_tx.is_valid(current_time)


class TestTransactionExamples:
    """Test specific transaction examples and use cases."""

    def test_simple_transfer_transaction(self):
        """Test a simple transfer transaction."""
        tx = Transaction(
            sender="alice",
            receiver="bob",
            operation_type=OperationType.TRANSFER,
            payload=b"send 10 coins",
            fee=1,
        )

        assert tx.is_valid()
        assert tx.operation_type == OperationType.TRANSFER
        assert tx.payload == b"send 10 coins"
        assert tx.fee == 1

    def test_federation_join_transaction(self):
        """Test a federation join transaction."""
        tx = Transaction(
            sender="new_node",
            receiver="federation_registry",
            operation_type=OperationType.FEDERATION_JOIN,
            payload=b'{"node_info": "capabilities"}',
            fee=10,
        )

        assert tx.is_valid()
        assert tx.operation_type == OperationType.FEDERATION_JOIN
        assert b"node_info" in tx.payload

    def test_message_transaction(self):
        """Test a message transaction."""
        tx = Transaction(
            sender="sender_node",
            receiver="receiver_node",
            operation_type=OperationType.MESSAGE,
            payload=b"Hello, federation!",
            fee=1,
        )

        assert tx.is_valid()
        assert tx.operation_type == OperationType.MESSAGE
        assert tx.payload == b"Hello, federation!"

    def test_transaction_workflow(self):
        """Test complete transaction workflow."""
        # Create transaction
        tx = Transaction(
            sender="alice", receiver="bob", operation_type=OperationType.TRANSFER
        )

        # Add nonce for uniqueness
        tx_with_nonce = tx.with_nonce(12345)
        assert tx_with_nonce.nonce == 12345

        # Sign transaction
        private_key = b"alice_private_key"
        signed_tx = tx_with_nonce.sign(private_key)
        assert signed_tx.signature != b""

        # Verify signature
        public_key = b"alice_private_key"  # Simplified
        assert signed_tx.verify_signature(public_key)

        # Check final transaction is valid
        assert signed_tx.is_valid()
