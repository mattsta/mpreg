"""
Transaction datastructure for blockchain operations.

This module provides an immutable Transaction class with cryptographic
signatures and comprehensive validation for distributed federation operations.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from hypothesis import strategies as st

from .blockchain_crypto import (
    derive_public_key,
    public_key_length,
    sign_payload,
    signature_length,
)
from .blockchain_crypto import (
    verify_signature as verify_payload_signature,
)
from .blockchain_types import (
    CryptoConfig,
    DigitalSignature,
    NodeId,
    Nonce,
    OperationType,
    PublicKey,
    Timestamp,
    TransactionFee,
    TransactionHash,
    TransactionId,
    TransactionPayload,
    compute_transaction_hash,
    current_timestamp,
    generate_transaction_id,
)


@dataclass(frozen=True, slots=True)
class Transaction:
    """
    Immutable transaction with cryptographic signature.

    Represents a single operation in the federated blockchain system.
    All transactions are cryptographically signed and tamper-evident.
    """

    transaction_id: TransactionId = field(default_factory=generate_transaction_id)
    sender: NodeId = ""
    receiver: NodeId = ""
    operation_type: OperationType = OperationType.TRANSFER
    payload: TransactionPayload = b""
    timestamp: Timestamp = field(default_factory=current_timestamp)
    fee: TransactionFee = 1
    nonce: Nonce = 0
    signature: DigitalSignature = b""
    public_key: PublicKey = b""
    signature_algorithm: str = "ed25519"

    def __post_init__(self) -> None:
        """Validate transaction fields."""
        if not self.transaction_id:
            raise ValueError("Transaction ID cannot be empty")
        if not self.sender:
            raise ValueError("Sender cannot be empty")
        if not self.receiver:
            raise ValueError("Receiver cannot be empty")
        if self.fee < 0:
            raise ValueError("Fee cannot be negative")
        if self.nonce < 0:
            raise ValueError("Nonce cannot be negative")
        if self.timestamp < 0:
            raise ValueError("Timestamp cannot be negative")
        _ = signature_length(self.signature_algorithm)
        if self.signature and not self.public_key:
            raise ValueError("Public key required when signature is present")
        if self.public_key:
            expected_public_key_length = public_key_length(self.signature_algorithm)
            if len(self.public_key) != expected_public_key_length:
                raise ValueError("Public key length is invalid")
        if self.signature:
            expected_signature_length = signature_length(self.signature_algorithm)
            if len(self.signature) != expected_signature_length:
                raise ValueError("Signature length is invalid")

    def get_hash(self) -> TransactionHash:
        """Compute deterministic hash of transaction data (excluding signature)."""
        return compute_transaction_hash(
            self.sender,
            self.receiver,
            self.operation_type,
            self.payload,
            self.timestamp,
            self.nonce,
        )

    def to_bytes(self) -> bytes:
        """Convert transaction to bytes for hashing and signatures."""
        data = {
            "transaction_id": self.transaction_id,
            "sender": self.sender,
            "receiver": self.receiver,
            "operation_type": self.operation_type.value,
            "payload": self.payload.hex(),
            "timestamp": self.timestamp,
            "fee": self.fee,
            "nonce": self.nonce,
        }
        # Create deterministic byte representation
        sorted_items = sorted(data.items())
        data_str = ":".join(f"{k}={v}" for k, v in sorted_items)
        return data_str.encode("utf-8")

    def sign(self, private_key: bytes, *, algorithm: str | None = None) -> Transaction:
        """Create new transaction with cryptographic signature."""
        selected_algorithm = algorithm or self.signature_algorithm
        signature = sign_payload(self.to_bytes(), private_key, selected_algorithm)
        public_key = derive_public_key(private_key, selected_algorithm)

        return Transaction(
            transaction_id=self.transaction_id,
            sender=self.sender,
            receiver=self.receiver,
            operation_type=self.operation_type,
            payload=self.payload,
            timestamp=self.timestamp,
            fee=self.fee,
            nonce=self.nonce,
            signature=signature,
            public_key=public_key,
            signature_algorithm=selected_algorithm,
        )

    def verify_signature(
        self,
        public_key: PublicKey | None = None,
        *,
        algorithm: str | None = None,
    ) -> bool:
        """Verify transaction signature against public key."""
        if not self.signature:
            return False
        key = public_key or self.public_key
        if not key:
            return False
        selected_algorithm = algorithm or self.signature_algorithm
        return verify_payload_signature(
            self.to_bytes(), self.signature, key, selected_algorithm
        )

    def is_valid(
        self,
        current_time: Timestamp | None = None,
        *,
        crypto_config: CryptoConfig | None = None,
        public_key: PublicKey | None = None,
    ) -> bool:
        """Check if transaction is valid."""
        if current_time is None:
            current_time = time.time()

        # Basic validation
        if not self.transaction_id or not self.sender or not self.receiver:
            return False

        # Fee validation
        if self.fee < 0:
            return False

        # Timestamp validation (not too far in future)
        if self.timestamp > current_time + 300:  # 5 minutes tolerance
            return False

        selected_algorithm = (
            crypto_config.signature_algorithm
            if crypto_config is not None
            else self.signature_algorithm
        )
        require_signatures = (
            crypto_config.require_signatures if crypto_config is not None else False
        )
        if require_signatures:
            if not self.signature:
                return False
            key = public_key or self.public_key
            if not key:
                return False
            if not verify_payload_signature(
                self.to_bytes(), self.signature, key, selected_algorithm
            ):
                return False
        elif self.signature:
            key = public_key or self.public_key
            if not key:
                return False
            if not verify_payload_signature(
                self.to_bytes(), self.signature, key, selected_algorithm
            ):
                return False

        return True

    def with_signature(
        self,
        signature: DigitalSignature,
        public_key: PublicKey | None = None,
    ) -> Transaction:
        """Create new transaction with specified signature."""
        return Transaction(
            transaction_id=self.transaction_id,
            sender=self.sender,
            receiver=self.receiver,
            operation_type=self.operation_type,
            payload=self.payload,
            timestamp=self.timestamp,
            fee=self.fee,
            nonce=self.nonce,
            signature=signature,
            public_key=public_key or self.public_key,
            signature_algorithm=self.signature_algorithm,
        )

    def with_nonce(self, nonce: Nonce) -> Transaction:
        """Create new transaction with specified nonce."""
        return Transaction(
            transaction_id=self.transaction_id,
            sender=self.sender,
            receiver=self.receiver,
            operation_type=self.operation_type,
            payload=self.payload,
            timestamp=self.timestamp,
            fee=self.fee,
            nonce=nonce,
            signature=self.signature,
            public_key=self.public_key,
            signature_algorithm=self.signature_algorithm,
        )

    def age_seconds(self, current_time: Timestamp | None = None) -> float:
        """Get age of transaction in seconds."""
        if current_time is None:
            current_time = time.time()
        return max(0.0, current_time - self.timestamp)

    def is_expired(
        self, timeout_seconds: float, current_time: Timestamp | None = None
    ) -> bool:
        """Check if transaction has expired."""
        return self.age_seconds(current_time) > timeout_seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert transaction to dictionary for serialization."""
        return {
            "transaction_id": self.transaction_id,
            "sender": self.sender,
            "receiver": self.receiver,
            "operation_type": self.operation_type.value,
            "payload": self.payload.hex(),
            "timestamp": self.timestamp,
            "fee": self.fee,
            "nonce": self.nonce,
            "signature": self.signature.hex() if self.signature else "",
            "public_key": self.public_key.hex() if self.public_key else "",
            "signature_algorithm": self.signature_algorithm,
            "hash": self.get_hash(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Transaction:
        """Create transaction from dictionary."""
        return cls(
            transaction_id=data["transaction_id"],
            sender=data["sender"],
            receiver=data["receiver"],
            operation_type=OperationType(data["operation_type"]),
            payload=bytes.fromhex(data["payload"]),
            timestamp=data["timestamp"],
            fee=data["fee"],
            nonce=data["nonce"],
            signature=bytes.fromhex(data["signature"]) if data["signature"] else b"",
            public_key=bytes.fromhex(data.get("public_key", ""))
            if data.get("public_key")
            else b"",
            signature_algorithm=data.get("signature_algorithm", "ed25519"),
        )

    def __str__(self) -> str:
        """String representation of transaction."""
        return (
            f"Transaction({self.operation_type.value}: "
            f"{self.sender} -> {self.receiver}, "
            f"fee={self.fee}, nonce={self.nonce})"
        )


@dataclass(frozen=True, slots=True)
class TransactionPool:
    """
    Immutable pool of pending transactions.

    Maintains transactions waiting to be included in blocks.
    """

    transactions: tuple[Transaction, ...] = field(default_factory=tuple)
    max_size: int = 10000

    def __post_init__(self) -> None:
        """Validate transaction pool."""
        if self.max_size <= 0:
            raise ValueError("Max size must be positive")
        if len(self.transactions) > self.max_size:
            raise ValueError(
                f"Too many transactions: {len(self.transactions)} > {self.max_size}"
            )

    def add_transaction(self, transaction: Transaction) -> TransactionPool:
        """Add transaction to pool."""
        if transaction in self.transactions:
            return self  # Already in pool

        new_transactions = list(self.transactions)
        new_transactions.append(transaction)

        # Remove oldest if over limit
        if len(new_transactions) > self.max_size:
            new_transactions = new_transactions[-self.max_size :]

        return TransactionPool(tuple(new_transactions), self.max_size)

    def remove_transaction(self, transaction_id: TransactionId) -> TransactionPool:
        """Remove transaction from pool."""
        new_transactions = [
            tx for tx in self.transactions if tx.transaction_id != transaction_id
        ]
        return TransactionPool(tuple(new_transactions), self.max_size)

    def get_transactions_by_fee(self, limit: int) -> tuple[Transaction, ...]:
        """Get transactions sorted by fee (highest first)."""
        sorted_transactions = sorted(
            self.transactions, key=lambda tx: tx.fee, reverse=True
        )
        return tuple(sorted_transactions[:limit])

    def get_valid_transactions(
        self, current_time: Timestamp | None = None
    ) -> tuple[Transaction, ...]:
        """Get all valid transactions."""
        return tuple(tx for tx in self.transactions if tx.is_valid(current_time))

    def size(self) -> int:
        """Get number of transactions in pool."""
        return len(self.transactions)

    def is_empty(self) -> bool:
        """Check if pool is empty."""
        return len(self.transactions) == 0

    def contains(self, transaction_id: TransactionId) -> bool:
        """Check if transaction is in pool."""
        return any(tx.transaction_id == transaction_id for tx in self.transactions)


# Hypothesis strategies for property-based testing


def transaction_id_strategy() -> st.SearchStrategy[TransactionId]:
    """Generate valid transaction IDs."""
    return st.text(min_size=1, max_size=100)


def node_id_strategy() -> st.SearchStrategy[NodeId]:
    """Generate valid node IDs."""
    return st.text(
        min_size=1,
        max_size=50,
        alphabet=st.characters(
            whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
        ),
    )


def transaction_payload_strategy() -> st.SearchStrategy[TransactionPayload]:
    """Generate valid transaction payloads."""
    return st.binary(max_size=1024)


def transaction_strategy() -> st.SearchStrategy[Transaction]:
    """Generate valid Transaction instances for testing."""

    @st.composite
    def _transaction(draw):
        signature_algorithm = "ed25519"
        include_signature = draw(st.booleans())
        base = Transaction(
            transaction_id=draw(transaction_id_strategy()),
            sender=draw(node_id_strategy()),
            receiver=draw(node_id_strategy()),
            operation_type=draw(st.sampled_from(OperationType)),
            payload=draw(transaction_payload_strategy()),
            timestamp=draw(st.floats(min_value=0, max_value=1700000000)),
            fee=draw(st.integers(min_value=0, max_value=1000)),
            nonce=draw(st.integers(min_value=0, max_value=1000000)),
            signature_algorithm=signature_algorithm,
        )
        if include_signature:
            from .blockchain_crypto import generate_keypair

            keypair = generate_keypair(signature_algorithm)
            return base.sign(keypair.private_key, algorithm=signature_algorithm)

        return base

    return _transaction()


def transaction_pool_strategy() -> st.SearchStrategy[TransactionPool]:
    """Generate valid TransactionPool instances for testing."""

    @st.composite
    def generate_valid_pool(draw):
        max_size = draw(st.integers(min_value=1, max_value=100))
        # Ensure transactions list doesn't exceed max_size
        transactions = draw(
            st.lists(transaction_strategy(), max_size=min(max_size, 20)).map(tuple)
        )
        return TransactionPool(transactions, max_size)

    return generate_valid_pool()
