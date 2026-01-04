"""Cryptography utilities for blockchain signing and verification."""

from __future__ import annotations

from dataclasses import dataclass

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from .blockchain_types import DigitalSignature, PrivateKey, PublicKey

SUPPORTED_SIGNATURE_ALGORITHMS = ("ed25519",)


@dataclass(frozen=True, slots=True)
class CryptoKeyPair:
    """Raw keypair material for signing operations."""

    private_key: PrivateKey
    public_key: PublicKey


@dataclass(frozen=True, slots=True)
class TransactionSigner:
    """Helper for signing transactions consistently."""

    private_key: PrivateKey
    public_key: PublicKey
    algorithm: str = "ed25519"

    @classmethod
    def create(cls, algorithm: str = "ed25519") -> TransactionSigner:
        keypair = generate_keypair(algorithm)
        return cls(
            private_key=keypair.private_key,
            public_key=keypair.public_key,
            algorithm=algorithm,
        )

    def sign(self, transaction: Transaction) -> Transaction:
        from .transaction import Transaction

        if not isinstance(transaction, Transaction):
            raise ValueError("Expected Transaction to sign")
        return transaction.sign(self.private_key, algorithm=self.algorithm)


def _require_algorithm(algorithm: str) -> None:
    if algorithm not in SUPPORTED_SIGNATURE_ALGORITHMS:
        raise ValueError(f"Unsupported signature algorithm: {algorithm}")


def signature_length(algorithm: str = "ed25519") -> int:
    _require_algorithm(algorithm)
    if algorithm == "ed25519":
        return 64
    raise ValueError(f"Unsupported signature algorithm: {algorithm}")


def public_key_length(algorithm: str = "ed25519") -> int:
    _require_algorithm(algorithm)
    if algorithm == "ed25519":
        return 32
    raise ValueError(f"Unsupported signature algorithm: {algorithm}")


def generate_keypair(algorithm: str = "ed25519") -> CryptoKeyPair:
    """Generate a new keypair for the selected signature algorithm."""
    _require_algorithm(algorithm)
    if algorithm == "ed25519":
        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        return CryptoKeyPair(
            private_key=private_key.private_bytes(
                Encoding.Raw, PrivateFormat.Raw, NoEncryption()
            ),
            public_key=public_key.public_bytes(Encoding.Raw, PublicFormat.Raw),
        )
    raise ValueError(f"Unsupported signature algorithm: {algorithm}")


def derive_public_key(private_key: PrivateKey, algorithm: str = "ed25519") -> PublicKey:
    """Derive a public key from a raw private key."""
    _require_algorithm(algorithm)
    if algorithm == "ed25519":
        key = ed25519.Ed25519PrivateKey.from_private_bytes(private_key)
        return key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    raise ValueError(f"Unsupported signature algorithm: {algorithm}")


def sign_payload(
    payload: bytes, private_key: PrivateKey, algorithm: str = "ed25519"
) -> DigitalSignature:
    """Sign payload bytes using the provided private key."""
    _require_algorithm(algorithm)
    if algorithm == "ed25519":
        key = ed25519.Ed25519PrivateKey.from_private_bytes(private_key)
        return key.sign(payload)
    raise ValueError(f"Unsupported signature algorithm: {algorithm}")


def verify_signature(
    payload: bytes,
    signature: DigitalSignature,
    public_key: PublicKey,
    algorithm: str = "ed25519",
) -> bool:
    """Verify signature for payload using the provided public key."""
    _require_algorithm(algorithm)
    if algorithm == "ed25519":
        key = ed25519.Ed25519PublicKey.from_public_bytes(public_key)
        try:
            key.verify(signature, payload)
            return True
        except InvalidSignature:
            return False
    raise ValueError(f"Unsupported signature algorithm: {algorithm}")
