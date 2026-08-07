"""Security helpers for fabric route announcements."""

from __future__ import annotations

from dataclasses import dataclass

from mpreg.datastructures.blockchain_crypto import (
    derive_public_key,
    generate_keypair,
    sign_payload,
    verify_signature,
)
from mpreg.datastructures.blockchain_types import (
    PrivateKey,
    PublicKey,
)

from .route_control import RouteAnnouncement, RouteWithdrawal


@dataclass(frozen=True, slots=True)
class RouteSecurityConfig:
    """Policy for route announcement signature handling."""

    require_signatures: bool = False
    allow_unsigned: bool = True
    signature_algorithm: str = "ed25519"


@dataclass(frozen=True, slots=True)
class RouteAnnouncementSigner:
    """Signer for route announcements."""

    private_key: PrivateKey
    public_key: PublicKey
    algorithm: str = "ed25519"

    @classmethod
    def create(cls, algorithm: str = "ed25519") -> RouteAnnouncementSigner:
        keypair = generate_keypair(algorithm)
        return cls(
            private_key=keypair.private_key,
            public_key=keypair.public_key,
            algorithm=algorithm,
        )

    def sign(self, announcement: RouteAnnouncement) -> RouteAnnouncement:
        signature = sign_payload(
            announcement.to_bytes(), self.private_key, self.algorithm
        )
        public_key = self.public_key or derive_public_key(
            self.private_key, self.algorithm
        )
        return announcement.with_signature(
            signature=signature,
            public_key=public_key,
            algorithm=self.algorithm,
        )

    def sign_withdrawal(self, withdrawal: RouteWithdrawal) -> RouteWithdrawal:
        signature = sign_payload(
            withdrawal.to_bytes(), self.private_key, self.algorithm
        )
        public_key = self.public_key or derive_public_key(
            self.private_key, self.algorithm
        )
        return withdrawal.with_signature(
            signature=signature,
            public_key=public_key,
            algorithm=self.algorithm,
        )


def verify_route_announcement(
    announcement: RouteAnnouncement,
    *,
    public_key: PublicKey,
    algorithm: str | None = None,
) -> bool:
    """Verify a route announcement signature against the provided key."""

    if not announcement.signature:
        return False
    selected_algorithm = algorithm or announcement.signature_algorithm
    return verify_signature(
        announcement.to_bytes(),
        announcement.signature,
        public_key,
        selected_algorithm,
    )


def verify_route_withdrawal(
    withdrawal: RouteWithdrawal,
    *,
    public_key: PublicKey,
    algorithm: str | None = None,
) -> bool:
    """Verify a route withdrawal signature against the provided key."""

    if not withdrawal.signature:
        return False
    selected_algorithm = algorithm or withdrawal.signature_algorithm
    return verify_signature(
        withdrawal.to_bytes(),
        withdrawal.signature,
        public_key,
        selected_algorithm,
    )
