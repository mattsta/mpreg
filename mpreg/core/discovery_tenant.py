from __future__ import annotations

from dataclasses import dataclass, field

from mpreg.core.payloads import PayloadMapping
from mpreg.datastructures.type_aliases import TenantId


@dataclass(frozen=True, slots=True)
class DiscoveryTenantCredential:
    tenant_id: TenantId
    token: str = field(repr=False)
    scheme: str = "bearer"

    def matches(self, *, scheme: str, token: str) -> bool:
        return self.scheme.lower() == scheme.lower() and self.token == token

    @classmethod
    def from_dict(cls, payload: PayloadMapping) -> DiscoveryTenantCredential:
        return cls(
            tenant_id=str(payload.get("tenant_id", "")).strip(),
            token=str(payload.get("token", "")).strip(),
            scheme=str(payload.get("scheme", "bearer")).strip().lower() or "bearer",
        )
