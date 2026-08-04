"""Data-plane drain admission policy (ERG-01).

Peeled from ``MPREGServer.opened`` so the role gate is unit-testable without
the mega-module connection loop.
"""

from __future__ import annotations

from typing import Any, Container

# Roles refused when the node is draining. Control/server/gossip still flow.
DATA_PLANE_ROLES: frozenset[str] = frozenset(
    {
        "rpc",
        "pubsub-publish",
        "pubsub-subscribe",
        "pubsub-unsubscribe",
        "pubsub",
        # COR-T10-03: fabric data-plane rides peer + client connections.
        "fabric-message",
    }
)

def is_data_plane_role(role: str | None) -> bool:
    return bool(role) and str(role) in DATA_PLANE_ROLES

def should_refuse_for_drain(*, draining: bool, role: str | None) -> bool:
    """Return True when a message role must be refused under drain."""
    return bool(draining) and is_data_plane_role(role)

def drain_unavailable_response(u: str | None = None) -> Any:
    """Build the canonical RPC unavailable response for drain refusal."""
    from mpreg.server_pkg.rpc_responses import unavailable_response

    return unavailable_response(
        str(u or "drain"), "node_draining: data-plane admission refused"
    )
