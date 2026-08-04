"""Data-plane drain admission policy (ERG-01).

Peeled from ``MPREGServer.opened`` so the role gate is unit-testable without
the mega-module connection loop.
"""

from __future__ import annotations

from typing import Any, Container

# Roles refused when the node is draining (known data-plane).
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

# COR-T13-05: under drain, only these roles (plus fabric CONTROL peel) are admitted.
# Unknown roles fail-closed (refused) so new data-plane names cannot slip through.
CONTROL_PLANE_ROLES: frozenset[str] = frozenset(
    {
        "server",
        "gossip",
        "fabric-gossip",
        "fabric-control",
        "consensus-vote",
        "consensus-proposal",
        "hello",
        "peer",
        "goodbye",
        "status",
        "STATUS",
        "GOODBYE",
    }
)

def is_data_plane_role(role: str | None) -> bool:
    return bool(role) and str(role) in DATA_PLANE_ROLES

def is_control_plane_role(role: str | None) -> bool:
    if not role:
        return False
    r = str(role)
    if r in CONTROL_PLANE_ROLES:
        return True
    # Case-insensitive match for STATUS/GOODBYE style roles
    return r.lower() in {c.lower() for c in CONTROL_PLANE_ROLES}

def is_fabric_control_plane(
    *,
    role: str | None = None,
    message_type: str | None = None,
    topic: str | None = None,
    fabric_payload: dict[str, Any] | None = None,
) -> bool:
    """COR-T11-04: Raft/membership CONTROL must not be refused under drain.

    Raft RPCs are UnifiedMessage with MessageType.CONTROL on raft topics, but
    the wire envelope is always role ``fabric-message``. Inspect inner type.
    """
    if role is not None and is_control_plane_role(role):
        # Explicit control roles are control-plane even without fabric payload.
        if str(role) != "fabric-message":
            return True
    mt = (message_type or "").lower()
    top = (topic or "").lower()
    if fabric_payload:
        mt = mt or str(
            fabric_payload.get("message_type")
            or fabric_payload.get("type")
            or ""
        ).lower()
        headers = fabric_payload.get("headers") or {}
        if isinstance(headers, dict):
            top = top or str(
                headers.get("topic") or fabric_payload.get("topic") or ""
            ).lower()
        else:
            top = top or str(fabric_payload.get("topic") or "").lower()
        # Nested payload common shapes
        if not mt:
            inner = fabric_payload.get("payload")
            if isinstance(inner, dict):
                mt = str(inner.get("message_type") or inner.get("type") or "").lower()
                top = top or str(inner.get("topic") or "").lower()
    if mt in {"control", "message_type.control"}:
        return True
    if "raft" in top or top.startswith("mpreg.raft") or ".raft." in top:
        return True
    if role == "fabric-control":
        return True
    return False

def should_refuse_for_drain(
    *,
    draining: bool,
    role: str | None,
    message_type: str | None = None,
    topic: str | None = None,
    fabric_payload: dict[str, Any] | None = None,
) -> bool:
    """Return True when a message role must be refused under drain.

    COR-T13-05: admit only known control-plane roles (and fabric CONTROL peel).
    Unknown roles and known data-plane roles are refused (fail-closed).
    """
    if not draining:
        return False
    if is_fabric_control_plane(
        role=role,
        message_type=message_type,
        topic=topic,
        fabric_payload=fabric_payload,
    ):
        return False
    if is_control_plane_role(role):
        return False
    # Known data-plane or unknown / empty role → refuse
    return True

def drain_unavailable_response(u: str | None = None) -> Any:
    """Build the canonical RPC unavailable response for drain refusal."""
    from mpreg.server_pkg.rpc_responses import unavailable_response

    return unavailable_response(
        str(u or "drain"), "node_draining: data-plane admission refused"
    )
