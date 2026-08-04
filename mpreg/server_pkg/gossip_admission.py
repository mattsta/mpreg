"""Fabric gossip envelope admission (HMAC policy).

Peeled from ``MPREGServer`` (PERF-02) so control-plane crypto policy lives
beside other server_pkg helpers rather than the mega-module hot path.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

def accept_fabric_gossip_payload(
    *,
    payload: dict[str, Any],
    require_hmac: bool,
    secret: str | None,
    node_name: str = "node",
) -> dict[str, Any] | None:
    """Apply optional gossip envelope HMAC policy; return payload without HMAC field.

    Returns ``None`` when the envelope is rejected (fail-closed when required).
    """
    from mpreg.fabric.gossip_signatures import (
        SIGNATURE_KEY,
        accept_gossip_payload,
    )

    ok = accept_gossip_payload(payload, require_hmac=require_hmac, secret=secret)
    if not ok:
        logger.warning(
            "[{}] Rejected fabric-gossip envelope (HMAC policy)",
            node_name,
        )
        return None
    if SIGNATURE_KEY in payload:
        return {k: v for k, v in payload.items() if k != SIGNATURE_KEY}
    return payload
