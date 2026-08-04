"""Fail-closed fabric hop header advancement (single source of truth).

Both ``MPREGServer._next_fabric_headers`` and ``ClusterMessenger.next_headers``
delegate path/budget math here so loop detection and hop-budget exhaustion
cannot drift between call sites.

Contract
--------
* Returns a new ``MessageHeaders`` (never ``None``).
* Raises ``MpregError(ROUTE_LOOP)`` when ``node_id`` is already on the path.
* Raises ``MpregError(HOP_BUDGET_EXCEEDED)`` when hop count would exceed budget.
* Callers that must not propagate (e.g. best-effort pubsub fan-out) catch those
  two codes and drop/respond locally.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from mpreg.core.errors import hop_budget_exceeded, route_loop_detected
from mpreg.fabric.message import MessageHeaders

def advance_fabric_headers(
    *,
    correlation_id: str,
    headers: MessageHeaders | None,
    node_id: str,
    cluster_id: str,
    max_hops: int | None,
    target_cluster: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> MessageHeaders:
    """Advance routing/federation paths and enforce hop budget (fail-closed)."""
    if headers is None:
        return MessageHeaders(
            correlation_id=correlation_id,
            source_cluster=cluster_id,
            target_cluster=target_cluster,
            routing_path=(node_id,),
            federation_path=(cluster_id,),
            hop_budget=max_hops,
            metadata=dict(metadata) if metadata is not None else {},
        )

    if node_id in headers.routing_path:
        raise route_loop_detected(
            node_id=node_id,
            correlation_id=headers.correlation_id or correlation_id,
            routing_path=headers.routing_path,
        )

    hop_budget = headers.hop_budget
    if hop_budget is None:
        hop_budget = max_hops
    elif max_hops is not None:
        hop_budget = min(hop_budget, max_hops)

    routing_path = headers.routing_path
    if not routing_path or routing_path[-1] != node_id:
        routing_path = (*routing_path, node_id)

    federation_path = headers.federation_path
    if not federation_path or federation_path[-1] != cluster_id:
        federation_path = (*federation_path, cluster_id)

    hop_count = max(0, len(routing_path) - 1)
    if hop_budget is not None and hop_count > hop_budget:
        raise hop_budget_exceeded(
            int(hop_budget),
            hop_count=hop_count,
            correlation_id=headers.correlation_id or correlation_id,
        )

    if metadata is not None:
        meta = dict(metadata)
    else:
        meta = dict(headers.metadata)

    return MessageHeaders(
        correlation_id=headers.correlation_id or correlation_id,
        source_cluster=headers.source_cluster or cluster_id,
        target_cluster=target_cluster if target_cluster is not None else headers.target_cluster,
        routing_path=routing_path,
        federation_path=federation_path,
        hop_budget=hop_budget,
        priority=headers.priority,
        metadata=meta,
        deadline_remaining_ms=headers.deadline_remaining_ms,
    )
