"""Unit tests for fail-closed fabric hop header advancement."""

from __future__ import annotations

import pytest

from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.fabric.hop_headers import advance_fabric_headers
from mpreg.fabric.message import MessageHeaders

def test_initial_headers_when_none() -> None:
    headers = advance_fabric_headers(
        correlation_id="corr-1",
        headers=None,
        node_id="ws://node-a",
        cluster_id="cluster-a",
        max_hops=7,
        target_cluster="cluster-b",
        metadata={"k": "v"},
    )
    assert headers.correlation_id == "corr-1"
    assert headers.source_cluster == "cluster-a"
    assert headers.target_cluster == "cluster-b"
    assert headers.routing_path == ("ws://node-a",)
    assert headers.federation_path == ("cluster-a",)
    assert headers.hop_budget == 7
    assert headers.metadata == {"k": "v"}

def test_appends_paths_and_preserves_budget() -> None:
    existing = MessageHeaders(
        correlation_id="corr-2",
        source_cluster="cluster-a",
        target_cluster="cluster-b",
        routing_path=("ws://node-a",),
        federation_path=("cluster-a",),
        hop_budget=5,
        metadata={"trace": "1"},
    )
    next_h = advance_fabric_headers(
        correlation_id="ignored-if-set",
        headers=existing,
        node_id="ws://node-b",
        cluster_id="cluster-b",
        max_hops=9,
        target_cluster="cluster-c",
    )
    assert next_h.correlation_id == "corr-2"
    assert next_h.routing_path == ("ws://node-a", "ws://node-b")
    assert next_h.federation_path == ("cluster-a", "cluster-b")
    # min(existing budget, max_hops)
    assert next_h.hop_budget == 5
    assert next_h.target_cluster == "cluster-c"
    assert next_h.metadata.get("trace") == "1"

def test_reenter_as_current_tail_is_route_loop() -> None:
    """Fail-closed: local node already on path (even as tail) is a loop.

    Callers must not re-advance headers for a hop they already recorded.
    The append-if-not-tail branch only runs after the membership check.
    """
    existing = MessageHeaders(
        correlation_id="c",
        routing_path=("ws://node-a", "ws://node-b"),
        federation_path=("ca", "cb"),
        hop_budget=10,
    )
    with pytest.raises(MpregError) as ei:
        advance_fabric_headers(
            correlation_id="c",
            headers=existing,
            node_id="ws://node-b",
            cluster_id="cb",
            max_hops=10,
        )
    assert ei.value.code == int(MpregErrorCode.ROUTE_LOOP)

def test_route_loop_raises() -> None:
    existing = MessageHeaders(
        correlation_id="c",
        routing_path=("ws://a", "ws://b"),
        hop_budget=10,
    )
    with pytest.raises(MpregError) as ei:
        advance_fabric_headers(
            correlation_id="c",
            headers=existing,
            node_id="ws://a",
            cluster_id="c1",
            max_hops=10,
        )
    assert ei.value.code == int(MpregErrorCode.ROUTE_LOOP)
    assert ei.value.retryable is False

def test_hop_budget_exceeded_raises() -> None:
    existing = MessageHeaders(
        correlation_id="c",
        routing_path=("n1", "n2"),
        hop_budget=1,
    )
    with pytest.raises(MpregError) as ei:
        advance_fabric_headers(
            correlation_id="c",
            headers=existing,
            node_id="n3",
            cluster_id="c1",
            max_hops=10,
        )
    assert ei.value.code == int(MpregErrorCode.HOP_BUDGET_EXCEEDED)

def test_max_hops_tightens_existing_budget() -> None:
    existing = MessageHeaders(
        correlation_id="c",
        routing_path=("n1",),
        hop_budget=20,
    )
    next_h = advance_fabric_headers(
        correlation_id="c",
        headers=existing,
        node_id="n2",
        cluster_id="c1",
        max_hops=3,
    )
    assert next_h.hop_budget == 3
