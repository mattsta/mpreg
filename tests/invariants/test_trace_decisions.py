"""INV-P6: W3C traceparent survives into route decision records."""

from __future__ import annotations

from mpreg.core.observability.trace_context import (
    TRACEPARENT_KEY,
    ensure_traceparent,
    extract_traceparent,
    generate_traceparent,
)
from mpreg.fabric.route_decision_log import RouteDecisionLog, make_record_from_route

def test_traceparent_round_trip_in_metadata() -> None:
    tp = generate_traceparent()
    meta = {TRACEPARENT_KEY: tp}
    assert extract_traceparent(meta) == tp
    ensured = ensure_traceparent(dict(meta))
    assert ensured == tp

def test_decision_log_preserves_and_filters_traceparent() -> None:
    tp = generate_traceparent()
    other = generate_traceparent()
    log = RouteDecisionLog()
    log.record(
        make_record_from_route(
            message_id="m1",
            correlation_id="c1",
            topic="t",
            message_type="rpc",
            reason="ok",
            cached=False,
            targets=["ws://x"],
            routing_path=["a", "b"],
            hops_required=1,
            traceparent=tp,
        )
    )
    log.record(
        make_record_from_route(
            message_id="m2",
            correlation_id="c2",
            topic="t",
            message_type="rpc",
            reason="no_fabric_path",
            cached=False,
            targets=[],
            routing_path=[],
            hops_required=0,
            traceparent=other,
        )
    )
    by_tp = log.recent(traceparent=tp)
    assert len(by_tp) == 1
    assert by_tp[0].message_id == "m1"
    assert by_tp[0].traceparent == tp
    assert by_tp[0].correlation_id == "c1"
