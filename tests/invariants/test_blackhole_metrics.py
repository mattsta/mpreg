"""A7: Unreachable decisions are observable (not silent drops)."""

from __future__ import annotations

from mpreg.fabric.route_decision_log import RouteDecisionLog, make_record_from_route
from mpreg.server_pkg.routing_handlers import RoutingPlane

def test_blackhole_count_increments() -> None:
    log = RouteDecisionLog()
    log.record(
        make_record_from_route(
            message_id="m1",
            correlation_id="c1",
            topic="t",
            message_type="rpc",
            reason="no_fabric_path",
            cached=False,
            targets=[],
            routing_path=[],
            hops_required=0,
        )
    )
    log.record(
        make_record_from_route(
            message_id="m2",
            correlation_id="c2",
            topic="t",
            message_type="rpc",
            reason="ok",
            cached=False,
            targets=["ws://x"],
            routing_path=["a", "b"],
            hops_required=1,
        )
    )
    stats = log.stats()
    assert stats["blackhole_count"] == 1
    assert stats["total_recorded"] == 2
    assert 0.0 < stats["reachable_ratio"] < 1.0

def test_routing_plane_records_unreachable() -> None:
    plane = RoutingPlane()
    plane.record_unreachable(message_id="x", reason="unreachable")
    assert plane.blackhole_stats()["blackhole_count"] == 1
