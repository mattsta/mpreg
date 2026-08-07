"""Per-router decision log isolation."""

from __future__ import annotations

from mpreg.fabric.route_decision_log import RouteDecisionLog, make_record_from_route
from mpreg.fabric.router import FabricRouter, FabricRoutingConfig


def test_router_owns_distinct_decision_log() -> None:
    cfg = FabricRoutingConfig(local_cluster_id="c1", local_node_id="n1")
    r1 = FabricRouter(config=cfg)
    r2 = FabricRouter(config=cfg)
    assert r1.decision_log is not r2.decision_log
    assert isinstance(r1.decision_log, RouteDecisionLog)
    r1.decision_log.record(
        make_record_from_route(
            message_id="m1",
            correlation_id="c",
            topic="t",
            message_type="rpc",
            reason="local",
            cached=False,
            targets=["n1"],
            routing_path=["n1"],
            hops_required=0,
        )
    )
    assert r1.decision_log.stats()["size"] == 1
    assert r2.decision_log.stats()["size"] == 0
    assert len(r1.decision_log.recent(limit=10)) == 1
    assert len(r2.decision_log.recent(limit=10)) == 0
