"""Routing-plane helpers for federation planning and decision observability."""

from __future__ import annotations

from typing import Any

from mpreg.fabric.federation_planner import (
    FabricFederationPlanner,
    FabricForwardingPlan,
)
from mpreg.fabric.route_decision_log import (
    RouteDecisionLog,
    make_record_from_route,
)


class RoutingPlane:
    """Thin facade over planner + decision log for server composition."""

    def __init__(
        self,
        *,
        planner: FabricFederationPlanner | None = None,
        decision_log: RouteDecisionLog | None = None,
    ) -> None:
        self.planner = planner
        self.decision_log = decision_log or RouteDecisionLog()

    def plan_next_hop(self, **kwargs: Any) -> FabricForwardingPlan | None:
        if self.planner is None:
            return None
        return self.planner.plan_next_hop(**kwargs)

    def record_unreachable(
        self,
        *,
        message_id: str,
        correlation_id: str = "",
        topic: str = "",
        reason: str = "no_fabric_path",
        traceparent: str | None = None,
    ) -> None:
        """Record an observable blackhole / unreachable decision (INV-R / A7)."""
        self.decision_log.record(
            make_record_from_route(
                message_id=message_id,
                correlation_id=correlation_id,
                topic=topic,
                message_type="control",
                reason=reason,
                cached=False,
                targets=[],
                routing_path=[],
                hops_required=0,
                traceparent=traceparent,
            )
        )

    def blackhole_stats(self) -> dict[str, Any]:
        return self.decision_log.stats()
