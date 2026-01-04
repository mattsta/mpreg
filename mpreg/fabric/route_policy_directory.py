"""Neighbor policy directory for route announcement filtering."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from mpreg.datastructures.type_aliases import ClusterId

from .route_control import RoutePolicy


@dataclass(frozen=True, slots=True)
class RouteNeighborPolicy:
    """Route policy assigned to a specific neighbor cluster."""

    cluster_id: ClusterId
    policy: RoutePolicy


@dataclass(slots=True)
class RoutePolicyDirectory:
    """Resolve neighbor-specific policies with a default fallback."""

    default_policy: RoutePolicy | None = None
    neighbor_policies: dict[ClusterId, RoutePolicy] = field(default_factory=dict)

    def policy_for(self, cluster_id: ClusterId) -> RoutePolicy | None:
        return self.neighbor_policies.get(cluster_id, self.default_policy)

    def register(self, neighbor: RouteNeighborPolicy) -> None:
        self.neighbor_policies[neighbor.cluster_id] = neighbor.policy

    def resolver(
        self, *, use_default: bool = True
    ) -> Callable[[ClusterId], RoutePolicy | None]:
        def _resolver(cluster_id: ClusterId) -> RoutePolicy | None:
            if cluster_id in self.neighbor_policies:
                return self.neighbor_policies[cluster_id]
            if use_default:
                return self.default_policy
            return None

        return _resolver
