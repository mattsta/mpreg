"""Routing engine skeleton for unified fabric decisions."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from mpreg.datastructures.type_aliases import ClusterId, HopCount, NodeId, Timestamp
from mpreg.fabric.federation_planner import FabricForwardingPlan

from .catalog import FunctionEndpoint
from .index import FunctionQuery, RoutingIndex


class FederationPlanner(Protocol):
    def plan_next_hop(
        self,
        *,
        target_cluster: ClusterId | None,
        visited_clusters: tuple[ClusterId, ...] = (),
        remaining_hops: HopCount | None = None,
    ) -> FabricForwardingPlan: ...  # pragma: no cover - protocol definition


class FunctionRouteReason(Enum):
    LOCAL_MATCH = "local_match"
    REMOTE_PLANNED = "remote_planned"
    REMOTE_UNAVAILABLE = "remote_unavailable"
    NO_MATCH = "no_match"
    NO_TARGET = "no_target_cluster"
    NO_FEDERATION = "no_federation_planner"


class ClusterRouteReason(Enum):
    LOCAL = "local"
    REMOTE_PLANNED = "remote_planned"
    REMOTE_UNAVAILABLE = "remote_unavailable"
    NO_TARGET = "no_target_cluster"
    NO_FEDERATION = "no_federation_planner"


@dataclass(frozen=True, slots=True)
class FunctionRoutePlan:
    target_cluster: ClusterId | None
    targets: tuple[FunctionEndpoint, ...]
    selected_target: FunctionEndpoint | None
    forwarding: FabricForwardingPlan | None
    is_local: bool
    reason: FunctionRouteReason


@dataclass(frozen=True, slots=True)
class ClusterRoutePlan:
    target_cluster: ClusterId | None
    forwarding: FabricForwardingPlan | None
    is_local: bool
    reason: ClusterRouteReason

    @property
    def can_forward(self) -> bool:
        if self.forwarding is None:
            return False
        return self.forwarding.can_forward


@dataclass(slots=True)
class RoutingEngine:
    """Routing decisions across local endpoints and federated paths."""

    local_cluster: ClusterId
    routing_index: RoutingIndex
    federation_planner: FederationPlanner | None = None
    node_load_provider: Callable[[NodeId], float | None] | None = None

    def plan_function_route(
        self,
        query: FunctionQuery,
        *,
        allow_local: bool = True,
        routing_path: tuple[ClusterId, ...] = (),
        hop_budget: HopCount | None = None,
        now: Timestamp | None = None,
    ) -> FunctionRoutePlan:
        matches = self.routing_index.find_functions(query, now=now)
        local_matches = [
            entry for entry in matches if entry.cluster_id == self.local_cluster
        ]
        target_cluster = query.cluster_id
        remote_matches = [
            entry for entry in matches if entry.cluster_id != self.local_cluster
        ]

        def select_target(
            candidates: list[FunctionEndpoint],
        ) -> FunctionEndpoint | None:
            if not candidates:
                return None

            def load_sort_key(
                entry: FunctionEndpoint,
            ) -> tuple[float, str, str, str, str]:
                load_score: float | None = None
                if self.node_load_provider:
                    try:
                        load_score = self.node_load_provider(entry.node_id)
                    except Exception:
                        load_score = None
                score = float(load_score) if load_score is not None else 1000.0
                return (
                    score,
                    entry.cluster_id,
                    entry.node_id,
                    entry.identity.function_id,
                    str(entry.identity.version),
                )

            candidates_sorted = sorted(candidates, key=load_sort_key)
            return candidates_sorted[0]

        if allow_local and (
            target_cluster is None or target_cluster == self.local_cluster
        ):
            if local_matches:
                selected = select_target(local_matches)
                return FunctionRoutePlan(
                    target_cluster=self.local_cluster,
                    targets=tuple(local_matches),
                    selected_target=selected,
                    forwarding=None,
                    is_local=True,
                    reason=FunctionRouteReason.LOCAL_MATCH,
                )
            if target_cluster == self.local_cluster:
                return FunctionRoutePlan(
                    target_cluster=target_cluster,
                    targets=tuple(),
                    selected_target=None,
                    forwarding=None,
                    is_local=False,
                    reason=FunctionRouteReason.NO_MATCH,
                )

        if target_cluster is None:
            if not remote_matches:
                return FunctionRoutePlan(
                    target_cluster=None,
                    targets=tuple(),
                    selected_target=None,
                    forwarding=None,
                    is_local=False,
                    reason=FunctionRouteReason.NO_MATCH,
                )
            selected = select_target(remote_matches)
            if not selected:
                return FunctionRoutePlan(
                    target_cluster=None,
                    targets=tuple(),
                    selected_target=None,
                    forwarding=None,
                    is_local=False,
                    reason=FunctionRouteReason.NO_MATCH,
                )
            target_cluster = selected.cluster_id

        if target_cluster == self.local_cluster:
            return FunctionRoutePlan(
                target_cluster=target_cluster,
                targets=tuple(),
                selected_target=None,
                forwarding=None,
                is_local=False,
                reason=FunctionRouteReason.NO_MATCH,
            )

        if target_cluster is not None and query.cluster_id is None:
            remote_matches = [
                entry for entry in remote_matches if entry.cluster_id == target_cluster
            ]

        selected = select_target(remote_matches)
        if not selected:
            return FunctionRoutePlan(
                target_cluster=target_cluster,
                targets=tuple(),
                selected_target=None,
                forwarding=None,
                is_local=False,
                reason=FunctionRouteReason.NO_MATCH,
            )

        if not self.federation_planner:
            return FunctionRoutePlan(
                target_cluster=target_cluster,
                targets=tuple(remote_matches),
                selected_target=selected,
                forwarding=None,
                is_local=False,
                reason=FunctionRouteReason.NO_FEDERATION,
            )

        plan = self.federation_planner.plan_next_hop(
            target_cluster=target_cluster,
            visited_clusters=routing_path,
            remaining_hops=hop_budget,
        )
        reason = (
            FunctionRouteReason.REMOTE_PLANNED
            if plan.can_forward
            else FunctionRouteReason.REMOTE_UNAVAILABLE
        )
        return FunctionRoutePlan(
            target_cluster=target_cluster,
            targets=tuple(remote_matches),
            selected_target=selected,
            forwarding=plan,
            is_local=False,
            reason=reason,
        )

    def plan_cluster_route(
        self,
        target_cluster: ClusterId | None,
        *,
        routing_path: tuple[ClusterId, ...] = (),
        hop_budget: HopCount | None = None,
    ) -> ClusterRoutePlan:
        if not target_cluster:
            return ClusterRoutePlan(
                target_cluster=target_cluster,
                forwarding=None,
                is_local=False,
                reason=ClusterRouteReason.NO_TARGET,
            )

        if target_cluster == self.local_cluster:
            return ClusterRoutePlan(
                target_cluster=target_cluster,
                forwarding=None,
                is_local=True,
                reason=ClusterRouteReason.LOCAL,
            )

        if not self.federation_planner:
            return ClusterRoutePlan(
                target_cluster=target_cluster,
                forwarding=None,
                is_local=False,
                reason=ClusterRouteReason.NO_FEDERATION,
            )

        plan = self.federation_planner.plan_next_hop(
            target_cluster=target_cluster,
            visited_clusters=routing_path,
            remaining_hops=hop_budget,
        )
        reason = (
            ClusterRouteReason.REMOTE_PLANNED
            if plan.can_forward
            else ClusterRouteReason.REMOTE_UNAVAILABLE
        )
        return ClusterRoutePlan(
            target_cluster=target_cluster,
            forwarding=plan,
            is_local=False,
            reason=reason,
        )
