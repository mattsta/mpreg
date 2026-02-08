"""
Fabric federation planner for multi-hop cluster forwarding.

Encapsulates cluster-level routing decisions for fabric forwarding while keeping
path computation and hop/loop handling separate from server IO.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from mpreg.datastructures.type_aliases import ClusterId, HopCount, NodeURL
from mpreg.fabric.federation_graph import GraphBasedFederationRouter
from mpreg.fabric.link_state import LinkStateMode
from mpreg.fabric.peer_directory import PeerNeighbor
from mpreg.fabric.route_control import RouteDestination, RouteTable

PeerLocator = Callable[[ClusterId], Sequence[NodeURL]]
NeighborLocator = Callable[[], Sequence[PeerNeighbor]]


class FabricForwardingFailureReason(StrEnum):
    """Reasons fabric forwarding may not select a next hop."""

    OK = "ok"
    FALLBACK_NEIGHBOR = "fallback_neighbor"
    NO_TARGET = "no_target_cluster"
    ALREADY_LOCAL = "already_local"
    LOOP_DETECTED = "loop_detected"
    HOP_BUDGET_EXHAUSTED = "hop_budget_exhausted"
    NO_PATH = "no_fabric_path"
    PATH_MISMATCH = "path_mismatch"
    NO_PEER = "no_peer_for_next_hop"


@dataclass(frozen=True, slots=True)
class FabricForwardingPlan:
    """Encapsulates a routing decision for the next fabric hop."""

    target_cluster: ClusterId
    next_cluster: ClusterId | None
    next_peer_url: NodeURL | None
    planned_path: tuple[ClusterId, ...]
    federation_path: tuple[ClusterId, ...]
    remaining_hops: HopCount
    reason: FabricForwardingFailureReason

    @property
    def can_forward(self) -> bool:
        """Check if this plan has a viable next hop."""
        return self.next_peer_url is not None


@dataclass(slots=True)
class FabricFederationPlanner:
    """
    Compute cluster-level paths and next-hop forwarding decisions.

    Keeps routing logic isolated so fabric handlers can stay focused on IO.
    """

    local_cluster: ClusterId
    graph_router: GraphBasedFederationRouter
    peer_locator: PeerLocator
    neighbor_locator: NeighborLocator | None = None
    default_max_hops: HopCount = 5
    route_table: RouteTable | None = None
    link_state_router: GraphBasedFederationRouter | None = None
    link_state_mode: LinkStateMode = field(default=LinkStateMode.DISABLED, repr=False)
    link_state_ecmp_paths: int = 1
    _ecmp_counter: int = field(default=0, repr=False)

    def plan_next_hop(
        self,
        *,
        target_cluster: ClusterId | None,
        visited_clusters: Sequence[ClusterId] = (),
        remaining_hops: HopCount | None = None,
    ) -> FabricForwardingPlan:
        if not target_cluster:
            return self._fail(
                target_cluster or "",
                visited_clusters,
                remaining_hops,
                FabricForwardingFailureReason.NO_TARGET,
            )

        if target_cluster == self.local_cluster:
            return self._fail(
                target_cluster,
                visited_clusters,
                remaining_hops,
                FabricForwardingFailureReason.ALREADY_LOCAL,
            )

        visited = tuple(visited_clusters)
        if visited and visited[-1] == self.local_cluster:
            visited = visited[:-1]
        if self.local_cluster in visited:
            return self._fail(
                target_cluster,
                visited,
                remaining_hops,
                FabricForwardingFailureReason.LOOP_DETECTED,
            )

        hop_budget = self.default_max_hops if remaining_hops is None else remaining_hops
        if hop_budget <= 0:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.HOP_BUDGET_EXHAUSTED,
            )

        link_state_plan = self._plan_from_link_state(
            target_cluster=target_cluster,
            visited=visited,
            hop_budget=hop_budget,
        )
        if link_state_plan is not None:
            return link_state_plan

        if self.link_state_mode is not LinkStateMode.ONLY:
            table_plan = self._plan_from_route_table(
                target_cluster=target_cluster,
                visited=visited,
                hop_budget=hop_budget,
            )
            if table_plan is not None:
                return table_plan

        planned_path = self.graph_router.find_optimal_path(
            self.local_cluster, target_cluster, max_hops=hop_budget
        )

        if not planned_path:
            return self._plan_from_peer_fallback(
                target_cluster=target_cluster,
                visited=visited,
                hop_budget=hop_budget,
            )

        planned_path_tuple = tuple(planned_path)
        if planned_path_tuple[0] != self.local_cluster:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.PATH_MISMATCH,
                planned_path=planned_path_tuple,
            )

        if len(planned_path_tuple) < 2:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.ALREADY_LOCAL,
                planned_path=planned_path_tuple,
            )

        next_cluster = planned_path_tuple[1]
        if next_cluster in visited:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.LOOP_DETECTED,
                planned_path=planned_path_tuple,
            )

        candidates = sorted(self.peer_locator(next_cluster))
        if not candidates:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.NO_PEER,
                planned_path=planned_path_tuple,
            )

        federation_path = visited + (self.local_cluster,)
        return FabricForwardingPlan(
            target_cluster=target_cluster,
            next_cluster=next_cluster,
            next_peer_url=candidates[0],
            planned_path=planned_path_tuple,
            federation_path=federation_path,
            remaining_hops=max(0, hop_budget - 1),
            reason=FabricForwardingFailureReason.OK,
        )

    def _plan_from_link_state(
        self,
        *,
        target_cluster: ClusterId,
        visited: tuple[ClusterId, ...],
        hop_budget: HopCount,
    ) -> FabricForwardingPlan | None:
        if self.link_state_mode is LinkStateMode.DISABLED:
            return None
        router = self.link_state_router
        if router is None:
            return None
        if self.link_state_ecmp_paths > 1:
            planned_path = self._select_ecmp_path(
                router.find_multiple_paths(
                    self.local_cluster,
                    target_cluster,
                    num_paths=self.link_state_ecmp_paths,
                    max_hops=hop_budget,
                )
            )
        else:
            planned_path = router.find_optimal_path(
                self.local_cluster, target_cluster, max_hops=hop_budget
            )
        if not planned_path:
            if self.link_state_mode is LinkStateMode.ONLY:
                return self._plan_from_peer_fallback(
                    target_cluster=target_cluster,
                    visited=visited,
                    hop_budget=hop_budget,
                )
            return None
        return self._plan_from_planned_path(
            target_cluster=target_cluster,
            visited=visited,
            hop_budget=hop_budget,
            planned_path=planned_path,
        )

    def _select_ecmp_path(self, paths: list[list[ClusterId]]) -> list[ClusterId] | None:
        if not paths:
            return None
        index = self._ecmp_counter % len(paths)
        self._ecmp_counter += 1
        return paths[index]

    def _plan_from_planned_path(
        self,
        *,
        target_cluster: ClusterId,
        visited: tuple[ClusterId, ...],
        hop_budget: HopCount,
        planned_path: list[ClusterId],
    ) -> FabricForwardingPlan:
        planned_path_tuple = tuple(planned_path)
        if planned_path_tuple[0] != self.local_cluster:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.PATH_MISMATCH,
                planned_path=planned_path_tuple,
            )
        if len(planned_path_tuple) < 2:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.ALREADY_LOCAL,
                planned_path=planned_path_tuple,
            )
        next_cluster = planned_path_tuple[1]
        if next_cluster in visited:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.LOOP_DETECTED,
                planned_path=planned_path_tuple,
            )
        candidates = sorted(self.peer_locator(next_cluster))
        if not candidates:
            return self._fail(
                target_cluster,
                visited,
                hop_budget,
                FabricForwardingFailureReason.NO_PEER,
                planned_path=planned_path_tuple,
            )
        federation_path = visited + (self.local_cluster,)
        return FabricForwardingPlan(
            target_cluster=target_cluster,
            next_cluster=next_cluster,
            next_peer_url=candidates[0],
            planned_path=planned_path_tuple,
            federation_path=federation_path,
            remaining_hops=max(0, hop_budget - 1),
            reason=FabricForwardingFailureReason.OK,
        )

    def _plan_from_peer_fallback(
        self,
        *,
        target_cluster: ClusterId,
        visited: tuple[ClusterId, ...],
        hop_budget: HopCount,
    ) -> FabricForwardingPlan:
        direct_peers = tuple(self.peer_locator(target_cluster))
        if direct_peers:
            if target_cluster in visited:
                return self._fail(
                    target_cluster,
                    visited,
                    hop_budget,
                    FabricForwardingFailureReason.LOOP_DETECTED,
                    planned_path=(self.local_cluster, target_cluster),
                )
            return FabricForwardingPlan(
                target_cluster=target_cluster,
                next_cluster=target_cluster,
                next_peer_url=direct_peers[0],
                planned_path=(self.local_cluster, target_cluster),
                federation_path=visited + (self.local_cluster,),
                remaining_hops=max(0, hop_budget - 1),
                reason=FabricForwardingFailureReason.OK,
            )
        fallback = self._pick_fallback_neighbor(visited)
        if fallback:
            next_cluster = fallback.cluster_id
            next_peer = fallback.node_id
            federation_path = visited + (self.local_cluster,)
            return FabricForwardingPlan(
                target_cluster=target_cluster,
                next_cluster=next_cluster,
                next_peer_url=next_peer,
                planned_path=(self.local_cluster, next_cluster),
                federation_path=federation_path,
                remaining_hops=max(0, hop_budget - 1),
                reason=FabricForwardingFailureReason.FALLBACK_NEIGHBOR,
            )
        return self._fail(
            target_cluster,
            visited,
            hop_budget,
            FabricForwardingFailureReason.NO_PATH,
        )

    def _plan_from_route_table(
        self,
        *,
        target_cluster: ClusterId,
        visited: tuple[ClusterId, ...],
        hop_budget: HopCount,
    ) -> FabricForwardingPlan | None:
        table = self.route_table
        if not table:
            return None
        avoid = visited + (self.local_cluster,)
        record = table.select_route(
            RouteDestination(cluster_id=target_cluster),
            avoid_clusters=avoid,
        )
        if not record:
            return None
        if hop_budget is not None and record.metrics.hop_count > hop_budget:
            return None
        next_cluster = record.next_hop
        if next_cluster in visited:
            return None
        candidates = sorted(self.peer_locator(next_cluster))
        if not candidates:
            return None
        planned_path = record.path.hops
        federation_path = visited + (self.local_cluster,)
        return FabricForwardingPlan(
            target_cluster=target_cluster,
            next_cluster=next_cluster,
            next_peer_url=candidates[0],
            planned_path=planned_path,
            federation_path=federation_path,
            remaining_hops=max(0, hop_budget - 1),
            reason=FabricForwardingFailureReason.OK,
        )

    def _fail(
        self,
        target_cluster: ClusterId,
        visited: Sequence[ClusterId],
        hop_budget: HopCount | None,
        reason: FabricForwardingFailureReason,
        *,
        planned_path: tuple[ClusterId, ...] | None = None,
    ) -> FabricForwardingPlan:
        remaining = self.default_max_hops if hop_budget is None else max(0, hop_budget)
        return FabricForwardingPlan(
            target_cluster=target_cluster,
            next_cluster=None,
            next_peer_url=None,
            planned_path=planned_path or tuple(),
            federation_path=tuple(visited),
            remaining_hops=remaining,
            reason=reason,
        )

    def _pick_fallback_neighbor(
        self, visited: Sequence[ClusterId]
    ) -> PeerNeighbor | None:
        if not self.neighbor_locator:
            return None
        visited_set = set(visited)
        visited_set.add(self.local_cluster)
        candidates = [
            neighbor
            for neighbor in self.neighbor_locator()
            if neighbor.cluster_id not in visited_set
        ]
        if not candidates:
            return None
        return sorted(candidates)[0]
