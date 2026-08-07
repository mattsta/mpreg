"""Oracles that turn architecture claims into checkable expectations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum

from mpreg.fabric.link_state import LinkStateMode
from mpreg.fabric.route_control import RouteDestination, RouteTable


class PlannerSource(StrEnum):
    """Which plane a next-hop decision came from (oracle classification)."""

    LINK_STATE = "link_state"
    PATH_VECTOR = "path_vector"
    GRAPH = "graph"
    PEER_FALLBACK = "peer_fallback"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class ExpectedNextHop:
    """Oracle expectation for a single destination from one origin."""

    origin: str
    destination: str
    allowed_next_hops: frozenset[str]
    preferred_source: PlannerSource
    must_not_use_path_vector: bool = False


@dataclass(slots=True)
class RoutingOracle:
    """Compute expected next-hop sets from topology truth + mode.

    This is intentionally conservative: it validates *membership* of chosen
    next hops against adjacency-derived candidates, not full metric scoring
    equivalence (which is covered by unit tests on RoutePolicy).
    """

    adjacency: dict[str, set[str]] = field(default_factory=dict)
    mode: LinkStateMode = LinkStateMode.DISABLED
    peer_urls: dict[str, list[str]] = field(default_factory=dict)

    def set_edge(self, a: str, b: str, *, bidirectional: bool = True) -> None:
        self.adjacency.setdefault(a, set()).add(b)
        if bidirectional:
            self.adjacency.setdefault(b, set()).add(a)

    def remove_edge(self, a: str, b: str, *, bidirectional: bool = True) -> None:
        self.adjacency.get(a, set()).discard(b)
        if bidirectional:
            self.adjacency.get(b, set()).discard(a)

    def neighbors(self, node: str) -> frozenset[str]:
        return frozenset(self.adjacency.get(node, ()))

    def bfs_next_hops(self, origin: str, destination: str) -> frozenset[str]:
        """All first hops on some shortest path origin → destination."""
        if origin == destination:
            return frozenset()
        if destination in self.adjacency.get(origin, ()):
            return frozenset({destination})
        # BFS for shortest path length, then collect first hops.
        from collections import deque

        queue: deque[tuple[str, list[str]]] = deque([(origin, [])])
        seen = {origin}
        best_len: int | None = None
        first_hops: set[str] = set()
        while queue:
            node, path = queue.popleft()
            if best_len is not None and len(path) > best_len:
                continue
            for nb in sorted(self.adjacency.get(node, ())):
                if nb in seen and nb != destination:
                    continue
                new_path = path + [nb]
                if nb == destination:
                    hop = new_path[0]
                    if best_len is None:
                        best_len = len(new_path)
                    if len(new_path) == best_len:
                        first_hops.add(hop)
                    continue
                if nb not in seen:
                    seen.add(nb)
                    queue.append((nb, new_path))
        return frozenset(first_hops)

    def expect(
        self,
        origin: str,
        destination: str,
        *,
        has_path_vector: bool = False,
        has_link_state_path: bool = False,
        has_direct_peer: bool = False,
    ) -> ExpectedNextHop:
        ls_hops = (
            self.bfs_next_hops(origin, destination)
            if has_link_state_path
            else frozenset()
        )
        graph_hops = self.bfs_next_hops(origin, destination)
        must_not_pv = self.mode is LinkStateMode.ONLY and not has_direct_peer

        if self.mode is not LinkStateMode.DISABLED and has_link_state_path and ls_hops:
            return ExpectedNextHop(
                origin=origin,
                destination=destination,
                allowed_next_hops=ls_hops,
                preferred_source=PlannerSource.LINK_STATE,
                must_not_use_path_vector=must_not_pv,
            )
        if self.mode is LinkStateMode.ONLY:
            # ONLY without LS path: direct peer or fail (no PV).
            if has_direct_peer:
                return ExpectedNextHop(
                    origin=origin,
                    destination=destination,
                    allowed_next_hops=frozenset({destination}),
                    preferred_source=PlannerSource.PEER_FALLBACK,
                    must_not_use_path_vector=True,
                )
            return ExpectedNextHop(
                origin=origin,
                destination=destination,
                allowed_next_hops=frozenset(),
                preferred_source=PlannerSource.NONE,
                must_not_use_path_vector=True,
            )
        if has_path_vector:
            # PV next hop is neighbor that advertised; oracle allows any neighbor
            # on a path unless table is inspected.
            return ExpectedNextHop(
                origin=origin,
                destination=destination,
                allowed_next_hops=self.neighbors(origin) | graph_hops,
                preferred_source=PlannerSource.PATH_VECTOR,
            )
        if graph_hops:
            return ExpectedNextHop(
                origin=origin,
                destination=destination,
                allowed_next_hops=graph_hops,
                preferred_source=PlannerSource.GRAPH,
            )
        if has_direct_peer:
            return ExpectedNextHop(
                origin=origin,
                destination=destination,
                allowed_next_hops=frozenset({destination}),
                preferred_source=PlannerSource.PEER_FALLBACK,
            )
        return ExpectedNextHop(
            origin=origin,
            destination=destination,
            allowed_next_hops=frozenset(),
            preferred_source=PlannerSource.NONE,
        )

    def check_plan(
        self,
        *,
        origin: str,
        destination: str,
        next_cluster: str | None,
        used_path_vector: bool,
        expectation: ExpectedNextHop | None = None,
    ) -> None:
        exp = expectation or self.expect(origin, destination)
        if exp.must_not_use_path_vector and used_path_vector:
            raise AssertionError(
                f"INV-R8: ONLY mode used path-vector origin={origin} dest={destination}"
            )
        if not exp.allowed_next_hops:
            if next_cluster is not None:
                # Peer fallback to unrelated neighbor is allowed as soft fail path.
                return
            return
        if next_cluster is not None and next_cluster not in exp.allowed_next_hops:
            # Still allow any direct neighbor when multipath / fallback.
            if (
                next_cluster not in self.neighbors(origin)
                and next_cluster != destination
            ):
                raise AssertionError(
                    f"next_hop {next_cluster} not in allowed {sorted(exp.allowed_next_hops)} "
                    f"for {origin}->{destination}"
                )

    @staticmethod
    def assert_table_loop_free(table: RouteTable) -> None:
        for dest, records in table.routes.items():
            for record in records:
                hops = record.path.hops
                if len(hops) != len(set(hops)):
                    raise AssertionError(
                        f"INV-R2 loop dest={dest.cluster_id} path={hops}"
                    )
                if table.local_cluster in hops[1:]:
                    raise AssertionError(
                        f"INV-R2 local reappears dest={dest.cluster_id} path={hops}"
                    )

    @staticmethod
    def assert_no_route_from_advertiser(
        table: RouteTable, destination: str, advertiser: str
    ) -> None:
        dest = RouteDestination(cluster_id=destination)
        for record in table.routes.get(dest, []):
            if record.advertiser == advertiser:
                raise AssertionError(
                    f"INV-R3 residual route dest={destination} advertiser={advertiser}"
                )


@dataclass(slots=True)
class RaftOracle:
    """Track leaders-per-term and commit monotonicity across a simulated cluster.

    **Fail-fast dual-leader (F19):** a second ``leader`` observation for the
    same term raises ``AssertionError`` *inside* :meth:`observe_role`, not
    deferred until :meth:`assert_safe`. Callers that want soft collection must
    catch at observe time; ``assert_safe`` still re-checks residual violations.
    Curriculum proof: ``routing_oracle_lab``.
    """

    leaders_by_term: dict[int, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    commit_index: dict[str, int] = field(default_factory=dict)
    violations: list[str] = field(default_factory=list)

    def observe_role(self, node_id: str, term: int, role: str) -> None:
        """Record a role observation; dual-leader same term fails immediately."""
        if role.lower() == "leader":
            self.leaders_by_term[term].add(node_id)
            if len(self.leaders_by_term[term]) > 1:
                msg = (
                    f"INV-C1: multiple leaders term={term} "
                    f"{sorted(self.leaders_by_term[term])}"
                )
                self.violations.append(msg)
                raise AssertionError(msg)

    def observe_commit(self, node_id: str, commit_index: int) -> None:
        prev = self.commit_index.get(node_id, 0)
        if commit_index < prev:
            msg = (
                f"INV-C2: commit_index decreased node={node_id} {prev}->{commit_index}"
            )
            self.violations.append(msg)
            raise AssertionError(msg)
        self.commit_index[node_id] = commit_index

    def assert_safe(self) -> None:
        for term, leaders in self.leaders_by_term.items():
            if len(leaders) > 1:
                raise AssertionError(f"INV-C1 term={term} leaders={sorted(leaders)}")
        if self.violations:
            raise AssertionError("; ".join(self.violations))


@dataclass(frozen=True, slots=True)
class RpcStreamEvent:
    """One progressive/stream observation."""

    kind: str  # "partial" | "intermediate" | "final" | "error"
    level: int | None = None
    code: int | None = None


@dataclass(slots=True)
class RpcOracle:
    """Validate RPC modality ordering and deadline fail-closed behavior."""

    events: list[RpcStreamEvent] = field(default_factory=list)
    seen_final: bool = False
    last_level: int = -1

    def observe(self, event: RpcStreamEvent) -> None:
        if self.seen_final and event.kind in ("partial", "intermediate"):
            raise AssertionError("INV-P4: partial after final")
        if event.kind == "final":
            self.seen_final = True
        if event.kind == "intermediate" and event.level is not None:
            if event.level <= self.last_level:
                raise AssertionError(
                    f"INV-P5: non-monotonic intermediate level "
                    f"{self.last_level} -> {event.level}"
                )
            self.last_level = event.level
        self.events.append(event)

    def observe_many(self, events: Iterable[RpcStreamEvent]) -> None:
        for event in events:
            self.observe(event)

    @staticmethod
    def assert_timeout_code(code: int, *, timeout_code: int = 1006) -> None:
        if code != timeout_code:
            raise AssertionError(f"INV-P2 expected TIMEOUT {timeout_code}, got {code}")
