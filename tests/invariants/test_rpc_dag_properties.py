"""C2: Level order and hop-budget properties for RPC DAGs."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.errors import MpregErrorCode
from mpreg.core.intermediate_results import IntermediateResultCollector
from mpreg.testing.oracles import RpcOracle, RpcStreamEvent

def topological_levels(edges: list[tuple[int, int]], n: int) -> list[list[int]]:
    """Kahn levels: nodes with no remaining deps form a level."""
    indeg = [0] * n
    succ: list[list[int]] = [[] for _ in range(n)]
    for a, b in edges:
        if 0 <= a < n and 0 <= b < n and a != b:
            succ[a].append(b)
            indeg[b] += 1
    remaining = set(range(n))
    levels: list[list[int]] = []
    while remaining:
        ready = sorted(i for i in remaining if indeg[i] == 0)
        if not ready:
            # cycle — break deterministically
            ready = [min(remaining)]
        levels.append(ready)
        for u in ready:
            remaining.discard(u)
            for v in succ[u]:
                indeg[v] -= 1
    return levels

@given(
    n=st.integers(min_value=1, max_value=8),
    edge_data=st.lists(
        st.tuples(st.integers(0, 7), st.integers(0, 7)), min_size=0, max_size=12
    ),
)
@settings(max_examples=80, deadline=None)
def test_levels_cover_all_nodes(n: int, edge_data: list[tuple[int, int]]) -> None:
    edges = [(a % n, b % n) for a, b in edge_data]
    levels = topological_levels(edges, n)
    seen = [node for level in levels for node in level]
    assert sorted(seen) == list(range(n))
    # INV-P1: level indices strictly increase in collector
    coll = IntermediateResultCollector(request_id="r", total_levels=len(levels))
    oracle = RpcOracle()
    for i, level in enumerate(levels):
        coll.start_level(i)
        mid = coll.complete_level(
            i, {str(x): x for x in level}, {str(x): x for x in level}
        )
        oracle.observe(RpcStreamEvent(kind="intermediate", level=mid.level_index))
    oracle.observe(RpcStreamEvent(kind="final"))

def test_hop_budget_code() -> None:
    from mpreg.server_pkg.rpc_handlers import RpcPlane

    resp = RpcPlane.hop_budget_exceeded("u")
    assert resp.error.code == int(MpregErrorCode.HOP_BUDGET_EXCEEDED)
