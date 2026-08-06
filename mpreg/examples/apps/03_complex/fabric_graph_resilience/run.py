"""L3 fabric_graph_resilience — FederationGraph Dijkstra + circuit breaker drill."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.federation_graph import (
    DijkstraRouter,
    FederationGraph,
    FederationGraphEdge,
    FederationGraphNode,
    GeographicCoordinate,
    NodeType,
)
from mpreg.fabric.federation_optimized import CircuitBreaker

def _node(node_id: str, region: str, lat: float, lon: float) -> FederationGraphNode:
    return FederationGraphNode(
        node_id=node_id,
        node_type=NodeType.CLUSTER,
        region=region,
        coordinates=GeographicCoordinate(lat, lon),
        max_capacity=1000,
    )

async def main() -> None:
    with app_run(
        "fabric_graph_resilience",
        "Fabric Graph + Resilience — Dijkstra path + circuit breaker",
        level="L3",
    ):
        graph = FederationGraph(cache_ttl_seconds=30.0)

        with scenario(
            "build three-cluster topology",
            "fabric.graph",
        ):
            graph.add_node(_node("us-west", "us", 37.77, -122.42))
            graph.add_node(_node("us-east", "us", 40.71, -74.00))
            graph.add_node(_node("eu-west", "eu", 51.50, -0.12))
            graph.add_edge(
                FederationGraphEdge(
                    "us-west",
                    "us-east",
                    latency_ms=70.0,
                    bandwidth_mbps=1000,
                    reliability_score=0.99,
                )
            )
            graph.add_edge(
                FederationGraphEdge(
                    "us-east",
                    "eu-west",
                    latency_ms=90.0,
                    bandwidth_mbps=800,
                    reliability_score=0.98,
                )
            )
            graph.add_edge(
                FederationGraphEdge(
                    "us-west",
                    "eu-west",
                    latency_ms=140.0,
                    bandwidth_mbps=500,
                    reliability_score=0.95,
                )
            )
            ensure(len(graph.nodes) == 3, f"nodes={len(graph.nodes)}")
            # adjacency is bidirectional → count unique undirected edges carefully
            edge_count = sum(len(v) for v in graph.adjacency.values()) // 2
            ensure(edge_count == 3, f"expected 3 undirected edges got {edge_count}")
            ok(f"topology nodes=3 edges={edge_count}")

        with scenario(
            "Dijkstra prefers lower-latency multi-hop path",
            "fabric.graph",
        ):
            router = DijkstraRouter(graph=graph)
            path = router.find_optimal_path("us-west", "eu-west", max_hops=5)
            ensure(path is not None, "no path us-west→eu-west")
            ensure(path[0] == "us-west" and path[-1] == "eu-west", f"bad ends {path}")
            # 70+90=160 via east vs 140 direct — direct should win
            ensure(
                path == ["us-west", "eu-west"] or "us-east" in path,
                f"unexpected path {path}",
            )
            # Prefer direct when cheaper
            ensure(
                path == ["us-west", "eu-west"],
                f"expected direct path, got {path}",
            )
            ok(f"optimal path={path}")

        with scenario(
            "removing direct edge forces multi-hop",
            "fabric.graph",
        ):
            removed = graph.remove_edge("us-west", "eu-west")
            ensure(removed is True, "remove_edge failed")
            router2 = DijkstraRouter(graph=graph)
            path2 = router2.find_optimal_path("us-west", "eu-west", max_hops=5)
            ensure(path2 is not None, "no alternate path")
            ensure(
                path2 == ["us-west", "us-east", "eu-west"],
                f"expected via us-east got {path2}",
            )
            ok(f"failover path={path2}")

        with scenario(
            "circuit breaker opens after failure threshold",
            "fabric.resilience",
        ):
            cb = CircuitBreaker(
                failure_threshold=3,
                success_threshold=2,
                timeout_seconds=0.3,
                max_timeout_seconds=2.0,
                # Phase G F9 fix: current_timeout syncs from timeout_seconds in __post_init__
            )
            ensure(
                cb.current_timeout == 0.3, f"F9 current_timeout={cb.current_timeout}"
            )
            ensure(cb.can_execute() is True, "closed breaker should allow")
            ensure(cb.state == "closed", f"state={cb.state}")
            for _ in range(3):
                cb.record_failure()
            ensure(cb.state == "open", f"expected open got {cb.state}")
            ensure(cb.can_execute() is False, "open breaker should block")
            ok("breaker opened after 3 failures")

        with scenario(
            "breaker half-opens after timeout then closes on success",
            "fabric.resilience",
        ):
            # Wait past timeout
            await asyncio.sleep(0.35)
            ensure(cb.can_execute() is True, "should half-open after timeout")
            ensure(
                cb.state in {"half_open", "closed"},
                f"unexpected state after timeout {cb.state}",
            )
            cb.record_success()
            cb.record_success()
            ensure(
                cb.state == "closed", f"expected closed after successes got {cb.state}"
            )
            ensure(cb.can_execute() is True, "closed should allow")
            ok(f"breaker recovered state={cb.state}")

        step(
            f"non-claim: lab-scale graph helpers — not planet OSPF/BGP SLA "
            f"(path_computations={graph.path_computations})"
        )
        ensure(graph.path_computations >= 1, "no path computations recorded")
        ok("graph stats recorded")

if __name__ == "__main__":
    asyncio.run(main())
