"""L2 topic_dependency_lab — TopicDependencyResolver graph + ready commands."""

from __future__ import annotations

import asyncio

from mpreg.core.enhanced_rpc import TopicAwareRPCCommand
from mpreg.core.topic_dependency_resolver import (
    DependencyGraph,
    DependencyResolutionEvent,
    DependencySpecification,
    DependencyStatus,
    DependencyType,
    create_topic_dependency_resolver,
)
from mpreg.core.topic_taxonomy import TopicTemplateEngine
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step


async def main() -> None:
    with app_run(
        "topic_dependency_lab",
        "Topic Dependency Lab — graph analysis + ready set",
        level="L2",
    ):
        engine = TopicTemplateEngine()
        resolver = create_topic_dependency_resolver(
            topic_template_engine=engine,
            default_timeout_ms=5000.0,
            max_concurrent_subscriptions=50,
        )

        with scenario("factory + empty stats", "rpc.topic_aware", "rpc.dependency"):
            ensure(resolver.topic_template_engine is not None, "no engine")
            ensure(resolver.default_timeout_ms == 5000.0, "timeout")
            stats = await resolver.get_dependency_statistics()
            ensure(isinstance(stats, dict), f"stats type {type(stats)}")
            ensure(len(resolver.active_dependency_graphs) == 0, "pre-existing graphs")
            ok(f"stats_keys={sorted(stats.keys())[:8]}")

        with scenario(
            "graph from explicit dependency patterns",
            "rpc.dependency",
            "rpc.topic_aware",
        ):
            cmd_a = TopicAwareRPCCommand(
                name="load",
                fun="load_user",
                args=("u1",),
                command_id="cmd-a",
            )
            cmd_b = TopicAwareRPCCommand(
                name="enrich",
                fun="enrich_user",
                args=("user_load.result",),
                kwargs={"profile": "cache_user.data"},
                dependency_topic_patterns=[
                    "mpreg.rpc.request.req1.command.cmd-a.result"
                ],
                command_id="cmd-b",
            )
            cmd_c = TopicAwareRPCCommand(
                name="notify",
                fun="notify",
                args=(),
                dependency_topic_patterns=[
                    "mpreg.rpc.request.req1.command.cmd-b.result"
                ],
                command_id="cmd-c",
            )
            g = await resolver.create_dependency_graph(
                "req-dep-1", [cmd_a, cmd_b, cmd_c]
            )
            ensure(g.request_id == "req-dep-1", g.request_id)
            ensure(
                len(g.command_dependencies) == 3, f"cmds {len(g.command_dependencies)}"
            )
            ensure(g.total_dependencies >= 1, f"deps {g.total_dependencies}")
            ensure("req-dep-1" in resolver.active_dependency_graphs, "not tracked")
            # cmd_a has no deps → ready immediately
            ready = g.get_ready_commands()
            ensure("cmd-a" in ready, f"ready {ready}")
            ensure(
                "cmd-b" not in ready or g.total_dependencies == 0, f"b ready? {ready}"
            )
            ok(
                f"deps={g.total_dependencies} cross={g.cross_system_dependencies} "
                f"ready={sorted(ready)} progress={g.resolution_progress:.0f}%"
            )

        with scenario("manual DependencyGraph ready set", "rpc.dependency"):
            dep1 = DependencySpecification(
                dependency_id="d1",
                dependency_type=DependencyType.RPC_COMMAND,
                topic_pattern="mpreg.rpc.x.result",
                required=True,
            )
            dep2 = DependencySpecification(
                dependency_id="d2",
                dependency_type=DependencyType.CACHE_UPDATE,
                topic_pattern="mpreg.cache.ns.key",
                required=False,
            )
            g = DependencyGraph(
                request_id="manual-1",
                command_dependencies={
                    "c1": [],
                    "c2": [dep1],
                    "c3": [dep1, dep2],
                },
                dependency_edges={"d1": {"c2", "c3"}, "d2": {"c3"}},
                pending_dependencies={"d1", "d2"},
                total_dependencies=2,
                cross_system_dependencies=1,
            )
            ensure(g.resolution_progress == 0.0, f"prog {g.resolution_progress}")
            ensure(g.all_dependencies_resolved is False, "should pending")
            ready0 = g.get_ready_commands()
            ensure(ready0 == {"c1"}, f"ready0 {ready0}")
            # Resolve d1
            g.resolved_dependencies["d1"] = DependencyResolutionEvent(
                dependency_id="d1",
                command_id="c2",
                request_id="manual-1",
                status=DependencyStatus.RESOLVED,
                resolved_value={"ok": True},
            )
            g.pending_dependencies.discard("d1")
            ready1 = g.get_ready_commands()
            ensure("c1" in ready1 and "c2" in ready1, f"ready1 {ready1}")
            # d2 is optional (required=False) so c3 is ready once required d1 resolves
            ensure("c3" in ready1, f"c3 should ready optional d2: {ready1}")
            ok(f"ready after d1={sorted(ready1)} progress={g.resolution_progress:.0f}%")

        with scenario("cleanup_request_dependencies", "rpc.dependency"):
            cmd = TopicAwareRPCCommand(name="solo", fun="solo", command_id="solo-1")
            await resolver.create_dependency_graph("req-clean", [cmd])
            ensure("req-clean" in resolver.active_dependency_graphs, "missing")
            cleaned = await resolver.cleanup_request_dependencies("req-clean")
            ensure(cleaned is True, "cleanup false")
            ensure("req-clean" not in resolver.active_dependency_graphs, "still there")
            # Missing id is a no-op success (idempotent cleanup)
            cleaned2 = await resolver.cleanup_request_dependencies("nope")
            ensure(cleaned2 is True, "missing cleanup should be idempotent True")
            ok("cleanup ok")

        with scenario("arg heuristic detects .result refs", "rpc.dependency"):
            cmd = TopicAwareRPCCommand(
                name="use",
                fun="use_result",
                args=("upstream_result",),  # contains _result
                kwargs={"data": "queue_orders.message"},
                command_id="heur-1",
            )
            g = await resolver.create_dependency_graph("req-heur", [cmd])
            deps = g.command_dependencies["heur-1"]
            ensure(len(deps) >= 1, f"expected heuristic deps, got {deps}")
            types = {d.dependency_type for d in deps}
            step(f"inferred types={sorted(t.value for t in types)} n={len(deps)}")
            ok(f"heuristic deps={len(deps)}")

        with scenario("empty command list graph", "rpc.dependency"):
            g = await resolver.create_dependency_graph("req-empty", [])
            ensure(g.total_dependencies == 0, g.total_dependencies)
            ensure(g.resolution_progress == 100.0, g.resolution_progress)
            ensure(g.get_ready_commands() == set(), "empty ready")
            await resolver.cleanup_request_dependencies("req-empty")
            ok("empty graph 100% progress")


if __name__ == "__main__":
    asyncio.run(main())
