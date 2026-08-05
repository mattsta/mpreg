"""L3 routing_oracle_lab — RoutingOracle + RaftOracle safety model."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.testing.oracles import RaftOracle, RoutingOracle, RpcOracle, RpcStreamEvent

async def main() -> None:
    with app_run(
        "routing_oracle_lab",
        "Routing Oracle Lab — BFS next hops + Raft safety",
        level="L3",
    ):
        with scenario("build graph edges", "oracle.routing", "fabric.graph"):
            ro = RoutingOracle()
            ro.set_edge("a", "b")
            ro.set_edge("b", "c")
            ro.set_edge("a", "d")
            ro.set_edge("d", "c")
            hops = ro.bfs_next_hops("a", "c")
            ensure(hops == frozenset({"b", "d"}), f"next hops {hops}")
            ensure(ro.neighbors("a") == frozenset({"b", "d"}), f"neighbors {ro.neighbors('a')}")
            ok(f"a→c next={sorted(hops)}")

        with scenario("edge removal updates paths", "oracle.routing"):
            ro = RoutingOracle()
            ro.set_edge("a", "b")
            ro.set_edge("b", "c")
            ro.set_edge("a", "c")  # direct
            ensure("c" in ro.bfs_next_hops("a", "c"), "direct hop")
            ro.remove_edge("a", "c")
            hops = ro.bfs_next_hops("a", "c")
            ensure(hops == frozenset({"b"}), f"after remove {hops}")
            ok(f"after remove direct: next={sorted(hops)}")

        with scenario("expect helper records intent", "oracle.routing"):
            ro = RoutingOracle()
            ro.set_edge("x", "y")
            # expect(origin, destination, *, has_direct_peer=…)
            exp = ro.expect("x", "y", has_direct_peer=True)
            ensure(exp is not None, "expect returned None")
            ok(f"expect recorded type={type(exp).__name__}")

        with scenario("Raft single leader per term", "oracle.raft", "cons.quorum_teach"):
            raft = RaftOracle()
            raft.observe_role("n1", term=1, role="leader")
            raft.observe_role("n2", term=1, role="follower")
            raft.observe_role("n3", term=1, role="follower")
            raft.assert_safe()
            ok("term1 single leader safe")

        with scenario("Raft dual leader same term fails", "oracle.raft"):
            raft = RaftOracle()
            raft.observe_role("n1", term=2, role="leader")
            failed = False
            try:
                # Friction F19: dual-leader raises on observe_role, not only assert_safe
                raft.observe_role("n2", term=2, role="leader")
            except Exception as exc:
                failed = True
                step(f"expected unsafe on observe: {type(exc).__name__}: {exc}")
            ensure(failed, "dual leader should be unsafe")
            ok("dual leader detected at observe_role (F19)")

        with scenario("Raft commit monotonic + RpcOracle timeout code", "oracle.raft"):
            raft = RaftOracle()
            raft.observe_role("n1", term=3, role="leader")
            raft.observe_commit("n1", 1)
            raft.observe_commit("n1", 2)
            raft.assert_safe()
            RpcOracle.assert_timeout_code(1006)
            bad = False
            try:
                RpcOracle.assert_timeout_code(1)
            except Exception:
                bad = True
            ensure(bad, "wrong timeout code should fail")
            ev = RpcStreamEvent(kind="timeout", code=1006)
            ensure(ev.kind == "timeout", f"event {ev}")
            ok("commit monotonic + timeout code helper")
            step("lab oracles model correctness; not live fabric control plane")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())
