"""L3 partition_safe_counter — majority vs minority quorum teaching (cons/chaos)."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.testing.faults import FaultInjector


async def main() -> None:
    """Honest Raft-shaped teaching without spinning a full ProductionRaft cluster.

    Full ProductionRaft integration lives in tests/test_production_raft_*.py.
    This app proves the *operator mental model* with FaultInjector:
    minority partitions cannot form a majority quorum.
    """
    with app_run(
        "partition_safe_counter",
        "Partition-Safe Counter — quorum teaching",
        level="L3",
    ):
        nodes = {"n1", "n2", "n3", "n4", "n5"}
        injector = FaultInjector(seed=7)

        with scenario("healthy mesh connectivity", "chaos.partition"):
            view = injector.view()
            for a in nodes:
                for b in nodes:
                    if a != b:
                        ensure(view.can_communicate(a, b), f"{a}->{b} should connect")
            ok("healthy mesh connectivity")

        majority = {"n1", "n2", "n3"}
        minority = {"n4", "n5"}

        with scenario(
            "majority vs minority partition",
            "cons.quorum_teach",
            "chaos.partition",
        ):
            step(f"partition majority={sorted(majority)} minority={sorted(minority)}")
            injector.partition(majority, minority)
            view = injector.view()

            ensure(view.can_communicate("n1", "n2"), "majority internal")
            ensure(view.can_communicate("n4", "n5"), "minority internal")
            ensure(not view.can_communicate("n1", "n4"), "cross-partition blocked")
            ensure(not view.can_communicate("n5", "n2"), "cross-partition blocked")

            majority_size = len(majority)
            minority_size = len(minority)
            cluster_n = len(nodes)
            quorum = (cluster_n // 2) + 1
            ensure(majority_size >= quorum, "majority must meet quorum")
            ensure(minority_size < quorum, "minority must NOT meet quorum")
            ok(
                f"quorum={quorum}: majority({majority_size}) can commit; "
                f"minority({minority_size}) cannot"
            )

        with scenario("heal restores mesh", "chaos.heal"):
            injector.heal()
            view = injector.view()
            ensure(view.can_communicate("n1", "n4"), "healed")
            ok("partition healed; full mesh restored")

        with scenario("crash isolates node", "chaos.crash"):
            injector.crash("n3")
            view = injector.view()
            ensure(not view.can_communicate("n1", "n3"), "crashed node isolated")
            alive_majority = majority - {"n3"}
            ensure(len(alive_majority) >= 2, "alive peers remain")
            ensure(view.can_communicate("n1", "n2"), "n1-n2 still up")
            ok(f"crash decisions recorded: {len(injector.decisions)} fault events")
            step(
                "non-claim: teaching model only — see tests/test_production_raft_* "
                "for live Raft"
            )


if __name__ == "__main__":
    asyncio.run(main())
