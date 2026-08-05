"""L3 partition_safe_counter — FaultInjector teaches majority vs minority partitions."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import ensure, ok, step
from mpreg.testing.faults import FaultInjector

async def main() -> None:
    """Honest Raft-shaped teaching without spinning a full ProductionRaft cluster.

    Full ProductionRaft integration lives in tests/test_production_raft_*.py.
    This app proves the *operator mental model* with FaultInjector:
    minority partitions cannot form a majority quorum.
    """
    nodes = {"n1", "n2", "n3", "n4", "n5"}
    injector = FaultInjector(seed=7)

    step("healthy mesh: all nodes can talk")
    view = injector.view()
    for a in nodes:
        for b in nodes:
            if a != b:
                ensure(view.can_communicate(a, b), f"{a}->{b} should connect")
    ok("healthy mesh connectivity")

    majority = {"n1", "n2", "n3"}
    minority = {"n4", "n5"}
    step(f"partition majority={sorted(majority)} minority={sorted(minority)}")
    injector.partition(majority, minority)
    view = injector.view()

    ensure(view.can_communicate("n1", "n2"), "majority internal")
    ensure(view.can_communicate("n4", "n5"), "minority internal")
    ensure(not view.can_communicate("n1", "n4"), "cross-partition blocked")
    ensure(not view.can_communicate("n5", "n2"), "cross-partition blocked")

    # Quorum sizes (Raft-style majority of 5 is 3)
    majority_size = len(majority)
    minority_size = len(minority)
    cluster_n = len(nodes)
    quorum = (cluster_n // 2) + 1
    ensure(majority_size >= quorum, "majority must meet quorum")
    ensure(minority_size < quorum, "minority must NOT meet quorum")
    ok(
        f"quorum={quorum}: majority({majority_size}) can commit; "
        f"minority({minority_size}) cannot (honest Raft teaching)"
    )

    step("heal partition")
    injector.heal()
    view = injector.view()
    ensure(view.can_communicate("n1", "n4"), "healed")
    ok("partition healed; full mesh restored")

    step("crash one majority node — remaining majority still >= quorum-1 check")
    injector.crash("n3")
    view = injector.view()
    ensure(not view.can_communicate("n1", "n3"), "crashed node isolated")
    alive_majority = majority - {"n3"}
    ensure(len(alive_majority) + 0 >= 2, "alive peers remain")
    # After crash without re-partition, non-crashed nodes still communicate
    ensure(view.can_communicate("n1", "n2"), "n1-n2 still up")
    ok(f"crash decisions recorded: {len(injector.decisions)} fault events")

if __name__ == "__main__":
    asyncio.run(main())
