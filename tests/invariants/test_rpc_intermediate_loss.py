"""C4: Intermediate results under hop/level loss — partial recovery story."""

from __future__ import annotations

from mpreg.core.intermediate_results import IntermediateResultCollector
from mpreg.testing.faults import FaultInjector
from mpreg.testing.oracles import RpcOracle, RpcStreamEvent

def test_partial_levels_survived_under_loss() -> None:
    """When later hops drop, clients still observe completed levels monotonically."""
    inj = FaultInjector(seed=7, control_drop_rate=0.0)
    coll = IntermediateResultCollector(request_id="req-loss", total_levels=4)
    oracle = RpcOracle()
    delivered = 0
    for level in range(4):
        coll.start_level(level)
        # Simulate hop loss on level >= 2 toward progress topic
        if level >= 2 and not inj.can_deliver("exec", "client", plane="data"):
            break
        # Force drop on level 2+
        if level >= 2:
            inj.control_drop_rate = 1.0
            if not inj.can_deliver("exec", "client", plane="control"):
                # Documented recovery: client keeps last intermediate; no final
                break
        mid = coll.complete_level(
            level,
            {f"L{level}": level},
            {f"L{i}": i for i in range(level + 1)},
        )
        oracle.observe(RpcStreamEvent(kind="intermediate", level=mid.level_index))
        delivered += 1

    assert delivered >= 2  # first levels always observed
    assert len(coll.intermediate_results) == delivered
    # No silent wrong final
    assert (
        not any(r.is_final_level for r in coll.intermediate_results) or delivered == 4
    )
