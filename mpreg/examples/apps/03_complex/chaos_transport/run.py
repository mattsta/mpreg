"""L3 chaos_transport — clock skew, duplicate, reorder, drop via FaultInjector."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.testing.faults import FaultInjector, FaultKind


async def main() -> None:
    with app_run(
        "chaos_transport",
        "Chaos Transport — skew, dup, reorder, drop model",
        level="L3",
    ):
        with scenario("seeded injector baseline", "chaos.transport", "chaos.partition"):
            inj = FaultInjector(seed=7, data_drop_rate=0.0, control_drop_rate=0.0)
            ensure(len(inj.decisions) == 0, "fresh injector should have no decisions")
            ensure(inj.view().can_communicate("a", "b"), "baseline should connect")
            ok(f"seed={inj.seed} kinds={[k.value for k in FaultKind]}")

        with scenario("clock skew per node", "chaos.clock_skew"):
            inj = FaultInjector(seed=7)
            wall = 1_700_000_000.0
            inj.set_clock_skew("node-a", 5.0)
            inj.set_clock_skew("node-b", -2.5)
            view = inj.view()
            ta = view.now_for("node-a", wall)
            tb = view.now_for("node-b", wall)
            ensure(abs(ta - (wall + 5.0)) < 1e-9, f"node-a skew {ta}")
            ensure(abs(tb - (wall - 2.5)) < 1e-9, f"node-b skew {tb}")
            ensure(
                view.clock_skew_seconds.get("node-a") == 5.0,
                f"skew map {view.clock_skew_seconds}",
            )
            ok(f"now_for a={ta - wall:+.1f}s b={tb - wall:+.1f}s")
            inj.clear_clock_skew()
            ensure(
                inj.view().clock_skew_seconds == {},
                f"clear failed {inj.view().clock_skew_seconds}",
            )
            ok("clear_clock_skew emptied map")

        with scenario("duplicate rate sampling", "chaos.duplicate"):
            inj = FaultInjector(seed=99, duplicate_rate=1.0)
            dups = sum(1 for _ in range(20) if inj.should_duplicate())
            ensure(dups == 20, f"rate=1.0 expected 20 dups got {dups}")
            inj2 = FaultInjector(seed=99, duplicate_rate=0.0)
            dups0 = sum(1 for _ in range(20) if inj2.should_duplicate())
            ensure(dups0 == 0, f"rate=0 expected 0 dups got {dups0}")
            ok(f"duplicate_rate 1.0→{dups} 0.0→{dups0}")

        with scenario("reorder when buffer deep enough", "chaos.reorder"):
            inj = FaultInjector(seed=3)
            # API: should_reorder(buffer_len) — needs ≥2 buffered msgs
            no = inj.should_reorder(0) or inj.should_reorder(1)
            ensure(not no, "buffer_len < 2 should never reorder")
            hits = sum(1 for _ in range(50) if inj.should_reorder(5))
            ensure(hits >= 1, f"expected some reorders with len=5 got {hits}")
            ok(f"reorder hits/50={hits} (buffer_len=5)")

        with scenario(
            "plane-separated drop rates",
            "chaos.drop",
            "chaos.transport",
        ):
            inj = FaultInjector(
                seed=1,
                control_drop_rate=1.0,
                data_drop_rate=0.0,
            )
            # With drop rate 1.0, control plane always drops if connected
            control_ok = inj.can_deliver("x", "y", plane="control")
            data_ok = inj.can_deliver("x", "y", plane="data")
            ensure(control_ok is False, "control_drop_rate=1 should drop")
            ensure(data_ok is True, "data_drop_rate=0 should deliver")
            ok("control dropped; data delivered (plane separation)")
            kinds = {d.get("kind") for d in inj.decisions}
            ensure("drop_random" in kinds, f"expected drop_random in {kinds}")
            ok(f"decision kinds={sorted(str(k) for k in kinds)}")

        with scenario("partition + crash compose with transport", "chaos.crash"):
            inj = FaultInjector(seed=2, data_drop_rate=0.0)
            inj.partition({"east"}, {"west"})
            ensure(
                not inj.can_deliver("east", "west", plane="data"),
                "partition should block east→west",
            )
            inj.crash("east")
            ensure(
                not inj.view().can_communicate("east", "east-peer"),
                "crashed node cannot communicate",
            )
            inj.recover("east")
            inj.heal()
            ensure(inj.view().can_communicate("east", "west"), "heal+recover failed")
            ok(f"composed events={len(inj.decisions)}")
            step(
                "non-claim: FaultInjector is a lab model — live WS partition hooks "
                "are separate platform work"
            )

        await asyncio.sleep(0)  # keep async main honest


if __name__ == "__main__":
    asyncio.run(main())
