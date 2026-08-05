"""L3 tier3_expansion — full multi-system expansion with scenario banner."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import app_run, ok, scenario, step
from mpreg.examples.tier3_full_system_expansion import main as tier3_main

async def main() -> None:
    with app_run(
        "tier3_expansion",
        "Tier3 Expansion — multi-plane system tour",
        level="L3",
    ):
        with scenario(
            "legacy tier3 full system expansion",
            "prod.tier3",
            "rpc.dag",
            "cache.l4",
            "pubsub.exchange",
            "queue.alo",
            "fabric.cross_rpc",
            "mon.timeline",
        ):
            step("running tier3_full_system_expansion backend")
            await tier3_main()
            ok("tier3 backend completed")
        step(
            "non-claim: expansion sketch — see FEATURE_CATALOG gaps for DNS/namespace"
        )

if __name__ == "__main__":
    asyncio.run(main())
