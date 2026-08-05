"""L1 plane_queue — capability plane tour (unified from legacy tier1)."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import ok, step
from mpreg.examples.tier1_single_system_full import demo_queue

async def main() -> None:
    step("running unified plane tour: queue")
    await demo_queue()
    ok("plane_queue plane tour completed")

if __name__ == "__main__":
    asyncio.run(main())
