"""L1 plane_cache — capability plane tour (unified from legacy tier1)."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import ok, step
from mpreg.examples.tier1_single_system_full import demo_cache

async def main() -> None:
    step("running unified plane tour: cache")
    await demo_cache()
    ok("plane_cache plane tour completed")

if __name__ == "__main__":
    asyncio.run(main())
