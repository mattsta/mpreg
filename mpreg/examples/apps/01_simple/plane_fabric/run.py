"""L1 plane_fabric — capability plane tour (unified from legacy tier1)."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import ok, step
from mpreg.examples.tier1_single_system_full import demo_fabric

async def main() -> None:
    step("running unified plane tour: fabric")
    await demo_fabric()
    ok("plane_fabric plane tour completed")

if __name__ == "__main__":
    asyncio.run(main())
