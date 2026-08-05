"""L3 tier3_expansion — unified from legacy tier3_full_system_expansion."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import ok, step
from mpreg.examples.tier3_full_system_expansion import main as tier3_main

async def main() -> None:
    step("full system expansion tour (legacy tier3)")
    await tier3_main()
    ok("tier3_expansion completed")

if __name__ == "__main__":
    asyncio.run(main())
