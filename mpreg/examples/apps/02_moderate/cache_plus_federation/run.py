"""L2 cache_plus_federation — unified from legacy tier2 integrations."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import ok, step
from mpreg.examples.tier2_integrations import cache_plus_federation

async def main() -> None:
    step("running integration tour: cache_plus_federation")
    await cache_plus_federation()
    ok("cache_plus_federation completed")

if __name__ == "__main__":
    asyncio.run(main())
